#!/usr/bin/env python3
"""Bounded, resumable, deterministic JSONL -> Parquet benchmark."""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import queue
import resource
import shutil
import sys
import tempfile
import threading
import time
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, Iterator, Optional

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - exercised by environment gating
    pa = pq = None

LOG = logging.getLogger("acl_benchmark")
CHECKPOINT_VERSION = 1
PROVENANCE_VERSION = "1"


class BenchmarkError(RuntimeError): pass
class GovernancePause(BenchmarkError): pass
class IncompatibleResume(BenchmarkError): pass


def parse_bytes(value: str | int) -> int:
    if isinstance(value, int): return value
    s = str(value).strip().upper().replace(" ", "")
    units = {"B": 1, "KIB": 1024, "MIB": 1024**2, "GIB": 1024**3,
             "KB": 1000, "MB": 1000**2, "GB": 1000**3}
    for unit in sorted(units, key=len, reverse=True):
        if s.endswith(unit):
            try: return int(float(s[:-len(unit)]) * units[unit])
            except ValueError: break
    try: return int(s)
    except ValueError as exc: raise ValueError(f"invalid byte size: {value}") from exc


@dataclass(frozen=True)
class GovernanceConfig:
    input_path: str
    output_dir: str
    workers: int = 2
    queue_capacity: int = 2
    result_queue_capacity: int = 2
    batch_rows: int = 1000
    batch_bytes: int = 16 * 1024 * 1024
    max_line_bytes: int = 4 * 1024 * 1024
    shard_rows: int = 100_000
    shard_bytes: int = 128 * 1024 * 1024
    checkpoint_every_records: int = 50_000
    checkpoint_every_seconds: float = 30.0
    min_free_bytes: int = 0
    min_free_percent: float = 0.0
    disk_safety_reserve: int = 0
    compact_target_rows: int = 1_000_000
    deterministic_seed: int = 1337
    filter_threshold: int = 500_000
    log_level: str = "INFO"

    def validate(self) -> "GovernanceConfig":
        positive = ("workers", "queue_capacity", "result_queue_capacity", "batch_rows",
                    "batch_bytes", "max_line_bytes", "shard_rows", "shard_bytes",
                    "checkpoint_every_records", "compact_target_rows")
        for name in positive:
            if getattr(self, name) <= 0: raise ValueError(f"{name} must be positive")
        if self.batch_rows > self.shard_rows or self.batch_bytes > self.shard_bytes:
            raise ValueError("batch limits must not exceed shard limits")
        if not 0 <= self.min_free_percent <= 100: raise ValueError("min_free_percent must be 0..100")
        if self.min_free_bytes < 0 or self.disk_safety_reserve < 0: raise ValueError("disk thresholds cannot be negative")
        return self

    def fingerprint(self) -> str:
        data = asdict(self); data.pop("output_dir", None)
        # Governance thresholds and checkpoint cadence may be changed to recover
        # from an operational pause; logical processing settings may not.
        for key in ("min_free_bytes", "min_free_percent", "disk_safety_reserve",
                    "checkpoint_every_records", "checkpoint_every_seconds", "log_level"):
            data.pop(key, None)
        data["input_path"] = str(Path(self.input_path).resolve())
        return hashlib.sha256(json.dumps(data, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, sort_keys=True, indent=2); fh.write("\n"); fh.flush(); os.fsync(fh.fileno())
        os.replace(name, path)
        try:
            dfd = os.open(path.parent, os.O_DIRECTORY); os.fsync(dfd); os.close(dfd)
        except (OSError, AttributeError): pass
    finally:
        if os.path.exists(name): os.unlink(name)


def file_identity(path: Path) -> dict[str, Any]:
    st = path.stat(); h = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(1024 * 1024): h.update(chunk)
    return {"path": str(path.resolve()), "size": st.st_size, "mtime_ns": st.st_mtime_ns, "sha256": h.hexdigest()}


def canonical(obj: Any) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()


def rolling_digest(previous: str, obj: Any) -> str:
    return hashlib.sha256(bytes.fromhex(previous) + canonical(obj)).hexdigest()


def transform(record: dict[str, Any], threshold: int) -> tuple[bool, dict[str, Any]]:
    value = int(record.get("value", 0)); accepted = value >= threshold
    out = dict(record); out["accepted"] = accepted; out["transform_version"] = 1
    return accepted, out


@dataclass
class Batch:
    seq: int
    start_offset: int
    end_offset: int
    start_line: int
    end_line: int
    records: list[dict[str, Any]]
    raw_bytes: int


@dataclass
class Result:
    batch: Batch
    accepted: list[dict[str, Any]]
    rejected: int


class Counters:
    def __init__(self) -> None:
        self.processed = self.accepted = self.rejected = self.batches = self.source_bytes = 0
        self.digest = "00" * 32
        self.last_offset = self.last_line = 0


def read_batches(path: Path, start_offset: int, start_line: int, start_seq: int, cfg: GovernanceConfig,
                 stats: dict[str, Any], stop: threading.Event) -> Iterator[Batch]:
    with path.open("rb") as fh:
        fh.seek(start_offset); offset = start_offset; line_no = start_line; seq = start_seq; records=[]; raw=0; first_offset=offset; first_line=line_no + 1
        while not stop.is_set():
            line = fh.readline()
            if not line: break
            line_no += 1; end = fh.tell(); size = len(line)
            if size > cfg.max_line_bytes: raise BenchmarkError(f"line {line_no} exceeds max_line_bytes")
            if size > cfg.batch_bytes: raise BenchmarkError(f"line {line_no} exceeds batch_bytes; cannot satisfy bounded batch invariant")
            try: rec = json.loads(line)
            except json.JSONDecodeError as exc: raise BenchmarkError(f"invalid JSON at line {line_no}: {exc}") from exc
            if not isinstance(rec, dict): raise BenchmarkError(f"line {line_no} is not a JSON object")
            if records and (len(records) >= cfg.batch_rows or raw + size > cfg.batch_bytes):
                yield Batch(seq, first_offset, offset, first_line, line_no - 1, records, raw)
                seq += 1; records=[]; raw=0; first_offset=offset; first_line=line_no
            records.append(rec); raw += size; offset=end
        if records: yield Batch(seq, first_offset, offset, first_line, line_no, records, raw)


def disk_safe(path: Path, cfg: GovernanceConfig) -> bool:
    usage = shutil.disk_usage(path)
    return usage.free - cfg.disk_safety_reserve >= cfg.min_free_bytes and usage.free * 100 >= usage.total * cfg.min_free_percent


def require_arrow() -> None:
    if pa is None: raise BenchmarkError("pyarrow is required for Parquet operations; install requirements.txt")


def shard_path(out: Path, seq: int) -> Path: return out / f"shard-{seq:012d}.parquet"


def write_shard(out: Path, result: Result, cfg: GovernanceConfig, digest: str) -> Path:
    require_arrow(); b=result.batch; final=shard_path(out, b.seq)
    metadata = {"provenance_version": PROVENANCE_VERSION, "batch_seq": str(b.seq), "source_start_offset": str(b.start_offset), "source_end_offset": str(b.end_offset), "source_start_line": str(b.start_line), "source_end_line": str(b.end_line), "record_count": str(len(result.accepted)), "processed_count": str(len(b.records)), "logical_sha256": digest, "config_fingerprint": cfg.fingerprint(), "active": "true"}
    if not result.accepted:
        return final
    if final.exists():
        pf=pq.ParquetFile(final); old={k.decode():v.decode() for k,v in (pf.schema_arrow.metadata or {}).items()}
        if old.get("batch_seq") != str(b.seq) or old.get("source_end_offset") != str(b.end_offset): raise BenchmarkError(f"conflicting durable shard {final}")
        return final
    table=pa.Table.from_pylist(result.accepted) if result.accepted else pa.table({"id": pa.array([], type=pa.int64()), "accepted": pa.array([], type=pa.bool_()), "transform_version": pa.array([], type=pa.int64())})
    md=dict(table.schema.metadata or {}); md.update({k.encode():v.encode() for k,v in metadata.items()}); table=table.replace_schema_metadata(md)
    fd,tmp=tempfile.mkstemp(prefix=f".{final.name}.", dir=out); os.close(fd)
    try:
        pq.write_table(table,tmp,compression="zstd"); pq.ParquetFile(tmp); os.replace(tmp,final)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)
    return final


def checkpoint_payload(cfg: GovernanceConfig, ident: dict[str, Any], c: Counters, complete: bool=False) -> dict[str, Any]:
    return {"schema_version": CHECKPOINT_VERSION, "input": ident, "last_durable_offset": c.last_offset, "last_durable_line": c.last_line, "next_batch_seq": c.batches, "total_processed_records": c.processed, "total_accepted_records": c.accepted, "total_rejected_records": c.rejected, "processed_batches": c.batches, "deterministic_digest": c.digest, "latest_durable_shard": c.batches - 1, "configuration_fingerprint": cfg.fingerprint(), "completion_state": "complete" if complete else "paused"}


def load_checkpoint(path: Path, cfg: GovernanceConfig, ident: dict[str, Any]) -> Optional[dict[str, Any]]:
    if not path.exists(): return None
    try: cp=json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc: raise BenchmarkError(f"invalid checkpoint: {exc}") from exc
    if cp.get("schema_version") != CHECKPOINT_VERSION or cp.get("input") != ident: raise IncompatibleResume("input identity/checkpoint schema mismatch")
    if cp.get("configuration_fingerprint") != cfg.fingerprint(): raise IncompatibleResume("configuration fingerprint mismatch")
    return cp


def recover_journal(out: Path) -> None:
    journal=out/"compaction.journal.json"
    if not journal.exists(): return
    j=json.loads(journal.read_text()); dest=out/j["destination"]; sources=[out/x for x in j["sources"]]
    if j["state"] == "verified" and dest.exists():
        for s in sources:
            if s.exists(): s.unlink()
        journal.unlink(missing_ok=True)
    elif j["state"] in ("prepared", "writing"):
        (out/j["destination_tmp"]).unlink(missing_ok=True); journal.unlink(missing_ok=True)


def run_benchmark(cfg: GovernanceConfig, *, interrupt_after: int | None = None, force_pause: bool=False) -> dict[str, Any]:
    cfg.validate(); require_arrow(); inp=Path(cfg.input_path); out=Path(cfg.output_dir); out.mkdir(parents=True, exist_ok=True); recover_journal(out)
    ident=file_identity(inp); cp_path=out/"checkpoint.json"; cp=load_checkpoint(cp_path,cfg,ident)
    if cp and cp.get("completion_state") == "complete" and not force_pause: return json.loads((out/"metrics.json").read_text()) if (out/"metrics.json").exists() else cp
    c=Counters(); start_offset=start_line=start_seq=0
    if cp:
        start_offset=cp["last_durable_offset"]; start_line=cp["last_durable_line"]; start_seq=cp["next_batch_seq"]; c.processed=cp["total_processed_records"]; c.accepted=cp["total_accepted_records"]; c.rejected=cp["total_rejected_records"]; c.batches=cp["processed_batches"]; c.digest=cp["deterministic_digest"]; c.last_offset=start_offset; c.last_line=start_line
    started=time.perf_counter(); cpu0=time.process_time(); stop=threading.Event(); q: queue.Queue[Batch|None]=queue.Queue(cfg.queue_capacity); rq: queue.Queue[Result|None]=queue.Queue(cfg.result_queue_capacity); errors: list[BaseException]=[]; op={"producer_wait_seconds":0.0,"backpressure_events":0,"queue_high_water":0,"governed_pause":False}
    def producer() -> None:
        try:
            for b in read_batches(inp,start_offset,start_line,start_seq,cfg,op,stop):
                while not disk_safe(out,cfg):
                    op["governed_pause"]=True; time.sleep(0.1)
                    if force_pause: stop.set(); break
                if stop.is_set(): break
                t=time.perf_counter(); q.put(b); waited=time.perf_counter()-t; op["producer_wait_seconds"]+=waited
                if waited > .001: op["backpressure_events"]+=1
                op["queue_high_water"]=max(op["queue_high_water"],q.qsize())
        except BaseException as exc: errors.append(exc); stop.set()
        finally:
            for _ in range(cfg.workers):
                while True:
                    try: q.put(None, timeout=.1); break
                    except queue.Full:
                        if stop.is_set(): return
    def worker() -> None:
        try:
            while True:
                b=q.get()
                if b is None: rq.put(None); return
                acc=[]; rej=0
                for rec in b.records:
                    ok, val=transform(rec,cfg.filter_threshold)
                    if ok: acc.append(val)
                    else: rej+=1
                result=Result(b,acc,rej)
                while True:
                    try: rq.put(result, timeout=.1); break
                    except queue.Full:
                        if stop.is_set(): return
                q.task_done()
        except BaseException as exc: errors.append(exc); stop.set(); rq.put(None)
    pt=threading.Thread(target=producer, daemon=True); workers=[threading.Thread(target=worker, daemon=True) for _ in range(cfg.workers)]; pt.start(); [t.start() for t in workers]; pending={}; ended=0; next_seq=start_seq; paused=False
    try:
        while ended < cfg.workers:
            item=rq.get()
            if item is None: ended+=1; continue
            pending[item.batch.seq]=item
            while next_seq in pending:
                res=pending.pop(next_seq); c.processed += len(res.batch.records); c.accepted += len(res.accepted); c.rejected += res.rejected; c.batches += 1
                for rec in res.accepted: c.digest=rolling_digest(c.digest,rec)
                write_shard(out,res,cfg,c.digest); c.last_offset=res.batch.end_offset; c.last_line=res.batch.end_line; next_seq+=1
                atomic_json(cp_path,checkpoint_payload(cfg,ident,c))
                if interrupt_after is not None and c.processed >= interrupt_after: stop.set(); paused=True; raise KeyboardInterrupt
                if force_pause and op.get("governed_pause"): paused=True; stop.set(); raise GovernancePause("disk watermark reached")
        if errors: raise errors[0]
        if op.get("governed_pause"):
            paused = True
    except (KeyboardInterrupt, GovernancePause):
        paused=True; stop.set()
    finally:
        stop.set(); pt.join(timeout=2); [t.join(timeout=2) for t in workers]
    if errors and not paused: raise errors[0]
    if paused:
        atomic_json(cp_path,checkpoint_payload(cfg,ident,c)); raise GovernancePause("execution paused; checkpoint is durable")
    atomic_json(cp_path,checkpoint_payload(cfg,ident,c,True)); metrics=make_metrics(c,op,started,cpu0,out,False); atomic_json(out/"metrics.json",metrics); return metrics


def make_metrics(c: Counters, op: dict[str,Any], started: float, cpu0: float, out: Path, compacted: bool) -> dict[str,Any]:
    elapsed=time.perf_counter()-started; usage=shutil.disk_usage(out); rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1024 if sys.platform != "darwin" else 1)
    return {"deterministic": {"processed_records":c.processed,"accepted_records":c.accepted,"rejected_records":c.rejected,"processed_batches":c.batches,"source_bytes":c.last_offset,"final_source_offset":c.last_offset,"deterministic_digest":c.digest},"operational":{"elapsed_seconds":elapsed,"cpu_seconds":time.process_time()-cpu0,"records_per_second":c.processed/max(elapsed,1e-9),"mib_per_second":c.last_offset/1024**2/max(elapsed,1e-9),"producer_wait_seconds":op.get("producer_wait_seconds",0),"backpressure_events":op.get("backpressure_events",0),"queue_high_water":op.get("queue_high_water",0),"peak_rss_bytes":rss,"parquet_shards":len(list(out.glob("shard-*.parquet"))),"compacted":compacted,"disk_free_bytes":usage.free}}


def active_files(out: Path) -> list[Path]:
    superseded=set()
    for p in out.glob("compact-*.parquet"):
        try:
            md=pq.ParquetFile(p).schema_arrow.metadata or {}; superseded.update(x for x in md.get(b"source_shards",b"").decode().split(",") if x)
        except Exception: continue
    return [p for p in out.glob("shard-*.parquet") if p.name not in superseded] + list(out.glob("compact-*.parquet"))


def compact_output(out: Path, target_rows: int) -> dict[str,Any]:
    require_arrow(); recover_journal(out); sources=sorted(out.glob("shard-*.parquet")); groups=[]; cur=[]; rows=0
    for p in sources:
        n=pq.ParquetFile(p).metadata.num_rows
        if cur and rows+n>target_rows: groups.append(cur); cur=[]; rows=0
        cur.append(p); rows+=n
    if cur: groups.append(cur)
    made=0
    for idx, group in enumerate(groups):
        dest=out/f"compact-{idx:06d}.parquet"; journal=out/"compaction.journal.json"; tmp=out/f".{dest.name}.tmp"
        if dest.exists(): continue
        atomic_json(journal,{"version":1,"sources":[p.name for p in group],"destination":dest.name,"destination_tmp":tmp.name,"state":"writing","verified":False})
        tables=[pq.read_table(p) for p in group]; table=pa.concat_tables(tables, promote_options="default") if len(tables)>1 else tables[0]; md=dict(table.schema.metadata or {}); md.update({b"compaction_version":b"1",b"source_shards":b",".join(p.name.encode() for p in group),b"record_count":str(table.num_rows).encode(),b"active":b"true"}); table=table.replace_schema_metadata(md); pq.write_table(table,tmp,compression="zstd"); pq.ParquetFile(tmp); os.replace(tmp,dest); atomic_json(journal,{"version":1,"sources":[p.name for p in group],"destination":dest.name,"destination_tmp":tmp.name,"state":"verified","verified":True});
        for p in group: p.unlink()
        journal.unlink(missing_ok=True); made+=1
    return {"groups":len(groups),"files_created":made,"rows":sum(pq.ParquetFile(p).metadata.num_rows for p in out.glob("compact-*.parquet"))}


def verify_output(out: Path, cfg: GovernanceConfig | None=None) -> dict[str,Any]:
    require_arrow(); cp=json.loads((out/"checkpoint.json").read_text()); files=active_files(out); seen=[]; rows=0; digest="00"*32
    for p in files:
        pf=pq.ParquetFile(p); md={k.decode():v.decode() for k,v in (pf.schema_arrow.metadata or {}).items()};
        if "record_count" not in md: raise BenchmarkError(f"missing metadata: {p}")
        rows+=pf.metadata.num_rows; seen.append((md.get("source_start_offset"),md.get("source_end_offset")))
        for batch in pf.iter_batches():
            for rec in batch.to_pylist(): digest=rolling_digest(digest,rec)
    if rows != cp["total_accepted_records"]: raise BenchmarkError(f"accepted row mismatch: {rows} != {cp['total_accepted_records']}")
    if files and digest != cp["deterministic_digest"]: raise BenchmarkError("logical digest mismatch")
    return {"valid":True,"active_files":len(files),"accepted_records":rows,"digest":digest,"checkpoint_state":cp["completion_state"]}


def generate(path: Path, records: int, payload_bytes: int, seed: int) -> None:
    if records<0 or payload_bytes<0: raise ValueError("records and payload-bytes must be non-negative")
    path.parent.mkdir(parents=True,exist_ok=True); alphabet="abcdefghijklmnopqrstuvwxyz0123456789"; fd,tmp=tempfile.mkstemp(prefix=f".{path.name}.",dir=path.parent)
    try:
        with os.fdopen(fd,"wb") as fh:
            for i in range(records):
                value=(i*1103515245 + seed*12345) % 1_000_000; payload=(alphabet[(i+seed)%len(alphabet)] * payload_bytes)
                rec={"id":i,"synthetic_id":f"synthetic-{seed:08x}-{i:012d}","value":value,"category":("accepted" if value>=500_000 else "rejected"),"payload":payload}
                fh.write(canonical(rec))
            fh.flush(); os.fsync(fh.fileno())
        os.replace(tmp,path)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)


def build_parser() -> argparse.ArgumentParser:
    p=argparse.ArgumentParser(description=__doc__); sp=p.add_subparsers(dest="command",required=True)
    g=sp.add_parser("generate"); g.add_argument("--output",type=Path,required=True); g.add_argument("--records",type=int,required=True); g.add_argument("--payload-bytes",type=int,default=256); g.add_argument("--seed",type=int,default=1337)
    def add_run(x):
        x.add_argument("--input",dest="input_path",required=True); x.add_argument("--output-dir",required=True); x.add_argument("--workers",type=int,default=2); x.add_argument("--queue-capacity",type=int,default=2); x.add_argument("--result-queue-capacity",type=int,default=2); x.add_argument("--batch-rows",type=int,default=1000); x.add_argument("--batch-bytes",type=parse_bytes,default=16*1024**2); x.add_argument("--max-line-bytes",type=parse_bytes,default=4*1024**2); x.add_argument("--shard-rows",type=int,default=100000); x.add_argument("--shard-bytes",type=parse_bytes,default=128*1024**2); x.add_argument("--checkpoint-every-records",type=int,default=50000); x.add_argument("--min-free-bytes",type=parse_bytes,default=0); x.add_argument("--min-free-percent",type=float,default=0); x.add_argument("--disk-safety-reserve",type=parse_bytes,default=0); x.add_argument("--compact-target-rows",type=int,default=1000000); x.add_argument("--filter-threshold",type=int,default=500000); x.add_argument("--compact",action="store_true")
    r=sp.add_parser("run"); add_run(r); v=sp.add_parser("verify"); v.add_argument("--output-dir",required=True); c=sp.add_parser("compact"); c.add_argument("--output-dir",required=True); c.add_argument("--target-rows",type=int,required=True); sp.add_parser("selftest"); return p


def main(argv: Optional[list[str]]=None) -> int:
    args=build_parser().parse_args(argv); logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s")
    try:
        if args.command=="generate": generate(args.output,args.records,args.payload_bytes,args.seed); print(f"generated {args.records} records at {args.output}"); return 0
        if args.command=="verify": print(json.dumps(verify_output(Path(args.output_dir)),indent=2)); return 0
        if args.command=="compact": print(json.dumps(compact_output(Path(args.output_dir),args.target_rows),indent=2)); return 0
        if args.command=="selftest": return selftest()
        cfg=GovernanceConfig(**{k:v for k,v in vars(args).items() if k in {f.name for f in GovernanceConfig.__dataclass_fields__.values()}}); result=run_benchmark(cfg); 
        if args.compact:
            result["compaction"]=compact_output(Path(cfg.output_dir),cfg.compact_target_rows)
            result["operational"]["compacted"] = True
            result["operational"]["parquet_shards"] = len(active_files(Path(cfg.output_dir)))
            atomic_json(Path(cfg.output_dir)/"metrics.json",result)
        print(json.dumps(result,indent=2)); return 0
    except GovernancePause as exc: print(str(exc),file=sys.stderr); return 75
    except (BenchmarkError, ValueError, OSError) as exc: print(f"error: {exc}",file=sys.stderr); return 2


def selftest() -> int:
    if pa is None: print("selftest blocked: pyarrow is unavailable",file=sys.stderr); return 2
    import unittest
    suite=unittest.defaultTestLoader.loadTestsFromModule(__import__("test_acl_heavy_data_benchmark")); return 0 if unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful() else 1


if __name__ == "__main__": sys.exit(main())
