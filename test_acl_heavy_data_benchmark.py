import json, tempfile, unittest
from unittest import mock
from pathlib import Path
from acl_heavy_data_benchmark import *

class BenchmarkTests(unittest.TestCase):
    def test_byte_sizes_and_validation(self):
        self.assertEqual(parse_bytes("16MiB"), 16*1024**2); self.assertEqual(parse_bytes("2GB"), 2_000_000_000)
        with self.assertRaises(ValueError): GovernanceConfig("x","y",batch_rows=0).validate()
    def test_generator_is_byte_deterministic(self):
        with tempfile.TemporaryDirectory() as d:
            a,b=Path(d)/"a",Path(d)/"b"; generate(a,30,20,7); generate(b,30,20,7); self.assertEqual(a.read_bytes(),b.read_bytes())
    def test_bounded_batches_and_oversized_line(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/"x"; generate(p,10,20,1); cfg=GovernanceConfig(str(p),d,batch_rows=3,batch_bytes=200,max_line_bytes=1000,shard_rows=3,shard_bytes=1000); bs=list(read_batches(p,0,0,0,cfg,{},__import__('threading').Event())); self.assertTrue(all(len(b.records)<=3 and b.raw_bytes<=200 for b in bs))
            p.write_bytes(b'{"x":"'+b'a'*100+b'"}\n');
            with self.assertRaises(BenchmarkError): list(read_batches(p,0,0,0,replace(cfg,max_line_bytes=20),{},__import__('threading').Event()))

    @unittest.skipIf(pa is None,"pyarrow unavailable")
    def test_staging_rows_and_bytes_are_hard_bounds(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); inp=root/"i"; generate(inp,4,10,4); (root/"o").mkdir()
            cfg=GovernanceConfig(str(inp),str(root/"o"),batch_rows=2,batch_bytes=4096,shard_rows=2,shard_bytes=200)
            batches=list(read_batches(inp,0,0,0,cfg,{},__import__('threading').Event()))
            records=[{"id":1,"value":900000,"payload":"x"}]
            with self.assertRaises(BenchmarkError): write_staged_shard(root/"o",records*3,batches[:1],cfg,"00"*32)
            with self.assertRaises(BenchmarkError): write_staged_shard(root/"o",[{"id":1,"value":900000,"payload":"x"*300}],batches[:1],replace(cfg,shard_rows=10),"00"*32)

    @unittest.skipIf(pa is None,"pyarrow unavailable")
    def test_multi_group_compaction_digest_is_order_invariant(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); inp=root/"i"; out=root/"o"; generate(inp,120,8,9)
            cfg=GovernanceConfig(str(inp),str(out),workers=3,queue_capacity=2,batch_rows=4,batch_bytes=4096,shard_rows=4,shard_bytes=4096,compact_target_rows=15)
            result=run_benchmark(cfg); before=result["deterministic"]["deterministic_digest"]; compact_output(out,15)
            compact_files=sorted(out.glob("compact-*.parquet")); self.assertGreater(len(compact_files),1)
            # Rename files in reverse lexical order; metadata remains the authority.
            for index,path in enumerate(reversed(compact_files)):
                path.rename(out/f"compact-reordered-{index:03d}.parquet")
            checked=verify_output(out); self.assertEqual(before,checked["digest"])

    @unittest.skipIf(pa is None,"pyarrow unavailable")
    def test_checkpoint_cadence_and_resume_provenance(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); inp=root/"i"; out=root/"o"; generate(inp,30,4,5)
            cfg=GovernanceConfig(str(inp),str(out),batch_rows=3,batch_bytes=2048,shard_rows=3,shard_bytes=2048,checkpoint_every_records=3,checkpoint_every_seconds=.000001)
            writes=[]; original=atomic_json
            def record(path,payload):
                if path.name=="checkpoint.json": writes.append(payload.copy())
                original(path,payload)
            with mock.patch("acl_heavy_data_benchmark.atomic_json",side_effect=record): result=run_benchmark(cfg)
            self.assertGreaterEqual(len(writes),10); self.assertEqual(writes[-1]["last_durable_offset"],inp.stat().st_size)
            self.assertEqual(json.loads((out/"checkpoint.json").read_text())["next_batch_seq"],result["deterministic"]["processed_batches"])

    @unittest.skipIf(pa is None,"pyarrow unavailable")
    def test_immutable_input_mismatch_and_global_sequence(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); inp=root/"i"; out=root/"o"; generate(inp,20,4,6)
            cfg=GovernanceConfig(str(inp),str(out),batch_rows=4,batch_bytes=2048,shard_rows=4,shard_bytes=2048)
            with self.assertRaises(GovernancePause): run_benchmark(cfg,interrupt_after=7)
            paused=json.loads((out/"checkpoint.json").read_text()); self.assertGreater(paused["next_batch_seq"],0)
            inp.write_bytes(inp.read_bytes()+b"\n")
            with self.assertRaises(IncompatibleResume): run_benchmark(cfg)

    @unittest.skipIf(pa is None,"pyarrow unavailable")
    def test_recovery_after_destination_rename(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); inp=root/"i"; out=root/"o"; generate(inp,40,5,7)
            cfg=GovernanceConfig(str(inp),str(out),batch_rows=4,batch_bytes=4096,shard_rows=4,shard_bytes=4096)
            run_benchmark(cfg); source=sorted(out.glob("shard-*.parquet"))[:3]; tables=[pq.read_table(p) for p in source]; table=pa.concat_tables(tables, promote_options="default"); dest=out/"compact-recovery.parquet"; tmp=out/".compact-recovery.tmp"; md=dict(table.schema.metadata or {}); md.update({b"source_shards":b",".join(p.name.encode() for p in source),b"record_count":str(table.num_rows).encode(),b"provenance_version":b"1",b"source_start_offset":b"0",b"source_end_offset":b"1",b"source_start_line":b"1",b"source_end_line":b"1",b"first_batch_seq":b"0",b"last_batch_seq":b"2",b"logical_sha256":digest_records(table.to_pylist()).encode()}); table=table.replace_schema_metadata(md); pq.write_table(table,tmp); os.replace(tmp,dest)
            atomic_json(out/"compaction.journal.json",{"version":1,"sources":[p.name for p in source],"destination":dest.name,"destination_tmp":tmp.name,"state":"writing","verified":False})
            recover_journal(out); self.assertFalse((out/"compaction.journal.json").exists()); self.assertTrue(dest.exists()); self.assertTrue(all(not p.exists() for p in source))

    @unittest.skipIf(pa is None,"pyarrow unavailable")
    def test_partial_active_overlap_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); inp=root/"i"; out=root/"o"; generate(inp,2,1,8); out.mkdir()
            cfg=GovernanceConfig(str(inp),str(out),batch_rows=1,batch_bytes=512,shard_rows=1,shard_bytes=512)
            b1=Batch(0,0,10,1,1,[{"id":0}],10); b2=Batch(1,5,15,2,2,[{"id":1}],10); r1={"id":0,"value":900000,"accepted":True,"transform_version":1}; r2={"id":1,"value":900000,"accepted":True,"transform_version":1}; write_staged_shard(out,[r1],[b1],cfg,digest_records([r1])); write_staged_shard(out,[r2],[b2],cfg,digest_records([r1,r2])); atomic_json(out/"checkpoint.json",{"total_accepted_records":2,"deterministic_digest":digest_records([r1,r2]),"completion_state":"complete"})
            with self.assertRaises(BenchmarkError): verify_output(out)
    @unittest.skipIf(pa is None,"pyarrow unavailable")
    def test_resume_idempotency_compaction_and_verify(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); inp=root/"input.jsonl"; generate(inp,80,10,3); base=root/"base"; resumed=root/"resumed"; common=dict(workers=2,queue_capacity=2,batch_rows=7,batch_bytes=4096,max_line_bytes=1000,shard_rows=7,shard_bytes=4096,filter_threshold=500000)
            cfg=GovernanceConfig(str(inp),str(base),**common); full=run_benchmark(cfg); cfg2=GovernanceConfig(str(inp),str(resumed),**common)
            with self.assertRaises(GovernancePause): run_benchmark(cfg2,interrupt_after=25)
            got=run_benchmark(cfg2); self.assertEqual(full["deterministic"],got["deterministic"]); self.assertEqual(run_benchmark(cfg2)["deterministic"],got["deterministic"])
            compact_output(resumed,20); self.assertTrue(verify_output(resumed)["valid"])
    @unittest.skipIf(pa is None,"pyarrow unavailable")
    def test_governed_pause_resume(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); inp=root/"i"; generate(inp,20,2,1); out=root/"o"; cfg=GovernanceConfig(str(inp),str(out),batch_rows=5,batch_bytes=1000,shard_rows=5,shard_bytes=1000,min_free_bytes=10**18)
            with self.assertRaises(GovernancePause): run_benchmark(cfg,force_pause=True)
            cfg=replace(cfg,min_free_bytes=0); self.assertTrue(run_benchmark(cfg)["deterministic"]["processed_records"]==20)

if __name__=="__main__": unittest.main()
