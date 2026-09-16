# ACL-Core Heavy-Data Benchmark

A production-oriented, deterministic benchmark for authorized synthetic/public JSONL data. It exercises bounded ingestion, parallel transformation, ordered durable Parquet output, resumable checkpoints, disk governance, compaction, and verification. It does not generate or search private keys, credentials, wallet secrets, or other sensitive material.

## Requirements and installation

Python 3.11+ and `pyarrow` are required for `run`, `verify`, and `compact`:

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
```

The reader uses binary incremental reads. Memory is bounded by queue capacity, bounded batches, worker results, and one bounded Parquet staging batch—not by total records.

## Usage

```bash
python acl_heavy_data_benchmark.py generate --output synthetic.jsonl --records 100000 --payload-bytes 256 --seed 1337
python acl_heavy_data_benchmark.py run --input synthetic.jsonl --output-dir out --workers 4 --queue-capacity 2 --batch-rows 5000 --batch-bytes 16MiB --max-line-bytes 4MiB --shard-rows 100000 --shard-bytes 128MiB --compact --compact-target-rows 1000000
python acl_heavy_data_benchmark.py verify --output-dir out
python acl_heavy_data_benchmark.py compact --output-dir out --target-rows 1000000
```

`run_server.sh INPUT OUTPUT_DIR [extra arguments]` supplies conservative defaults. Generated records are byte-for-byte reproducible for the same count, payload size, and seed. Filtering accepts deterministic `value >= filter_threshold` records.

## Durability and recovery

Each bounded batch has a monotonically increasing sequence and exact source byte/line range. Workers may finish out of order, but commits are ordered. A shard is written to a same-directory temporary path, closed, reopened for validation, and atomically renamed. The checkpoint is written using flush, `fsync`, `os.replace`, and parent-directory fsync where supported. The invariant is `checkpoint_offset <= last fully durable output boundary`.

Resume validates the full input identity (resolved path, size, mtime, SHA-256) and configuration fingerprint, seeks directly to the saved byte offset, and restores sequence/counters/digest. Existing shards are reconciled by sequence and range. Completed runs are idempotent.

Before admission, disk free bytes and percentage are checked after subtracting the configured safety reserve. Low disk causes a durable governed pause (exit code 75); already admitted work drains. A later run resumes after pressure clears. Filesystem atomic rename semantics are assumed for a single filesystem.

## Parquet and compaction

Output shards contain provenance metadata: source range, batch sequence, counts, logical digest, schema/provenance version, and config fingerprint. Compaction writes a journal before writing, validates the destination, atomically installs it, then deletes source shards. The journal makes destination-durable/source-cleanup crashes recoverable. Verification treats compacted sources as superseded and rejects count/digest/topology inconsistencies.

## Metrics and tests

`metrics.json` is atomically written and separates deterministic counters/digest from timings, throughput, queue backpressure, shard count, and disk free space. Run:

```bash
python -m py_compile acl_heavy_data_benchmark.py test_acl_heavy_data_benchmark.py
python -m unittest -v
python acl_heavy_data_benchmark.py selftest
```

The self-test is the same real unittest path and covers deterministic generation, bounded batches, interruption/resume, idempotency, governed pause/resume, Parquet metadata, compaction, and verification. Typical layout is `checkpoint.json`, `metrics.json`, `shard-*.parquet`, and optionally `compact-*.parquet`; temporary files are same-directory and removed after successful transactions.

Tune batch and queue limits to available RAM and disk throughput. Larger batches reduce overhead but increase in-flight memory; shard limits are explicitly bounded by both rows and bytes.
