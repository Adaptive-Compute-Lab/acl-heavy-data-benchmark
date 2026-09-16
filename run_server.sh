#!/usr/bin/env bash
set -euo pipefail
INPUT=${1:?input JSONL path required}; OUTPUT=${2:?output directory required}; shift 2
exec python3 "$(dirname "$0")/acl_heavy_data_benchmark.py" run --input "$INPUT" --output-dir "$OUTPUT" --workers 2 --queue-capacity 2 --result-queue-capacity 2 --batch-rows 1000 --batch-bytes 16MiB --max-line-bytes 4MiB --shard-rows 100000 --shard-bytes 128MiB --checkpoint-every-records 50000 --min-free-percent 5 --compact-target-rows 1000000 "$@"
