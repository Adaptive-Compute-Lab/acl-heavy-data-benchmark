import json, tempfile, unittest
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
