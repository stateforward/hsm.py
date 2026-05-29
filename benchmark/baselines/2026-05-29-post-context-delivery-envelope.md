`PYTHONPATH=. .venv/bin/python bench/throughput_benchmark.py --rounds 9 --warmup 2000`

```text
python=3.13.0|cpus=16
ping_pong|ops=40000|median_ns_per_op=7164.4|p95_ns_per_op=7225.3|ops_per_sec=139579|startup_us=55.6
hierarchical|ops=40000|median_ns_per_op=7659.7|p95_ns_per_op=7766.1|ops_per_sec=130554|startup_us=53.3
guarded_self|ops=40000|median_ns_per_op=37510.5|p95_ns_per_op=39776.8|ops_per_sec=26659|startup_us=39.2
traffic_ring|ops=60000|median_ns_per_op=7308.9|p95_ns_per_op=8339.7|ops_per_sec=136819|startup_us=56.4
```
