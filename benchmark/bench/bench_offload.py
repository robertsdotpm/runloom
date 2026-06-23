"""Offload (blocking-pool) throughput bench + A/B knobs.

Measures runloom.blocking() round-trips/sec on the M:N scheduler.  Env knobs:
  BENCH_N      total offloads (default 100000)
  BENCH_HUBS   hub threads (default 8)
  BENCH_FIB    concurrent fibers; each does BENCH_N//BENCH_FIB offloads (default 2000)
  BENCH_DIRECT =1 calls the fn inline (no offload) -> pure-scheduler baseline
  RUNLOOM_BLOCKPOOL_SHARDS   submit shards (1 = legacy single queue; default = #hubs)
  RUNLOOM_BLOCKPOOL_WORKERS  total pool workers (default = shards * 3)

Run:  PYTHON_GIL=0 PYTHONPATH=src python3 bench/bench_offload.py
A/B the two scaling fixes:
  RUNLOOM_BLOCKPOOL_SHARDS=1 ...   # legacy submit convoy
  (default)                        # sharded
Measured 8-hub, free-threaded 3.13t: legacy per-job-tstate path ~20k/s;
persistent-tstate + sharded ~670k/s (~33x); pure-scheduler baseline ~1M/s.
"""
import runloom, time, os
N    = int(os.environ.get("BENCH_N", "100000"))
HUBS = int(os.environ.get("BENCH_HUBS", "8"))
FIB  = int(os.environ.get("BENCH_FIB", "2000"))
DIRECT = os.environ.get("BENCH_DIRECT", "0") == "1"
def work(): return None
def main():
    each = N // FIB
    if DIRECT:
        def loop():
            for _ in range(each): work()              # no offload: pure scheduler
    else:
        def loop():
            for _ in range(each): runloom.blocking(work)
    for _ in range(FIB): runloom.fiber(loop)
t0 = time.perf_counter(); runloom.run(HUBS, main); dt = time.perf_counter() - t0
print("N=%d HUBS=%d %s shards=%s workers=%s : %.3fs  %s ops/s"
      % (N, HUBS, "DIRECT" if DIRECT else "OFFLOAD",
         os.environ.get("RUNLOOM_BLOCKPOOL_SHARDS","auto"),
         os.environ.get("RUNLOOM_BLOCKPOOL_WORKERS","auto"),
         dt, format(int(N/dt), ",")))
