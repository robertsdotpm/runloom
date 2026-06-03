"""Phase C v2: yield-in-hub stress + bench.

Before Phase C v2, M:N hubs ran fire-and-forget gs: any goroutine that
called sched_yield while running on a hub would have its yield routed
to the *global* scheduler (effectively unbinding it from the hub) and
then never get resumed by anyone.

Phase C v2 adds:
  - thread-local current_hub / current_g pointers set when hub_main
    runs a g.
  - runloom_mn_yield_current() that pushes the current g onto its hub's
    local FIFO (separate from the steal-able fresh-g deque) and asm-
    yields back to hub_main.
  - runloom_sched_yield consults runloom_mn_yield_current first.

This test spawns N gs across H hubs, each yielding Y times before a
counter increment, and checks the final counter is N * Y.  Demonstrates
the cooperative scheduler now runs across multiple OS threads in
free-threaded Python 3.13t.
"""
import sys
import time
import threading

sys.path.insert(0, "src")
import runloom_c


def make_yielder(yields, counter, lock):
    def worker():
        for _ in range(yields):
            runloom_c.sched_yield()
        with lock:
            counter[0] += 1
    return worker


def run_case(hubs, n_gs, yields_per_g, label):
    runloom_c.mn_init(hubs)
    counter = [0]
    lock = threading.Lock()
    t0 = time.perf_counter()
    for _ in range(n_gs):
        runloom_c.mn_go(make_yielder(yields_per_g, counter, lock))
    runloom_c.mn_run()
    runloom_c.mn_fini()
    dt = time.perf_counter() - t0
    total_yields = n_gs * yields_per_g
    expected = n_gs
    status = "OK" if counter[0] == expected else "FAIL ({0} vs {1})".format(
        counter[0], expected)
    print("  {:<28} hubs={} N={} Y={}  total_yields={}  "
          "{:>6.0f} ms  ({:>5.2f} M y/s)  {}".format(
              label, hubs, n_gs, yields_per_g, total_yields,
              dt * 1000.0, total_yields / dt / 1e6, status))


def main():
    print("Phase C v2: yield in M:N hub")
    print("---------------------------------")
    run_case(1, 100,  100, "single hub, light")
    run_case(1, 1000, 100, "single hub, heavy")
    run_case(2, 200,  100, "2 hubs")
    run_case(4, 200,  100, "4 hubs")
    run_case(8, 400,  100, "8 hubs")
    run_case(8, 1000, 50,  "8 hubs many gs")


if __name__ == "__main__":
    main()
