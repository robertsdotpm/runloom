"""M:N sleep-in-hub smoke test.

Each goroutine sleeps for a per-g duration on whichever hub it runs on.
Verifies that:
  - runloom.sched_sleep inside a hub goroutine pushes onto the HUB's
    per-thread sleep heap (not the global single-thread heap, which
    no hub processes).
  - hub_main pops due sleepers off its local heap and runs them.
  - mn_run waits for sleeping gs to finish (pending count stays
    positive until the g actually completes).

Run with `~/.pyenv/versions/3.13.13t/bin/python3.13t bench/bench_mn_sleep.py`.
"""
import sys
import time
import threading

sys.path.insert(0, "src")
import runloom_c


def sleeper(secs, label, results, lock):
    t0 = time.perf_counter()
    runloom_c.sched_sleep(secs)
    dt = time.perf_counter() - t0
    with lock:
        results.append((label, secs, dt))


def main():
    print("M:N sleep-in-hub smoke (3.13t)")
    print()
    N_HUBS = 4
    N = 32
    BASE = 0.05
    STEP = 0.005

    results = []
    lock = threading.Lock()

    runloom_c.mn_init(N_HUBS)
    t0 = time.perf_counter()
    for i in range(N):
        secs = BASE + STEP * i
        runloom_c.mn_go(lambda secs=secs, i=i: sleeper(secs, i, results, lock))
    runloom_c.mn_run()
    runloom_c.mn_fini()
    wall = time.perf_counter() - t0

    results.sort()
    max_diff = max(abs(actual - target) for _, target, actual in results)
    longest_target = BASE + STEP * (N - 1)
    print(f"  hubs={N_HUBS}, sleepers={N}, "
          f"target range [{BASE:.3f}s, {longest_target:.3f}s]")
    print(f"  wall:               {wall*1000:>7.1f} ms")
    print(f"  longest target:     {longest_target*1000:>7.1f} ms")
    print(f"  overhead vs longest: {(wall - longest_target)*1000:>7.1f} ms")
    print(f"  max wake jitter:    {max_diff*1000:>7.2f} ms")
    print(f"  completed:          {len(results)} / {N}")
    print()
    print("  first 5 sleepers (label, target, actual, diff):")
    for label, target, actual in results[:5]:
        print(f"    g{label:>2}: target={target*1000:.1f}ms "
              f"actual={actual*1000:.1f}ms diff={(actual-target)*1000:+.2f}ms")


if __name__ == "__main__":
    main()
