#!/usr/bin/env python3
"""quiescence_check.py -- quiescence-predicate oracle (Go synctest / Tokio pause /
Kotlin runTest, for the runtime).

"Did everything settle, or did we lose a wake?" is pygo's recurring bug
signature. runloom_c._quiescent() makes it a DECIDABLE runtime query: is the
runtime SETTLED -- every live goroutine durably blocked (parked on a
timer/channel/fd), nothing runnable -- so no progress happens until a timer fires
or external I/O arrives?  {quiescent, live, parked, inflight}, sampled from an
observer thread (the caller's own RUNNING state would otherwise keep inflight>0).

This oracle drives a cooperative-park workload and asserts the runtime actually
REACHES quiescence (settles) -- a runtime that never settles (a lost wake keeping
a goroutine spuriously runnable, or a hot spin) would never show inflight==0 with
live>0 -- and then fully drains.

  PYTHONPATH=src python tools/verify/quiescence_check.py [--workers W] [--nap S]
"""
import argparse
import sys
import threading
import time


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=40)
    ap.add_argument("--hubs", type=int, default=4)
    ap.add_argument("--nap", type=float, default=0.15)
    args = ap.parse_args()

    import runloom
    import runloom_c as rc
    from runloom import monkey
    monkey.patch()   # cooperative time.sleep -> PARKED_SLEEP (a timer park, not a hub block)

    obs = {"settle": None, "max_parked": 0, "samples": 0}

    def monitor():
        for _ in range(2000):
            q = rc._quiescent()
            obs["samples"] += 1
            obs["max_parked"] = max(obs["max_parked"], q["parked"])
            if q["quiescent"] and q["live"] > 0:
                obs["settle"] = dict(q)   # a real settle: every live g parked
            time.sleep(0.001)

    done = {"n": 0}
    lock = threading.Lock()

    def worker():
        import time as t
        t.sleep(args.nap)                 # cooperative timer park
        with lock:
            done["n"] += 1

    def main_fn():
        for _ in range(args.workers):
            rc.mn_fiber(worker)

    mt = threading.Thread(target=monitor, daemon=True)
    mt.start()
    runloom.run(args.hubs, main_fn)

    settle = obs["settle"]
    print("quiescence_check: samples=%d max_parked=%d workers_done=%d settle=%r"
          % (obs["samples"], obs["max_parked"], done["n"], settle))
    if done["n"] != args.workers:
        print("quiescence_check: FAIL -- only %d/%d workers drained"
              % (done["n"], args.workers))
        return 1
    if settle is None:
        print("quiescence_check: FAIL -- runtime NEVER observed settled (every "
              "sample had runnable/in-flight work: a lost wake or a hot spin?)")
        return 1
    q = rc._quiescent()
    if not q["quiescent"] or q["live"] != 0:
        print("quiescence_check: FAIL -- post-run not fully settled: %r" % (q,))
        return 1
    print("quiescence_check: PASS -- runtime settled to all-parked mid-run "
          "(quiescent with %d parked) and drained to empty" % settle["parked"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
