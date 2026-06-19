"""runloom.tools.mn_stress -- seeded randomized stress/fuzz driver for the
M:N (multi-hub, work-stealing) scheduler.

This targets the path the rest of the Python test-suite never touches:
goroutines spread across N OS-thread hubs on free-threaded CPython,
exchanging values through channels and select() while the work-stealing
deque migrates ready goroutines between hubs.  That is exactly where
lost-wakes, double-resumes, and cross-hub channel-handoff races would
live -- the bugs the formal models in verify/ rule out in the abstract,
checked here against the real extension under real parallelism.

Every run is a seeded, reproducible "token conservation" experiment:

    nprod producers push a known multiset of tokens into a shared pool of
    channels; a coordinator closes the channels once producers finish;
    ncons consumers drain them (some via recv-range, some via select) and
    report partial sums.  INVARIANT: every token is received exactly once
    -- total count and checksum must match what was sent.

Between iterations runloom_c._self_check() must report zero violations.
The whole thing runs under tools.watchdog.run_guarded, so a hang is
caught, fully dumped, and tagged with the reproducing --seed instead of
wedging forever.

CLI:
    python tools/mn_stress.py [--iters N] [--hubs H] [--seed S]
                              [--goroutines G] [--timeout SEC] [--verbose]

Exit 0 = clean.  Non-zero = a mismatch, self-check violation, or hang
(the offending seed is printed for a deterministic repro).
"""
import argparse
import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import runloom_c
from tools.watchdog import run_guarded


def _one_experiment(rng, stable=False):
    """Run a single seeded token-conservation experiment on the M:N
    scheduler.  Returns (sent_count, sent_sum, recv_count, recv_sum).

    stable=True restricts the workload to the patterns known to be solid
    (range-recv + close, no select-across-channels) so the run is a clean
    regression gate.  stable=False (default) is the full fuzzer and will
    exercise the contended select() path -- see tools/README.md finding A."""
    nhubs = rng.choice([2, 3, 4])
    nprod = rng.randint(2, 8)
    ncons = rng.randint(2, 8)
    nchan = rng.randint(1, 4)
    if stable:
        # Every channel must have at least one range-consumer or its tokens
        # are never received; cap nchan at ncons and cover round-robin.
        nchan = min(nchan, ncons)
    per_prod = rng.randint(5, 40)

    # Pool of channels with assorted buffering (0 = unbuffered handoff).
    chans = [runloom_c.Chan(rng.choice([0, 0, 1, 2, 8])) for _ in range(nchan)]
    prod_done = runloom_c.Chan(nprod)          # producers report completion
    results = runloom_c.Chan(ncons)            # consumers report (count, sum)

    sent_count = nprod * per_prod
    # token value = prod_id * 1000 + seq ; checksum is deterministic
    sent_sum = 0
    for pid in range(nprod):
        for seq in range(per_prod):
            sent_sum += pid * 1000 + seq

    def producer(pid):
        def run():
            for seq in range(per_prod):
                token = pid * 1000 + seq
                ch = chans[(pid + seq) % nchan]
                ch.send(token)
                if (seq & 7) == 0:
                    runloom_c.sched_yield_classic()
            prod_done.send(pid)
        return run

    def closer():
        # Wait for every producer, then close all channels so consumers
        # ranging over them terminate (exercises close-wakes-receivers).
        for _ in range(nprod):
            prod_done.recv()
        for ch in chans:
            ch.close()

    def consumer_range(cid):
        # Drain one channel by iterating until it's closed.
        def run():
            ch = chans[cid % nchan]
            count = 0
            total = 0
            for v in ch:
                count += 1
                total += v
            results.send((count, total))
        return run

    def consumer_select(cid):
        # Drain via select across ALL channels until all are closed.
        def run():
            count = 0
            total = 0
            closed = [False] * nchan
            while not all(closed):
                cases = [("recv", chans[i]) for i in range(nchan) if not closed[i]]
                if not cases:
                    break
                idx, (val, ok) = runloom_c.select(cases)
                # Map idx back to the channel it fired on.
                live = [i for i in range(nchan) if not closed[i]]
                ci = live[idx]
                if ok:
                    count += 1
                    total += val
                else:
                    closed[ci] = True
            results.send((count, total))
        return run

    runloom_c.mn_init(nhubs)
    # Spread consumers between the two styles (stable => range only).
    for cid in range(ncons):
        if stable or (cid % 2 == 0 and nchan > 0):
            runloom_c.mn_fiber(consumer_range(cid))
        else:
            runloom_c.mn_fiber(consumer_select(cid))
    for pid in range(nprod):
        runloom_c.mn_fiber(producer(pid))
    runloom_c.mn_fiber(closer)
    runloom_c.mn_run()

    # Collect consumer reports (buffered; drained from the main thread).
    recv_count = 0
    recv_sum = 0
    drained = 0
    while drained < ncons:
        got = results.try_recv()
        if got is None:
            break
        (c, s), ok = got
        recv_count += c
        recv_sum += s
        drained += 1
    runloom_c.mn_fini()
    return sent_count, sent_sum, recv_count, recv_sum, dict(
        nhubs=nhubs, nprod=nprod, ncons=ncons, nchan=nchan, per_prod=per_prod)


def run(iters=200, hubs=None, seed=None, timeout=20.0, verbose=False, stable=False):
    if seed is None:
        seed = random.randrange(1 << 30)
    print("mn_stress: iters={0} seed={1} timeout={2}s mode={3}".format(
        iters, seed, timeout, "stable" if stable else "fuzz(+select)"))
    for i in range(iters):
        # Per-iteration deterministic sub-seed so any failing iteration is
        # reproducible in isolation.
        iseed = seed + i
        rng = random.Random(iseed)

        def work():
            return _one_experiment(rng, stable=stable)

        try:
            sc, ssum, rc, rsum, params = run_guarded(
                work, seconds=timeout,
                label="mn_stress iter={0} seed={1}".format(i, iseed))
        except TimeoutError as e:
            print("\nHANG at iter={0} seed={1}\n  repro: "
                  "python tools/mn_stress.py --iters 1 --seed {1}\n  {2}"
                  .format(i, iseed, e))
            return 2

        if (sc, ssum) != (rc, rsum):
            print("\nMISMATCH at iter={0} seed={1} params={2}\n"
                  "  sent  count={3} sum={4}\n  recv  count={5} sum={6}\n"
                  "  repro: python tools/mn_stress.py --iters 1 --seed {1}"
                  .format(i, iseed, params, sc, ssum, rc, rsum))
            return 1

        v = runloom_c._self_check(0)
        if v != 0:
            print("\nSELF_CHECK VIOLATION at iter={0} seed={1}: {2} violations"
                  .format(i, iseed, v))
            runloom_c._self_check(1)
            return 1

        if verbose or (i % 25 == 0):
            print("  iter {0:4d} seed={1} ok  ({2} tokens, {3} hubs)"
                  .format(i, iseed, sc, params["nhubs"]))

    print("CLEAN: {0} iterations, no mismatch / hang / self-check violation".format(iters))
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--iters", type=int, default=200)
    p.add_argument("--hubs", type=int, default=None,
                   help="(reserved; hub count is randomized per iteration)")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--goroutines", type=int, default=None,
                   help="(reserved; goroutine count is randomized per iteration)")
    p.add_argument("--timeout", type=float, default=20.0)
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--stable", action="store_true",
                   help="restrict to known-good patterns (no contended "
                        "select-across-channels); use as a clean gate")
    args = p.parse_args(argv)
    return run(iters=args.iters, hubs=args.hubs, seed=args.seed,
               timeout=args.timeout, verbose=args.verbose, stable=args.stable)


if __name__ == "__main__":
    sys.exit(main())
