#!/usr/bin/env python3
"""result_oracle.py -- live per-goroutine RESULT oracle (RocksDB db_stress
expected-state / ScyllaDB Gemini reference oracle; QA-steal rank 15).

pygo's soak checks done==N + a resource slope, so a goroutine that RUNS but
computes the WRONG value -- a corrupted stack frame, cross-goroutine data bleed, a
mis-migrated tstate -- passes silently. This closes that: each goroutine computes
a deterministic checksum of its id (a stack-exercising pure function), returns it
into its own slot, and the harness verifies EVERY returned value against the same
function recomputed as an in-memory model. Any mismatch is silent corruption the
counters cannot see.

Strongest composed with the chaos + rare-path tools:
    RUNLOOM_FORCE_STACKGROW=1 python setup.py build_ext --inplace --force  # then
    PYTHONPATH=src python tools/verify/result_oracle.py --buggify --seed N

--teeth makes one goroutine return a deliberately-wrong value, to prove the oracle
catches a bad result (not just a missing one).
"""
import argparse
import os
import sys


def compute(i):
    """Deterministic, stack-exercising checksum of i.  Pure -> the model
    recomputes it identically.  Locals + a per-call buffer so a corrupted frame /
    cross-goroutine bleed changes the result."""
    acc = (i * 2654435761) & 0xFFFFFFFF
    buf = [((i + k) * 2246822519) & 0xFF for k in range(64)]
    for k in range(400):
        acc = (acc * 1103515245 + 12345 + buf[(acc + k) & 63]) & 0xFFFFFFFF
    return acc


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=0xBEEF)
    ap.add_argument("--workers", type=int, default=4000)
    ap.add_argument("--hubs", type=int, default=4)
    ap.add_argument("--buggify", action="store_true")
    ap.add_argument("--teeth", action="store_true")
    args = ap.parse_args()

    if args.buggify:
        os.environ["RUNLOOM_BUGGIFY"] = str(args.seed)
        os.environ.setdefault("RUNLOOM_DELAY_MAX_NS", "3000")

    import runloom
    import runloom_c as rc

    n = args.workers
    results = [None] * n          # one slot per goroutine (distinct index; no shared slot)
    bad_i = (n // 2) if args.teeth else -1

    def worker(i):
        v = compute(i)
        if i == bad_i:
            v ^= 0x1                # teeth: corrupt one result by a single bit
        results[i] = v

    def main_fn():
        for i in range(n):
            rc.mn_fiber((lambda i=i: worker(i)))

    runloom.run(args.hubs, main_fn)

    # Verify every returned value against the in-memory model.
    missing = [i for i in range(n) if results[i] is None]
    wrong = [i for i in range(n) if results[i] is not None and results[i] != compute(i)]

    print("result_oracle: workers=%d missing=%d wrong=%d%s" % (
        n, len(missing), len(wrong), " (buggify)" if args.buggify else ""))

    if args.teeth:
        if bad_i in wrong:
            print("result_oracle[teeth]: oracle FIRED -- caught the corrupted "
                  "result at goroutine %d" % bad_i)
            return 0
        print("result_oracle[teeth]: FAIL -- oracle vacuous (corrupt result not "
              "caught)")
        return 1

    if missing:
        print("result_oracle: FAIL -- %d goroutine(s) never returned (liveness); "
              "first=%r" % (len(missing), missing[:5]))
        return 1
    if wrong:
        print("result_oracle: FAIL -- %d SILENT-CORRUPTION result(s); first=%r "
              "(got %r, want %r)" % (
                  len(wrong), wrong[:5], results[wrong[0]], compute(wrong[0])))
        return 1
    print("result_oracle: PASS -- all %d results match the model (no silent "
          "corruption)" % n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
