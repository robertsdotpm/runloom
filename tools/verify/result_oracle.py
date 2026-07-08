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

--fault-spawn N interleaves a real fault with the workload (AWS ShardStore
FailDiskOnce): a spawn-alloc OOM at the Nth spawn.  Because the workers are
INDEPENDENT (each writes its own slot, no channel a death could strand), a dropped
worker is tolerated (relaxed count) while survivors must stay CORRECT and the
scheduler consistent -- rank 11's "fault in the op sequence, wrong result never"
property, in a fault-tolerant workload.  (The full fault-in-the-lifefuzz-alphabet
integration needs lifefuzz's producer/consumer workload made fault-tolerant so a
dropped goroutine doesn't deadlock the rest; scoped in QA_STEAL_ROADMAP.md.)
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
    ap.add_argument("--fault-spawn", type=int, default=0, metavar="N",
                    help="inject a spawn-alloc OOM at the Nth spawn (fault "
                         "interleaved with the workload; survivors must stay "
                         "correct, dropped ones are tolerated)")
    args = ap.parse_args()

    if args.buggify:
        os.environ["RUNLOOM_BUGGIFY"] = str(args.seed)
        os.environ.setdefault("RUNLOOM_DELAY_MAX_NS", "3000")
    if args.fault_spawn:
        # Fault-in-the-workload (AWS ShardStore FailDiskOnce in the op alphabet):
        # a spawn OOM at the Nth spawn (nth:N fires exactly once).  Set before import so the site's armed flag
        # caches armed.  The workers are INDEPENDENT (each writes its own slot,
        # no channel a death could strand), so a dropped worker just leaves a gap
        # -- no deadlock.  The property: the fault must not CORRUPT a survivor's
        # result or the scheduler state; dropped workers are a relaxed count.
        os.environ["RUNLOOM_FAULT_SPAWN_G"] = "nth:%d:12" % args.fault_spawn

    import runloom
    import runloom_c as rc

    n = args.workers
    results = [None] * n          # one slot per goroutine (distinct index; no shared slot)
    bad_i = (n // 2) if args.teeth else -1
    spawn_fail = [0]

    def worker(i):
        v = compute(i)
        if i == bad_i:
            v ^= 0x1                # teeth: corrupt one result by a single bit
        results[i] = v

    def main_fn():
        for i in range(n):
            try:
                rc.mn_fiber((lambda i=i: worker(i)))
            except (MemoryError, RuntimeError):
                spawn_fail[0] += 1   # the injected spawn OOM dropped this worker

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

    if args.fault_spawn:
        # Fault mode: dropped workers (missing) are EXPECTED and tolerated (the
        # spawn OOM legitimately dropped them).  What stays STRICT: a survivor's
        # result must be CORRECT (the fault must not corrupt live goroutines) and
        # the scheduler must stay consistent.
        try:
            rc._self_check()
        except Exception as e:  # noqa: BLE001
            print("result_oracle[fault]: FAIL -- fault corrupted scheduler state: "
                  "%r" % (e,))
            return 1
        if wrong:
            print("result_oracle[fault]: FAIL -- fault CORRUPTED %d survivor "
                  "result(s); first=%r (got %r, want %r)" % (
                      len(wrong), wrong[:5], results[wrong[0]], compute(wrong[0])))
            return 1
        print("result_oracle[fault]: PASS -- spawn-OOM (at spawn #%d) dropped %d "
              "worker(s) (%d missing); all %d survivors correct, self_check clean"
              % (args.fault_spawn, spawn_fail[0], len(missing), n - len(missing)))
        return 0

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
