#!/usr/bin/env python3
"""chess_greybox.py -- COVERAGE-GUIDED (greybox) interleaving fuzzer over the
controlled M:N baton.  The feedback-directed complement to chess_explore.py.

chess_explore enumerates EVERY schedule within a preemption bound c (a coverage
THEOREM, but exponential in c and fan-out -- it cannot reach the deep, rare
interleavings where the foreign-thread-wake SIGSEGV / lost cross-hub wake class
lives).  pct_find samples randomly with a depth guarantee but has no memory.
This is the third axis (Wolff et al. ASPLOS'24, "reachability-guided" fuzzing):
keep a CORPUS of schedules, define a SCHEDULE-COVERAGE fingerprint, and MUTATE the
corpus toward schedules that exercise NEW scheduling decisions -- so feedback
reaches interleavings the bounded explorer can't afford, with no exhaustive blowup.

It reuses chess_explore.run_prefix verbatim, so it needs NO new C: the baton hook
already exposes run(prefix) -> (fan-out trace, outcome) via RUNLOOM_MN_SCHEDULE +
RUNLOOM_MN_FANOUT.  A found bug is reported WITH its reproducing
RUNLOOM_MN_SCHEDULE (deterministic replay -- the whole point of the baton).

Coverage fingerprint: the set of PREEMPTION EDGES exercised -- each grant where
the chosen hub != the continue-same-hub default contributes
(bucketed-position, from_hub -> to_hub).  A schedule that produces a new edge is
saved to the corpus; this rewards genuinely new inversions, not loop-carried
repeats.  (SCOPE = the same CLOSED workloads chess_explore supports: CPU +
chan/lock/sync + logical-clock sleep; offload/real-IO are NON_REPLAYABLE.)

Usage:
  chess_greybox.py [--workload P] [--iters N] [--time S] [--timeout S]
                   [--seed S] [--max-prefix L] [-- ENV=V ...]
  chess_greybox.py --teeth          # prove it FINDS chess_target's late-value bug

Exit: 0 = no bug found (coverage report); 1 = a BUG/CRASH/WEDGE found (repro
printed); 2 = setup error.
"""
import argparse
import os
import random
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)
import chess_explore  # noqa: E402  -- reuse run_prefix (the black-box harness)

DEFAULT_WORKLOAD = os.path.join(HERE, "chess_target.py")


def fingerprint(trace):
    """Set of preemption edges: each grant where chosen k != default index, as
    (position_bucket, from_hub -> to_hub).  Position is log-bucketed so a loop's
    Nth iteration doesn't explode coverage."""
    edges = set()
    prev_hub = None
    for i, rec in enumerate(trace):
        hub = rec.get("hub")
        if rec.get("k") != rec.get("def") and prev_hub is not None:
            bucket = i.bit_length()        # 0,1,2,3,.. log buckets
            edges.add((bucket, prev_hub, hub))
        prev_hub = hub
    return edges


def realized_choices(trace):
    return [rec.get("k", 0) for rec in trace]


def mutate(trace, rng):
    """Build a new prefix that follows the realized schedule up to a real branch
    point (fan-out > 1) and flips to a different valid index there, sometimes
    flipping a second point too (deeper inversions).  None if no branch exists."""
    choices = [i for i, rec in enumerate(trace) if rec.get("cnt", 1) > 1]
    if not choices:
        return None
    realized = realized_choices(trace)
    flips = 1 if rng.random() < 0.7 else 2
    pts = sorted(rng.sample(choices, min(flips, len(choices))))
    j0 = pts[0]
    newp = realized[:j0]
    last = j0
    for j in pts:
        # carry realized choices between flip points
        newp += realized[last:j]
        cnt = trace[j]["cnt"]
        alt = rng.choice([x for x in range(cnt) if x != realized[j]])
        newp.append(alt)
        last = j + 1
    return newp


def main(argv):
    ap = argparse.ArgumentParser(description="coverage-guided interleaving fuzzer")
    ap.add_argument("--workload", default=DEFAULT_WORKLOAD)
    ap.add_argument("--iters", type=int, default=400)
    ap.add_argument("--time", type=float, default=0.0, help="wall-clock budget (s); overrides --iters if >0")
    ap.add_argument("--timeout", type=float, default=20.0, help="per-run cap (s)")
    ap.add_argument("--seed", type=int, default=12345)
    ap.add_argument("--max-prefix", type=int, default=64)
    ap.add_argument("--teeth", action="store_true",
                    help="run chess_target with a late TARGET and assert a BUG is found")
    ap.add_argument("rest", nargs="*", help="-- ENV=V ... passed to the workload")
    args = ap.parse_args(argv)

    extra_env = {}
    for kv in args.rest:
        if "=" in kv:
            k, v = kv.split("=", 1)
            extra_env[k] = v
    if args.teeth:
        extra_env.setdefault("CHESS_N", "5")
        extra_env.setdefault("CHESS_TARGET", "4")   # a late value -> needs an inversion

    # run_prefix runs the workload with cwd=ROOT, so a relative path silently
    # fails to open -> 0 grants -> a FALSE green. Resolve to an absolute path.
    wl = args.workload
    if not os.path.isabs(wl):
        for cand in (os.path.abspath(wl), os.path.join(HERE, wl)):
            if os.path.isfile(cand):
                wl = cand
                break
    if not os.path.isfile(wl):
        print("chess_greybox: workload not found: {0}".format(args.workload))
        return 2
    args.workload = wl

    rng = random.Random(args.seed)
    coverage = set()
    corpus = [[]]                       # seed: empty prefix = pure default policy
    findings = []
    iters = 0
    t0 = None

    def budget_left():
        if args.time > 0:
            return (time.monotonic() - t0) < args.time
        return iters < args.iters

    # prime with the default schedule
    trace, outcome, last, _hub = chess_explore.run_prefix(args.workload, [], args.timeout, extra_env)
    if outcome != "OK":
        findings.append(([], outcome, last))
    if len(trace) == 0:
        # No grant points: the workload did not engage the controlled baton.
        # Refuse to report a false green -- this is a setup error, not "no bugs".
        print("chess_greybox: workload produced 0 grant points -- it must use "
              "mn_init(>1)+mn_fiber+sched_sleep under RUNLOOM_MN_SEED and be "
              "REPLAYABLE (no offload/real-IO). last output: {0!r}".format(last[:80]))
        return 2
    coverage |= fingerprint(trace)
    seed_traces = {tuple(): trace}

    import time as _t
    t0 = _t.monotonic()
    new_cov_hits = 0
    while budget_left():
        iters += 1
        base = rng.choice(corpus)
        bt = seed_traces.get(tuple(base))
        if bt is None:
            bt, o, l, _h = chess_explore.run_prefix(args.workload, base, args.timeout, extra_env)
            seed_traces[tuple(base)] = bt
        mp = mutate(bt, rng)
        if mp is None or len(mp) > args.max_prefix:
            continue
        trace, outcome, last, _hub = chess_explore.run_prefix(args.workload, mp, args.timeout, extra_env)
        if outcome != "OK":
            findings.append((mp, outcome, last))
            print("  FOUND {0} at iter {1}: {2}".format(outcome, iters, last[:80]))
            print("    REPRO: RUNLOOM_MN_SEED=1 RUNLOOM_MN_SCHEDULE={0} {1}"
                  .format(",".join(str(k) for k in mp),
                          " ".join("{0}={1}".format(k, v) for k, v in extra_env.items())))
            if args.teeth:
                break
        fp = fingerprint(trace)
        if fp - coverage:
            coverage |= fp
            corpus.append(mp)
            seed_traces[tuple(mp)] = trace
            new_cov_hits += 1

    print("\nchess_greybox: {0} iters, coverage={1} edges, corpus={2}, {3} findings"
          .format(iters, len(coverage), len(corpus), len(findings)))

    if args.teeth:
        ok = any(o == "BUG" for _p, o, _l in findings)
        print("teeth: {0} -- {1}".format(
            "PASS (greybox found chess_target's late-value bug)" if ok else
            "FAIL (did not find the known bug -- mutation/coverage broken)",
            "exit 0" if ok else "exit 1"))
        return 0 if ok else 1

    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
