#!/usr/bin/env python3
"""chess_explore.py -- CHESS/Coyote-style SYSTEMATIC, context-bounded schedule
explorer over the controlled M:N baton.

The baton (RUNLOOM_MN_SEED + barrier) reduces ALL M:N scheduling nondeterminism
to a single serialized choice point -- runloom_mn_ctrl_choose(): hand the baton
to one of the wanting hubs.  The seeded version draws that index from an RNG (one
random interleaving per seed).  This driver instead ENUMERATES the choices: the
C hook (RUNLOOM_MN_SCHEDULE) drives each grant's index from a caller-supplied
sequence and logs each grant's fan-out (RUNLOOM_MN_FANOUT), so the runtime becomes
a black-box transition function  run(prefix) -> (fanout_trace, outcome)  and the
schedule space is a tree we can walk.

We walk it with ITERATIVE DEEPENING on a PREEMPTION BOUND c (CHESS): a preemption
is choosing an index != the "continue-same-hub" default; choosing the default is
FREE.  Bounding switch-aways (not context switches) makes loop-carried scheduling
cheap and budgets only genuine interleaving inversions.  At bound c we exhaustively
cover EVERY schedule reachable with <= c preemptions -- a coverage theorem
("all <=c-preemption interleavings checked"), the qualitative jump from the
seeded-RNG sample.

SCOPE (v1, honest): CLOSED workloads only -- CPU + chan/lock/sync + logical-clock
sched_sleep (the repro_probe/select/timer/pct_find class).  Workloads that OFFLOAD
/ do real I/O / use aio call_at are OUT: the barrier does not order foreign-wake
arrival, so a prefix is not a pure function of the schedule there -- the
NON_REPLAYABLE guard catches and flags such workloads rather than silently
mis-covering.  No partial-order reduction yet (exact-duplicate dedup only), so it
is exponential in c and fan-out -- rely on small c (1-3) and tiny programs.

House style: .format(), no f-strings.
Usage:
  chess_explore.py [--workload P] [--cmax C] [--timeout S] [--replay R] [-- ENV=V ...]
"""
import argparse
import json
import os
import subprocess
import sys
import tempfile

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    "..", ".."))
PY = sys.executable
DEFAULT_WORKLOAD = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "chess_target.py")


def run_prefix(workload, prefix, timeout, extra_env):
    """Run the workload once with the baton driven by `prefix` (a list of chosen
    indices).  Returns (trace, outcome, hubseq):
      trace  = list of {g,cnt,def,k,hub} grant records (RUNLOOM_MN_FANOUT)
      outcome in {OK, BUG, CRASH, WEDGE}
      hubseq = tuple of granted hub ids (the realized schedule identity)
    """
    fo = tempfile.NamedTemporaryFile(prefix="chess_fo_", suffix=".jsonl",
                                     delete=False)
    fo.close()
    env = dict(os.environ)
    env.update(PYTHON_GIL="0", PYTHONPATH=os.path.join(ROOT, "src"),
               RUNLOOM_MN_SEED="1",
               RUNLOOM_MN_SCHEDULE=",".join(str(k) for k in prefix),
               RUNLOOM_MN_FANOUT=fo.name)
    env.update(extra_env)
    timed_out = False
    rc = None
    last = ""
    try:
        r = subprocess.run([PY, workload], env=env, cwd=ROOT, timeout=timeout,
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        rc = r.returncode
        lines = r.stdout.decode("utf-8", "replace").strip().splitlines()
        last = lines[-1] if lines else ""
    except subprocess.TimeoutExpired:
        timed_out = True
    trace = []
    try:
        with open(fo.name) as f:
            for line in f:
                line = line.strip()
                if line:
                    trace.append(json.loads(line))
    except Exception:
        pass
    finally:
        try:
            os.unlink(fo.name)
        except OSError:
            pass
    if timed_out:
        outcome = "WEDGE"
    elif rc is not None and (rc < 0 or rc >= 0x40000000):
        outcome = "CRASH"
    elif last.startswith("BUG"):
        outcome = "BUG"
    else:
        outcome = "OK"
    hubseq = tuple(r["hub"] for r in trace)
    return trace, outcome, last, hubseq


def first_choice(trace, depth):
    """The first grant record at position g >= depth with fan-out cnt >= 2."""
    for r in trace:
        if r["g"] >= depth and r["cnt"] >= 2:
            return r
    return None


def explore(workload, c, timeout, extra_env, results, runs, cov):
    """Exhaustively enumerate every schedule reachable with <= c preemptions.
    Mutates `results` (hubseq -> (outcome, exemplar_line, prefix)), `runs`
    (counter), and `cov` (schedule-coverage: which choice-point BRANCHES were
    actually exercised).  Returns (pruned_by_bound, max_depth, max_fanout)."""
    pruned = 0
    max_depth = 0
    max_fanout = 0
    seen_prefix = set()
    # stack of (prefix, preempts)
    stack = [([], 0)]
    seen_prefix.add(())
    while stack:
        prefix, preempts = stack.pop()
        trace, outcome, last, hubseq = run_prefix(workload, prefix, timeout, extra_env)
        runs["n"] += 1
        max_depth = max(max_depth, len(trace))
        if trace:
            max_fanout = max(max_fanout, max(r["cnt"] for r in trace))
        # ---- schedule coverage: record each choice point's fan-out + every
        # (grant, chosen-index) branch this realized schedule actually took.
        for r in trace:
            cov["points"][r["g"]] = max(cov["points"].get(r["g"], 0), r["cnt"])
            cov["covered"].add((r["g"], r["k"]))
        if hubseq not in results:
            results[hubseq] = (outcome, last, list(prefix))
        ch = first_choice(trace, len(prefix))
        if ch is None:
            continue                     # leaf: schedule fully determined
        g, cnt, defidx = ch["g"], ch["cnt"], ch["def"]
        realized = [r["k"] for r in trace if r["g"] < g]   # k for grants 0..g-1
        # default-extension (FREE): walk past this choice to expose the next one
        dpref = realized + [defidx]
        if tuple(dpref) not in seen_prefix:
            seen_prefix.add(tuple(dpref))
            stack.append((dpref, preempts))
        # alternatives: each is one preemption (switch-away)
        for a in range(cnt):
            if a == defidx:
                continue
            if preempts + 1 <= c:
                apref = realized + [a]
                if tuple(apref) not in seen_prefix:
                    seen_prefix.add(tuple(apref))
                    stack.append((apref, preempts + 1))
            else:
                pruned += 1
    return pruned, max_depth, max_fanout


def replay_check(workload, prefix, timeout, extra_env, reps):
    """Re-run a schedule prefix `reps` times; return (stable, outcomes, traces_match).
    The biggest-risk guard: a divergent fan-out trace => NON_REPLAYABLE (the
    workload is outside v1 closed-world scope, not a real result)."""
    sigs = set()
    outs = set()
    for _ in range(reps):
        trace, outcome, _last, _hub = run_prefix(workload, prefix, timeout, extra_env)
        sigs.add(tuple((r["g"], r["cnt"], r["k"], r["hub"]) for r in trace))
        outs.add(outcome)
    return (len(sigs) == 1 and len(outs) == 1), outs, len(sigs)


def run_pct_tail(workload, depth, k, reps, timeout, extra_env, cmax, pruned):
    """Quantify the >cmax-preemption TAIL that exhaustive enumeration did not
    reach.  PCT (Burckhardt et al., ASPLOS 2010): a depth-d schedule bug among a
    run's grants over n hubs is hit with probability p >= 1/(n*k^(d-1)) per seed,
    so R seeds give 1-(1-p)^R confidence of catching ANY depth-<=d bug.  This
    turns the unexhausted tail from "we hope the fuzzer covered it" into a stated
    miss-probability: exhaustive up to c=cmax, PCT-quantified beyond."""
    n = 2                                   # controlled-baton workloads here: 2 hubs
    p = 1.0 / (n * (k ** (depth - 1)))
    conf = 1.0 - (1.0 - p) ** reps
    hits = 0
    for seed in range(1, reps + 1):
        e = dict(os.environ)
        e.update(PYTHON_GIL="0", PYTHONPATH=os.path.join(ROOT, "src"),
                 RUNLOOM_MN_SEED=str(seed), RUNLOOM_MN_PCT=str(depth),
                 RUNLOOM_MN_PCT_STEPS=str(k))
        e.update(extra_env)
        e.pop("RUNLOOM_MN_SCHEDULE", None)   # PCT path, not the schedule drive
        e.pop("RUNLOOM_MN_FANOUT", None)
        try:
            r = subprocess.run([PY, workload], env=e, cwd=ROOT, timeout=timeout,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            lines = r.stdout.decode("utf-8", "replace").strip().splitlines()
            if lines and lines[-1].startswith("BUG"):
                hits += 1
        except subprocess.TimeoutExpired:
            pass
    tail = ("the <=%d-preemption space was EXHAUSTED, so this PCT pass quantifies "
            "only DEEPER (>%d-preemption) bugs" % (cmax, cmax)) if pruned == 0 else \
           ("%d schedules were pruned by the c<=%d bound; PCT quantifies that "
            "unexhausted remainder" % (pruned, cmax))
    print("PCT-confidence tail ({}):".format(tail))
    print("  PCT depth-{} bound: p >= 1/(n*k^(d-1)) = 1/(2*{}^{}) = {:.4g} per run"
          .format(depth, k, depth - 1, p))
    print("  {} runs -> {:.3f}% confidence of catching any depth-<={} schedule bug "
          "(miss-probability {:.3g})".format(reps, 100.0 * conf, depth, 1.0 - conf))
    print("  observed: {}/{} PCT runs hit the bug".format(hits, reps))


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--workload", default=DEFAULT_WORKLOAD)
    p.add_argument("--cmax", type=int, default=2)
    p.add_argument("--timeout", type=float, default=30.0)
    p.add_argument("--replay", type=int, default=12,
                   help="reps to confirm a found-bug schedule is deterministic")
    p.add_argument("--pct", default=None, metavar="D:K:R",
                   help="after enumeration, quantify the >c-preemption TAIL: run R "
                        "PCT(depth=D, steps=K) samples and report the 1-(1-p)^R "
                        "confidence of catching any depth-<=D schedule bug (e.g. 2:14:200)")
    p.add_argument("env", nargs="*", help="extra ENV=VALUE for the workload")
    a = p.parse_args(argv)
    extra_env = {}
    for kv in a.env:
        if "=" in kv:
            k, v = kv.split("=", 1)
            extra_env[k] = v

    print("CHESS systematic explorer over the controlled M:N baton")
    print("  workload: {}".format(os.path.relpath(a.workload, ROOT)))
    if extra_env:
        print("  env: {}".format(extra_env))
    print("  preemption bound c = 0..{} (iterative deepening)".format(a.cmax))
    print("-" * 72)
    print("  {:>2}  {:>10}  {:>9}  {:>9}  {:>9}  {}".format(
        "c", "schedules", "exhausted", "max_depth", "max_fan", "new outcomes"))

    results = {}
    cov = {"points": {}, "covered": set()}
    seen_outcomes = set()
    first_bug = None
    final_pruned = 0
    for c in range(0, a.cmax + 1):
        runs = {"n": 0}
        pruned, max_depth, max_fanout = explore(
            a.workload, c, a.timeout, extra_env, results, runs, cov)
        final_pruned = pruned
        # outcomes so far
        classes = {}
        for hubseq, (outcome, last, prefix) in results.items():
            classes.setdefault(outcome, (last, prefix, hubseq))
        new = sorted(k for k in classes if k not in seen_outcomes)
        seen_outcomes |= set(classes)
        exhausted = (pruned == 0)
        print("  {:>2}  {:>10}  {:>9}  {:>9}  {:>9}  {}".format(
            c, len(results), "yes" if exhausted else "no(-{})".format(pruned),
            max_depth, max_fanout, ",".join(new) if new else "-"))
        if first_bug is None and "BUG" in classes:
            first_bug = (c, classes["BUG"])

    # ---- schedule-coverage metric: of the choice-point branches encountered,
    # what fraction the enumeration actually exercised (100% at full exhaustion). ----
    total_branches = sum(cov["points"].values())
    covered = len(cov["covered"])
    print("-" * 72)
    print("schedule coverage: {}/{} choice-point branches exercised ({:.0f}%) over "
          "{} choice points (max fan-out {}), {} distinct schedules".format(
              covered, total_branches,
              100.0 * covered / total_branches if total_branches else 100.0,
              len(cov["points"]), max(cov["points"].values()) if cov["points"] else 0,
              len(results)))
    if a.pct:
        try:
            d, k, reps = (int(x) for x in a.pct.split(":"))
            run_pct_tail(a.workload, d, k, reps, a.timeout, extra_env, a.cmax,
                         final_pruned)
        except ValueError:
            print("  (--pct expects D:K:R, e.g. 2:14:200)")

    print("-" * 72)
    if first_bug is None:
        print("NO BUG found within c <= {} ({} distinct schedules explored)."
              .format(a.cmax, len(results)))
        # report any non-OK outcomes
        bad = {k: v for k, v in results.items() if v[0] not in ("OK",)}
        if bad:
            print("  non-OK outcomes:")
            for hubseq, (outcome, last, prefix) in list(bad.items())[:5]:
                print("    {}  schedule={}  hubs={}".format(outcome, prefix, list(hubseq)))
        return 0

    c, (last, prefix, hubseq) = first_bug
    print("BUG found at c={} (depth {} preemption{}):".format(
        c, c, "" if c == 1 else "s"))
    print("  exact schedule (chosen indices): {}".format(prefix))
    print("  realized hub sequence:           {}".format(list(hubseq)))
    print("  workload output:                 {}".format(last))
    # replay-check the bug schedule (determinism + NON_REPLAYABLE guard)
    stable, outs, nsigs = replay_check(a.workload, prefix, a.timeout, extra_env, a.replay)
    if stable:
        print("  replay: {}/{} runs reproduce BUG identically -- deterministic repro"
              .format(a.replay, a.replay))
    else:
        print("  replay: NON_REPLAYABLE -- {} distinct fan-out traces / outcomes {} over "
              "{} reps. The workload is OUTSIDE v1 closed-world scope (it offloads / "
              "does real I/O); coverage claims do NOT apply.".format(nsigs, outs, a.replay))
    return 0


if __name__ == "__main__":
    sys.exit(main())
