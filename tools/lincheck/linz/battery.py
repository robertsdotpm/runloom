"""The linearizability battery driver.

For each (primitive, seed): record a concurrent history on the real M:N scheduler
in a FRESH hermetic subprocess (mn statics + cached env flags reset only at process
birth), then check it against the sequential reference spec with the pure-Python
WGL checker.  Under --seeded the history is a function of the seed, so:

  * a NOT-LINEARIZABLE verdict is a real correctness bug in the primitive,
    reproducible from the single integer seed;
  * running the SAME seed twice must yield the IDENTICAL history -- a divergence
    means the schedule is not fully seed-driven (an unenumerated wake source),
    itself a finding (this is the same twice-and-compare the lifefuzz mn kinds do).

This is the generative pillar: unbounded coverage of each primitive from a ~15-line
spec + a ~20-line workload, every failure a reproducible integer.

Usage:
  battery.py [primitive ...] [--seeds A B] [--procs K] [--ops M] [--hubs H]
             [--wallclock] [--budget N] [-v]
  (no primitive -> all of: chan mutex rwmutex semaphore waitgroup event)
"""
import hashlib
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import checker  # noqa: E402
import specs    # noqa: E402

REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
PY = os.environ.get("RUNLOOM_PYTHON",
                    os.path.expanduser("~/.pyenv/versions/3.14.4t/bin/python3"))
ALL = ["chan", "mutex", "rwmutex", "semaphore", "waitgroup", "event"]

# Primitives whose seeded history is bit/observably reproducible: the native
# families parked on the C ready-ring, which the seeded baton fully orders.  For
# these a same-seed observable divergence is a REAL new determinism regression.
#
# The Co* foreign-safe family (runloom.sync.Lock == CoLock, runloom.sync.Event ==
# CoEvent) wakes multiple parked waiters in an order NOT governed by the seed
# (derived from non-seed-stable object identity, contract #9), so their schedule
# jitters run-to-run.  That is a determinism-COVERAGE gap, not a correctness bug:
# every recorded history is still checked for linearizability (which needs no
# determinism), and the observable OUTCOME still linearizes -- so Lock/Event get
# linearizability coverage with the reproducibility gate relaxed and the jitter
# reported informationally.
SEED_DETERMINISTIC = {"chan", "rwmutex", "semaphore", "waitgroup"}


def hermetic_env():
    env = {k: v for k, v in os.environ.items() if not k.startswith("RUNLOOM_")}
    env["PYTHON_GIL"] = "0"
    env["PYTHONHASHSEED"] = "0"            # int observables only, but pin anyway
    env["PYTHON_TLBC"] = "0"              # no first-run re-exec banner
    env["PYTHONPATH"] = os.path.join(REPO, "src")
    return env


def record_one(primitive, seed, seeded, procs, ops, hubs, timeout=120):
    """Run record.py in a fresh subprocess; return (payload_dict, err_str)."""
    fd, path = tempfile.mkstemp(prefix="linz_%s_" % primitive, suffix=".json")
    os.close(fd)
    cmd = [PY, os.path.join(HERE, "record.py"), primitive, "--out", path]
    if seeded:
        cmd += ["--seeded", str(seed)]
    else:
        cmd += ["--wallclock"]
    if procs is not None:
        cmd += ["--procs", str(procs)]
    if ops is not None:
        cmd += ["--ops", str(ops)]
    if hubs is not None:
        cmd += ["--hubs", str(hubs)]
    try:
        p = subprocess.run(cmd, env=hermetic_env(), cwd=REPO,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           timeout=timeout)
    except subprocess.TimeoutExpired:
        _rm(path)
        return None, "TIMEOUT (possible lost-wake / deadlock)"
    if p.returncode != 0:
        _rm(path)
        return None, "record exit=%d stderr=%s" % (
            p.returncode, p.stderr.decode("utf-8", "replace")[-400:])
    try:
        with open(path, "r") as fh:
            payload = json.load(fh)
    except Exception as exc:                # noqa: BLE001
        _rm(path)
        return None, "bad json: %s" % (exc,)
    _rm(path)
    return payload, None


def _rm(path):
    try:
        os.unlink(path)
    except OSError:
        pass


def spec_for(primitive, meta):
    if primitive == "semaphore":
        return specs.Semaphore(meta.get("capacity", 4))
    return specs.REGISTRY[primitive]()


def observable(events):
    """The linearizability-relevant projection: inputs+outputs in call order,
    with the exact wake-interleaving stamps dropped.  Two runs with the same
    observable projection reproduce the same (potential) finding; a NOT-
    LINEARIZABLE history's failing inputs/outputs recur under the same seed."""
    ev = sorted(events, key=lambda e: e["call"])
    return [(e["proc"], e["op"], tuple(e["args"]), e["res"], tuple(e["rets"]))
            for e in ev]


def check_seed(primitive, seed, seeded, procs, ops, hubs, budget, verbose):
    """Returns (status, detail).  status in
    {ok, jitter, NONDET, NONLIN, UNKNOWN, ERROR}."""
    payload, err = record_one(primitive, seed, seeded, procs, ops, hubs)
    if err:
        return "ERROR", err
    events = payload["events"]
    meta = payload["meta"]
    spec = spec_for(primitive, meta)
    ops_list = checker.ops_from_events(events, spec)
    res = checker.check(spec, ops_list, budget=budget)
    if res.verdict == checker.NOT_LINEARIZABLE:
        return "NONLIN", "seed=%d %d ops NOT linearizable | history=%s" % (
            seed, res.nops, json.dumps(events))
    if res.verdict == checker.UNKNOWN:
        return "UNKNOWN", "seed=%d %s (%d ops, %d steps)" % (
            seed, res.detail, res.nops, res.steps)
    if seeded:
        # reproducibility: same seed must reproduce the same OBSERVABLE history.
        payload2, err2 = record_one(primitive, seed, seeded, procs, ops, hubs)
        if err2:
            return "ERROR", "2nd run: %s" % err2
        if observable(events) != observable(payload2["events"]):
            if primitive in SEED_DETERMINISTIC:
                return "NONDET", ("seed=%d observable history not reproducible "
                                  "(native primitive lost seed-determinism)" % seed)
            return "conondet", ("seed=%d Co* wake-order not seed-governed "
                                "(linearizable; determinism-coverage gap)" % seed)
        if events != payload2["events"]:
            # observable-identical but wake-interleaving stamps differ: the Co*
            # multi-waiter wake jitter, harmless to linearizability.
            return "jitter", "seed=%d wake-order jitter only" % seed
    if verbose:
        print("  [ok] %s seed=%d: LINEARIZABLE (%d ops, %d steps)" % (
            primitive, seed, res.nops, res.steps))
    return "ok", "%d ops" % res.nops


FATAL = ("NONLIN", "UNKNOWN", "ERROR", "NONDET")


def run(primitives, seed_lo, seed_hi, procs, ops, hubs, seeded, budget, verbose):
    total = 0
    findings = []           # fatal: real correctness / native-determinism failures
    info = []               # informational: Co* wake-order jitter / conondet
    for primitive in primitives:
        nlin = 0
        for seed in range(seed_lo, seed_hi):
            total += 1
            status, detail = check_seed(primitive, seed, seeded, procs, ops,
                                        hubs, budget, verbose)
            if status in ("ok", "jitter", "conondet"):
                nlin += 1                       # linearizable either way
                if status != "ok":
                    info.append((status, primitive, seed, detail))
            if status in FATAL:
                findings.append((status, primitive, seed, detail))
                print("  [%s] %s seed=%d: %s" % (status, primitive, seed, detail))
        note = ""
        if primitive not in SEED_DETERMINISTIC and seeded:
            note = "  (Co* family: linearizability-only, wake-order not seed-governed)"
        print("== %-10s %d/%d seeds LINEARIZABLE ==%s" % (
            primitive, nlin, seed_hi - seed_lo, note))
    print("\n== battery: %d checks | %d fatal findings | %d informational (Co* jitter) ==" % (
        total, len(findings), len(info)))
    return 0 if not findings else 1


def main(argv):
    prims = []
    seed_lo, seed_hi = 0, 20
    procs = ops = hubs = None
    seeded = True
    budget = checker.DEFAULT_BUDGET
    verbose = False
    i = 1
    while i < len(argv):
        a = argv[i]
        if a == "--seeds":
            seed_lo, seed_hi = int(argv[i + 1]), int(argv[i + 2])
            i += 3
        elif a == "--procs":
            procs = int(argv[i + 1]); i += 2
        elif a == "--ops":
            ops = int(argv[i + 1]); i += 2
        elif a == "--hubs":
            hubs = int(argv[i + 1]); i += 2
        elif a == "--wallclock":
            seeded = False; i += 1
        elif a == "--budget":
            budget = int(argv[i + 1]); i += 2
        elif a in ("-v", "--verbose"):
            verbose = True; i += 1
        elif a in specs.REGISTRY:
            prims.append(a); i += 1
        else:
            print("unknown arg / primitive: %s" % a); return 2
    if not prims:
        prims = ALL
    return run(prims, seed_lo, seed_hi, procs, ops, hubs, seeded, budget, verbose)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
