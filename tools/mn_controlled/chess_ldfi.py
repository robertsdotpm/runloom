#!/usr/bin/env python3
"""chess_ldfi.py -- lineage-driven fault injection (v1, single-fault) over the baton.

LDFI (Alvaro et al., SoCC 2015): rather than randomly perturbing timing, reason
about which FAULTS break a success.  Here the fault is a DROPPED chan wake.  From
a PASSING run under the seeded baton (a deterministic, reproducible wake order),
enumerate every wake and re-run with that single wake DROPPED:
  * the run now HANGS  -> that wake is LOAD-BEARING: no backup path re-delivers
    it, so the fiber it would have woken is stranded -- a fault-(in)tolerance
    finding.
  * the run still completes -> that wake was REDUNDANT (a backup path covered it).
The set of load-bearing wakes is the program's fault-tolerance profile.

v1 = depth-1 (single-wake drops): "does this workload tolerate ANY dropped wake?".
Follow-on (true LDFI): depth>1 minimal CUT SETS via backward provenance -- reason
from the success's lineage to the smallest fault combination that prevents it.

Run under RUNLOOM_MN_SEED so the wake order is serialized + reproducible.
House style: .format(), no f-strings.
"""
import argparse
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import chess_explore as ce

ROOT = ce.ROOT
PY = ce.PY
DEFAULT_WORKLOAD = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "chess_chan.py")


def run(workload, drop, timeout, env, count_file=None):
    e = dict(os.environ)
    e.update(PYTHON_GIL="0", PYTHONPATH=os.path.join(ROOT, "src"),
             RUNLOOM_MN_SEED="1")
    e.update(env)
    if drop is not None:
        e["RUNLOOM_LDFI_DROP"] = str(drop)
    if count_file:
        e["RUNLOOM_LDFI_COUNT"] = count_file
    try:
        r = subprocess.run([PY, workload], env=e, cwd=ROOT, timeout=timeout,
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        lines = r.stdout.decode("utf-8", "replace").strip().splitlines()
        out = "OK" if r.returncode == 0 else "ERR(%s)" % r.returncode
        return out, (lines[-1] if lines else "")
    except subprocess.TimeoutExpired:
        return "HANG", "(timeout -- a fiber stranded)"


def main(argv=None):
    import itertools
    p = argparse.ArgumentParser()
    p.add_argument("--workload", default=DEFAULT_WORKLOAD)
    p.add_argument("--timeout", type=float, default=15.0)
    p.add_argument("--maxdepth", type=int, default=1,
                   help="search for minimal CUT SETS up to this size (1 = single-fault; "
                        ">1 enumerates wake combinations to find the smallest set whose "
                        "simultaneous drop hangs the program)")
    p.add_argument("env", nargs="*", help="extra ENV=VALUE for the workload")
    a = p.parse_args(argv)
    env = {}
    for kv in a.env:
        if "=" in kv:
            k, v = kv.split("=", 1)
            env[k] = v

    # baseline run: count the wakes (drop index way past the end -> drops nothing)
    cf = tempfile.NamedTemporaryFile(prefix="ldfi_c_", suffix=".txt", delete=False)
    cf.close()
    base_out, base_last = run(a.workload, 10 ** 9, a.timeout, env, cf.name)
    n_wakes = 0
    try:
        n_wakes = int(open(cf.name).read().strip())
    except Exception:
        pass
    finally:
        try:
            os.unlink(cf.name)
        except OSError:
            pass

    print("LDFI single-fault injection (dropped chan wake) over the baton")
    print("  workload: {}".format(os.path.relpath(a.workload, ROOT)))
    if env:
        print("  env: {}".format(env))
    print("  baseline (no drop): {}  '{}'  -- {} chan wakes occurred".format(
        base_out, base_last, n_wakes))
    if base_out != "OK":
        print("  baseline did not pass -- LDFI needs a passing run to perturb.")
        return 1
    print("-" * 64)

    def drop_hangs(combo):
        out, _last = run(a.workload, ",".join(str(x) for x in combo), a.timeout, env)
        return out == "HANG"

    # Minimal cut-set search by increasing size: a set S is a MINIMAL cut iff
    # dropping S hangs and no proper subset of S does (so we skip any combo that
    # is a superset of an already-found cut).
    minimal = []          # list of frozensets, each a minimal cut set
    for size in range(1, a.maxdepth + 1):
        found_this_size = []
        for combo in itertools.combinations(range(n_wakes), size):
            cs = frozenset(combo)
            if any(m <= cs for m in minimal):
                continue                       # superset of a smaller cut -> non-minimal
            if drop_hangs(combo):
                found_this_size.append(cs)
                tag = ("LOAD-BEARING" if size == 1 else
                       "depth-{} cut".format(size))
                print("  drop {{{}}} -> HANG   {} (no surviving path)".format(
                    ",".join("#%d" % x for x in combo), tag))
            elif size == 1:
                print("  drop {{#{}}} -> OK     redundant (a backup path completes it)"
                      .format(combo[0]))
        minimal.extend(found_this_size)

    print("-" * 64)
    if not minimal:
        print("NO cut set <= size {} found: the program TOLERATES every combination of "
              "up to {} dropped wakes (fault-tolerant at this depth).".format(
                  a.maxdepth, a.maxdepth))
    else:
        by_size = {}
        for m in minimal:
            by_size.setdefault(len(m), []).append(sorted(m))
        sizes = sorted(by_size)
        print("minimal cut sets found (smallest fault that hangs the program):")
        for s in sizes:
            print("  size {}: {}".format(
                s, ", ".join("{" + ",".join("#%d" % x for x in c) + "}"
                             for c in by_size[s])))
        if sizes[0] == 1:
            print("Fault-tolerance: NONE at depth 1 -- {} single wake(s) are each a "
                  "sole path.".format(len(by_size[1])))
        else:
            print("Fault-tolerance: tolerates any single dropped wake; smallest hang "
                  "needs {} simultaneous drops.".format(sizes[0]))
    if a.maxdepth > 1:
        print("NOTE: this index-based cut-set search is sound for FIXED-lineage "
              "workloads (the set of wakes that occur does not change under injection). "
              "Truly redundant wakes shift the lineage (a dropped wake makes a "
              "different wake occur) -- and select() commits its case before the wake, "
              "so it is not redundant either; depth>1 there needs backward-provenance "
              "LDFI (the deeper follow-on).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
