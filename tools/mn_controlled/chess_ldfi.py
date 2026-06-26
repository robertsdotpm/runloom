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
    p = argparse.ArgumentParser()
    p.add_argument("--workload", default=DEFAULT_WORKLOAD)
    p.add_argument("--timeout", type=float, default=15.0)
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

    critical, redundant = [], []
    for k in range(n_wakes):
        out, last = run(a.workload, k, a.timeout, env)
        if out == "HANG":
            tag = "LOAD-BEARING  (drop -> hang: no backup path)"
            critical.append(k)
        elif out == "OK":
            tag = "redundant     (drop tolerated)"
            redundant.append(k)
        else:
            tag = out
        print("  drop wake #{:<3} -> {:<10} {}".format(k, out, tag))

    print("-" * 64)
    print("{}/{} wakes are LOAD-BEARING (dropping any one strands a fiber); "
          "{} redundant.".format(len(critical), n_wakes, len(redundant)))
    if not redundant:
        print("Fault-tolerance: NONE -- every wake is load-bearing (each delivery is "
              "the sole path; a single dropped wake hangs the program).")
    else:
        print("Fault-tolerance: {} wake(s) have a backup path and are tolerated; the "
              "load-bearing set {} is the minimal single-fault cut.".format(
                  len(redundant), critical))
    return 0


if __name__ == "__main__":
    sys.exit(main())
