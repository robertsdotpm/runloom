#!/usr/bin/env python3
"""stacked_fault_sweep.py -- compound (two-at-once) fault injection
(SQLite stacked OOM+IO injectors, AWS ShardStore faults-in-the-op-alphabet;
QA-steal rank 10).

pygo's counted sweep (fault_sweep_counted.py) arms ONE RUNLOOM_FAULT_* site at a
time.  The highest-risk, least-tested code is the unwind path that ITSELF hits a
fault -- fd/handle/g leaks and half-migrated tstate corruption hide there.  The
runtime already keys arming + counters per-site (netpoll_init.c.inc), so stacking
needs no C change: set TWO RUNLOOM_FAULT_* env vars.

For each historically bug-dense pair, site A faults ONCE (one error+cleanup) --
not persistently, which would SHADOW an earlier site in the same path -- and site
B faults at each reachable point ("nth:N") in the SAME run, so two faults compound
(a fault while another fault is unwinding).  After each, the post-state GS/FD
census (reused from fault_sweep_counted's workload) must return to the both-armed-
but-never-firing baseline: a survived compound fault that leaks a goroutine or fd
is a torn-cleanup bug the single-site sweep misses.

  PYTHONPATH=src python tools/verify/stacked_fault_sweep.py [PAIR ...]
    PAIR = SITE_A,SITE_B  (default: the bug-dense pairs below)
"""
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools"))
from fault_sweep_counted import WORKLOAD   # noqa: E402  (reuse the census workload)

PY = sys.executable

# (A, B): A faults once while B is swept.  Concrete mapping of the
# survey's bug-dense pairs to real sites (there is no dedicated migrate/offload
# site -- they are conditions the workload induces; the ARMED site is the enum):
#   migration x alloc -> the two spawn-alloc sites (g-struct + coro/stack)
#   offload   x fd    -> the two fdio pipe sites
#   cross-group       -> spawn OOM while an fd cleanup is unwinding
DEFAULT_PAIRS = [
    ("SPAWN_G", "SPAWN_STACK"),
    ("FD_READ", "FD_WRITE"),
    ("SPAWN_G", "FD_WRITE"),
]


def run_pair(a_spec, b_site, b_spec, timeout):
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src", SWEEP_SITE=b_site)
    env["RUNLOOM_FAULT_" + PAIR_A] = a_spec          # A persistent / baseline
    env["RUNLOOM_FAULT_" + b_site] = b_spec          # B swept
    try:
        p = subprocess.run([PY, "-c", WORKLOAD], cwd=ROOT, env=env,
                           capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return "HANG", None, None, None
    fired = gs = fd = None
    for line in (p.stdout or "").splitlines():
        if line.startswith("FIRED="):
            for kv in line.split():
                k, _, v = kv.partition("=")
                if k == "FIRED":
                    fired = int(v)
                elif k == "GS":
                    gs = int(v)
                elif k == "FD":
                    fd = int(v)
    if p.returncode < 0:
        return "CRASH(sig %d)" % -p.returncode, fired, gs, fd
    if p.returncode != 0:
        return "graceful", fired, gs, fd
    return "ok", fired, gs, fd


PAIR_A = None   # set per pair (module-level so run_pair can read it)


def sweep_pair(a, b, code=12, maxn=200, timeout=30):
    global PAIR_A
    PAIR_A = a
    findings = []
    NEVER = "nth:1000000000:%d" % code
    # A faults ONCE (at its first reach -> one error+cleanup) rather than
    # persistently, so it does not SHADOW B (an always-fault at an earlier site in
    # the same path stops the run before B's later site is ever reached).  B then
    # faults at each reachable point in the same run -> two faults compounding.
    ONCE = "once:%d" % code
    # Baseline: BOTH armed but neither fires -> leak-free GS/FD for this workload.
    _, _, base_gs, base_fd = run_pair(NEVER, b, NEVER, timeout)
    base_gs = base_gs if base_gs is not None else 0
    base_fd = base_fd if base_fd is not None else 0
    print("  baseline %s(never) x %s(never): gs=%d fd=%d" % (a, b, base_gs, base_fd),
          flush=True)
    n = 0
    while n < maxn:
        n += 1
        verdict, fired, gs, fd = run_pair(ONCE, b, "nth:%d:%d" % (n, code), timeout)
        if verdict.startswith("CRASH") or verdict == "HANG":
            findings.append((n, "%s(once)x%s(N=%d): %s" % (a, b, n, verdict)))
            print("  %s(once) x %s N=%-3d %s   <-- FINDING" % (a, b, n, verdict),
                  flush=True)
            continue
        if fired and ((gs is not None and gs > base_gs)
                      or (fd is not None and fd > base_fd + 2)):
            v = "post-state leak gs=%s(base %s) fd=%s(base %s)" % (gs, base_gs, fd, base_fd)
            findings.append((n, "%s(once)x%s(N=%d): %s" % (a, b, n, v)))
            print("  %s(once) x %s N=%-3d POST-STATE LEAK gs=%s/%s fd=%s/%s  <-- FINDING"
                  % (a, b, n, gs, base_gs, fd, base_fd), flush=True)
            continue
        if fired == 0:
            print("  %s(once) x %s exhausted at N=%d (%d compound points clean)"
                  % (a, b, n, n - 1), flush=True)
            return n - 1, findings
    print("  %s(once) x %s hit maxn=%d without exhausting" % (a, b, maxn), flush=True)
    return maxn, findings


def main(argv):
    pairs = ([tuple(x.split(",")) for x in argv[1:]] if len(argv) > 1
             else DEFAULT_PAIRS)
    t0 = time.time()
    total, all_findings = 0, []
    print("== stacked (compound) fault sweep: %s =="
          % " ".join("%sx%s" % p for p in pairs), flush=True)
    for a, b in pairs:
        print("-- %s (once) x %s (nth sweep) --" % (a, b), flush=True)
        pts, findings = sweep_pair(a, b)
        total += pts
        all_findings += [(a, b) + f for f in findings]
    print("== done in %.0fs: %d compound injection points, %d findings =="
          % (time.time() - t0, total, len(all_findings)), flush=True)
    for a, b, n, v in all_findings:
        print("  FINDING %s" % v)
    return 1 if all_findings else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
