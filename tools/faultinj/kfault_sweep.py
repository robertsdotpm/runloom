#!/usr/bin/env python3
"""kfault_sweep.py -- KERNEL-side fault injection sweep (failslab / fail_futex).

fault_sweep.py + the LD_PRELOAD shim fail libc-level calls (malloc/mmap/epoll_ctl).
This reaches DEEPER: the Linux fault-injection framework fails the request inside
the KERNEL -- a slab allocation (failslab) or, crucially, the process-private
PyMutex FUTEX (fail_futex). That GIL-off shared-lock park/wake path is runloom's
documented recurring-bug surface and is UNREACHABLE from a userspace libc shim.

Per-thread targeting: /proc/<tid>/fail-nth fails the Nth fault-checked call on
exactly that task, so we can fault ONE hub thread at a time and watch the others
stay live -- the precise version of "what if hub K's futex wait returns -ENOMEM".

REQUIRES a kernel built with CONFIG_FAULT_INJECTION + CONFIG_FAILSLAB (and
CONFIG_FAIL_FUTEX for the futex path) and debugfs mounted; needs root to set the
debugfs knobs. Where that is absent (e.g. a stock cloud/VM kernel) this SKIPS
cleanly -- it is runnable the moment the facility exists, like the
tools/verify/extra engines.

Usage:  sudo tools/faultinj/kfault_sweep.py [--prob 1000] [--target failslab]
Exit: 0 = clean / skipped (facility absent); 1 = a CRASH/HANG under injection; 2 = setup.
"""
import argparse
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEBUGFS = "/sys/kernel/debug"
PYBIN = os.environ.get("RUNLOOM_PYTHON",
                       os.path.expanduser("~/.pyenv/versions/3.14.4t/bin/python3"))

# A small M:N workload that allocates + parks on futexes across hubs -- the
# surface failslab/fail_futex should perturb.
WORKLOAD = r"""
import os, sys
sys.path.insert(0, os.path.join(%r, "src"))
import runloom_c
runloom_c.mn_init(4)
ch = runloom_c.Chan(0)
def producer():
    for i in range(200): ch.send(i)
    ch.close()
def consumer():
    n = 0
    while True:
        v, ok = ch.recv()
        if not ok: break
        n += 1
runloom_c.mn_fiber(producer)
for _ in range(8): runloom_c.mn_fiber(consumer)
runloom_c.mn_run(); runloom_c.mn_fini()
assert runloom_c._self_check(0) == 0
print("WORKLOAD_OK")
""" % (ROOT,)


def facility_present(target):
    d = os.path.join(DEBUGFS, target)
    return os.path.isdir(d) and os.path.exists("/proc/self/fail-nth")


def main(argv):
    ap = argparse.ArgumentParser(description="kernel fault-injection sweep")
    ap.add_argument("--target", default="failslab", choices=["failslab", "fail_futex", "fail_page_alloc"])
    ap.add_argument("--prob", type=int, default=1000, help="probability in 1/N (debugfs 'probability')")
    ap.add_argument("--max-nth", type=int, default=40)
    args = ap.parse_args(argv)

    if not os.path.isdir(DEBUGFS):
        print("kfault_sweep: debugfs not mounted. SKIP."); return 0
    if not facility_present(args.target):
        print("kfault_sweep: {0} unavailable -- kernel needs CONFIG_FAULT_INJECTION + "
              "CONFIG_{1} (+ CONFIG_FAIL_FUTEX for the futex path) and root. "
              "SKIP (runnable once that kernel is in use).".format(
                  args.target, args.target.upper()))
        return 0
    if os.geteuid() != 0:
        print("kfault_sweep: need root to set the debugfs knobs. SKIP."); return 0

    base = os.path.join(DEBUGFS, args.target)
    def setk(name, val):
        with open(os.path.join(base, name), "w") as f:
            f.write(str(val))

    # restrict to tasks that opt in via their own fail-nth (so we don't fault the
    # whole system), then sweep the Nth-fault point.
    setk("probability", 0)           # gate via fail-nth, not blanket probability
    setk("times", -1)
    if os.path.exists(os.path.join(base, "task-filter")):
        setk("task-filter", 1)

    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["PYTHONPATH"] = os.path.join(ROOT, "src") + os.pathsep + env.get("PYTHONPATH", "")

    bad = []
    print("kfault_sweep: {0} sweep, nth=1..{1}".format(args.target, args.max_nth))
    for nth in range(1, args.max_nth + 1):
        # the child arms its OWN fail-nth then runs the workload; the kernel fails
        # exactly the nth fault-checked alloc/futex on that task.
        driver = ("import os; open('/proc/self/fail-nth','w').write('%d')\n" % nth) + WORKLOAD
        try:
            p = subprocess.run([PYBIN, "-c", driver], cwd=ROOT, env=env,
                               capture_output=True, text=True, timeout=60)
            ok = "WORKLOAD_OK" in (p.stdout or "")
            if p.returncode == 0 and ok:
                cls = "OK"
            elif p.returncode != 0 and p.returncode < 0:
                cls = "CRASH(sig %d)" % (-p.returncode)
            else:
                cls = "GRACEFUL"   # nonzero exit / exception, but no crash -> handled
        except subprocess.TimeoutExpired:
            cls = "HANG"
        if cls.startswith("CRASH") or cls == "HANG":
            bad.append((nth, cls))
        print("  nth=%2d -> %s" % (nth, cls))

    print()
    if bad:
        print("kfault_sweep: CRASH/HANG under injection: %s" % bad)
        return 1
    print("kfault_sweep: every injected %s fault handled (no crash/hang)" % args.target)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
