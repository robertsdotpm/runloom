"""Single-fault-injection sweep: fail each Nth allocation/syscall in turn and
classify how runloom's cleanup path reacted.

For N in 1..MAX, run workload.py under faultinj.so with FAULTINJ_NTH=N and
classify the outcome:

  OK        exit 0 -- the injected failure was on a non-critical path or
            handled and the workload still completed.
  GRACEFUL  Python raised (MemoryError / OSError traceback) -- correct.
  CRASH     killed by a signal (SIGSEGV / SIGABRT) -- a cleanup-path BUG.
  HANG      timed out -- a lost-wake / stranded-goroutine BUG on the error path.

CRASH/HANG are the findings.  Default targets are the low-frequency,
runloom-specific syscalls (epoll_ctl/eventfd/timerfd/mmap) where the signal is
cleanest; malloc/calloc/realloc are available but noisier (CPython itself
churns millions of allocations and may Py_FatalError on an early failure,
which is a CPython limitation, not a runloom bug).

Usage:  tools/fault_sweep.py [targets] [maxN]
        tools/fault_sweep.py epoll_ctl,eventfd 40
        PYTHON=/path/to/py3.13t tools/fault_sweep.py
"""
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SO = os.path.join(HERE, "faultinj", "faultinj.so")
WORKLOAD = os.path.join(HERE, "faultinj", "workload.py")
PY = os.environ.get("PYTHON", sys.executable)
DEFAULT_TARGETS = ["epoll_ctl", "eventfd", "timerfd", "mmap"]


def run_one(target, nth, timeout=25):
    env = dict(os.environ)
    env["LD_PRELOAD"] = SO
    env["FAULTINJ_TARGET"] = target
    env["FAULTINJ_NTH"] = str(nth)
    env["FAULTINJ_VERBOSE"] = "1"   # so we can tell "injected" from "too few calls"
    env["PYTHON_GIL"] = "0"
    try:
        p = subprocess.run([PY, WORKLOAD], env=env, timeout=timeout,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.TimeoutExpired:
        return "HANG", ""
    rc = p.returncode
    err = p.stderr.decode("utf-8", "replace")
    injected = "[faultinj] inject" in err
    if not injected:
        # the workload made fewer than `nth` calls to this target: no fault
        # was actually injected, so this N tells us nothing.
        return "NOINJECT", ""
    if rc < 0:
        return "CRASH(sig{0})".format(-rc), err[-500:]
    if rc == 0:
        return "OK", ""
    if "Traceback" in err or "Error" in err:
        return "GRACEFUL", ""
    return "EXIT({0})".format(rc), err[-500:]


def sweep(target, maxn):
    print("== fault sweep: target={0}  N=1..{1} ==".format(target, maxn))
    findings = []
    counts = {}
    for n in range(1, maxn + 1):
        status, detail = run_one(target, n)
        key = status.split("(")[0]
        counts[key] = counts.get(key, 0) + 1
        if key in ("CRASH", "HANG"):
            findings.append((target, n, status, detail))
            print("  N={0:<4d} {1}".format(n, status))
            if detail:
                print("    " + detail.replace("\n", "\n    ")[:700])
    injected = sum(v for k, v in counts.items() if k != "NOINJECT")
    print("  summary: " + ", ".join("{0}={1}".format(k, v) for k, v in sorted(counts.items())))
    print("  injected on {0} of {1} runs (rest: workload made fewer calls)".format(injected, maxn))
    if findings:
        print("  >>> {0} CLEANUP-PATH BUG(S) on this target".format(len(findings)))
    elif injected:
        print("  >>> all {0} injected failures handled gracefully (no crash/hang)".format(injected))
    else:
        print("  >>> target never reached -- raise maxN or pick a hotter target")
    return findings


def main():
    targets = sys.argv[1].split(",") if len(sys.argv) > 1 else DEFAULT_TARGETS
    maxn = int(sys.argv[2]) if len(sys.argv) > 2 else 60
    print("[fault_sweep] python: {0}".format(PY))
    all_findings = []
    for t in targets:
        all_findings += sweep(t, maxn)
        print()
    if all_findings:
        print("TOTAL FINDINGS: {0}".format(len(all_findings)))
        for target, n, status, detail in all_findings:
            print("  {0} N={1} -> {2}".format(target, n, status))
        return 1
    print("TOTAL FINDINGS: 0 -- every injected failure was handled gracefully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
