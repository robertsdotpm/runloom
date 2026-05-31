#!/usr/bin/env python3
"""Run the pygo test suite one FILE per subprocess.

The in-process pytest run is order- and load-sensitive: a file that leaves a
hub thread running, leaks a parker, or SIGSEGVs under contention can wedge or
poison every file collected after it, turning a single real bug into a "the
suite hangs sometimes" mystery (and masking which file was at fault).  This
runner gives each test FILE its own fresh interpreter, so:

  * global runtime state (per-thread schedulers, the shared netpoll, hub
    threads) cannot leak across files;
  * a hang becomes a per-file timeout (reported as TIMEOUT, rc=124) instead
    of a dead run;
  * a crash (SIGSEGV) is contained to its own file and reported, not fatal to
    the rest of the suite.

It mirrors what test_mn.py already does at the snippet level, lifted to the
whole directory.  The per-test invariant checks in conftest.py still run
inside each subprocess.

Usage:
  tests/run_isolated.py                 # every tests/test_*.py
  tests/run_isolated.py test_aio_net.py test_chan.py
  tests/run_isolated.py -k cancel       # pass-through pytest args after files
  PYGO_TEST_TIMEOUT=600 tests/run_isolated.py

Exit status is non-zero if any file failed, timed out, or crashed.
"""
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)

# Per-file wall-clock ceiling.  A hang (lost wake / un-interruptible park)
# trips this and is reported as TIMEOUT rather than blocking the whole run.
DEFAULT_TIMEOUT = int(os.environ.get("PYGO_TEST_TIMEOUT", "300"))

# Files that need a longer ceiling (soak / stress spin many goroutines).
SLOW_FILES = {
    "test_soak.py": 900,
    "test_stress.py": 600,
    "test_mn.py": 600,
    "test_workloads.py": 600,
}


def discover():
    out = []
    for name in sorted(os.listdir(HERE)):
        if name.startswith("test_") and name.endswith(".py"):
            out.append(name)
    return out


def run_file(name, pytest_args):
    path = os.path.join(HERE, name)
    timeout = SLOW_FILES.get(name, DEFAULT_TIMEOUT)
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["PYGO_GIL"] = "0"
    # Keep the in-tree .so importable regardless of how the runner was invoked.
    src = os.path.join(REPO, "src")
    env["PYTHONPATH"] = src + os.pathsep + env.get("PYTHONPATH", "")
    cmd = [sys.executable, "-m", "pytest", path, "-q",
           "-p", "no:cacheprovider"] + list(pytest_args)
    t0 = time.monotonic()
    try:
        p = subprocess.run(cmd, cwd=REPO, env=env, timeout=timeout,
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                           text=True)
        rc, out = p.returncode, p.stdout
    except subprocess.TimeoutExpired as e:
        out = (e.stdout or "")
        if isinstance(out, bytes):
            out = out.decode("utf-8", "replace")
        rc = 124
        out += "\n[run_isolated: TIMED OUT after {0}s]".format(timeout)
    return rc, out, time.monotonic() - t0


def classify(rc):
    if rc == 0:
        return "PASS"
    if rc == 124:
        return "TIMEOUT"
    if rc < 0:
        return "CRASH(sig{0})".format(-rc)
    return "FAIL"


def main(argv):
    # Split argv into (file names that exist in tests/) and (pytest pass-through).
    files, passthru = [], []
    known = set(discover())
    for a in argv:
        base = os.path.basename(a)
        if base in known:
            files.append(base)
        else:
            passthru.append(a)
    if not files:
        files = discover()

    print("== pygo isolated suite: {0} file(s), {1} ==".format(
        len(files), sys.executable))
    results = []
    for name in files:
        sys.stdout.write("  {0:<28} ".format(name))
        sys.stdout.flush()
        rc, out, dt = run_file(name, passthru)
        verdict = classify(rc)
        results.append((name, verdict, rc, out, dt))
        # Pull pytest's own summary tail line for a one-liner.
        summary = ""
        for line in reversed(out.splitlines()):
            ls = line.strip()
            if ls and ("passed" in ls or "failed" in ls or "error" in ls
                       or "no tests ran" in ls or "TIMED OUT" in ls):
                summary = ls.strip("= ")
                break
        print("{0:<12} {1:6.1f}s  {2}".format(verdict, dt, summary))
        # Surface conftest leak reports even when the file passed (report
        # mode does not fail the test, so the tail-on-failure path misses them).
        for line in out.splitlines():
            if "[pygo-leak]" in line:
                print("      {0}".format(line.strip()))

    bad = [r for r in results if r[1] != "PASS"]
    print("-" * 60)
    if bad:
        print("FAILURES ({0}):".format(len(bad)))
        for name, verdict, rc, out, dt in bad:
            print("\n### {0}  [{1}]".format(name, verdict))
            tail = out.splitlines()[-30:]
            print("\n".join(tail))
    npass = len(results) - len(bad)
    print("\n== {0} passed, {1} not-passed ({2}) ==".format(
        npass, len(bad), ", ".join(r[0] for r in bad) if bad else "all green"))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
