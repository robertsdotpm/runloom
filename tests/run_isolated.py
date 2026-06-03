#!/usr/bin/env python3
"""Run the runloom test suite one FILE per subprocess.

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
  RUNLOOM_TEST_TIMEOUT=600 tests/run_isolated.py

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
DEFAULT_TIMEOUT = int(os.environ.get("RUNLOOM_TEST_TIMEOUT", "300"))

# Files that need a longer ceiling (soak / stress spin many goroutines).
SLOW_FILES = {
    "test_soak.py": 900,
    "test_stress.py": 600,
    "test_mn.py": 600,
    "test_workloads.py": 600,
}

# Files that assert tight wall-clock UPPER bounds to *prove* cooperative
# overlap (e.g. "two 50ms waits finish in <90ms").  Those bounds are real
# correctness checks but they flake if the scheduler thread is starved of a
# CPU, so these run in a dedicated serial lane -- never overlapping each other
# or the parallel pool -- regardless of -j.  (Identified by grepping for
# assertLess on an elapsed/wall delta, plus the join-timing test that TSan
# slowdown tripped.)  Everything else is embarrassingly parallel: each file
# already gets its own interpreter + scheduler + ephemeral ports.
SERIAL_FILES = frozenset({
    "test_blocking.py",
    "test_aio.py",
    "test_context.py",
    "test_monkey.py",
    "test_time.py",
    "test_sched_fairness.py",
    "test_selectors_compat.py",
    "test_scheduler_channel_compat.py",
    "test_threading_compat.py",
    "test_process_compat.py",
})

# Default parallel workers for the non-timing files.  Capped well under the
# core count so the timing lane (and each pooled file's own threads) never
# oversubscribe.  Override with -jN / --jobs N or RUNLOOM_TEST_JOBS.
def _default_jobs():
    try:
        env = os.environ.get("RUNLOOM_TEST_JOBS")
        if env:
            return max(1, int(env))
        return max(1, min(8, (os.cpu_count() or 4)))
    except ValueError:
        return 4


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
    env["RUNLOOM_GIL"] = "0"
    # Skip pytest's third-party plugin autoload.  ~20 unrelated plugins
    # (codspeed/sanic/aiohttp/faker/hypothesis/...) are installed here and
    # pytest imports every one of them per process -- ~4s of pure overhead per
    # file, and one of them pulls _brotli which RE-ENABLES the GIL (wrong for
    # the free-threaded target).  The suite uses none of them.  Opt back in
    # with RUNLOOM_TEST_PYTEST_PLUGINS=1 if a test ever needs one.
    if os.environ.get("RUNLOOM_TEST_PYTEST_PLUGINS") != "1":
        env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
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


def _summary_line(out):
    for line in reversed(out.splitlines()):
        ls = line.strip()
        if ls and ("passed" in ls or "failed" in ls or "error" in ls
                   or "no tests ran" in ls or "TIMED OUT" in ls):
            return ls.strip("= ")
    return ""


def main(argv):
    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Split argv into (file names that exist in tests/), -j/--jobs, and
    # pytest pass-through.
    files, passthru = [], []
    known = set(discover())
    jobs = _default_jobs()
    it = iter(argv)
    for a in it:
        if a in ("-j", "--jobs"):
            jobs = max(1, int(next(it)))
            continue
        if a.startswith("-j") and a[2:].isdigit():
            jobs = max(1, int(a[2:]))
            continue
        base = os.path.basename(a)
        if base in known:
            files.append(base)
        else:
            passthru.append(a)
    if not files:
        files = discover()

    parallel = [f for f in files if f not in SERIAL_FILES]
    serial   = [f for f in files if f in SERIAL_FILES]
    jobs = min(jobs, len(parallel)) or 1

    print("== runloom isolated suite: {0} file(s), j={1} parallel + {2} serial, "
          "{3} ==".format(len(files), jobs, len(serial), sys.executable))

    results = []
    print_lock = threading.Lock()

    def record(name, rc, out, dt):
        verdict = classify(rc)
        with print_lock:
            results.append((name, verdict, rc, out, dt))
            print("  {0:<28} {1:<12} {2:6.1f}s  {3}".format(
                name, verdict, dt, _summary_line(out)))
            # Surface conftest leak reports even when the file passed (report
            # mode does not fail, so the tail-on-failure path misses them).
            for line in out.splitlines():
                if "[runloom-leak]" in line:
                    print("      {0}".format(line.strip()))
            sys.stdout.flush()

    # Phase 1: the embarrassingly-parallel bulk.
    if parallel:
        with ThreadPoolExecutor(max_workers=jobs) as pool:
            futs = {pool.submit(run_file, n, passthru): n for n in parallel}
            for fut in as_completed(futs):
                name = futs[fut]
                rc, out, dt = fut.result()
                record(name, rc, out, dt)

    # Phase 2: timing-sensitive files, strictly one at a time so the
    # scheduler thread is never starved of a CPU.
    for name in serial:
        rc, out, dt = run_file(name, passthru)
        record(name, rc, out, dt)

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
