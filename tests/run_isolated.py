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
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)

# Per-file wall-clock ceiling.  A hang (lost wake / un-interruptible park)
# trips this and is reported as TIMEOUT rather than blocking the whole run.
DEFAULT_TIMEOUT = int(os.environ.get("RUNLOOM_TEST_TIMEOUT", "300"))

# Global deadline scaler (libuv's UV_TEST_TIMEOUT_MULTIPLIER).  A slow / loaded /
# emulated machine multiplies EVERY per-file ceiling (and the post-SIGABRT grace)
# by this, so "the box was busy" reads as slow rather than a false TIMEOUT --
# without changing any individual timeout.  Default 1; a genuine wedge still trips
# eventually.  Set e.g. RUNLOOM_TIMEOUT_MULT=3 on a contended host.
TIMEOUT_MULT = max(0.01, float(os.environ.get("RUNLOOM_TIMEOUT_MULT", "1")))

# Files that need a longer ceiling (soak / stress spin many fibers).
SLOW_FILES = {
    "test_soak.py": 900,
    "test_stress.py": 600,
    "test_mn.py": 600,
    "test_workloads.py": 600,
    # vendored asyncio conformance (tests/aio/): the big modules
    "test_tasks.py": 400,
    "test_events.py": 400,
    "test_taskgroups.py": 300,
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
    # Timing-sensitive netpoll deadline test (asserts 0.02 <= dt < 2.0 on a
    # ~40ms park) -- green isolated + at low -j, but starves into a false failure
    # in the parallel pool once the worker count is raised (surfaced at j32+).
    "test_cov100_netpoll_small.py",
})

# Default parallel workers for the non-timing files.  Scales with the core
# count but stays in a MEASURED-safe band: each pooled file spawns its own hub
# threads, so too many concurrent files oversubscribe the box and starve the
# timing-sensitive pool files (test_adv_aio, test_cov100_netpoll_small) into
# false failures.  On a 64-core box the tests phase measured 149s @ 8 workers,
# 91s @ 32 (both green), but FLAKED @ 48 and @ 64 -- so the knee is ~cpu/2,
# capped at 32.  Floor at the historical min(8,n) so small boxes are unchanged.
# Override with -jN / --jobs N or RUNLOOM_TEST_JOBS (e.g. =64 on a big idle box).
def _default_jobs():
    try:
        env = os.environ.get("RUNLOOM_TEST_JOBS")
        if env:
            return max(1, int(env))
        n = os.cpu_count() or 4
        return max(1, min(32, max(min(8, n), n // 2)))
    except ValueError:
        return 4


# Subdirectory of tests/ to run (set by --suite).  "" = tests/ itself.  A
# subsuite (e.g. tests/aio/, the vendored asyncio conformance) is discovered and
# run with the SAME per-file subprocess isolation as the top-level suite.
SUITE = ""


def suite_dir():
    return os.path.join(HERE, SUITE) if SUITE else HERE


def discover():
    out = []
    for name in sorted(os.listdir(suite_dir())):
        if name.startswith("test_") and name.endswith(".py"):
            out.append(name)
    return out


def run_file(name, pytest_args):
    path = os.path.join(suite_dir(), name)
    timeout = (SLOW_FILES.get(name, DEFAULT_TIMEOUT)) * TIMEOUT_MULT
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["RUNLOOM_GIL"] = "0"
    # TLBC now stays ON by default: runloom_c's GC frames anchor
    # (module_gcframes.c.inc) makes parked-fiber frames visible to the free-
    # threaded collector, so the specializing interpreter is safe -- the p565/p524
    # crash the old PYTHON_TLBC=0 preset used to avoid is fixed at the source.
    # Running the suite TLBC-on matches production and exercises the anchor under
    # every test.  We no longer preset PYTHON_TLBC=0, and because the anchor is
    # active runloom.run() no longer os.execv's mid-pytest (the old
    # capture-corruption hazard that motivated the preset is gone).  Diagnostic
    # axis preserved: export PYTHON_TLBC=0 (inherited into env via the copy above)
    # to force a TLBC-off run, e.g. for the gc.disable() discriminator or a bisect.
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
    # Enable faulthandler so a hang can be DUMPED (not just killed): on timeout we
    # send SIGABRT, which faulthandler turns into a full all-thread (hub) Python+C
    # traceback to stderr -> captured below.  Turns an opaque 300s TIMEOUT into a
    # diagnosable stack (e.g. which hub is wedged where on a lost netpoll arm).
    env["PYTHONFAULTHANDLER"] = "1"
    cmd = [sys.executable, "-m", "pytest", path, "-v",
           "-p", "no:cacheprovider"] + list(pytest_args)
    t0 = time.monotonic()
    p = subprocess.Popen(cmd, cwd=REPO, env=env,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                         text=True)
    try:
        out, _ = p.communicate(timeout=timeout)
        rc = p.returncode
    except subprocess.TimeoutExpired:
        # Hung.  Extract which test was running (last one mentioned in -v output),
        # SIGABRT to dump stacks, then kill if needed.
        try:
            p.send_signal(signal.SIGABRT)
        except Exception:
            pass
        try:
            out, _ = p.communicate(timeout=15 * TIMEOUT_MULT)
        except subprocess.TimeoutExpired:
            p.kill()
            out, _ = p.communicate()
        rc = 124
        # Find the last test name in the verbose output (the one that hung).
        last_test = "(unknown test)"
        for line in reversed((out or "").splitlines()):
            if "::" in line and not line.startswith("="):
                # Extract test name from lines like:
                # "tests/test_X.py::Class::test_name PASSED"
                # or just "tests/test_X.py::Class::test_name" (still running when hung)
                parts = line.split()
                if parts:
                    last_test = parts[0]
                    break
        out = (out or "") + (
            "\n[run_isolated: TIMED OUT after {0}s on {1}; SIGABRT faulthandler "
            "dump (if any) is above]".format(timeout, last_test))
    return rc, out, time.monotonic() - t0


def classify(rc):
    if rc == 0:
        return "PASS"
    if rc == 5:
        # pytest exit 5 = "no tests collected".  For this suite that means the
        # file skipped at MODULE level (e.g. a Linux-only test on macOS/Windows
        # doing `pytest.skip(..., allow_module_level=True)` -> 0 items collected).
        # That is a skip, not a failure.  (A real collection/import error exits 2,
        # not 5, so this does not mask broken files.)
        return "SKIP"
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

    # Split argv into (file names that exist in the suite), -j/--jobs, and
    # pytest pass-through.  --suite <name> selects a tests/<name>/ subsuite.
    global SUITE
    argv = list(argv)
    if "--suite" in argv:
        i = argv.index("--suite")
        SUITE = argv[i + 1]
        del argv[i:i + 2]
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

    # Phase 3: retry any non-pass ONCE, isolated.  This box is shared (continuous
    # soak loops + sibling worktrees' suites) and the pool adds its own load, so a
    # tight wall-clock assert can be starved into a FALSE failure -- it then
    # passes when re-run alone.  A genuine failure fails again.  Every retry is
    # logged (RECOVERED vs STILL FAILING) so a chronically-flaky file stays
    # visible instead of being silently masked.  Disable with RUNLOOM_TEST_NORETRY=1.
    if os.environ.get("RUNLOOM_TEST_NORETRY") != "1":
        flaky = [i for i, r in enumerate(results) if r[1] not in ("PASS", "SKIP")]
        if flaky:
            print("-" * 60)
            print("retrying {0} non-pass file(s) ISOLATED (load-flake filter):".format(
                len(flaky)))
            for i in flaky:
                name = results[i][0]
                rc, out, dt = run_file(name, passthru)
                v = classify(rc)
                results[i] = (name, v, rc, out, dt)
                with print_lock:
                    print("  retry {0:<28} {1:<10} {2:6.1f}s  {3}".format(
                        name, v, dt,
                        "RECOVERED (load flake)" if v in ("PASS", "SKIP")
                        else "STILL FAILING (real)"))

    bad = [r for r in results if r[1] not in ("PASS", "SKIP")]
    nskip = len([r for r in results if r[1] == "SKIP"])
    print("-" * 60)
    if bad:
        print("FAILURES ({0}):".format(len(bad)))
        for name, verdict, rc, out, dt in bad:
            print("\n### {0}  [{1}]".format(name, verdict))
            tail = out.splitlines()[-30:]
            print("\n".join(tail))
    npass = len(results) - len(bad) - nskip
    print("\n== {0} passed, {1} skipped, {2} not-passed ({3}) ==".format(
        npass, nskip, len(bad), ", ".join(r[0] for r in bad) if bad else "all green"))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
