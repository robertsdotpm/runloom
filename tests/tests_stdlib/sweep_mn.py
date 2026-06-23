#!/usr/bin/env python3
"""Drive the vendored CPython stdlib test corpus through runloom's M:N scheduler,
one module per subprocess, and classify the outcome of each.

Each module is run by ``run_one_mn.py`` in its own free-threaded child process
(see that file for the goroutine/M:N mechanics).  This driver:

  * discovers every ``test_*.py`` under tests_stdlib/test, including the ones
    nested in subpackages (test_asyncio/, test_importlib/, ...), as dotted
    module names;
  * runs each child with a wall-clock timeout, in a small parallel pool;
  * classifies the result into one of:
        PASS    - suite ran and wasSuccessful()
        FAIL    - suite ran but had unittest failures/errors (semantic; may
                  include env collisions when -j > 1)
        LOADERR - module raised before/while loading (import error, missing dep)
        CRASH   - child killed by a signal (SIGSEGV/SIGABRT/...) -- the gold:
                  a real runloom scheduler/coroutine bug
        HANG    - child hit the timeout (lost wake / deadlock) -- also gold
        ERROR   - child exited non-zero without a signal
  * saves the full child stderr for every non-PASS module under results/<STATUS>/
    (verbosity=2, so the last line before a CRASH names the executing test);
  * writes results.csv and prints a summary, calling out CRASH/HANG up front.

Usage:
    sweep_mn.py                          # full sweep, all discovered modules
    sweep_mn.py test.test_heapq ...      # only the named modules (smoke test)
    sweep_mn.py --list                   # print discovered module names, exit
    sweep_mn.py --jobs 8 --hubs 4 --timeout 180
"""
import argparse
import csv
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

HERE = os.path.dirname(os.path.abspath(__file__))      # tests_stdlib/
REPO = os.path.dirname(HERE)
TESTROOT = os.path.join(HERE, "test")                  # vendored `test` package
RESULTS = os.environ.get("RUNLOOM_SWEEP_RESULTS", os.path.join(HERE, "results"))

# Signal-number -> name, for readable CRASH labels (child rc is -signum).
SIGNAMES = {6: "SIGABRT", 4: "SIGILL", 7: "SIGBUS", 8: "SIGFPE",
            11: "SIGSEGV", 5: "SIGTRAP", 9: "SIGKILL", 10: "SIGUSR1"}


def discover(testroot):
    """Every test_*.py reachable as an importable module, dotted from `test`.

    Only descends into directories that are real packages (have __init__.py);
    data dirs (decimaltestdata/, certs, ...) are skipped.  A subpackage like
    test_asyncio is reached only through its submodules, never as a bare name,
    so its aggregating __init__ is not run twice.
    """
    parent = os.path.dirname(testroot)                 # tests_stdlib/
    mods = []
    for dirpath, dirnames, filenames in os.walk(testroot):
        dirnames[:] = sorted(d for d in dirnames if d != "__pycache__")
        if dirpath != testroot and "__init__.py" not in filenames:
            dirnames[:] = []                           # not a package: prune
            continue
        pkg = os.path.relpath(dirpath, parent).replace(os.sep, ".")
        for fn in sorted(filenames):
            if fn.startswith("test_") and fn.endswith(".py"):
                mods.append(pkg + "." + fn[:-3])
    return sorted(mods)


def classify(rc, timed_out, result_line):
    """Map (returncode, timeout?, parsed RESULT line) -> status string."""
    if timed_out:
        return "HANG"
    if rc is not None and rc < 0:
        return "CRASH"
    if rc not in (0, None) and rc >= 0:
        # Non-zero clean exit with no RESULT line is an ERROR; with one we still
        # trust the parsed verdict below.
        if not result_line:
            return "ERROR"
    if result_line is None:
        return "ERROR"
    fields = parse_result(result_line)
    if fields.get("exc") not in (None, "None"):
        return "LOADERR"
    if fields.get("ok") == "True":
        return "PASS"
    return "FAIL"


def parse_result(line):
    out = {}
    for tok in line.split():
        if "=" in tok:
            k, _, v = tok.partition("=")
            out[k] = v
    return out


def run_module(mod, hubs, timeout, stack=0):
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"        # real free-threading: hubs run in parallel
    env["PYGO_GIL"] = "0"
    env.setdefault("PYTHONUNBUFFERED", "1")
    if stack > 0:
        env["RUNLOOM_MN_STACK"] = str(stack)
    cmd = [sys.executable, os.path.join(HERE, "run_one_mn.py"), mod, str(hubs)]
    # Run each child in a throwaway cwd: stdlib tests scatter temp files /
    # `tempcwd/` into the working dir, which would otherwise pollute the repo.
    # run_one_mn resolves its paths from __file__, so cwd is irrelevant to it.
    workdir = tempfile.mkdtemp(prefix="runloom_sweep_")
    t0 = time.time()
    timed_out = False
    try:
        p = subprocess.run(cmd, cwd=workdir, env=env, timeout=timeout,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           text=True)
        rc, out, err = p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired as e:
        timed_out = True
        rc = None
        out = e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or "")
        err = e.stderr.decode() if isinstance(e.stderr, bytes) else (e.stderr or "")
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
    elapsed = time.time() - t0

    # Find the RESULT token anywhere (a test may have printed to stdout without
    # a trailing newline, leaving RESULT mid-line); take the last occurrence.
    result_line = None
    idx = out.rfind("RESULT module=")
    if idx != -1:
        result_line = out[idx:].splitlines()[0]
    status = classify(rc, timed_out, result_line)
    fields = parse_result(result_line) if result_line else {}

    detail = ""
    if status == "CRASH":
        detail = SIGNAMES.get(-rc, "sig%d" % -rc)
    return {
        "module": mod, "status": status, "rc": rc, "elapsed": round(elapsed, 1),
        "detail": detail,
        "ran": fields.get("ran", ""), "fail": fields.get("fail", ""),
        "err": fields.get("err", ""), "skip": fields.get("skip", ""),
        "mn_rc": fields.get("mn_rc", ""),
        "stdout": out, "stderr": err,
    }


def save_log(res):
    if res["status"] == "PASS":
        return
    d = os.path.join(RESULTS, res["status"])
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, res["module"] + ".log")
    with open(path, "w") as f:
        f.write("# module=%s status=%s rc=%s detail=%s elapsed=%ss\n"
                % (res["module"], res["status"], res["rc"], res["detail"],
                   res["elapsed"]))
        f.write("# --- child stdout ---\n")
        f.write(res["stdout"])
        f.write("\n# --- child stderr (verbosity=2; last line before a CRASH "
                "names the executing test) ---\n")
        f.write(res["stderr"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("modules", nargs="*", help="explicit module names (default: discover all)")
    ap.add_argument("--jobs", "-j", type=int, default=8)
    ap.add_argument("--hubs", type=int, default=4)
    ap.add_argument("--timeout", type=int, default=180)
    ap.add_argument("--stack", type=int, default=8 * 1024 * 1024,
                    help="M:N goroutine stack size in bytes (default 8 MB, enough "
                         "for deep stdlib C bursts; pass 0 for the raw 128 KB "
                         "baseline that reproduces the stack-overflow crashes)")
    ap.add_argument("--list", action="store_true", help="print discovered modules and exit")
    args = ap.parse_args()

    mods = args.modules or discover(TESTROOT)
    if args.list:
        for m in mods:
            print(m)
        print("# %d modules" % len(mods), file=sys.stderr)
        return 0

    os.makedirs(RESULTS, exist_ok=True)
    csv_path = os.path.join(RESULTS, "results.csv")
    counts = {}
    crashes, hangs = [], []
    lock = threading.Lock()
    done = 0
    total = len(mods)
    t_start = time.time()

    print("sweep: %d modules, jobs=%d hubs=%d timeout=%ds stack=%s -> %s"
          % (total, args.jobs, args.hubs, args.timeout,
             args.stack or "128K(default)", RESULTS), flush=True)

    with open(csv_path, "w", newline="") as cf:
        writer = csv.writer(cf)
        writer.writerow(["module", "status", "detail", "rc", "elapsed",
                         "ran", "fail", "err", "skip", "mn_rc"])
        with ThreadPoolExecutor(max_workers=args.jobs) as ex:
            futs = {ex.submit(run_module, m, args.hubs, args.timeout, args.stack): m for m in mods}
            for fut in as_completed(futs):
                res = fut.result()
                with lock:
                    done += 1
                    counts[res["status"]] = counts.get(res["status"], 0) + 1
                    save_log(res)
                    writer.writerow([res["module"], res["status"], res["detail"],
                                     res["rc"], res["elapsed"], res["ran"],
                                     res["fail"], res["err"], res["skip"],
                                     res["mn_rc"]])
                    cf.flush()
                    tag = res["status"]
                    if tag == "CRASH":
                        crashes.append((res["module"], res["detail"]))
                        tag = "CRASH(%s)" % res["detail"]
                    elif tag == "HANG":
                        hangs.append(res["module"])
                    if res["status"] != "PASS":
                        print("[%4d/%4d] %-9s %s  (%ss)"
                              % (done, total, tag, res["module"], res["elapsed"]),
                              flush=True)

    dt = time.time() - t_start
    print("\n==== SWEEP DONE in %.0fs ====" % dt, flush=True)
    for k in sorted(counts):
        print("  %-8s %d" % (k, counts[k]))
    if crashes:
        print("\n-- CRASH (%d) --" % len(crashes))
        for m, d in sorted(crashes):
            print("  %-8s %s" % (d, m))
    if hangs:
        print("\n-- HANG (%d) --" % len(hangs))
        for m in sorted(hangs):
            print("  %s" % m)
    print("\nresults.csv + per-module logs under %s" % RESULTS)
    return 0


if __name__ == "__main__":
    sys.exit(main())
