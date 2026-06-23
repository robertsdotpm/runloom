# -*- coding: utf-8 -*-
"""Run every synthetic toy program in an isolated subprocess and classify.

Each program prints "PASS" and exits 0 when runloom is healthy.  A runloom bug
surfaces as:
    FAIL   -- exit 1 / "FAIL:" (wrong outcome -- logic/semantic bug)
    CRASH  -- negative or abnormal exit (segfault/abort -- memory/scheduler bug)
    HANG   -- harness timeout (lost wakeup / deadlock)

Usage:  python synthetic/run_all.py [--jobs N] [--timeout S] [--out results.json]
                                     [--only SUBSTR]
Children run under the free-threaded interpreter with PYTHON_GIL=0 (M:N needs
the GIL off); the runner itself can use any interpreter.
"""
import argparse
import concurrent.futures as cf
import glob
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PY = os.environ.get(
    "RUNLOOM_PY", os.path.expanduser("~/.pyenv/versions/3.13.13t/bin/python3"))


def classify(rc, out):
    if rc == 0 and "PASS" in out:
        return "PASS"
    if rc == 124:
        return "HANG"
    if rc is not None and rc < 0:
        return "CRASH"
    if rc in (139, 134, 135, 136, 133):     # SEGV/ABRT/BUS/... as 128+sig
        return "CRASH"
    if rc == 1 or "FAIL" in out:
        return "FAIL"
    return "ERROR"


def run_one(progdir, timeout):
    main = os.path.join(progdir, "main.py")
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    try:
        p = subprocess.run([PY, main], cwd=HERE, env=env, timeout=timeout,
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                           text=True)
        rc, out = p.returncode, p.stdout
    except subprocess.TimeoutExpired as e:
        rc = 124
        out = (e.output or "") if isinstance(e.output, str) else ""
    tail = "\n".join(out.strip().splitlines()[-4:])
    return {"dir": os.path.basename(progdir), "rc": rc,
            "status": classify(rc, out), "tail": tail}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jobs", type=int, default=8)
    ap.add_argument("--timeout", type=float, default=40.0)
    ap.add_argument("--out", default=os.path.join(HERE, "results.json"))
    ap.add_argument("--only", default=None, help="substring filter on dir name")
    args = ap.parse_args()

    dirs = sorted(d for d in glob.glob(os.path.join(HERE, "[0-9]" * 4 + "__*"))
                  if os.path.isdir(d))
    if args.only:
        dirs = [d for d in dirs if args.only in os.path.basename(d)]
    print("running {0} programs, jobs={1}, timeout={2}s, py={3}".format(
        len(dirs), args.jobs, args.timeout, PY))

    results = []
    counts = {}
    done = 0
    with cf.ThreadPoolExecutor(max_workers=args.jobs) as ex:
        futs = {ex.submit(run_one, d, args.timeout): d for d in dirs}
        for fut in cf.as_completed(futs):
            r = fut.result()
            results.append(r)
            counts[r["status"]] = counts.get(r["status"], 0) + 1
            done += 1
            if r["status"] != "PASS":
                print("  [{0}] {1}".format(r["status"], r["dir"]), flush=True)
            if done % 100 == 0:
                print("  ... {0}/{1} done".format(done, len(dirs)), flush=True)

    results.sort(key=lambda r: r["dir"])
    with open(args.out, "w") as f:
        json.dump({"counts": counts, "results": results}, f, indent=1)

    print("\n==== summary ====")
    for st in ("PASS", "FAIL", "CRASH", "HANG", "ERROR"):
        if st in counts:
            print("  {0:6} {1}".format(st, counts[st]))
    print("  total  {0}".format(len(results)))
    print("results -> {0}".format(args.out))
    nonpass = [r for r in results if r["status"] != "PASS"]
    return 1 if nonpass else 0


if __name__ == "__main__":
    sys.exit(main())
