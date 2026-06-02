#!/usr/bin/env python3
"""pct_explore.py -- drive pygo's PCT (Probabilistic Concurrency Testing) mode.

The runtime supports a controlled single-hub scheduler when PYGO_PCT_SEED is
set: instead of FIFO, the ready-pop runs the highest-priority ready goroutine,
with random priorities + d-1 random "priority change points" that demote a
goroutine mid-run -- giving a probabilistic lower bound on finding any depth-d
bug (Burckhardt et al, ASPLOS 2010).

SCOPE (read this): PCT controls only the single-hub cooperative run() path,
where the ready-pop order *is* the schedule. It does NOT control the M:N hubs
(real parallel OS threads), and it does not reach the aio bridge's call_soon
ordering (a Python-level FIFO queue inside one loop goroutine). Its real
surface is: several goroutines parked on channels/select/sleep in single-hub
mode, whose wake order it permutes. See tools/pct/README.md and QUALITY_CAMPAIGN.md.

Two modes:
  demo  -- a built-in single-hub channel workload; shows how many DISTINCT
           schedules N seeds explore, and checks conservation every run.
  sweep -- run a pytest target under N PCT seeds; report any seed whose run
           FAILS (an order-dependent bug) with the exact repro env.

House style: .format(), no f-strings.
"""
import argparse
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))


def demo_once(seed, m=6):
    """Single-hub: m receivers parked on one channel, producer sends 2m + close.
    Which receiver gets which value depends on parked-wake order -> PCT permutes
    it. Returns (schedule_signature, conserved?)."""
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["PYTHONPATH"] = os.path.join(ROOT, "src")
    if seed is not None:
        env["PYGO_PCT_SEED"] = str(seed)
    code = (
        "import sys; sys.path.insert(0, 'src'); import pygo_core\n"
        "m = {}\n".format(m) +
        "ch = pygo_core.Chan(); got = []\n"
        "def receiver(rid):\n"
        "    while True:\n"
        "        v, ok = ch.recv()\n"
        "        if not ok: break\n"
        "        got.append((rid, v))\n"
        "for r in range(m):\n"
        "    pygo_core.go(lambda r=r: receiver(r))\n"
        "def producer():\n"
        "    for v in range(m*2): ch.send(v)\n"
        "    ch.close()\n"
        "pygo_core.go(producer)\n"
        "pygo_core.run()\n"
        "sig = ''.join(str(r) for r, _ in got)\n"
        "vals = sorted(v for _, v in got)\n"
        "ok = (vals == list(range(m*2)))\n"
        "print(sig + '|' + ('OK' if ok else 'LOST'))\n"
    )
    out = subprocess.run([sys.executable, "-c", code], env=env, cwd=ROOT,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=60)
    line = out.stdout.decode(errors="replace").strip().splitlines()
    if out.returncode != 0 or not line:
        return None, False
    sig, _, status = line[-1].partition("|")
    return sig, (status == "OK")


def cmd_demo(args):
    print("PCT demo: {} receivers on one channel, single hub".format(args.m))
    base_sig, base_ok = demo_once(None, args.m)
    print("  FIFO (no PCT): schedule {}  {}".format(base_sig, "OK" if base_ok else "LOST"))
    seen = {}
    lost = 0
    for s in range(1, args.seeds + 1):
        sig, ok = demo_once(s, args.m)
        if sig is None:
            continue
        seen[sig] = seen.get(sig, 0) + 1
        if not ok:
            lost += 1
            print("  seed {:>3}: CONSERVATION VIOLATION (schedule {})".format(s, sig))
    print("-" * 56)
    print("  {} seeds -> {} DISTINCT schedules explored "
          "(FIFO alone explores 1)".format(args.seeds, len(seen)))
    print("  conservation violations: {}".format(lost))
    return 1 if lost else 0


def cmd_sweep(args):
    print("PCT sweep: {} under {} seeds (depth {})".format(
        args.target, args.seeds, args.depth))
    failures = []
    for s in range(1, args.seeds + 1):
        env = dict(os.environ)
        env["PYTHON_GIL"] = "0"
        env["PYTHONPATH"] = os.path.join(ROOT, "src")
        env["PYGO_PCT_SEED"] = str(s)
        env["PYGO_PCT_DEPTH"] = str(args.depth)
        try:
            p = subprocess.run(
                [sys.executable, "-m", "pytest", args.target, "-q",
                 "-p", "no:cacheprovider"],
                env=env, cwd=ROOT, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, timeout=600)
            ok = p.returncode == 0
        except subprocess.TimeoutExpired:
            ok = False
        sys.stderr.write("  seed {:>3}: {}\n".format(s, "pass" if ok else "FAIL"))
        if not ok:
            failures.append(s)
    print("-" * 56)
    if failures:
        print("{} order-dependent FAILURE(s). Reproduce e.g.:".format(len(failures)))
        print("  PYGO_PCT_SEED={} PYGO_PCT_DEPTH={} PYTHON_GIL=0 PYTHONPATH=src "
              "python -m pytest {}".format(failures[0], args.depth, args.target))
        return 1
    print("all {} seeds passed (no order-dependent failure found at depth {})".format(
        args.seeds, args.depth))
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd")
    d = sub.add_parser("demo", help="built-in single-hub workload")
    d.add_argument("--seeds", type=int, default=50)
    d.add_argument("--m", type=int, default=6)
    d.set_defaults(func=cmd_demo)
    sw = sub.add_parser("sweep", help="run a pytest target under N PCT seeds")
    sw.add_argument("target")
    sw.add_argument("--seeds", type=int, default=30)
    sw.add_argument("--depth", type=int, default=3)
    sw.set_defaults(func=cmd_sweep)
    args = ap.parse_args()
    if not getattr(args, "func", None):
        ap.print_help()
        return 2
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
