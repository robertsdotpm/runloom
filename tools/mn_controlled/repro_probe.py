#!/usr/bin/env python3
"""repro_probe.py -- measure deterministic-replay stability of the controlled
M:N scheduler.  For each seed, run the SAME workload K times in fresh
subprocesses and report whether the cross-hub receive-order signature is
identical every time (the deterministic-replay target) or varies (residual
scheduling nondeterminism).

This is the objective yardstick for the barrier-rendezvous work: a seed is
"stable" iff all K runs produce one signature.  House style: .format().

Usage: repro_probe.py [seeds] [reps]   (defaults: 8 seeds, 6 reps each)
Env passthrough: RUNLOOM_MN_BARRIER, RUNLOOM_MN_TRACE, etc. flow to the children.
"""
import os
import subprocess
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))


def run_once(seed, m=6, hubs=3):
    code = (
        "import sys; sys.path.insert(0, 'src'); import runloom_c\n"
        "m = {}; hubs = {}\n".format(m, hubs) +
        "runloom_c.mn_init(hubs)\n"
        "ch = runloom_c.Chan(); got = []\n"
        "def receiver(rid):\n"
        "    while True:\n"
        "        v, ok = ch.recv()\n"
        "        if not ok: break\n"
        "        got.append((rid, v))\n"
        "for r in range(m):\n"
        "    runloom_c.mn_fiber(lambda r=r: receiver(r))\n"
        "def producer():\n"
        "    for v in range(m*2): ch.send(v)\n"
        "    ch.close()\n"
        "runloom_c.mn_fiber(producer)\n"
        "runloom_c.mn_run(); runloom_c.mn_fini()\n"
        "sig = ''.join(str(r) for r, _ in got)\n"
        "vals = sorted(v for _, v in got)\n"
        "print(sig + '|' + ('OK' if vals == list(range(m*2)) else 'LOST'))\n"
    )
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["PYTHONPATH"] = os.path.join(ROOT, "src")
    env["RUNLOOM_MN_SEED"] = str(seed)
    try:
        out = subprocess.run([sys.executable, "-c", code], env=env, cwd=ROOT,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=60)
    except subprocess.TimeoutExpired:
        return "TIMEOUT"
    line = out.stdout.decode(errors="replace").strip().splitlines()
    sig, sep, status = (line[-1] if line else "ERR").partition("|")
    # A crash / lost-conservation run is an error, not a determinism datum:
    # tag it so it is reported separately rather than as a "distinct signature".
    if not line or sep == "" or status != "OK":
        return "ERR(" + sig + ")"
    return sig


def main():
    seeds = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    reps = int(sys.argv[2]) if len(sys.argv) > 2 else 6
    barrier = os.environ.get("RUNLOOM_MN_BARRIER", "(unset)")
    print("repro probe: {} seeds x {} reps  [RUNLOOM_MN_BARRIER={}]".format(seeds, reps, barrier))
    stable = 0
    for s in range(1, seeds + 1):
        sigs = [run_once(s) for _ in range(reps)]
        uniq = sorted(set(sigs))
        ok = len(uniq) == 1
        stable += ok
        mark = "STABLE  " if ok else "VARIES  "
        if ok:
            print("  seed {:>3}: {} {}".format(s, mark, uniq[0]))
        else:
            print("  seed {:>3}: {} {} distinct: {}".format(s, mark, len(uniq), uniq))
    print("-" * 60)
    print("  {}/{} seeds reproduce identically across {} reps".format(stable, seeds, reps))
    return 0 if stable == seeds else 1


if __name__ == "__main__":
    sys.exit(main())
