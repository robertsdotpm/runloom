#!/usr/bin/env python3
"""demo.py -- controlled M:N scheduler (PYGO_MN_SEED), the working case.

Runs a small cross-hub channel workload under the execution baton and shows
that different seeds explore different cross-hub interleavings while every run
stays conservation-clean. This is the part that works; see README.md for the
two open gaps (not-yet-deterministic replay; intermittent deadlock on complex
select+coordinator workloads).

Each subprocess: 3 hubs, several goroutines that recv from a shared channel and
record their (hub-influenced) order; the producer sends 2*m values then closes.
The receive order varies with the seed (serialized, seed-chosen handoff).

House style: .format(), no f-strings.
"""
import os
import subprocess
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))


def run(seed, m=6, hubs=3):
    code = (
        "import sys; sys.path.insert(0, 'src'); import pygo_core\n"
        "m = {}; hubs = {}\n".format(m, hubs) +
        "pygo_core.mn_init(hubs)\n"
        "ch = pygo_core.Chan(); got = []\n"
        "def receiver(rid):\n"
        "    while True:\n"
        "        v, ok = ch.recv()\n"
        "        if not ok: break\n"
        "        got.append((rid, v))\n"
        "for r in range(m):\n"
        "    pygo_core.mn_go(lambda r=r: receiver(r))\n"
        "def producer():\n"
        "    for v in range(m*2): ch.send(v)\n"
        "    ch.close()\n"
        "pygo_core.mn_go(producer)\n"
        "pygo_core.mn_run(); pygo_core.mn_fini()\n"
        "sig = ''.join(str(r) for r, _ in got)\n"
        "vals = sorted(v for _, v in got)\n"
        "print(sig + '|' + ('OK' if vals == list(range(m*2)) else 'LOST'))\n"
    )
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["PYTHONPATH"] = os.path.join(ROOT, "src")
    if seed is not None:
        env["PYGO_MN_SEED"] = str(seed)
    out = subprocess.run([sys.executable, "-c", code], env=env, cwd=ROOT,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30)
    line = out.stdout.decode(errors="replace").strip().splitlines()
    return line[-1] if line else "ERR"


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 16
    print("controlled M:N demo: 3 hubs, cross-hub channel drain")
    base = run(None)
    print("  free-running (no PYGO_MN_SEED): {}".format(base))
    seen, lost = {}, 0
    for s in range(1, n + 1):
        sig, _, status = run(s).partition("|")
        seen[sig] = seen.get(sig, 0) + 1
        if status != "OK":
            lost += 1
            print("  seed {:>3}: CONSERVATION VIOLATION ({})".format(s, sig))
    print("-" * 56)
    print("  {} seeds -> {} distinct cross-hub interleavings".format(n, len(seen)))
    print("  conservation violations: {}".format(lost))
    print("  (note: this is seeded EXPLORATION, not deterministic replay -- see README)")
    return 1 if lost else 0


if __name__ == "__main__":
    sys.exit(main())
