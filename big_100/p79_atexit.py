"""big_100 / 79 -- atexit shutdown test.

Each iteration launches a child that runs a small runloom program: it registers
an atexit handler, spawns background goroutines, lets them wind down, and exits.
The parent verifies the child printed both its main-exit marker AND its atexit
marker and exited 0 -- i.e. interpreter shutdown ran the atexit handlers in the
right order even with runloom's hub threads in the picture.

Stresses: interpreter shutdown order, atexit under the M:N runtime.
"""
import os
import subprocess

import harness
import procutil
import runloom

CHILD = r'''
import sys
sys.path.insert(0, {src!r})
import atexit
import runloom
atexit.register(lambda: (sys.stdout.write("ATEXIT-RAN\n"), sys.stdout.flush()))
flag = [True]
def w():
    n = 0
    while flag[0] and n < 800:
        runloom.sleep(0.001); n += 1
def main():
    for _ in range(24):
        runloom.go(w)
    runloom.sleep(0.03)
    flag[0] = False                 # let the background goroutines wind down
runloom.run(4, main)
sys.stdout.write("MAIN-EXIT\n"); sys.stdout.flush()
'''


def setup(H):
    import sys
    src = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "src")
    script = os.path.join(H.make_tmpdir("big100_atexit_"), "child.py")
    with open(script, "w") as f:
        f.write(CHILD.format(src=src))
    H.state = {"py": sys.executable, "script": script}


def worker(H, wid, rng, state):
    while H.running():
        try:
            env = dict(os.environ)
            env["PYTHON_GIL"] = "0"
            proc = procutil.popen([state["py"], state["script"]],
                                  stdout=subprocess.PIPE, env=env,
                                  running=H.running)
        except OSError:
            break
        out, _ = proc.communicate()
        if not H.check(proc.returncode == 0,
                       "child exited {0} wid={1}".format(
                           proc.returncode, wid)):
            return
        if not H.check(b"ATEXIT-RAN" in out and b"MAIN-EXIT" in out,
                       "atexit/main markers missing wid={0}: {1!r}".format(
                           wid, out[:80])):
            return
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


if __name__ == "__main__":
    harness.main("p79_atexit", body, setup=setup, default_funcs=120,
                 describe="child runloom programs run atexit handlers on exit")
