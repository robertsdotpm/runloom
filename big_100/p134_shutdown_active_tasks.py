"""big_100 / 134 -- shutdown with active (parked) tasks.

Each iteration launches a CHILD runloom program (a string run via subprocess):
its main() spawns many goroutines that spend the whole run PARKED in recv / a
short-sleep loop (i.e. active, parked tasks), then signals a deterministic
wind-down (flip a stop flag + close the recv sockets so the parked recv wakes)
and RETURNS from runloom.run().  The runtime must tear the scheduler down
deterministically -- mn_run() joins every (now woken) goroutine, mn_fini()
cleans up -- and the process must exit 0 without hanging or segfaulting,
printing DONE-MARKER then MAIN-EXIT.

The parent runs many such children and checks each exits 0 within a timeout and
printed its marker.

IMPORTANT runloom semantic (see the candidate finding in the agent report):
runloom.run()/mn_run() JOINS every goroutine -- it does NOT return on
quiescence and does NOT cancel still-parked goroutines when main() returns.
A child that returns from main() while goroutines are parked FOREVER (recv with
no sender, or sleep(3600)) HANGS run() indefinitely (verified: even a
sleep-only child times out).  So "active tasks at shutdown" is modelled the way
runloom actually supports it: the tasks are parked right up to the last moment,
then main() wakes them (stop flag + socket close) as part of returning, and the
test asserts that THAT teardown is deterministic and crash-free.

Stresses: scheduler teardown with goroutines parked until the last moment,
close-driven wake at shutdown, mn_run/mn_fini shutdown determinism, no
hang/segfault at interpreter shutdown.
"""
import os
import subprocess

import harness
import procutil

# The child program.  main() spawns recv-parked + short-sleep-loop goroutines
# (active, parked tasks), lets them all reach their park point, prints
# DONE-MARKER, then deterministically winds them down: flip stop[0], then close
# every recv socket so the parked recv returns b'' / raises and the goroutine
# exits.  runloom.run() then joins them and returns; the child prints MAIN-EXIT
# and exits 0.
CHILD = r'''
import sys, os, socket
sys.path.insert(0, {src!r})
import runloom
import runloom.monkey
runloom.monkey.patch()                # cooperative socket recv on the hubs

stop = [False]

def parked_recv(sock):
    try:
        while not stop[0]:
            b = sock.recv(1)      # parked here; main closes the peer at exit
            if not b:
                break
    except OSError:
        pass

def parked_sleep():
    # A short-sleep loop guarded by the stop flag: parked on the timer heap
    # almost all the time, but able to wind down within one tick when main
    # flips stop[0] -- mn_run() can then join it (a bare sleep(3600) cannot be
    # cancelled at main-return and would hang run()).
    while not stop[0]:
        runloom.sleep(0.01)

def main():
    socks = []
    for _ in range(40):
        a, b = socket.socketpair()
        a.setblocking(True)
        b.setblocking(True)
        socks.append((a, b))
        runloom.fiber(parked_recv, a)
    for _ in range(40):
        runloom.fiber(parked_sleep)
    runloom.sleep(0.05)           # let all 80 actually reach their park point
    sys.stdout.write("DONE-MARKER\n"); sys.stdout.flush()
    # Deterministic wind-down of the still-parked tasks, then RETURN.
    stop[0] = True
    for a, b in socks:
        try: a.close()
        except OSError: pass
        try: b.close()
        except OSError: pass

runloom.run(4, main)
sys.stdout.write("MAIN-EXIT\n"); sys.stdout.flush()
'''


def setup(H):
    import sys
    src = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "src")
    script = os.path.join(H.make_tmpdir("big100_shutdown_"), "child.py")
    with open(script, "w") as f:
        f.write(CHILD.format(src=src))
    H.state = {"py": sys.executable, "script": script}


def worker(H, wid, rng, state):
    for _ in H.round_range():
        env = dict(os.environ)
        env["PYTHON_GIL"] = "0"
        try:
            proc = procutil.popen([state["py"], state["script"]],
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, env=env,
                                  running=H.running)
        except OSError:
            break
        try:
            out, err = proc.communicate(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.communicate(timeout=10)
            except Exception:
                pass
            H.fail("child HUNG on shutdown-with-parked-tasks wid={0}".format(wid))
            return
        except OSError:
            if not H.running():
                break
            raise
        if not H.check(proc.returncode == 0,
                       "child exited {0} wid={1} (segfault/hang at shutdown?) "
                       "stderr={2!r}".format(
                           proc.returncode, wid, err[-200:])):
            return
        if not H.check(b"DONE-MARKER" in out and b"MAIN-EXIT" in out,
                       "child markers missing wid={0}: {1!r}".format(
                           wid, out[:120])):
            return
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    H.check(H.total_ops() > 0, "no child ran to a clean parked-shutdown exit")
    H.log("clean_child_shutdowns={0} exited={1}/{2}".format(
        H.total_ops(), H.exited, H.expected))


if __name__ == "__main__":
    harness.main("p134_shutdown_active_tasks", body, setup=setup, post=post,
                 default_funcs=120,
                 describe="child runloom returns from run() with 80 goroutines "
                          "still parked; clean deterministic teardown, exit 0")
