"""big_100 / 27 -- subprocess timeout killer.

Goroutines spawn commands that sleep effectively forever, wait a random short
time, then kill them and reap.  The kill+reap must always complete (a leaked
zombie or a wait that never returns would stall the goroutine), and other
goroutines must keep running while one waits out its timeout.

Stresses: cancellation/kill, process cleanup, waitpid via pidfd.
"""
import subprocess

import procutil
import time

import harness


def worker(H, wid, rng, state):
    H.sleep(rng.random() * 0.5)
    while H.running():
        try:
            proc = procutil.popen(["sleep", "3600"], running=H.running)
        except OSError as e:
            if not H.running():
                break
            H.fail("spawn failed wid={0}: {1}".format(wid, e))
            return
        timeout = rng.uniform(0.02, 0.4)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and H.running():
            if proc.poll() is not None:
                break
            H.sleep(0.01)
        if proc.poll() is None:
            proc.kill()
        proc.wait()                 # cooperative reap (pidfd)
        if not H.check(proc.returncode is not None,
                       "process not reaped wid={0}".format(wid)):
            return
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, None)


if __name__ == "__main__":
    harness.main("p27_subprocess_timeout", body, default_funcs=600,
                 describe="spawn sleepers, kill after random timeout, reap")
