"""big_100 / 26 -- subprocess echo farm.

A pool of goroutines each repeatedly spawns a `cat` subprocess, pipes a random
payload through it, and verifies the echo comes back byte-for-byte.  All the
pipe draining + child reaping is made cooperative by the monkey layer
(communicate() -> selectors/os offload, wait() -> pidfd).

Stresses: Popen, pipes, communicate, child reaping, fd churn.
"""
import subprocess

import procutil

import harness


def worker(H, wid, rng, state):
    H.sleep(rng.random() * 0.5)
    while H.running():
        payload = rng.randbytes(rng.randint(16, 4096))
        try:
            proc = procutil.popen(["cat"], stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    running=H.running)
            out, _ = proc.communicate(payload)
            if not H.check(out == payload,
                           "cat echo mismatch wid={0}: {1} != {2} bytes"
                           .format(wid, len(out), len(payload))):
                return
            if not H.check(proc.returncode == 0,
                           "cat exited {0} wid={1}".format(
                               proc.returncode, wid)):
                return
            H.op(wid)
            H.task_done(wid)
        except OSError as e:
            if not H.running():
                break
            H.fail("subprocess error wid={0}: {1}".format(wid, e))
            return


def body(H):
    H.run_pool(H.funcs, worker, None)


if __name__ == "__main__":
    harness.main("p26_subprocess_echo", body, default_funcs=800,
                 describe="many `cat` subprocesses echo payloads via pipes")
