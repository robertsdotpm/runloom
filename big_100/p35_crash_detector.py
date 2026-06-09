"""big_100 / 35 -- subprocess crash detector.

Children exit in one of three ways chosen at random: clean (0), a nonzero
status, or killed by SIGABRT.  The parent must classify each outcome by
returncode exactly -- a clean exit, the right nonzero code, or the negative
signal number.

Stresses: error propagation, wait status decoding, reaping.
"""
import signal
import subprocess

import procutil

import harness


def worker(H, wid, rng, state):
    H.sleep(rng.random() * 0.5)
    while H.running():
        pick = rng.random()
        if pick < 0.4:
            cmd = ["sh", "-c", "exit 0"]
            expected = 0
        elif pick < 0.8:
            code = rng.randint(1, 120)
            cmd = ["sh", "-c", "exit {0}".format(code)]
            expected = code
        else:
            cmd = ["sh", "-c", "kill -ABRT $$"]
            expected = -signal.SIGABRT
        try:
            proc = procutil.popen(cmd, running=H.running)
            proc.wait()
            if not H.check(proc.returncode == expected,
                           "misclassified wid={0}: rc={1} expected={2} "
                           "cmd={3}".format(wid, proc.returncode, expected,
                                            cmd[-1])):
                return
            H.op(wid)
            H.task_done(wid)
        except OSError as e:
            if not H.running():
                break
            H.fail("crash-detector error wid={0}: {1}".format(wid, e))
            return


def body(H):
    H.run_pool(H.funcs, worker, None)


if __name__ == "__main__":
    harness.main("p35_crash_detector", body, default_funcs=600,
                 describe="classify child exit: clean / nonzero / SIGABRT")
