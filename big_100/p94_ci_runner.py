"""big_100 / 94 -- local CI runner.

A pool of runner goroutines each pull "jobs" and execute them as subprocesses,
collecting output and enforcing a timeout.  Jobs come in three flavours: clean
exit (must produce its marker + rc 0), nonzero exit (must report the right
code), and a runaway (must be killed by the timeout).  Each runner classifies
the outcome and verifies it matches what the job was supposed to do.

Stresses: subprocesses, pipes, scheduling, timeout-kill cancellation.
"""
import subprocess
import time

import harness
import procutil


def run_clean(H):
    proc = procutil.popen(["sh", "-c", "printf done; exit 0"],
                          stdout=subprocess.PIPE)
    out, _ = proc.communicate()
    return proc.returncode == 0 and out == b"done"


def run_fail(H, code):
    proc = procutil.popen(["sh", "-c", "exit {0}".format(code)])
    proc.wait()
    return proc.returncode == code


def run_timeout(H, rng):
    proc = procutil.popen(["sleep", "10"])
    deadline = time.monotonic() + 0.2
    while time.monotonic() < deadline and H.running():
        if proc.poll() is not None:
            break
        H.sleep(0.01)
    killed = proc.poll() is None
    if killed:
        proc.kill()
    proc.wait()
    return killed and proc.returncode is not None


def worker(H, wid, rng, state):
    while H.running():
        pick = rng.random()
        try:
            if pick < 0.5:
                ok = run_clean(H)
                what = "clean"
            elif pick < 0.85:
                code = rng.randint(1, 120)
                ok = run_fail(H, code)
                what = "fail({0})".format(code)
            else:
                ok = run_timeout(H, rng)
                what = "timeout"
        except OSError as e:
            if not H.running():
                break
            H.fail("ci job spawn error wid={0}: {1}".format(wid, e))
            return
        if not H.check(ok, "ci job misclassified wid={0}: {1}".format(
                wid, what)):
            return
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, None)


if __name__ == "__main__":
    harness.main("p94_ci_runner", body, default_funcs=150,
                 describe="CI jobs as subprocesses with timeouts; classify outcomes")
