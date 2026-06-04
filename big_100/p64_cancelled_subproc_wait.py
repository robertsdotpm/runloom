"""big_100 / 64 -- cancelled subprocess wait.

Goroutines spawn a short-lived child and wait for it cooperatively, but a
cancellation can fire before the child exits.  On cancel the goroutine stops
waiting, then kills and reaps the child anyway -- so no process is left
unreaped and the wait state is always cleaned up.

Stresses: wait-state cleanup, reaping after a cancelled wait.
"""
import harness
import cancelutil
import procutil


def cancellable_wait(H, ctx, proc, poll=0.01):
    """Wait for proc, bailing if ctx is cancelled.  Returns True if it exited,
    False if the wait was cancelled."""
    while True:
        if proc.poll() is not None:
            return True
        if not cancelutil.cancellable_sleep(ctx, poll):
            return False                    # cancelled


def worker(H, wid, rng, state):
    while H.running():
        proc = procutil.popen(["sleep", "{0:.2f}".format(rng.uniform(0.02, 0.3))])
        ctx, cancel = cancelutil.WithTimeout(cancelutil.Background(),
                                             rng.uniform(0.0, 0.2))
        exited = cancellable_wait(H, ctx, proc)
        cancel()
        if not exited:
            state["cancelled"][wid & 1023] += 1
            proc.kill()
        proc.wait()                         # reap regardless
        if not H.check(proc.returncode is not None,
                       "child not reaped after cancelled wait wid={0}".format(
                           wid)):
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {"cancelled": [0] * 1024}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    H.log("cancelled_waits={0}".format(sum(H.state["cancelled"])))


if __name__ == "__main__":
    harness.main("p64_cancelled_subproc_wait", body, setup=setup, post=post,
                 default_funcs=300,
                 describe="cancel a subprocess wait, then kill+reap cleanly")
