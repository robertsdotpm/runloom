"""big_100 / 57 -- timeout wrapper torture.

Every blocking operation a goroutine performs is wrapped in a random timeout.
Many of them fire.  The key property: a timeout firing on one operation must
not poison the next -- the timer is cleaned up and the goroutine keeps making
progress (no leaked timer goroutines, no wedged scheduler).

Stresses: timer cancellation, the exception/early-return paths.
"""
import harness
import cancelutil
import runloom


def setup(H):
    # A channel that mostly never has data, so recvs usually time out.
    H.state = {"quiet": runloom.Chan(1), "lock": runloom.sync.Lock(),
               "completed": [0] * 1024, "timedout": [0] * 1024}


def worker(H, wid, rng, state):
    quiet = state["quiet"]
    lock = state["lock"]
    while H.running():
        kind = rng.randrange(3)
        timeout = rng.uniform(0.0005, 0.02)
        if kind == 0:
            ctx, cancel = cancelutil.WithTimeout(cancelutil.Background(),
                                                 timeout)
            full = cancelutil.cancellable_sleep(ctx, rng.uniform(0.0, 0.04))
            cancel()
            if full:
                state["completed"][wid & 1023] += 1
            else:
                state["timedout"][wid & 1023] += 1
        elif kind == 1:
            if lock.acquire(timeout=timeout):
                try:
                    pass
                finally:
                    lock.release()
                state["completed"][wid & 1023] += 1
            else:
                state["timedout"][wid & 1023] += 1
        else:
            ctx, cancel = cancelutil.WithTimeout(cancelutil.Background(),
                                                 timeout)
            got = cancelutil.cancellable_recv(ctx, quiet)
            cancel()
            if got is None:
                state["timedout"][wid & 1023] += 1
            else:
                state["completed"][wid & 1023] += 1
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    H.log("completed={0} timed_out={1}".format(
        sum(H.state["completed"]), sum(H.state["timedout"])))


if __name__ == "__main__":
    harness.main("p57_timeout_torture", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="random timeouts on every op; a fired timeout doesn't poison the next")
