"""big_100 / 55 -- lock cancellation test.

A heavily contended lock.  A few "hog" goroutines hold it for a while; the rest
try to acquire it with a short timeout and frequently give up (a cancelled
wait).  Each cancelled acquire must leave the lock's wait queue clean: the lock
keeps being acquirable, and the guarded counter equals exactly the number of
SUCCESSFUL acquires (a leaked/corrupt waiter would lose increments or wedge the
lock entirely, which the watchdog would catch).

Stresses: cancellation (timed-out acquire) while blocked on a lock, wait-queue
cleanup.
"""
import harness
import runloom


def setup(H):
    H.state = {"lock": runloom.sync.Lock(), "counter": [0],
               "cancelled": [0] * 1024}


def hog(H, wid, rng, state):
    lock = state["lock"]
    while H.running():
        lock.acquire()
        try:
            state["counter"][0] += 1
            runloom.sleep(rng.uniform(0.002, 0.02))     # long hold
        finally:
            lock.release()
        H.op(wid)
        runloom.sleep(rng.uniform(0.0, 0.001))


def waiter(H, wid, rng, state):
    lock = state["lock"]
    while H.running():
        got = lock.acquire(timeout=rng.uniform(0.0005, 0.003))
        if got:
            try:
                state["counter"][0] += 1
            finally:
                lock.release()
            H.op(wid)
        else:
            state["cancelled"][wid & 1023] += 1     # cancelled wait
        H.task_done(wid)


def body(H):
    hogs = max(2, H.funcs // 50)
    waiters = H.funcs - hogs
    H.run_pool(hogs, hog, H.state)
    H.run_pool(waiters, waiter, H.state)


def post(H):
    counter = H.state["counter"][0]
    ops = H.total_ops()
    # Every increment happened under the lock, exactly once per successful
    # acquire; H.op is bumped once per successful acquire too.
    H.check(counter == ops,
            "lock corruption: counter {0} != successful acquires {1}".format(
                counter, ops))
    H.log("counter={0} successful_acquires={1} cancelled={2}".format(
        counter, ops, sum(H.state["cancelled"])))


if __name__ == "__main__":
    harness.main("p55_lock_cancellation", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="timed-out (cancelled) lock waits leave the lock clean")
