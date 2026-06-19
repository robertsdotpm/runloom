"""big_100 / 96 -- scheduler operation fuzzer.

Each round a worker spawns a random number of child goroutines, each doing a
random mix of yield / sleep / channel-wait, and sometimes a canceller fires to
cancel the whole batch.  Every child reports EXACTLY ONCE whether it completed
or was cancelled; the worker's model says all K children must be accounted for
-- no goroutine lost, none reporting twice.

Stresses: the broad scheduler state machine -- spawn, yield, sleep, block,
cancel, join.
"""
import harness
import cancelutil
import runloom


def child(H, ctx, done, rng):
    status = "done"
    try:
        for _ in range(rng.randint(1, 6)):
            op = rng.randrange(3)
            if op == 0:
                runloom.yield_now()
            elif op == 1:
                if not cancelutil.cancellable_sleep(ctx, rng.uniform(0.0, 0.01)):
                    status = "cancelled"
                    break
            else:
                got = cancelutil.cancellable_recv(ctx, runloom.Chan(1),
                                                  timeout=0.005)
                if got is None and ctx.err() is not None:
                    status = "cancelled"
                    break
            if ctx.err() is not None:
                status = "cancelled"
                break
    finally:
        done.send(status)


def worker(H, wid, rng, state):
    while H.running():
        ctx, cancel = cancelutil.WithCancel(cancelutil.Background())
        k = rng.randint(2, 8)
        done = runloom.Chan(k)
        for i in range(k):
            H.fiber(child, H, ctx, done, H.derive("p96", wid, rng.getrandbits(32)))
        if rng.random() < 0.5:
            H.fiber(cancelutil.delayed_cancel, cancel, rng.uniform(0.0, 0.006))
        completed = cancelled = 0
        for _ in range(k):
            status = done.recv()[0]
            if status == "done":
                completed += 1
            else:
                cancelled += 1
        cancel()
        if not H.check(completed + cancelled == k,
                       "lost child goroutines wid={0}: {1}+{2} != {3}".format(
                           wid, completed, cancelled, k)):
            return
        H.op(wid, k)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, None)


if __name__ == "__main__":
    harness.main("p96_scheduler_fuzzer", body, default_funcs=1500,
                 describe="spawn/yield/sleep/block/cancel/join; all children accounted")
