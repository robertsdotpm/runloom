"""big_100 / 56 -- cancellation storm.

Thousands of goroutines each run a random operation -- a sleep, a file
write, a lock hold, or a channel wait -- under a cancellation context, and
about half the time a canceller fires mid-operation.  Whether the op completes
or is cancelled, its resources must be released: the fd auditor confirms files
don't leak and the watchdog confirms nothing wedges.

Stresses: cooperative cancellation everywhere, resource cleanup on the
cancelled path.
"""
import os

import harness
import cancelutil
import runloom


def op_sleep(H, ctx, rng, state):
    cancelutil.cancellable_sleep(ctx, rng.uniform(0.001, 0.05))


def op_file(H, ctx, rng, state, wid):
    path = os.path.join(state["base"], "f{0}".format(wid))
    f = open(path, "wb")
    try:
        for _ in range(20):
            if ctx.err() is not None:
                break
            f.write(b"x" * 128)
            if not cancelutil.cancellable_sleep(ctx, 0.001):
                break
    finally:
        f.close()
        try:
            os.remove(path)
        except OSError:
            pass


def op_lock(H, ctx, rng, state):
    lock = state["lock"]
    if lock.acquire(timeout=rng.uniform(0.001, 0.01)):
        try:
            cancelutil.cancellable_sleep(ctx, rng.uniform(0.0, 0.005))
        finally:
            lock.release()


def op_chan(H, ctx, rng, state):
    # Wait for a value that may never come; cancellation/timeout must free us.
    cancelutil.cancellable_recv(ctx, state["ch"], timeout=0.02)


def worker(H, wid, rng, state):
    ops = (op_sleep, op_file, op_lock, op_chan)
    while H.running():
        ctx, cancel = cancelutil.WithCancel(cancelutil.Background())
        if rng.random() < 0.5:
            H.fiber(cancelutil.delayed_cancel, cancel, rng.uniform(0.0, 0.01))
        op = rng.choice(ops)
        try:
            if op is op_file:
                op(H, ctx, rng, state, wid)
            else:
                op(H, ctx, rng, state)
        finally:
            cancel()                # idempotent; ensures the ctx is torn down
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {"base": H.make_tmpdir("big100_cancel_"),
               "lock": runloom.sync.Lock(),
               "ch": runloom.Chan(1)}
    H.fd_ceiling = 0


def body(H):
    H.run_pool(H.funcs, worker, H.state)

    def auditor():
        base = harness.count_fds()
        while H.running():
            fds = harness.count_fds()
            H.fd_ceiling = max(H.fd_ceiling, fds)
            # Every concurrently-PARKED waiter holds a TWO-fd wake primitive (an
            # os.pipe on POSIX, a socketpair on Windows), so under a cancellation
            # storm where ~all workers park at once the legitimate peak is ~2 fds
            # per worker, not 1.  Bound at 2*funcs (+ headroom) so the auditor
            # still catches a real leak -- cancelled ops not freeing their parker
            # grows fds WITHOUT bound, blowing past this -- without false-flagging
            # the normal 2-fd-per-parker concurrency.  (Measured: peak tracks
            # ~2*funcs linearly with low steady-state, i.e. concurrency not leak.)
            H.check(fds < base + 2 * H.funcs + 5000,
                    "fd leak under cancellation: {0} open (base {1})".format(
                        fds, base))
            H.sleep(1.0)
        H.log("fd_ceiling={0}".format(H.fd_ceiling))

    H.fiber(auditor)


if __name__ == "__main__":
    harness.main("p56_cancellation_storm", body, setup=setup, default_funcs=3000,
                 describe="random ops under cancellation; resources always freed")
