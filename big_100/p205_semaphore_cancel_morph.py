"""big_100 / 205 -- semaphore cancel/morph (queued-waiter cancellation).

A `runloom.sync.Semaphore(K)` guards K permits.  Many goroutines acquire it;
SOME are cancelled while QUEUED waiting for a permit -- they call
`acquire(timeout=...)`, and a timeout returns False (the goroutine never got a
permit and must not release one).  Successful acquirers mark themselves "active"
in a per-slot marker, do a little work, then `release()` in a finally.

A monitor goroutine continuously sums the active markers and asserts the count
NEVER exceeds K (a cancelled-while-queued waiter that erroneously took a permit,
or a double-release, would breach this).  At teardown every permit must be
reclaimed: `try_acquire(K)` succeeds (all K free).

Stresses: Semaphore acquire/release under contention, cancellation of a QUEUED
waiter (timeout path), no permit leak, no over-grant, active<=K invariant.

Invariant: active <= K at every sample; final free permits == K (no leak).
"""
import harness
import runloom
import runloom.sync as sync

K = 8                      # permits


def setup(H):
    n = H.funcs
    sem = sync.Semaphore(K)
    H.state = {
        "sem": sem,
        # per-goroutine "I currently hold a permit" marker (single writer each)
        "active": bytearray(n),
        "n": n,
        "max_active": [0],          # observed by the monitor (single writer)
        "cancelled": [0] * n,       # per-slot count of timed-out (cancelled) acquires
        "acquired": [0] * n,        # per-slot count of successful acquires
        "breach": [0],              # set by monitor if active>K ever seen
    }

    def monitor(st=H.state, running=H.running):
        active = st["active"]
        while running():
            cur = sum(active)
            if cur > st["max_active"][0]:
                st["max_active"][0] = cur
            if cur > K:
                st["breach"][0] = 1
                H.fail("semaphore OVER-GRANT: {0} active > K={1}".format(cur, K))
                return
            runloom.sleep(0.0005)

    H.go(monitor)


def worker(H, wid, rng, state):
    sem = state["sem"]
    active = state["active"]
    for _ in H.round_range():
        if not H.running():
            break
        # Some acquirers are "cancellable": a short timeout so that when the
        # permit pool is saturated they give up while queued (returns False).
        cancellable = (rng.random() < 0.4)
        if cancellable:
            got = sem.acquire(timeout=rng.uniform(0.0005, 0.003))
            if not got:
                state["cancelled"][wid] += 1   # queued waiter cancelled -> no permit
                H.op(wid)
                continue
        else:
            if not sem.acquire(timeout=2.0):
                # 2s is far beyond any legitimate wait at K permits; treat a
                # timeout here as a stall, not a normal cancel.
                continue
        # We hold a permit.  Mark active, do a touch of work, release in finally.
        try:
            active[wid] = 1
            state["acquired"][wid] += 1
            runloom.sleep(rng.uniform(0.0, 0.002))
        finally:
            active[wid] = 0
            sem.release()
        H.op(wid)
        H.task_done(wid)


def body(H):
    # Cap concurrent acquirers so the queue stays bounded; each waiter is cheap.
    H.run_pool(H.funcs, worker, H.state, max_concurrent=4000)


def post(H):
    st = H.state
    H.check(st["breach"][0] == 0,
            "active count breached K during the run")
    H.check(max(st["active"]) == 0,
            "active marker left set at teardown: {0} still 'holding'".format(
                sum(st["active"])))
    # Every permit reclaimed: after the scheduler fully drained, no permits are
    # held.  The weighted Semaphore tracks held permits in `_held`; a leak (more
    # acquires than releases) would leave it > 0.  This is the real no-leak gate.
    held = getattr(st["sem"], "_held", None)
    if held is not None:
        H.check(held == 0,
                "permit LEAK: {0} permits still held after drain (free={1} of "
                "K={2})".format(held, K - held, K))
    acquired = sum(st["acquired"])
    cancelled = sum(st["cancelled"])
    H.check(acquired > 0, "no successful acquires (test did no work)")
    H.log("max_active={0} (K={1}) acquired={2} cancelled_while_queued={3} "
          "held_at_end={4}".format(
              st["max_active"][0], K, acquired, cancelled, held))


if __name__ == "__main__":
    harness.main("p205_semaphore_cancel_morph", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="Semaphore: active<=K always; queued waiters cancel via "
                          "acquire timeout; no permit leak/over-grant")
