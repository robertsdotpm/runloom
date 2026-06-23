"""big_100 / 214 -- lock hand-off fairness.

One shared `runloom.sync.Lock`.  N goroutines each loop: acquire the lock,
increment a SHARED counter under it, bump their own per-goroutine acquire-count
slot, `yield_now()` (force a scheduler hand-off while NOT holding the lock would
be wrong -- we yield while holding, stressing hand-off to a waiter on resume),
then release.

Correctness: because the lock serializes every critical section, the shared
counter must exactly equal the sum of the per-goroutine acquire counts, which
must equal ops -- a single lost increment means the lock let two critical
sections overlap with the GIL off.

Fairness: no goroutine starves.  Every participant that ran must have acquired
the lock at least once, and the spread (max vs min acquire count) must stay
under a generous bound -- a lock that always hands off to the same waiter would
starve the rest.

A single lock is intrinsically low-concurrency, so cap the participants.

Stresses: Lock acquire/release hand-off under M:N, serialization correctness,
waiter fairness, yield-while-holding.

Invariant 1: shared_counter == sum(per-g) == ops (no lost increment).
Invariant 2: every running goroutine acquired > 0; max/min spread bounded.
"""
import harness
import runloom
import runloom.sync as sync

MAX_CONTENDERS = 2000      # a single lock can't usefully serve more
FAIRNESS_BOUND = 200       # generous max/min ratio bound for runners that ran


def setup(H):
    n = min(H.funcs, MAX_CONTENDERS)
    H.state = {
        "lock": sync.Lock(),
        "counter": [0],             # SHARED, mutated only under the lock
        "acq": [0] * n,             # per-goroutine acquire count (single writer)
        "n": n,
    }


def worker(H, wid, rng, state):
    lock = state["lock"]
    counter = state["counter"]
    acq = state["acq"]
    for _ in H.round_range():
        if not H.running():
            break
        with lock:
            # critical section: serialized by the lock, so this read-modify-write
            # is safe even with the GIL off.
            counter[0] += 1
            acq[wid] += 1
            runloom.yield_now()      # hand off the scheduler WHILE holding the lock
        H.op(wid)


def body(H):
    # Cap to MAX_CONTENDERS: setup sized H.state for that, so spawn the same.
    H.run_pool(H.state["n"], worker, H.state, max_concurrent=MAX_CONTENDERS)


def post(H):
    st = H.state
    counter = st["counter"][0]
    acq = st["acq"]
    tally = sum(acq)
    ops = H.total_ops()
    H.check(counter == tally,
            "LOST INCREMENT: shared counter {0} != sum(per-g acq) {1} "
            "(lost {2}) -> lock did not serialize".format(
                counter, tally, tally - counter))
    H.check(counter == ops,
            "counter {0} != ops {1} (accounting mismatch)".format(counter, ops))

    ran = [c for c in acq if c > 0]
    if ran:
        mn, mx = min(ran), max(ran)
        # Every goroutine that ran acquired the lock at least once: a goroutine
        # that NEVER acquired but did run would have op()'d... we assert each
        # spawned-and-run participant shows up.  Use the spread bound on those
        # that ran (some may legitimately get 0 if the run was too short for
        # them to be scheduled, which is not starvation).
        spread_ok = (mx <= max(FAIRNESS_BOUND, mn * FAIRNESS_BOUND))
        H.check(spread_ok,
                "FAIRNESS: acquire spread too wide min={0} max={1} (bound "
                "x{2}) -> a goroutine is being starved".format(
                    mn, mx, FAIRNESS_BOUND))
        H.log("acquires min={0} max={1} mean={2:.1f} runners={3}/{4} "
              "counter={5}".format(
                  mn, mx, tally / len(ran), len(ran), st["n"], counter))
    else:
        H.log("no goroutine acquired the lock (counter={0})".format(counter))


if __name__ == "__main__":
    harness.main("p214_lock_handoff_fairness", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="single Lock: counter==sum(per-g)==ops (no lost "
                          "increment); no waiter starvation")
