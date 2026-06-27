"""big_100 / 319 -- sync.Watch versioned broadcast: no torn pair, no lost wake.

`runloom.sync.Watch` is a tokio::sync::watch -- a single latest-value cell that
many observers watch for CHANGES.  `set(v)` updates the value, bumps a version
counter UNDER the guard, snapshots-and-clears the waiter list, and broadcasts a
wake to every current observer; `wait_changed(seen, timeout=None)` parks until
`version > seen` and returns the `(value, version)` pair (or None on timeout).
sync.Watch is exercised by ZERO other big_100 programs.

Two M:N hazards live in that fan-out:

  * TORN PAIR -- get/return of the `(value, version)` pair straddles a yield, so
    a watcher acts on a value from one publish and a version from another (a
    half-published cell).  We make that detectable by publishing only values
    where `value == encode(version)` for an invertible `encode`; a watcher that
    ever sees a pair with `value != encode(version)` observed a torn pair.

  * LOST WAKE / LOST UPDATE -- a watcher parks on an old version *after* a set()
    has already bumped it (or the broadcast wake is dropped), so the watcher
    sleeps forever on a value that already changed.  set() bumping the version
    under the guard before snapshotting the waiters is exactly what closes that
    window; if it regresses, a watcher strands below the final version K.

Topology (closed-world per worker, so conservation is checkable): each pool
worker owns ONE Watch, spawns ONE publisher fiber and M watcher fibers, and
fences them with a WaitGroup before returning.  The publisher set()s a strictly
increasing sequence of K distinct values `encode(1)..encode(K)` with small
sleeps between, ending on the SENTINEL version K.  Each watcher loops
`wait_changed(seen)`: every non-None return guarantees `ver > seen` (set() bumps
under the guard), so versions it sees are STRICTLY increasing; it checks
`value == encode(ver)` (torn-pair biter) and records its last version, exiting
when `ver >= K`.  (A Watch is a latest-value cell, so a watcher may legally SKIP
intermediate versions if the publisher ran ahead -- we assert strictly-increasing
and final-coverage, never that every version is seen.)

Oracle:
  * TORN PAIR  (hot, fail-fast): every observed `(value, version)` satisfies
    `value == encode(version)`; versions strictly increase per watcher.
  * COVERAGE   (post): the MINIMUM last-observed version across ALL watchers is
    >= K -- no watcher permanently missed a broadcast / suffered a lost wake.
  * require_no_lost guards a STRANDED watcher: a lost wake leaves a watcher
    parked forever -> its WaitGroup.done() never fires -> its owning worker hangs
    on wg.wait() -> the worker is LOST (and the watchdog catches the wedge).

Stresses: sync.Watch versioned broadcast, wake-ALL fan-out across hubs,
(value,version) pair atomicity across a yield, lost-wake / lost-update on a
re-parking watcher, set()-under-guard version ordering.

Good TSan / controlled-M:N-replay target: the (value,version) pair read vs the
under-guard set() is a pure memory-ordering surface, and the broadcast-wake vs
re-park is the classic lost-wakeup window -- a data-race or a missed wake shows
up before the value oracle even fires.
"""
import random

import harness
import runloom
import runloom.sync as rsync

# encode(version): invertible map so a torn (value, version) pair -- a value from
# one publish glued to a version from another -- fails value == encode(version).
MULT = 0x9E3779B1            # odd -> invertible mod 2**k
XORK = 0x5A5A5A5A


def encode(version):
    return ((version * MULT) ^ XORK) & 0xFFFFFFFFFFFF


# Number of distinct publishes (versions 1..K) the publisher emits per round.
PUBLISHES = 24
# Watchers per worker (each fans in through the same Watch).
WATCHERS = 8


def publisher(w, k, pause_seed):
    """set() a strictly increasing sequence encode(1)..encode(K), tiny sleeps
    between so watchers transiently re-park (exercises the broadcast wake on a
    re-appended waiter).  Owns its OWN random.Random (a shared Random under M:N
    with the GIL off corrupts its Mersenne state)."""
    prng = random.Random(pause_seed)
    for version in range(1, k + 1):
        w.set(encode(version))
        if (version & 3) == 0:
            runloom.sleep(prng.uniform(0.0, 0.0006))
        else:
            runloom.yield_now()


def watcher(H, w, k, results, j, wid):
    """Drain the Watch until the final version K.  Every returned pair must be
    torn-free (value == encode(version)) and the version strictly greater than
    the last one we saw.  Records its last-observed version into its OWN local
    result slot `results[j]` (single writer -> no race even GIL-off; this list
    is owned by ONE worker, fenced by the WaitGroup before the worker reads it)."""
    seen = 0
    while seen < k:
        r = w.wait_changed(seen)
        if r is None:                       # timeout (we pass none, so unreached)
            continue
        value, ver = r
        if ver <= seen:
            H.fail("Watch wait_changed returned non-increasing version: "
                   "ver={0} <= seen={1} (stale/duplicate after wake)".format(
                       ver, seen))
            results[j] = seen
            return
        if value != encode(ver):
            H.fail("torn (value,version) pair: value={0!r} but encode({1})="
                   "{2!r} -- a half-published cell read across a yield".format(
                       value, ver, encode(ver)))
            results[j] = seen
            return
        seen = ver
        H.op(wid)
    results[j] = seen                       # single writer per local slot


def worker(H, wid, rng, state):
    strand = state["strand"]               # count of under-K watchers (shard)
    ran = state["ran"]                     # count of watchers that ran (shard)
    nwatch = state["nwatch"]
    npub = state["npub"]
    slot = wid & 1023
    for _ in H.round_range():
        if not H.running():
            break
        w = rsync.Watch(encode(0))
        pause_seed = rng.getrandbits(48)
        results = [0] * nwatch             # worker-local; one writer per index
        wg = runloom.WaitGroup()
        wg.add(nwatch + 1)

        def run_publisher(w=w, npub=npub, pause_seed=pause_seed):
            try:
                publisher(w, npub, pause_seed)
            finally:
                wg.done()

        def run_watcher(j, w=w, npub=npub, results=results):
            try:
                watcher(H, w, npub, results, j, wid)
            finally:
                wg.done()

        # Spawn watchers FIRST so they are queued before the publisher's first
        # set() (a watcher that arrives after set() simply reads the current
        # version > seen and proceeds -- still correct, but spawning first
        # maximizes the re-park-then-broadcast window we want to stress).
        for j in range(nwatch):
            H.fiber(run_watcher, j)
        H.fiber(run_publisher)
        wg.wait()                          # fences `results` before we read it
        # COVERAGE: every watcher that ran must have reached the final version K.
        # A lost broadcast wake leaves a watcher stranded below K.  We count, not
        # `min`, so the shard is sum-friendly across wid-aliased workers (a single
        # surviving increment still flags the bug, even if some are lost).
        for seen in results:
            if seen < npub:
                strand[slot] = strand[slot] + 1
        ran[slot] = ran[slot] + nwatch
        H.task_done(wid)


def setup(H):
    H.state = {
        "strand": [0] * 1024,              # under-K watchers (lost-wake stranded)
        "ran": [0] * 1024,                 # watchers that completed a run
        "npub": PUBLISHES,
        "nwatch": WATCHERS,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    stranded = sum(H.state["strand"])
    ran = sum(H.state["ran"])
    H.log("watchers_ran={0} stranded_below_K={1} ops={2} K={3} nwatch={4}".format(
        ran, stranded, H.total_ops(), H.state["npub"], H.state["nwatch"]))
    H.check(ran > 0, "no watchers ran")
    # COVERAGE: no watcher may end below the final version K.  A lost broadcast
    # wake / lost update strands a re-parked watcher below K.
    H.check(stranded == 0,
            "coverage broken: {0} watcher(s) ended below K={1} (lost broadcast "
            "wake / lost update on a re-parked watcher)".format(
                stranded, H.state["npub"]))
    # A stranded watcher that never wakes hangs its owning worker on wg.wait()
    # -> the worker is LOST.  require_no_lost catches that wedge.
    H.require_no_lost("watch-coverage")


if __name__ == "__main__":
    harness.main("p319_watch_no_lost_update", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="sync.Watch versioned broadcast; every (value,version) "
                          "torn-free (value==encode(version)) and every watcher "
                          "reaches the final version K (no lost wake)")
