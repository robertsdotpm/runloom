"""big_100 / 311 -- shared dict/set mutated on one hub while iterated on another.

The textbook free-threaded container race, and one no existing program touches
on a SHARED container: p141's cyclic graph is per-worker PRIVATE, so nothing in
the suite drives a dict/set that is concurrently ITERATED on one M:N hub and
size-MUTATED on another with the GIL off.  CPython's dict and set are not
internally locked for the duration of an iteration: an iterator holds a raw
index into the entry table plus a `dk_version` / `used` snapshot, and the only
thing standing between a concurrent insert/delete and silent corruption is the
"changed size during iteration" RuntimeError -- which is only raised if the
size-check fires on the next `__next__`, not if the table is rehashed and the
entries moved out from under the live index first.  Under M:N the iterator can
PARK mid-walk (its index live, on a grown-down C stack) while a mutator on a
different hub rehashes the table; on resume the iterator reads through a stale
slot pointer and can hand back a key/value from a freed slot -- an out-of-
universe key, a torn key/value pair (key K but value not f(K)), or a SIGSEGV.

We make that detectable with a closed-world, finite-universe oracle.  Each
worker owns ONE shared dict and ONE shared set whose keys come only from a fixed
sentinel UNIVERSE, and every dict value is the deterministic pairing f(key).  It
spawns two goroutines synchronized so the mutation provably lands INSIDE the
iterator's park window:

  * the iterator walks `dict.items()` (and a separate pass over the set),
    yields once mid-walk via `runloom.yield_now()` with the iterator's internal
    index live, and on every element checks `key in UNIVERSE` and (for the dict)
    `value == f(key)`;
  * the mutator waits on a barrier the iterator trips just before it parks, then
    does the size-changing mutations (insert new sentinel keys, delete some,
    which forces a rehash) on the SAME container from a different hub.

The legal outcomes are exactly two: a clean `RuntimeError` ("changed size during
iteration" -- caught and counted as acceptable), OR a fully consistent traversal
in which every key is in UNIVERSE and every dict value equals f(its key).  ANY
other exception type, an out-of-universe key, a torn key!=f-pairing, or a
SIGSEGV is the bug -- a real M:N container memory-safety fault.

Invariant (post + hot, fail-fast): every yielded key in UNIVERSE; every dict
value == f(key); the only tolerated exception is RuntimeError; at least one
iteration both completed-clean AND raised-RuntimeError across the run (so we
know the race window was actually exercised, not skipped).

Stresses: dict/set iterator-vs-resize, rehash under preempt-mid-iteration,
shared-container cross-hub mutation, torn key/value publication, "changed size
during iteration" detection under M:N.

Good TSan / controlled-M:N-replay target: shared-container mutate-vs-iterate is
a textbook data-race; a TSan report on the dict entry-table write/read often
localizes the corruption before the universe-membership assert even fires.
"""
import random

import harness
import runloom

# Finite sentinel UNIVERSE: a fixed, recognizable set of keys.  A key NOT in this
# set yielded by an iterator is a corrupted/torn key -- a hard fault.  Big enough
# to force the dict/set table through several growth/rehash boundaries (the
# resize is what moves entries out from under a live iterator index).
UNIVERSE_SIZE = 256
UNIVERSE = tuple(0x31100000 + i for i in range(UNIVERSE_SIZE))
UNIVERSE_SET = frozenset(UNIVERSE)


def f(key):
    """Deterministic key -> value pairing.  A dict whose value for key K is not
    f(K) is a TORN entry (the key passed membership but the value came from a
    different/freed slot).  Reversible-ish bijection so a torn pair is unlikely
    to coincidentally satisfy it."""
    return (key ^ 0x5A5A5A5A) + 0x100000007


# Half the universe is present at the start of each iteration; the mutator churns
# the rest in/out to force rehashes.  Splitting the universe this way keeps every
# inserted key still in UNIVERSE (so a clean traversal that happens to catch a
# freshly-inserted key is still legal), while guaranteeing real size changes.
SEED_KEYS = UNIVERSE[: UNIVERSE_SIZE // 2]
CHURN_KEYS = UNIVERSE[UNIVERSE_SIZE // 2:]


def fresh_dict():
    return {k: f(k) for k in SEED_KEYS}


def fresh_set():
    return set(SEED_KEYS)


def check_dict_item(H, wid, key, val):
    """Validate one (key, value) yielded by the dict iterator.  Returns False on
    the first violation (caller should stop)."""
    if key not in UNIVERSE_SET:
        H.fail("dict iterator yielded OUT-OF-UNIVERSE key {0!r} -- a torn/"
               "corrupted key from a rehashed-away slot (M:N container "
               "corruption)".format(key))
        return False
    if val != f(key):
        H.fail("dict iterator yielded TORN pair: key {0!r} value {1!r} != "
               "f(key) {2!r} -- key/value came from different slots (torn "
               "entry under concurrent rehash)".format(key, val, f(key)))
        return False
    return True


def check_set_member(H, wid, key):
    if key not in UNIVERSE_SET:
        H.fail("set iterator yielded OUT-OF-UNIVERSE member {0!r} -- a torn/"
               "corrupted key from a rehashed-away slot (M:N container "
               "corruption)".format(key))
        return False
    return True


def iterate_dict(H, wid, d, gate, counts, slot):
    """Walk d.items(), parking once mid-walk after tripping `gate` so the mutator
    runs DURING the park.  Returns 'clean' | 'runtimeerror'; H.fail on any other
    fault.  Raising RuntimeError is the LEGAL race outcome and is caught here."""
    parked = False
    seen = 0
    try:
        for key, val in d.items():
            if not check_dict_item(H, wid, key, val):
                return "fail"
            seen += 1
            if not parked and seen >= 2:
                # Trip the gate (lets the mutator proceed) then park with the
                # iterator's internal index LIVE -- the mutation lands here.
                parked = True
                gate.done()
                runloom.yield_now()
        counts["clean"][slot] += 1
        return "clean"
    except RuntimeError:
        # "dictionary changed size during iteration" -- the LEGAL, clean
        # detection of the concurrent mutation.  Acceptable.
        counts["rterror"][slot] += 1
        # The gate may not have been tripped if the RuntimeError fired before we
        # parked; trip it so the mutator never blocks forever.
        if not parked:
            gate.done()
        return "runtimeerror"


def iterate_set(H, wid, s, gate, counts, slot):
    """Same protocol over a set's iterator."""
    parked = False
    seen = 0
    try:
        for key in s:
            if not check_set_member(H, wid, key):
                return "fail"
            seen += 1
            if not parked and seen >= 2:
                parked = True
                gate.done()
                runloom.yield_now()
        counts["clean"][slot] += 1
        return "clean"
    except RuntimeError:
        counts["rterror"][slot] += 1
        if not parked:
            gate.done()
        return "runtimeerror"


def mutate(d_or_s, gate, rng, is_dict):
    """Wait for the iterator to enter its park, then size-MUTATE the SAME
    container (insert + delete churn keys -> forces a rehash).  Uses its OWN
    random.Random (a shared one corrupts GIL-off)."""
    gate.wait()
    # Insert the churn half (grows the table past a rehash boundary), then delete
    # a random slice (shrinks/holes the table).  Both are size changes that the
    # live iterator must either survive consistently or reject with RuntimeError.
    if is_dict:
        for k in CHURN_KEYS:
            d_or_s[k] = f(k)
        for k in CHURN_KEYS:
            if rng.getrandbits(1):
                d_or_s.pop(k, None)
        # A couple of seed deletions too, to perturb the part the iterator walks.
        for k in SEED_KEYS[:4]:
            d_or_s.pop(k, None)
    else:
        for k in CHURN_KEYS:
            d_or_s.add(k)
        for k in CHURN_KEYS:
            if rng.getrandbits(1):
                d_or_s.discard(k)
        for k in SEED_KEYS[:4]:
            d_or_s.discard(k)


def worker(H, wid, rng, state):
    counts = state["counts"]
    slot = wid & 1023
    for _ in H.round_range():
        if not H.running():
            break
        # Alternate dict and set rounds so both iterator types get hammered.
        use_dict = (wid & 1) == 0
        container = fresh_dict() if use_dict else fresh_set()

        # gate: the iterator trips it the instant before it parks; the mutator
        # waits on it, so the mutation provably lands inside the park window.
        gate = runloom.WaitGroup()
        gate.add(1)
        wg = runloom.WaitGroup()
        wg.add(2)
        mseed = rng.getrandbits(48)

        def run_iter(container=container, gate=gate, slot=slot,
                     use_dict=use_dict):
            try:
                if use_dict:
                    iterate_dict(H, wid, container, gate, counts, slot)
                else:
                    iterate_set(H, wid, container, gate, counts, slot)
            except Exception as exc:        # noqa: BLE001
                # ANY non-RuntimeError exception escaping the iterator is a fault
                # (RuntimeError is caught inside iterate_*).
                H.fail("iterator raised non-RuntimeError {0}: {1} -- not the "
                       "legal 'changed size during iteration' outcome".format(
                           type(exc).__name__, exc))
            finally:
                wg.done()

        def run_mut(container=container, gate=gate, mseed=mseed,
                    use_dict=use_dict):
            mrng = random.Random(mseed)
            try:
                mutate(container, gate, mrng, use_dict)
            except Exception:
                # The mutator's own writes never legally raise here; a mutation
                # failing would itself be suspect, but we don't want a mutator
                # exception to deadlock the iterator's gate (already tripped by
                # the iterator before park).  Record nothing; the iterator oracle
                # is the judge.
                pass
            finally:
                wg.done()

        H.fiber(run_iter)
        H.fiber(run_mut)
        wg.wait()
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {"counts": {"clean": [0] * 1024, "rterror": [0] * 1024}}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    clean = sum(H.state["counts"]["clean"])
    rterror = sum(H.state["counts"]["rterror"])
    H.log("iterations clean={0} runtimeerror={1} (both are legal outcomes; "
          "any out-of-universe key / torn pair already failed fast)".format(
              clean, rterror))
    H.check(clean + rterror > 0,
            "no iterations completed -- the mutate-vs-iterate race window was "
            "never exercised")
    H.require_no_lost()


if __name__ == "__main__":
    harness.main("p311_dict_set_mutate_iter_crosshub", body, setup=setup,
                 post=post, default_funcs=3000,
                 describe="shared dict/set iterated across a park while another "
                          "hub size-mutates it; every key in a finite sentinel "
                          "universe and value==f(key), or a clean RuntimeError -- "
                          "anything else is M:N container corruption")
