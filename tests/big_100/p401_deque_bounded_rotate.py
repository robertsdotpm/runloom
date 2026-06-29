"""big_100 / 401 -- shared bounded deque hammered by append/rotate/pop while iterated cross-hub.

collections.deque is a C doubly-linked list of 64-slot blocks with a fast
UNLOCKED append/appendleft/pop/popleft path and, crucially, a `rotate()` path
that has no documented per-object lock at all.  An iterator holds a live raw
pointer into one block plus that block's leftindex/rightindex; rotate() relinks
the block chain and rewrites those per-block indices.  Under M:N an iterator can
PARK mid-walk (its block pointer live, on a grown-down C stack) while an appender
on one hub links a fresh block, a rotator on another hub re-threads the chain,
and a popper on a third hub unlinks/frees a block.  On resume the iterator can
read through a freed/relinked slot and hand back an out-of-universe element, or
SIGSEGV -- the same hazard OrderedDict's order list has, but on a structure
whose rotate path is even less guarded.

We make that detectable with a closed-world, finite-universe oracle.  Each
worker owns ONE shared bounded `deque(maxlen=MAXLEN)` whose elements come only
from a fixed sentinel UNIVERSE.  Every value pushed in is a member of UNIVERSE,
so a yielded element NOT in UNIVERSE is a torn/freed slot -- a hard fault.  Each
round spawns four sub-goroutines on the shared deque:

  * an APPENDER: append()/appendleft() of sentinel keys (which, at maxlen, also
    silently pops the far end -- the bounded-evict path);
  * a ROTATOR: rotate(+k) / rotate(-k) -- the unlocked relink path;
  * a POPPER: pop()/popleft() (tolerating IndexError on an empty deque);
  * an ITERATOR: walks the deque, yields once mid-walk via runloom.yield_now()
    with its internal block pointer LIVE, and on every element checks
    `elem in UNIVERSE`.

The legal outcomes of the iterator are exactly two: a clean traversal in which
every element is in UNIVERSE, OR a clean `RuntimeError('deque mutated during
iteration')` (caught and counted as acceptable).  ANY other exception type, an
out-of-universe element, or a SIGSEGV is the bug -- a real M:N container
memory-safety fault on the deque block chain.

After the four sub-goroutines join (quiescent), post-round invariants on the now
unraced deque must hold exactly:
    len(d) == len(list(d)) == len(list(reversed(d)))   (block chain consistent
        forward and backward -- a lost/duplicated block breaks one of these)
    len(d) <= MAXLEN                                    (maxlen never exceeded)
and every element of the quiescent deque is still in UNIVERSE.

Invariant (post + hot, fail-fast): every yielded/resident element in UNIVERSE;
the only tolerated iterator exception is RuntimeError 'deque mutated during
iteration'; quiescent fwd/rev lengths agree and maxlen is honored; at least one
iteration completed across the run (so the race window was actually exercised).

Stresses: deque block-chain link/unlink under concurrent append/appendleft/pop/
popleft/rotate, the unlocked rotate relink, bounded-maxlen evict, iterator-vs-
mutate "deque mutated during iteration" detection, and a parked iterator holding
a live block pointer across a cross-hub relink.
"""
import random
from collections import deque

import harness
import runloom

# Finite sentinel UNIVERSE: a fixed, recognizable set of element values.  Any
# value NOT in this set yielded/resident is a corrupted element from a freed or
# relinked block -- a hard fault.  Large enough that the deque churns through
# several 64-slot blocks (a block is 64 elements; MAXLEN>64 forces a multi-block
# chain so link/unlink and the rotate relink actually exercise block boundaries).
UNIVERSE_SIZE = 512
UNIVERSE = tuple(0x40100000 + i for i in range(UNIVERSE_SIZE))
UNIVERSE_SET = frozenset(UNIVERSE)

# Bounded deque length.  >64 so the chain spans multiple internal blocks; the
# bounded-evict path (append at maxlen silently pops the far end) is exercised
# whenever the appender outruns the popper.
MAXLEN = 200

# How many seed elements the deque starts each round with (a full chain so the
# very first iteration already walks several blocks).
SEED = UNIVERSE[:MAXLEN]

# Rotate magnitudes that cross block boundaries (one is a multiple of 64 so the
# relink lands exactly on a block edge; the others are off-edge).
ROTATE_K = (1, 13, 64, 65, 127, 191)

# Number of mutations each mutator sub-goroutine performs per round, with one
# yield_now mid-burst so its writes interleave with the iterator's park window.
BURST = 24


def fresh_deque():
    """A full bounded deque seeded entirely from UNIVERSE."""
    return deque(SEED, maxlen=MAXLEN)


def is_universe(x):
    return x in UNIVERSE_SET


def appender(H, wid, d, rng):
    """Hammer append()/appendleft() with sentinel keys, yielding mid-burst so the
    writes land inside the iterator's park.  All pushed values are in UNIVERSE,
    so a clean iterator never legally sees an out-of-universe element from us."""
    for i in range(BURST):
        k = UNIVERSE[rng.randrange(UNIVERSE_SIZE)]
        if rng.getrandbits(1):
            d.append(k)
        else:
            d.appendleft(k)
        if i == BURST // 2:
            runloom.yield_now()


def rotator(H, wid, d, rng):
    """Hammer the UNLOCKED rotate() relink path, +k and -k, mid-burst yield."""
    for i in range(BURST):
        k = ROTATE_K[rng.randrange(len(ROTATE_K))]
        if rng.getrandbits(1):
            d.rotate(k)
        else:
            d.rotate(-k)
        if i == BURST // 2:
            runloom.yield_now()


def popper(H, wid, d, rng):
    """Hammer pop()/popleft(); an empty deque raising IndexError is expected and
    benign (the popper simply outran the appender), NOT a fault."""
    for i in range(BURST):
        try:
            if rng.getrandbits(1):
                d.pop()
            else:
                d.popleft()
        except IndexError:
            # Deque momentarily empty -- legal, not a corruption.
            pass
        if i == BURST // 2:
            runloom.yield_now()


def iterate(H, wid, d, counts, slot):
    """Walk the deque, park once mid-walk with the internal block pointer live,
    and validate every element is in UNIVERSE.  Returns and counts the outcome:
    'clean' or 'runtimeerror'.  A RuntimeError('deque mutated during iteration')
    is the LEGAL race-detection outcome and is caught here; ANY other exception
    propagates to the wrapper and is a fault."""
    seen = 0
    parked = False
    try:
        for elem in d:
            if not is_universe(elem):
                H.fail("deque iterator yielded OUT-OF-UNIVERSE element {0!r} -- "
                       "a torn/corrupted value from a freed or relinked block "
                       "(M:N deque block-chain corruption)".format(elem))
                return "fail"
            seen += 1
            if not parked and seen >= 3:
                # Park with the iterator's block pointer LIVE; the appender/
                # rotator/popper relink the chain DURING this park.
                parked = True
                runloom.yield_now()
        counts["clean"][slot] += 1
        return "clean"
    except RuntimeError as exc:
        # The deque's own "mutated during iteration" guard -- the legal, clean
        # detection of the concurrent mutation.  Only this exact guard is
        # tolerated; a differently-worded RuntimeError is suspect, so assert it.
        if "mutated during iteration" not in str(exc):
            H.fail("deque iterator raised unexpected RuntimeError {0!r} -- not "
                   "the legal 'deque mutated during iteration' guard".format(
                       str(exc)))
            return "fail"
        counts["rterror"][slot] += 1
        return "runtimeerror"


def quiescent_check(H, wid, d):
    """After all mutators+iterator have joined, the deque is unraced.  Its
    forward and reverse lengths must agree (block chain intact both ways), it
    must not exceed maxlen, and every resident element must still be in
    UNIVERSE.  Any mismatch is a structural corruption that survived the race."""
    n = len(d)
    fwd = list(d)
    rev = list(reversed(d))
    if not H.check(n == len(fwd) == len(rev),
                   "quiescent deque length mismatch: len()={0} forward-walk={1} "
                   "reverse-walk={2} -- a lost or duplicated block (chain "
                   "corrupted under concurrent relink)".format(
                       n, len(fwd), len(rev))):
        return False
    if not H.check(n <= MAXLEN,
                   "quiescent deque EXCEEDS maxlen: len()={0} > {1} -- the "
                   "bounded-evict path lost the far-end pop under concurrent "
                   "append".format(n, MAXLEN)):
        return False
    for elem in fwd:
        if not is_universe(elem):
            H.fail("quiescent deque holds OUT-OF-UNIVERSE element {0!r} -- a "
                   "corrupted value persisted past the race".format(elem))
            return False
    # Forward and reverse must be exact mirrors (no torn ordering).
    if fwd != list(reversed(rev)):
        H.fail("quiescent deque forward != reverse(reverse): forward and "
               "reverse iterators disagree on order -- block chain links "
               "inconsistent")
        return False
    return True


def worker(H, wid, rng, state):
    counts = state["counts"]
    slot = wid & 1023
    for _ in H.round_range():
        if not H.running():
            break
        d = fresh_deque()

        # Each sub-goroutine needs its OWN random stream (a shared random.Random
        # corrupts GIL-off), derived deterministically from this worker's rng.
        a_seed = rng.getrandbits(48)
        r_seed = rng.getrandbits(48)
        p_seed = rng.getrandbits(48)

        wg = runloom.WaitGroup()
        wg.add(4)

        def run_iter(d=d, slot=slot):
            try:
                iterate(H, wid, d, counts, slot)
            except Exception as exc:        # noqa: BLE001
                # Any exception escaping iterate() (i.e. NOT the caught legal
                # RuntimeError guard) is a fault on the deque block chain.
                H.fail("deque iterator raised non-tolerated {0}: {1} -- not the "
                       "legal 'deque mutated during iteration' outcome".format(
                           type(exc).__name__, exc))
            finally:
                wg.done()

        def run_append(d=d, a_seed=a_seed):
            try:
                appender(H, wid, d, random.Random(a_seed))
            except Exception as exc:        # noqa: BLE001
                H.fail("deque appender raised {0}: {1} -- append/appendleft on a "
                       "bounded deque must not raise".format(
                           type(exc).__name__, exc))
            finally:
                wg.done()

        def run_rotate(d=d, r_seed=r_seed):
            try:
                rotator(H, wid, d, random.Random(r_seed))
            except Exception as exc:        # noqa: BLE001
                H.fail("deque rotator raised {0}: {1} -- rotate() must not "
                       "raise".format(type(exc).__name__, exc))
            finally:
                wg.done()

        def run_pop(d=d, p_seed=p_seed):
            try:
                popper(H, wid, d, random.Random(p_seed))
            except Exception as exc:        # noqa: BLE001
                # popper catches IndexError internally; anything else is a fault.
                H.fail("deque popper raised {0}: {1} -- only IndexError on an "
                       "empty deque is legal".format(type(exc).__name__, exc))
            finally:
                wg.done()

        H.fiber(run_iter)
        H.fiber(run_append)
        H.fiber(run_rotate)
        H.fiber(run_pop)
        wg.wait()

        # All four have joined -> the deque is quiescent.  Structural oracle.
        if not quiescent_check(H, wid, d):
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {"counts": {"clean": [0] * 1024, "rterror": [0] * 1024}}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    clean = sum(H.state["counts"]["clean"])
    rterror = sum(H.state["counts"]["rterror"])
    H.log("iterations clean={0} runtimeerror={1} (both are legal outcomes; any "
          "out-of-universe element / quiescent length mismatch already failed "
          "fast)".format(clean, rterror))
    H.check(clean + rterror > 0,
            "no deque iterations completed -- the append/rotate/pop-vs-iterate "
            "race window was never exercised")
    H.require_no_lost()


if __name__ == "__main__":
    harness.main("p401_deque_bounded_rotate", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="shared bounded deque hammered by append/appendleft/"
                          "pop/popleft/rotate across hubs while a parked iterator "
                          "walks it; every element in a finite sentinel universe "
                          "with quiescent fwd/rev len agreement and maxlen "
                          "honored, or a clean 'deque mutated during iteration' "
                          "RuntimeError -- anything else is M:N deque corruption")
