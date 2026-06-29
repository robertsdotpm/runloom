"""big_100 / 403 -- shared list sorted on one hub while mutated + iterated on others.

The list-sort-mutate free-threaded hazard, which no existing program drives on a
SHARED list.  CPython's `list.sort()` is NOT a quick in-place comparison swap: it
TEMPORARILY BLANKS the list -- it saves `ob_item`/`ob_size`, points the list at an
EMPTY array, sets a private "in sort" sentinel, sorts a detached copy of the keys
in a freshly-malloc'd keys-array, then restores `ob_item` at the end.  CPython
raises `ValueError("list modified during sort")` only if, on RESTORE, it notices
the list was touched (ob_item swapped out from under it).  With the GIL off, a
concurrent `append` / `__setitem__` / iteration on the SAME list races three live
buffers at once: the saved-and-restored ob_item, the in-flight keys-array malloc,
and any iterator's raw index into ob_item.  Under M:N the sorter can PARK mid-sort
(its detached keys-array live, the list blanked to the empty array) while a sibling
on another hub appends -- forcing a `list_resize` realloc of ob_item -- and a third
fiber iterates through a now-stale `it_seq->ob_item + it_index`.  The CORRECT FT
runtime defends every one of these with a PER-LIST critical section: sort, append,
__setitem__, pop and each iterator step take the list's lock, so the ob_item swap
can never actually be observed half-done from another thread.  The two CPython
mutation-detection escape hatches are tolerated if they ever fire -- ValueError
"list modified during sort" (raised on the REENTRANT path: a comparison that
mutates the list mid-sort) and RuntimeError "...changed size during iteration"
(note: this is a dict/set check; a plain `list` iterator does NOT raise it, so for
a list the size-change is absorbed by the lock, not reported) -- but the REAL,
always-checkable falsifiable signal is corruption: an out-of-universe element
handed back from a freed/realloc'd slot, a torn value, a use-after-realloc, a
conservation break (ob_item vs ob_size disagree), or a SIGSEGV.  If the per-list
critical section ever regressed, THAT is what this catches.

Closed-world finite-UNIVERSE oracle.  Each round one shared list is seeded with
sentinel ints drawn ONLY from a fixed UNIVERSE, and we spawn three fibers --
round-robined across hubs by mn_fiber -- that each REPEAT their op many times with
a yield between repeats, so the three operations genuinely overlap on the one
shared list (a single short sort/append cannot overlap a sibling on another hub;
many interleaved repeats do):

  * SORTER calls list.sort() (alternating reverse= so a real reorder happens); the
    only tolerated raise is ValueError "list modified during sort".
  * MUTATOR appends fresh sentinels (forces ob_item realloc growth), overwrites
    random indices (in-place ob_item stores), and pops back toward the seed size
    (shrink/realloc) -- every written value still in UNIVERSE.
  * ITERATOR walks the list, parking mid-walk (index LIVE) so an append-realloc /
    sort-restore lands inside the walk; on each element checks the value is in
    UNIVERSE; a RuntimeError, if it ever fired, is tolerated.

Hot/fail-fast invariant: every element EVER observed (by the iterator, and by
post's final walk) is in UNIVERSE -- a value outside UNIVERSE is a torn/freed slot
read from a raced ob_item realloc (hard fault).  Any exception that is NOT one of
the two tolerated mutation-detection errors is a hard fault.

Quiescent post invariant (the primary falsifiable check): after all three fibers
of a round RETURN, the surviving list is a PERMUTATION OF A SUBSET OF UNIVERSE --
every element in UNIVERSE, and (the conservation check) `len(lst) == len(list(it))`,
i.e. a fresh independent walk of the now-quiet list returns exactly len(lst)
elements: no slot was dropped or duplicated by a raced realloc.  A mismatch means
ob_item and ob_size disagree -- a torn resize.  Verified once per round
(single-owner, quiescent) and tallied.

Coverage (the suite's flaky-random lesson): post() requires only that real work
happened (a sort, an iteration, a mutation, and a quiescent check each completed at
least once); it does NOT require the rarely-reachable detect branches, so there is
no flaky-random coverage hole.  The worker still round-robins the sort direction by
worker id in its first ops for determinism.  EXPECTED RESULT on a correct FT
runtime: sort_detect == iter_detect == 0 (the per-list lock prevents the race) with
zero corruption -- a PASS that proves the safety property, and a regression would
flip a conservation/out-of-universe check, not merely the counters.

Stresses: list.sort ob_item blank/restore vs concurrent append-realloc /
__setitem__ / pop, per-list critical-section integrity, iterator index vs realloc,
conservation of slots under M:N.
"""
import random

import harness
import runloom

# Finite sentinel UNIVERSE.  A value NOT in here, yielded by any walk of the list,
# is a torn / freed / use-after-realloc slot -- a hard fault.  Made large enough
# that the seeded list plus the appended churn crosses several list_resize growth
# boundaries (the over-allocation doubling), which is precisely what reallocates
# ob_item out from under a parked sorter/iterator.
UNIVERSE_SIZE = 512
UNIVERSE = tuple(0x40300000 + i for i in range(UNIVERSE_SIZE))
UNIVERSE_SET = frozenset(UNIVERSE)

# The list starts each round seeded with the first half; the mutator appends from
# the second half (real size growth -> realloc) and overwrites indices with values
# drawn from anywhere in UNIVERSE (in-place ob_item stores).  Every written value
# stays in UNIVERSE so a clean traversal catching a fresh value is still legal.
SEED_KEYS = UNIVERSE[: UNIVERSE_SIZE // 2]
CHURN_KEYS = UNIVERSE[UNIVERSE_SIZE // 2:]

# Each role repeats its operation this many times per round, yielding between
# repeats, so the three fibers -- spread round-robin across hubs by mn_fiber --
# genuinely OVERLAP on the same shared list (one short sort/append cannot overlap a
# sibling on another hub; many interleaved repeats do, the way p311's long churn
# loop spans an iterator's park).  This is what turns the gate from "fires once
# atomically" into a real concurrent window.
REPEAT = 12

# Substrings that mark the two LEGAL CPython mutation-detection errors.  We match
# on the message so an unrelated ValueError/RuntimeError from corruption is still
# treated as a hard fault rather than silently tolerated.
SORT_RACE_MSG = "modified during sort"
ITER_RACE_MSG = "changed size during iteration"


def fresh_list(rng):
    """A new shared list seeded with sentinel ints from UNIVERSE, in a shuffled
    order so sort() has real work to do."""
    keys = list(SEED_KEYS)
    rng.shuffle(keys)
    return keys


def value_ok(H, where, val):
    """True iff val is a legal UNIVERSE sentinel.  H.fail (hard fault) otherwise."""
    if val not in UNIVERSE_SET:
        H.fail("{0} observed OUT-OF-UNIVERSE element {1!r} -- a torn/freed slot "
               "read from a raced ob_item realloc (M:N list corruption)".format(
                   where, val))
        return False
    return True


def run_sorter(H, wid, lst, gate, rng, reverse, counts, slot):
    """Sort the shared list REPEAT times, alternating direction so a real reorder
    happens, yielding between sorts so the mutator/iterator on other hubs interleave
    INSIDE the sort window over the repeats.  Trips `gate` once so the siblings
    start; the only tolerated raise is ValueError 'list modified during sort' --
    anything else is a hard fault."""
    gate.done()
    rev = reverse
    for r in range(REPEAT):
        if not H.running():
            break
        try:
            lst.sort(reverse=rev)
            counts["sort_clean"][slot] += 1
        except ValueError as exc:
            if SORT_RACE_MSG in str(exc):
                # The legal, clean detection that the list was mutated under sort.
                counts["sort_detect"][slot] += 1
            else:
                H.fail("list.sort() raised ValueError NOT the legal 'modified "
                       "during sort' detection: {0!r} -- corrupted sort state"
                       .format(exc))
                return
        except Exception as exc:        # noqa: BLE001
            H.fail("list.sort() raised non-ValueError {0}: {1} -- not the legal "
                   "'modified during sort' outcome (M:N sort corruption)".format(
                       type(exc).__name__, exc))
            return
        rev = not rev
        runloom.yield_now()


def run_mutator(H, wid, lst, gate, mrng, counts, slot):
    """Over REPEAT passes, APPEND fresh sentinels (forces ob_item realloc growth),
    OVERWRITE random indices (in-place ob_item stores), and POP some back off
    (shrink) -- every written value in UNIVERSE -- yielding between passes so the
    resize churn spans the sorter's and iterator's windows.  Index/pop ops are
    guarded: a transient out-of-range / pop-from-empty under a concurrent resize is
    itself a legal race outcome (IndexError, tolerated); any other exception is a
    fault."""
    gate.wait()
    did = False
    for r in range(REPEAT):
        if not H.running():
            break
        try:
            for k in CHURN_KEYS:
                lst.append(k)           # growth -> list_resize realloc of ob_item
            for _ in range(len(CHURN_KEYS)):
                n = len(lst)
                if n <= 0:
                    break
                idx = mrng.randrange(n)
                v = UNIVERSE[mrng.randrange(UNIVERSE_SIZE)]
                try:
                    lst[idx] = v        # in-place ob_item store
                except IndexError:
                    pass                # list shrank under us -- legal resize race
            # Shrink back toward the seed size so the list churns across the
            # over-allocation boundary every pass instead of growing without bound.
            target = len(SEED_KEYS)
            while len(lst) > target:
                try:
                    lst.pop()           # shrink -> may free-and-realloc ob_item
                except IndexError:
                    break               # emptied under us -- legal resize race
            did = True
        except IndexError:
            pass
        except Exception as exc:        # noqa: BLE001
            H.fail("mutator raised unexpected {0}: {1} -- appending/overwriting/"
                   "popping in-UNIVERSE values must not fault".format(
                       type(exc).__name__, exc))
            return
        runloom.yield_now()
    if did:
        counts["mutated"][slot] += 1


def run_iterator(H, wid, lst, gate, counts, slot):
    """Walk the shared list REPEAT times while it is sorted+mutated, parking
    mid-walk (index LIVE) so a concurrent append-realloc / sort-restore lands inside
    the walk.  Every element must be in UNIVERSE.  The ONLY tolerated raise is
    RuntimeError 'changed size during iteration'; anything else is a hard fault."""
    gate.wait()
    for r in range(REPEAT):
        if not H.running():
            break
        parked = False
        seen = 0
        try:
            for val in lst:
                if not value_ok(H, "iterator", val):
                    return
                seen += 1
                if not parked and seen >= 2:
                    # Park with the iterator's internal ob_item index LIVE.
                    parked = True
                    runloom.yield_now()
            counts["iter_clean"][slot] += 1
        except RuntimeError as exc:
            if ITER_RACE_MSG in str(exc):
                counts["iter_detect"][slot] += 1
            else:
                H.fail("iterator raised RuntimeError NOT the legal 'changed size "
                       "during iteration' detection: {0!r}".format(exc))
                return
        except Exception as exc:        # noqa: BLE001
            H.fail("iterator raised non-RuntimeError {0}: {1} -- not the legal "
                   "'changed size during iteration' outcome (M:N list "
                   "corruption)".format(type(exc).__name__, exc))
            return
        runloom.yield_now()


def quiescent_check(H, wid, lst, counts, slot):
    """Single-owner, post-join conservation oracle: now that all three fibers have
    RETURNED, the surviving list must be a permutation of a SUBSET of UNIVERSE --
    every element in UNIVERSE, and len(lst) == len(list(it)) (a fresh independent
    walk returns exactly len(lst) elements: no slot dropped or duplicated by a
    raced ob_item/ob_size resize)."""
    n = len(lst)
    walked = list(lst)              # fresh independent iterator over the now-quiet list
    if len(walked) != n:
        H.fail("CONSERVATION BREAK: len(lst)={0} but a fresh walk returned {1} "
               "elements -- ob_item and ob_size disagree (torn resize left the "
               "list internally inconsistent)".format(n, len(walked)))
        return
    for val in walked:
        if not value_ok(H, "post-walk", val):
            return
    counts["quiesced"][slot] += 1


def worker(H, wid, rng, state):
    counts = state["counts"]
    slot = wid & 1023
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        lst = fresh_list(rng)

        # Round-robin the sort direction (and a small iterator head-start) by
        # worker id in the first ops -- deterministic whether one worker does K
        # rounds or K workers do 1 -- so the clean-sort branch is reliably hit at
        # both reverse polarities even at low op-count under load; random after
        # that to preserve the concurrent mix.  (post() does NOT require the
        # detect branches, so there is no flaky-coverage hole; this is just for a
        # deterministic clean-path spread.)
        if i < 4:
            reverse = bool((wid + i) & 1)
            tight = bool(((wid + i) >> 1) & 1)
        else:
            reverse = bool(rng.getrandbits(1))
            tight = bool(rng.getrandbits(1))
        i += 1

        # gate: the sorter trips it the instant before its first sort; the mutator
        # and iterator wait on it, so their REPEAT loops begin overlapping the sort
        # loop from its start.
        gate = runloom.WaitGroup()
        gate.add(1)
        wg = runloom.WaitGroup()
        wg.add(3)
        mseed = rng.getrandbits(48)

        def do_sort(lst=lst, gate=gate, reverse=reverse):
            try:
                run_sorter(H, wid, lst, gate, rng, reverse, counts, slot)
            finally:
                wg.done()

        def do_mut(lst=lst, gate=gate, mseed=mseed):
            mrng = random.Random(mseed)
            try:
                run_mutator(H, wid, lst, gate, mrng, counts, slot)
            finally:
                wg.done()

        def do_iter(lst=lst, gate=gate, tight=tight):
            try:
                # A loose round gives the sort a small head start (varies the phase
                # of where the walk lands relative to a sort restore); a tight round
                # walks immediately.
                if not tight:
                    runloom.yield_now()
                run_iterator(H, wid, lst, gate, counts, slot)
            finally:
                wg.done()

        H.fiber(do_sort)
        H.fiber(do_mut)
        H.fiber(do_iter)
        wg.wait()

        # All three fibers have returned -> the list is now quiescent and
        # single-owned; run the conservation oracle on it.
        if not H.running() and H.failed:
            return
        quiescent_check(H, wid, lst, counts, slot)

        H.op(wid)
        H.task_done(wid)
        if H.failed:
            return


def setup(H):
    H.state = {"counts": {
        "sort_clean": [0] * 1024,
        "sort_detect": [0] * 1024,
        "iter_clean": [0] * 1024,
        "iter_detect": [0] * 1024,
        "mutated": [0] * 1024,
        "quiesced": [0] * 1024,
    }}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    c = H.state["counts"]
    sort_clean = sum(c["sort_clean"])
    sort_detect = sum(c["sort_detect"])
    iter_clean = sum(c["iter_clean"])
    iter_detect = sum(c["iter_detect"])
    mutated = sum(c["mutated"])
    quiesced = sum(c["quiesced"])
    H.log("sort_clean={0} sort_detect={1} iter_clean={2} iter_detect={3} "
          "mutated={4} quiesced={5} ops={6} (clean and detect are both legal; "
          "any out-of-universe element / conservation break already failed fast)"
          .format(sort_clean, sort_detect, iter_clean, iter_detect, mutated,
                  quiesced, H.total_ops()))
    # The race window must actually have been exercised: rounds completed, the
    # mutator ran, and the conservation oracle passed on real rounds.
    H.check(sort_clean + sort_detect > 0,
            "no sort completed -- the sort-vs-mutate race window was never "
            "exercised")
    H.check(iter_clean + iter_detect > 0,
            "no iteration completed -- the iterate-vs-mutate race window was "
            "never exercised")
    H.check(mutated > 0, "no round mutated the shared list")
    H.check(quiesced > 0,
            "no round reached the quiescent conservation check")
    H.require_no_lost()


if __name__ == "__main__":
    harness.main("p403_list_sort_mutate", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="shared list sorted on one hub while a sibling appends/"
                          "overwrites and a third iterates; every element in a "
                          "finite sentinel universe and len(lst)==len(list(it)) at "
                          "rest, tolerating only 'modified during sort' / 'changed "
                          "size during iteration' -- anything else is M:N list "
                          "corruption")
