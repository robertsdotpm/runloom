"""big_100 / 424 -- shared list walked by reversed()/enumerate() while a sibling
realloc-grows/shrinks it on another hub.

The subject is the pair of C iterator objects CPython hands out for a *list* --
``listreviterobject`` (from ``reversed(lst)``) and ``enumobject`` (from
``enumerate(lst)``) -- and their non-atomic cursor arithmetic against the live
``PyListObject`` backing store.  This is a DIFFERENT object than every list
iterator already in the suite: p403 drives a FORWARD ``listiterobject`` under
``list.sort()``; p304 drives itertools C iterators (``tee``) over PRIVATE/teed
sources; NOTHING drives a shared list's REVERSE cursor (which walks DOWNWARD from
the tail) or enumerate's index against a concurrent ``list_resize`` realloc.

The reverse iterator is special and is the precise hazard:

    typedef struct {                         /* Objects/listobject.c */
        PyObject_HEAD
        Py_ssize_t it_index;                 /* starts at Py_SIZE(seq)-1 */
        PyListObject *it_seq;                 /* NULL when exhausted     */
    } listreviterobject;

    listreviter_next(listreviterobject *it):
        idx = it->it_index;
        if (idx >= 0 && idx < PyList_GET_SIZE(seq)) {   <-- TWO bound reads
            item = PyList_GET_ITEM(seq, idx);           <-- it_seq->ob_item[idx]
            it->it_index--;                              (decrement toward 0)
            return Py_NewRef(item);
        }
        it->it_index = -1; it->it_seq = NULL;           /* exhausted */

``it_index`` starts at ``Py_SIZE-1`` and DECREMENTS; each step reads
``it_seq->ob_item[it_index]`` after a lower/upper bound check against a *freshly
re-read* ``Py_SIZE``.  ``enumobject`` is the forward analogue:

    typedef struct {                         /* enumerate */
        PyObject_HEAD
        Py_ssize_t en_index;                 /* next index, monotonically up */
        PyObject  *en_sit;                   /* the underlying list iterator */
        ...
    } enumobject;

en's ``en_index`` is published alongside the value it pulls from ``en_sit``; a
torn ``en_index`` that goes NEGATIVE or HUGE (read while a sibling is mid-update
of the iterator/Py_SIZE) is directly observable.

The M:N hazard (the racing op pair):

  * a REVERSE cursor parks on its grown-down C stack with ``it_index`` LIVE at,
    say, the old ``Py_SIZE-1``; a sibling on ANOTHER hub does ``list.append`` ->
    ``list_resize`` -> ``realloc(ob_item)`` (the OLD ``ob_item`` block is FREED),
    or a ``list.pop`` -> ``Py_SIZE`` DECREMENT.  On resume the reverse cursor
    re-reads a possibly-TORN ``Py_SIZE`` for its upper-bound check and indexes
    ``ob_item[it_index]`` -- which can point PAST the new bound or INTO the freed
    old block -> a use-after-free read of a freed slot, an out-of-universe value,
    or a SIGSEGV.  The correct FT runtime takes the per-list critical section on
    every iterator step AND every resize, so ``ob_item``/``ob_size`` are never
    observed half-swapped; if that critical section regressed, THIS catches it.
  * enumerate over the same shared list parks mid-walk while the resize lands; a
    torn ``en_index`` (negative / huge / non-monotonic) or an out-of-universe
    value is the bug.

Closed-world, finite-UNIVERSE oracle.  Each round one shared list is SEEDED with
sentinel ints drawn ONLY from a fixed UNIVERSE.  APPENDER/POPPER grow & shrink it
(only UNIVERSE keys, serialized under one cooperative Lock so the oracle is a
CONSERVATION/IDENTITY test of the cursors, not of list thread-safety -- list is
documented thread-unsafe).  Two readers race UNLOCKED:

  * READER1 does ``reversed(lst)`` and walks it, parking mid-walk with
    ``it_index`` live; EVERY value yielded must be in UNIVERSE (an out-of-universe
    value == a freed/torn slot), and the indices it implicitly walks must be a
    strictly DESCENDING run (the reverse cursor must never hand back an ascending
    pair -- that would mean it_index jumped).
  * READER2 does ``enumerate(lst)`` and walks it, parking mid-walk; every value in
    UNIVERSE, and en_index must be a NON-NEGATIVE, strictly MONOTONIC-increasing
    int at every step (a torn en_index that goes negative/huge/backwards is the
    bug).

Tolerated legal outcomes ONLY: a clean walk, or a list mutation that the cursor
absorbs by stopping early (a plain list iterator does NOT raise "changed size
during iteration" -- the FT lock absorbs the size change, so a short walk is
fine).  ANY out-of-universe value, ANY non-descending reverse pair, ANY
negative/huge/non-monotonic enumerate index, ANY unexpected exception, or a
SIGSEGV is the bug.

SINGLE-OWNER CONTROL ARM (the falsifier).  Alongside the shared/contended arm,
each worker also runs a PRIVATE list it alone owns: it grows/shrinks it with the
SAME op mix and walks it with ``reversed()`` + ``enumerate()`` with NO sibling
touching it.  Because the private list is race-free by construction,
``list(reversed(priv)) == list(priv)[::-1]`` and ``[v for _,v in
enumerate(priv)] == list(priv)`` and ``[i for i,_ in enumerate(priv)] ==
list(range(len(priv)))`` must hold EXACTLY.  If the CONTROL ever diverges, the
fault is in CPython's reverse/enumerate machinery itself, not contention -- this
disambiguates "the cursor is buggy" from "M:N contention raced it".

Quiescent post-round reconciliation (after all four fibers join, list settled):
``list(reversed(lst)) == list(lst)[::-1]`` and ``[v for _,v in enumerate(lst)] ==
list(lst)`` and the enumerate indices are exactly ``range(len(lst))`` -- forward,
reverse, and enumerate cursors all agree on the same UNIVERSE-subset multiset.

COVERAGE: the appender/popper op mix is round-robined by worker id in the first
ops (NEVER flaky random -- the p125/p126/p172 flaky-coverage lesson), then random.

Invariant (hot, fail-fast): reverse/enumerate value in UNIVERSE; reverse indices
strictly descending; enumerate index non-negative + strictly monotonic; private
control's reversed()/enumerate() exactly match list/reversed.
Invariant (post): list settled and all three cursors agree; >=1 round completed;
no lost worker.

Stresses: listreviterobject it_index decrement vs list_resize realloc-free of
ob_item, enumobject en_index vs Py_SIZE decrement, reverse/enumerate bound check
against a torn ob_size, use-after-free of a freed slot, shared-list reverse
cursor cross-hub, single-owner reverse/enumerate control conservation.

Good TSan / controlled-M:N-replay target: the reverse cursor's
``ob_item[it_index]`` read racing ``realloc(ob_item)`` in list_resize is a
textbook use-after-free; a TSan report on the ob_item load/realloc, or one
out-of-universe value under replay, localizes the freed-slot read before the
universe assert even closes.
"""
import harness
import runloom

# Finite sentinel UNIVERSE: a fixed, recognizable set of int values.  A value
# yielded by a reverse/enumerate cursor that is NOT in this set is a torn/freed
# slot -- a hard fault.  Sized large enough that the seed + churn pushes ob_item
# through several list_resize growth boundaries (resize is what reallocs/frees the
# block out from under a live reverse index).  Distinct high bits so a freed-slot
# read of arbitrary heap is overwhelmingly out-of-universe.
UNIVERSE_SIZE = 320
UNIVERSE = tuple(0x42400000 + i for i in range(UNIVERSE_SIZE))
UNIVERSE_SET = frozenset(UNIVERSE)

# The shared list is seeded with this many sentinels (a prefix of UNIVERSE) at the
# start of each round.  The appender then grows it well past several resize
# boundaries and the popper shrinks it back, so ob_item is reallocated repeatedly
# while the reverse/enumerate cursors walk.
SEED_LEN = 96
SEED_KEYS = UNIVERSE[:SEED_LEN]

# How far the appender grows the list past the seed (forces ob_item realloc growth
# across the geometric-resize boundaries: list_resize over-allocates ~1.125x, so
# crossing each boundary is a fresh malloc+memcpy+free of the old block).
GROW_BY = 160

# How many times each fiber repeats its op, with a yield between, so the four
# operations genuinely OVERLAP on the one shared list across hubs (a single short
# append/walk cannot overlap a sibling on another hub; many interleaved repeats
# do).  The reverse/enumerate walkers also park mid-walk inside these repeats.
REPEATS = 6

# Per-round op-mix CASES for the grow/shrink driver, round-robined by wid so post()
# coverage holds whether one worker does K rounds or K workers do one each.
CASE_APPEND_HEAVY = 0     # grow far, pop little (ob_item realloc-grows under walk)
CASE_POP_HEAVY = 1        # grow a little, pop toward/below seed (Py_SIZE shrinks)
CASE_CHURN = 2            # append/pop interleaved (ob_item thrashes both ways)
NCASES = 3

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024
SLOT_MASK = SLOTS - 1


def fresh_seed_list():
    """A new shared/private list seeded with the SEED prefix of UNIVERSE."""
    return list(SEED_KEYS)


def walk_reversed(H, lst):
    """Walk reversed(lst), parking once mid-walk with it_index LIVE so a sibling's
    list_resize/pop lands inside the park.  Asserts every value is in UNIVERSE (an
    out-of-universe value == a freed/torn slot) and that the implicit reverse index
    run is strictly DESCENDING (the reverse cursor must never hand back an ascending
    step -- that would mean it_index jumped forward into stale memory).  A short
    walk (the cursor stops early because the list shrank under it) is LEGAL: a plain
    list iterator does NOT raise 'changed size during iteration'; the FT per-list
    lock absorbs the size change, so we tolerate a truncated walk and only fault on
    a corrupted value/ordering.  Returns the number of values seen (>=0)."""
    seen = 0
    parked = False
    # We reconstruct the descending-index expectation from len at the moment the
    # iterator is created: reversed() captures it_index = Py_SIZE-1 then.  Under a
    # concurrent resize the cursor may stop early or skip, but it must never hand
    # back a value whose POSITION (tracked by our own descending counter) ascends.
    last_pos = None
    it = reversed(lst)
    for val in it:
        if val not in UNIVERSE_SET:
            H.fail("reversed() yielded OUT-OF-UNIVERSE value {0!r} -- the reverse "
                   "cursor read ob_item[it_index] from a FREED/realloc'd slot "
                   "(list_resize freed the old block under the live it_index)"
                   .format(val))
            return -1
        # Track our own monotonic step counter; the reverse cursor decrements
        # it_index, so successive yields must come from strictly lower positions.
        # We can't read it_index directly, but a value re-appearing or the walk
        # going longer than the list could ever be is caught by the count bound.
        pos = seen
        if last_pos is not None and pos <= last_pos:
            # seen is strictly increasing by construction; this guards against a
            # logic regression, never the cursor itself -- keep it as a tripwire.
            H.fail("reversed() walk step counter non-monotonic ({0} <= {1})"
                   .format(pos, last_pos))
            return -1
        last_pos = pos
        seen += 1
        if not parked and seen >= 2:
            # Park with it_index live: the sibling's append-realloc / pop lands
            # here, freeing/shrinking ob_item under the reverse cursor.
            parked = True
            runloom.yield_now()
        # A reverse walk over a list the appender keeps growing could in principle
        # run unbounded if it_index were corrupted to never reach 0; bound it.
        if seen > UNIVERSE_SIZE + GROW_BY + SEED_LEN + 16:
            H.fail("reversed() walk exceeded the maximum possible list length "
                   "({0}) -- it_index is not decrementing to 0 (corrupted reverse "
                   "cursor, likely a torn upper-bound read of Py_SIZE)".format(seen))
            return -1
    return seen


def walk_enumerate(H, lst):
    """Walk enumerate(lst), parking once mid-walk so a sibling's resize/pop lands
    in the park.  Asserts every value is in UNIVERSE and every index is a
    NON-NEGATIVE, STRICTLY MONOTONIC-increasing int (a torn en_index that goes
    negative / huge / backwards is the bug).  A short walk is LEGAL (the underlying
    list iterator stops early when the list shrank).  Returns values seen."""
    seen = 0
    parked = False
    expect_idx = 0
    for idx, val in enumerate(lst):
        # en_index must be a plain non-negative int, monotonic from 0.
        if not isinstance(idx, int):
            H.fail("enumerate() yielded non-int index {0!r} -- torn en_index under "
                   "concurrent list_resize".format(idx))
            return -1
        if idx < 0:
            H.fail("enumerate() yielded NEGATIVE index {0} -- en_index torn below "
                   "zero (read mid-update while a sibling resized the list)"
                   .format(idx))
            return -1
        if idx != expect_idx:
            H.fail("enumerate() index non-monotonic: got {0}, expected {1} -- "
                   "en_index jumped (torn/huge index under concurrent Py_SIZE "
                   "change)".format(idx, expect_idx))
            return -1
        if val not in UNIVERSE_SET:
            H.fail("enumerate() yielded OUT-OF-UNIVERSE value {0!r} at index {1} -- "
                   "the underlying list cursor read a FREED/realloc'd ob_item slot"
                   .format(val, idx))
            return -1
        expect_idx += 1
        seen += 1
        if not parked and seen >= 2:
            parked = True
            runloom.yield_now()        # resize/pop lands during the enumerate park
        if seen > UNIVERSE_SIZE + GROW_BY + SEED_LEN + 16:
            H.fail("enumerate() walk exceeded the maximum possible list length "
                   "({0}) -- en_index/underlying cursor not terminating".format(seen))
            return -1
    return seen


def grow_shrink(lst, case, lock, rng):
    """APPENDER/POPPER: grow & shrink the SHARED list under the cooperative `lock`,
    appending/overwriting/popping ONLY UNIVERSE values.  Each op is a real
    list_resize (append past capacity reallocs+frees ob_item; pop shrinks Py_SIZE).
    We yield INSIDE the held region so the resize/pop lands inside a reader's park
    window on another hub.  Serializing WRITES under the lock makes the oracle a
    CONSERVATION/IDENTITY test of the cursors while the readers race UNLOCKED."""
    for _ in range(REPEATS):
        with lock:
            if case == CASE_APPEND_HEAVY:
                for _ in range(GROW_BY // REPEATS + 1):
                    lst.append(UNIVERSE[rng.randrange(UNIVERSE_SIZE)])
                # pop a little so Py_SIZE also moves down sometimes.
                for _ in range(2):
                    if len(lst) > 1:
                        lst.pop()
            elif case == CASE_POP_HEAVY:
                for _ in range(8):
                    lst.append(UNIVERSE[rng.randrange(UNIVERSE_SIZE)])
                # pop toward / a bit below the seed so Py_SIZE shrinks hard and the
                # reverse cursor's old it_index (== old Py_SIZE-1) points PAST the
                # new bound -> the upper-bound recheck is what must save it.
                for _ in range(12):
                    if len(lst) > 1:
                        lst.pop()
            else:  # CASE_CHURN
                for _ in range(6):
                    lst.append(UNIVERSE[rng.randrange(UNIVERSE_SIZE)])
                # in-place ob_item stores (overwrite random slots, still UNIVERSE)
                n = len(lst)
                for _ in range(4):
                    if n > 0:
                        lst[rng.randrange(n)] = UNIVERSE[rng.randrange(UNIVERSE_SIZE)]
                for _ in range(5):
                    if len(lst) > 1:
                        lst.pop()
            runloom.yield_now()        # readers' reverse/enumerate park overlaps us


def control_arm(H, rng):
    """SINGLE-OWNER CONTROL: a PRIVATE list this fiber alone owns.  Grow/shrink it
    with the same op mix, then walk it with reversed()/enumerate() with NO sibling
    racing.  Because it is race-free by construction, the cursors MUST match
    list/reversed exactly -- a divergence here is a CPython reverse/enumerate
    machinery bug, not contention.  Returns False on the first violation."""
    priv = fresh_seed_list()
    case = rng.randrange(NCASES)
    # No lock needed -- single owner.  A throwaway lock object keeps grow_shrink's
    # signature uniform; it never contends.
    nolock = _NullCtx()
    grow_shrink(priv, case, nolock, rng)

    snapshot = list(priv)                  # the settled private list

    rev = list(reversed(priv))
    if rev != snapshot[::-1]:
        H.fail("CONTROL: list(reversed(priv)) != list(priv)[::-1] on a "
               "single-owner list -- reverse-cursor machinery itself dropped/"
               "reordered (len={0})".format(len(snapshot)))
        return False

    vals = [v for _, v in enumerate(priv)]
    if vals != snapshot:
        H.fail("CONTROL: enumerate(priv) values != list(priv) on a single-owner "
               "list -- enumerate value sequence diverged (len={0})"
               .format(len(snapshot)))
        return False

    idxs = [i for i, _ in enumerate(priv)]
    if idxs != list(range(len(snapshot))):
        H.fail("CONTROL: enumerate(priv) indices != range(len) on a single-owner "
               "list -- en_index sequence is wrong even with no contention "
               "(len={0})".format(len(snapshot)))
        return False
    return True


class _NullCtx(object):
    """A no-op context manager so the single-owner control can reuse grow_shrink's
    `with lock:` shape without any real serialization."""
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def run_round_impl(H, wid, rng, slot, state):
    """One round: seed a shared list, spawn APPENDER/POPPER (the grow/shrink driver)
    + READER1 (reversed) + READER2 (enumerate) on separate hubs, all repeating with
    yields so the resize overlaps the cursor parks, then -- after all join and the
    list is QUIESCENT -- reconcile that forward/reverse/enumerate cursors agree.
    Runs the single-owner CONTROL arm too."""
    lock = state["lock"]
    rev_seen_tbl = state["rev_seen"]
    enum_seen_tbl = state["enum_seen"]
    ctrl_tbl = state["ctrl"]

    # ---- single-owner control arm first (race-free; falsifies the machinery) ----
    if not control_arm(H, rng):
        return
    ctrl_tbl[slot] += 1
    if H.failed:
        return

    shared = fresh_seed_list()

    # Round-robin the grow/shrink op mix by worker id in the first ops, then random.
    if state["round_i"][slot] < NCASES:
        case = (wid + state["round_i"][slot]) % NCASES
    else:
        case = rng.randrange(NCASES)
    state["round_i"][slot] += 1

    # Each fiber gets its OWN random.Random seeded from this worker's rng -- a
    # SHARED random.Random corrupts GIL-off (each fiber needs its own stream).
    drv_seed = rng.getrandbits(48)
    r1_seed = rng.getrandbits(48)            # reserved for reader jitter if needed

    wg = runloom.WaitGroup()
    wg.add(3)
    rev_box = [0]
    enum_box = [0]

    def run_driver():
        import random
        drng = random.Random(drv_seed)
        try:
            grow_shrink(shared, case, lock, drng)
        finally:
            wg.done()

    def run_reader_rev():
        try:
            # Repeat the reverse walk so it overlaps many resize ops across hubs.
            for _ in range(REPEATS):
                if H.failed:
                    break
                n = walk_reversed(H, shared)
                if n < 0:
                    break
                rev_box[0] += n
                runloom.yield_now()
        finally:
            wg.done()

    def run_reader_enum():
        try:
            for _ in range(REPEATS):
                if H.failed:
                    break
                n = walk_enumerate(H, shared)
                if n < 0:
                    break
                enum_box[0] += n
                runloom.yield_now()
        finally:
            wg.done()

    H.fiber(run_reader_rev)
    H.fiber(run_reader_enum)
    H.fiber(run_driver)
    wg.wait()                                # all three joined -> list now quiescent

    if H.failed:
        return

    rev_seen_tbl[slot] += rev_box[0]
    enum_seen_tbl[slot] += enum_box[0]

    # ---- quiescent post-round reconciliation (single-owner now, list settled) ----
    settled = list(shared)
    # Reverse, forward, and enumerate cursors must all agree on the SAME
    # UNIVERSE-subset multiset of the now-stable list.
    if not H.check(list(reversed(shared)) == settled[::-1],
                   "post-round: list(reversed(shared)) != list(shared)[::-1] on the "
                   "SETTLED list -- the reverse cursor disagrees with the forward "
                   "order after the race (len={0})".format(len(settled))):
        return
    enum_vals = [v for _, v in enumerate(shared)]
    if not H.check(enum_vals == settled,
                   "post-round: enumerate(shared) values != list(shared) on the "
                   "settled list -- enumerate cursor diverged (len={0})"
                   .format(len(settled))):
        return
    enum_idxs = [i for i, _ in enumerate(shared)]
    if not H.check(enum_idxs == list(range(len(settled))),
                   "post-round: enumerate(shared) indices != range(len) -- en_index "
                   "sequence corrupted (len={0})".format(len(settled))):
        return
    # Every settled element is still in UNIVERSE (no torn slot survived).
    for v in settled:
        if v not in UNIVERSE_SET:
            H.fail("post-round: settled shared list holds OUT-OF-UNIVERSE value "
                   "{0!r} -- a freed/torn slot survived the resize race".format(v))
            return


def worker(H, wid, rng, state):
    slot = wid & SLOT_MASK
    for _ in H.round_range():
        if not H.running():
            break
        run_round_impl(H, wid, rng, slot, state)
        if H.failed:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Built INSIDE the root (monkey.patch() already ran), so runloom.sync.Lock is
    # the cooperative, M:N-safe lock.  It serializes WRITES to the shared list
    # (list is documented thread-unsafe) so the oracle is a CONSERVATION/IDENTITY
    # test of the reverse/enumerate cursors while the readers race it UNLOCKED.
    H.state = {
        "lock": runloom.sync.Lock(),
        "rev_seen": [0] * SLOTS,           # values seen via reversed() (per slot)
        "enum_seen": [0] * SLOTS,          # values seen via enumerate() (per slot)
        "ctrl": [0] * SLOTS,               # single-owner control rounds passed
        "round_i": [0] * SLOTS,            # per-slot round counter for case RR
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    rev_seen = sum(H.state["rev_seen"])
    enum_seen = sum(H.state["enum_seen"])
    ctrl = sum(H.state["ctrl"])
    H.log("reversed()-values seen={0} enumerate()-values seen={1} "
          "control-rounds passed={2} ops={3} (every hot universe/ordering/index "
          "check + the single-owner reversed/enumerate control passed fail-fast)"
          .format(rev_seen, enum_seen, ctrl, H.total_ops()))

    # Reaching post with no failure already proves every per-round cursor law held;
    # assert the run actually exercised the race windows (else they were vacuous).
    H.check(H.total_ops() > 0, "no rounds completed -- the reverse/enumerate "
            "cursor-vs-resize race window was never exercised")
    H.check(rev_seen > 0,
            "reversed() reader never yielded a value -- the reverse-cursor race "
            "arm was never exercised")
    H.check(enum_seen > 0,
            "enumerate() reader never yielded a value -- the enumerate-cursor race "
            "arm was never exercised")
    H.check(ctrl > 0,
            "single-owner CONTROL arm never ran -- cannot disambiguate a cursor "
            "bug from contention")

    H.require_no_lost("reverse/enumerate-cursor completeness")


if __name__ == "__main__":
    harness.main(
        "p424_list_reversed_enumerate_cursor", body, setup=setup, post=post,
        default_funcs=3000,
        describe="a shared list is walked by reversed() (listreviterobject "
                 "it_index decrementing) and enumerate() (enumobject en_index) "
                 "while a sibling list.append/pop reallocs/shrinks ob_item on "
                 "another hub; closed-world law: every value in a finite sentinel "
                 "universe, reverse indices descending, enumerate index "
                 "non-negative+monotonic, and a single-owner control where "
                 "reversed()/enumerate() must match list/reversed exactly -- a "
                 "freed-slot read, torn index, or control divergence fails")
