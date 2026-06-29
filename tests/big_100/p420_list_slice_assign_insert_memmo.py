"""big_100 / 420 -- list ob_item memmove (list_ass_slice / ins1) vs a live iterator.

The subject is CPython's ``listobject.c`` and the two mutators that MEMMOVE the
``PyListObject.ob_item`` array to open or close a gap in the MIDDLE of a list,
something the existing list program (p403) never drives -- p403 hammers
sort()/append()/__setitem__/pop(), all of which touch the array END or rewrite it
wholesale, but NEVER the mid-array memmove paths:

  * ``lst.insert(i, x)`` -> ``ins1()`` : after a possible ``list_resize()`` (which
    may ``PyMem_Realloc`` ob_item to a NEW base, FREEING the old block), it does
        ``memmove(&ob_item[i+1], &ob_item[i], (n - i)*sizeof(PyObject*))``
    to shift the tail up one slot, THEN stores ``Py_SET_SIZE(self, n+1)``.
  * ``lst[i:j] = seq`` and ``del lst[i:j]`` -> ``list_ass_slice()`` : computes the
    new length, may ``list_resize()`` (realloc/free of the old ob_item), then
        ``memmove(&ob_item[ihigh+d], &ob_item[ihigh], tail*sizeof(PyObject*))``
    to slide the tail, copies the new items in, and only AFTER that updates
    ``Py_SET_SIZE``.

The exact non-atomic C state under attack is therefore: the in-flight
``memmove`` over ``ob_item`` PLUS the SEPARATE, not-yet-committed ``ob_size``
(``Py_SET_SIZE``) store.  Between "memmove started" and "ob_size updated" the
array contents and the length field momentarily DISAGREE -- a slot is duplicated
or a torn region exists, and ob_size may name slots the memmove has not yet
populated.  Worse, if ins1/list_ass_slice went through list_resize, the OLD
ob_item block has been ``PyMem_Realloc``'d (potentially FREED) while another
fiber may still hold ``it_seq->ob_item + it_index`` from before the realloc.

The M:N hazard: a ``list_iterator`` (or ``reversed()`` -> ``list_reverseiterator``)
holds a RAW base+index into ob_item (``it->it_seq``, ``it->it_index``) and is NOT
a snapshot -- ``listiter_next`` re-reads ``Py_SIZE(seq)`` and ``seq->ob_item[idx]``
on every step.  Under M:N that iterator can PARK mid-walk (its index live, on a
grown-down C stack) while a SIBLING on another hub runs insert(0,..) /
``lst[i:j]=[...]`` / ``del lst[i:j]``, memmoving every element under the live
index or reallocating ob_item out from under it.  On resume the iterator reads
through a stale base or a half-memmoved slot and can hand back:
  - an OUT-OF-UNIVERSE value (a freed/half-written slot -- the hard fault), or
  - a torn region, or a SIGSEGV dereferencing a realloc'd-away ob_item.
The only thing standing between this and corruption is the per-list critical
section that must cover the memmove + the ob_size store as one indivisible unit.

CLOSED-WORLD, FALSIFIABLE oracle.  A shared list is seeded from a fixed sentinel
UNIVERSE; every value the list ever holds is a UNIVERSE key, so a value OUTSIDE
the universe is a freed / half-memmoved slot.  Per round the worker owns one
shared list and spawns four goroutines gated so the mutation lands INSIDE the
iterator park window:
  * SHIFTER: insert(0, key) / lst[i:j]=[keys] / del lst[i:j]  (the memmove paths);
  * MUTATOR: append(key) / pop()  (END churn that forces list_resize realloc);
  * ITERATOR: for v in lst -- parks mid-walk; every v must be in UNIVERSE;
  * REVERSED: for v in reversed(lst) -- the list_reverseiterator counts DOWN, a
    distinct ob_item access pattern; same universe membership.

Legal iterator outcomes are exactly two (mirroring p311): a clean walk in which
every value is in UNIVERSE, OR a ``RuntimeError`` -- but note CPython's list does
NOT raise "changed size during iteration", so a size shrink merely makes the
iterator stop early (legal); we tolerate RuntimeError if it ever appears but do
not require it.  ANY out-of-universe value, ANY other exception type, or a
SIGSEGV is the bug.

QUIESCENT POST-RECONCILIATION (after all four join, list provably still):
  len(lst) == len(list(lst)) == len(list(reversed(lst)))  -- ob_item and ob_size
  agree FORWARD and BACKWARD (a torn-length window left ob_size naming slots the
  forward or reverse walk disagrees on); every resident value in UNIVERSE.

SINGLE-OWNER CONTROL ARM (the falsifier).  One private fiber per round runs the
IDENTICAL insert(0,key)/lst[i:j]=[..]/del lst[i:j]/append/pop sequence on a
PRIVATE list with NO other accessor.  A single-owner list is race-free by
construction, so it MUST end as a permutation of a UNIVERSE subset with
forward-length == reverse-length and every value in UNIVERSE.  If the CONTROL
loses/duplicates a slot or its fwd!=rev length, the fault is in CPython's
memmove/resize machinery itself, not contention -- this disambiguates "list_ass_slice
is buggy" from "M:N contention tore a shared slot".

Stresses: list_ass_slice/ins1 ob_item memmove vs concurrent list/reversed
iterator raw base+index read, list_resize PyMem_Realloc-free under a live
iterator, torn ob_size (Py_SET_SIZE) window, fwd/rev length agreement, out-of-
universe (freed/half-memmoved) slot publication under M:N.

Good TSan / controlled-M:N-replay target: the memmove write over ob_item vs the
iterator's ob_item[idx] read is a textbook data race; a TSan report on the
ob_item store/load (or a single out-of-universe value under replay) localizes the
torn slot before the universe-membership assert even fires.
"""
import random

import harness
import runloom

# Finite sentinel UNIVERSE: a fixed, recognizable set of values.  Any value a list
# ever holds is drawn ONLY from here, so a value an iterator yields that is NOT in
# this set is a freed / half-memmoved / torn ob_item slot -- a hard fault.  Sized
# to push the list through several list_resize growth boundaries (the over-
# allocation doubling that triggers PyMem_Realloc, which is what frees the old
# ob_item out from under a live iterator).
UNIVERSE_SIZE = 256
UNIVERSE = tuple(0x42000000 + i for i in range(UNIVERSE_SIZE))
UNIVERSE_SET = frozenset(UNIVERSE)

# Half the universe seeds the list; the other half is the churn pool the SHIFTER /
# MUTATOR insert/append, so every value that ever enters the list is still in
# UNIVERSE (a clean walk that catches a freshly-inserted value is still legal)
# while real size changes (and thus memmoves + reallocs) are guaranteed.
SEED_VALUES = UNIVERSE[: UNIVERSE_SIZE // 2]
CHURN_VALUES = UNIVERSE[UNIVERSE_SIZE // 2:]

# The mid-list memmove CASES the SHIFTER round-robins (NOT random -- pure random
# selection reliably MISSES a case at low op-count under load, the flaky-coverage
# bug the suite already had to fix in p125/p126/p172).  post() relies on the
# deterministic round-robin guaranteeing each ran once tally>0.
CASE_INSERT_HEAD = 0   # lst.insert(0, key)        -> ins1, memmove whole tail up
CASE_SLICE_ASSIGN = 1  # lst[i:j] = [keys]          -> list_ass_slice, memmove + grow
CASE_SLICE_DEL = 2     # del lst[i:j]               -> list_ass_slice, memmove tail down
CASE_INSERT_MID = 3    # lst.insert(mid, key)       -> ins1, memmove partial tail
NCASES = 4

# How many memmove-driving ops the SHIFTER does per round, with a yield between
# each so the iterator/reversed walk lands inside the half-memmoved window.
SHIFT_OPS = 6
# END-churn ops the MUTATOR does (append/pop) -- drives list_resize doublings.
CHURN_OPS = 8


def fresh_list():
    """A list seeded from the UNIVERSE.  Returned to both the shared round and the
    private control so the two arms run the identical starting state."""
    return list(SEED_VALUES)


def shifter_step(lst, case, key, rng):
    """Do ONE mid-list memmove operation on `lst`, selected by `case`.  These are
    exactly the ins1 / list_ass_slice paths: the tail of ob_item is memmove'd to
    open/close a gap and ob_size is updated separately.  All inserted values are
    drawn from UNIVERSE so the closed-world oracle holds."""
    n = len(lst)
    if case == CASE_INSERT_HEAD:
        # ins1 at index 0: memmove the ENTIRE ob_item array up one slot (after a
        # possible list_resize realloc) -- the maximal-overlap memmove.
        lst.insert(0, key)
    elif case == CASE_SLICE_ASSIGN:
        # list_ass_slice growing: replace a mid 1-element slice with several keys,
        # which memmoves the tail UP and may list_resize-realloc ob_item.
        if n >= 2:
            i = rng.randrange(n - 1)
            lst[i:i + 1] = [key, key]
        else:
            lst.insert(0, key)
    elif case == CASE_SLICE_DEL:
        # list_ass_slice shrinking: delete a mid slice, memmove the tail DOWN over
        # the deleted region; ob_size shrinks separately.
        if n >= 3:
            i = rng.randrange(n - 2)
            j = min(n, i + rng.randint(1, 2))
            del lst[i:j]
        elif n >= 1:
            del lst[0:1]
    elif case == CASE_INSERT_MID:
        # ins1 at a mid index: memmove only the tail from `mid` up one slot.
        mid = n // 2
        lst.insert(mid, key)


def run_shifter_sequence(lst, wid, rng):
    """Run SHIFT_OPS memmove ops, round-robining the cases by id so each memmove
    path is exercised, yielding between each so a parked iterator's resume lands
    in a half-memmoved window.  Returns the list of cases run (for the control
    arm to replay identically)."""
    cases = []
    for i in range(SHIFT_OPS):
        case = (wid + i) % NCASES
        key = CHURN_VALUES[rng.randrange(len(CHURN_VALUES))]
        shifter_step(lst, case, key, rng)
        cases.append((case, key))
        runloom.yield_now()           # iterator/reversed resumes mid-memmove window
    return cases


def replay_shifter_sequence(lst, cases, rng):
    """Replay the EXACT (case, key) sequence on a PRIVATE list for the control
    arm.  Same memmove paths, no concurrent accessor."""
    for case, key in cases:
        # Re-derive the same kind of op; the private list's length differs from the
        # shared list's only by concurrent churn, but the OP KIND is identical so
        # the same ins1/list_ass_slice C path runs race-free here.
        shifter_step(lst, case, key, rng)


def run_mutator(lst, wid, rng):
    """END churn: append(key) / pop().  append grows ob_size and triggers
    list_resize PyMem_Realloc doublings (freeing the old ob_item under a live
    iterator); pop shrinks.  All values from UNIVERSE."""
    for i in range(CHURN_OPS):
        if (i & 1) == 0 or len(lst) == 0:
            lst.append(CHURN_VALUES[rng.randrange(len(CHURN_VALUES))])
        else:
            lst.pop()
        runloom.yield_now()


def trip_once(gate, tripped):
    """Release `gate` exactly once across the whole iterator lifetime.  `tripped`
    is a one-element list used as a single-fiber flag (the iterator is the sole
    writer of it), so a clean short walk and the finally-path can never double-
    done() a WaitGroup whose count is 1."""
    if not tripped[0]:
        tripped[0] = True
        gate.done()


def walk_forward(H, wid, lst, gate, tripped):
    """Walk `for v in lst` -- list_iterator counts UP, re-reading ob_item[idx] each
    step.  Trips `gate` just before parking so the SHIFTER's memmove provably lands
    DURING the park, then checks every value is in UNIVERSE.  Returns 'clean' |
    'runtimeerror' | 'fail'.  A list iterator does not raise 'changed size during
    iteration', so a shrink merely ends the walk early (legal); RuntimeError is
    tolerated if it ever appears but not required."""
    seen = 0
    parked = False
    try:
        for v in lst:
            if v not in UNIVERSE_SET:
                H.fail("forward iterator yielded OUT-OF-UNIVERSE value {0!r} -- a "
                       "freed/half-memmoved ob_item slot read through a stale base "
                       "or torn region during list_ass_slice/ins1 on another hub"
                       .format(v))
                return "fail"
            seen += 1
            if not parked and seen >= 2:
                parked = True
                trip_once(gate, tripped)  # release the SHIFTER/MUTATOR
                runloom.yield_now()       # park with it_index live -> memmove lands
        return "clean"
    except RuntimeError:
        return "runtimeerror"


def walk_reversed(H, wid, lst, gate, tripped):
    """Walk `for v in reversed(lst)` -- list_reverseiterator counts DOWN from
    Py_SIZE-1, a DISTINCT ob_item access pattern (the reverse iterator is more
    sensitive to an ob_size that shrank mid-walk: its index can momentarily point
    past a shrunk array).  Same universe membership; same park protocol."""
    seen = 0
    parked = False
    try:
        for v in reversed(lst):
            if v not in UNIVERSE_SET:
                H.fail("reversed() iterator yielded OUT-OF-UNIVERSE value {0!r} -- "
                       "a freed/half-memmoved ob_item slot or an index past a "
                       "torn-shrunk ob_size during list_ass_slice on another hub"
                       .format(v))
                return "fail"
            seen += 1
            if not parked and seen >= 2:
                parked = True
                trip_once(gate, tripped)
                runloom.yield_now()
        return "clean"
    except RuntimeError:
        return "runtimeerror"


def run_round_impl(H, wid, rng, slot, state):
    """One round: build a shared list, spawn SHIFTER + MUTATOR + ITERATOR +
    REVERSED gated so the memmove lands in the iterators' park windows, join them
    ALL, then run the quiescent fwd/rev length-agreement + universe reconciliation
    and the single-owner control arm.

    The shared list's mid-list memmove paths are NOT internally locked against the
    iterator GIL-off, so we serialize the WRITERS (shifter + mutator) under a
    cooperative lock to make the oracle a test of MEMORY SAFETY + length agreement
    (did ob_item/ob_size ever disagree as seen by a reader) rather than of list's
    thread-safety -- which is documented as absent.  The iterators hold NO lock:
    that is the memmove-vs-iterate race the test actually probes.  We yield INSIDE
    the held writer region so a memmove overlaps the iterator's resume on another
    hub."""
    counts = state["counts"]
    lock = state["lock"]

    lst = fresh_list()

    # Two gates: one per iterator, each tripped the instant before that iterator
    # parks.  The SHIFTER/MUTATOR wait on BOTH so their memmoves provably land
    # while at least one iterator is parked mid-walk.
    fwd_gate = runloom.WaitGroup()
    fwd_gate.add(1)
    rev_gate = runloom.WaitGroup()
    rev_gate.add(1)

    wg = runloom.WaitGroup()
    wg.add(4)

    # Per-fiber RNG seeds (a SHARED random.Random corrupts GIL-off -- each fiber
    # gets its own).  We also capture the SHIFTER's exact case/key sequence so the
    # control arm replays it identically.
    shift_seed = rng.getrandbits(48)
    mut_seed = rng.getrandbits(48)
    ctrl_seed = rng.getrandbits(48)
    shifter_cases = [None]            # filled by the shifter fiber, read after join

    # One-shot trip flags: each iterator is the SOLE writer of its own flag, so the
    # gate is released exactly once whether the walk parked, finished early, or
    # failed -- never double-done() (the deadlock a double-done on a count-1
    # WaitGroup caused).  The finally-path guarantees the writers are never wedged.
    fwd_tripped = [False]
    rev_tripped = [False]

    def run_shift():
        srng = random.Random(shift_seed)
        try:
            fwd_gate.wait()           # let an iterator enter its park first
            rev_gate.wait()
            with lock:
                shifter_cases[0] = run_shifter_sequence(lst, wid, srng)
        finally:
            wg.done()

    def run_mut():
        mrng = random.Random(mut_seed)
        try:
            fwd_gate.wait()
            rev_gate.wait()
            with lock:
                run_mutator(lst, wid, mrng)
        finally:
            wg.done()

    def run_fwd():
        try:
            r = walk_forward(H, wid, lst, fwd_gate, fwd_tripped)
            if r == "clean":
                counts["fwd_clean"][slot] += 1
            elif r == "runtimeerror":
                counts["rterror"][slot] += 1
        except Exception as exc:        # noqa: BLE001
            H.fail("forward iterator raised non-RuntimeError {0}: {1} -- not a "
                   "legal list-iteration outcome (a torn ob_item/ob_size fault)"
                   .format(type(exc).__name__, exc))
        finally:
            # If the walk was too short to ever park (or failed), release the gate
            # here so the writers never block forever.
            trip_once(fwd_gate, fwd_tripped)
            wg.done()

    def run_rev():
        try:
            r = walk_reversed(H, wid, lst, rev_gate, rev_tripped)
            if r == "clean":
                counts["rev_clean"][slot] += 1
            elif r == "runtimeerror":
                counts["rterror"][slot] += 1
        except Exception as exc:        # noqa: BLE001
            H.fail("reversed() iterator raised non-RuntimeError {0}: {1} -- not a "
                   "legal list-iteration outcome (a torn ob_item/ob_size fault)"
                   .format(type(exc).__name__, exc))
        finally:
            trip_once(rev_gate, rev_tripped)
            wg.done()

    H.fiber(run_fwd)
    H.fiber(run_rev)
    H.fiber(run_shift)
    H.fiber(run_mut)
    wg.wait()                           # all four joined -> list provably quiescent

    if H.failed:
        return

    # ---- quiescent fwd/rev length-agreement reconciliation --------------------
    # ob_item and ob_size must agree FORWARD and BACKWARD now that no one mutates.
    n = len(lst)
    fwd = list(lst)
    rev = list(reversed(lst))
    if not H.check(len(fwd) == n,
                   "post-quiescent length disagreement: len(lst)={0} but "
                   "len(list(lst))={1} -- ob_size and the forward ob_item walk "
                   "disagree (a torn Py_SET_SIZE window from list_ass_slice/ins1 "
                   "left ob_size naming a slot the iterator can't reach)"
                   .format(n, len(fwd))):
        return
    if not H.check(len(rev) == n,
                   "post-quiescent length disagreement: len(lst)={0} but "
                   "len(list(reversed(lst)))={1} -- ob_size and the REVERSE "
                   "ob_item walk disagree (torn ob_size vs the reverse cursor)"
                   .format(n, len(rev))):
        return
    if not H.check(fwd == rev[::-1],
                   "post-quiescent fwd/rev content disagreement: the forward walk "
                   "and the reversed walk are not mirror images -- a slot was "
                   "duplicated or torn by an uncovered ob_item memmove"):
        return
    for v in fwd:
        if v not in UNIVERSE_SET:
            H.fail("shared list holds OUT-OF-UNIVERSE value {0!r} after the round "
                   "-- a freed/half-memmoved slot survived in ob_item".format(v))
            return

    # ---- single-owner CONTROL arm (the falsifier) -----------------------------
    # Replay the IDENTICAL shifter sequence + an equivalent end-churn on a PRIVATE
    # list with NO other accessor.  Race-free by construction: it MUST end as a
    # permutation of a UNIVERSE subset with fwd-length == rev-length.  A loss /
    # duplication / fwd!=rev HERE is a CPython memmove/resize bug, not contention.
    cases = shifter_cases[0]
    if cases is not None:
        ctrl = fresh_list()
        crng = random.Random(ctrl_seed)
        replay_shifter_sequence(ctrl, cases, crng)
        run_mutator(ctrl, wid, random.Random(ctrl_seed ^ 0x9E3779B9))
        cn = len(ctrl)
        cfwd = list(ctrl)
        crev = list(reversed(ctrl))
        if not H.check(len(cfwd) == cn and len(crev) == cn,
                       "single-owner CONTROL list fwd/rev length mismatch: "
                       "len={0} fwd={1} rev={2} on a PRIVATE list with no other "
                       "accessor -- CPython's ins1/list_ass_slice memmove or "
                       "list_resize lost/duplicated a slot independent of "
                       "contention".format(cn, len(cfwd), len(crev))):
            return
        if not H.check(cfwd == crev[::-1],
                       "single-owner CONTROL list fwd != reverse-of-rev on a "
                       "PRIVATE list -- a memmove duplicated/tore a slot in the "
                       "absence of any contention (CPython bug, not M:N)"):
            return
        for v in cfwd:
            if v not in UNIVERSE_SET:
                H.fail("single-owner CONTROL list holds OUT-OF-UNIVERSE value "
                       "{0!r} on a PRIVATE list -- a half-memmoved/freed slot in "
                       "ins1/list_ass_slice with no contention".format(v))
                return
        # Record that this slot exercised the control arm (race-free per-slot tally).
        counts["control"][slot] += 1


def worker(H, wid, rng, state):
    slot = wid & 1023
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
    # the cooperative M:N-safe lock.  The lock serializes the WRITERS (shifter +
    # mutator) so the oracle tests memory-safety + length agreement while the
    # iterators race UNLOCKED.
    H.state = {
        "lock": runloom.sync.Lock(),
        "counts": {
            "fwd_clean": [0] * 1024,
            "rev_clean": [0] * 1024,
            "rterror": [0] * 1024,
            "control": [0] * 1024,
        },
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    counts = H.state["counts"]
    fwd_clean = sum(counts["fwd_clean"])
    rev_clean = sum(counts["rev_clean"])
    rterror = sum(counts["rterror"])
    control = sum(counts["control"])
    H.log("forward-clean={0} reversed-clean={1} runtimeerror={2} control-arm={3} "
          "ops={4} (every out-of-universe value / fwd!=rev length / torn slot "
          "already failed fast)".format(
              fwd_clean, rev_clean, rterror, control, H.total_ops()))
    # Reaching post with no failure already proves every per-round fwd/rev length-
    # agreement + universe + control check held; assert the window was not vacuous.
    H.check(H.total_ops() > 0,
            "no rounds completed -- the list_ass_slice/ins1 memmove-vs-iterate "
            "race window was never exercised")
    H.check(fwd_clean + rev_clean + rterror > 0,
            "no iterator walk ever completed -- the memmove-vs-iterate window was "
            "never exercised")
    H.check(control > 0,
            "the single-owner control arm never ran -- the falsifier that "
            "separates a CPython memmove bug from M:N contention was never "
            "exercised")
    H.require_no_lost("list-memmove-iterate completeness")


if __name__ == "__main__":
    harness.main(
        "p420_list_slice_assign_insert_memmo", body, setup=setup, post=post,
        default_funcs=3000,
        describe="a shared list's mid-array memmove paths (lst.insert(0,x) -> "
                 "ins1, lst[i:j]=[..]/del lst[i:j] -> list_ass_slice) run on one "
                 "hub while a forward list iterator and a reversed() iterator walk "
                 "it parked mid-walk on others; every value in a finite sentinel "
                 "universe, post-quiescent len==len(fwd)==len(rev), and a single-"
                 "owner control list stays a clean UNIVERSE permutation -- an out-"
                 "of-universe (freed/half-memmoved) slot, fwd!=rev length, or a "
                 "control-arm divergence fails")
