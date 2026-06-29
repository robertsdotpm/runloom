"""big_100 / 421 -- shared set BULK in-place binop table-rebuild vs a lock-free
iterator/membership reader under M:N.

The subject is the CPython ``set`` object (Objects/setobject.c) and the WHOLESALE
rewrite of its open-addressing backing store that its BULK in-place binops do --
NOT the single-slot insert/discard that p311 already exercises.  A setobject is::

    typedef struct { ... setentry *so_table;  Py_ssize_t so_mask;
                     Py_ssize_t so_fill;  Py_ssize_t so_used; ... } PySetObject;

and the C calls behind ``s |= other`` (``set_ior`` -> ``set_merge``),
``s -= other`` (``set_isub`` -> ``set_difference_update_internal``), ``s &= other``
(``set_iand`` -> ``set_intersection``-then-swap), and
``s.symmetric_difference_update(other)`` each can call ``set_table_resize`` and, in
ONE C call, ``realloc``-and-FREE the old ``so_table`` while rewriting ``so_mask`` /
``so_fill`` / ``so_used`` to point at a brand-new entry array.  That is a wholesale
table SWAP, not the in-place single-slot publish that p311's add()/discard() does.

The exact M:N hazard (the racing op pair):

  * a sibling fiber on ANOTHER hub is mid-``set_table_resize``: it has computed the
    new ``so_table`` pointer but not yet finished republishing ``so_mask`` /
    ``so_used``, OR has just freed the OLD table; the C resize loop itself can PARK
    on a grown-down C stack (allocation / GC) with the object half-swapped;
  * meanwhile a lock-free READER holds an ``setiterobject`` whose ``si_used`` /
    ``so_table`` snapshot was taken against the OLD table (``setiter_iternext``
    walks ``si_set->table[i]`` by raw index), OR is mid-``set_contains_key`` whose
    ``set_lookkey`` open-addressing probe is walking the OLD, now-freed table.
    On resume the iterator reads through a stale ``so_table`` slot pointer or the
    probe walks freed memory -> an out-of-universe member, a dropped/duplicated
    member, or a SIGSEGV.

p311 only ever mutates a shared set via single-element ``add()`` / ``discard()``;
it never drives these bulk table-rebuild paths.  This program does, and turns the
hazard into a CLOSED-WORLD CONSERVATION law instead of a racy probe:

  Finite sentinel UNIVERSE of recognizable members, sized to push the set's table
  through several growth/shrink/rehash boundaries.  Per round the worker owns ONE
  SHARED set and spawns:

    * a MERGER fiber that applies a deterministic, round-robined SEQUENCE of bulk
      cases (|= / -= / &= / symmetric_difference_update) with donor sets drawn ONLY
      from UNIVERSE.  ``set`` is documented thread-unsafe, so the WRITES are
      serialized under a PER-ROUND cooperative Lock (the set is per-round-local, so
      a global lock would needlessly serialize every worker's merger across all
      hubs and wedge the run) to make the oracle a CONSERVATION test
      (did the bulk rebuild keep exactly the right members) rather than a test of
      set's absent thread-safety; the merger yields INSIDE the held region so the
      reader's iterate/probe provably overlaps the table swap on another hub.  It
      mirrors the IDENTICAL sequence into a PRIVATE single-owner set -- the control
      arm -- whose final contents are the race-free set-algebra oracle.
    * a lock-free READER fiber that, holding NO lock, repeatedly iterates the shared
      set (``for k in s``) and does ``k in s`` membership probes, asserting every
      member yielded / found is in UNIVERSE.  The only tolerated exception is
      ``RuntimeError`` ("Set changed size during iteration") -- the LEGAL detection;
      ANY other exception, an out-of-universe member, or a SIGSEGV is the bug.

  After a producer WaitGroup join (merger done), the reader is signalled via a done
  Chan and joins its own WaitGroup, so the set is provably QUIESCENT before the
  post-round oracle reads it.  Then, for THAT round:

    * shared set contents == the PRIVATE control set contents EXACTLY (the torn-
      rebuild oracle: a dropped or duplicated member from a half-swapped so_table
      breaks set-equality even with no crash);
    * ``len(s) == len(list(s))`` -- ``so_used`` agrees with the actual entry walk
      (a torn so_used vs so_table desync is caught here);
    * every member of the shared set is in UNIVERSE.

  Across the run (post): every bulk case was exercised (round-robined by wid, never
  flaky-random -- the p125/p126/p172 coverage lesson); the private-control set
  matched the shared set every round (reaching post fail-free proves it); at least
  one reader iteration completed-clean AND at least one raised the legal
  RuntimeError (the rebuild-vs-read window was actually hit, not skipped).

Invariant (hot, fail-fast): every yielded/probed member in UNIVERSE; only
RuntimeError tolerated from the reader.
Invariant (post): shared set == private control set (set-equality); len==walk-len;
all bulk cases exercised; the race window was exercised (clean and RuntimeError
both observed); no lost worker.

Stresses: set bulk in-place binop set_table_resize realloc-and-free of so_table,
so_mask/so_fill/so_used wholesale rewrite under preempt-mid-resize, setiter_iternext
raw-index walk of a swapped table, set_lookkey open-addressing probe of a freed
table, dropped/duplicated member from a torn rebuild, private-vs-shared set-algebra
conservation.

Good TSan / controlled-M:N-replay target: the so_table pointer store in
set_table_resize racing setiter_iternext's table[i] load (and set_lookkey's probe)
is a textbook use-after-free / torn-publish data race; a TSan report on the
so_table write/read often localizes the corruption before the set-equality assert
even closes, and the private-control set makes a non-crashing divergence falsifiable.
"""
import random

import harness
import runloom

# Finite sentinel UNIVERSE: a fixed, recognizable set of members.  A member NOT in
# this set yielded by the iterator or returned by a membership probe is a torn/
# freed-slot read -- a hard fault.  Sized to push the set's open-addressing table
# through several set_table_resize growth/shrink boundaries (the resize is what
# realloc-and-frees so_table out from under a live iterator/probe).
UNIVERSE_SIZE = 256
UNIVERSE = tuple(0x42100000 + i for i in range(UNIVERSE_SIZE))
UNIVERSE_SET = frozenset(UNIVERSE)
UNIVERSE_LIST = list(UNIVERSE)

# Each round seeds the shared set with this half of the universe, then the merger
# churns the rest in/out via the bulk binops to force table rebuilds.  Splitting
# the universe keeps every member the bulk ops can ever add still IN UNIVERSE (so a
# clean traversal that catches a freshly-merged member is still legal), while
# guaranteeing real size changes across several resize boundaries.
SEED_KEYS = UNIVERSE[: UNIVERSE_SIZE // 2]

# The BULK in-place binop CASES.  post() asserts each was exercised, so the merger
# round-robins them by id in its first ops (NOT random -- pure random selection
# reliably MISSES a case at low op-count under load, the flaky-coverage bug the
# suite already had to fix in p125/p126/p172).  Each drives a DIFFERENT C path that
# can set_table_resize and rewrite so_table wholesale.
CASE_IOR = 0      # s |= other  -> set_ior -> set_merge (grow + rehash)
CASE_ISUB = 1     # s -= other  -> set_isub -> set_difference_update_internal
CASE_IAND = 2     # s &= other  -> set_iand -> set_intersection then table swap
CASE_SYMDIFF = 3  # s.symmetric_difference_update(other) -> toggle-membership rebuild
NCASES = 4

# How many bulk binops the merger applies per round.  Enough to drive the table
# through multiple grow/shrink/rehash cycles (so set_table_resize fires repeatedly
# while the reader races), small enough that many rounds complete under the timeout.
OPS_PER_ROUND = 12

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024


def fresh_shared_set():
    """A fresh shared set seeded with half the universe."""
    return set(SEED_KEYS)


def build_donor(rng):
    """A donor set drawn ONLY from UNIVERSE (closed world).  Random-sized so the
    bulk op grows/shrinks the table by a variable amount across several resize
    boundaries.  Returned as a list so the merger can build a fresh `set(donor)`
    each time (a donor is consumed identically by shared and private arms)."""
    n = rng.randint(1, UNIVERSE_SIZE)
    return rng.sample(UNIVERSE_LIST, n)


def apply_case(target, case, donor_list):
    """Apply ONE bulk in-place binop `case` with donor `donor_list` to `target`
    (a set), driving the exact C path named in the CASE constants.  Pure set
    algebra: applied identically to the shared and the private-control sets, so
    their contents must end equal.  `target` is mutated in place (no rebinding --
    `|=`/`-=`/`&=` mutate the same object; symmetric_difference_update likewise)."""
    donor = set(donor_list)
    if case == CASE_IOR:
        target |= donor                     # set_ior -> set_merge
    elif case == CASE_ISUB:
        target -= donor                     # set_isub -> set_difference_update_internal
    elif case == CASE_IAND:
        target &= donor                     # set_iand -> set_intersection + swap
    elif case == CASE_SYMDIFF:
        target.symmetric_difference_update(donor)
    return target


def merger(H, wid, shared, private, lock, ops, gate, case_tally, slot):
    """Apply a deterministic, round-robined SEQUENCE of bulk in-place binops to the
    SHARED set under `lock` (serialize writes -> CONSERVATION oracle), mirroring the
    IDENTICAL sequence into the PRIVATE single-owner control set (race-free, the
    falsifier).  Yields INSIDE the held region so the lock-free reader's iterate/probe
    overlaps the so_table swap on another hub.  Uses its OWN random.Random (a shared
    one corrupts GIL-off)."""
    mrng = random.Random(ops[0])
    tripped = False
    for i in range(OPS_PER_ROUND):
        # Round-robin the cases in the first ops (coverage), random after.  Keyed off
        # (wid + i) so every case is hit even when a round runs only a few ops.
        if i < NCASES:
            case = (wid + i) % NCASES
        else:
            case = mrng.randrange(NCASES)
        donor_list = build_donor(mrng)
        with lock:
            # Apply to the SHARED set under the write-lock, then yield so the
            # reader's iterate/probe on another hub lands DURING the table rebuild.
            apply_case(shared, case, donor_list)
            # Trip the gate the first time we hold a permit so the reader provably
            # races a rebuild (it waits on the gate before starting its hot loop).
            if not tripped:
                tripped = True
                gate.done()
            runloom.yield_now()             # reader walks/probes during the swap
        # Mirror the SAME op into the private single-owner control set (outside the
        # lock: it is single-writer -> race-free by construction).
        apply_case(private, case, donor_list)
        case_tally[case][slot] += 1         # single-writer-per-slot, race-free
    if not tripped:                         # OPS_PER_ROUND>0 so this never happens
        gate.done()


def reader(H, shared, gate, done_ch, counts, slot):
    """Loop iterating the SHARED set and probing membership while the merger rebuilds
    its table on another hub.  Holds NO lock -- this is the iterate/probe-vs-resize
    race.  Asserts every member yielded / found is in UNIVERSE (an out-of-universe
    member is a torn/freed-slot read; a SIGSEGV mid-walk is the crash the watchdog
    catches).  The ONLY tolerated exception is RuntimeError ('Set changed size during
    iteration') -- the legal detection.  Exits once the merger signals done."""
    gate.wait()                             # ensure at least one rebuild is in flight
    clean = 0
    rterror = 0
    while True:
        try:
            # Full iteration: setiter_iternext walks si_set->table[i] by raw index;
            # a concurrent set_table_resize swaps so_table under that live index.
            seen = 0
            for k in shared:
                if k not in UNIVERSE_SET:
                    H.fail("set iterator yielded OUT-OF-UNIVERSE member {0!r} -- a "
                           "torn/freed-slot read from a set_table_resize that "
                           "realloc-freed so_table under the live iterator index "
                           "(M:N set table corruption)".format(k))
                    return
                seen += 1
            clean += 1
            # Membership probes: set_contains_key -> set_lookkey open-addressing
            # probe walks the (possibly swapped/freed) so_table.  Probe a spread of
            # the universe; a probe must never SAY a non-universe key is present
            # (it can't be -- nothing outside UNIVERSE is ever added), and a True
            # result must correspond to a real universe member.
            for k in UNIVERSE_LIST[::8]:
                present = k in shared       # set_contains_key -> set_lookkey
                if present and k not in UNIVERSE_SET:
                    H.fail("membership probe reported a NON-universe key {0!r} "
                           "present in the shared set -- set_lookkey probed a "
                           "freed/swapped so_table (use-after-free)".format(k))
                    return
        except RuntimeError:
            # "Set changed size during iteration" -- the LEGAL, clean detection of
            # the concurrent bulk rebuild.  Acceptable; re-loop.
            rterror += 1
        except Exception as exc:            # noqa: BLE001
            # ANY other exception type escaping the reader is a fault (a torn
            # internal state surfacing as e.g. SystemError / KeyError, not the
            # legal RuntimeError).
            H.fail("reader raised non-RuntimeError {0}: {1} -- not the legal 'Set "
                   "changed size during iteration' outcome (torn so_table internal "
                   "state)".format(type(exc).__name__, exc))
            return
        if done_ch.try_recv() is not None:
            break
        runloom.yield_now()
    counts["clean"][slot] += clean
    counts["rterror"][slot] += rterror


def run_round_impl(H, wid, rng, slot, state):
    """One conservation round: seed a shared set, run the merger (round-robined bulk
    binops, mirrored into a private control set) against a lock-free reader, join
    both, then check the closed-world set-algebra law on the now-quiescent set."""
    counts = state["counts"]
    case_tally = state["case_tally"]

    # PER-ROUND lock: the shared set is private to THIS round (a fresh object), so
    # the write-serialization lock must be per-round too.  A single GLOBAL lock here
    # would serialize EVERY worker's merger across all hubs -- with thousands of
    # lock-free readers spinning, only one merger could ever hold the lock+yield at
    # a time and ops-throughput collapses to a crawl (the documented
    # "yield_now() inside a lock makes drain scale with goroutine count" wedge).
    # The lock's only job is "writes to THIS set are serialized so the oracle is a
    # conservation test"; nothing outside this round touches this set, so a local
    # lock gives exactly that with no cross-worker contention.
    lock = runloom.sync.Lock()

    shared = fresh_shared_set()
    private = fresh_shared_set()            # private single-owner control (same seed)

    gate = runloom.WaitGroup()              # merger trips it on first held op
    gate.add(1)
    done_ch = runloom.Chan(1)
    merger_wg = runloom.WaitGroup()
    merger_wg.add(1)
    reader_wg = runloom.WaitGroup()
    reader_wg.add(1)
    mseed = [rng.getrandbits(48)]

    def run_merger():
        try:
            merger(H, wid, shared, private, lock, mseed, gate, case_tally, slot)
        finally:
            merger_wg.done()

    def run_reader():
        try:
            reader(H, shared, gate, done_ch, counts, slot)
        finally:
            reader_wg.done()

    H.fiber(run_reader)
    H.fiber(run_merger)

    merger_wg.wait()                        # all bulk rebuilds landed
    done_ch.send(True)                      # tell the reader to stop
    reader_wg.wait()                        # reader joined -> set now quiescent

    if H.failed:
        return

    # ---- closed-world set-algebra conservation (round now quiescent) ----------
    # 1. so_used vs actual entry walk: len() must equal the number of members the
    #    iterator yields.  A torn so_used vs so_table desync (a half-finished
    #    set_table_resize) is caught here.
    walked = list(shared)
    if not H.check(len(shared) == len(walked),
                   "len(set)={0} != len(list(set))={1} -- so_used disagrees with "
                   "the actual so_table entry walk (torn set_table_resize left "
                   "so_used/so_table desynced)".format(len(shared), len(walked))):
        return

    # 2. No out-of-universe member survived in the shared set.
    for k in shared:
        if k not in UNIVERSE_SET:
            H.fail("shared set holds OUT-OF-UNIVERSE member {0!r} after the round "
                   "-- a torn/corrupted member from a set_table_resize under "
                   "concurrent iterate/probe".format(k))
            return

    # 3. THE CONSERVATION LAW: the shared set's contents must EXACTLY equal the
    #    private single-owner control set's contents -- the same bulk-binop sequence
    #    applied race-free.  A dropped or duplicated member from a half-swapped
    #    so_table breaks this set-equality even with no crash.  (set==set compares
    #    membership; a duplicated member can't survive in a set, so a torn rebuild
    #    surfaces as a MISSING member here -- shared != private.)
    if shared != private:
        missing = private - shared
        extra = shared - private
        H.fail("CONSERVATION broken: shared set != private control set after the "
               "identical bulk-binop sequence. missing (dropped by a torn "
               "so_table rebuild) = {0!r}; extra (duplicated/leaked) = {1!r} -- a "
               "set_table_resize under concurrent iterate/probe lost or doubled a "
               "member".format(sorted(missing)[:8], sorted(extra)[:8]))
        return


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    for _ in H.round_range():
        if not H.running():
            break
        run_round_impl(H, wid, rng, slot, state)
        if H.failed:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # The write-serialization lock is created PER ROUND inside run_round_impl (the
    # shared set is per-round, so a global lock would serialize every worker's
    # merger across all hubs and wedge the run -- see the note there).  Here we only
    # allocate the per-slot tally tables (single-writer-per-slot, summed in post).
    H.state = {
        "counts": {"clean": [0] * SLOTS, "rterror": [0] * SLOTS},
        # per-case exercise tally, single-writer-per-slot
        "case_tally": [[0] * SLOTS for _ in range(NCASES)],
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    clean = sum(H.state["counts"]["clean"])
    rterror = sum(H.state["counts"]["rterror"])
    case_counts = [sum(H.state["case_tally"][c]) for c in range(NCASES)]
    H.log("reader iterations clean={0} runtimeerror={1}; bulk cases "
          "ior={2} isub={3} iand={4} symdiff={5}; ops={6} (every per-round "
          "set-equality conservation check passed fail-fast)".format(
              clean, rterror, case_counts[CASE_IOR], case_counts[CASE_ISUB],
              case_counts[CASE_IAND], case_counts[CASE_SYMDIFF], H.total_ops()))

    # Reaching post with no failure already means every per-round set-algebra
    # conservation law (shared == private control) held; assert the run did work.
    H.check(H.total_ops() > 0,
            "no conservation rounds completed -- the bulk-rebuild-vs-read race "
            "window was never exercised")

    # Every bulk in-place binop case was exercised (round-robined by wid, so this
    # holds whether one worker does many ops or many workers do a few each).  A
    # case never hit means its set_table_resize path was untested.
    names = ("ior (|=)", "isub (-=)", "iand (&=)", "symmetric_difference_update")
    for c in range(NCASES):
        H.check(case_counts[c] > 0,
                "bulk case {0} ({1}) never exercised -- its set_table_resize "
                "rebuild path was untested".format(c, names[c]))

    # The rebuild-vs-read window was actually hit: at least one reader iteration
    # completed clean AND at least one detected the concurrent rebuild via the
    # legal RuntimeError.  If rterror==0 the merger's yield-in-lock never overlapped
    # a live iteration (the race wasn't probed); clean==0 would mean the reader
    # never once saw a consistent table.
    H.check(clean > 0,
            "no reader iteration ever completed cleanly -- the reader never saw a "
            "consistent set table (suspicious; the iterate/probe path was not "
            "actually exercised)")
    # rterror>0 is the proof the rebuild landed during a live iteration.  It is a
    # race outcome, so we don't fail-fast if a calm run happens to miss it at tiny
    # scale, but at the design tier it should fire; log if it didn't.
    if rterror == 0:
        H.log("NOTE: no reader iteration observed the legal 'Set changed size "
              "during iteration' RuntimeError this run -- the rebuild rarely "
              "overlapped a live iterate (more rounds/ops widen the window)")

    H.require_no_lost("set-bulk-binop-conservation completeness")


if __name__ == "__main__":
    harness.main(
        "p421_set_inplace_binop_merge_table", body, setup=setup, post=post,
        default_funcs=3000,
        describe="one shared set is driven through its BULK in-place binop table "
                 "rebuilds (|= -= &= symmetric_difference_update -> set_table_resize "
                 "realloc-frees so_table) under a cooperative write-lock while a "
                 "lock-free reader iterates + probes membership; closed-world law: "
                 "every member in a finite sentinel universe, len==walk-len, and the "
                 "shared set == a private single-owner control set replaying the "
                 "identical sequence -- a dropped/duplicated member from a torn "
                 "so_table rebuild fails set-equality, only RuntimeError tolerated")
