"""big_100 / 502 -- bisect.insort sorted-list conservation + partition invariant under M:N.

bisect.insort(a, x) locates the insertion point with a C binary search over the
list's ob_item array and then does a list.insert -- which is an ob_item memmove
that shifts every element at/after the insertion point up by one slot and bumps
ob_size.  The whole operation mutates the SAME backing array in place.  The
delicate moment is the WINDOW between one insort and the next (or between an
insort and a rescan of the list): the fiber yields there, the scheduler may
migrate it to a different hub, and a runtime that ALIASED the list object, tore
its ob_size (length), or exposed a half-shifted ob_item mid-memmove to the
resumed frame would let the fiber observe a torn length or an out-of-order
element that no correct single-threaded insort sequence could ever produce.

WHERE M:N BREAKS IT (the gap this program probes).  A single list, filled only by
bisect.insort, is a pure single-writer object: after N insort calls its length is
EXACTLY N (insort keeps duplicates, never drops), and the array is fully
non-decreasing by construction.  These are closed-form, race-free facts about a
single-owner object.  If -- across a yield inserted between insorts -- runloom
ever hands the resumed fiber a list whose length is not what the fiber itself
built, or whose elements are out of order, or whose bisect_left/bisect_right
partition no longer brackets a probe key, then the runtime has corrupted a
single-owner object across a hub migration.  On a CORRECT runtime every one of
these invariants holds deterministically (verified below against plain threads).

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  A list mutated ONLY by bisect.insort, owned by ONE fiber and never shared, is a
  race-free object.  We verified with a standalone plain-threads control (8 OS
  threads, each building its own list with bisect.insort, GIL on AND off) that
  after N insort calls: (a) len(lst) == N exactly, every time (conservation --
  insort never drops or doubles an element); (b) the list is fully non-decreasing;
  (c) for any probe key x, bisect_left(lst, x) <= bisect_right(lst, x), and the
  partition lst[:left] all < x, lst[left:] all >= x, lst[:right] all <= x,
  lst[right:] all > x holds byte-for-byte; (d) a length + boundary snapshot taken
  before a yield re-reads IDENTICALLY after the yield.  0 violations across
  millions of insorts.  Under a CORRECT runloom every one must also hold; a
  violation is a single-owner-object corruption across a hub migration, and the
  load-bearing oracle PASSES on a correct runtime (program exits 0 when no bug).

ORACLES:
  * LOAD-BEARING -- SINGLE-OWNER SORTED-LIST INVARIANT (worker, HARD, fail-fast).
    Each fiber owns a FRESH list (a fiber-local variable, never shared).  It
    inserts a known count of per-fiber keys via bisect.insort, yielding between
    insorts so siblings reliably interleave on the same hub.  Across each yield it
    snapshots (len, first, last) and re-asserts them identical on resume.  After
    the batch it asserts:
      - len(lst) == the number of insort calls made        (CONSERVATION, exact)
      - lst is fully non-decreasing                         (sorted invariant)
      - for each probe key x: left = bisect_left(lst, x), right = bisect_right(
        lst, x); left <= right; every element of lst[:left] < x; every element of
        lst[left:] >= x; every element of lst[:right] <= x; every element of
        lst[right:] > x                                     (partition invariant)
    Single-owner: the list is a fiber-local variable, created fresh each round and
    never handed to another fiber.  A failure is a runloom single-owner-object
    desync (torn length, out-of-order element, or broken partition across a yield).

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-insort
    (parked inside the ob_item memmove window and never resumed) never returns;
    the watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (insort_batches >
    0), i.e. the memmove-across-yield hazard was really exercised.

There is NO shared-list arm.  A shared list read by a lock-free reader while
another fiber insorts it would show transient out-of-order / torn-length states --
but that is DOCUMENTED racing-iteration semantics of a shared mutable list under
M:N (identical to sharing a list across OS threads), NOT a runloom bug.  Failing
on it would mislabel documented Python behavior as a runtime fault, so we deliberately
do not build it; the single-owner list keeps len==count a TRUE race-free
conservation law.

FAIL ON: a single-owner insort-built list whose length != the number of insort
calls, whose elements are out of order, whose bisect_left/bisect_right partition
does not bracket a probe key, or whose (len, first, last) snapshot changes across
a yield.  Any of these on a fiber-private list is a runloom object-migration
corruption.

Stresses: bisect.insort C binary search + list.insert ob_item memmove + ob_size
bump on a fiber-local list across hub migration + yield, bisect_left/bisect_right
partition consistency, single-owner-object length/order conservation under M:N.

Good TSan / controlled-M:N-replay target: the ob_item memmove + ob_size store
inside list.insert is a single-owner in-place array mutation; a data-race report
on the list object under the single-owner arm -- or a deterministic replay that
resumes the fiber's frame with a torn ob_size / half-shifted ob_item -- is the
cleanest signal before the conservation/partition oracle even fires.
"""
import bisect

import harness
import runloom

# Number of insort calls per batch.  Big enough that the backing list crosses
# several ob_item realloc/growth boundaries (each growth is a fresh malloc + copy,
# the moment a migration could hand back a stale array), small enough that many
# batches complete under the timeout.  len(lst) MUST equal this after the batch.
BATCH_INSORTS = 96

# How many probe keys the partition oracle checks per batch.  Drawn from inside
# and outside the value band so bisect_left/bisect_right sometimes coincide
# (key absent) and sometimes bracket a run of duplicates.
PROBE_KEYS = 12

# Value band the keys are drawn from.  A modest span means insort produces real
# duplicates (so bisect_left < bisect_right for hot keys), exercising the
# duplicate-run partition boundary.  Per-fiber offset keeps batches distinct but
# is not load-bearing (the list is single-owner regardless).
VALUE_SPAN = 512

# Yield cadence inside the insort loop: insert a few, then yield, so a sibling
# reliably interleaves in the window between two insorts on the same hub.  A yield
# after EVERY insort is fine too but this keeps batches cheap while still parking
# the fiber repeatedly across the memmove boundary.
YIELD_EVERY = 8

# Cap on batches per round so the forever loop (--rounds 0) stays bounded per
# round entry; H.running() still governs the outer duration.
INNER_CAP = 100000


def verify_sorted(H, lst, wid):
    """Assert the fiber-local list is fully non-decreasing.  A single out-of-order
    adjacent pair on a list built purely by insort is impossible on a correct
    runtime -- it would mean an element was placed wrong or the array was torn."""
    prev = None
    for i, v in enumerate(lst):
        if prev is not None and v < prev:
            H.fail("sorted invariant BROKEN: lst[{0}]={1} < lst[{2}]={3} on a "
                   "single-owner insort-built list (wid {4}, len {5}) -- an "
                   "out-of-order element means the ob_item array was torn or a "
                   "sibling's list aliased this fiber's across a yield".format(
                       i, v, i - 1, prev, wid, len(lst)))
            return False
        prev = v
    return True


def verify_partition(H, lst, x, wid):
    """Assert the bisect_left/bisect_right partition brackets probe key x.

    left = bisect_left(lst, x): lst[:left] all < x, lst[left:] all >= x.
    right = bisect_right(lst, x): lst[:right] all <= x, lst[right:] all > x.
    left <= right always.  These are closed-form facts about a sorted list; a
    violation means the list was not actually sorted/consistent when bisect read
    it (a torn array across the C binary search)."""
    left = bisect.bisect_left(lst, x)
    right = bisect.bisect_right(lst, x)
    if left > right:
        H.fail("partition BROKEN: bisect_left({0})={1} > bisect_right({0})={2} "
               "(wid {3}, len {4}) -- bisect read a torn/out-of-order array".format(
                   x, left, right, wid, len(lst)))
        return False
    # lst[:left] strictly < x
    if left > 0 and lst[left - 1] >= x:
        H.fail("partition BROKEN: lst[left-1]={0} >= probe {1} (left={2}, wid "
               "{3}) -- bisect_left boundary wrong on a single-owner list".format(
                   lst[left - 1], x, left, wid))
        return False
    # lst[left:] all >= x
    if left < len(lst) and lst[left] < x:
        H.fail("partition BROKEN: lst[left]={0} < probe {1} (left={2}, wid {3}) "
               "-- bisect_left boundary wrong on a single-owner list".format(
                   lst[left], x, left, wid))
        return False
    # lst[:right] all <= x
    if right > 0 and lst[right - 1] > x:
        H.fail("partition BROKEN: lst[right-1]={0} > probe {1} (right={2}, wid "
               "{3}) -- bisect_right boundary wrong on a single-owner list".format(
                   lst[right - 1], x, right, wid))
        return False
    # lst[right:] all > x
    if right < len(lst) and lst[right] <= x:
        H.fail("partition BROKEN: lst[right]={0} <= probe {1} (right={2}, wid "
               "{3}) -- bisect_right boundary wrong on a single-owner list".format(
                   lst[right], x, right, wid))
        return False
    return True


def insort_batch(H, wid, rng, state):
    """One batch: build a FRESH fiber-local list via bisect.insort, yielding across
    the memmove boundary, then verify conservation + sorted + partition invariants.

    The list is a fiber-local variable, created here and never shared -- so
    len(lst) == number of insort calls is a TRUE race-free conservation law, and
    the sorted/partition checks are closed-form facts a correct runtime must
    preserve across every hub migration."""
    lst = []
    base = (wid * 131) % 4096          # per-fiber offset; not load-bearing
    inserted = 0

    for i in range(BATCH_INSORTS):
        key = base + rng.randrange(VALUE_SPAN)
        bisect.insort(lst, key)
        inserted += 1

        # Snapshot the list's boundary + length BEFORE parking across the yield.
        # On a single-owner list these MUST be byte-for-byte identical on resume;
        # a change means the resumed frame saw an aliased/torn object.
        if (i % YIELD_EVERY) == (YIELD_EVERY - 1):
            snap_len = len(lst)
            snap_first = lst[0]
            snap_last = lst[-1]
            runloom.yield_now()
            if i & 1:
                runloom.sleep(0.0002)
            if len(lst) != snap_len:
                H.fail("length TORN across yield: len went {0} -> {1} on a "
                       "single-owner insort list (wid {2}) -- the list object was "
                       "aliased or ob_size torn across a hub migration".format(
                           snap_len, len(lst), wid))
                return
            if lst[0] != snap_first or lst[-1] != snap_last:
                H.fail("boundary TORN across yield: (first,last) went ({0},{1}) "
                       "-> ({2},{3}) on a single-owner insort list (wid {4}) -- "
                       "ob_item mutated with no insort by this fiber".format(
                           snap_first, snap_last, lst[0], lst[-1], wid))
                return

    # ---- CONSERVATION: exactly one element per insort call, none lost/doubled --
    if len(lst) != inserted:
        H.fail("conservation BROKEN: {0} insort calls but len(lst)={1} on a "
               "single-owner list (wid {2}) -- insort dropped or doubled an "
               "element across a hub migration (impossible on a correct runtime; "
               "insort keeps every element including duplicates)".format(
                   inserted, len(lst), wid))
        return

    # ---- SORTED invariant: the whole list is non-decreasing --------------------
    if not verify_sorted(H, lst, wid):
        return

    # ---- PARTITION invariant: bisect_left/right bracket each probe key ---------
    for _ in range(PROBE_KEYS):
        # Probe keys drawn from a slightly wider band than the values so some fall
        # outside the list entirely (left == right == 0 or len) and some hit a
        # duplicate run (left < right).
        x = base + rng.randrange(-8, VALUE_SPAN + 8)
        if not verify_partition(H, lst, x, wid):
            return

    # Race-free non-vacuity tally: ONE slot per worker (single writer, wid-indexed).
    state["batches"][wid] += 1


def worker(H, wid, rng, state):
    """Each fiber repeatedly builds and verifies its OWN sorted list.  The list is
    single-owner; the yields between insorts park the fiber across the ob_item
    memmove boundary so a sibling reliably interleaves before the invariant rescan."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            insort_batch(H, wid, rng, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # ONE conservation/non-vacuity slot per worker (single-writer-per-slot,
    # race-free; wid-indexed -- see p405).  Allocated here where H.funcs is known.
    H.state = {
        "batches": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    batches = sum(H.state["batches"])
    H.log("bisect single-owner sorted-list batches verified: {0} (each batch: "
          "len==insort-count conservation + fully non-decreasing + bisect_left/"
          "right partition on {1} probe keys + (len,first,last) stable across "
          "every yield -- all passed fail-fast); ops={2}".format(
              batches, PROBE_KEYS, H.total_ops()))

    # NON-VACUITY: the load-bearing single-owner insort hazard actually ran.
    H.check(batches > 0,
            "no insort batches completed -- the single-owner sorted-list "
            "memmove-across-yield hazard was never exercised (oracle vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-insort (stranded inside the
    # ob_item memmove window and never resumed).
    H.require_no_lost("bisect insort sorted-invariant")


if __name__ == "__main__":
    harness.main(
        "p502_bisect_insort_sorted_invariant", body, setup=setup, post=post,
        default_funcs=8000,
        describe="each fiber builds its OWN list purely via bisect.insort, "
                 "yielding across the list.insert ob_item memmove boundary so a "
                 "sibling interleaves before the rescan.  LOAD-BEARING single-owner "
                 "invariant: after N insort calls len(lst)==N (race-free "
                 "conservation), the list is fully non-decreasing, and "
                 "bisect_left(x)<=bisect_right(x) with the [:left]<x<=[left:] "
                 "partition holding for every probe key; a (len,first,last) "
                 "snapshot re-reads identically across each yield.  A torn length, "
                 "out-of-order element, or broken partition on a fiber-private list "
                 "is a runloom object-migration corruption.  No shared-list arm "
                 "(that would fire on documented racing-iteration semantics)")
