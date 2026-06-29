"""big_100 / 437 -- array.array frombytes-realloc export-guard + byteswap in-place involution under M:N.

The subject is CPython's ``arrayobject`` driven through the DISTINCT in-place
paths p404 leaves untouched: ``array_frombytes`` / ``array_inplace_concat``
(``a += b``) -- both RESIZE paths that call ``array_resize`` ->
``PyMem_Realloc(self->ob_item, ...)`` (the ob_item buffer can FREE-and-MOVE) and
then ``memcpy`` the incoming bytes onto the grown tail -- versus ``array_byteswap``
-- an in-place per-item byte-reversal loop that rewrites every item THROUGH
``ob_item`` and is NOT a resize, so it is NOT export-guarded.  p404 only exercises
append/extend/pop; frombytes/byteswap/iadd are a separate realloc + bulk-memcpy +
in-place-transform surface with the SAME ``ob_exports`` export counter, never
attacked.

THE EXACT C-LEVEL STATE UNDER ATTACK.
  * ``array_resize`` (called by frombytes and ``+=``): when ``ob_exports > 0`` it
    raises ``BufferError`` ("cannot resize an array that is exporting buffers")
    and MUST NOT realloc; when ``ob_exports == 0`` it ``PyMem_Realloc``s ob_item
    (which may MOVE the buffer) and rewrites ``ob_size``/``allocated``.  The
    racing pair is the getbuffer/release export RMW on ``ob_exports`` versus a
    sibling's frombytes-realloc: a torn ob_exports that let the realloc slip past
    the guard would FREE the buffer the live memoryview points at -- a UAF.
  * ``array_byteswap``: for itemsize>1 it reverses the bytes of EACH item in place
    over ob_item (a sequence of ``Py_MEMCPY``-style per-item swaps).  This is NOT
    a resize and NOT export-guarded, so byteswap racing an UNLOCKED reader is an
    inherent data race we must NOT trigger; instead we read the byteswapped result
    only AFTER the view is released, validating the in-place transform survived as
    a COMPLETE, atomic-per-array map (every item swapped exactly once, no
    half-swapped torn item).

THE PRECISE M:N HAZARD (the racing op pair we DO attack).  A VIEWER fiber holds a
live ``memoryview`` over a shared array (``ob_exports`` up), stamps marker values
into cells THROUGH the view, trips a gate and PARKS on its grown-down C stack with
the raw ob_item pointer captured -- while a CHURNER on ANOTHER hub calls
``frombytes(b)`` / ``a += array`` (RESIZE attempts).  Each MUST raise
``BufferError``: a success is an unguarded realloc that frees ob_item under the
live view (UAF).  On resume the viewer re-reads its stamped cells: a changed /
out-of-universe / SIGSEGV value means the buffer was relocated under it.  Only
AFTER the viewer releases (ob_exports back to 0) does the churner's frombytes
succeed; we then byteswap-twice and check the involution.

THE INVARIANTS (falsifiable, both hot fail-fast AND post-quiescent).
  SAFETY (resize-guard path): while the memoryview is live EVERY sibling
    frombytes()/iadd MUST raise BufferError (counted).  A silent success ==
    unguarded realloc UAF -> FAIL.  Any OTHER exception type -> FAIL.
  INTEGRITY (relocate-under-view): a value stamped through the view before the
    park is unchanged on resume (frombytes did NOT relocate ob_item under the
    live view).
  CONSERVATION / INVOLUTION (in-place rewrite path, read AFTER release): with the
    view dropped, the churner frombytes-grows the array (old cells preserved,
    appended cells == the known bytes), then byteswap() then byteswap() again MUST
    return ``a[i] == f(i)`` for EVERY i (byteswap is an involution; a torn /
    half-swapped item leaves an out-of-UNIVERSE value -- units in == units out,
    no half-swapped item); and ``len(a)*itemsize == memoryview(a).nbytes`` with
    typecode/itemsize stable (no torn length/capacity desync).
  SINGLE-OWNER CONTROL ARM: a PRIVATE array (one writer, race-free by
    construction) does the byte-EXACT same frombytes-after-self-release +
    double-byteswap sequence; the shared array's post-release content MUST be
    byte-identical to the private control.  A divergence that never crashes -- a
    relocated/dropped/doubled chunk, a half-applied byteswap -- is localized to
    CPython's realloc/memcpy/byteswap machinery, NOT contention.

COVERAGE (the flaky-random lesson the suite already fixed in p125/p126/p404): the
resize KIND (frombytes vs ``+=``) and the view SHAPE (single vs double view) are
timeout-bound, so round-robin them by worker id in the first ops, then go random.
post() asserts the blocked-while-exported path, the succeeded-after-release path,
and the double-byteswap involution were all exercised.

Stresses: array_frombytes/array_inplace_concat array_resize PyMem_Realloc(ob_item)
vs the ob_exports export guard, getbuffer/release RMW vs frombytes-realloc UAF,
array_byteswap in-place per-item reversal as a complete atomic-per-array
involution, torn ob_exports, write-through-view survival across a park, torn
length/capacity, shared-vs-private byte-exact divergence.

Good TSan / controlled-M:N-replay target: the ob_exports increment/decrement vs
array_resize's PyMem_Realloc(ob_item) read is a textbook FT data race; a TSan
report on that counter, or one out-of-universe item after the double byteswap
under replay, localizes the fault before the involution assert even closes.
"""
import array
import struct

import harness
import runloom


# Typecode 'I' = unsigned 32-bit; itemsize 4.  A multi-byte item is REQUIRED for
# byteswap to do real per-item work (itemsize 1 is a no-op); unsigned keeps every
# value (including byteswapped intermediates) inside the cell's legal range so the
# closed-world universe never has to special-case sign.
TYPECODE = "I"
ITEMSIZE = array.array(TYPECODE).itemsize        # 4

# Base length: enough cells that ob_item spans several cache lines / pages, the
# read/write loop touches many slots, and a frombytes-grow past capacity provokes
# a real PyMem_Realloc(+copy) of the live buffer (not an in-place slack fill).
BASE_LEN = 384

# How many items the after-release frombytes appends -- pushes the array well past
# its current capacity so the growth is a genuine reallocation, not slack fill.
GROW_ITEMS = 200

# struct format for one 'I' cell, NATIVE byte order (matches array's storage), and
# little-endian for building frombytes payloads deterministically.
CELL_FMT_NATIVE = "I"
CELL_FMT_LE = "<I"

# Stride at which the viewer stamps markers through the live view.  The churner
# never writes any cell while the view is live (all its ops are resize attempts
# that must be BLOCKED), so the markers cannot collide with churner writes; the
# stride only spreads the stamps across multiple cache lines / pages.
MARK_STRIDE = 29

# Mask to keep every value inside the unsigned 32-bit cell.
MASK32 = 0xFFFFFFFF

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024

# Resize KINDS attempted while the view is live (both are export-guarded resize
# paths through array_resize -> PyMem_Realloc(ob_item)).
KIND_FROMBYTES = 0       # a.frombytes(b)   -> array_frombytes -> array_resize
KIND_IADD = 1            # a += array       -> array_inplace_concat -> array_resize
NKIND = 2

NSHAPE = 2               # single-view / double-view


def f(i):
    """Deterministic index -> 32-bit value.  a[i] must equal f(i) for every seeded
    cell; a value read through a live view that is neither f(i) nor a marker this
    viewer itself stamped is a torn/relocated read.  AFTER a double byteswap every
    cell must again equal f(i) (involution) -- a half-swapped item leaves an
    out-of-universe value.  A wide, well-mixed function so a coincidental match
    (e.g. a byteswap that happens to be its own value) is essentially impossible;
    a palindromic byte pattern would defeat the involution check, so the mixer is
    chosen to make f(i) != byteswap(f(i)) for every i in range (asserted below)."""
    return (i * 0x9E3779B1 + 0x12345) & MASK32


def byteswap32(v):
    """The value of one 'I' item after a single array.byteswap() -- a 4-byte
    reversal.  Used to validate the SINGLE byteswap as a complete map and to prove
    f(i) is not its own byteswap (so the double-swap involution is non-trivial)."""
    return struct.unpack(CELL_FMT_LE, struct.pack(">I", v & MASK32))[0]


def write_marker(i):
    """A distinct, in-universe marker value the viewer stamps through the view at
    index i; disjoint-looking from f(i) but still a legal 32-bit cell value."""
    return ((f(i) ^ 0x0F0F0F0F) + 0x77) & MASK32


def fresh_array():
    """A fresh array.array('I', ...) with a[i] == f(i) for i in [0, BASE_LEN)."""
    return array.array(TYPECODE, (f(i) for i in range(BASE_LEN)))


def grow_payload(n):
    """The deterministic frombytes payload that appends items f(BASE_LEN + j) for
    j in [0, n).  Returned as bytes so the SAME payload drives the shared array
    and the private control byte-for-byte (n*itemsize bytes)."""
    vals = [f(BASE_LEN + j) for j in range(n)]
    return struct.pack("<" + "I" * n, *vals)


def check_cell(H, wid, i, got, written):
    """Validate one value read through the live view.  `written` is the set of
    indices the viewer stamped a marker into (so the legal value there is
    write_marker(i), not f(i)).  Returns False on the first violation."""
    expect = write_marker(i) if i in written else f(i)
    if got != expect:
        H.fail("viewer read TORN value at index {0}: got {1!r} expected {2!r} "
               "({3}) -- a read through a freed/relocated ob_item (array "
               "frombytes-realloc vs live-view export-guard UAF under M:N)".format(
                   i, got, expect, "marker" if i in written else "f(i)"))
        return False
    return True


def viewer(H, wid, arr, gate_parked, gate_churned, gate_released, written,
           slot, counts, double):
    """Hold a live memoryview over `arr`, stamp markers through it, park with the
    view LIVE while the churner attempts frombytes/iadd resizes, then prove the
    buffer was not freed/relocated.

    Ordering (load-bearing -- the p404 release-race trap): the viewer trips
    `gate_parked` with the view LIVE, then WAITS on `gate_churned` -- it does NOT
    release the view until the churner has finished every blocked-resize attempt.
    Only then does it release the view(s) and trip `gate_released` so the churner's
    SUCCESS phase begins with ob_exports back at zero.  A bare yield_now would let
    the view be released before the churner resizes, turning a legal post-release
    success into a false 'leaked resize'."""
    ok = True
    mv = memoryview(arr)
    mv2 = memoryview(arr) if double else None
    try:
        # Full read pass: every seeded cell must equal f(i) (ob_exports is up, no
        # resize can have touched ob_item yet).
        for i in range(BASE_LEN):
            if not check_cell(H, wid, i, mv[i], written):
                ok = False
                break
        if ok:
            # Stamp markers through the view at a deterministic spread of cells.
            for i in range(0, BASE_LEN, MARK_STRIDE):
                mv[i] = write_marker(i)
                written.add(i)
            # If double-view, the SECOND view must observe the same writes (same
            # underlying ob_item) -- a torn export would alias a stale copy.
            if mv2 is not None:
                for i in range(0, BASE_LEN, MARK_STRIDE):
                    if not check_cell(H, wid, i, mv2[i], written):
                        ok = False
                        break
        # Hand off to the churner WITH the view(s) live (the raw ob_item pointer is
        # captured on this fiber's stack), then PARK on gate_churned -- the
        # churner's frombytes/iadd resize attempts land in this window and must all
        # be blocked by the export guard, NOT free this buffer under us.
        gate_parked.done()
        gate_churned.wait()
        # The churner has finished its blocked phase; ob_item must be intact.
        # Re-read every stamped cell: a changed value (or a SIGSEGV / out-of-
        # universe value) means the buffer was relocated under the live view.
        if ok:
            for i in range(0, BASE_LEN, MARK_STRIDE):
                if not check_cell(H, wid, i, mv[i], written):
                    ok = False
                    break
    finally:
        # Release ALL exported views.  Only after ob_exports drops to zero is the
        # churner's SUCCESS phase (gate_released) allowed to resize.
        if mv2 is not None:
            mv2.release()
        mv.release()
        gate_released.done()
    if ok:
        counts["views"][slot] += 1
    return ok


def attempt_resize(arr, kind, payload):
    """Perform ONE resize attempt of the given kind on `arr`.  Raises BufferError
    if a view is exported (the export guard on array_resize), or grows the array if
    it succeeds.  Both kinds route through array_resize -> PyMem_Realloc(ob_item).

    `+=` REBINDS the local name to the (same, in-place-grown) array object;
    array.__iadd__ returns self, so the shared object identity is preserved (unlike
    Counter.__iadd__) -- but to be defensive against any name detachment we return
    the (possibly re-bound) object so the caller keeps the live reference."""
    if kind == KIND_FROMBYTES:
        arr.frombytes(payload)
        return arr
    # KIND_IADD: build a donor array from the SAME payload bytes and += it.  This is
    # array_inplace_concat, a resize via array_resize, export-guarded identically.
    donor = array.array(TYPECODE)
    donor.frombytes(payload)
    arr += donor
    return arr


def churn_blocked(H, wid, arr, kind, payload):
    """Attempt frombytes/iadd resizes on `arr` while a view is exported.  Every
    attempt MUST raise BufferError (return 'blocked'); a SUCCESS is the UAF bug; any
    OTHER exception is also a fault.  Returns 'blocked' | 'leaked' | 'badexc'.

    Drives the assigned kind PLUS the other kind in the same live-view window so
    each round widens the surface where a torn ob_exports could let a realloc slip
    past the guard.  A small payload (a few items) is enough -- the guard must fire
    BEFORE the realloc regardless of size."""
    small = grow_payload(4)
    for k in (kind, (kind + 1) % NKIND):
        try:
            attempt_resize(arr, k, small)
        except BufferError:
            continue              # the correct, guarded outcome
        except Exception as exc:  # noqa: BLE001
            H.fail("resize while view exported raised {0} (not BufferError): {1} "
                   "-- kind={2} (frombytes/iadd export guard misfired)".format(
                       type(exc).__name__, exc, k))
            return "badexc"
        # No exception: the resize SUCCEEDED while a view was live -> the export
        # guard did not serialize against the realloc path: a use-after-free.
        H.fail("frombytes/iadd resize SUCCEEDED while a memoryview was exported "
               "(kind={0}) -- ob_exports guard did not block array_resize's "
               "PyMem_Realloc(ob_item): a buffer-export-vs-resize use-after-free "
               "(the live view now points into a freed/relocated allocation)"
               .format(k))
        return "leaked"
    return "blocked"


def churn_after_release(H, wid, arr, kind, slot, counts, expected_bytes):
    """After the viewer released its view(s), the array must be freely resizable.
    frombytes-grow it past capacity (a real PyMem_Realloc) -- old cells preserved,
    appended cells == the known payload -- then byteswap() TWICE (involution: every
    cell back to f(i)) and reconcile against the byte-exact private control.

    Returns True on success.  All the integrity / involution / control checks live
    here, AFTER ob_exports == 0, so the byteswap in-place rewrite is read by a
    single owner (never raced against a reader -- that race is inherent and we do
    not trigger it)."""
    payload = grow_payload(GROW_ITEMS)
    pre_len = len(arr)

    # Snapshot the pre-grow cells (markers + f(i)) so we can assert frombytes did
    # NOT relocate/disturb the existing items -- only appended onto the tail.
    pre_snapshot = arr.tolist()

    try:
        if kind == KIND_FROMBYTES:
            arr.frombytes(payload)
        else:
            donor = array.array(TYPECODE)
            donor.frombytes(payload)
            arr += donor
    except BufferError as exc:
        # A BufferError here means a view is STILL counted as exported after
        # release(): a torn/leaked ob_exports that never decremented -- the
        # mirror-image FT bug (resize wrongly blocked forever).
        H.fail("frombytes/iadd BLOCKED after view release: {0} -- ob_exports did "
               "not decrement (torn export counter under M:N, resize stuck)".format(
                   exc))
        return False
    except Exception as exc:  # noqa: BLE001
        H.fail("post-release frombytes/iadd raised {0}: {1}".format(
            type(exc).__name__, exc))
        return False

    # INTEGRITY: frombytes appends onto the tail -- it must NOT have relocated or
    # disturbed the existing items.  Every pre-grow cell is unchanged; every newly
    # appended cell equals the payload value.
    if len(arr) != pre_len + GROW_ITEMS:
        H.fail("frombytes/iadd grew array to len {0}, expected {1} (={2}+{3}) -- "
               "torn ob_size after the realloc".format(
                   len(arr), pre_len + GROW_ITEMS, pre_len, GROW_ITEMS))
        return False
    for i in range(pre_len):
        if arr[i] != pre_snapshot[i]:
            H.fail("frombytes/iadd DISTURBED existing cell {0}: now {1!r} was "
                   "{2!r} -- array_resize relocated/clobbered an existing item "
                   "instead of only appending (torn realloc copy)".format(
                       i, arr[i], pre_snapshot[i]))
            return False
    for j in range(GROW_ITEMS):
        want = f(BASE_LEN + j)
        if arr[pre_len + j] != want:
            H.fail("appended cell {0} (item {1}) == {2!r}, expected payload value "
                   "{3!r} -- frombytes memcpy dropped/garbled an appended item"
                   .format(pre_len + j, j, arr[pre_len + j], want))
            return False

    # CONSERVATION / INVOLUTION on the in-place byteswap path.  Snapshot the array,
    # byteswap once (a complete per-item 4-byte reversal), assert EVERY item now
    # equals byteswap32 of its prior value (units in == units out, no half-swapped
    # item), byteswap again, assert the involution restored EVERY item exactly.
    before = arr.tolist()
    arr.byteswap()
    once = arr.tolist()
    if len(once) != len(before):
        H.fail("byteswap changed length {0} -> {1} -- byteswap is in-place and "
               "must never resize (torn ob_size)".format(len(before), len(once)))
        return False
    for i in range(len(before)):
        want = byteswap32(before[i])
        if once[i] != want:
            H.fail("single byteswap produced item {0} == {1!r}, expected "
                   "byteswap({2!r})={3!r} -- a half-swapped/torn item (the "
                   "in-place per-item byte reversal did not complete atomically "
                   "for this item)".format(i, once[i], before[i], want))
            return False
    arr.byteswap()
    twice = arr.tolist()
    if twice != before:
        # Find the first divergence for a precise message.
        bad = next((i for i in range(min(len(twice), len(before)))
                    if twice[i] != before[i]), -1)
        H.fail("double byteswap is NOT an involution: item {0} == {1!r} != "
               "original {2!r} -- byteswap(byteswap(x)) must equal x for every "
               "item; a divergence is a torn/half-swapped item left out of the "
               "value universe".format(
                   bad, twice[bad] if bad >= 0 else None,
                   before[bad] if bad >= 0 else None))
        return False

    # Structural invariants on the grown, twice-swapped array.
    if arr.typecode != TYPECODE:
        H.fail("typecode changed under frombytes/byteswap: {0!r} != {1!r}".format(
            arr.typecode, TYPECODE))
        return False
    if arr.itemsize != ITEMSIZE:
        H.fail("itemsize changed under frombytes/byteswap: {0} != {1}".format(
            arr.itemsize, ITEMSIZE))
        return False
    nbytes = memoryview(arr).nbytes
    if len(arr) * ITEMSIZE != nbytes:
        H.fail("torn length/capacity: len {0} * itemsize {1} = {2} != buffer "
               "nbytes {3} -- length/allocation desync after the concurrent "
               "frombytes-realloc".format(
                   len(arr), ITEMSIZE, len(arr) * ITEMSIZE, nbytes))
        return False

    # SINGLE-OWNER CONTROL: the shared array's post-release transform (frombytes
    # grow onto the SAME pre-grow content, then double byteswap == identity) must
    # leave it byte-identical to the private control that did the identical
    # sequence single-owner.  A divergence that never crashed is a realloc / memcpy
    # / byteswap machinery bug, NOT contention -- the control has one writer.
    shared_bytes = arr.tobytes()
    if shared_bytes != expected_bytes:
        H.fail("shared-vs-control divergence: shared array content differs from "
               "the byte-exact private control after the identical frombytes-grow "
               "+ double-byteswap sequence (len shared={0}B control={1}B) -- a "
               "realloc/memcpy relocated/dropped/doubled an item or a byteswap "
               "half-applied".format(len(shared_bytes), len(expected_bytes)))
        return False

    counts["resized"][slot] += 1
    counts["swapped"][slot] += 1
    return True


def build_control(kind):
    """The PRIVATE single-owner control: a fresh array seeded f(i), grown by the
    same frombytes/iadd payload, then double-byteswapped (identity).  Returns its
    final tobytes() so the shared arm can reconcile byte-exact.  Race-free by
    construction (one writer); a divergence localizes the fault to CPython, not
    contention.  Note the shared arm STAMPS markers through the view before the
    park, so the control must stamp the same markers to match byte-for-byte."""
    ctrl = fresh_array()
    # Mirror the viewer's marker stamps (the shared array carries them into the
    # post-release content).
    for i in range(0, BASE_LEN, MARK_STRIDE):
        ctrl[i] = write_marker(i)
    payload = grow_payload(GROW_ITEMS)
    if kind == KIND_FROMBYTES:
        ctrl.frombytes(payload)
    else:
        donor = array.array(TYPECODE)
        donor.frombytes(payload)
        ctrl += donor
    ctrl.byteswap()
    ctrl.byteswap()
    return ctrl.tobytes()


def worker(H, wid, rng, state):
    counts = state["counts"]
    slot = wid & (SLOTS - 1)
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Deterministic coverage of both resize kinds AND both view shapes in the
        # first ops; random after.  Pure random misses a kind at low op-count under
        # load (the p125/p126/p404 flaky-coverage bug).
        if i < NKIND * NSHAPE:
            kind = (wid + i) % NKIND
            double = ((wid + i) // NKIND) % NSHAPE == 1
        else:
            kind = rng.randrange(NKIND)
            double = rng.getrandbits(1) == 1
        i += 1

        arr = fresh_array()
        written = set()
        # The byte-exact private control for this kind (single-owner, race-free).
        expected_bytes = build_control(kind)

        # Three gates pin the ordering so the resize-attempt window provably
        # overlaps a LIVE view (the p404 release-race fix):
        #   gate_parked    viewer -> churner: view is now live, go attempt resizes.
        #   gate_churned   churner -> viewer: blocked-resize attempts done, release.
        #   gate_released  viewer -> churner: view dropped (ob_exports==0), SUCCESS.
        gate_parked = runloom.WaitGroup()
        gate_parked.add(1)
        gate_churned = runloom.WaitGroup()
        gate_churned.add(1)
        gate_released = runloom.WaitGroup()
        gate_released.add(1)
        wg = runloom.WaitGroup()
        wg.add(2)

        def run_viewer(arr=arr, gate_parked=gate_parked,
                       gate_churned=gate_churned, gate_released=gate_released,
                       written=written, double=double):
            try:
                viewer(H, wid, arr, gate_parked, gate_churned, gate_released,
                       written, slot, counts, double)
            except Exception as exc:           # noqa: BLE001
                H.fail("viewer raised {0}: {1} -- unexpected (a live-view read "
                       "should never fault)".format(type(exc).__name__, exc))
            finally:
                wg.done()

        def run_churner(arr=arr, gate_parked=gate_parked,
                        gate_churned=gate_churned, gate_released=gate_released,
                        kind=kind, expected_bytes=expected_bytes):
            res = None
            try:
                # Run while the viewer holds the view LIVE: every frombytes/iadd
                # must raise BufferError.  Then release the viewer (gate_churned) so
                # it can drop the view before our SUCCESS phase.
                gate_parked.wait()
                res = churn_blocked(H, wid, arr, kind, None)
                if res == "blocked":
                    counts["blocked"][slot] += 1
                gate_churned.done()
                # Wait for the viewer to release its view(s), then resize for real,
                # double-byteswap, and reconcile against the control.
                gate_released.wait()
                churn_after_release(H, wid, arr, kind, slot, counts,
                                    expected_bytes)
            except Exception as exc:           # noqa: BLE001
                H.fail("churner raised {0}: {1}".format(
                    type(exc).__name__, exc))
            finally:
                # Ensure gate_churned is always tripped even if churn_blocked
                # raised, so the viewer never parks forever on it.
                if res is None:
                    gate_churned.done()
                wg.done()

        H.fiber(run_viewer)
        H.fiber(run_churner)
        wg.wait()
        if H.failed:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # f(i) must NOT be its own byteswap for any seeded index, else the double-swap
    # involution would be trivially satisfied even by a torn single swap; assert it
    # here, inside the root, before any worker runs.
    for i in range(BASE_LEN + GROW_ITEMS):
        v = f(i)
        if byteswap32(v) == v:
            raise AssertionError(
                "f({0})={1!r} is a byteswap palindrome -- the involution check "
                "would be trivial; pick a different mixer".format(i, v))
    H.state = {"counts": {"views": [0] * SLOTS, "blocked": [0] * SLOTS,
                          "resized": [0] * SLOTS, "swapped": [0] * SLOTS}}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    views = sum(H.state["counts"]["views"])
    blocked = sum(H.state["counts"]["blocked"])
    resized = sum(H.state["counts"]["resized"])
    swapped = sum(H.state["counts"]["swapped"])
    H.log("clean-view-passes={0} frombytes/iadd-BLOCKED-while-exported={1} "
          "frombytes-SUCCEEDED-after-release={2} double-byteswap-involutions={3} "
          "ops={4} (any torn value / leaked-resize / non-involution / "
          "control-divergence already failed fast)".format(
              views, blocked, resized, swapped, H.total_ops()))
    H.check(H.total_ops() > 0, "no rounds completed -- the frombytes-realloc vs "
            "export-guard race window was never exercised")
    # Both halves of the export guard, plus the involution path, must have been
    # exercised: a frombytes/iadd correctly BLOCKED while a view was live, a
    # frombytes correctly SUCCEEDING once the view was released, and the double
    # byteswap restoring every item.
    H.check(blocked > 0, "no frombytes/iadd was ever attempted while a view was "
            "exported -- the BufferError guard path was never exercised")
    H.check(resized > 0, "no frombytes ever succeeded after view release -- the "
            "export-counter-decrement path was never exercised")
    H.check(swapped > 0, "no double-byteswap involution was ever checked -- the "
            "in-place byteswap conservation path was never exercised")
    H.require_no_lost("array-frombytes-realloc/byteswap completeness")


if __name__ == "__main__":
    harness.main(
        "p437_array_frombytes_inplace_reallo", body, setup=setup, post=post,
        default_funcs=3000,
        describe="a shared array.array('I') is read/stamped through a live "
                 "memoryview across a park while a sibling hub attempts "
                 "frombytes()/+= (array_resize -> PyMem_Realloc(ob_item)) on it: "
                 "every resize while the view is exported MUST raise BufferError "
                 "(not free ob_item under the view), stamped cells survive the "
                 "park; after release frombytes grows it (old cells intact, "
                 "appended==payload), double byteswap() is an involution (every "
                 "item back to f(i), no half-swapped value), len*itemsize==nbytes "
                 "and typecode/itemsize stable, and the post-release content is "
                 "byte-exact == a single-owner private control -- anything else is "
                 "an FT realloc-vs-export-guard UAF or a torn in-place byteswap")
