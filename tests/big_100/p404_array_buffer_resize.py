"""big_100 / 404 -- array.array buffer-export vs concurrent resize (use-after-free guard).

array.array exports a RAW C buffer over its ob_item allocation, and append /
extend / pop can realloc (and free) that allocation.  The only thing standing
between a concurrent resize and a use-after-free is the buffer-export counter
(ob_exports): while ANY memoryview / struct.pack_into target is live over the
array, a resize MUST raise BufferError ("cannot resize an array that is
exporting buffers") rather than free the buffer out from under the live view.

Under M:N that export counter is incremented on one hub (the viewer takes a
memoryview) and decremented later, while a SIBLING fiber on a (possibly)
different hub appends past capacity -- forcing realloc(ob_item).  The
hazard is a torn ob_exports: if the FT realloc path does not serialize against
the export-count check, the churner can free the OLD allocation while the
viewer still holds a pointer into it (a raw C pointer captured on a grown-down
fiber stack across a yield_now park).  The viewer then reads/writes through a
dangling pointer -- silent corruption, an out-of-universe value, or a SIGSEGV.

Closed-world oracle.  Each round seeds a fresh array.array('q', ...) so that
a[i] == f(i) for i in [0, BASE_LEN).  Two fibers are spawned and THREE gates
pin the ordering so the resize attempts provably land while a view is LIVE
(not before the view exists, and not after it has already been released -- that
release race is a real authoring trap; see the viewer docstring):

  * the VIEWER takes one (or more) memoryview(s) over the array, reads every
    element and checks mv[i] == f(i) (or a legal value it itself wrote through
    the view), writes a few cells through the view, then trips `gate_parked`
    with the view STILL LIVE (its raw C pointer captured on this fiber's stack)
    and PARKS on `gate_churned` -- it does NOT release the view until the churner
    has finished.  On resume it re-reads the same cells (must still equal what it
    wrote -- the buffer was NOT relocated under it), releases the view(s), and
    trips `gate_released`.
  * the CHURNER waits on `gate_parked` (so it runs while the view is live), then
    attempts append / extend / pop / pack_into-resize on the SAME array.  Every
    resize attempt MUST raise BufferError (caught + counted) -- a resize that
    SUCCEEDS while a view is exported is the bug.  It trips `gate_churned` to
    release the viewer, waits on `gate_released` (view now dropped,
    ob_exports==0), and only THEN does its resizes succeed; we re-assert the
    structural invariants on the grown array.

Invariants (hot, fail-fast + post):
  * every value read through a live view equals f(its index) or a value this
    viewer itself wrote (no out-of-universe / torn value, no SIGSEGV);
  * a resize while a view is exported raises BufferError -- NEVER silently
    succeeds (a success = an unguarded realloc = a UAF) and NEVER raises any
    OTHER exception;
  * a value written through the view survives the park (buffer not relocated);
  * post-release the array's typecode/itemsize are unchanged and
    len(a)*itemsize == memoryview(a).nbytes (no torn length/capacity desync);
  * both the BLOCKED-while-exported and the SUCCEEDED-after-release resize paths
    are actually exercised across the run.

Coverage: the four churn kinds (append / extend / pop / pack_into) and the
single-vs-double-view shape are timeout-bound (few rounds complete under load),
so pure-random selection reliably misses a kind at low op-count and flakes the
post() coverage check.  Round-robin the kind by worker id in the first ops
(deterministic whether one worker does K ops or K workers do 1 each), then go
random -- the same fix p125/p126/p172 needed.

Stresses: array.array buffer-export vs resize, ob_exports refcount across hubs,
realloc-vs-live-memoryview use-after-free, BufferError serialization under M:N,
torn length/capacity, struct.pack_into target lifetime, write-through-view
consistency across a park.

Good TSan / controlled-M:N-replay target: the ob_exports increment/decrement vs
the resize-path read is a textbook FT data race; a TSan report on that counter
often localizes the UAF before the universe-value assert even fires.
"""
import array
import struct

import harness
import runloom

# Typecode 'q' = signed 64-bit; itemsize 8.  A wide item makes a torn value
# (key read through a freed slot) overwhelmingly likely to leave the universe.
TYPECODE = "q"
ITEMSIZE = array.array(TYPECODE).itemsize       # 8

# Base length: enough cells that the read/write loop touches several cache lines
# and the array's ob_item spans multiple pages, and large enough that an append
# past capacity provokes a real realloc(+copy) of the live buffer.
BASE_LEN = 512

# Number of churn appends/extends in the "after-release" phase -- pushes the
# array well past its current capacity so the growth is a genuine reallocation,
# not an in-place slack fill.
GROW_N = 300

# struct format for one 'q' cell, native byte order (matches array's storage).
CELL_FMT = "q"

# Stride at which the viewer writes markers through the view.  The churner's
# pack_into targets a cell that is NEVER a multiple of this, so an in-place
# pack_into write can't collide with the viewer's marker oracle.
MARK_STRIDE = 37
PACK_IDX = 1                                     # 1 % 37 != 0 -> never marked

NCHURN = 4          # append / extend / pop / pack_into-resize
NSHAPE = 2          # single-view / double-view


def f(i):
    """Deterministic index -> value.  a[i] must equal f(i) for every seeded
    cell; a value read through a live view that is neither f(i) nor a value this
    viewer itself wrote is a torn/corrupted read from a freed slot.  A wide,
    well-mixed function so a coincidental match is essentially impossible."""
    v = (i * 0x9E3779B97F4A7C15 + 0x12345) ^ 0xA5A5A5A5A5A5A5A5
    # Keep it inside signed 64-bit range for the 'q' typecode.
    v &= 0x7FFFFFFFFFFFFFFF
    return v


def fresh_array():
    """A fresh array.array('q', ...) with a[i] == f(i) for i in [0, BASE_LEN)."""
    return array.array(TYPECODE, (f(i) for i in range(BASE_LEN)))


def write_marker(i):
    """A distinct, in-universe marker value the viewer writes through the view at
    index i; chosen disjoint-looking from f(i) but still a legal cell value."""
    v = (f(i) ^ 0x0F0F0F0F0F0F0F0F) + 0x77
    return v & 0x7FFFFFFFFFFFFFFF


def check_cell(H, wid, i, got, written):
    """Validate one value read through the live view.  `written` is the set of
    indices the viewer has written a marker into (so the legal value there is
    write_marker(i), not f(i)).  Returns False on the first violation."""
    expect = write_marker(i) if i in written else f(i)
    if got != expect:
        H.fail("viewer read TORN value at index {0}: got {1!r} expected {2!r} "
               "({3}) -- a read through a freed/relocated buffer (array buffer-"
               "export vs concurrent resize UAF under M:N)".format(
                   i, got, expect,
                   "marker" if i in written else "f(i)"))
        return False
    return True


def viewer(H, wid, arr, gate_parked, gate_churned, gate_released, written,
           slot, counts, double):
    """Take a live memoryview over `arr`, read/write through it, and prove the
    buffer was not freed/relocated while the concurrent churner attempts
    resizes.

    Ordering (this is load-bearing -- see below): the viewer trips `gate_parked`
    with the view LIVE, then WAITS on `gate_churned` -- it does NOT release the
    view until the churner has finished every blocked-resize attempt.  Only then
    does it release the view(s) and trip `gate_released` so the churner's
    SUCCESS phase begins with ob_exports back at zero.

    Why the wait (and not a bare yield_now): a viewer that merely `gate_parked`
    + `yield_now()` and then falls into its `finally`/release RACES the churner
    -- the view is usually already released by the time the churner resizes, so
    the resize legally succeeds and a naive oracle reads that as a "leaked
    resize" (a FALSE POSITIVE: instrumentation showed ~99.9% of such "leaks"
    happened with the view already dead).  Holding the view live across the
    churner's whole blocked phase is what makes "a resize succeeded while a view
    was exported" a TRUE invariant violation rather than a gate artifact."""
    ok = True
    mv = memoryview(arr)
    mv2 = memoryview(arr) if double else None
    try:
        # Full read pass: every seeded cell must equal f(i).
        for i in range(BASE_LEN):
            if not check_cell(H, wid, i, mv[i], written):
                ok = False
                break
        if ok:
            # Write markers through the view at a deterministic spread of cells.
            for i in range(0, BASE_LEN, MARK_STRIDE):
                mv[i] = write_marker(i)
                written.add(i)
            # If double-view, the SECOND view must observe the same writes
            # (same underlying buffer) -- a torn export would alias a stale copy.
            if mv2 is not None:
                for i in range(0, BASE_LEN, MARK_STRIDE):
                    if not check_cell(H, wid, i, mv2[i], written):
                        ok = False
                        break
        # Hand off to the churner WITH the view(s) live (the raw C pointer is
        # captured on this fiber's stack), then PARK on gate_churned -- the
        # churner's resize attempts land in this window and must all be blocked
        # by the export guard, NOT free this buffer under us.
        gate_parked.done()
        gate_churned.wait()
        # The churner has finished its blocked phase; the buffer must be intact.
        # Re-read every written cell: a changed value (or a SIGSEGV / out-of-
        # universe value) means the buffer was relocated under the live view.
        if ok:
            for i in range(0, BASE_LEN, MARK_STRIDE):
                if not check_cell(H, wid, i, mv[i], written):
                    ok = False
                    break
    finally:
        # Now release ALL exported views.  Only after ob_exports drops to zero
        # is the churner's SUCCESS phase (gate_released) allowed to resize.
        if mv2 is not None:
            mv2.release()
        mv.release()
        gate_released.done()
    if ok:
        counts["views"][slot] += 1
    return ok


def attempt_resize(arr, kind):
    """Perform ONE resize operation of the given kind on `arr`.  Raises
    BufferError if a view is exported (the guard), IndexError on pop-empty, or
    nothing if it succeeds."""
    if kind == 0:
        arr.append(f(BASE_LEN))
    elif kind == 1:
        arr.extend((f(BASE_LEN + j) for j in range(8)))
    elif kind == 2:
        arr.pop()
    else:
        # A pack_into WRITE (legal while a view is live -- it never resizes)
        # immediately followed by an append (a resize -- must be blocked).  This
        # checks that a transient pack_into export doesn't corrupt the ob_exports
        # accounting of the still-live memoryview.  Target PACK_IDX, a cell the
        # viewer never marks (its markers land on multiples of 37), and write
        # that cell's own seeded value f(PACK_IDX) -- a value-preserving in-place
        # write, so it cannot conflict with the viewer's marker oracle while
        # still exercising the pack_into export path.
        struct.pack_into(CELL_FMT, arr, PACK_IDX * ITEMSIZE, f(PACK_IDX))
        arr.append(f(BASE_LEN))


def churn_blocked(H, wid, arr, kind):
    """Attempt resize(s) of `arr` while a view is exported.  Every attempt MUST
    raise BufferError (return 'blocked'); a SUCCESS is the UAF bug; any OTHER
    exception is also a fault.  Returns 'blocked' | 'leaked' | 'badexc'.

    Drives the assigned kind PLUS a couple of the other kinds in the same live-
    view window, so each round widens the surface where a torn ob_exports could
    let a realloc slip past the guard."""
    for k in (kind, (kind + 1) % NCHURN, (kind + 2) % NCHURN):
        try:
            attempt_resize(arr, k)
        except BufferError:
            continue              # the correct, guarded outcome for this kind
        except Exception as exc:  # noqa: BLE001
            H.fail("resize while view exported raised {0} (not BufferError): "
                   "{1} -- kind={2}".format(type(exc).__name__, exc, k))
            return "badexc"
        # No exception: the resize SUCCEEDED while a view was live -> the export
        # guard did not serialize against the realloc path: a use-after-free.
        H.fail("resize SUCCEEDED while a memoryview was exported (kind={0}) -- "
               "ob_exports guard did not block the realloc: a buffer-export-vs-"
               "resize use-after-free (the live view now points into a freed/"
               "relocated allocation)".format(k))
        return "leaked"
    return "blocked"


def churn_after_release(H, wid, arr, kind, slot, counts):
    """After the viewer has released its view(s), the array must be freely
    resizable again.  Grow it past capacity (a real realloc) and re-assert the
    structural invariants: typecode/itemsize unchanged, no torn length/capacity.
    Returns True on success."""
    pre_len = len(arr)
    try:
        if kind == 2:
            # The 'pop' kind: shrink then grow, exercising both directions.
            for _ in range(min(8, pre_len)):
                arr.pop()
        for j in range(GROW_N):
            arr.append(f(BASE_LEN + j))
        arr.extend((f(BASE_LEN + GROW_N + j) for j in range(8)))
    except BufferError as exc:
        # A BufferError here means a view is STILL counted as exported after
        # release(): a torn/leaked ob_exports that never decremented -- the
        # mirror-image FT bug (resize wrongly blocked forever).
        H.fail("resize BLOCKED after view release: {0} -- ob_exports did not "
               "decrement (torn export counter under M:N, resize stuck)".format(
                   exc))
        return False
    except Exception as exc:  # noqa: BLE001
        H.fail("post-release resize raised {0}: {1}".format(
            type(exc).__name__, exc))
        return False
    # Structural invariants on the grown array.
    if arr.typecode != TYPECODE:
        H.fail("typecode changed under resize: {0!r} != {1!r}".format(
            arr.typecode, TYPECODE))
        return False
    if arr.itemsize != ITEMSIZE:
        H.fail("itemsize changed under resize: {0} != {1}".format(
            arr.itemsize, ITEMSIZE))
        return False
    nbytes = memoryview(arr).nbytes
    if len(arr) * ITEMSIZE != nbytes:
        H.fail("torn length/capacity: len {0} * itemsize {1} = {2} != buffer "
               "nbytes {3} -- length/allocation desync after concurrent "
               "resize".format(len(arr), ITEMSIZE, len(arr) * ITEMSIZE, nbytes))
        return False
    counts["resized"][slot] += 1
    return True


def worker(H, wid, rng, state):
    counts = state["counts"]
    slot = wid & 1023
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Deterministic coverage of all four churn kinds AND both view shapes in
        # the first ops; random after that to preserve the concurrent mix.  Pure
        # random misses a kind at low op-count under load (the suite's old
        # flaky-coverage bug -- p125/p126/p172).
        if i < NCHURN * NSHAPE:
            kind = (wid + i) % NCHURN
            double = ((wid + i) // NCHURN) % NSHAPE == 1
        else:
            kind = rng.randrange(NCHURN)
            double = rng.getrandbits(1) == 1
        i += 1

        arr = fresh_array()
        written = set()
        # Three gates pin the ordering so the resize-attempt window provably
        # overlaps a LIVE view (without this, the churner mostly resizes a view
        # the viewer already released -- a gate artifact, not a runtime leak):
        #   gate_parked    viewer -> churner: view is now live, go resize.
        #   gate_churned   churner -> viewer: blocked-resize attempts are done,
        #                  you may release the view now.
        #   gate_released  viewer -> churner: view released (ob_exports==0), the
        #                  SUCCESS-resize phase may begin.
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
                        kind=kind):
            res = None
            try:
                # Run while the viewer holds the view LIVE: every resize must be
                # BufferError.  Then release the viewer (gate_churned) so it can
                # drop the view before our SUCCESS phase.
                gate_parked.wait()
                res = churn_blocked(H, wid, arr, kind)
                if res == "blocked":
                    counts["blocked"][slot] += 1
                gate_churned.done()
                # Wait for the viewer to release its view(s), then resize for
                # real and re-check structure.
                gate_released.wait()
                churn_after_release(H, wid, arr, kind, slot, counts)
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
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {"counts": {"views": [0] * 1024, "blocked": [0] * 1024,
                          "resized": [0] * 1024}}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    views = sum(H.state["counts"]["views"])
    blocked = sum(H.state["counts"]["blocked"])
    resized = sum(H.state["counts"]["resized"])
    H.log("clean-view-passes={0} resize-BLOCKED-while-exported={1} "
          "resize-SUCCEEDED-after-release={2} ops={3} (any torn value / "
          "leaked-resize / structural desync already failed fast)".format(
              views, blocked, resized, H.total_ops()))
    H.check(H.total_ops() > 0, "no rounds completed -- the buffer-export vs "
            "resize race window was never exercised")
    # Both halves of the guard must have actually been exercised: a resize that
    # was correctly BLOCKED while a view was live, AND a resize that correctly
    # SUCCEEDED once the view was released.
    H.check(blocked > 0, "no resize was ever attempted while a view was "
            "exported -- the BufferError guard path was never exercised")
    H.check(resized > 0, "no resize ever succeeded after view release -- the "
            "export-counter-decrement path was never exercised")
    H.require_no_lost()


if __name__ == "__main__":
    harness.main("p404_array_buffer_resize", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="shared array.array('q') read/written through a live "
                          "memoryview across a park while a sibling hub "
                          "append/extend/pop-churns it: a resize while a view "
                          "is exported MUST raise BufferError (not free the "
                          "buffer under the view), every read==f(index) or a "
                          "written value, typecode/itemsize stable and "
                          "len*itemsize==nbytes -- anything else is an FT "
                          "buffer-export-vs-resize use-after-free")
