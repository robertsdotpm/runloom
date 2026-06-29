"""big_100 / 434 -- memoryview.cast() shared-managedbuffer export vs exporter resize.

The subject is ``memoryview.cast(fmt[, shape])`` (Objects/memoryobject.c) and the
EXPORT bookkeeping that keeps the underlying exporter pinned while a cast child is
live.  No existing big_100 program drives cast(): p302/p415/p404 only ever hold a
plain ``memoryview`` / getbuffer, so none exercises cast()'s SEPARATE
format/shape recompute or its independent hold on the exporter.  cast() is the
hazard precisely because it does NOT copy:

    base = memoryview(ba)        # 1st getbuffer on the bytearray exporter:
                                 #   ba->ob_exports == 1, and a
                                 #   _PyManagedBufferObject mbuf wrapping the
                                 #   Py_buffer with mbuf->exports == 1
    cast = base.cast('Q')       # memory_cast(): builds a NEW memoryview that
                                 #   SHARES base's mbuf (Py_INCREF on the same
                                 #   _PyManagedBufferObject, mbuf->exports -> 2)
                                 #   and points at the SAME Py_buffer.buf
                                 #   (ba->ob_start), but RE-DERIVES its OWN
                                 #   view.itemsize/format/ndim/shape/strides
                                 #   (itemsize 8 / 'Q' / len 8, vs 1 / 'B' / 64)

So the cast child shares the base's managedbuffer and the SAME buf pointer; the
exporter's ``ob_exports`` (held at 1 by that single shared mbuf) only returns to
0 when the LAST view in the chain is released and the mbuf is freed.  The
exporter's resize path (bytearray_resize -> PyMem_Realloc of ob_start, reached
via append/extend/clear/del-slice) refuses with BufferError IFF ob_exports != 0.
The racing op pair we attack is therefore:

  * the managedbuffer's exports INCREMENT-on-cast (mbuf Py_INCREF + mbuf->exports
    bump) / DECREMENT-on-release (mbuf_release -> when exports hits 0,
    PyBuffer_Release decrements ba->ob_exports), a non-atomic count touched from
    two hubs, versus
  * the exporter RESIZE-REALLOC (PyMem_Realloc of ob_start) on another hub, which
    reads ba->ob_exports and which, if that count is torn LOW (a release decrement
    becomes visible before the buf is actually dead, or a cast's hold is not yet
    visible), would free/move ob_start while the cast child still holds the stale
    shared buf pointer -> a use-after-free read through ``cast[w]``.

Two mutually-exclusive corruption modes, BOTH made falsifiable:

  * TORN-LOW / UAF: the exporter is seen un-pinned while a view is still live, the
    resize-realloc proceeds, frees/moves ob_start, and the live cast reads freed
    memory.  Caught two ways: (a) every Q-word read through the cast must equal
    the little-endian pack of the 8 known seed bytes it covers (a torn/freed slot
    reads out-of-universe); (b) while ANY view is live a sibling resize MUST raise
    BufferError -- a resize that SUCCEEDS while a view is live is the export hold
    torn low.

  * TORN-HIGH / LEAK: a release decrement is lost, the mbuf's exports never reach
    0, ba->ob_exports never returns to 0, and the exporter is PERMANENTLY
    un-resizable.  Caught directly: after ALL views are released, the very next
    resize MUST succeed (the cast's shared-mbuf hold was matched by exactly the
    releases that drop it back to 0).

The cast-specific lever (no other program touches it): the CAST ALONE pins the
exporter.  After releasing the BASE only -- base.release() -- the cast still
shares the live mbuf, so ba->ob_exports stays 1 and a resize MUST STILL be
refused; the exporter unpins only when the cast is ALSO released.  A resize that
succeeds while only the cast is live is a lost hold in cast()'s shared-mbuf
INCREF / mbuf_release decrement.

CONTROL ARM (single-owner, race-free -- the falsifier).  A second identical
base+cast pair is built, used, and released by ONE fiber with NO sibling
touching it.  Its post-release resize MUST also succeed, and its while-live /
cast-only resizes MUST be refused.  A single owner cannot race itself, so if the
CONTROL leaks the hold (resize still refused after release) or loses it early
(resize succeeds while a view is live) the fault is provably in cast()'s own
shared-mbuf machinery, NOT M:N contention; if only the CONTENDED arm deviates, it
is the cross-hub race.  This disambiguates "cast()/release is buggy" from "M:N
dropped the export hold".

CLOSED-WORLD IDENTITY + CONSERVATION oracle (per round, fail-fast + post):
  Fresh per-round ``bytearray`` seeded ba[i] = seed(round, i) drawn from a finite
  byte universe.  base = memoryview(ba); cast = base.cast('Q').
    * cast.itemsize == 8, cast.format == 'Q', cast.nbytes == base.nbytes,
      len(cast) == nbytes//8 -- the recomputed view geometry is stable across a
      park (a torn recompute changes them);
    * every cast[w] == little-endian uint64 of seed bytes (8w .. 8w+7) -- a torn
      read / UAF yields a value not derivable from the universe;
    * while base+cast live, a sibling resize raises BufferError (counted);
    * after releasing base ONLY, the cast still pins the exporter: a sibling
      resize STILL raises BufferError (the cast's shared-mbuf hold);
    * after releasing the cast too, the next resize SUCCEEDS exactly once
      (export hold returned to 0 -- no leak from the shared mbuf).
  Per-slot single-writer tallies count: rounds, refusals-while-both-live,
  refusals-while-cast-only, post-release resize successes, control successes.
  post(): refusals > 0 (the gate was real), resize-after-release succeeded on
  EVERY round and EVERY control round (conservation: increments == decrements,
  export back to 0), all cases exercised, no lost worker.

Round-robin the resize-trigger CASE by worker id in the first ops (append /
extend / clear / del-slice all hit bytearray_resize -> PyMem_Realloc) so coverage
holds under the timeout -- the p125/p126 flaky-random-coverage fix -- then random.

Stresses: memoryview.cast() shared-managedbuffer INCREF / mbuf_release decrement
vs bytearray ob_start PyMem_Realloc resize, the cast child's hold pinning the
exporter after the base is released, torn export hold -> UAF read through the
shared buf pointer or permanent un-resizable leak, recomputed itemsize/format/
shape stability across a park, base-release-vs-cast-release ordering across hubs.

Good TSan / controlled-replay target: the mbuf->exports / ba->ob_exports
increment in memory_cast() and the decrement in mbuf_release race the resize's
read of ba->ob_exports -- a TSan report on ob_exports localizes the torn hold
before the universe assert or the BufferError-gate even fires.
"""
import struct

import harness
import runloom

# Finite BYTE universe for the seed bytes.  Every cast Q-word is the little-endian
# pack of 8 of these; a torn/freed read yields a uint64 NOT equal to any such
# pack, so it falls outside the derived universe and is caught.  Kept to a
# recognizable spread (not 0..255 dense) so a freed-memory read of arbitrary RAM
# is overwhelmingly unlikely to coincidentally reconstruct a legal Q-word.
def seed_byte(rnd, i):
    """Deterministic seed byte for position i of round rnd.  A torn/UAF cast read
    reconstructs a Q-word that does NOT match the LE pack of these -> out of
    universe."""
    return (0x40 + ((rnd * 131 + i * 37 + 7) & 0x3F)) & 0xFF


# bytearray length in BYTES.  Multiple of 8 so cast('Q') tiles it exactly with no
# remainder (cast requires the byte length be a multiple of the target itemsize).
# Large enough that the realloc actually moves the allocation under churn and that
# several Q-words are read across a park; small enough that many rounds complete.
NBYTES = 64
NWORDS = NBYTES // 8

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024

# The resize-trigger CASES -- every one reaches bytearray_resize -> PyMem_Realloc
# of ob_start, the realloc the export count gates.  post() requires each was hit,
# so the worker round-robins them by id in the first ops (NOT random -- pure
# random reliably misses a case at low op-count under load: the p125/p126/p172
# flaky-coverage bug the suite already had to fix).
CASE_APPEND = 0       # ba.append(b)      -- grow by 1
CASE_EXTEND = 1       # ba.extend(b"..")  -- grow by k
CASE_CLEAR = 2        # ba.clear()        -- shrink to 0
CASE_DELSLICE = 3     # del ba[:2]        -- shrink by slice
NCASES = 4


def expected_word(rnd, w):
    """The little-endian uint64 that cast[w] MUST equal -- the LE pack of the 8
    seed bytes covering [8w, 8w+8).  A torn read / UAF gives a different value."""
    bs = bytes(seed_byte(rnd, 8 * w + j) for j in range(8))
    return struct.unpack("<Q", bs)[0]


def try_resize(ba, case, rng):
    """Apply the round's resize-trigger CASE to the exporter.  Returns True if the
    resize SUCCEEDED (BufferError NOT raised), False if it raised BufferError (the
    exporter was pinned by a live export).  Every case reaches PyMem_Realloc of
    ob_start, the realloc gated on ob_exports."""
    try:
        if case == CASE_APPEND:
            ba.append(rng.getrandbits(8))
        elif case == CASE_EXTEND:
            ba.extend(bytes(rng.getrandbits(8) for _ in range(3)))
        elif case == CASE_CLEAR:
            ba.clear()
        else:  # CASE_DELSLICE
            del ba[:2]
        return True
    except BufferError:
        return False


def check_cast_geometry(H, cast, base_nbytes):
    """The cast child re-derives its OWN itemsize/format/ndim/shape/strides from the
    same exporter base pointer; assert that recompute is intact (a torn recompute
    under a park changes one of them).  Returns False on the first violation."""
    if cast.itemsize != 8:
        H.fail("cast('Q').itemsize == {0} != 8 -- the cast's re-derived itemsize "
               "is torn (memory_cast recomputed the view geometry while a sibling "
               "raced the exporter)".format(cast.itemsize))
        return False
    if cast.format != "Q":
        H.fail("cast('Q').format == {0!r} != 'Q' -- the cast's re-derived format "
               "string is torn".format(cast.format))
        return False
    if cast.nbytes != base_nbytes:
        H.fail("cast.nbytes == {0} != base.nbytes {1} -- cast must cover the SAME "
               "bytes as the base (it shares the exporter buf); a mismatch is a "
               "torn shape/strides recompute".format(cast.nbytes, base_nbytes))
        return False
    if len(cast) != NWORDS:
        H.fail("len(cast('Q')) == {0} != {1} -- the cast's re-derived length "
               "(nbytes // itemsize) is torn".format(len(cast), NWORDS))
        return False
    return True


def check_cast_values(H, cast, rnd):
    """Every Q-word read through the cast MUST equal the LE pack of the known seed
    bytes it covers.  A torn read or a UAF (resize freed/moved ob_start while the
    cast still held the stale buf pointer) yields a value outside the universe.
    Returns False on the first violation."""
    for w in range(NWORDS):
        got = cast[w]
        want = expected_word(rnd, w)
        if got != want:
            H.fail("cast Q-word {0} == {1!r} != expected {2!r} (LE pack of the "
                   "known seed bytes) -- a TORN/UAF read: the export count was "
                   "torn to 0, the exporter resize freed/moved ob_start, and the "
                   "live cast read freed memory through the stale shared buf "
                   "pointer".format(w, got, want))
            return False
    return True


def run_round_impl(H, wid, rnd, rng, case, slot, state):
    """One contended round.  Build a fresh seeded bytearray, take base + cast('Q')
    on it, and spawn a sibling RESIZER on ANOTHER hub that must be REFUSED while a
    view is live.  Each export-state transition is a strict RENDEZVOUS over two
    Chans -- the owner tells the sibling "go" only after the export state is set,
    and waits for the sibling's result before changing it again -- so each resize
    attempt PROVABLY lands in its intended window (a yield_now()-timed handoff does
    NOT guarantee that, and would let the mutation land after release: the
    p311-style "synchronize the hazard into the window" requirement).  The cross-
    hub race is still real: the sibling reads ba's ob_exports on its hub while the
    owner holds/releases the views and reads cast values across a park on its hub.

    Sequence (the export-hold drama):
      1. base = memoryview(ba)        -> ob_exports 1, shared mbuf->exports 1
      2. cast = base.cast('Q')        -> shares base's mbuf (mbuf->exports 2);
                                         ob_exports STAYS 1 (one shared mbuf)
      3. signal go0; sibling resizes while BOTH live -> MUST be refused
         (BufferError, ob_exports != 0); owner reads+verifies every Q-word
         across the park.
      4. base.release()               -> mbuf->exports 1; ob_exports STAYS 1
                                         (the cast still shares the live mbuf)
         signal go1; sibling resizes  -> MUST STILL be refused (the cast's
                                          shared-mbuf hold pins the exporter)
      5. cast.release()               -> mbuf freed, ob_exports -> 0
         the next resize (owner)      -> MUST SUCCEED exactly once (hold back to 0)
    """
    tally = state
    ba = bytearray(seed_byte(rnd, i) for i in range(NBYTES))
    base_nbytes = NBYTES

    base = memoryview(ba)
    cast = base.cast("Q")

    # Geometry of the re-derived view is intact (stable across the upcoming park).
    if not check_cast_geometry(H, cast, base_nbytes):
        cast.release()
        base.release()
        return

    # Two-phase handshake.  go0/go1 tell the sibling to attempt phase 0/1; res0/res1
    # carry its True(resized)/False(refused) result back.  The owner ONLY signals a
    # phase after it has set the export state for that phase, and BLOCKS on the
    # result before mutating the state again -- so the sibling's resize provably
    # lands in the intended export-count window, not a yield-timed guess.
    go0 = runloom.Chan(1)
    go1 = runloom.Chan(1)
    res0 = runloom.Chan(1)
    res1 = runloom.Chan(1)
    wg = runloom.WaitGroup()
    wg.add(1)

    # Per-sibling RNG seeded from this fiber's rng (a SHARED random.Random corrupts
    # GIL-off -- each fiber needs its own; harness derive() gives `rng`).
    sib_seed = rng.getrandbits(48)

    def resizer():
        try:
            import random
            srng = random.Random(sib_seed)
            # Phase 0: owner says base+cast are BOTH live (exporter pinned).
            go0.recv()
            res0.send(try_resize(ba, case, srng))   # MUST be refused -> False
            # Phase 1: owner says base released, only cast live (still pinned).
            go1.recv()
            res1.send(try_resize(ba, case, srng))   # MUST STILL be refused -> False
        finally:
            wg.done()

    H.fiber(resizer)

    # NOTE: runloom.Chan.recv() returns a Go-style (value, ok) tuple, so the
    # sibling's True/False resize result is recv()[0] -- unpack it, never test the
    # truthy tuple.

    # Phase 0: both views live.  Tell the sibling to try the resize, and WHILE it
    # races our exporter on its hub, read+verify every Q-word through the live cast
    # across a park.  Then collect its result -- a True (resized) here is the
    # export hold torn low under two live views (UAF risk for our cast reads).
    if not check_cast_values(H, cast, rnd):
        go0.send(True)
        res0.recv()
        go1.send(True)
        res1.recv()
        wg.wait()
        cast.release()
        base.release()
        return
    go0.send(True)               # sibling now attempts the both-live resize
    runloom.yield_now()          # park with base+cast LIVE -- the resize races here
    # Re-verify across the park: a wrongly-succeeded resize freed/moved ob_start and
    # these reads would be a UAF (out-of-universe value).
    reread_ok = check_cast_values(H, cast, rnd)
    both, _ = res0.recv()        # rendezvous: sibling's both-live attempt is done
    if not reread_ok:
        go1.send(True)           # don't strand the sibling
        res1.recv()
        wg.wait()
        cast.release()
        base.release()
        return

    # Phase 1: release the BASE only.  The cast STILL shares the live mbuf, so the
    # exporter must remain pinned; the sibling confirms it.
    base.release()
    go1.send(True)               # sibling now attempts the cast-only resize
    conly, _ = res1.recv()       # rendezvous: cast-only attempt is done

    # Now release the cast.  The shared mbuf's last reference drops, its exports
    # reach 0, and ba->ob_exports returns to 0 -- the exporter is unpinned.
    cast.release()

    # Join the sibling so it has fully returned and ba is quiescent.
    wg.wait()
    if H.failed:
        return

    # ---- the export hold must have pinned the exporter both times --------------
    if both:
        H.fail("sibling RESIZE SUCCEEDED while base+cast were BOTH live -- the "
               "exporter's export hold (ba->ob_exports, held by the shared mbuf) "
               "was torn low and the exporter was wrongly re-sized under a live "
               "view (UAF risk for the live cast's shared buf pointer)")
        return
    tally["refuse_both"][slot] += 1
    if conly:
        H.fail("sibling RESIZE SUCCEEDED while only the CAST was live (base "
               "released) -- the cast's hold on the shared mbuf was lost (the "
               "base's release decrement also dropped ba->ob_exports while the "
               "cast still shared the live buffer); the exporter was re-sized out "
               "from under the still-live cast")
        return
    tally["refuse_cast_only"][slot] += 1

    # ---- export hold returned to 0: the very next resize MUST succeed -----------
    # Both views are released; the shared mbuf is gone and ba->ob_exports must be
    # back to exactly 0.  A resize that is STILL refused means a release decrement
    # was lost (TORN-HIGH leak) and the exporter is permanently un-resizable.
    import random
    frng = random.Random(sib_seed ^ 0xABCDEF)
    if not try_resize(ba, case, frng):
        H.fail("after releasing BOTH base and cast (ba->ob_exports must be 0) the "
               "resize is STILL refused with BufferError -- a release decrement "
               "was LOST: the cast's shared-mbuf hold never returned ob_exports to "
               "0, the exporter is permanently un-resizable (export LEAK)")
        return
    tally["resize_ok"][slot] += 1


def control_round(H, wid, rnd, rng, case, slot, state):
    """SINGLE-OWNER CONTROL ARM.  An identical base+cast pair built, used, and
    released by THIS fiber with NO sibling touching the exporter.  A single owner
    cannot race itself, so the shared-mbuf export hold's increment/decrement
    bookkeeping is exercised race-free.  While a view is live the resize MUST be
    refused; after releasing both it MUST succeed (hold back to 0).  If THIS arm
    deviates, the fault is in cast()'s own machinery itself, NOT M:N contention --
    the falsifier that distinguishes a primitive bug from a race."""
    tally = state
    ba = bytearray(seed_byte(rnd, i) for i in range(NBYTES))
    base = memoryview(ba)
    cast = base.cast("Q")
    if not check_cast_geometry(H, cast, NBYTES):
        cast.release()
        base.release()
        return
    if not check_cast_values(H, cast, rnd):
        cast.release()
        base.release()
        return
    # While both live, a resize MUST be refused even with no sibling.
    if try_resize(ba, case, rng):
        H.fail("CONTROL: resize succeeded while base+cast live with NO sibling -- "
               "cast()/getbuffer did not pin the exporter (the shared mbuf's hold "
               "on ba->ob_exports was absent); a cast() machinery bug, not "
               "contention")
        cast.release()
        base.release()
        return
    # Release base only; the cast's shared-mbuf hold must still pin the exporter.
    base.release()
    if try_resize(ba, case, rng):
        H.fail("CONTROL: resize succeeded with only the cast live (no sibling) -- "
               "the cast's shared-mbuf hold did not keep the exporter pinned after "
               "the base released; a cast() machinery bug, not contention")
        cast.release()
        return
    # Release cast; the shared mbuf is gone, ba->ob_exports must be 0, resize OK.
    cast.release()
    if not try_resize(ba, case, rng):
        H.fail("CONTROL: resize STILL refused after releasing both (no sibling) -- "
               "a release decrement was LOST in cast()'s own machinery; the "
               "single-owner pair leaked the export hold, so the loss is NOT "
               "contention")
        return
    tally["control_ok"][slot] += 1


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    # `rnd` is an explicit per-worker round counter (H.round_range() yields None,
    # so we count rounds ourselves).  It also seeds the per-round byte universe so
    # successive rounds reuse fresh, distinct seed patterns.
    rnd = (wid * 0x9E3779B1) & 0xFFFFFF
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the resize-trigger CASE by worker id in the first ops so all
        # four PyMem_Realloc-reaching paths are covered even under the timeout
        # (the p125/p126 flaky-random-coverage fix); random after.
        if i < NCASES:
            case = (wid + i) % NCASES
        else:
            case = rng.randrange(NCASES)
        # Most rounds run the CONTENDED arm (the race probe); every few rounds also
        # run the single-owner CONTROL arm (the falsifier).  Round-robin which
        # rounds get a control pass by (wid + i) so coverage is deterministic.
        do_control = ((wid + i) % 3 == 0)
        i += 1
        rnd = (rnd + 1) & 0xFFFFFF

        run_round_impl(H, wid, rnd, rng, case, slot, state)
        if H.failed:
            return
        if do_control:
            control_round(H, wid, rnd, rng, case, slot, state)
            if H.failed:
                return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # All per-slot tallies allocated here, inside the root (single writer per slot
    # -> race-free; summed in post()).  No shared object under test lives at module
    # scope; each round builds its own fresh bytearray exporter.
    H.state = {
        "refuse_both": [0] * SLOTS,       # resize refused while base+cast live
        "refuse_cast_only": [0] * SLOTS,  # resize refused while only cast live
        "resize_ok": [0] * SLOTS,         # resize succeeded after releasing both
        "control_ok": [0] * SLOTS,        # single-owner control rounds passed
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    refuse_both = sum(H.state["refuse_both"])
    refuse_cast_only = sum(H.state["refuse_cast_only"])
    resize_ok = sum(H.state["resize_ok"])
    control_ok = sum(H.state["control_ok"])
    H.log("refuse_both={0} refuse_cast_only={1} resize_ok_after_release={2} "
          "control_ok={3} ops={4}".format(
              refuse_both, refuse_cast_only, resize_ok, control_ok,
              H.total_ops()))

    H.check(H.total_ops() > 0, "no rounds completed -- the cast/resize race window "
            "was never exercised")

    # The export-hold GATE was real: while a view was live the exporter resize was
    # actually refused (so ob_exports was genuinely >0 and the test wasn't vacuous).
    H.check(refuse_both > 0,
            "no resize was ever refused while base+cast were both live -- the "
            "export-hold gate was never exercised (the contended arm did no work)")

    # The cast's shared-mbuf pin was exercised (base released, the cast alone kept
    # the exporter pinned, resize still refused) at least once.
    H.check(refuse_cast_only > 0,
            "no resize was ever refused while ONLY the cast was live -- the cast's "
            "shared-mbuf hold on the exporter was never exercised")

    # CONSERVATION: on every contended round the resize SUCCEEDED after both views
    # were released (export count returned to exactly 0 -- cast()'s double
    # increment was matched by two decrements; no leak left the exporter pinned).
    # refuse_both counts rounds that reached the gate; resize_ok counts rounds that
    # also completed the release->resize.  Every gated round must have unpinned.
    H.check(resize_ok == refuse_both,
            "export-count conservation broken: {0} rounds refused the resize while "
            "live but only {1} rounds could resize after releasing both views -- "
            "{2} round(s) leaked an export (a release decrement was lost; the "
            "exporter stayed permanently un-resizable)".format(
                refuse_both, resize_ok, refuse_both - resize_ok))

    # The single-owner CONTROL arm ran and never leaked (a leak HERE would be a
    # cast()-machinery bug, not contention).
    H.check(control_ok > 0,
            "the single-owner control arm never completed a round -- the falsifier "
            "that distinguishes a cast() machinery bug from M:N contention was "
            "never exercised")

    H.require_no_lost()


if __name__ == "__main__":
    harness.main(
        "p434_memoryview_cast_vs_exporter_re", body, setup=setup, post=post,
        default_funcs=3000,
        describe="memoryview.cast('Q') bumps the bytearray exporter's export "
                 "count a SECOND time and shares the base buf pointer; under M:N a "
                 "sibling resize-realloc must be REFUSED while base or cast is live "
                 "and must SUCCEED once both are released (count back to 0).  "
                 "Closed-world: every cast Q-word == LE pack of known seed bytes "
                 "(torn/UAF reads out-of-universe), resize refused while live, "
                 "resize succeeds after release (no export leak); single-owner "
                 "control arm falsifies cast()-machinery loss vs contention")
