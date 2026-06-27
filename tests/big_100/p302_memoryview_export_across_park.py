"""big_100 / 302 -- Py_buffer export count held LIVE across a park, cross-hub.

The buffer protocol is the one CPython lifetime guard no other big_100 program
exercises as an *export-count* target (p222 only uses a memoryview as recv_into
scratch; never holds one live across a yield).  When you `memoryview(ba)` over a
bytearray, the exporter bumps a NON-ATOMIC int field (`ob_exports` / the mbuf
export count) that MUST forbid `resize`/`extend`/`clear`/`append` -- every such
mutation must raise `BufferError` -- until every view is released.  That guard
is the only thing standing between a `bytearray.resize()` and a `realloc()` of
the backing store out from under a live view: drop the guard and the view points
at freed/moved memory -> silent garbage on read, or SIGSEGV.

Under M:N with the GIL off this is a real race.  A holder goroutine takes a live
memoryview over its bytearray (incrementing the export count), then PARKS
(sleep/yield), provably overlapping a sibling MUTATOR goroutine on ANOTHER hub
that attempts to resize the SAME bytearray.  Each holder/mutator pair gets a
FRESH, single-owner bytearray so exactly one live view pins it -- the
BufferError-while-live invariant is then EXACT, not an aggregate over a shared
pool (which would make a legitimate "all views released" append racily succeed
and false-positive the safety oracle).  If the export count's
read-modify-write desyncs (or a preempt fires mid-getbuffer / mid-releasebuffer),
either (a) the resize SUCCEEDS while a view is live -- a use-after-realloc
window -- or (b) the count never returns to 0 and the bytearray is permanently
un-resizable (a leaked export).

Two hard oracles, both biting (not "hope it crashes"):

  * SAFETY (boolean) -- while ANY view is live over a bytearray, EVERY resize
    attempt must raise BufferError.  A mutator that resized successfully with a
    live export sets the shared `uaf_observed` flag; post() H.check()s it is
    NEVER set.  One success = one UAF window = fail.
  * DATA-INTEGRITY -- the holder stamps a region with its own wid byte before
    taking the view, parks, and on resume re-reads the view: `view[0]` and a
    checksum of the slice MUST equal the stamped pattern.  A realloc that slid
    the buffer (a torn export count that let a resize through) changes these
    bytes silently.
  * CONSERVATION (post) -- after all views are released the bytearray MUST
    resize successfully (export count returned to 0); a leaked export would
    leave it permanently un-resizable, which we detect at teardown.

The holder and mutator rendezvous on a two-party barrier (Event pair) so the
resize attempt PROVABLY overlaps the live-view park -- the window is not left to
timing luck.  Each goroutine owns its own random.Random (a shared one corrupts
the Mersenne state GIL-off).

Invariant (fail-fast): no successful resize while a view is live (uaf_observed
stays False); every post-park view read equals the stamped wid pattern; after
release every bytearray resizes again.

Stresses: buffer-protocol export count (ob_exports) RMW under M:N, BufferError
resize guard held across a cooperative park + hub migration, memoryview.release
across migration, preempt-mid-getbuffer/releasebuffer, leaked-export detection.

Good TSan / controlled-M:N-replay target: the export count is a plain non-atomic
int field on the exporter; a data-race report on its increment/decrement is
often the first signal, before the BufferError oracle even fires.
"""
import random

import harness
import runloom

# Each holder/mutator pair contends over ONE bytearray with a SINGLE export
# count, so the BufferError invariant is exact: exactly one live view pins it,
# and any resize while that view is live must be refused.  The cross-hub race is
# the holder fiber (parked on one hub, view live) vs the mutator fiber (resizing
# on another hub) -- they rendezvous on an Event barrier so the resize provably
# overlaps the parked live view.  A pool of these is held in state so we can
# verify at teardown that every export was returned (no leaked-export stall).
BA_LEN = 32               # small: this is a correctness probe, not a scale soak
SLOT = 8                  # stamped region the view covers
PARK_SLEEP = 0.0008       # holder parks here while the mutator attempts a resize


def stamp_byte(wid):
    """The single byte this worker stamps across its region (never 0)."""
    return (wid & 0x7F) | 0x80


def try_resize(ba):
    """Attempt a real resize/mutation of `ba`.  Returns True iff the mutation
    SUCCEEDED (the buffer was actually grown/shrunk) -- which, while any view is
    live, is the UAF window we are hunting.  A BufferError (the correct guard
    firing) returns False.  We append-then-pop so a *successful* mutation leaves
    the length unchanged for the next round (the failure path mutates nothing)."""
    try:
        ba.append(0)        # forbidden while an export is live -> BufferError
    except BufferError:
        return False
    # The append SUCCEEDED -- this is the bug.  Undo it best-effort so the arena
    # stays usable for the conservation check, but the success is already recorded
    # by the caller.  (Under the bug the backing store may have moved; the pop is
    # itself then operating on possibly-freed memory, which is the whole hazard.)
    try:
        del ba[-1]
    except Exception:
        pass
    return True


def mutator(H, ba, ready_evt, done_evt, state, slot):
    """Sibling on (likely) another hub: wait until the holder has a LIVE view and
    is parked, then attempt to resize the holder's bytearray.  While the view is
    live this MUST raise BufferError; a success is a use-after-realloc window."""
    # Rendezvous: block until the holder signals "view is live, I am parking".
    ready_evt.wait()
    succeeded = try_resize(ba)
    if succeeded:
        # The export guard let a resize through while a view was live -> UAF.
        state["uaf_observed"][0] = True
        H.fail("SAFETY: bytearray resize/append SUCCEEDED while a memoryview "
               "export was live (use-after-realloc window) -- export count "
               "desynced under M:N")
    else:
        state["blocked"][slot] += 1   # the guard correctly refused (expected)
    done_evt.set()                    # let the holder unpark and finish


def holder(H, wid, ba, off, ready_evt, done_evt, state, slot):
    """Stamp our region, take a LIVE memoryview over it, signal the mutator, park
    (so the resize provably overlaps the live view), then verify integrity and
    that resize is refused while we still hold the view."""
    b = stamp_byte(wid)
    for i in range(off, off + SLOT):
        ba[i] = b
    mv = memoryview(ba)[off:off + SLOT]
    try:
        # Announce the view is live and we are about to park; the mutator on the
        # other hub now attempts its resize WHILE this view is outstanding.
        ready_evt.set()
        # PARK while the view is live -- this is the export-across-a-park window.
        runloom.sleep(PARK_SLEEP)
        runloom.yield_now()
        # Wait until the mutator has finished its (must-fail) resize attempt so
        # the overlap is provable, not merely likely.
        done_evt.wait()
        # DATA-INTEGRITY: a torn export count that let a realloc through would
        # have slid the backing store -> these bytes change silently.
        if mv[0] != b:
            H.fail("DATA-INTEGRITY: memoryview saw moved/garbage memory: "
                   "view[0]={0} != stamped {1} (backing store reallocated "
                   "under a live export)".format(mv[0], b))
            return
        if any(x != b for x in mv):
            H.fail("DATA-INTEGRITY: memoryview slice not all {0} after park "
                   "(partial realloc / torn buffer)".format(b))
            return
        # SAFETY (self): while WE still hold the view, our own resize must fail.
        if try_resize(ba):
            state["uaf_observed"][0] = True
            H.fail("SAFETY: holder's own resize SUCCEEDED while its memoryview "
                   "was still live")
            return
        H.op(wid)
    finally:
        mv.release()
    # CONSERVATION (per-op): exactly one view pinned this bytearray, so once we
    # release it the export count MUST be 0 -- a resize must now succeed.  A
    # leaked export (count never reached 0) leaves it permanently un-resizable.
    try:
        ba.append(0)
        del ba[-1]
    except BufferError:
        state["leaked"][slot] += 1
        H.fail("CONSERVATION: bytearray still un-resizable immediately after "
               "its only view was released -- a memoryview export leaked "
               "(export count never returned to 0)")


def worker(H, wid, rng, state):
    """Each worker, each round, gets a FRESH single-owner bytearray, runs as the
    holder (live view + park) and spawns a paired mutator on another hub to race
    the export-count guard.  Both the holder and mutator are fresh fibers per
    round so the sysmon can preempt them mid-getbuffer/releasebuffer."""
    slot = wid & 1023
    off = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Fresh bytearray every round: exactly ONE export (the holder's view)
        # pins it, so the BufferError-while-live invariant is exact.
        ba = bytearray(BA_LEN)
        ready_evt = runloom.sync.Event()
        done_evt = runloom.sync.Event()
        wg = runloom.WaitGroup()
        wg.add(2)

        def run_holder(ba=ba, off=off, ready_evt=ready_evt, done_evt=done_evt):
            try:
                holder(H, wid, ba, off, ready_evt, done_evt, state, slot)
            finally:
                wg.done()

        def run_mutator(ba=ba, ready_evt=ready_evt, done_evt=done_evt):
            try:
                mutator(H, ba, ready_evt, done_evt, state, slot)
            finally:
                wg.done()

        H.fiber(run_holder)
        H.fiber(run_mutator)
        wg.wait()
        H.task_done(wid)


def setup(H):
    H.state = {
        "uaf_observed": [False],     # the boolean SAFETY flag (one writer/race-ok)
        "blocked": [0] * 1024,       # mutator resize attempts correctly refused
        "leaked": [0] * 1024,        # per-op leaked-export (un-resizable) count
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    blocked = sum(H.state["blocked"])
    leaked = sum(H.state["leaked"])
    H.log("ops={0} export-guard-refusals(blocked resizes)={1} leaked-exports={2} "
          "uaf_observed={3}".format(H.total_ops(), blocked, leaked,
                                    H.state["uaf_observed"][0]))
    # SAFETY: the boolean invariant -- no resize ever succeeded while a view lived.
    H.check(not H.state["uaf_observed"][0],
            "UAF window observed: a resize succeeded while a memoryview export "
            "was live (export count desynced under M:N)")
    H.check(blocked > 0,
            "no resize was ever refused -- the BufferError guard never actually "
            "ran (the oracle would not have caught a UAF)")
    # CONSERVATION: every per-op bytearray was resizable again the instant its
    # only view was released (checked in the holder); any leak is a stuck export.
    H.check(leaked == 0,
            "CONSERVATION: {0} bytearray(s) still un-resizable after their only "
            "view was released -- a memoryview export leaked (count never "
            "returned to 0)".format(leaked))


if __name__ == "__main__":
    harness.main("p302_memoryview_export_across_park", body, setup=setup,
                 post=post, default_funcs=3000,
                 describe="hold a LIVE memoryview over a shared bytearray across "
                          "a park while a sibling on another hub attempts a "
                          "resize; resize-while-live MUST raise BufferError "
                          "(uaf_observed stays False) and view bytes survive")
