"""big_100 / 415 -- io.BytesIO.getbuffer() export held LIVE across a park, cross-hub.

A DISTINCT C buffer exporter from p302's bytearray/memoryview.  `io.BytesIO`
exposes the buffer protocol through `getbuffer()`, but the C plumbing is its own:
the view is NOT a plain memoryview over a contiguous `bytearray` store -- it is a
memoryview backed by an internal `_io._BytesIOBuffer` proxy object, and the export
is gated by a SEPARATE `exports` counter on the `bytesio` C struct (Modules/
_io/bytesio.c).  Every length-changing operation -- `truncate()`, a `write()`
that would re-`realloc` the backing `buf`, even `close()` -- first checks
`SHARED_EXPORTS_CHECK` / `self->exports > 0` and MUST raise `BufferError`
("Existing exports of data: object cannot be re-sized") while any view is live.
That non-atomic `exports` int is the only thing standing between a `truncate()`
and a `PyMem_Realloc()` of `buf` out from under a live view -> garbage read or
SIGSEGV.  Because the exporter, the proxy object, AND the resize-guard path all
differ from bytearray's `ob_exports`, this exercises a buffer-export RMW that
NO other big_100 program (p222 recv_into scratch, p302 bytearray) touches.

Under M:N with the GIL off this is a real race.  A HOLDER goroutine takes a live
`getbuffer()` view over its FRESH single-owner BytesIO (incrementing `exports`),
STAMPS a region through the writable view, then PARKS (sleep + yield), provably
overlapping a sibling MUTATOR goroutine on ANOTHER hub that attempts
`truncate()` / `write()`-grow on the SAME BytesIO.  Each holder/mutator pair gets
its OWN BytesIO so exactly one live view pins it -- the BufferError-while-live
invariant is then EXACT, not an aggregate over a shared pool (which would let a
legitimate "all views released" truncate racily succeed and false-positive the
oracle).  If the `exports` read-modify-write desyncs (or a preempt fires
mid-getbuffer / mid-release), either (a) the truncate SUCCEEDS while a view is
live -- a use-after-realloc window -- or (b) the count never returns to 0 and the
BytesIO is permanently un-resizable (a leaked export).

Three hard oracles, all biting (not "hope it crashes"):

  * SAFETY (boolean) -- while ANY view is live over a BytesIO, EVERY truncate/
    write-grow attempt must raise BufferError.  A mutator that resized
    successfully with a live export sets the shared `uaf_observed` flag; post()
    H.check()s it is NEVER set.  One success = one UAF window = fail.
  * DATA-INTEGRITY -- the holder stamps a region with its own wid byte THROUGH
    the live writable view, parks, and on resume re-reads the view: `view[0]` and
    every byte of the slice MUST equal the stamped pattern.  A realloc that slid
    `buf` (a torn export count that let a resize through) changes these bytes
    silently.  After release, `getvalue()` must ALSO still carry the stamp -- the
    store must not have been slid out from under the view.
  * CONSERVATION (post + per-op) -- after the only view is released the BytesIO
    MUST truncate/write successfully (exports returned to 0); a leaked export
    leaves it permanently un-resizable, which the holder detects right after
    release and post() re-confirms in aggregate.

Coverage is GUARANTEED, not random: the mutator round-robins its length-changing
op (truncate-shrink / truncate-grow / write-grow / close) by `(wid + round)`
so every guard branch is exercised even at a handful of ops/worker under load
(the suite's p125/p126/p172 flaky-random-coverage lesson -- pure random reliably
MISSES a branch at low op-count).  The holder and mutator rendezvous on an Event
pair so the resize attempt PROVABLY overlaps the live-view park; each goroutine
owns its own random.Random (a shared one corrupts the Mersenne state GIL-off).

Invariant (fail-fast): no successful truncate/write while a view is live
(uaf_observed stays False); every post-park view read AND post-release getvalue
equals the stamped wid pattern; after release every BytesIO resizes again.

Stresses: io.BytesIO `exports` count RMW under M:N (a distinct C exporter from
bytearray), BufferError resize-guard held across a cooperative park + hub
migration, getbuffer/release across migration, preempt-mid-getbuffer/release,
truncate/write-grow/close-vs-live-view, leaked-export detection.

Good TSan / controlled-M:N-replay target: `bytesio.exports` is a plain non-atomic
C int; a data-race report on its increment (getbuffer) / decrement (release) vs
the `SHARED_EXPORTS_CHECK` read in the truncate path is often the first signal,
before the BufferError oracle even fires.
"""
import io
import random

import harness
import runloom

# Each holder/mutator pair contends over ONE BytesIO with a SINGLE export count,
# so the BufferError invariant is exact: exactly one live view pins it, and any
# length-change while that view is live must be refused.  The cross-hub race is
# the holder fiber (parked on one hub, view live) vs the mutator fiber (resizing
# on another hub) -- they rendezvous on an Event barrier so the resize provably
# overlaps the parked live view.
BIO_LEN = 64               # small: a correctness probe, not a scale soak
SLOT = 16                  # stamped region the view covers (offset 0..SLOT)
PARK_SLEEP = 0.0008        # holder parks here while the mutator attempts a resize

# Mutator op selector: every length-changing path that the live-view guard must
# refuse.  Round-robined by (wid + round) so each branch is provably exercised.
NCASES = 4
CASE_TRUNCATE_SHRINK = 0   # truncate() shorter -> would free the tail
CASE_TRUNCATE_GROW = 1     # truncate() longer  -> would realloc/zero-extend buf
CASE_WRITE_GROW = 2        # seek past end + write -> realloc-grow the backing buf
CASE_CLOSE = 3             # close() -> would free buf entirely under the view


def stamp_byte(wid):
    """The single byte this worker stamps across its region (never 0, so a zeroed
    realloc'd region is distinguishable from the live stamp)."""
    return (wid & 0x7F) | 0x80


def try_mutate(bio, case, rng):
    """Attempt a real length-changing mutation of `bio` per `case`.  Returns True
    iff the mutation SUCCEEDED -- which, while a view is live, is the
    use-after-realloc window we hunt.  A BufferError (the correct guard firing)
    returns False.  Successful paths are undone best-effort so the BytesIO stays
    usable for the conservation check; the success is already recorded by the
    caller.  (Under the bug the backing store may have moved, so the undo is
    itself operating on possibly-freed memory -- the whole hazard.)"""
    try:
        if case == CASE_TRUNCATE_SHRINK:
            old = bio.tell()
            bio.truncate(BIO_LEN // 2)     # forbidden while exported -> BufferError
            bio.truncate(BIO_LEN)          # undo (only reached if guard FAILED)
            bio.seek(old)
        elif case == CASE_TRUNCATE_GROW:
            bio.truncate(BIO_LEN * 2)      # realloc-grow -> BufferError while live
            bio.truncate(BIO_LEN)          # undo
        elif case == CASE_WRITE_GROW:
            old = bio.tell()
            bio.seek(BIO_LEN + 8)          # past the end: the write must realloc buf
            bio.write(b"Z")                # forbidden while exported -> BufferError
            bio.truncate(BIO_LEN)          # undo the grow
            bio.seek(old)
        else:  # CASE_CLOSE
            bio.close()                    # would free buf under the live view
    except BufferError:
        return False
    return True


def mutator(H, bio, case, mseed, ready_evt, done_evt, state, slot):
    """Sibling on (likely) another hub: wait until the holder has a LIVE view and
    is parked, then attempt its length-changing op on the holder's BytesIO.  While
    the view is live this MUST raise BufferError; a success is a use-after-realloc
    window."""
    rng = random.Random(mseed)
    # Rendezvous: block until the holder signals "view is live, I am parking".
    ready_evt.wait()
    succeeded = try_mutate(bio, case, rng)
    if succeeded:
        # The export guard let a resize through while a view was live -> UAF.
        state["uaf_observed"][0] = True
        H.fail("SAFETY: BytesIO truncate/write/close SUCCEEDED while a "
               "getbuffer() export was live (use-after-realloc window) -- the "
               "`exports` count desynced under M:N (case {0})".format(case))
    else:
        state["blocked"][slot] += 1       # the guard correctly refused (expected)
        state["cases"][case][slot] += 1   # per-branch coverage tally
    done_evt.set()                        # let the holder unpark and finish


def holder(H, wid, bio, ready_evt, done_evt, state, slot):
    """Take a LIVE writable view via getbuffer(), STAMP our region THROUGH the
    view, signal the mutator, park (so the resize provably overlaps the live
    view), then verify integrity and that resize is refused while we still hold
    the view; after release confirm the BytesIO is resizable again and the stamp
    survived in getvalue()."""
    b = stamp_byte(wid)
    mv = bio.getbuffer()
    try:
        # Stamp THROUGH the live writable view (getbuffer() is read-write); a
        # realloc under the view would slide these very bytes.
        for i in range(SLOT):
            mv[i] = b
        # Announce the view is live and we are about to park; the mutator on the
        # other hub now attempts its resize WHILE this view is outstanding.
        ready_evt.set()
        # PARK while the view is live -- this is the export-across-a-park window.
        runloom.sleep(PARK_SLEEP)
        runloom.yield_now()
        # Wait until the mutator has finished its (must-fail) resize attempt so the
        # overlap is provable, not merely likely.
        done_evt.wait()
        # DATA-INTEGRITY: a torn export count that let a realloc through would have
        # slid the backing store -> these bytes change silently.
        if mv[0] != b:
            H.fail("DATA-INTEGRITY: getbuffer view saw moved/garbage memory: "
                   "view[0]={0} != stamped {1} (BytesIO buf reallocated under a "
                   "live export)".format(mv[0], b))
            return
        for i in range(SLOT):
            if mv[i] != b:
                H.fail("DATA-INTEGRITY: getbuffer slice not all {0} after park "
                       "(view[{1}]={2}) -- partial realloc / torn buffer".format(
                           b, i, mv[i]))
                return
        # SAFETY (self): while WE still hold the view, our own truncate must fail.
        try:
            bio.truncate(BIO_LEN // 2)
            state["uaf_observed"][0] = True
            H.fail("SAFETY: holder's own truncate SUCCEEDED while its getbuffer "
                   "view was still live")
            return
        except BufferError:
            pass                          # correct: guard refused while live
        H.op(wid)
    finally:
        mv.release()
    # CONSERVATION (per-op): exactly one view pinned this BytesIO, so once we
    # release it the export count MUST be 0 -- a truncate/write must now succeed.
    # A leaked export (count never reached 0) leaves it permanently un-resizable.
    try:
        bio.truncate(BIO_LEN * 2)
        bio.seek(BIO_LEN)
        bio.write(b"!")                   # write-grow path must also be unlocked
        bio.truncate(BIO_LEN)
    except BufferError:
        state["leaked"][slot] += 1
        H.fail("CONSERVATION: BytesIO still un-resizable immediately after its "
               "only getbuffer view was released -- an export leaked (count never "
               "returned to 0)")
        return
    # DATA-INTEGRITY (post-release): the stamp must survive in the settled store;
    # a slid realloc during the live window would have lost it.
    val = bio.getvalue()
    if len(val) < SLOT or any(val[i] != b for i in range(SLOT)):
        H.fail("DATA-INTEGRITY: stamped bytes did NOT survive in getvalue() after "
               "release -- the store was slid under the live view (stamp {0} lost; "
               "got {1!r})".format(b, bytes(val[:SLOT])))


def worker(H, wid, rng, state):
    """Each worker, each round, gets a FRESH single-owner BytesIO, runs as the
    holder (live getbuffer view + park) and spawns a paired mutator on another hub
    to race the export-count guard.  Both fibers are fresh per round so sysmon can
    preempt them mid-getbuffer/release.  The mutator's length-changing op is
    ROUND-ROBINED by (wid + round) so every guard branch (truncate-shrink/grow,
    write-grow, close) is provably exercised even at low op-count under load --
    the p125/p126 flaky-random-coverage lesson; pure random would MISS a branch."""
    slot = wid & 1023
    rnd = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the mutator case by (wid + round) -> deterministic coverage
        # whether one worker does many rounds or many workers do one each.
        case = (wid + rnd) % NCASES
        rnd += 1
        # Fresh BytesIO every round: exactly ONE export (the holder's view) pins
        # it, so the BufferError-while-live invariant is exact.
        bio = io.BytesIO(bytes(BIO_LEN))
        ready_evt = runloom.sync.Event()
        done_evt = runloom.sync.Event()
        wg = runloom.WaitGroup()
        wg.add(2)
        mseed = rng.getrandbits(48)

        def run_holder(bio=bio, ready_evt=ready_evt, done_evt=done_evt):
            try:
                holder(H, wid, bio, ready_evt, done_evt, state, slot)
            finally:
                wg.done()

        def run_mutator(bio=bio, case=case, mseed=mseed, ready_evt=ready_evt,
                        done_evt=done_evt):
            try:
                mutator(H, bio, case, mseed, ready_evt, done_evt, state, slot)
            finally:
                wg.done()

        H.fiber(run_holder)
        H.fiber(run_mutator)
        wg.wait()
        H.task_done(wid)


def setup(H):
    H.state = {
        "uaf_observed": [False],          # the boolean SAFETY flag (race-ok write)
        "blocked": [0] * 1024,            # mutator resize attempts correctly refused
        "leaked": [0] * 1024,             # per-op leaked-export (un-resizable) count
        # per-case coverage: cases[case][slot], summed in post() to prove every
        # guard branch (truncate-shrink/grow, write-grow, close) actually ran.
        "cases": [[0] * 1024 for _ in range(NCASES)],
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    blocked = sum(H.state["blocked"])
    leaked = sum(H.state["leaked"])
    case_tot = [sum(H.state["cases"][c]) for c in range(NCASES)]
    H.log("ops={0} export-guard-refusals(blocked resizes)={1} leaked-exports={2} "
          "uaf_observed={3} per-case(shrink/grow/write/close)={4}".format(
              H.total_ops(), blocked, leaked, H.state["uaf_observed"][0],
              case_tot))
    # SAFETY: the boolean invariant -- no resize ever succeeded while a view lived.
    H.check(not H.state["uaf_observed"][0],
            "UAF window observed: a truncate/write/close succeeded while a "
            "getbuffer() export was live (BytesIO `exports` count desynced under "
            "M:N)")
    H.check(blocked > 0,
            "no resize was ever refused -- the BufferError guard never actually "
            "ran (the oracle would not have caught a UAF)")
    # COVERAGE: every guard branch must have been exercised at least once (the
    # round-robin makes this deterministic; a 0 here means the workload never
    # reached that case -- e.g. funcs too small for one full NCASES cycle).
    for c in range(NCASES):
        H.check(case_tot[c] > 0,
                "guard branch case {0} (truncate-shrink/grow/write-grow/close) "
                "was never exercised -- coverage gap (need funcs/rounds >= "
                "{1})".format(c, NCASES))
    # CONSERVATION: every per-op BytesIO was resizable again the instant its only
    # view was released (checked in the holder); any leak is a stuck export.
    H.check(leaked == 0,
            "CONSERVATION: {0} BytesIO(s) still un-resizable after their only "
            "getbuffer view was released -- an export leaked (count never "
            "returned to 0)".format(leaked))
    H.require_no_lost("getbuffer holders/mutators")


if __name__ == "__main__":
    harness.main("p415_bytesio_getbuffer_across_park", body, setup=setup,
                 post=post, default_funcs=3000,
                 describe="hold a LIVE io.BytesIO.getbuffer() view over a fresh "
                          "single-owner BytesIO across a park while a sibling on "
                          "another hub attempts truncate/write-grow/close; "
                          "resize-while-live MUST raise BufferError (uaf_observed "
                          "stays False), stamped bytes survive, and after release "
                          "the BytesIO is resizable again (no leaked export)")
