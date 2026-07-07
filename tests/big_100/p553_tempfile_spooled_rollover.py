"""big_100 / 553 -- tempfile.SpooledTemporaryFile rollover conservation under M:N.

tempfile.SpooledTemporaryFile starts life holding its bytes in an in-memory
io.BytesIO.  Once the accumulated size EXCEEDS max_size (strictly greater --
writing exactly max_size stays in memory, one more byte rolls it), the object
calls rollover(): it opens a REAL on-disk temp file, copies the buffered prefix
out of the BytesIO into that fd, seeks the fd to the buffer's old position, and
swaps self._file from the BytesIO to the real file object, flipping _rolled to
True.  Every subsequent read/write/seek then goes to the fd.

WHERE M:N COULD BREAK IT (the gap this program probes).  The rollover is a small
multi-step state transition (open fd -> copy buffered prefix -> seek -> rebind
self._file -> set _rolled).  runloom drives it on a stackful coroutine that can
be PARKED at a cooperative yield in the middle of the surrounding write loop and
resumed on a DIFFERENT hub.  If, across that park, the buffered in-memory prefix
were lost (BytesIO contents dropped before the copy), double-written (copied
twice), the fd position left wrong (a later read starts at the wrong offset), or
the _file / _rolled fields torn (self._file swapped but _rolled still False, or
vice versa), then a read-back of the file would NOT equal the exact byte stream
that was written.  The whole point of SpooledTemporaryFile is that the rollover
is INVISIBLE: the bytes you read back must be exactly the bytes you wrote,
regardless of whether they currently live in RAM or on disk.

WHICH ORACLE IS LOAD-BEARING, AND WHY (single-owner rollover conservation):

  Each fiber owns its OWN SpooledTemporaryFile (created in a fiber-local
  variable, never shared).  It writes a KNOWN byte stream -- byte at offset i is
  a deterministic function of (wid, i), so a sibling's stream has DIFFERENT bytes
  at every offset -- that is guaranteed to CROSS max_size (so rollover always
  fires).  The write is split so a cooperative yield brackets the exact rollover
  boundary:

    1. write exactly max_size bytes           -> still in memory, _rolled False
    2. YIELD  (a sibling rolls its own file on another hub while we are parked)
    3. assert not _rolled                      (the boundary has not moved)
    4. write 1 byte                            -> crosses max_size, rollover fires
    5. YIELD  (parked immediately after the state transition)
    6. assert _rolled                          (the fd swap took effect)
    7. write the remaining tail, with yields interleaved
    8. seek(0), read the whole file back, assert it equals the EXACT expected
       stream for THIS wid (byte-for-byte), and assert the final length + _rolled
       state are correct.

  Because the file is single-owner, on a CORRECT runtime the read-back MUST equal
  the written stream every time -- SpooledTemporaryFile makes the same guarantee
  a plain file does, and a single owner touching it across cooperative yields is
  no different from touching it across function calls.  A mismatch means the
  rollover transition lost/doubled the buffered prefix, corrupted the fd
  position, or torn the _file/_rolled fields across a hub migration -- a real
  runloom bug.  On a correct runtime this program exits 0.

ORACLES:
  * LOAD-BEARING -- ROLLOVER CONSERVATION (worker, HARD, fail-fast).  Single-owner
    SpooledTemporaryFile; write a wid-derived stream crossing max_size with yields
    bracketing the rollover; read-back must equal the written stream exactly and
    _rolled must be True at the end (stream length > max_size).  Any content
    mismatch, wrong length, or wrong _rolled state is a FAIL.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-rollover
    (parked inside the copy/seek/rebind and never resumed) never returns; the
    watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually rolled files over
    (rollover_checks > 0), else the hazard was never exercised.

FAIL ON: read-back bytes != the exact wid-derived stream, wrong file length,
_rolled not True after crossing max_size, _rolled True before crossing it, or a
crash inside rollover.  There is NO shared-object arm here: SpooledTemporaryFile
is inherently single-owner (one buffer, one fd, one position), so the whole
program is the load-bearing oracle -- no report-only measured arm is needed.

Resource discipline: max_size is small (rollover ALWAYS fires) and each file is
CLOSED in a finally at the end of its round, so the on-disk fd count stays bounded
by the number of concurrently-running fibers rather than growing per round.
max_funcs caps the forever loop's --funcs 1000000 so we never open a million fds.

Stresses: SpooledTemporaryFile rollover (in-memory BytesIO -> on-disk fd swap),
the buffered-prefix copy + fd seek + self._file/_rolled rebind across a hub
migration, read-back exactness after rollover, single-owner file position
integrity under cooperative yields.
"""
import io
import tempfile

import harness
import runloom

# max_size is small so a modest stream always crosses it and rollover ALWAYS
# fires.  Writing exactly max_size stays in memory; one more byte rolls it.
MAX_SIZE = 256

# The tail written AFTER the rollover boundary.  Total stream length is
# MAX_SIZE + 1 + TAIL, comfortably past max_size so _rolled must end True.  Kept
# modest so the on-disk file stays small (file-heavy only after rollover).
TAIL = 512

# The tail is written in this many segments with a yield between each, so a
# sibling reliably interleaves while this fiber's fd-backed writes are in flight.
TAIL_SEGMENTS = 4

# Total length of the wid-derived stream this fiber writes.
TOTAL = MAX_SIZE + 1 + TAIL

# Prime modulus for the byte ramp so the pattern tiles without a short period.
PERIOD = 251

# Cap the inner per-fiber loop so a single fiber in --rounds 0 mode does not spin
# unboundedly within one round before checking task_done.
INNER_CAP = 100000


def stream_byte(wid, offset):
    """The expected byte at `offset` in wid's stream.

    A deterministic function of BOTH wid and offset, so a sibling fiber's stream
    differs at every offset: if a rollover leaked another fiber's buffer or fd
    into this file, the read-back bytes would not match this wid's ramp."""
    return ((wid * 131) + offset) % PERIOD


def expected_stream(wid):
    """The exact byte string this fiber writes into its SpooledTemporaryFile."""
    return bytes(stream_byte(wid, i) for i in range(TOTAL))


def rollover_check(H, wid, state, expected):
    """Single-owner rollover conservation check.

    Write a wid-derived stream that crosses max_size into a private
    SpooledTemporaryFile with yields bracketing the rollover, then read it back
    and assert exact equality + correct _rolled state.  The file is closed in a
    finally so its fd never outlives the round."""
    spool = tempfile.SpooledTemporaryFile(max_size=MAX_SIZE, mode="w+b")
    try:
        # ---- 1. fill exactly to max_size: still an in-memory BytesIO ----------
        spool.write(expected[:MAX_SIZE])

        # ---- 2. YIELD parked just BEFORE the rollover boundary ---------------
        runloom.yield_now()

        # ---- 3. the boundary must not have moved: exactly max_size is unrolled
        if spool._rolled:
            H.fail("SpooledTemporaryFile rolled over EARLY: _rolled is True after "
                   "writing exactly max_size ({0}) bytes, but rollover must only "
                   "fire once the size EXCEEDS max_size (wid {1}) -- the rollover "
                   "boundary was torn across a yield".format(MAX_SIZE, wid))
            return

        # ---- 4. one more byte crosses max_size -> rollover fires here ---------
        spool.write(expected[MAX_SIZE:MAX_SIZE + 1])

        # ---- 5. YIELD parked immediately AFTER the rollover transition -------
        runloom.yield_now()

        # ---- 6. the fd swap must have taken effect ---------------------------
        if not spool._rolled:
            H.fail("SpooledTemporaryFile did NOT roll over: _rolled is still False "
                   "after writing max_size+1 ({0}) bytes (wid {1}) -- the in-memory "
                   "BytesIO -> on-disk fd transition was lost across a yield".format(
                       MAX_SIZE + 1, wid))
            return
        # After rollover the backing object must be a real file, not the BytesIO.
        if isinstance(spool._file, io.BytesIO):
            H.fail("SpooledTemporaryFile._file is still an in-memory BytesIO after "
                   "_rolled went True (wid {0}) -- the self._file rebind was torn "
                   "from the _rolled flag across a hub migration".format(wid))
            return

        # ---- 7. write the tail on the fd, with yields interleaved ------------
        pos = MAX_SIZE + 1
        seg = TAIL // TAIL_SEGMENTS
        for s in range(TAIL_SEGMENTS):
            end = pos + seg if s < TAIL_SEGMENTS - 1 else TOTAL
            spool.write(expected[pos:end])
            pos = end
            runloom.yield_now()

        # ---- 8. read the whole file back and assert EXACT equality -----------
        spool.seek(0)
        got = spool.read()

        if len(got) != TOTAL:
            H.fail("rollover conservation broken: read back {0} bytes, wrote {1} "
                   "(wid {2}) -- the rollover copy lost or doubled part of the "
                   "buffered prefix, or the fd position was corrupted".format(
                       len(got), TOTAL, wid))
            return

        if got != expected:
            # Locate the first differing offset for a precise diagnosis.
            bad = 0
            for i in range(TOTAL):
                if got[i] != expected[i]:
                    bad = i
                    break
            H.fail("rollover conservation broken: read-back stream differs from the "
                   "written stream at offset {0} (got {1}, expected {2}) for wid {3} "
                   "-- the in-memory prefix was lost/doubled or a sibling's buffer/fd "
                   "leaked across the rollover transition".format(
                       bad, got[bad], expected[bad], wid))
            return

        # ---- final: length is past max_size, so it MUST remain rolled --------
        if not spool._rolled:
            H.fail("SpooledTemporaryFile._rolled reverted to False after a "
                   "{0}-byte stream (> max_size {1}) was written and read back "
                   "(wid {2}) -- the _rolled flag was torn".format(
                       TOTAL, MAX_SIZE, wid))
            return

        state["rollover_checks"][wid] += 1     # single-writer-per-slot, race-free
    finally:
        spool.close()


def worker(H, wid, rng, state):
    """Each fiber repeatedly runs the single-owner rollover conservation check on
    its OWN SpooledTemporaryFile.  The stream is wid-derived so a cross-fiber
    buffer/fd leak would corrupt the read-back."""
    expected = expected_stream(wid)          # this fiber's private, fixed stream
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            rollover_check(H, wid, state, expected)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # One race-free slot per worker (wid-indexed, single writer) for the
    # non-vacuity tally.  Allocated here where H.funcs is known.
    H.state = {
        "rollover_checks": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["rollover_checks"])
    H.log("SpooledTemporaryFile rollover conservation: {0} single-owner "
          "write-cross-max_size-read-back checks (each verified byte-exact "
          "read-back + correct _rolled state fail-fast); ops={1}".format(
              checks, H.total_ops()))

    # NON-VACUITY: the load-bearing rollover hazard was actually exercised.
    H.check(checks > 0,
            "no rollover conservation checks completed -- the SpooledTemporaryFile "
            "in-memory -> on-disk rollover transition was never exercised (oracle "
            "would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded mid-rollover
    # inside the buffered-prefix copy / fd seek / self._file rebind).
    H.require_no_lost("spooled rollover conservation")


if __name__ == "__main__":
    harness.main(
        "p553_tempfile_spooled_rollover", body, setup=setup, post=post,
        default_funcs=2000, max_funcs=2000,
        describe="each fiber writes a KNOWN wid-derived byte stream that crosses "
                 "max_size into its OWN tempfile.SpooledTemporaryFile, with "
                 "cooperative yields bracketing the in-memory-BytesIO -> on-disk-fd "
                 "rollover transition.  LOAD-BEARING single-owner conservation: the "
                 "read-back MUST equal the written stream byte-for-byte and _rolled "
                 "MUST reflect the final size (> max_size).  A lost/doubled buffered "
                 "prefix, a corrupted fd position, or a torn _file/_rolled field "
                 "across a hub migration fails")
