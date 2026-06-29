"""big_100 / 414 -- BufferedWriter flush/close under cancellation, per-worker
framed records over a pipe, byte-exact conservation across the drain.

Each worker owns ONE os.pipe() and wraps the write end in a buffered writer via
the patched open(fd, "wb") -- which, for a POLLABLE fd, routes through pure-Python
_pyio (NOT the immutable C BufferedWriter), whose FileIO.write issues the
cooperative os.write that PARKS on wait_fd when the pipe's kernel buffer fills.
So a flush() of a large _write_buf drains it in slices: each os.write moves a
partial count, the fiber parks on WRITE-readiness, and only AFTER the park returns
does `del self._write_buf[:n]` retire the slice that reached the pipe.  A reader
fiber on (generally) a different M:N hub drains the read end and validates the
byte stream.

WHY THIS STRESSES FT.  _pyio.BufferedWriter._flush_unlocked is the hazard:

    while self._write_buf:
        n = self.raw.write(self._write_buf)   # <- cooperative os.write; PARKS
        del self._write_buf[:n]               # <- slice retired AFTER the park

The whole drain is a cooperative loop with a park between reading the buffer and
mutating it.  Under M:N (GIL off, parallel hubs) a preempt or a deadline-cancel
landing inside that park -- in the partial-write slice path -- can (a) drop the
un-drained tail (committed bytes never reach the pipe), (b) double-emit a slice
(`del [:n]` lost / re-run -> a byte appears twice), or (c) tear a frame so the
reader desyncs.  And a writer that is dropped WITHOUT close() must have its
finalizer flush-or-discard the buffer with no UAF on the bytearray and no second
flush re-emitting an already-drained slice.

THE FALSIFIABLE INVARIANT (closed-world frame oracle).  The worker writes a
strictly increasing sequence of self-describing frames:

    MAGIC(4) | wid(4) | seq(4) | length(4) | payload(length) | crc32(4)

payload is the deterministic function PAYLOAD(wid, seq) and crc is over it.  The
reader walks the byte stream frame by frame and asserts, for EVERY frame:
  * MAGIC intact (a torn/double-drained tail corrupts it first);
  * wid == this pipe's owner (no cross-pipe bleed);
  * seq == previous seq + 1 (STRICT ordering; a dropped or duplicated frame
    breaks this);
  * the length-bytes payload == PAYLOAD(wid, seq) and crc matches (a torn slice
    yields wrong bytes / crc).
The reader records the highest seq it accepted as a clean, contiguous, in-order
frame: `clean_seq`.

The worker independently records `committed_seq`: the highest seq whose enclosing
flush() RETURNED SUCCESSFULLY -- i.e. the writer was told those bytes are out the
door.  Conservation, checked per worker in post():

    clean_seq  >=  committed_seq          (no committed frame lost or torn)

A committed frame that never arrives, arrives corrupt, arrives twice, or arrives
out of order makes clean_seq fall short of committed_seq (or fails a frame check
outright) -- the bug signal.  After a cancel, the writer stops; whatever it had
already flushed is committed and MUST still arrive intact, and the cancel must
NOT cause a re-flush of an already-drained slice (which the reader would see as a
duplicate/torn frame).

FOUR CLOSE DISCIPLINES, round-robined by worker id over the first ops (the suite
learned in p125/p126/p172 that pure-random case selection MISSES a case at low
op-count under load and flakes the post() coverage check; deterministic
round-robin keyed off (wid+i) fixes that):
  0 CLEAN          -- write all frames, flush, explicit close().  Every frame is
                      committed; clean_seq must reach the last seq.
  1 CANCEL_MID     -- a sibling cancels the context mid-stream; the worker stops
                      at the next inter-frame check, flushes what it can, closes.
                      committed = whatever the last successful flush covered.
  2 DROP_NO_CLOSE  -- never call close(); drop the writer reference so the _pyio
                      finalizer flushes-or-discards.  Tests the del-at-teardown
                      path (flush on __del__ must not UAF the bytearray nor
                      double-emit).  committed = last EXPLICITLY-flushed seq (the
                      finalizer flush is best-effort and not counted as committed).
  3 CANCEL_INFLUSH -- queue a big un-flushed backlog (> pipe buffer) so flush()
                      PARKS deep in the slice loop, and time the cancel to land
                      during that park.  The partial-write slice path is the
                      exact spot the un-drained tail can be dropped / doubled.

Stresses: _pyio.BufferedWriter _write_buf drain across a park, cooperative
os.write partial-write slice path, flush/close under cooperative cancellation,
del-at-teardown flush-or-discard, byte-exact conservation + strict frame ordering
under M:N.
"""
import os
import struct
import zlib

import harness
import runloom
import cancelutil

# Frame: MAGIC, wid, seq, payload-length (big-endian), then payload, then crc32.
MAGIC = 0x42574643            # "BWFC"
HDR = struct.Struct(">IIII")  # magic, wid, seq, length
CRC = struct.Struct(">I")
HDR_SIZE = HDR.size           # 16
CRC_SIZE = CRC.size           # 4

# Per-pipe buffered-writer buffer.  Kept below a typical 64 KiB pipe kernel
# buffer so a single flush of a few frames can fit, but a backlog of several
# frames forces the os.write partial-write / park path (the hazard).
BUF_SIZE = 16 * 1024

# Records per worker round and the payload size range.  RECORDS small enough that
# a round completes within the timeout-bounded window at scale, large enough that
# CLEAN runs commit a multi-frame, multi-flush stream; payloads span the pipe-
# buffer boundary so flushes park.
RECORDS = 24
PAYLOAD_MIN = 200
PAYLOAD_MAX = 6000

# Close disciplines (round-robined by worker id; see module docstring).
CLEAN = 0
CANCEL_MID = 1
DROP_NO_CLOSE = 2
CANCEL_INFLUSH = 3
NCASES = 4


def payload_for(wid, seq, n):
    """Deterministic, wid/seq-specific payload bytes.  A torn or double-drained
    slice yields bytes that don't match this (and fail the crc), so the reader
    catches corruption even if framing happens to re-align."""
    base = (wid * 2654435761 + seq * 40503) & 0xFFFFFFFF
    return bytes(((base >> (8 * (i & 3))) ^ (i * 167 + seq)) & 0xFF
                 for i in range(n))


def frame_bytes(wid, seq, payload):
    """Serialize one self-describing frame."""
    crc = zlib.crc32(payload) & 0xFFFFFFFF
    return (HDR.pack(MAGIC, wid, seq, len(payload)) + payload
            + CRC.pack(crc))


def read_exact(fd, n):
    """Cooperative read of exactly n bytes from the pipe read end; returns the
    bytes, or b"" on clean EOF (writer closed with nothing more buffered).  A
    short read followed by EOF mid-frame returns what it got (the caller treats a
    partial trailing frame as the benign cancel/drop truncation, not corruption --
    only bytes the writer COMMITTED are asserted to be whole)."""
    chunks = []
    got = 0
    while got < n:
        try:
            b = os.read(fd, n - got)
        except OSError:
            break
        if not b:
            break
        chunks.append(b)
        got += len(b)
    return b"".join(chunks)


class FrameError(Exception):
    """A frame violated the oracle -- corruption (torn/dropped/dup/reorder)."""


def reader_body(H, wid, rfd, result):
    """Drain the pipe read end and validate the frame stream.  Sets
    result["clean_seq"] to the highest seq accepted as a clean, contiguous,
    in-order frame (-1 if none); on a hard frame violation calls H.fail and sets
    result["corrupt"].  A truncated TRAILING frame (cancel/drop cut the stream
    mid-frame) is the benign end-of-stream, not corruption."""
    expected_seq = None
    clean_seq = -1
    try:
        while True:
            hdr = read_exact(rfd, HDR_SIZE)
            if len(hdr) == 0:
                break                       # clean EOF at a frame boundary
            if len(hdr) < HDR_SIZE:
                break                       # truncated trailing header: benign tail
            magic, fwid, seq, length = HDR.unpack(hdr)
            if magic != MAGIC:
                raise FrameError(
                    "bad MAGIC 0x{0:08x} != 0x{1:08x} after seq {2} -- torn / "
                    "double-drained _write_buf tail desynced the stream".format(
                        magic, MAGIC, clean_seq))
            if fwid != wid:
                raise FrameError(
                    "frame wid {0} != pipe owner {1} (seq {2}) -- cross-writer "
                    "buffer bleed".format(fwid, wid, seq))
            if not (0 <= length <= PAYLOAD_MAX):
                raise FrameError(
                    "insane length {0} (seq {1}) -- corrupted header from a torn "
                    "slice".format(length, seq))
            body = read_exact(rfd, length)
            if len(body) < length:
                break                       # truncated trailing payload: benign tail
            crcb = read_exact(rfd, CRC_SIZE)
            if len(crcb) < CRC_SIZE:
                break                       # truncated trailing crc: benign tail
            (crc,) = CRC.unpack(crcb)
            # Past this point the frame is STRUCTURALLY complete -- the writer
            # committed every byte of it -- so any mismatch is real corruption.
            if expected_seq is None:
                expected_seq = seq          # first frame anchors the sequence
            if seq != expected_seq:
                raise FrameError(
                    "out-of-order/dropped/dup frame: got seq {0} expected {1} -- "
                    "a flush dropped the tail or re-emitted a slice".format(
                        seq, expected_seq))
            want = payload_for(wid, seq, length)
            if body != want:
                raise FrameError(
                    "torn payload at seq {0}: {1} bytes mismatch -- slice came "
                    "from the wrong buffer offset".format(
                        seq, sum(1 for a, b in zip(body, want) if a != b)))
            if (zlib.crc32(body) & 0xFFFFFFFF) != crc:
                raise FrameError(
                    "crc mismatch at seq {0} -- payload corrupted in the "
                    "_write_buf drain".format(seq))
            clean_seq = seq
            expected_seq = seq + 1
    except FrameError as exc:
        result["corrupt"] = True
        H.fail("p414 wid={0}: {1}".format(wid, exc))
    except OSError:
        pass                                # pipe torn down at shutdown: benign
    finally:
        result["clean_seq"] = clean_seq
        try:
            os.close(rfd)
        except OSError:
            pass


def run_writer(H, wid, rng, case, wfd, result):
    """Write RECORDS frames into a BufferedWriter over the pipe write end,
    flushing at random points, under the given close discipline.  Records the
    highest seq COMMITTED (covered by a flush() that returned) into
    result["committed_seq"]."""
    committed_seq = -1
    # Buffered writer over the pollable pipe fd -> _pyio.BufferedWriter (the
    # cooperative os.write path).  closefd=True so close()/__del__ closes wfd.
    bw = open(wfd, "wb", buffering=BUF_SIZE)

    # CANCEL cases: a context whose cancel a sibling fiber trips after a jittered
    # delay, landing mid-stream (CANCEL_MID) or during a deep parked flush
    # (CANCEL_INFLUSH).
    ctx = cancel = None
    if case in (CANCEL_MID, CANCEL_INFLUSH):
        ctx, cancel = cancelutil.WithCancel(cancelutil.Background())
        if case == CANCEL_MID:
            delay = rng.uniform(0.0, 0.003)
        else:
            # Land the cancel a touch later, so a big backlog flush is already
            # parked deep in the slice loop when it fires.
            delay = rng.uniform(0.001, 0.006)
        runloom.fiber(cancelutil.delayed_cancel, cancel, delay)

    last_flushed = -1            # highest seq fully resident in _write_buf+pipe
    pending_unflushed = False
    dropped = False
    try:
        for seq in range(RECORDS):
            # Cooperative cancel check between frames (cancel can't preempt a
            # parked os.write, but it stops us writing MORE once observed).
            if ctx is not None and ctx.err() is not None:
                break
            if case == CANCEL_INFLUSH:
                # Bias toward big payloads so the accumulated backlog blows past
                # the pipe kernel buffer and the eventual flush parks deep.
                n = rng.randint(PAYLOAD_MAX // 2, PAYLOAD_MAX)
            else:
                n = rng.randint(PAYLOAD_MIN, PAYLOAD_MAX)
            payload = payload_for(wid, seq, n)
            bw.write(frame_bytes(wid, seq, payload))
            last_flushed = seq          # in the buffer now; not yet committed
            pending_unflushed = True

            # Flush at random points (CANCEL_INFLUSH defers flushing to build a
            # backlog, then flushes once near the end so the drain is one big
            # parked slice loop).
            do_flush = (rng.random() < 0.4) if case != CANCEL_INFLUSH \
                else (seq == RECORDS - 1)
            if do_flush:
                bw.flush()              # cooperative drain; PARKS on a full pipe
                committed_seq = last_flushed   # flush returned -> bytes are out
                pending_unflushed = False
                runloom.yield_now()     # interleave with the reader + siblings

        # End-of-stream handling per discipline.
        if case == DROP_NO_CLOSE:
            # Drop the writer WITHOUT close(): the _pyio finalizer must flush-or-
            # discard on __del__.  committed_seq stays at the last EXPLICIT flush
            # (the finalizer flush is best-effort, not counted as committed); the
            # reader still asserts whatever DOES arrive is a clean prefix.
            dropped = True
            del bw                       # drop ref -> __del__ flush-or-discard
            return committed_seq
        if case in (CANCEL_MID, CANCEL_INFLUSH):
            # Cancelled: do a best-effort final flush of what's still buffered,
            # then close.  A cancelled flush must leave the buffer consistent --
            # it must not re-emit an already-drained slice nor tear a frame.
            try:
                bw.flush()
                if pending_unflushed:
                    committed_seq = last_flushed
            except OSError:
                pass                     # pipe gone at teardown: benign
            bw.close()
            return committed_seq
        # CLEAN: flush + explicit close; every frame is committed.
        bw.flush()
        committed_seq = last_flushed
        bw.close()
        return committed_seq
    finally:
        if not dropped:
            try:
                bw.close()               # idempotent; a second flush must be a
            except Exception:            # no-op (buffer already drained)
                pass
        result["committed_seq"] = committed_seq
        if cancel is not None:
            cancel()                     # release the context


def worker(H, wid, rng, state):
    slot = wid & 1023
    counts = state["counts"]
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the four close disciplines over each worker's first ops so
        # all four are exercised whether one worker does NCASES ops or NCASES
        # workers do one each (pure-random selection flakes the post() coverage
        # check at low op-count under load -- p125/p126/p172 lesson).  Random
        # after the seed window to keep the concurrent mix.
        if i < NCASES:
            case = (wid + i) % NCASES
        else:
            case = rng.randrange(NCASES)
        i += 1

        try:
            rfd, wfd = os.pipe()
        except OSError:
            # fd exhaustion at over-scale: benign box limit, skip this round.
            H.note_scale_limit("os.pipe: too many open files")
            return

        # Per-round shared scratch the reader + writer write disjoint keys into
        # (single-writer-per-key -> race-free without a lock).
        result = {"clean_seq": -1, "committed_seq": -1, "corrupt": False}

        wg = runloom.WaitGroup()
        wg.add(2)

        def do_reader(rfd=rfd, result=result):
            try:
                reader_body(H, wid, rfd, result)
            finally:
                wg.done()

        def do_writer(case=case, wfd=wfd, result=result,
                      wseed=rng.getrandbits(48)):
            import random
            wrng = random.Random(wseed)
            try:
                run_writer(H, wid, wrng, case, wfd, result)
            except Exception as exc:        # noqa: BLE001
                # A writer exception while the run is live is a fault; an OSError
                # once the run is over (pipe torn down at shutdown) is benign and
                # swallowed by the wrapper-style guard here.
                if H.running():
                    H.fail("p414 wid={0} case={1}: writer raised {2}: {3}".format(
                        wid, case, type(exc).__name__, exc))
            finally:
                wg.done()

        # Spawn reader and writer on (generally) different hubs and join both so
        # the round is one accountable op.
        H.fiber(do_reader)
        H.fiber(do_writer)
        wg.wait()

        if result["corrupt"]:
            return                          # H.fail already fired

        # CONSERVATION: every COMMITTED frame must have arrived clean and in
        # order -- the reader's contiguous clean_seq must reach committed_seq.
        cs = result["clean_seq"]
        cm = result["committed_seq"]
        if cm >= 0:
            if not H.check(cs >= cm,
                           "p414 wid={0} case={1}: committed seq {2} but reader "
                           "only got clean contiguous seq {3} -- a flushed "
                           "(committed) frame was LOST or torn in the _write_buf "
                           "drain".format(wid, case, cm, cs)):
                return
            counts["committed"][slot] += 1

        # Coverage tally (which disciplines actually ran).
        counts["case"][case][slot] += 1
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {
        "counts": {
            "committed": [0] * 1024,
            # one [0]*1024 shard array per case
            "case": [[0] * 1024 for _ in range(NCASES)],
        }
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    counts = H.state["counts"]
    committed = sum(counts["committed"])
    per_case = [sum(counts["case"][c]) for c in range(NCASES)]
    H.log("committed-rounds={0} per-case clean={1} cancel_mid={2} "
          "drop_no_close={3} cancel_inflush={4} ops={5}".format(
              committed, per_case[CLEAN], per_case[CANCEL_MID],
              per_case[DROP_NO_CLOSE], per_case[CANCEL_INFLUSH],
              H.total_ops()))
    H.check(H.total_ops() > 0, "no rounds completed")
    # At least one round must have COMMITTED frames whose conservation we
    # verified -- otherwise the flush oracle never actually ran.
    H.check(committed > 0,
            "no round ever committed+verified a flushed frame -- the flush "
            "conservation oracle never exercised")
    # Each of the four close disciplines must have been exercised (the
    # round-robin guarantees this whether 1 worker did NCASES ops or NCASES
    # workers did 1 each).
    H.check(per_case[CLEAN] > 0, "CLEAN discipline never exercised")
    H.check(per_case[CANCEL_MID] > 0, "CANCEL_MID discipline never exercised")
    H.check(per_case[DROP_NO_CLOSE] > 0,
            "DROP_NO_CLOSE (del-at-teardown flush) discipline never exercised")
    H.check(per_case[CANCEL_INFLUSH] > 0,
            "CANCEL_INFLUSH (cancel during parked flush) discipline never "
            "exercised")
    H.require_no_lost()


if __name__ == "__main__":
    harness.main("p414_bufferedwriter_flush_cancel", body, setup=setup,
                 post=post, default_funcs=3000,
                 describe="per-worker BufferedWriter writes framed records over a "
                          "pipe, randomly flush/close/cancel/drop; every committed "
                          "frame arrives exactly once, in order, byte-exact -- a "
                          "torn/dropped/dup slice in the _write_buf drain is the bug")
