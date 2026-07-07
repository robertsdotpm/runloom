"""big_100 / 551 -- io.BufferedRandom seek/read/write buffer-coherence conservation
under M:N (single-owner file, bytearray model oracle).

io.BufferedRandom is the read+write buffered wrapper open() hands back for the
"r+b"/"w+b"/"rb+" modes.  It keeps TWO internal cursors over ONE backing FileIO:
a read-ahead buffer (bytes prefetched past what the caller consumed) and a write
buffer (bytes not yet flushed to the fd).  The load-bearing invariant of the
object is BUFFER COHERENCE across position changes:

    * a seek() to a position OUTSIDE the current read-ahead window MUST discard
      that window (else a later read returns prefetched bytes for the WRONG
      offset);
    * a write() MUST flush/invalidate any read-ahead buffer that overlaps the
      written region (else a read of the just-written bytes returns the STALE
      pre-write contents that were sitting in the read-ahead buffer);
    * the file's true contents == every write applied in order.

That coherence is maintained by mutating the (buffer, position, raw-fd-offset)
triple together on each seek/write.  Under the pygo M:N runtime a fiber can PARK
(cooperative yield) and MIGRATE to a different hub in the middle of a
read/seek/write sequence -- carrying that internal triple across the hub
boundary.  If a park+migration lands between "fill the read-ahead buffer" and
"read back a byte the fiber just overwrote", a runtime that torn/reordered/lost
the buffer-invalidation state would hand back a byte from BEFORE the intervening
write.  That is the hazard this program makes exactly falsifiable.

WHICH ORACLE IS LOAD-BEARING, AND WHY (single-owner + closed-world model):

  Each fiber owns its OWN temp file, its OWN io.BufferedRandom over it, and its
  OWN bytearray `model` that mirrors every write.  NOTHING is shared between
  fibers -- no shared file, no shared object, no shared buffer.  So this is NOT a
  test of BufferedRandom's (documented-absent) thread-safety; it is a test that a
  SINGLE-OWNER buffered stream stays self-coherent even when its owning fiber
  parks and migrates hubs mid-sequence.

  The model is the source of truth: after any write the fiber updates `model`, and
  every read is compared byte-for-byte against `model[offset:offset+n]`.  Because
  the file is per-fiber, the bytes read back MUST equal the model at that offset,
  ALWAYS -- a buffer-coherence conservation law.  On a correct runtime the program
  exits 0 (every read matches; the whole-file capstone equals the model).

ORACLES:
  * LOAD-BEARING -- BUFFER COHERENCE (worker, HARD, fail-fast).  Per op the fiber
    does one of: APPEND (write new bytes at EOF), OVERWRITE (rewrite a random
    interior span), SEEK+READ verify (seek to a random offset, YIELD, read, assert
    == model), or the HAZARD sequence: fill the read-ahead buffer with a short
    read at offset A, YIELD, overwrite a span at offset B (possibly inside the
    just-prefetched window), YIELD (park+migrate carrying the invalidation state),
    seek back to B and read -- the bytes read back MUST be the NEW bytes, never the
    stale pre-write buffer.  A mismatch (stale/torn/reordered byte) is a hard fail.

  * CONSERVATION CAPSTONE (worker, HARD, fail-fast).  When the run window ends,
    the fiber flush()es, seek(0)s, reads the ENTIRE file and asserts it equals its
    `model` byte-for-byte and that the file length equals len(model) -- every
    write landed exactly once, in order, with no stale/dropped/duplicated byte.

  * NON-VACUITY (post, HARD): sum of per-wid verified-byte counts > 0, so the
    coherence oracle actually ran.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that parked mid-read
    (stranded inside the buffered read/seek) and vanished never returns; the
    watchdog + require_no_lost catch it.

RACE-FREE COUNTERS.  verified_bytes and ops are [0]*H.funcs indexed by wid (one
writer per slot), allocated in setup() where H.funcs is known -- never wid&MASK
aliasing (that would lose increments GIL-off).

FAIL ON: a read that disagrees with the single-owner model (stale read-ahead
buffer surviving an intervening write across a park+migration, a torn
buffer/position pair, a byte reordered/dropped/duplicated), or the whole-file
capstone mismatching the model.  There is no shared-object/report-only arm here:
every object is single-owner, so any read/model disagreement is a real runtime
coherence bug, never documented Python semantics.

Resource-bounded: one temp file + one BufferedRandom per fiber, register_close'd;
max_funcs caps the forever loop's --funcs 1000000 so the fd/tmpfile count stays
sane.  The file is kept small (<= MAX_FILE) so a single read fills the read-ahead
window over most of the file, maximizing the chance a stale-buffer bug is exposed.

Stresses: io.BufferedRandom read-ahead invalidation on seek()/write(), write-
buffer flush ordering, the (buffer, position, raw-offset) triple crossing a hub
migration mid read/seek/write, exact per-file content conservation.
"""
import io
import os

import harness
import runloom

# Keep each fiber's file small so ONE read() fills the read-ahead window over
# most of the file -- that is the state a stale-buffer bug reuses after an
# intervening write.  Small also keeps 2000 concurrent files/models cheap.
MAX_FILE = 2048
MAX_CHUNK = 32                     # bytes per write/read op (small; many ops/file)

# Round-robin the op cases by op index (deterministic first-touch coverage, then
# rng) so post() coverage holds whether one fiber does K ops or K fibers do 1.
CASE_APPEND = 0                    # write new bytes at EOF (grow, up to MAX_FILE)
CASE_OVERWRITE = 1                 # rewrite a random interior span
CASE_SEEK_READ = 2                 # seek to a random offset, YIELD, read, verify
CASE_HAZARD = 3                    # fill read-ahead, YIELD, overwrite, YIELD, read-back
NCASES = 4


def apply_write(f, model, off, data):
    """Write `data` at absolute `off` through the BufferedRandom and mirror the
    exact same mutation into the single-owner `model` bytearray.  Handles both
    interior overwrite and growth past EOF (with any implicit zero-fill gap, which
    cannot happen here because off is always <= len(model))."""
    f.seek(off)
    f.write(data)
    end = off + len(data)
    if end > len(model):
        # Grow the model to match; off <= len(model) is guaranteed by callers, so
        # there is never an unwritten (zero-filled) gap to reason about.
        model.extend(b"\x00" * (end - len(model)))
    model[off:end] = data


def verify_read(H, wid, f, model, off, n, state):
    """Seek to `off`, YIELD (park+migrate), read up to n bytes, and assert they
    equal the single-owner model at that offset.  Returns the number of bytes read
    (0 at/after EOF).  A mismatch is a hard buffer-coherence fault."""
    f.seek(off)
    runloom.yield_now()                       # park+migrate holding (buf,pos,raw-off)
    got = f.read(n)
    exp = bytes(model[off:off + len(got)])
    if got != exp:
        H.fail("BufferedRandom read INCOHERENT: wid {0} read {1!r} at off {2} "
               "(len {3}) but single-owner model says {4!r} -- a stale read-ahead "
               "buffer, torn (buffer,position) pair, or reordered byte surviving a "
               "park+migration".format(wid, got, off, n, exp))
        return -1
    state["verified"][wid] += len(got)
    return len(got)


def op_hazard(H, wid, rng, f, model, state):
    """The load-bearing hazard sequence: fill the read-ahead buffer, then perform
    an INTERVENING write that overlaps the prefetched window, across parks, and
    read the written span back -- it MUST be the new bytes, never the stale
    pre-write buffer.  Single-owner, so a mismatch is a real runtime coherence
    bug."""
    if not model:
        return 0
    # 1) Fill the read-ahead buffer with a short read at offset A (the buffered
    #    reader prefetches well past what we consume, covering much of the file).
    a = rng.randrange(len(model))
    if verify_read(H, wid, f, model, a, rng.randint(1, MAX_CHUNK), state) < 0:
        return -1
    # 2) INTERVENING write at offset B, likely inside the just-prefetched window.
    b = rng.randrange(len(model))
    span = min(len(model) - b, rng.randint(1, MAX_CHUNK))
    newdata = bytes(rng.randrange(256) for _ in range(span))
    # A write MUST invalidate any read-ahead buffer overlapping [b, b+span).
    apply_write(f, model, b, newdata)
    runloom.yield_now()                       # park+migrate carrying invalidation state
    # 3) Read the written span back -- must be the NEW bytes.  If a stale read-ahead
    #    buffer survived the write+park, this returns the pre-write contents.
    f.seek(b)
    runloom.yield_now()
    got = f.read(span)
    if got != newdata:
        H.fail("BufferedRandom STALE-BUFFER read: wid {0} wrote {1!r} at off {2} "
               "then read back {3!r} -- the read-ahead buffer was NOT invalidated "
               "by the intervening write across the park+migration (stale pre-write "
               "bytes returned)".format(wid, newdata, b, got))
        return -1
    state["verified"][wid] += span
    return 1


def worker(H, wid, rng, state):
    """Single-owner BufferedRandom + bytearray model.  Each fiber drives its own
    file through the four op cases (round-robined for coverage), then a whole-file
    conservation capstone when the window ends."""
    path = os.path.join(state["dir"], "brw_{0}.bin".format(wid))
    f = open(path, "w+b")
    H.register_close(f)
    model = bytearray()
    opi = 0

    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin case selection for the first ops (deterministic coverage),
        # then rng-mixed thereafter.
        if opi < NCASES:
            case = opi
        else:
            case = rng.randrange(NCASES)

        if case == CASE_APPEND and len(model) < MAX_FILE:
            span = min(MAX_FILE - len(model), rng.randint(1, MAX_CHUNK))
            newdata = bytes(rng.randrange(256) for _ in range(span))
            apply_write(f, model, len(model), newdata)
            runloom.yield_now()
            # Verify the just-appended tail reads back correctly.
            if verify_read(H, wid, f, model, len(model) - span, span, state) < 0:
                return
        elif case == CASE_OVERWRITE and model:
            off = rng.randrange(len(model))
            span = min(len(model) - off, rng.randint(1, MAX_CHUNK))
            newdata = bytes(rng.randrange(256) for _ in range(span))
            apply_write(f, model, off, newdata)
            runloom.yield_now()
            if verify_read(H, wid, f, model, off, span, state) < 0:
                return
        elif case == CASE_SEEK_READ and model:
            off = rng.randrange(len(model))
            if verify_read(H, wid, f, model, off, rng.randint(1, MAX_CHUNK), state) < 0:
                return
        elif case == CASE_HAZARD:
            if op_hazard(H, wid, rng, f, model, state) < 0:
                return
        else:
            # Empty model on a read/overwrite case: seed a byte so subsequent
            # rounds have content (still deterministic, single-owner).
            newdata = bytes(rng.randrange(256) for _ in range(rng.randint(1, MAX_CHUNK)))
            apply_write(f, model, len(model), newdata)

        # Occasionally flush so the write buffer/read buffer interplay is exercised
        # (flush must not corrupt the read-ahead window either).
        if opi & 7 == 0:
            f.flush()

        H.op(wid)
        opi += 1
        if opi & 15 == 0:
            H.task_done(wid)

    # ---- CONSERVATION CAPSTONE: whole file == model, byte-for-byte -----------
    f.flush()
    f.seek(0)
    whole = f.read()
    if len(whole) != len(model):
        H.fail("conservation broken: wid {0} file length {1} != model length {2} "
               "-- a write was dropped, duplicated, or the file was truncated across "
               "the run".format(wid, len(whole), len(model)))
        return
    if whole != bytes(model):
        # Locate the first divergence for the message.
        first = next((i for i in range(len(whole)) if whole[i] != model[i]), -1)
        H.fail("conservation broken: wid {0} whole-file contents diverge from the "
               "single-owner model at byte {1} (file={2!r} model={3!r}) -- a stale/"
               "torn/reordered byte accumulated across the run".format(
                   wid, first,
                   bytes(whole[first:first + 8]), bytes(model[first:first + 8])))
        return
    state["verified"][wid] += len(whole)
    H.task_done(wid)


def setup(H):
    d = H.make_tmpdir(prefix="big100_p551_")
    H.state = {
        "dir": d,
        "verified": [0] * H.funcs,        # per-wid verified bytes (single writer/slot)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    verified = sum(H.state["verified"])
    H.log("BufferedRandom buffer-coherence: {0} bytes verified against the single-"
          "owner model (every read matched + whole-file capstone equalled the model, "
          "fail-fast); ops={1}".format(verified, H.total_ops()))
    # NON-VACUITY: the load-bearing coherence oracle actually ran.
    H.check(verified > 0,
            "no bytes were verified against the model -- the BufferedRandom buffer-"
            "coherence hazard was never exercised (oracle would be vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished mid buffered read/seek.
    H.require_no_lost("bufferedrandom seek/rw coherence")


if __name__ == "__main__":
    harness.main(
        "p551_bufferedrandom_seek_rw_coherence", body, setup=setup, post=post,
        default_funcs=2000, max_funcs=2000,
        describe="each fiber drives a SINGLE-OWNER io.BufferedRandom over its own "
                 "temp file, mirroring every write into a bytearray model.  The "
                 "read-ahead buffer must be invalidated on seek()/write(); a stale "
                 "buffer surviving a park+migration would return bytes from before "
                 "an intervening write.  LOAD-BEARING: every read (incl. a fill-read-"
                 "ahead / intervening-write / read-back hazard sequence across parks) "
                 "must equal model[off:off+n]; a whole-file capstone must equal the "
                 "model byte-for-byte -- any stale/torn/reordered byte fails")
