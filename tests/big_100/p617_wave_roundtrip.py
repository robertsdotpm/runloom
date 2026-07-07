"""big_100 / 617 -- wave in-memory write/read round-trip isolation under M:N.

wave.Wave_write is a mutable per-instance object.  A wave file opened for WRITING
threads a large amount of instance state through every setparams()/writeframes()
call and finally PATCHES the RIFF header in place on close():

  * self._nframeswritten / self._datawritten -- the running frame + byte counts,
    bumped by each writeframes() as data is appended to the backing stream;
  * self._nchannels / self._sampwidth / self._framerate -- the format parameters
    that are packed into the fmt- chunk and used to compute the frame size;
  * self._file (a chunk-writer over the backing BytesIO) -- whose write cursor
    MUST stay in lock-step with the byte counts;
  * close() then SEEKS back to the RIFF/data length fields and rewrites them with
    the final totals (an in-place header patch over the same BytesIO cursor).

wave.Wave_read re-parses that byte string: it reads the RIFF/fmt-/data chunks,
recovers nchannels/sampwidth/framerate/nframes from the header, and readframes(n)
streams n frames of raw PCM back out of the data chunk.

Under M:N many fibers run on a handful of hub OS-threads with the GIL OFF.  A
fiber that is PARKED (yield/sleep) in the middle of writing frames -- between two
writeframes() calls, or between the last writeframes() and the close() header
patch -- lets a sibling fiber on the same hub run.  The hazard this program
probes: if runloom did NOT properly isolate each fiber's Wave_write instance (a
torn _datawritten/_nframeswritten, a frame block written to the wrong stream
cursor, a format parameter clobbered, or the close() header seek/patch landing on
a sibling's buffer), the wave a fiber produces would read back with WRONG format
parameters, a wrong frame count, or WRONG frame bytes -- a sibling's PCM bleeding
into this fiber's clip.

Because every fiber owns its OWN BytesIO + its OWN wave.open(fileobj, "wb") + its
OWN wave.open(fileobj, "rb"), this is a SINGLE-OWNER round-trip.  Nothing is
shared between fibers.  On a correct runtime the clip a fiber builds is exactly
the clip it reads back, every time -- so the program EXITS 0 (PASS) when there is
no bug.

WHICH ORACLE IS LOAD-BEARING, AND WHY (a closed-world round-trip, single-owner):

  Each fiber, per iteration, generates a KNOWN clip whose FORMAT parameters AND
  PCM bytes are tagged with the fiber's wid (plus a per-iteration idx), so a byte
  -- or a framerate -- that leaked in from a SIBLING fiber's wave is immediately
  recognizable (its embedded "W<wid>" tag or its unique per-fiber framerate would
  be wrong).  The fiber:

    1. Opens a fresh in-memory wave for WRITING (wave.open(BytesIO, "wb")) and
       sets nchannels/sampwidth (wid-derived) + a UNIQUE per-fiber framerate.
    2. Writes the KNOWN PCM payload in several writeframes() chunks (each a whole
       number of frames), YIELDING between chunks (and once mid-stream) so a
       sibling reliably interleaves while this fiber is parked with a half-written
       data chunk (torn _datawritten window).
    3. Closes the write wave (which SEEKS back and patches the RIFF/data lengths),
       snapshots buf.getvalue().
    4. Re-opens that byte string for READING (wave.open(BytesIO, "rb")) and
       asserts the CLOSED-WORLD round-trip law:
         (a) nchannels / sampwidth == what this fiber set;
         (b) framerate == this fiber's UNIQUE per-fiber framerate (a sibling's
             framerate here is a cross-fiber header leak);
         (c) nframes == the exact number of frames written;
         (d) readframes(nframes) == the EXACT known PCM bytes (no truncation, no
             torn block, no sibling bytes);
         (e) a second readframes() past the end returns b"" (stream exhausted at
             exactly the written length -- no trailing sibling data).

  We verified the analogous single-owner round-trip with a standalone plain-
  threads control (16 OS threads, each building + reading its own in-memory wave
  with wid-tagged PCM + a unique framerate, GIL ON and OFF): 100% of round-trips
  reproduce the exact bytes and parameters -- 0 mismatches.  Each thread's
  Wave_write/Wave_read instance is independent and properly isolated.  Under a
  CORRECT runloom each fiber's round-trip MUST also be byte-exact.  If a fiber's
  read-back bytes differ from what it wrote, the frame count is wrong, or a
  sibling's framerate/bytes appear, that is a runloom M:N fiber-isolation bug (a
  torn _datawritten, a mis-cursored block write, a clobbered format parameter, or
  a header patch landing on the wrong buffer), and the load-bearing single-owner
  oracle FAILS -- otherwise it PASSES (exit 0).

ORACLES:
  * LOAD-BEARING -- WAVE ROUND-TRIP INTEGRITY (worker, HARD, fail-fast).  The
    closed-world (a)-(e) checks above on a fiber's OWN in-memory wave.  Single-
    owner: the BytesIO, the write wave, the read wave, and the expected payload
    are all fiber-local, never shared.  A failure is a runloom isolation desync,
    never documented Python semantics (an unsynchronized SHARED Wave_write would
    tear exactly like a shared file across OS threads -- documented behavior -- so
    we never share one).
  * NON-VACUITY (post, HARD): the round-trip hazard actually ran
    (roundtrips > 0 -- else the oracle is vacuous).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-write
    (stranded inside writeframes / the close() header patch on a torn offset)
    never returns; the watchdog + require_no_lost catch it.

FAIL ON: a format parameter mismatch, a wrong per-fiber framerate (cross-fiber
header leak), a wrong nframes, read-back PCM != the known payload (truncation /
torn block / sibling bytes), or trailing data past the written length.

Stresses: wave.Wave_write per-instance stream state (_datawritten /
_nframeswritten / the backing BytesIO cursor), the writeframes() block append +
the close() in-place header seek/patch across a fiber yield, wave.Wave_read
header re-parse + readframes() PCM recovery, per-fiber in-memory wave isolation
under M:N with the GIL off.

Good TSan / controlled-M:N-replay target: _datawritten and the BytesIO write
cursor are a get-then-advance pair driven per writeframes() chunk, and close()
does a seek-back-and-overwrite of the length fields; a fiber parked between two
chunk writes, or between the last write and the header patch, is the cleanest
window for a mis-cursored block or a header patch on the wrong buffer -- a TSan
report on the BytesIO buffer, or a single byte off in the read-back PCM under
replay, localizes the tear before the round-trip law even closes.
"""
import io
import wave

import harness
import runloom

# Frames per fiber-owned clip band.  Small enough that build+read is cheap under
# tens of thousands of fibers, large enough that _datawritten grows across several
# writeframes() chunks and the round-trip crosses multiple block boundaries.
FRAMES_MIN = 64
FRAMES_MAX = 512

# writeframes() chunks per clip -- the PCM payload is split into this many whole-
# frame writes, with a yield between each, so a sibling reliably interleaves while
# this fiber sits parked with a half-written data chunk (torn _datawritten window).
WRITE_CHUNKS = 4

# Base for the UNIQUE per-fiber framerate.  Each fiber uses BASE_FRAMERATE + wid so
# a framerate read back that belongs to a SIBLING (a different wid) is immediately
# recognizable as a cross-fiber header leak.  Kept well inside a 4-byte unsigned.
BASE_FRAMERATE = 8000

# Sustained round-trips per worker, bounded by H.running().  The isolation hazard
# only manifests under SUSTAINED churn -- many fibers simultaneously building and
# reading clips while parked mid-write across a yield, so a sibling reliably
# interleaves before this fiber resumes.  A single round-trip per fiber barely
# overlaps a sibling's and does NOT reproduce.
INNER_CAP = 100000


def fiber_format(wid):
    """Deterministic (nchannels, sampwidth, framerate) for fiber wid.

    nchannels/sampwidth vary across fibers (so the frame size varies and the fmt-
    chunk carries different values) while framerate is UNIQUE per fiber -- a
    cross-fiber header leak shows a framerate belonging to a different wid."""
    nchannels = 1 + (wid & 1)            # {1, 2}
    sampwidth = 1 + ((wid >> 1) & 1)     # {1, 2}
    framerate = BASE_FRAMERATE + wid     # UNIQUE per fiber (cross-fiber canary)
    return nchannels, sampwidth, framerate


def build_pcm(wid, idx, nframes, framesize):
    """Deterministic, wid-tagged PCM payload of exactly nframes*framesize bytes.

    The payload begins with an ASCII tag embedding wid/idx so a byte sequence that
    leaked in from a SIBLING fiber's clip (a different wid) is immediately
    recognizable, then is filled with a per-fiber repeating byte to the exact
    whole-frame length.  Single-owner: the fiber that built it is the only one
    that reads it back."""
    size = nframes * framesize
    tag = "W{0}:I{1}:".format(wid, idx).encode("ascii")
    if len(tag) >= size:
        return tag[:size]
    fill_byte = ((wid * 7 + idx * 31 + 1) & 0xFF)
    return tag + bytes([fill_byte]) * (size - len(tag))


def round_trip(H, wid, idx, rng, state):
    """One single-owner wave build+read round-trip.

    Builds a fiber-local in-memory wave with wid-tagged format + PCM (yielding
    between writeframes chunks so a sibling interleaves on a torn-offset stream),
    then re-opens the bytes read-only and asserts the closed-world round-trip law.
    Every object here is fiber-local -- a mismatch is a runloom isolation bug."""
    nchannels, sampwidth, framerate = fiber_format(wid)
    framesize = nchannels * sampwidth
    nframes = rng.randint(FRAMES_MIN, FRAMES_MAX)
    payload = build_pcm(wid, idx, nframes, framesize)

    # ---- BUILD: fiber-local BytesIO + fiber-local write wave -------------------
    wbuf = io.BytesIO()
    ww = wave.open(wbuf, "wb")
    try:
        ww.setnchannels(nchannels)
        ww.setsampwidth(sampwidth)
        ww.setframerate(framerate)
        # Split the payload into WRITE_CHUNKS whole-frame writes, yielding between
        # each so a sibling on this hub runs while this fiber is parked with a
        # partially-written data chunk (torn _datawritten / cursor window).
        total_frames = nframes
        base = total_frames // WRITE_CHUNKS
        written = 0
        for c in range(WRITE_CHUNKS):
            fcount = base if c < WRITE_CHUNKS - 1 else (total_frames - written)
            start = written * framesize
            end = (written + fcount) * framesize
            ww.writeframes(payload[start:end])
            written += fcount
            # PARK mid-clip: a sibling on this hub runs now, while this fiber's
            # Wave_write sits at a partially-written offset.  If _datawritten / the
            # BytesIO cursor / the format params are not fiber-isolated, the
            # sibling's writes bleed into this clip.
            runloom.yield_now()
            if c == 0:
                runloom.sleep(0.0002)
    finally:
        # close() SEEKS back and patches the RIFF/data length fields in place.
        ww.close()

    audio_bytes = wbuf.getvalue()

    # ---- READ: fiber-local read wave over the snapshot -------------------------
    rbuf = io.BytesIO(audio_bytes)
    rr = wave.open(rbuf, "rb")
    try:
        # (a) format parameters must match exactly.
        if rr.getnchannels() != nchannels:
            H.fail("fiber {0} idx {1}: wave round-trip NCHANNELS wrong: read {2}, "
                   "wrote {3} -- a torn fmt- header or clobbered format parameter "
                   "under concurrent fiber writes".format(
                       wid, idx, rr.getnchannels(), nchannels))
            return
        if rr.getsampwidth() != sampwidth:
            H.fail("fiber {0} idx {1}: wave round-trip SAMPWIDTH wrong: read {2}, "
                   "wrote {3} -- a torn fmt- header or clobbered format parameter "
                   "under concurrent fiber writes".format(
                       wid, idx, rr.getsampwidth(), sampwidth))
            return

        # (b) framerate must be THIS fiber's unique value -- no sibling framerate.
        got_fr = rr.getframerate()
        if got_fr != framerate:
            H.fail("fiber {0} idx {1}: wave round-trip FRAMERATE wrong: read {2}, "
                   "wrote {3} -- a sibling fiber's framerate leaked into this "
                   "fiber's single-owner wave header (fmt- chunk isolation failure "
                   "under M:N)".format(wid, idx, got_fr, framerate))
            return

        # (c) frame count must match exactly (patched by close()).
        got_nf = rr.getnframes()
        if got_nf != nframes:
            H.fail("fiber {0} idx {1}: wave round-trip NFRAMES wrong: read {2}, "
                   "wrote {3} -- _nframeswritten / the data-length header patch was "
                   "torn across a yield (a frame block was lost, doubled, or the "
                   "close() seek/patch landed on the wrong buffer)".format(
                       wid, idx, got_nf, nframes))
            return

        # (d) read-back PCM must EXACTLY equal the known payload.
        got = rr.readframes(nframes)
        if got != payload:
            got_head = repr(got[:48]) if got else "empty"
            exp_head = repr(payload[:48])
            H.fail("fiber {0} idx {1}: wave round-trip PCM mismatch: got {2} "
                   "(len {3}), expected {4} (len {5}) -- read-back frames are "
                   "truncated, torn, or carry a sibling fiber's PCM (Wave_write "
                   "per-instance stream isolation failure under M:N)".format(
                       wid, idx, got_head, len(got), exp_head, len(payload)))
            return

        # (e) the stream is exhausted at exactly the written length -- no trailing
        # sibling data past the data chunk.
        tail = rr.readframes(nframes)
        if tail:
            H.fail("fiber {0} idx {1}: wave round-trip TRAILING data: {2} extra "
                   "bytes past the {3}-frame clip -- a sibling's frames bled past "
                   "this fiber's data chunk (torn _datawritten / data-length "
                   "header under M:N)".format(wid, idx, len(tail), nframes))
            return
    finally:
        rr.close()

    state["roundtrips"][wid] += 1


def worker(H, wid, rng, state):
    """Each fiber runs sustained single-owner wave build+read round-trips,
    fail-fast on the first closed-world round-trip violation."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            round_trip(H, wid, idx, rng, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # One race-free slot per worker (single-writer-per-slot).  H.funcs is already
    # capped to max_funcs here, so this array is bounded.
    H.state = {
        "roundtrips": [0] * H.funcs,      # single-owner round-trips per worker
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    rts = sum(H.state["roundtrips"])
    H.log("wave: {0} single-owner in-memory wave build+read round-trips (every "
          "closed-world round-trip law -- format params, framerate, frame count, "
          "exact PCM bytes, no trailing data -- passed fail-fast); ops={1}".format(
              rts, H.total_ops()))

    # NON-VACUITY: the load-bearing round-trip hazard was actually exercised.
    H.check(rts > 0,
            "no wave round-trips completed -- the load-bearing wave build/read "
            "isolation hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-write.
    H.require_no_lost("wave membuf round-trip")


if __name__ == "__main__":
    harness.main(
        "p617_wave_roundtrip", body, setup=setup, post=post,
        default_funcs=4000, max_funcs=6000,
        describe="wave.Wave_write is a mutable per-instance object threading "
                 "_datawritten / _nframeswritten / the backing stream cursor "
                 "through every writeframes(), then SEEKING back to patch the "
                 "RIFF/data length fields on close().  Under M:N a fiber parked "
                 "mid-write lets a sibling run; if the Wave_write instance is not "
                 "fiber-isolated, block writes or the header patch interleave and "
                 "the clip reads back with wrong params/bytes.  LOAD-BEARING "
                 "(single-owner): each fiber builds its OWN in-memory wave "
                 "(BytesIO) with wid-tagged PCM + a UNIQUE per-fiber framerate, "
                 "yielding between writeframes chunks, then re-opens it read-only "
                 "and asserts the closed-world round-trip law -- nchannels/"
                 "sampwidth/framerate exact, nframes exact, read-back PCM==the "
                 "known payload, no trailing data.  A mismatch (torn _datawritten, "
                 "mis-cursored block, clobbered format param, header patch on the "
                 "wrong buffer, sibling PCM) is a runloom M:N isolation bug (0 "
                 "under plain threads GIL on AND off)")
