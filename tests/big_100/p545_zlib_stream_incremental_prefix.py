"""big_100 / 545 -- zlib incremental compress/decompress streaming-prefix conservation under M:N.

zlib.compressobj() and zlib.decompressobj() each own a LIVE C z_stream struct that
is advanced IN PLACE by every .compress()/.decompress() call: the C code parks its
window, pending-output buffer, bit-accumulator and internal deflate/inflate state
in that per-object scratch and mutates it on each feed.  A compressobj feeds
INCREMENTALLY -- you push plaintext chunk by chunk and the emitted deflate bytes
depend on ALL prior chunks (the sliding window back-references them) -- so the
object's C state is a running accumulator, not a per-call pure function.

WHERE M:N COULD BREAK IT (the hazard this program probes).  runloom runs tens of
thousands of goroutines M:N across >1 hubs with the GIL OFF.  A fiber that is
parked (yield_now / sleep) BETWEEN two chunk feeds of its own compressobj -- while
a sibling on another hub drives ITS OWN compressobj/decompressobj -- must have its
z_stream scratch left untouched by that sibling.  If the C zlib code shared any
process-global scratch, or if a preemption mid-.compress() left the object's
z_stream half-updated and a sibling's call clobbered it, then this fiber's running
DECOMPRESSED prefix would stop equalling its running FED prefix, or a copy() taken
mid-stream would clone a torn state.  Each such divergence is a real runtime
corruption -- NOT documented Python behavior -- because the object is SINGLE-OWNER.

WHICH ORACLE IS LOAD-BEARING, AND WHY.

  Each fiber pairs its OWN compressobj + decompressobj (never shared) and feeds a
  wid-tagged plaintext CHUNK BY CHUNK.  After each chunk it does a Z_SYNC_FLUSH on
  the compressor (which flushes every byte fed so far to a byte boundary so the
  decompressor can emit the full prefix), decompresses the produced bytes into a
  running `got` buffer, and asserts:

      got  ==  fed          (the STREAMING-PREFIX conservation law)

  i.e. the accumulated decompressed output EXACTLY equals the accumulated fed
  plaintext prefix, after every single chunk.  Between chunks it YIELDS so a
  sibling reliably interleaves on the same hub / a parallel hub.  At end it
  flush()es the compressor (Z_FINISH) and asserts the FULL round-trip
  got == fed.  This is a closed-world conservation invariant on a SINGLE-OWNER
  object: on a correct runtime it is mathematically guaranteed to hold (zlib is
  deterministic; a compressobj+decompressobj pair round-trips its own byte
  stream), so the program exits 0 when there is no bug.  If `got != fed` at any
  chunk, this fiber's private z_stream scratch was corrupted by concurrent M:N
  execution -- a runtime bug (torn stream / cross-fiber scratch leak).

  COPY() CLONE arm (also single-owner, load-bearing).  Mid-stream the fiber takes
  co2 = co.copy() and feeds the SAME next chunk to both the original and the
  clone.  Because copy() must clone the z_stream exactly, both must emit BYTE-
  IDENTICAL deflate output for that chunk (+ Z_SYNC_FLUSH).  A divergence means
  copy() cloned a TORN stream (a sibling mutated the source mid-clone, or the
  clone shares scratch with the source) -- a runtime bug.  The main stream then
  continues on the original co, so the clone is a pure verification probe and
  never perturbs the prefix law.

  This is an M:N-SPECIFIC hazard (0 under plain OS threads): real threads own
  their own compressobj/decompressobj and z_stream structs, so an incremental
  round-trip never diverges.  A runloom fiber can be preempted WHILE parked
  between feeds; if that ever let a sibling's zlib call touch this fiber's
  z_stream, the prefix law breaks.

ORACLES:
  * LOAD-BEARING -- STREAMING-PREFIX ROUND-TRIP (worker, HARD, fail-fast).  Per
    chunk: got == fed after Z_SYNC_FLUSH + decompress; at end the full-stream
    round-trip after Z_FINISH.  Single-owner co/do/fed/got, all fiber-local.

  * LOAD-BEARING -- COPY() CLONE CONTINUATION (worker, HARD, fail-fast).  A mid-
    stream co.copy() fed the same chunk must emit byte-identical output to the
    source.  Single-owner clone.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside a C
    zlib call on a corrupted z_stream never returns; the watchdog + require_no_lost
    catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (stream_checks > 0).

FAIL ON: a fiber's own accumulated decompressed prefix diverging from its fed
prefix, a copy() clone emitting different bytes than its source, or a full-stream
round-trip mismatch.  All are single-owner corruptions -- a real runtime bug, not
documented Python semantics.

Distinct from p483/p484 (bz2/lzma one-shot round-trip) by the INCREMENTAL
streaming-prefix invariant (running got == running fed across many yields) plus
the copyobj clone-continuation check; deepens zlib, which p414/p417 only touch
incidentally.

Stresses: zlib.compressobj/decompressobj incremental C z_stream advancement across
hub-migration + yield, Z_SYNC_FLUSH byte-boundary prefix flush, copyobj clone of a
live deflate stream, running-prefix conservation, per-fiber z_stream scratch
isolation under GIL-off M:N.

Good TSan / controlled-M:N-replay target: the z_stream scratch (window, pending
buffer, deflate/inflate state) is mutated in place per call; a data-race report on
that per-object C buffer -- or a controlled replay that resumes a fiber mid-feed
after a sibling ran -- localizes the torn stream before the prefix law even closes.
"""
import zlib

import harness
import runloom

# Chunks per stream session.  Enough that the sliding window carries real cross-
# chunk back-references (so a torn mid-stream state would visibly break the prefix
# law) and there are several yield boundaries per session, small enough that many
# sessions complete under the validation timeout.
MIN_CHUNKS = 4
MAX_CHUNKS = 10

# Per-chunk plaintext size band.  Skewed/varied so the deflate window and pending
# buffer actually fill and the compressed output is non-trivial.
MIN_CHUNK_BYTES = 48
MAX_CHUNK_BYTES = 320


def make_chunk(wid, cidx, rng):
    """A wid-tagged plaintext chunk.  The wid/chunk tag makes any cross-fiber
    scratch leak visible as WRONG bytes in the decompressed prefix (not just a
    length mismatch), and the pseudo-random body fills the deflate window so the
    running stream carries real back-references across chunk boundaries."""
    tag = ("W{0}:C{1}:".format(wid, cidx)).encode("ascii")
    n = rng.randint(MIN_CHUNK_BYTES, MAX_CHUNK_BYTES)
    body = bytes((wid * 31 + cidx * 7 + i * 13) & 0xFF for i in range(n))
    return tag + body


def run_stream(H, wid, rng, state):
    """One full single-owner streaming session: feed a wid-tagged plaintext chunk
    by chunk through THIS fiber's own compressobj+decompressobj, asserting the
    running decompressed prefix equals the running fed prefix after every chunk
    (with a yield between chunks so siblings interleave), plus a mid-stream copy()
    clone-continuation check, then a final Z_FINISH full round-trip."""
    level = wid % 10                       # vary level across fibers for coverage
    co = zlib.compressobj(level)           # default method DEFLATED, wbits MAX_WBITS
    do = zlib.decompressobj()              # default wbits MAX_WBITS -- matches co

    fed = bytearray()                      # everything fed to the compressor
    got = bytearray()                      # everything recovered from the decompressor

    nchunks = rng.randint(MIN_CHUNKS, MAX_CHUNKS)
    copy_at = rng.randrange(nchunks)       # one mid-stream clone check per session

    for cidx in range(nchunks):
        if not H.running():
            return
        chunk = make_chunk(wid, cidx, rng)

        if cidx == copy_at:
            # COPY() CLONE arm: clone the live compressor, feed the SAME chunk to
            # both, and assert byte-identical output.  copy() must clone the
            # z_stream exactly, so a divergence is a torn/shared clone.
            co2 = co.copy()
            comp = co.compress(chunk) + co.flush(zlib.Z_SYNC_FLUSH)
            comp2 = co2.compress(chunk) + co2.flush(zlib.Z_SYNC_FLUSH)
            if comp != comp2:
                H.fail("copy() clone DIVERGED (wid {0}, chunk {1}): the mid-stream "
                       "co.copy() emitted {2} bytes but the source emitted {3} for "
                       "the identical chunk -- the clone cloned a torn z_stream or "
                       "shares scratch with its source under M:N".format(
                           wid, cidx, len(comp2), len(comp)))
                return
            # Main stream continues on the ORIGINAL co (co2 is a discarded probe).
        else:
            comp = co.compress(chunk) + co.flush(zlib.Z_SYNC_FLUSH)

        fed += chunk
        got += do.decompress(comp)

        # YIELD at the hazard boundary: park BETWEEN feeds so a sibling driving its
        # own compressobj on this or another hub reliably interleaves before this
        # fiber resumes and re-checks its private z_stream.
        runloom.yield_now()
        if cidx & 1:
            runloom.sleep(0.0002)

        # STREAMING-PREFIX conservation law: after Z_SYNC_FLUSH the decompressor
        # has emitted every byte fed so far, so the accumulated decompressed prefix
        # MUST equal the accumulated fed prefix -- exactly, byte for byte.
        if bytes(got) != bytes(fed):
            first = _first_diff(got, fed)
            H.fail("streaming-prefix MISMATCH (wid {0}, chunk {1}): decompressed "
                   "prefix ({2} bytes) != fed prefix ({3} bytes), first diff at "
                   "offset {4} -- this fiber's SINGLE-OWNER z_stream scratch was "
                   "corrupted by concurrent M:N execution (torn stream / cross-"
                   "fiber scratch leak)".format(
                       wid, cidx, len(got), len(fed), first))
            return

        state["stream_checks"][wid] += 1   # one writer per slot (race-free)

    # Final Z_FINISH: flush the compressor's tail and assert the FULL round-trip.
    tail = co.flush()                      # Z_FINISH -- ends the stream
    got += do.decompress(tail)
    got += do.flush()
    if bytes(got) != bytes(fed):
        first = _first_diff(got, fed)
        H.fail("full round-trip MISMATCH (wid {0}): final decompressed stream "
               "({1} bytes) != fed plaintext ({2} bytes), first diff at offset "
               "{3} -- the single-owner compressobj/decompressobj pair failed to "
               "round-trip its own byte stream under M:N".format(
                   wid, len(got), len(fed), first))
        return
    state["roundtrips"][wid] += 1


def _first_diff(a, b):
    """Byte offset of the first difference between two buffers (min length if one
    is a prefix of the other).  Diagnostics only."""
    n = min(len(a), len(b))
    for i in range(n):
        if a[i] != b[i]:
            return i
    return n


def worker(H, wid, rng, state):
    """Each fiber repeatedly runs a full single-owner streaming session.  The
    load-bearing prefix + copy() checks fire fail-fast inside run_stream; the
    session churns the scheduler with many parked-between-feed yields so siblings
    reliably interleave against this fiber's private z_stream."""
    for _ in H.round_range():
        if not H.running():
            break
        run_stream(H, wid, rng, state)
        if H.failed:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Race-free per-worker conservation counters: ONE slot per wid (single writer),
    # allocated here where H.funcs is known.  NEVER masked/sharded -- these feed the
    # exact non-vacuity / completeness accounting.
    H.state = {
        "stream_checks": [0] * H.funcs,    # per-chunk prefix-law checks that passed
        "roundtrips": [0] * H.funcs,       # full Z_FINISH round-trips that passed
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    schecks = sum(H.state["stream_checks"])
    rtrips = sum(H.state["roundtrips"])
    H.log("zlib streaming-prefix [single-owner LOAD-BEARING]: {0} per-chunk prefix "
          "conservation checks + {1} full Z_FINISH round-trips (all passed fail-"
          "fast); ops={2}".format(schecks, rtrips, H.total_ops()))

    # NON-VACUITY: the load-bearing incremental-prefix hazard was actually
    # exercised (else the conservation law was vacuous).
    H.check(schecks > 0,
            "no streaming-prefix checks ran -- the incremental compress/decompress "
            "z_stream hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside a C zlib
    # call on a corrupted z_stream).
    H.require_no_lost("zlib streaming-prefix conservation")


if __name__ == "__main__":
    harness.main(
        "p545_zlib_stream_incremental_prefix", body, setup=setup, post=post,
        default_funcs=8000,
        describe="each fiber pairs its OWN zlib compressobj+decompressobj and feeds "
                 "a wid-tagged plaintext CHUNK BY CHUNK (compress + Z_SYNC_FLUSH "
                 "then decompress), yielding between chunks so siblings interleave. "
                 "LOAD-BEARING streaming-prefix conservation: after every chunk the "
                 "accumulated decompressed output MUST exactly equal the accumulated "
                 "fed prefix, and a mid-stream co.copy() clone MUST emit byte-"
                 "identical output to its source; at end Z_FINISH must round-trip "
                 "the full stream.  Single-owner z_stream, so any divergence is a "
                 "torn stream / cross-fiber scratch leak -- a real runtime bug, not "
                 "documented Python semantics")
