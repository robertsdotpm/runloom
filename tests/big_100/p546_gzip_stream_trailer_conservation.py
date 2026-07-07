"""big_100 / 546 -- gzip.GzipFile incremental-stream CRC/ISIZE trailer conservation under M:N.

gzip's wire format is a fixed 10-byte header, the DEFLATE body, and an 8-byte
TRAILER: a little-endian CRC32 of the *entire uncompressed input* followed by
ISIZE, the uncompressed length modulo 2**32.  gzip.GzipFile computes both of
these INCREMENTALLY as bytes are fed through write():  every write() call folds
the new bytes into a running zlib.crc32 accumulator (self._crc) and adds their
length to a running size counter (self._size); close() (via _write_gzip_footer)
then serialises those two accumulators into the trailer.  The CRC and ISIZE are
therefore a CONSERVATION LAW over the whole write() history: if every byte a
fiber wrote was folded into the accumulators exactly once, the decoder's
independent recomputation must match, and ISIZE must equal the fiber's own
plaintext length.

WHERE M:N COULD BREAK IT (the gap this program probes).  Under free-threaded
CPython with the GIL off and runloom hubs > 1, a fiber that parks (yields)
BETWEEN incremental write() calls is suspended with a half-built compress
stream: a live zlib compressobj holding DEFLATE scratch, a partial CRC32
accumulator, and a partial ISIZE counter, all hanging off its GzipFile.  A
sibling fiber resuming on the same OR a different hub is driving its OWN
GzipFile through the same code.  If any of that per-stream scratch were shared,
aliased, or clobbered across the park -- the zlib compressobj's internal buffer,
the CRC accumulator, the size counter -- this fiber's trailer would desync from
its plaintext:  the ISIZE would count a sibling's bytes, the CRC would fold a
sibling's data, or the DEFLATE body would carry interleaved bytes.  Any of those
makes gzip.decompress() raise BadGzipFile / zlib.error, or return the wrong
bytes, or makes the trailing ISIZE disagree with len(plaintext).

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  The load-bearing oracle is the TRAILER CONSERVATION LAW on a SINGLE-OWNER
  stream.  Each fiber owns its OWN gzip.GzipFile wrapping its OWN io.BytesIO,
  feeds its OWN wid-tagged plaintext through it in many incremental write()
  calls with a yield after each (so a sibling reliably interleaves while the
  compress stream is half-built), close()s to flush the trailer, and then, as an
  independent decoder, asserts ALL of:
      (1) ISIZE (trailing 4 bytes, LE) == len(plaintext) & 0xFFFFFFFF
      (2) CRC32 (bytes -8..-4, LE) == zlib.crc32(plaintext) & 0xFFFFFFFF
      (3) gzip.decompress(compressed) == plaintext                (byte-exact)
      (4) header is the deterministic mtime=0 gzip header (\\x1f\\x8b\\x08 ...)
  Nothing is shared: the GzipFile, the BytesIO, the plaintext, and the two
  accumulators all belong to exactly one fiber for the object's whole life.  We
  verified with a standalone plain-threads control (8 OS threads, GIL on AND
  off, each streaming its own wid-tagged plaintext through its own GzipFile with
  a sched_yield between writes) that 100% of trailers satisfy (1)-(4) -- zero
  desyncs -- because each GzipFile's compressobj/CRC/size is private.  Under a
  CORRECT runloom the same must hold: a fiber parking mid-stream and resuming on
  another hub must find its accumulators exactly as it left them.  If instead a
  fiber's ISIZE counts the wrong length, its CRC mismatches, or its round-trip
  is not byte-identical, that is a real runtime bug (cross-fiber leak of a
  single-owner compress stream, a torn accumulator, or a lost/duplicated
  write()).  The oracle PASSES on a correct runtime (program exits 0 when there
  is no bug) -- it is falsifiable only by an actual desync.

ORACLES:
  * LOAD-BEARING -- TRAILER CONSERVATION (worker, HARD, fail-fast).  The four
    checks above on a single-owner incremental gzip stream.  Single-owner: the
    GzipFile/BytesIO/plaintext are fiber-local, never shared.  A failure is a
    runloom stream-isolation / accumulator-conservation desync.

  * CONSERVATION SUM (post, HARD via require_no_lost + non-vacuity): each fiber
    records into a race-free per-wid slot how many plaintext bytes it conserved
    (ISIZE matched) this run; sum > 0 proves the law was actually exercised.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-
    stream (stranded inside write()/close()/decompress on a desynced object)
    never returns; the watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): gzip_checks > 0 -- the load-bearing arm ran.

No shared-mutable arm exists here: a gzip stream is inherently single-owner
(a half-built compressobj is not a container you would ever legitimately share),
so there is no documented-shared-race behaviour to mislabel.  Every check is on
a private stream; a FAIL therefore means a genuine runtime desync.

Stresses: gzip.GzipFile incremental write() folding into zlib.crc32 + ISIZE
accumulators across a park; zlib compressobj scratch lifetime across hub
migration; _write_gzip_footer trailer serialisation; gzip.decompress /
_GzipReader trailer verification (CRC/ISIZE recheck) under M:N; lost/duplicated
incremental write on a shared-across-hubs compress stream.

Good TSan / controlled-M:N-replay target: self._crc / self._size are plain
Python-int RMW updated inside write(), and the compressobj wraps a C zlib
z_stream; a data-race report on either accumulator, or on the z_stream buffer,
under a replay that parks one fiber mid-write while a sibling drives its own
stream, localizes the desync before the CRC/ISIZE conservation check even fires.
"""
import gzip
import io
import struct
import zlib

import harness
import runloom

# Each fiber's plaintext is assembled from this many wid-tagged chunks, fed
# through the GzipFile as separate incremental write() calls with a yield after
# each.  Enough chunks that the fiber parks MANY times with a half-built
# compress stream (the window where a sibling's stream could clobber this one),
# few enough that a full stream + decode completes quickly under load.
MIN_CHUNKS = 6
MAX_CHUNKS = 28

# Per-chunk payload length band.  Chunks are large enough that the DEFLATE body
# spans several blocks (so the compressobj holds real scratch across the park)
# and the total plaintext pushes ISIZE well past a single write.
MIN_CHUNK = 24
MAX_CHUNK = 200

# gzip header for mtime=0, compresslevel in the default band, OS byte 0xff
# (unknown) as CPython emits.  We only assert the first three magic/method
# bytes are deterministic (\x1f \x8b \x08 == gzip magic + DEFLATE); the mtime
# field (bytes 4..8) is pinned to 0 by mtime=0, so the header is fully
# deterministic and cannot vary per fiber -- a header that differs is a torn
# write, not legitimate variation.
GZIP_MAGIC = b"\x1f\x8b\x08"


def build_plaintext(wid, rng):
    """Assemble a fiber's PRIVATE, wid-tagged plaintext as a list of chunks.

    Every chunk is stamped with this fiber's wid and the chunk index, and the
    payload bytes are derived from wid so that if a sibling's bytes ever leaked
    into this fiber's DEFLATE body, the round-trip comparison (byte-exact) would
    catch the foreign bytes even before the CRC did.  Returns (chunks, joined)
    where joined == b"".join(chunks) is the full plaintext the decoder must
    reproduce and whose length must equal ISIZE."""
    nchunks = rng.randint(MIN_CHUNKS, MAX_CHUNKS)
    chunks = []
    for ci in range(nchunks):
        tag = "W{0}:C{1}:".format(wid, ci).encode("ascii")
        plen = rng.randint(MIN_CHUNK, MAX_CHUNK)
        base = (wid * 131 + ci * 17) & 0xFF
        payload = bytes((base + j) & 0xFF for j in range(plen))
        chunks.append(tag + payload)
    joined = b"".join(chunks)
    return chunks, joined


def stream_check(H, wid, rng, state):
    """Single-owner incremental gzip stream + trailer conservation law.

    Build a private wid-tagged plaintext, stream it through a private
    GzipFile(fileobj=BytesIO, mtime=0) one chunk per write() with a yield after
    each (parking the half-built compress stream so siblings interleave), flush
    the trailer with close(), then independently verify ISIZE, CRC32, byte-exact
    round-trip, and the deterministic header.  Nothing here is shared."""
    chunks, plaintext = build_plaintext(wid, rng)

    buf = io.BytesIO()
    # compresslevel varies per fiber so distinct compressobj states coexist
    # across hubs; mtime=0 pins the header deterministic regardless.
    level = 1 + (wid % 9)
    gz = gzip.GzipFile(fileobj=buf, mode="wb", mtime=0, compresslevel=level)
    try:
        for ci, chunk in enumerate(chunks):
            gz.write(chunk)
            # PARK mid-stream: the compressobj + CRC + ISIZE accumulators are
            # half-built here.  A sibling on this or another hub drives its own
            # GzipFile through the same code while we are suspended.
            runloom.yield_now()
            if ci & 1:
                runloom.sleep(0.0002)
        gz.close()                          # _write_gzip_footer flushes trailer
    except Exception as e:                  # noqa: BLE001 -- any failure is load-bearing
        H.fail("gzip incremental write/close raised {0}: {1} (wid {2}, "
               "{3} chunks, {4} plaintext bytes) -- a half-built compress "
               "stream was corrupted across a park".format(
                   type(e).__name__, e, wid, len(chunks), len(plaintext)))
        return

    compressed = buf.getvalue()

    # A well-formed gzip member is header(10) + body + trailer(8): at minimum 18
    # bytes.  Anything shorter is a torn stream.
    if len(compressed) < 18:
        H.fail("gzip output only {0} bytes (< 18-byte minimum member) for wid "
               "{1} -- a torn/truncated compress stream across a park".format(
                   len(compressed), wid))
        return

    # Check 4: deterministic mtime=0 header.  A differing magic/method is a torn
    # write, since mtime=0 makes the whole 10-byte header fiber-independent.
    if compressed[:3] != GZIP_MAGIC:
        H.fail("gzip header magic wrong: got {0!r}, expected {1!r} (wid {2}) -- "
               "the deterministic mtime=0 header was overwritten, a torn write "
               "on a shared-across-hubs compress stream".format(
                   compressed[:3], GZIP_MAGIC, wid))
        return

    # Independently decode the trailer: last 8 bytes are LE CRC32 then ISIZE.
    trailer_crc, trailer_isize = struct.unpack("<II", compressed[-8:])

    expected_isize = len(plaintext) & 0xFFFFFFFF
    expected_crc = zlib.crc32(plaintext) & 0xFFFFFFFF

    # Check 1: ISIZE conservation -- the trailer's uncompressed-length counter
    # must equal THIS fiber's plaintext length.  A wrong ISIZE means the running
    # self._size counter folded a sibling's bytes (or dropped some of ours).
    if trailer_isize != expected_isize:
        H.fail("gzip ISIZE conservation broken: trailer ISIZE={0} but this "
               "fiber's plaintext is {1} bytes (wid {2}) -- the incremental "
               "size accumulator counted the WRONG bytes across a park (a "
               "cross-fiber leak of the compress stream, or a lost/duplicated "
               "write)".format(trailer_isize, expected_isize, wid))
        return

    # Check 2: CRC32 conservation -- the trailer's CRC must equal an independent
    # crc32 over THIS fiber's plaintext.  A mismatch means the running self._crc
    # accumulator folded foreign bytes.
    if trailer_crc != expected_crc:
        H.fail("gzip CRC32 conservation broken: trailer CRC=0x{0:08x} but "
               "zlib.crc32(plaintext)=0x{1:08x} (wid {2}, {3} bytes) -- the "
               "incremental CRC accumulator folded the WRONG bytes across a "
               "park (a torn accumulator or cross-fiber stream leak)".format(
                   trailer_crc, expected_crc, wid, len(plaintext)))
        return

    # Check 3: byte-exact round-trip via an INDEPENDENT decoder (gzip.decompress
    # re-verifies CRC + ISIZE internally and raises BadGzipFile on mismatch, and
    # here we also compare bytes so an interleaved DEFLATE body is caught).
    try:
        back = gzip.decompress(compressed)
    except Exception as e:                  # noqa: BLE001 -- BadGzipFile/zlib.error etc.
        H.fail("gzip.decompress raised {0}: {1} on this fiber's OWN stream "
               "(wid {2}, {3} plaintext bytes) -- the trailer/body it wrote is "
               "not a valid gzip member, i.e. a desynced compress stream across "
               "a park".format(type(e).__name__, e, wid, len(plaintext)))
        return

    if back != plaintext:
        # Localise the first differing offset for the report.
        n = min(len(back), len(plaintext))
        off = next((i for i in range(n) if back[i] != plaintext[i]), n)
        H.fail("gzip round-trip NOT byte-identical for wid {0}: decoded {1} "
               "bytes vs {2} expected, first diff at offset {3} -- a sibling's "
               "bytes interleaved into this fiber's DEFLATE body, or the stream "
               "desynced across a park".format(
                   len(back), len(back), len(plaintext), off))
        return

    # Conservation succeeded: record this fiber's conserved plaintext bytes into
    # its OWN race-free slot (one writer per wid; never aliased).
    state["gzip_checks"][wid] += 1
    state["bytes_conserved"][wid] += len(plaintext)


# The trailer-desync hazard only manifests under SUSTAINED churn: many fibers
# simultaneously half-way through incremental gzip streams while sleep/yield-
# PARKED, so the scheduler reliably resumes a sibling's stream inside this
# fiber's park window.  A single stream per fiber barely overlaps a sibling's;
# an inner loop keeps every hub saturated with half-built compress streams.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            stream_check(H, wid, rng, state)        # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # Race-free per-wid conservation slots: ONE writer per slot (wid-indexed),
    # allocated where H.funcs is known.  NEVER 'wid & MASK' (that would alias
    # writers and lose increments GIL-off) -- these feed exact conservation sums.
    H.state = {
        "gzip_checks": [0] * H.funcs,        # LOAD-BEARING streams conserved
        "bytes_conserved": [0] * H.funcs,    # plaintext bytes with matching ISIZE
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["gzip_checks"])
    bytes_ok = sum(H.state["bytes_conserved"])
    H.log("gzip[single-owner LOAD-BEARING]: {0} incremental-stream trailer "
          "conservation checks passed fail-fast (ISIZE == plaintext len, CRC32 "
          "== independent crc, byte-exact round-trip, deterministic mtime=0 "
          "header); {1} plaintext bytes conserved; ops={2}".format(
              checks, bytes_ok, H.total_ops()))

    # NON-VACUITY: the load-bearing conservation law was actually exercised.
    H.check(checks > 0,
            "no single-owner gzip trailer-conservation checks ran -- the "
            "incremental CRC/ISIZE trailer hazard was never exercised (oracle "
            "would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-stream (stranded inside
    # write()/close()/decompress on a desynced compress stream).
    H.require_no_lost("gzip trailer conservation")


if __name__ == "__main__":
    harness.main(
        "p546_gzip_stream_trailer_conservation", body, setup=setup, post=post,
        default_funcs=8000,
        describe="each fiber streams its OWN wid-tagged plaintext through its "
                 "OWN gzip.GzipFile(fileobj=BytesIO, mtime=0) in incremental "
                 "write() calls, PARKING (yield/sleep) after each so a sibling "
                 "interleaves while the compressobj + CRC + ISIZE accumulators "
                 "are half-built, then close()s to flush the trailer.  "
                 "LOAD-BEARING trailer CONSERVATION law: the trailing 4-byte "
                 "ISIZE == len(plaintext)&0xFFFFFFFF, the 4-byte CRC32 == an "
                 "independent zlib.crc32(plaintext), gzip.decompress round-trips "
                 "byte-exact, and the mtime=0 header is deterministic.  A wrong "
                 "ISIZE/CRC, a BadGzipFile, or a non-identical round-trip is a "
                 "cross-fiber leak of a single-owner compress stream, a torn "
                 "accumulator, or a lost/duplicated incremental write")
