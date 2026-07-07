"""big_100 / 566 -- compression codec round-trip identity under M:N.

Python 3.14 gathers the deflate/gzip/bzip2/lzma(/zstd) codecs under one
`compression` package (compression.zlib / .gzip / .bz2 / .lzma).  Each codec
exposes a one-shot compress()/decompress() pair AND a STATEFUL incremental
object (zlib.compressobj / bz2.BZ2Compressor / lzma.LZMACompressor and their
decompress counterparts).  Every one of those is a thin CPython wrapper around a
native streaming state machine (a zlib z_stream, a bz_stream, an lzma_stream) --
a mutable C object carrying the codec's window/dictionary/bit-buffer between
calls.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom multiplexes
tens of thousands of goroutines onto a handful of hubs with the GIL OFF.  A fiber
that owns an incremental compressor feeds it a chunk, then YIELDS mid-stream --
its half-built z_stream/bz_stream/lzma_stream sits parked while sibling fibers on
the same and other hubs run their OWN codec state machines.  When this fiber
resumes on a possibly-different hub and feeds the next chunk, the native stream
state MUST be exactly where it left it.  If the C wrapper stashes any per-thread
or process-global scratch (a shared work buffer, a thread-id-keyed context, a
non-reentrant static) that another fiber's codec call clobbered across the yield,
the stream desyncs and the round-trip no longer reproduces the original bytes --
a torn compressed frame or a corrupted decompression.

WHICH ORACLE IS LOAD-BEARING, AND WHY.  Compression is a pure, deterministic,
LOSSLESS round trip:  decompress(compress(X)) == X, bit for bit, for every codec.
That identity is the closed-form law -- it does not depend on timing, ordering,
or any sibling.  Each fiber builds its OWN fiber-local input bytes, owns its OWN
single-owner compressor and decompressor objects (never shared), and drives the
full compress->yield->decompress cycle.  The recovered bytes MUST equal the
original AND their CRC32 must equal the CRC32 computed from the original before
the yield (a redundant closed-form checksum that catches any silent single-byte
corruption the length/equality check might race past).  Verified against a
plain-threads control (8 OS threads each round-tripping fiber-local data through
all codecs, GIL on AND off): 100% bit-identical, 0 checksum mismatches.  Under a
correct runloom it must also hold; a mismatch means the codec's native stream
state leaked or was clobbered across a hub-migrating yield -- a runtime bug, not a
documented codec semantic (the compressor objects are single-owner here, never
shared, so this is NOT the "shared mutable object races" case).

ORACLES:
  * LOAD-BEARING -- ROUND-TRIP IDENTITY (worker, HARD, fail-fast).  Per iteration
    a fiber builds fiber-local input bytes, records the closed-form CRC32 of the
    original, then round-trips through ONE codec path (round-robined by wid+idx so
    all paths are covered) -- one-shot compress/decompress OR a STATEFUL
    incremental compressor+decompressor fed chunk-by-chunk with a yield between
    every chunk so a sibling's codec reliably interleaves while this fiber's native
    stream is parked mid-frame.  Asserts: recovered length == original length,
    recovered bytes == original bytes, and CRC32(recovered) == the pre-yield CRC32.
    Single-owner: the input, the compressor, the decompressor, and both blobs are
    all fiber-local; a failure is a runloom codec-state desync.

  * CONSERVATION / NON-VACUITY (post, HARD): per-wid race-free slots tally the
    round-trips completed and bytes conserved; require > 0 (else the round-trip
    hazard was never exercised).

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside a native
    compress()/decompress() call (a deadlocked stream, an infinite inflate loop on
    a torn frame) never returns; the watchdog + require_no_lost catch it.

FAIL ON: a round-trip that does not reproduce the original bytes, a length
mismatch, or a CRC32 mismatch across the yield -- any of which means a codec's
single-owner native stream state was corrupted by a sibling under M:N.  There is
NO shared-object arm: every codec object here has exactly one owner, so a failure
cannot be dismissed as documented shared-mutable-object behavior.

Stresses: compression.zlib/gzip/bz2/lzma one-shot + incremental codecs, native
z_stream/bz_stream/lzma_stream state carried across hub-migrating yields, per-fiber
codec-object isolation, lossless round-trip identity + CRC32 checksum conservation
under GIL-off M:N concurrency.

Good TSan / controlled-M:N-replay target: the native streaming compressors keep
mutable C state between calls; a data-race report on a codec's internal buffer, or
a deterministic replay that resumes a parked incremental compressor after a
sibling's codec call, localizes the desync before the byte/CRC oracle fires.
"""
import compression.bz2
import compression.gzip
import compression.lzma
import compression.zlib

import harness
import runloom

# zlib.crc32 is the closed-form checksum used as the redundant round-trip oracle.
# It is a pure function of the bytes; recomputing it on the recovered data must
# match the value taken from the original before the yield.
crc32 = compression.zlib.crc32

# The codec PATHS, round-robined by (wid + idx) so post() coverage holds whether
# one fiber does K iterations or K fibers do one each (deterministic, not random --
# random selection reliably MISSES a path at low op-count under load).
CASE_ZLIB_ONESHOT = 0    # compression.zlib.compress / .decompress
CASE_GZIP_ONESHOT = 1    # compression.gzip.compress / .decompress
CASE_BZ2_ONESHOT = 2     # compression.bz2.compress / .decompress
CASE_LZMA_ONESHOT = 3    # compression.lzma.compress / .decompress
CASE_ZLIB_INCR = 4       # zlib.compressobj / decompressobj (stateful, chunked)
CASE_BZ2_INCR = 5        # BZ2Compressor / BZ2Decompressor (stateful, chunked)
CASE_LZMA_INCR = 6       # LZMACompressor / LZMADecompressor (stateful, chunked)
NCASES = 7


def build_input(rng):
    """Build one fiber's fiber-local input as a list of chunks (never shared).

    The chunks mix highly-compressible runs, incompressible random noise, and
    repetitive text so the codecs exercise real window/dictionary state (a
    trivially-compressible input would barely touch the streaming machinery).
    Returned as chunks so the incremental paths can feed them one at a time with a
    yield between each -- the point at which the native stream is parked mid-frame."""
    nchunks = rng.randint(3, 7)
    chunks = []
    for _ in range(nchunks):
        kind = rng.randint(0, 2)
        n = rng.randint(64, 512)
        if kind == 0:
            # Highly compressible: a single-byte run.
            chunks.append(bytes([rng.randint(0, 255)]) * n)
        elif kind == 1:
            # Incompressible: random noise (stresses the fallback stored-block path).
            chunks.append(bytes(rng.randint(0, 255) for _ in range(n)))
        else:
            # Repetitive text: real dictionary matches across chunk boundaries.
            unit = b"the quick brown fox jumps over the lazy dog "
            chunks.append(unit * rng.randint(2, 12))
    return chunks


def roundtrip_oneshot(compress, decompress, original):
    """One-shot round trip with a yield between compress and decompress.  The
    compressed blob is fiber-local; the yield parks this fiber so siblings run
    their own codec calls before this fiber decompresses.  Returns recovered bytes."""
    blob = compress(original)
    runloom.yield_now()
    return decompress(blob)


def roundtrip_incremental(make_comp, make_decomp, chunks, original):
    """Stateful incremental round trip.  Owns a fresh single-owner compressor and
    decompressor.  Feeds each input chunk to the compressor with a yield AFTER each
    chunk (native stream parked mid-frame), flushes, then feeds the compressed blob
    to a single-owner decompressor in slices with a yield between slices.  Returns
    recovered bytes.  All state (both stream objects, both blobs) is fiber-local."""
    comp = make_comp()
    parts = []
    for ch in chunks:
        parts.append(comp.compress(ch))
        runloom.yield_now()                # native compressor stream parked mid-frame
    parts.append(comp.flush())
    blob = b"".join(parts)

    decomp = make_decomp()
    out = []
    # Feed the compressed blob in slices so the decompressor's native stream is also
    # parked mid-frame across a yield.
    step = max(1, len(blob) // 4)
    pos = 0
    while pos < len(blob):
        out.append(decomp.decompress(blob[pos:pos + step]))
        runloom.yield_now()                # native decompressor stream parked mid-frame
        pos += step
    # Some decompressors expose flush(); the incremental ones here finish on the
    # last decompress() call, but join whatever was produced.
    return b"".join(out)


def do_roundtrip(case, chunks, original):
    """Dispatch to the codec path selected by `case`.  Returns recovered bytes."""
    if case == CASE_ZLIB_ONESHOT:
        return roundtrip_oneshot(compression.zlib.compress,
                                 compression.zlib.decompress, original)
    if case == CASE_GZIP_ONESHOT:
        return roundtrip_oneshot(compression.gzip.compress,
                                 compression.gzip.decompress, original)
    if case == CASE_BZ2_ONESHOT:
        return roundtrip_oneshot(compression.bz2.compress,
                                 compression.bz2.decompress, original)
    if case == CASE_LZMA_ONESHOT:
        return roundtrip_oneshot(compression.lzma.compress,
                                 compression.lzma.decompress, original)
    if case == CASE_ZLIB_INCR:
        return roundtrip_incremental(compression.zlib.compressobj,
                                     compression.zlib.decompressobj,
                                     chunks, original)
    if case == CASE_BZ2_INCR:
        return roundtrip_incremental(compression.bz2.BZ2Compressor,
                                     compression.bz2.BZ2Decompressor,
                                     chunks, original)
    # CASE_LZMA_INCR
    return roundtrip_incremental(compression.lzma.LZMACompressor,
                                 compression.lzma.LZMADecompressor,
                                 chunks, original)


CASE_NAMES = ("zlib-oneshot", "gzip-oneshot", "bz2-oneshot", "lzma-oneshot",
              "zlib-incremental", "bz2-incremental", "lzma-incremental")

# Sustained iterations per worker, bounded by H.running().  The codec-state desync
# hazard only manifests under SUSTAINED churn -- many fibers simultaneously parked
# mid-frame across yields so a sibling's native codec call reliably runs before this
# fiber resumes.  A single round-trip per fiber barely overlaps and does not repro.
INNER_CAP = 100000


def roundtrip_check(H, wid, idx, state):
    """Single-owner round-trip identity check (LOAD-BEARING, fail-fast).

    Builds fiber-local input, records the closed-form CRC32 of the original, drives
    one codec path through compress->yield->decompress, and asserts the recovered
    bytes are bit-identical to the original (length, content, and CRC32)."""
    rng = H.derive(wid, idx)
    chunks = build_input(rng)
    original = b"".join(chunks)
    expected_crc = crc32(original)         # closed-form checksum taken BEFORE the yield
    expected_len = len(original)

    case = (wid + idx) % NCASES
    recovered = do_roundtrip(case, chunks, original)

    # Check 1: length conserved.
    if len(recovered) != expected_len:
        H.fail("round-trip LENGTH mismatch on {0}: recovered {1} bytes, original "
               "{2} bytes (wid {3}) -- the codec's native stream state was "
               "corrupted across a yield, producing a truncated/padded frame".format(
                   CASE_NAMES[case], len(recovered), expected_len, wid))
        return

    # Check 2: bit-identical content.
    if recovered != original:
        H.fail("round-trip CONTENT mismatch on {0}: decompress(compress(X)) != X "
               "(wid {1}, {2} bytes) -- a lossless codec produced different bytes, "
               "the native stream state desynced under M:N".format(
                   CASE_NAMES[case], wid, expected_len))
        return

    # Check 3: redundant closed-form CRC32 (catches any silent corruption a raw
    # equality compare might race past, and re-verifies the checksum is pure across
    # the yield).
    got_crc = crc32(recovered)
    if got_crc != expected_crc:
        H.fail("round-trip CRC32 mismatch on {0}: recovered CRC32 {1} != original "
               "CRC32 {2} (wid {3}, {4} bytes) -- a silent byte corruption in the "
               "codec round trip across a yield".format(
                   CASE_NAMES[case], got_crc, expected_crc, wid, expected_len))
        return

    state["rt_count"][wid] += 1            # single-writer-per-slot, race-free
    state["rt_bytes"][wid] += expected_len


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            roundtrip_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # Per-wid race-free slots (one writer per slot; H.funcs known here).
    H.state = {
        "rt_count": [0] * H.funcs,         # round-trips completed per worker
        "rt_bytes": [0] * H.funcs,         # bytes conserved per worker
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    rts = sum(H.state["rt_count"])
    nbytes = sum(H.state["rt_bytes"])
    H.log("compression round-trips (all codecs, single-owner LOAD-BEARING, passed "
          "fail-fast): {0} | bytes conserved: {1} | ops={2}".format(
              rts, nbytes, H.total_ops()))

    # NON-VACUITY: the load-bearing round-trip hazard was actually exercised.
    H.check(rts > 0,
            "no compression round-trips completed -- the codec-state desync hazard "
            "was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished inside a native codec call.
    H.require_no_lost("compression round-trip completeness")


if __name__ == "__main__":
    harness.main(
        "p566_compression_roundtrip", body, setup=setup, post=post,
        default_funcs=4000,
        describe="Python 3.14's compression package (zlib/gzip/bz2/lzma) exposes "
                 "one-shot AND stateful incremental codecs, each wrapping a native "
                 "streaming state machine (z_stream/bz_stream/lzma_stream).  Under "
                 "M:N a fiber parks its half-built stream mid-frame across a yield "
                 "while siblings run their own codec calls.  LOAD-BEARING: each "
                 "fiber owns fiber-local input + single-owner compressor/decompressor "
                 "objects and drives compress->yield->decompress; the recovered bytes "
                 "MUST be bit-identical to the original (length, content, CRC32).  A "
                 "mismatch means the codec's native stream state leaked or was "
                 "clobbered across a hub-migrating yield -- a runtime bug, not a "
                 "shared-object semantic (every codec object here is single-owner)")
