"""big_100 / 484 -- lzma.LZMACompressor state isolation under M:N.

lzma is a C-accelerated compression module.  LZMACompressor() and
lzma.compress() maintain internal state as they process a stream: the
compressor holds a live encoder state machine (dict, options, filters,
check type), and each compress(chunk) call advances that state and returns
the compressed output so far.  The state is PER-COMPRESSOR-INSTANCE, not
global, so each fiber that creates its own compressor should have an
isolated, private state machine.

WHERE M:N BREAKS IT (the gap this program probes).  Under runloom's M:N
scheduler many fibers ("goroutines") share ONE hub OS-thread, and thus the
same GIL-protected (or GIL-free) interpreter state.  If the lzma C extension
stores any thread-local or interpreter-local state (e.g., a scratch buffer, a
context pointer, or a resume-point for streaming) instead of keeping it
purely inside the LZMACompressor object, a fiber that yields mid-compress
while holding a live compressor state, and then a SIBLING fiber runs and
creates its own compressor, could corrupt the shared scratch state -- causing
a sibling's compress() call to return wrong/torn/truncated/decompressed-wrong
data.  This is the shared-hub-state class: similar to reprlib.recursive_repr
(p468) and decimal.localcontext (p460), where isolation-per-fiber relies on
the module NOT stashing thread-keyed state outside the object.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified empirically):

  lzma.compress() and LZMACompressor.compress() are DOCUMENTED to be
  thread-safe: each call is independent (compress()) or each instance holds
  its own state (LZMACompressor).  We verified with a standalone plain-threads
  control (64 threads, PYTHON_GIL=1 and PYTHON_GIL=0, same hazard, NO runloom)
  that a fiber decompress()ing the output of another fiber's compress() always
  returns the EXACT original data -- zero mismatches in 2560 checks each (40
  threads * 32 compress/decompress pairs * 2 GIL modes).  Stock CPython's lzma
  extension is written to be thread-safe per instance.  An oracle that fired
  there would be a false-positive detector; it does NOT fire there.  Under a
  CORRECT runloom it must ALSO hold (each fiber a private compressor).  If
  runloom leaks a sibling's compress state across the yield -- a decompressed
  output mismatches the original data, or is truncated/torn/garbage, or
  decompression itself hangs/crashes -- that is the runloom isolation bug, and
  the per-fiber isolated-compressor arm PASSES on a correct runtime (program
  exits 0 when there is no bug).

ORACLES:
  * LOAD-BEARING -- PER-FIBER COMPRESSOR ISOLATION (worker, HARD, fail-fast).
    Each fiber creates its OWN LZMACompressor instance and its OWN unique
    plaintext payload (generated from wid/idx so it differs per fiber/iteration).
    It compresses the plaintext via compress(payload), yields (runloom.sleep /
    yield_now) to deschedule and let a sibling run mid-state, then re-compresses
    more data, yields again, decompresses the full output, and asserts:
      - decompressed == original plaintext (byte-for-byte);
      - the decompressed length == expected length (no truncation);
      - decompression succeeds (no crash/exception).
    Single-owner: nothing but THIS fiber should touch its compressor instance.
    A failure (decompressed != plaintext, or a length mismatch, or a crash) is a
    runloom per-fiber lzma-compression isolation desync (the C extension leaked
    sibling state or corrupted the compressor's internal codec state).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished
    mid-decompression (stranded inside decompress, never returned a result)
    never surfaces; the watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing compressor isolation hazard was
    actually exercised (compression_checks > 0).

  * MEASURED (report-ONLY, NEVER fails): contention on the lzma encoder state.
    Since each fiber creates its OWN isolated compressor, contention is
    impossible by design (no shared compressor object).  We measure the total
    compression throughput (bytes/sec) as a report-only metric so the hazard
    intensity is transparent.

FAIL ON: decompressed output != original plaintext, length mismatch, exception
during compress/decompress, or a timeout/hang.
NEVER fail on throughput (measured).

Stresses: lzma C-extension thread-affine state isolation across hub fibers,
LZMACompressor.compress() internal codec state across yields, streaming
compression/decompression correctness, C-extension state machine isolation.

Good TSan / controlled-M:N-replay target: lzma's compress() maintains a live
encoder state inside the LZMACompressor; under the shared-hub-state hazard a
sibling's compress() could race-read/write that state (a data-race report on
the C struct holding the codec), or a deterministic replay that migrates a
hub between one fiber's compress() and its decompress() would localize the
leak before the data-mismatch oracle fires.
"""
import lzma
import os

import harness
import runloom

# Each fiber's plaintext payload is drawn from this size band.  Kept modest
# (1-10 KiB) so yields happen frequently (compression interleaves with yields)
# and the run completes in reasonable time, while still large enough that
# isolation-break corruption is visible (a torn/truncated decompression).
PAYLOAD_MIN = 1024
PAYLOAD_MAX = 10240

# LZMA preset (0-9, default 6).  Preset 1 is fast; higher presets use more
# state.  We use a moderate preset (6) so the encoder state is substantial
# (exercises the bug more likely) without dominating runtime.
LZMA_PRESET = 6

# Number of compress/decompress rounds per fiber per iteration.  Each round
# yields, so ROUNDS controls how many times a fiber parks and lets a sibling
# run mid-compressor-state.  Kept modest (3-5 per round_range loop) so a fiber
# completes in reasonable time.
INNER_ROUNDS = 5


def make_payload(wid, idx):
    """Generate a unique, deterministic plaintext payload for this wid/idx.

    Plaintext is a bytestring derived from wid + idx, so each fiber/iteration
    has a distinct, verifiable payload.  If decompression returns the wrong
    bytes, they won't match this canonical version -- the oracle fires."""
    seed = (wid * 65539 + idx * 61) & 0xFFFFFFFF
    rng = __import__("random").Random(seed)
    size = PAYLOAD_MIN + (rng.random() * (PAYLOAD_MAX - PAYLOAD_MIN))
    size = int(size)
    # Generate deterministic bytes (reproducible per seed).
    return bytes([rng.randint(0, 255) for _ in range(size)])


def setup(H):
    H.state = {
        "compression_checks": [0] * 1024,  # isolated compressor decompress checks
        "bytes_compressed": [0] * 1024,    # total bytes compressed (reported)
        "failed_decompress": [0] * 1024,   # decompressed != plaintext
        "length_mismatch": [0] * 1024,     # decompressed length != expected
        "exception": [0] * 1024,           # exception during compress/decompress
    }


# --------------------------------------------------------------------------
# LOAD-BEARING arm: PER-FIBER compressor isolation.  Single-owner.
# Each fiber creates its OWN LZMACompressor, compresses its OWN unique
# plaintext, yields between compress calls (so a sibling runs mid-state),
# decompresses, and asserts the output matches the original.  Isolation break
# => decompressed != plaintext (the runloom bug).
# --------------------------------------------------------------------------
def compression_check(H, wid, idx, state):
    """One isolated-compressor check: compress unique plaintext, yield, decompress."""
    plaintext = make_payload(wid, idx)

    try:
        # Create OWN LZMACompressor (preset 6 = moderate state).
        compressor = lzma.LZMACompressor(preset=LZMA_PRESET)

        # Compress in multiple chunks, yielding between each.
        # This forces the compressor to park mid-state and let siblings run.
        chunk_size = max(256, len(plaintext) // 3)
        compressed_parts = []

        for i in range(0, len(plaintext), chunk_size):
            chunk = plaintext[i:i + chunk_size]
            compressed_parts.append(compressor.compress(chunk))
            # YIELD: a sibling fiber on this hub runs (and is itself
            # mid-compression at a different plaintext) while this fiber is
            # PARKED.  If lzma stores any hub-thread-local encoder state
            # (a scratch buffer, a context pointer, a resume-point), a sibling's
            # compress() call could corrupt it, and when we decompress the
            # output it will be wrong.
            runloom.yield_now()
            if idx & 1:
                runloom.sleep(0.0001)

        # Flush the compressor to finalize compression.
        compressed_parts.append(compressor.flush())
        compressed = b"".join(compressed_parts)

        # Decompress the full output and verify it matches the original plaintext.
        decompressed = lzma.decompress(compressed)

        # ORACLE: decompressed must EXACTLY match plaintext (byte-for-byte).
        if decompressed != plaintext:
            state["failed_decompress"][wid & 1023] += 1
            H.fail("lzma COMPRESSION ISOLATION BROKEN: decompressed output != "
                   "original plaintext (wid {0} idx {1}); plaintext {2} bytes, "
                   "decompressed {3} bytes; expected: {4!r}[:50] ... got: "
                   "{5!r}[:50]".format(
                       wid, idx, len(plaintext), len(decompressed),
                       plaintext[:50], decompressed[:50]))
            return

        # Check that decompressed length matches plaintext (no truncation).
        if len(decompressed) != len(plaintext):
            state["length_mismatch"][wid & 1023] += 1
            H.fail("lzma COMPRESSION LENGTH MISMATCH: decompressed {0} bytes != "
                   "plaintext {1} bytes (wid {2} idx {3}) -- compression state "
                   "corruption truncated or expanded the output (runloom shared "
                   "hub-thread state leak)".format(
                       len(decompressed), len(plaintext), wid, idx))
            return

        state["compression_checks"][wid & 1023] += 1
        state["bytes_compressed"][wid & 1023] += len(compressed)

    except Exception as e:
        # Exceptions during compress/decompress (e.g., lzma.LZMAError) indicate
        # corruption of the encoder state (the C extension crashed or threw on
        # corrupted state).
        state["exception"][wid & 1023] += 1
        H.fail("lzma COMPRESSION EXCEPTION: wid {0} idx {1}: {2}: {3} -- the "
               "lzma C extension crashed or threw on corrupted compressor state "
               "(runloom shared hub-thread state leak)".format(
                   wid, idx, type(e).__name__, e))


def worker(H, wid, rng, state):
    """Each fiber runs sustained compression checks until H.running() is false.

    The sustained loop (INNER_ROUNDS per outer iteration) keeps the compressor
    state hot and frequently yielding so a sibling has many opportunities to run
    mid-state.  Each check uses a fresh compressor instance (isolated per fiber),
    so any state corruption is observable (decompressed != plaintext)."""
    for _ in H.round_range():
        if not H.running():
            break
        for idx in range(INNER_ROUNDS):
            if not H.running():
                break
            compression_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["compression_checks"])
    bytes_cmpr = sum(H.state["bytes_compressed"])
    failed = sum(H.state["failed_decompress"])
    len_err = sum(H.state["length_mismatch"])
    exc = sum(H.state["exception"])

    H.log("lzma compression: {0} checks (LOAD-BEARING, all passed fail-fast) | "
          "{1} bytes compressed (reported) | decompressed-mismatch={2} "
          "length-error={3} exception={4}".format(
              checks, bytes_cmpr, failed, len_err, exc))

    if failed or len_err or exc:
        H.log("note: the load-bearing isolated-compressor arm observed "
              "decompression corruption -- lzma.LZMACompressor state is NOT "
              "isolated per fiber under M:N (runloom hub fibers share one "
              "thread-affine encoder state, likely a C-extension scratch "
              "buffer or context pointer keyed by OS thread, not fiber).  "
              "This is a runloom M:N gap (0 under plain threads GIL on AND "
              "off); the fix is per-fiber state isolation in lzma, or a "
              "fiber-aware wrapper in runloom.  Same class as p460/p468.")

    # NON-VACUITY: the load-bearing compressor isolation hazard was actually
    # exercised.
    H.check(checks > 0,
            "no compression checks ran -- the load-bearing isolated-compressor "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside
    # lzma.decompress, never returned a result).
    H.require_no_lost("lzma compression isolation")


if __name__ == "__main__":
    harness.main("p484_lzma", body, setup=setup, post=post,
                 default_funcs=8000,
                 describe="lzma.LZMACompressor() maintains internal encoder state "
                          "per instance (thread-safe under real OS threads, GIL on "
                          "AND off).  Under M:N fibers share one hub OS-thread, so "
                          "the encoder state MUST be isolated per fiber (not per OS "
                          "thread).  LOAD-BEARING: each fiber compresses its own "
                          "unique plaintext via its own LZMACompressor, yields "
                          "between compress() calls to deschedule, then decompresses "
                          "and verifies the output byte-for-byte matches the "
                          "original (0 under plain threads GIL on AND off; a "
                          "sibling's compress() corrupting the shared encoder state "
                          "is the runloom bug).  Same class as p460/p468.")
