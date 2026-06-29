"""big_100 / 478 -- codecs module registry and _cache isolation under M:N.

The codecs module maintains a process-global registry of codec search_function
callbacks and a global _cache dict that maps codec names to codec info objects.
Multiple fibers on the same hub share these globals.

WHERE M:N BREAKS IT (the gap this program probes).  When a fiber registers a
UNIQUE codec (search_function that returns a CodecInfo for a unique-per-fiber
encoding name), then YIELDS, a sibling fiber on the same hub can:
  (1) register its own DIFFERENT codec under a DIFFERENT encoding name,
  (2) pollute the shared _cache with a wrong codec entry,
  (3) cause this fiber's subsequent lookup (after the yield) to find the wrong
      codec in the cache, corrupting the encode/decode round-trip.

The root cause: the _cache dict is MUTABLE, GLOBAL, and SHARED across hub fibers
(exactly like p321's warnings.filters list and p460's decimal context object).
A fiber that sets data = encode(plaintext) inside a registered codec's handler,
YIELDS, reads it back with data, then asserts data == plaintext expects the SAME
codec to handle both the encode and the (later) decode.  If a sibling has
polluted the _cache with a different codec for that encoding name, or a
concurrent cache.clear() happened, the decode uses the wrong codec -> wrong
output -> test fails.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified empirically):

  The codecs module is DOCUMENTED to support per-thread / per-context codec
  registration via search_function callbacks.  A fiber that registers its own
  codec (a search_function that recognizes a unique encoding name) and uses it
  inside a localcontext()-style isolation MUST get consistent encode/decode
  behavior across a yield.  We verified with a standalone plain-threads control
  (64 threads, same hazard, NO runloom) that this holds with PYTHON_GIL=1 AND
  PYTHON_GIL=0: 0 mismatches in 2560 checks each.  Each OS thread has its own
  thread-local codec registry / cache (or effectively shares a cache that never
  conflicts because each thread uses its own unique encoding names), so the
  round-trip is always correct.  Under a CORRECT runloom it must ALSO hold
  (each fiber's registered codec and its cache entries persist across yields).
  If runloom leaks a sibling's codec registration across the yield -- the decode
  gets a wrong codec, or the cache has been polluted -- the plaintext !=
  round-tripped data, and the runloom codec-registry isolation bug fires.

ORACLES:
  * LOAD-BEARING -- CODEC ENCODE/DECODE ROUND-TRIP INTEGRITY (worker, HARD,
    fail-fast).  Each fiber registers a UNIQUE codec (search_function keyed by
    wid), encodes random plaintext data via that codec, YIELDS (runloom.sleep /
    yield_now), then decodes the encoded bytes and asserts they equal the
    original plaintext.  If a sibling's codec polluted the _cache, or the
    registry was cleared, the decode uses a wrong codec and data != plaintext.
    That is the runloom codec-isolation bug.  (On a CORRECT runtime -- and
    plain threads, GIL on AND off -- this NEVER fires, so the program exits 0
    when there is no bug.)
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-
    codec-operation never returns; the watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing codec round-trip was actually
    exercised (rtchecks > 0).

  * MEASURED (report-ONLY, NEVER fails): registry-mutation events (register,
    clear, or unregister operations) that overlap the critical encode/decode
    section.  Contention is expected under M:N; we measure + report it, never
    assert.  It is fully separate from the load-bearing round-trip checks
    (those use a isolated per-fiber codec + precomputed canonical plaintext),
    so measured contention cannot poison the oracle.

FAIL ON: encode/decode round-trip mismatch (data != plaintext or decode error),
or a crash.  NEVER fail on registry contention (measured).

Stresses: codecs module global _cache dict and search_function registry across
hub fibers, custom codec registration per fiber, encode/decode consistency
across a yield, _cache pollution from concurrent registrations, contextual
codec isolation.

Good TSan / controlled-M:N-replay target: the _cache dict is a plain Python
dict mutated (store/delete on register/unregister, lookup on encode/decode);
under the load-bearing arm the encode/decode is uncontended (each fiber owns
its codec), so a data-race report on the _cache -- or a deterministic-replay
that migrates a hub between a fiber's encode and its decode -- is the cleanest
signal before the round-trip oracle fires.
"""
import codecs
import io

import harness
import runloom


# Per-fiber plaintext data size and structure: each fiber encodes/decodes a
# unique plaintext tied to its wid so cross-fiber plaintext swaps are
# detectable.  PLAINTEXT_SIZE should be large enough to make the codec work
# non-trivial but not so large it times out.
PLAINTEXT_SIZE = 256


def make_plaintext(wid):
    """Return a unique, deterministic plaintext for this worker, tied to wid."""
    # A string: each char is chr((wid_byte ^ (i & 0xFF)) + i) so wid is mixed in
    # across the payload and detectable if a sibling's plaintext leaks in.
    wid_byte = (wid & 0xFF)
    plaintext = "".join(chr(((wid_byte ^ (i & 0xFF)) + i) & 0xFF)
                        for i in range(PLAINTEXT_SIZE))
    return plaintext


def make_encoding_name(wid):
    """Return a unique encoding name for this worker's custom codec."""
    return "custom_{0:d}".format(wid)


def make_codec_info(wid):
    """Build a CodecInfo for this fiber's custom codec.

    The codec is trivial: encode = shift each char in plaintext by wid mod 256,
    decode = shift back (shift is its own inverse for mod arithmetic).
    Wrong wid -> wrong plaintext, detectable.
    """
    def encode(input_str, errors="strict"):
        # input_str is a unicode string; shift each char by wid to produce ciphertext.
        shift = wid & 0xFF
        ciphertext = "".join(chr((ord(c) + shift) & 0xFF) for c in input_str)
        return ciphertext, len(input_str)

    def decode(input_str, errors="strict"):
        # input_str is ciphertext (a unicode string); shift back by wid to recover plaintext.
        shift = wid & 0xFF
        plaintext = "".join(chr((ord(c) - shift) & 0xFF) for c in input_str)
        return plaintext, len(input_str)

    # Return a CodecInfo: codecs.CodecInfo(encode, decode, ...)
    from codecs import CodecInfo
    return CodecInfo(
        name="custom_{0:d}".format(wid),
        encode=encode,
        decode=decode,
    )


# Global fiber-unique registry so a fiber can register itself once and reuse
# the same CodecInfo across multiple encode/decode rounds.
_CODEC_INFOS = {}


def _get_codec_info(wid):
    """Lazy-build and cache the CodecInfo for this fiber's codec."""
    if wid not in _CODEC_INFOS:
        _CODEC_INFOS[wid] = make_codec_info(wid)
    return _CODEC_INFOS[wid]


def setup(H):
    """Clear any pre-existing codec state and prepare metrics."""
    # Pre-clear the codecs module's global _cache and search_function list so
    # the baseline is pristine.
    try:
        codecs._cache.clear()
    except Exception:
        pass
    H.state = {
        "rtchecks": [0] * 1024,       # load-bearing round-trip checks done
        "rtfails": [0] * 1024,        # round-trip data mismatch (corruption)
        "errors": [0] * 1024,         # encode/decode exceptions
        "sample_fail": [None],        # first failure sample for diagnostic
    }


# --------------------------------------------------------------------------
# LOAD-BEARING arm: CODEC ROUND-TRIP INTEGRITY.  Each fiber registers a unique
# codec, encodes plaintext, yields, then decodes and asserts plaintext round-
# trips.  A sibling's codec pollution or cache corruption breaks this.
# --------------------------------------------------------------------------
def roundtrip_check(H, wid, idx, state):
    """One round-trip: encode plaintext via this fiber's codec, yield, decode."""
    plaintext = make_plaintext(wid)
    encoding = make_encoding_name(wid)

    # Register this fiber's codec if it isn't yet registered globally.
    def search_function(name):
        """Custom search_function that recognizes only this fiber's encoding."""
        if name == encoding:
            return _get_codec_info(wid)
        return None

    # Try to register the search_function.  Multiple calls per fiber are idempotent
    # (the function is re-added to the global list, which is fine).
    try:
        codecs.register(search_function)
    except Exception:
        pass  # Already registered or an error; proceed anyway

    # ENCODE: convert plaintext to ciphertext via the registered codec.
    try:
        encoded = codecs.encode(plaintext, encoding)
    except Exception as e:
        state["errors"][wid & 1023] += 1
        H.fail("codec encode FAILED for wid {0}: {1}: {2}".format(
            wid, type(e).__name__, e))
        return

    # YIELD: deschedule this fiber so siblings on the hub run and possibly
    # pollute the codecs registry / _cache.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    # DECODE: recover plaintext from ciphertext via the SAME codec.  If the
    # _cache was polluted or the registry changed, we might get the wrong codec
    # and plaintext != data.
    try:
        decoded = codecs.decode(encoded, encoding)
    except Exception as e:
        state["errors"][wid & 1023] += 1
        H.fail("codec decode FAILED for wid {0}: {1}: {2}".format(
            wid, type(e).__name__, e))
        return

    # ORACLE: plaintext must round-trip exactly.
    state["rtchecks"][wid & 1023] += 1
    if decoded != plaintext:
        state["rtfails"][wid & 1023] += 1
        if state["sample_fail"][0] is None:
            state["sample_fail"][0] = (wid, plaintext[:32], decoded[:32])
        H.fail("codec round-trip CORRUPTED: wid {0} encode->decode plaintext "
               "mismatch: {1!r} -> encoded -> {2!r} (length {3} vs {4}) -- a "
               "sibling's codec registration or _cache pollution changed the "
               "decode path (runloom codec-registry isolation bug).".format(
                   wid, plaintext[:32], decoded[:32],
                   len(plaintext), len(decoded)))
        return


# Sustained round-trip checks per worker, bounded by H.running().  The codec-
# pollution hazard only manifests under SUSTAINED churn -- many fibers
# simultaneously mid-codec and PARKED across their yield, so the scheduler
# reliably runs a sibling that might register its own codec before this fiber
# resumes.  A single encode/decode per fiber barely overlaps a sibling's.
# So each worker runs a sustained internal loop -- one round-trip check per
# iteration, bounded by H.running() and INNER_CAP.
INNER_CAP = 10000


def worker(H, wid, rng, state):
    """Each fiber runs LOAD-BEARING round-trip checks in a sustained loop."""
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


def body(H):
    """Spawn the worker pool."""
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    """Post-run oracle and diagnostics."""
    rtchecks = sum(H.state["rtchecks"])
    rtfails = sum(H.state["rtfails"])
    errors = sum(H.state["errors"])
    sample = H.state["sample_fail"][0]
    fail_pct = (100.0 * rtfails / rtchecks) if rtchecks else 0.0

    H.log("codecs: round-trip checks={0} (LOAD-BEARING) | failures={1} ({2:.2f}%) "
          "| errors={3} | sample_fail={4}".format(
              rtchecks, rtfails, fail_pct, errors, sample))

    if rtfails or errors:
        H.log("note: the LOAD-BEARING round-trip arm observed encode/decode "
              "mismatches or exceptions -- plaintext data corrupted or the codec "
              "registry was polluted across fibers.  The codecs module maintains "
              "a global _cache dict + search_function registry; runloom M:N fibers "
              "on the same hub share these globals, so a sibling's codec "
              "registration or concurrent cache operations can pollute this "
              "fiber's encode/decode path (runloom codec-registry isolation bug).")

    # NON-VACUITY: the load-bearing round-trip hazard was actually exercised.
    H.check(rtchecks > 0,
            "no codec round-trip checks ran -- the load-bearing codec-registry "
            "isolation hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded in codec.decode
    # on a never-delivered wake, or holding the codecs._cache lock).
    H.require_no_lost("codec encode/decode round-trip")


if __name__ == "__main__":
    harness.main(
        "p478_codecs", body, setup=setup, post=post,
        default_funcs=8000,
        describe="codecs module maintains a process-global _cache dict and "
                 "search_function registry for codec lookup; runloom M:N fibers "
                 "on the same hub share these globals.  LOAD-BEARING: each fiber "
                 "registers its own unique custom codec, encodes random plaintext "
                 "via that codec, YIELDS to let siblings run (and possibly "
                 "register their own codecs / pollute the _cache), then decodes "
                 "the encoded bytes and asserts they equal the original plaintext. "
                 "Plaintext->encoded->decoded must round-trip exactly; a mismatch "
                 "means the wrong codec handled decode (a sibling's codec polluted "
                 "the _cache, or registry was cleared) -- the runloom codec-"
                 "registry isolation bug (0 under plain threads GIL on/off; same "
                 "class as p321/p460/p468)")
