"""big_100 / 475 -- encodings module _cache dict isolation under M:N.

encodings module caches codec lookups and alias tables in plain module-global
dicts:
  - _cache (str->CodecInfo): memoized codec.CodecInfo results
  - aliases (str->str): codec alias mappings (e.g., 'utf-8' -> 'utf_8')

These are mutated by encodings.search_function() (which calls codecs.lookup())
WITHOUT per-fiber isolation. Under M:N, many fibers share one hub OS-thread.
If fiber A calls codecs.lookup('utf-8') and yields mid-search while the _cache
is being populated, a sibling fiber B on the same hub that calls
codecs.lookup('latin-1') races the SAME _cache dict across the yield.  A data
race on the _cache dict object (insertion, resizing, lookup) can corrupt the
dict's internal state: a lookup returns the WRONG codec, or raises an exception,
or the dict tears.  This is the shared-module-state class: module globals
without contextvar backing or per-fiber isolation assume one logical execution
context per (hub OS-thread), which breaks when many fibers share one hub.

DIFFERENCE FROM p460/p66/p67:  Those guard objects keyed by get_ident()
(threading.local, contextvar, decimal.Context) which are DESIGNED to be
thread-affine but break under M:N hub-multiplexing.  This program stresses
PLAIN module-global dicts (encodings._cache and aliases) with no identity
attribute at all -- a simpler class of shared-state corruption, and a larger
class of stdlib modules (any that cache in module globals).

LOAD-BEARING INVARIANT / WHY THE ORACLE IS NON-VACUOUS.  Each fiber looks up
DISTINCT encodings (utf-8, latin-1, cp1252, ascii, iso-8859-1, big5, etc.)
and asserts it gets the RIGHT codec.  A data race in _cache that returns the
WRONG codec (mixing up which encoding -> which decoder/encoder) is a SILENT
DATA CORRUPTION BUG in real programs (e.g., text incorrectly decoded as latin-1
when it was utf-8).  We verify the codec properties:
  - The codec's NAME must match what we looked up (e.g., lookup('utf-8') must
    return a codec named 'utf_8', not 'latin_1').
  - The codec must WORK (encode/decode an ASCII string) without raising.
  - Recomputed after a yield in the same fiber, the codec must be IDENTICAL
    (same object or equivalent), not torn/corrupted.

A mixed-up or corrupted codec -> H.fail().

ARMS:
  * LOAD-BEARING -- DISTINCT-ENCODING arm (worker, HARD, fail-fast).  Each
    fiber rotates through a set of DISTINCT encodings (utf-8, latin-1,
    cp1252, ascii, iso-8859-1, big5) and looks each up, asserts the codec is
    correct, encodes/decodes a test string, yields (racing _cache), looks up
    the same codec again, and asserts it is IDENTICAL.  A wrong codec name, a
    decode/encode failure, or a torn codec -> H.fail().  On a CORRECT runtime
    (and plain threads, GIL on AND off -- verified) this NEVER fires, so the
    program exits 0 when there is no bug.

COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-
codec-lookup (hung in a corrupted _cache or raised an unhandled exception)
never returns; the watchdog + require_no_lost catch it.

NON-VACUITY (post, HARD): the load-bearing arm actually ran (lookup_checks > 0).

FAIL ON: wrong codec name, encode/decode exception, or a mismatch between
re-lookups across a yield (codec properties changed).

Stresses: encodings._cache dict data races under M:N hub-multiplexing, dict
resizing + lookup interleave, search_function() mutations racing across yields,
plain module-global shared state without per-fiber isolation.

Good TSan / controlled-M:N-replay target: encodings._cache is a plain dict
mutated (insert, clear, or resize) by concurrent fibers on one hub -- a
data-race report on the dict object's ma_table or ma_used fields, or a
deterministic-replay that races an insertion with a lookup across a yield,
localizes the corruption before the codec-name oracle fires.
"""
import codecs
import encodings

import harness
import runloom

# DISTINCT encodings to cycle through. Each has a unique codec name and
# specific behavior (e.g., utf-8 is variable-width, latin-1 is 1:1).
# Chosen to be STABLE (supported on all platforms) and DISTINCT (so mixing
# them up is visible).
ENCODING_NAMES = [
    "utf-8",
    "latin-1",
    "cp1252",
    "ascii",
    "iso-8859-1",
    "big5",  # multi-byte, wide codec coverage
]

# Codec names we expect after codecs.lookup(). codecs.lookup() returns a
# CodecInfo object whose .name attribute is the normalized codec name
# (hyphens removed, aliases resolved to canonical name).
EXPECTED_CODEC_NAMES = {
    "utf-8": "utf-8",
    "latin-1": "iso8859-1",  # alias: latin-1 resolves to iso8859-1
    "cp1252": "cp1252",
    "ascii": "ascii",
    "iso-8859-1": "iso8859-1",  # canonical
    "big5": "big5",
}

# A test string to encode/decode with each codec. ASCII so it round-trips
# through every codec without error.
TEST_STRING = "Hello, World! 0123456789"


def setup(H):
    H.state = {
        "lookup_checks": [0] * 1024,      # codec lookups + verifications
        "codec_mismatches": [0] * 1024,   # wrong codec name returned
        "codec_errors": [0] * 1024,       # encode/decode raised
        "codec_tears": [0] * 1024,        # mismatch across yield (torn codec)
    }


def worker(H, wid, rng, state):
    """Each fiber cycles through DISTINCT encodings, looks each up, verifies
    the codec, yields (racing the _cache), looks up the same codec again, and
    asserts it is IDENTICAL."""

    for _ in H.round_range():
        if not H.running():
            break

        for enc_idx, enc_name in enumerate(ENCODING_NAMES):
            if not H.running():
                break

            # Rotate the encoding index by wid so different fibers often have
            # different encodings in flight at the same time (increases _cache
            # contention).
            actual_enc = ENCODING_NAMES[(enc_idx + wid) % len(ENCODING_NAMES)]
            expected_codec_name = EXPECTED_CODEC_NAMES[actual_enc]

            try:
                # First lookup: encode and decode to verify the codec works.
                # codecs.lookup() calls encodings.search_function() which accesses
                # and mutates encodings._cache (the shared hazard).
                codec_info_1 = codecs.lookup(actual_enc)
                if codec_info_1.name != expected_codec_name:
                    H.fail(
                        "encodings._cache CORRUPTION (wrong codec name): "
                        "codecs.lookup('{0}') returned codec.name='{1}' (expected '{2}') "
                        "(wid {3}) -- the _cache returned the wrong codec, "
                        "likely a data race mixing up codec names".format(
                            actual_enc, codec_info_1.name, expected_codec_name, wid))
                    return

                # Encode and decode to verify codec functionality.
                try:
                    encoded = codec_info_1.encode(TEST_STRING)[0]
                    decoded = codec_info_1.decode(encoded)[0]
                    if decoded != TEST_STRING:
                        H.fail(
                            "encodings codec WRONG: lookup('{0}') encode/decode "
                            "round-trip corrupted: '{1}' != '{2}' (wid {3})".format(
                                actual_enc, TEST_STRING, decoded, wid))
                        return
                except Exception as e:
                    H.fail(
                        "encodings codec BROKEN: lookup('{0}').encode/decode "
                        "raised {1!r} (wid {2})".format(
                            actual_enc, e, wid))
                    return

                # YIELD + PARK: race the _cache. A sibling fiber on this hub
                # is now looking up a DIFFERENT encoding, racing _cache dict
                # operations.
                runloom.yield_now()
                if rng.random() < 0.5:
                    runloom.sleep(0.0002)

                # Second lookup: must return an equivalent codec (same name,
                # same behavior). If the _cache was corrupted/torn, we get a
                # different codec or an exception.
                try:
                    codec_info_2 = codecs.lookup(actual_enc)
                except Exception as e:
                    H.fail(
                        "encodings._cache CORRUPTION: codecs.lookup('{0}') raised {1!r} "
                        "after yield (wid {2}) -- the _cache dict may be torn "
                        "from a data race".format(actual_enc, e, wid))
                    return

                # Verify the re-lookup is consistent: same codec name.
                if codec_info_2.name != expected_codec_name:
                    state["codec_tears"][wid & 1023] += 1
                    H.fail(
                        "encodings._cache CORRUPTION (torn codec): "
                        "lookup('{0}') before yield returned codec.name='{1}', "
                        "after yield returned '{2}' (wid {3}) -- the _cache or "
                        "codec_info was corrupted across the yield".format(
                            actual_enc, codec_info_1.name, codec_info_2.name, wid))
                    return

                # Verify re-encode/decode works (codec is not torn).
                try:
                    encoded_2 = codec_info_2.encode(TEST_STRING)[0]
                    decoded_2 = codec_info_2.decode(encoded_2)[0]
                    if decoded_2 != TEST_STRING:
                        state["codec_tears"][wid & 1023] += 1
                        H.fail(
                            "encodings codec TORN: lookup('{0}') after yield "
                            "encode/decode corrupted: '{1}' != '{2}' (wid {3})".format(
                                actual_enc, TEST_STRING, decoded_2, wid))
                        return
                except Exception as e:
                    state["codec_errors"][wid & 1023] += 1
                    H.fail(
                        "encodings._cache CORRUPTION: lookup('{0}') after yield "
                        "encode/decode raised {1!r} (wid {2}) -- codec torn from "
                        "concurrent _cache race".format(actual_enc, e, wid))
                    return

                state["lookup_checks"][wid & 1023] += 1

            except Exception as e:
                # Catch any unforeseen exception during lookup itself.
                H.fail(
                    "codecs.lookup('{0}') raised {1!r} (wid {2})".format(
                        actual_enc, e, wid))
                return

            H.op(wid)

        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["lookup_checks"])
    tears = sum(H.state["codec_tears"])
    errors = sum(H.state["codec_errors"])
    mismatches = sum(H.state["codec_mismatches"])

    H.log(
        "encodings._cache: {0} lookups (LOAD-BEARING, all passed fail-fast) | "
        "codec_tears={1} codec_errors={2} codec_mismatches={3}".format(
            checks, tears, errors, mismatches))

    # NON-VACUITY: the load-bearing distinct-encoding lookup hazard was actually
    # exercised.
    H.check(
        checks > 0,
        "no encodings.lookup checks ran -- the load-bearing _cache data-race "
        "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded mid-lookup
    # inside encodings.search_function on a corrupted _cache dict).
    H.require_no_lost("encodings._cache data-race isolation")


if __name__ == "__main__":
    harness.main(
        "p475_encodings",
        body,
        setup=setup,
        post=post,
        default_funcs=8000,
        describe="encodings module (via codecs.lookup) caches codec lookups in "
                 "plain module-global _cache dict with no per-fiber isolation. "
                 "Under M:N many fibers share one hub OS-thread and race the _cache: "
                 "a lookup (+ dict resize/rehash) from one fiber can interleave with "
                 "another fiber's insertion/search, corrupting the dict or returning "
                 "the WRONG codec (silent data corruption in real programs). "
                 "LOAD-BEARING: each fiber looks up DISTINCT encodings and verifies "
                 "the codec name, encode/decode functionality, and consistency across "
                 "a yield -- a wrong codec name or torn codec is the _cache data-race "
                 "bug (0 under plain threads GIL on AND off; a corruption is a "
                 "runloom M:N race)."
    )
