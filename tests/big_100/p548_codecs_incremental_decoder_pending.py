"""big_100 / 548 -- codecs IncrementalDecoder pending-buffer isolation under M:N.

codecs.getincrementaldecoder(enc)() returns a STATEFUL decoder object: when it is
fed a chunk that ends in the MIDDLE of a multibyte sequence (a half-delivered
UTF-8 3-byte char, a lone UTF-16 surrogate high half, three of a UTF-32 code
unit's four bytes), it CANNOT emit that character yet, so it stashes the partial
bytes in a PER-INSTANCE pending buffer and returns them on the NEXT feed once the
rest of the sequence arrives.  The whole point of the incremental API is that this
pending buffer survives ACROSS feed() calls -- and in a blocking-style fiber, a
feed() call is exactly where the fiber may PARK (we yield between feeds).  So the
pending partial-multibyte state has to survive a hub migration and a sibling's
decoder running in between.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom gives each fiber
its own Python frame stack, but the IncrementalDecoder's pending buffer lives in
the decoder OBJECT's C-level state (utf-8's `pendingbytes`/`pendingsize`, the
BufferedIncrementalDecoder's `buffer`, the utf-16/32 endianness+BOM latch).  If
that per-instance buffer were somehow exposed, shared, or clobbered when the
owning fiber parks between feeds and a sibling's incremental decoder runs on the
same hub, the reassembled text would TEAR at the multibyte boundary: the resumed
feed would prepend the WRONG pending bytes, yielding a replacement char (U+FFFD),
a mojibake code point, or a length/content mismatch versus the original string.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain CPython):

  Feed the encoded bytes of a known string to a SINGLE-OWNER IncrementalDecoder
  ONE BYTE AT A TIME with decode(chunk, final=False), yielding between feeds, and
  the concatenation of every partial result plus a final decode(b'', True) MUST
  EXACTLY equal the original string, with NO replacement char (U+FFFD) ever
  appearing at an intermediate step (the original never contained one).  We
  verified in a standalone control -- utf-8/utf-16/utf-32, feeding byte-by-byte so
  EVERY multibyte char is cut and the pending buffer is non-empty across the yield
  -- that a correctly isolated decoder reassembles the text with 0 replacement
  chars and byte-exact equality.  Under a CORRECT runloom this must also hold: the
  decoder's pending partial-sequence buffer is single-owner state that must survive
  the park unchanged.  If the reassembled text differs from the original, or a
  U+FFFD appears, that is a pending-buffer isolation desync -- the single-owner
  load-bearing oracle PASSES on a correct runtime (program exits 0 when no bug).

ORACLES:
  * LOAD-BEARING -- INCREMENTAL DECODE ISOLATION (worker, HARD, fail-fast).  Each
    fiber owns its OWN IncrementalEncoder + IncrementalDecoder (a fresh pair per
    iteration, round-robined across utf-8 / utf-16 / utf-32).  It builds a
    wid+idx-TAGGED string dense in multibyte chars (accented Latin, CJK, astral
    emoji), encodes it, then feeds the bytes byte-by-byte with final=False,
    runloom.yield_now() between feeds so a sibling reliably interleaves while this
    decoder's pending buffer holds a partial sequence.  After the final
    decode(b'', True) it asserts:
      - the reassembled text EXACTLY equals the original (no tear/mojibake/leak);
      - no intermediate feed emitted a U+FFFD replacement char (the original has
        none, so one appearing means a partial sequence was mis-reassembled);
      - the decoder was fully drained (no residual pending bytes: a final feed of
        b'' with final=True must return '').
    Single-owner: the encoder/decoder pair and the string are fiber-local, never
    shared.  A failure is a runloom pending-buffer isolation desync.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-feed
    (stranded inside decode() on a desynced pending buffer) never returns; the
    watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran
    (incremental_checks > 0) AND the feeds genuinely cut multibyte sequences
    (partial_boundaries > 0), so the pending buffer was really exercised (else the
    oracle is vacuous -- e.g. an ASCII-only string never fills the pending buffer).

FAIL ON: a reassembled string that differs from the fiber's own original, an
intermediate U+FFFD, or residual pending bytes after final=True -- each a real
runtime corruption of the single-owner decoder's pending partial-multibyte state
across a park.  There is NO shared decoder anywhere (sharing an IncrementalDecoder
across fibers would tear EXACTLY like sharing it across threads -- documented
Python behavior, not a runloom bug), so this program keeps the oracle strictly
single-owner and never mislabels shared-object semantics as a fault.

Deepens codecs on the INCREMENTAL path (p475/p478 cover only the registry/_cache);
the pending partial-sequence buffer surviving a park is the isolation subject.

Stresses: codecs.getincrementaldecoder / getincrementalencoder per-instance
pending-buffer state, utf-8 pendingbytes / utf-16 surrogate-pair latch / utf-32
4-byte code-unit reassembly across feed() calls, BOM detection on the first feed,
decode(final=False) partial-sequence buffering across hub migration + yield, and
final decode(b'', True) drain under M:N concurrency.
"""
import codecs

import harness
import runloom

# A pool of multibyte code points spanning the encoding difficulty classes so that
# byte-by-byte feeding cuts EVERY width of multibyte sequence:
#   * 2-byte UTF-8 / BMP    : accented Latin (U+00E9 etc.)
#   * 3-byte UTF-8 / BMP    : CJK (U+4E16 etc.)
#   * 4-byte UTF-8 / astral : emoji (surrogate pair in UTF-16, needs all 4 UTF-32
#     bytes) -- the hardest reassembly, and the one that tears loudest on a
#     pending-buffer desync.
MULTIBYTE_POOL = (
    "éüñå"          # é ü ñ å  -- 2-byte UTF-8
    "世界文字"          # 世 界 文 字 -- 3-byte UTF-8
    "\U0001f600\U0001f30d\U0001f680"    # 😀 🌍 🚀 -- 4-byte UTF-8 / astral
    "ΓΩЖ"                # Γ Ω Ж  -- 2-byte UTF-8, other scripts
)

# Round-robined encodings.  All three are self-synchronising incremental codecs
# with a genuine pending buffer: utf-8 stashes partial 2/3/4-byte sequences;
# utf-16/utf-32 emit+consume a BOM on the first feed and reassemble 2/4-byte code
# units, latching endianness -- so a byte-split lands mid-code-unit for all three.
ENCODINGS = ("utf-8", "utf-16", "utf-32")

# Replacement char the decoder emits when it gives up on a malformed / mis-
# reassembled partial sequence.  Our inputs are always well-formed, so ANY U+FFFD
# at an intermediate feed means the pending buffer was corrupted across a park.
REPLACEMENT = "�"

# Sustained checks per worker, bounded by H.running().  The pending-buffer hazard
# only manifests under SUSTAINED churn -- many fibers simultaneously mid-feed with
# a non-empty pending buffer while parked across a yield, so the scheduler reliably
# runs a sibling's decoder before this fiber resumes.  A single feed per fiber
# barely overlaps and does NOT reproduce.
INNER_CAP = 100000


def build_tagged_string(wid, idx):
    """Build a wid+idx-TAGGED string dense in multibyte chars.

    The tag (wid, idx digits) makes a cross-fiber pending-buffer leak visible as a
    content mismatch: if this fiber's reassembly picked up a sibling's pending
    bytes, the resulting text would not match THIS fiber's original.  The string is
    kept short so byte-by-byte feeding with a yield per byte stays cheap under load
    while still cutting many multibyte sequences."""
    # Interleave ASCII tag digits with multibyte chars so partial sequences are
    # frequent but the tag survives to prove identity.
    n = len(MULTIBYTE_POOL)
    parts = []
    tag = "{0}.{1}".format(wid, idx)
    for i, ch in enumerate(tag):
        parts.append(ch)
        parts.append(MULTIBYTE_POOL[(wid + idx + i) % n])
    return "".join(parts)


def incremental_check(H, wid, idx, state):
    """Single-owner incremental encode/decode isolation check.

    Owns a fresh encoder/decoder pair (round-robined encoding), encodes a tagged
    string, feeds the bytes BYTE-BY-BYTE with final=False and a yield between
    feeds, and asserts byte-exact reassembly with no replacement char and full
    drain.  A pending-buffer desync across the park would tear the multibyte
    boundary."""
    enc = ENCODINGS[(wid + idx) % len(ENCODINGS)]
    original = build_tagged_string(wid, idx)

    encoder = codecs.getincrementalencoder(enc)()
    decoder = codecs.getincrementaldecoder(enc)()

    # Encode the whole string in one shot via the incremental encoder + final flush
    # (single-owner encoder; the interesting pending state is on the DECODE side).
    payload = encoder.encode(original, False) + encoder.encode("", True)

    nbytes = len(payload)
    pieces = []
    partial_boundaries = 0
    for i in range(nbytes):
        chunk = payload[i:i + 1]
        final = False
        out = decoder.decode(chunk, final)
        if out == "":
            # The decoder held this byte in its pending buffer -- i.e. we cut a
            # multibyte sequence and the partial state must survive the yield.
            partial_boundaries += 1
        elif REPLACEMENT in out:
            H.fail("incremental decode emitted U+FFFD at byte {0}/{1} (enc {2}, "
                   "wid {3}, idx {4}) -- a well-formed partial multibyte sequence "
                   "was mis-reassembled, the decoder's pending buffer was corrupted "
                   "across a park".format(i, nbytes, enc, wid, idx))
            return
        pieces.append(out)
        # PARK between feeds while the pending buffer may hold a partial sequence,
        # so a sibling's decoder reliably interleaves before we resume.
        runloom.yield_now()

    # Final drain: a correctly isolated decoder has nothing left to emit, or emits
    # the last completed char.  It must NOT leave residual pending bytes.
    tail = decoder.decode(b"", True)
    if REPLACEMENT in tail:
        H.fail("incremental decode emitted U+FFFD at final drain (enc {0}, wid "
               "{1}, idx {2}) -- residual partial sequence in the pending buffer "
               "after byte-by-byte feed, a pending-buffer desync".format(
                   enc, wid, idx))
        return
    pieces.append(tail)

    reassembled = "".join(pieces)
    if reassembled != original:
        H.fail("incremental decode TORE the string (enc {0}, wid {1}, idx {2}): "
               "reassembled {3!r} != original {4!r} -- the decoder's pending "
               "partial-multibyte buffer was clobbered or leaked across a park "
               "(len got {5} vs want {6})".format(
                   enc, wid, idx, reassembled, original,
                   len(reassembled), len(original)))
        return

    # A second final drain must yield '' -- the decoder is fully empty (no lingering
    # pending bytes that a desync would surface later).
    residue = decoder.decode(b"", True)
    if residue != "":
        H.fail("incremental decoder NOT DRAINED (enc {0}, wid {1}, idx {2}): a "
               "second final decode returned {3!r} -- residual pending bytes "
               "survived, a pending-buffer desync".format(enc, wid, idx, residue))
        return

    state["incremental_checks"][wid & 1023] += 1
    if partial_boundaries:
        state["partial_boundaries"][wid & 1023] += partial_boundaries


def worker(H, wid, rng, state):
    """Each fiber runs the LOAD-BEARING single-owner incremental encode/decode
    isolation check in a sustained inner loop, parking (yield) between every byte
    feed while its pending buffer may hold a partial multibyte sequence."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            incremental_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        # LOAD-BEARING single-owner checks (non-vacuity tally; sharded wid & 1023
        # is fine here -- this feeds a > 0 non-vacuity assertion, NOT a conservation
        # law, so aliased increments cannot cause a false pass).
        "incremental_checks": [0] * 1024,
        # Count of feeds that landed mid-multibyte-sequence (pending buffer non-
        # empty across the yield); proves the hazard was really exercised.
        "partial_boundaries": [0] * 1024,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    ichecks = sum(H.state["incremental_checks"])
    boundaries = sum(H.state["partial_boundaries"])
    H.log("codecs[single-owner LOAD-BEARING]: {0} incremental encode/decode "
          "isolation checks (all byte-exact, 0 U+FFFD, fully drained) across {1} "
          "partial-multibyte feed boundaries (pending buffer non-empty across the "
          "park)".format(ichecks, boundaries))

    # NON-VACUITY: the load-bearing arm actually ran.
    H.check(ichecks > 0,
            "no single-owner incremental decode checks ran -- the load-bearing "
            "pending-buffer isolation hazard was never exercised (oracle vacuous)")

    # NON-VACUITY (hazard reality): the feeds genuinely cut multibyte sequences, so
    # the pending buffer was really held across a park.  If this were 0, we'd be
    # feeding whole chars per byte (impossible for our multibyte pool) -- guard it.
    H.check(boundaries > 0,
            "no feed ever landed mid-multibyte-sequence -- the pending buffer was "
            "never exercised across a park (oracle would not test the hazard)")

    # COMPLETENESS: no fiber parked-then-vanished inside decode() on a pending
    # buffer.
    H.require_no_lost("codecs incremental decoder pending-buffer isolation")


if __name__ == "__main__":
    harness.main(
        "p548_codecs_incremental_decoder_pending", body, setup=setup, post=post,
        default_funcs=8000,
        describe="codecs.getincrementaldecoder(enc)() buffers PARTIAL multibyte "
                 "sequences between feed() calls inside per-instance pending state; "
                 "a fiber parks (yields) between feeds while that buffer holds a "
                 "half-delivered char.  LOAD-BEARING: each fiber owns its own "
                 "IncrementalEncoder+Decoder (utf-8/16/32 round-robin), encodes a "
                 "wid-tagged multibyte-dense string, feeds the bytes byte-by-byte "
                 "with final=False and a yield between feeds so a sibling's decoder "
                 "interleaves while the pending buffer is non-empty, then asserts "
                 "byte-exact reassembly, no intermediate U+FFFD, and full drain.  A "
                 "reassembled string that differs from the fiber's own original, a "
                 "replacement char, or residual pending bytes is a pending-buffer "
                 "isolation desync in runloom")
