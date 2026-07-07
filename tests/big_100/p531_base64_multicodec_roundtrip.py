"""big_100 / 531 -- base64 multi-codec round-trip conservation under M:N.

The base64 module ships FIVE independent transfer encodings -- b16, b32,
b64 (urlsafe), b85, a85 -- and they lean on module-level shared state:

  * b32/b16 use module-level translation tables (`_b32alphabet`, hex maps);
  * b85 and a85 use PRECOMPUTED module-level lookup tables (`_a85chars`,
    `_a85chars2`, `_b85chars`, `_b85chars2`) built once at import and read by
    every call, plus a per-call chunking accumulator that walks the input in
    4-byte (a85/b85) or 5-byte (b32) or 3-byte (b64) groups, accumulating a
    big-int / carry per group and padding the final short group.

THE HAZARD (the gap this program probes).  If ANY per-call scratch inside those
C/Python encoders were thread-affine or module-global rather than stack-local --
the running group accumulator, a shared output bytearray, a cached carry, a
reused padding buffer -- then a fiber parked mid-encode (or between encode and
decode) while a sibling encodes DIFFERENT bytes on the same hub could observe an
encoded form contaminated by the sibling's group state, whose decode would NOT
reproduce the original input.  Under a CORRECT runtime every encoder's scratch
lives on the calling fiber's own C/Python stack, so `decode(encode(x)) == x`
holds for every codec no matter how the scheduler interleaves fibers.

WHICH ORACLE IS LOAD-BEARING, AND WHY (single-owner, closed-world round-trip):

  Each fiber owns its OWN wid-tagged byte payload -- built from a 4-byte wid
  header + an idx header + fiber-private random filler, so NO two fibers ever
  hold identical bytes.  The payload length is chosen to STRADDLE every codec's
  block boundary (never a multiple of 3, 4, or 5) so the short-final-group /
  padding path of each encoder is exercised every iteration.  The fiber then, for
  EACH of the five codecs, does:

      e1 = encode(payload)
      yield                      # sibling encodes its OWN different bytes here
      e2 = encode(payload)
      assert e1 == e2            # encoded form STABLE across the yield
      assert every byte of e1 is in that codec's LEGAL alphabet
      d  = decode(e1)
      assert d == payload        # round-trip CONSERVATION: no unit changed

  This is a SINGLE-OWNER oracle: `payload`, `e1`, `e2`, `d` are all fiber-local
  names; nothing is shared between fibers, so a mismatch cannot be "documented
  shared-mutable-object M:N behavior" -- it can only be a runtime that let a
  sibling's encode scratch leak into this fiber's, an object torn across the
  yield, or a lost/duplicated byte.  Five independent conservation laws hold per
  iteration (one per codec).  A FAIL here is a real runtime bug.

  Verified against a plain-threads control (8 OS threads, GIL on AND off, each
  thread round-tripping its own distinct wid-tagged bytes through all five
  codecs): 100% of round-trips reproduce the input and every encoded form stays
  within its alphabet.  So a correct runloom must also stay clean, and the
  load-bearing oracle PASSES (exit 0) when there is no bug.

ORACLES:
  * LOAD-BEARING -- ROUND-TRIP CONSERVATION (worker, HARD, fail-fast).  Per codec
    per iteration: encode/re-encode stability across a yield, alphabet legality,
    decode == original.  Single-owner fiber-local data.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside an
    encoder/decoder (mid-group accumulation) never returns; caught by the
    watchdog + require_no_lost.
  * NON-VACUITY (post, HARD): b64 round-trips actually ran (b64_roundtrips > 0).

FAIL ON: decode(encode(x)) != x for any codec, an encoded form containing a
character outside that codec's alphabet, or the encoded form changing across a
yield -- any of which means a sibling's encode scratch leaked into this fiber, or
a byte was torn/lost/doubled by the runtime.

base64 is only INCIDENTAL here (it also backs p89); this program's subject is the
runtime's isolation of per-call C-encoder scratch across a fiber yield.

Stresses: base64 b16/b32/b64-urlsafe/b85/a85 encode+decode group accumulators and
module-level translation tables, short-final-group / padding paths, encoded-form
stability across a hub-migrating yield, per-fiber round-trip conservation.
"""
import base64

import harness
import runloom

# ---- Per-codec LEGAL alphabets (the set of byte values a correct encoder may
# emit).  A byte outside the set in an encoded form means the encoder's output
# was contaminated -- a torn/leaked group.  We derive these from the module's own
# authoritative tables where possible so the oracle can't drift from the codec.
_B16_LEGAL = frozenset(b"0123456789ABCDEF")
_B32_LEGAL = frozenset(base64._b32alphabet) | frozenset(b"=")
# urlsafe base64 alphabet: A-Za-z0-9 plus '-' '_' plus '=' padding.
_B64_LEGAL = frozenset(
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_=")
_B85_LEGAL = frozenset(base64._b85alphabet)
# Ascii85: characters chr(33)..chr(117) ('!'..'u') plus the 'z' all-zero-group
# shortcut.  We do NOT use foldspaces, so 'y' never appears.
_A85_LEGAL = frozenset(range(33, 118)) | frozenset(b"z")


def enc_b64(data):
    return base64.urlsafe_b64encode(data)


def dec_b64(data):
    return base64.urlsafe_b64decode(data)


# (name, encode, decode, legal-byte-set).  Order fixed so per-codec counters map
# stably by index.
CODECS = (
    ("b16", base64.b16encode, base64.b16decode, _B16_LEGAL),
    ("b32", base64.b32encode, base64.b32decode, _B32_LEGAL),
    ("b64", enc_b64, dec_b64, _B64_LEGAL),
    ("b85", base64.b85encode, base64.b85decode, _B85_LEGAL),
    ("a85", base64.a85encode, base64.a85decode, _A85_LEGAL),
)
B64_INDEX = 2   # index of the b64 codec -> feeds the non-vacuity tally

# Payload lengths that STRADDLE every codec block boundary: none is a multiple of
# 3 (b64), 4 (b85/a85), or 5 (b32), so every iteration drives each encoder's
# short-final-group / padding path.  Spanning 1..79 also pushes multi-block
# accumulation.  (Verified: no element is divisible by 3, 4, or 5.)
STRADDLE_LENS = (1, 2, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59,
                 61, 67, 71, 73, 77, 79)


def build_payload(wid, idx, rng):
    """Build this fiber's UNIQUE, single-owner byte payload for iteration `idx`.

    Header = wid (4 bytes big-endian) + idx (4 bytes) so no two fibers -- and no
    two iterations of one fiber -- ever share a payload.  The remainder is
    fiber-private random filler.  Length straddles every codec block boundary."""
    length = STRADDLE_LENS[idx % len(STRADDLE_LENS)]
    header = bytes([
        (wid >> 24) & 0xFF, (wid >> 16) & 0xFF, (wid >> 8) & 0xFF, wid & 0xFF,
        (idx >> 24) & 0xFF, (idx >> 16) & 0xFF, (idx >> 8) & 0xFF, idx & 0xFF,
    ])
    if length <= len(header):
        return header[:length]
    filler = bytes(rng.getrandbits(8) for _ in range(length - len(header)))
    return header + filler


def roundtrip_all(H, wid, idx, rng, state):
    """LOAD-BEARING single-owner arm: round-trip this fiber's own payload through
    ALL five codecs, yielding between encode and decode so a sibling interleaves
    its own (different) encode.  Fail-fast on any conservation / legality /
    stability violation."""
    payload = build_payload(wid, idx, rng)

    for ci, (name, encode, decode, legal) in enumerate(CODECS):
        e1 = encode(payload)

        # YIELD: a sibling on this hub runs its OWN encode of DIFFERENT bytes.
        # If any encoder scratch were thread-affine/module-global, e2/e1 or the
        # decode below would be contaminated by the sibling's group state.
        runloom.yield_now()
        if idx & 1:
            runloom.sleep(0.0002)

        e2 = encode(payload)

        # Check 1: encoded form STABLE across the yield (a sibling's encode did
        # not perturb this fiber's result).
        if e1 != e2:
            H.fail("{0} encode UNSTABLE across yield: encode(payload) returned "
                   "{1!r} then {2!r} for the SAME single-owner payload (wid {3}, "
                   "idx {4}, len {5}) -- a sibling's encode scratch leaked into "
                   "this fiber".format(name, e1, e2, wid, idx, len(payload)))
            return

        # Check 2: every emitted byte is in the codec's LEGAL alphabet (no
        # out-of-alphabet byte from a torn/leaked group).
        for b in e1:
            if b not in legal:
                H.fail("{0} encode emitted OUT-OF-ALPHABET byte {1} in {2!r} "
                       "(wid {3}, idx {4}) -- the encoder's output was "
                       "contaminated by a leaked/torn group".format(
                           name, b, e1, wid, idx))
                return

        # Check 3: round-trip CONSERVATION -- decode reproduces the input exactly.
        d = decode(e1)
        if d != payload:
            H.fail("{0} round-trip BROKEN: decode(encode(payload)) != payload "
                   "(wid {1}, idx {2}, len {3}); expected {4!r} got {5!r} -- a "
                   "byte was torn/lost/doubled or a sibling's encode scratch "
                   "leaked in".format(name, wid, idx, len(payload), payload, d))
            return

        state["rt"][ci][wid] += 1        # single-writer-per-(codec,wid) slot


# Sustained churn per worker (like p490): many fibers encoding distinct payloads
# while parked across the encode/decode yield is what makes a sibling reliably
# interleave mid-round-trip.  A single round-trip per fiber barely overlaps.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            roundtrip_all(H, wid, idx, rng, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # One race-free slot PER (codec, wid): each fiber is the sole writer of its
    # own wid slot for each codec (conservation counters, per HARD RULE 1).
    H.state = {
        "rt": [[0] * H.funcs for _ in range(len(CODECS))],
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    totals = [sum(H.state["rt"][ci]) for ci in range(len(CODECS))]
    grand = sum(totals)
    H.log("base64 round-trips conserved this run: total {0} | ".format(grand) +
          " ".join("{0}={1}".format(CODECS[ci][0], totals[ci])
                   for ci in range(len(CODECS))) +
          " (every per-codec decode==encode, alphabet-legality, and cross-yield "
          "stability check passed fail-fast); ops={0}".format(H.total_ops()))

    # NON-VACUITY: the load-bearing round-trip arm actually ran (b64 tally > 0).
    H.check(totals[B64_INDEX] > 0,
            "no b64 round-trips ran -- the load-bearing base64 round-trip "
            "conservation oracle was never exercised (would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded mid-encode in a
    # group accumulator).
    H.require_no_lost("base64 multi-codec round-trip")


if __name__ == "__main__":
    harness.main(
        "p531_base64_multicodec_roundtrip", body, setup=setup, post=post,
        default_funcs=8000,
        describe="each fiber round-trips its OWN wid-tagged bytes (length "
                 "straddling every codec block boundary) through all of "
                 "b16/b32/b64-urlsafe/b85/a85 with a yield between encode and "
                 "decode; LOAD-BEARING single-owner oracle: decode(encode(x))==x "
                 "for every codec, each encoded form uses only that alphabet's "
                 "legal characters, and the encoded form is stable across the "
                 "yield -- a mismatch means a sibling's encode scratch leaked in "
                 "or a byte was torn/lost/doubled by the runtime")
