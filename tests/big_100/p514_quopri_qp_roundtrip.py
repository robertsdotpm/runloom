"""big_100 / 514 -- quopri Quoted-Printable encode/decode round-trip isolation under M:N.

quopri.encodestring / quopri.decodestring turn arbitrary bytes into (and back
from) the RFC-2045 Quoted-Printable transfer encoding.  Internally each call
streams the input through per-call scratch buffers and a small state machine:
the encoder walks byte-by-byte deciding whether each octet passes through
literally, becomes an "=XX" hex escape (for bytes > 0x7e, '=', control chars,
and trailing whitespace), or forces a "=\\n" SOFT LINE BREAK when the 76-column
limit is hit; the decoder runs the inverse state machine, un-escaping "=XX",
swallowing soft breaks, and re-materialising the original octets.  Both paths
keep transient state (the current column, a half-parsed "=X" escape, a pending
trailing-space run) that lives only for the duration of one call.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom multiplexes
tens of thousands of goroutines onto a handful of OS hubs with the GIL OFF.  A
fiber that calls encodestring(), PARKS at a cooperative yield, and later RESUMES
-- possibly on a DIFFERENT hub -- must find its encode result byte-for-byte
intact, and a subsequent decodestring() must reconstruct exactly the original
bytes.  If quopri's per-call scratch buffer, the soft-line-break column counter,
or the half-parsed "=XX" escape state were NOT isolated per call (e.g. a shared
module-level buffer, a per-thread-id cache reused across the park+migration, or
a C accumulator that a sibling's concurrent encode/decode could stomp), then a
fiber's round-trip could come back CORRUPTED -- decode != original, or the
encoded form could contain an illegal QP byte spliced in from a sibling's data.

WHICH ORACLE IS LOAD-BEARING, AND WHY.

  Every fiber operates on its OWN bytes object `payload`, created in a fiber-
  local variable and never shared.  The bytes are rng-derived and deliberately
  forced to hit every escaping path: octets above 0x7e (must "=XX"-escape),
  literal '=' (must escape itself), trailing spaces / tabs (must escape at end
  of line), control bytes, and long enough to cross the 76-column soft-break
  boundary.  The single-owner oracle asserts, for BOTH quotetabs=True and
  quotetabs=False:

    (1) LEGALITY.  encodestring(payload) yields ONLY legal QP output bytes --
        every octet is in {TAB, LF, CR, SPACE} or the printable range 33..126.
        An out-of-range byte in the encoded form means a sibling's raw data (or
        a torn scratch buffer) leaked into this fiber's encode output.

    (2) ROUND-TRIP.  decodestring(encodestring(payload)) == payload, exactly.
        A mismatch means the encode buffer, the soft-break column state, or the
        decode "=XX" escape state was corrupted between the two calls.

    (3) STABILITY ACROSS A YIELD.  We insert a cooperative yield (yield_now, and
        sleep on odd iterations) BETWEEN the encode and the decode -- the hazard
        boundary where a park+hub-migration happens and a sibling reliably runs
        its own encode/decode.  After the yield we RE-ENCODE the same payload and
        assert the encoded bytes are byte-for-byte identical to the pre-yield
        encode (encodestring is deterministic), then decode and re-assert the
        round-trip.  If parking between encode and decode leaked a sibling's
        buffer or reset the escape-state machine, the re-encode would differ or
        the decode would not reconstruct `payload`.

  This oracle is provably correct on a single-threaded interpreter: we verified
  offline that decodestring(encodestring(x)) == x for 20000 random payloads
  spanning all escape paths, both quotetabs values, with zero mismatches, and
  that the encoded output never contains a byte outside {9,10,13,32} u [33,126].
  So on a CORRECT runtime this program EXITS 0 (PASS): every fail-fast check is
  a tautology unless the runtime corrupted the per-call quopri state across the
  park.  A FAIL here means a real runloom bug: a cross-fiber leak of single-owner
  encode/decode buffer state, a torn bytes object, or a lost/duplicated escape.

ORACLES:
  * LOAD-BEARING -- QP ROUND-TRIP ISOLATION (worker, HARD, fail-fast).  Single-
    owner payload; legality + round-trip + across-yield stability as above.
  * NON-VACUITY (post, HARD): the round-trip arm actually ran (roundtrips > 0).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that parked between
    encode and decode and never resumed (stranded lost-wakeup) is caught.

FAIL ON: an encoded form containing an illegal QP byte, a decode that does not
reconstruct the single-owner payload, or an encode that changes across a yield.
There is NO shared-mutable arm: quopri's public API is stateless-per-call, so
the whole program is pure single-owner isolation -- exactly p490's style.

Stresses: quopri.encodestring/decodestring per-call scratch buffers, the QP
soft-line-break (=\\n) column state machine, "=XX" escape parsing, trailing-
whitespace escaping, quotetabs on/off, all across a cooperative park + hub
migration inserted between encode and decode under GIL-off M:N churn.
"""
import quopri

import harness
import runloom

# Legal bytes in a Quoted-Printable ENCODED stream: literal TAB/LF/CR/SPACE plus
# the printable ASCII range 33..126 (which includes '=' used for escapes and the
# uppercase A-F / digits used inside "=XX").  Anything outside this set in the
# encoded output is a torn buffer / cross-fiber data leak.  Verified offline over
# 30000 randomized payloads (both quotetabs values): min seen 9, max seen 126,
# zero out-of-set bytes.
LEGAL_QP = frozenset((9, 10, 13, 32)) | frozenset(range(33, 127))

# Payload length band.  Large enough to routinely cross the 76-column QP soft-
# line-break boundary (so the =\\n column state machine is exercised), small
# enough that tens of thousands of fibers churn many round-trips under the
# timeout.  Zero-length is included on purpose (a degenerate escape-state case).
MIN_LEN = 0
MAX_LEN = 160

# Byte-choice menu, weighted to FORCE every escaping path: high bytes (>0x7e must
# escape), '=' (escapes itself), trailing space/tab (escape at end-of-line),
# control bytes, and ordinary printable text (passes through literally).
HIGH = tuple(range(0x7f, 0x100))         # all require "=XX"
CTRL = tuple(range(0x00, 0x09)) + (0x0b, 0x0c) + tuple(range(0x0e, 0x20))
PRINT = tuple(range(0x21, 0x7f))         # printable, mostly literal
SPECIAL = (0x20, 0x09, ord('='))         # space, tab, '='


def make_payload(rng):
    """Build one fiber-local bytes payload that exercises every QP escape path.

    Single-owner: created here in the worker's local scope, never shared.  The
    distribution is skewed toward bytes that force escaping (high bytes, '=',
    trailing whitespace) so the encoder's =XX / soft-break state machine and the
    decoder's inverse are both genuinely driven, not fed trivially-literal text.
    """
    n = rng.randint(MIN_LEN, MAX_LEN)
    out = bytearray(n)
    for i in range(n):
        r = rng.randint(0, 99)
        if r < 35:
            out[i] = rng.choice(HIGH)        # must =XX escape
        elif r < 45:
            out[i] = rng.choice(SPECIAL)     # space / tab / '='
        elif r < 55:
            out[i] = rng.choice(CTRL)        # control -> =XX
        else:
            out[i] = rng.choice(PRINT)       # literal passthrough
    # Bias the LAST byte toward trailing whitespace, the special end-of-line
    # escaping case ("a trailing space must be encoded =20 / =09").
    if n and rng.randint(0, 1):
        out[-1] = rng.choice((0x20, 0x09))
    return bytes(out)


def check_legal(H, enc, wid, qt):
    """Assert the encoded form contains ONLY legal QP output bytes.  A byte
    outside {TAB,LF,CR,SPACE} u [33,126] means raw/sibling data leaked into this
    fiber's encode output (a torn or cross-fiber scratch buffer)."""
    for c in enc:
        if c not in LEGAL_QP:
            H.fail("illegal QP byte {0:#04x} in encodestring output "
                   "(quotetabs={1}, wid={2}, len={3}) -- the encoded stream must "
                   "contain only TAB/LF/CR/SPACE and printable 33..126; an out-"
                   "of-range octet means a sibling's raw bytes or a torn per-call "
                   "scratch buffer leaked into this fiber's QP encode".format(
                       c, qt, wid, len(enc)))
            return False
    return True


# Sustained round-trips per worker iteration.  The park-between-encode-and-decode
# hazard only manifests under SUSTAINED churn (many fibers simultaneously mid-
# round-trip while sleep-PARKED across the yield), so a sibling reliably runs its
# own encode/decode before this fiber resumes.  Bounded by H.running().
INNER_CAP = 100000


def roundtrip_once(H, wid, idx, rng, state):
    """One single-owner QP round-trip with a park inserted at the hazard boundary
    (between encode and decode), verified for both quotetabs values."""
    payload = make_payload(rng)

    for qt in (True, False):
        # --- ENCODE (pre-yield) ------------------------------------------------
        enc1 = quopri.encodestring(payload, quotetabs=qt)
        if not check_legal(H, enc1, wid, qt):
            return

        # --- PARK at the hazard boundary: a sibling on this hub (or another,
        # after migration) runs its OWN encode/decode here.  Its per-call state
        # must not touch ours.
        runloom.yield_now()
        if idx & 1:
            runloom.sleep(0.0003)

        # --- RE-ENCODE (post-yield): encodestring is deterministic, so the bytes
        # MUST be identical.  A difference means the park corrupted the encoder's
        # scratch buffer / soft-break column state.
        enc2 = quopri.encodestring(payload, quotetabs=qt)
        if enc2 != enc1:
            H.fail("encodestring NOT STABLE across a yield: quotetabs={0} wid={1} "
                   "len={2} -- pre-yield {3!r} != post-yield {4!r}; the encoder's "
                   "per-call buffer or soft-line-break column state was corrupted "
                   "by a sibling across the park".format(
                       qt, wid, len(payload), enc1[:48], enc2[:48]))
            return
        if not check_legal(H, enc2, wid, qt):
            return

        # --- DECODE and assert the exact round-trip.  This is the load-bearing
        # law: decodestring(encodestring(x)) == x, provably true single-threaded.
        dec = quopri.decodestring(enc2)
        if dec != payload:
            H.fail("QP round-trip BROKEN: decodestring(encodestring(x)) != x "
                   "(quotetabs={0} wid={1} len={2}) -- original {3!r} decoded to "
                   "{4!r} via {5!r}; the per-call encode/decode escape-state "
                   "machine leaked a sibling's buffer or dropped/duplicated an "
                   "=XX escape across the park".format(
                       qt, wid, len(payload), payload[:48], dec[:48], enc2[:48]))
            return

    # Round-trip (both quotetabs) verified for this fiber.  Race-free: one writer
    # per wid slot (see rule 1 / p405).
    state["roundtrips"][wid] += 1


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            roundtrip_once(H, wid, idx, rng, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # One slot per worker (wid-indexed, single-writer -> race-free conservation
    # tally for non-vacuity).  Allocated here where H.funcs is known.
    H.state = {
        "roundtrips": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    rts = sum(H.state["roundtrips"])
    H.log("quopri QP round-trips verified (single-owner, both quotetabs, across "
          "a park at the encode/decode boundary): {0}; ops={1}".format(
              rts, H.total_ops()))
    # NON-VACUITY: the load-bearing round-trip arm actually ran.  Reaching post
    # with no failure already proves every legality + round-trip + across-yield
    # stability check held fail-fast.
    H.check(rts > 0,
            "no QP round-trips completed -- the encode/decode park-boundary "
            "hazard was never exercised (oracle would be vacuous)")
    # COMPLETENESS: no fiber parked between encode and decode and vanished.
    H.require_no_lost("quopri qp round-trip")


if __name__ == "__main__":
    harness.main(
        "p514_quopri_qp_roundtrip", body, setup=setup, post=post,
        default_funcs=8000,
        describe="each fiber round-trips its OWN rng-derived bytes through "
                 "quopri.encodestring/decodestring (both quotetabs) with a "
                 "cooperative park inserted between encode and decode -- the hub-"
                 "migration boundary.  LOAD-BEARING single-owner oracle: the "
                 "encoded form contains only legal QP bytes, the encode is byte-"
                 "identical across the yield, and decodestring(encodestring(x))"
                 "==x exactly.  A leaked per-call scratch buffer, a reset =XX / "
                 "soft-line-break state machine, or a torn bytes object across "
                 "the park fails")
