"""big_100 / 513 -- binascii chained-CRC + hex/base64 roundtrip conservation under M:N.

binascii is a thin C wrapper over a handful of stateless-LOOKING routines that
actually lean on process-global scratch:

  * binascii.crc32(data, value) folds `data` into the running CRC seed `value`
    using a shared 256-entry lookup table.  The documented CONTRACT is that CRC
    chaining is associative over concatenation:
        crc32(a + b) == crc32(b, crc32(a))
    i.e. you may thread ONE running CRC scalar through many crc32 calls, chunk by
    chunk, and get the exact same digest as the one-shot over the whole buffer.
  * b2a_hex / a2b_hex and b2a_base64 / a2b_base64 convert through a small C
    conversion buffer / table.  a2b_hex(b2a_hex(x)) == x and
    a2b_base64(b2a_base64(x)) == x are exact roundtrip identities.

WHERE M:N COULD BREAK IT (the hazard this program probes).  The CRC lookup table
and the codec scratch buffer are meant to be read-only / stack-local per call.
But if any of that scratch is thread-affine (a per-OS-thread static buffer, a
cached module-level bytearray, a table lazily filled once), then under runloom a
fiber that PARKS (yields) in the MIDDLE of a chained-CRC computation and is
resumed after a SIBLING ran crc32/hexlify on a DIFFERENT hub could have its
running digest corrupted -- the sibling would have clobbered a buffer the first
fiber's C routine assumed was private.  The visible symptom: the chained CRC no
longer equals the one-shot CRC, or a codec roundtrip stops being the identity.

WHICH ORACLE IS LOAD-BEARING, AND WHY.

  Each fiber owns a PRIVATE, wid-tagged byte stream (built in a fiber-local
  variable, never shared).  It:

    1. Computes the one-shot digest  expected = binascii.crc32(whole)  up front.
    2. Re-derives the SAME digest incrementally: it walks `whole` in random-sized
       chunks, threading a SINGLE-OWNER scalar `crc` through
       crc = binascii.crc32(chunk, crc), and YIELDS (runloom.yield_now) after
       every chunk so a sibling reliably interleaves its own crc32/codec work on
       another hub while this fiber's chain is half-built.
    3. Asserts the chained digest equals the one-shot digest.  `crc` is a plain
       int owned by exactly ONE fiber; the ONLY way chained != one-shot is that a
       sibling's crc32 call corrupted C scratch this fiber's chain depended on --
       a real torn-C-scratch runtime bug, NEVER a shared-container race (there is
       no shared container -- `crc`, `whole`, and every chunk are fiber-local).
    4. Roundtrips the same private buffer through hex and base64 with a yield
       INSIDE each roundtrip (between encode and decode), asserting
       a2b_hex(b2a_hex(whole)) == whole and a2b_base64(b2a_base64(whole)) == whole.

  Because the CRC-chaining law is a pure mathematical identity (associativity of
  CRC32 over concatenation) and the operands are single-owner, the load-bearing
  oracle PASSES on a correct runtime (program exits 0 when there is no bug).  A
  mismatch is a genuine desync of binascii's C state across a runloom park/resume.

ORACLES:
  * LOAD-BEARING -- CHAINED-CRC == ONE-SHOT (worker, HARD, fail-fast).  Single-
    owner running-CRC scalar threaded across yields; must equal the one-shot CRC
    of the same private buffer.  A CONSERVATION-style exact law (every byte folded
    in exactly once, in order), not a racy probe.
  * LOAD-BEARING -- CODEC ROUNDTRIP IDENTITY (worker, HARD, fail-fast).  Single-
    owner buffer roundtripped through hex and base64 across a yield; must return
    byte-for-byte identical.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-chain
    (parked inside a crc32/codec call on a corrupted scratch reference) never
    returns; the watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): crc_chains > 0 AND codec_roundtrips > 0 -- both
    load-bearing arms actually ran.

FAIL ON: a chained CRC that differs from the one-shot CRC of the same private
buffer, or a hex/base64 roundtrip that is not the identity, across a runloom
park/resume.  There is NO shared-mutable oracle here -- every operand is single-
owner -- so any failure is a real torn-C-scratch / cross-fiber-leak bug.

Stresses: binascii.crc32 incremental chaining (shared CRC table + running seed
scalar) across hub migration + yield, b2a_hex/a2b_hex and b2a_base64/a2b_base64
conversion scratch under M:N, C-level scratch-buffer thread-affinity, single-owner
digest conservation.

Good TSan / controlled-M:N-replay target: if binascii keeps any per-thread or
module-level scratch, a data-race report on that buffer -- or a deterministic
replay that resumes a half-built chain right after a sibling's crc32 -- localizes
the torn digest before the associativity law even closes.
"""
import binascii

import harness
import runloom

# Per-fiber private stream length band.  Big enough that the chunked chain spans
# many chunks (hence many yields, so a sibling reliably interleaves mid-chain),
# small enough that thousands of fibers churn many streams under the timeout.
MIN_STREAM = 48
MAX_STREAM = 320

# Chunk size band for the incremental CRC walk.  Small chunks -> many crc32 calls
# and many yields per stream -> the park/resume window straddles a sibling's C
# scratch use on another hub as often as possible.
MIN_CHUNK = 1
MAX_CHUNK = 13

# Sustained churn per worker, bounded by H.running().  The torn-scratch hazard
# only manifests under many fibers simultaneously parked mid-chain, so a single
# stream per fiber barely overlaps a sibling; we loop.
INNER_CAP = 100000


def make_stream(wid, idx, rng):
    """Build ONE fiber's PRIVATE, wid-tagged byte stream (fiber-local, never
    shared).  The wid tag makes each fiber's data distinct so a cross-fiber
    scratch corruption produces a wrong digest that could not be a coincidental
    match.  Content is otherwise random within the length band."""
    n = rng.randint(MIN_STREAM, MAX_STREAM)
    buf = bytearray(n)
    # Embed a wid/idx tag in the first few bytes so streams differ across fibers.
    tag = (wid & 0xFFFFFFFF)
    buf[0] = tag & 0xFF
    if n > 1:
        buf[1] = (tag >> 8) & 0xFF
    if n > 2:
        buf[2] = (tag >> 16) & 0xFF
    if n > 3:
        buf[3] = idx & 0xFF
    for i in range(4, n):
        buf[i] = rng.randrange(256)
    return bytes(buf)


def crc_check(H, wid, idx, rng, state):
    """LOAD-BEARING: chained CRC over a private stream must equal the one-shot CRC.

    Single-owner running-CRC scalar threaded across yields.  Any mismatch is a
    torn-C-scratch bug (no shared container is involved)."""
    whole = make_stream(wid, idx, rng)

    # One-shot digest of the whole private buffer.
    expected = binascii.crc32(whole)

    # Incrementally re-derive it: thread a single-owner scalar `crc` through the
    # buffer in random chunks, YIELDING after each chunk so a sibling interleaves
    # its own crc32/codec work on another hub while our chain is half-built.
    crc = 0
    pos = 0
    n = len(whole)
    while pos < n:
        clen = rng.randint(MIN_CHUNK, MAX_CHUNK)
        chunk = whole[pos:pos + clen]
        crc = binascii.crc32(chunk, crc)
        pos += clen
        runloom.yield_now()                # sibling runs mid-chain (park/resume)

    if crc != expected:
        H.fail("chained-CRC != one-shot: binascii.crc32 chained over {0} chunks "
               "gave {1:#010x} but the one-shot crc32 of the same {2}-byte "
               "single-owner buffer is {3:#010x} (wid {4}) -- a sibling's crc32 "
               "corrupted this fiber's running-CRC C scratch across a "
               "park/resume".format((n + MAX_CHUNK - 1) // 1, crc, n, expected,
                                     wid))
        return
    state["crc_chains"][wid] += 1           # single-writer-per-slot, race-free


def codec_check(H, wid, idx, rng, state):
    """LOAD-BEARING: hex and base64 roundtrips of a private buffer are the identity.

    Single-owner buffer; a yield sits BETWEEN encode and decode so a sibling's
    codec call runs against the shared conversion scratch while this fiber's
    intermediate (hex/base64) form is live."""
    whole = make_stream(wid, idx, rng)

    # ---- hex roundtrip: a2b_hex(b2a_hex(x)) == x ----
    hexed = binascii.b2a_hex(whole)
    runloom.yield_now()                     # sibling codecs against shared scratch
    back = binascii.a2b_hex(hexed)
    if back != whole:
        H.fail("hex roundtrip broken: a2b_hex(b2a_hex(x)) != x for a {0}-byte "
               "single-owner buffer (wid {1}) -- the codec conversion scratch was "
               "corrupted across a park/resume".format(len(whole), wid))
        return

    # ---- base64 roundtrip: a2b_base64(b2a_base64(x)) == x ----
    b64 = binascii.b2a_base64(whole)
    runloom.yield_now()
    back2 = binascii.a2b_base64(b64)
    if back2 != whole:
        H.fail("base64 roundtrip broken: a2b_base64(b2a_base64(x)) != x for a "
               "{0}-byte single-owner buffer (wid {1}) -- the codec conversion "
               "scratch was corrupted across a park/resume".format(
                   len(whole), wid))
        return
    state["codec_roundtrips"][wid] += 1     # single-writer-per-slot, race-free


def worker(H, wid, rng, state):
    """Each fiber runs BOTH load-bearing arms per iteration over its OWN private,
    wid-tagged buffers.  Nothing is shared between fibers -- every operand (the
    running-CRC scalar, the stream, every chunk, the hex/base64 intermediates) is
    fiber-local -- so the only thing the yields expose is binascii's C scratch."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            crc_check(H, wid, idx, rng, state)          # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            codec_check(H, wid, idx, rng, state)        # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # Both tallies are one slot per worker (wid-indexed, single writer) -> race-
    # free without a lock.  Allocated here where H.funcs is known.
    H.state = {
        "crc_chains": [0] * H.funcs,        # chained==one-shot checks that passed
        "codec_roundtrips": [0] * H.funcs,  # hex+base64 roundtrip checks that passed
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    crc_chains = sum(H.state["crc_chains"])
    codec_roundtrips = sum(H.state["codec_roundtrips"])
    H.log("binascii[LOAD-BEARING]: {0} chained-CRC==one-shot checks + {1} "
          "hex/base64 roundtrip-identity checks all passed fail-fast; ops={2}".format(
              crc_chains, codec_roundtrips, H.total_ops()))

    # NON-VACUITY: both load-bearing arms actually ran.
    H.check(crc_chains > 0,
            "no chained-CRC checks ran -- the binascii.crc32 incremental-chaining "
            "hazard was never exercised (oracle would be vacuous)")
    H.check(codec_roundtrips > 0,
            "no codec roundtrip checks ran -- the hex/base64 conversion-scratch "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside a crc32 /
    # a2b_hex / a2b_base64 call on a corrupted scratch reference).
    H.require_no_lost("binascii crc/codec conservation")


if __name__ == "__main__":
    harness.main(
        "p513_binascii_crc_hex_conservation", body, setup=setup, post=post,
        default_funcs=8000,
        describe="each fiber CHAINS binascii.crc32 through a single-owner running "
                 "scalar over its OWN wid-tagged byte stream in random chunks with "
                 "a yield between chunks, asserting the chained CRC equals the one-"
                 "shot crc32 of the whole buffer (the associativity-of-CRC32 "
                 "conservation law); it ALSO asserts a2b_hex(b2a_hex(x))==x and "
                 "a2b_base64(b2a_base64(x))==x across a yield.  Every operand is "
                 "fiber-local, so a chained!=one-shot mismatch or a broken codec "
                 "roundtrip is a real torn-C-scratch / cross-fiber-leak bug in "
                 "binascii's shared CRC table / codec conversion buffer under M:N, "
                 "never a shared-container race")
