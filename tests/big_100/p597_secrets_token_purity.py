"""big_100 / 597 -- secrets CSPRNG structural + compare_digest purity under M:N.

The `secrets` module is CPython's cryptographic-strength randomness front end.
Its generators (token_bytes / token_hex / token_urlsafe / randbelow / randbits /
choice) all delegate to a SINGLE process-global SystemRandom instance
(secrets._sysrand), which is STATELESS by design: SystemRandom overrides
random()/getrandbits() to read os.urandom fresh on every call and makes
getstate()/setstate()/seed() raise, so there is NO mutable seed state to race.
That is exactly why the module is safe to share -- and exactly what makes its
outputs subject to hard, falsifiable STRUCTURAL laws that must hold regardless of
concurrency.  compare_digest is a PURE C function (constant-time buffer compare).

WHERE M:N COULD BREAK IT (the gap this program probes).  Under free-threaded
CPython with the GIL off and runloom driving tens of thousands of goroutines
across >1 hubs, every secrets call funnels through the shared _sysrand and through
os.urandom (a getrandom() syscall that may become a cooperative yield point under
monkey.patch()).  If the M:N runtime torn-read the returned int/bytes object, or
leaked a sibling fiber's freshly-produced value into this fiber's local across a
hub migration, a generator would return a value that VIOLATES its closed-form
range/length/round-trip law: randbelow(n) >= n, randbits(k) with bit_length > k,
token_bytes(n) whose len != n, a token_hex string with a non-hex char, a
token_urlsafe that does not urlsafe-b64-decode back to exactly n bytes, or a
choice() result outside its fiber-local sequence.  Those are not documented
Python semantics -- a CSPRNG's range/length guarantees are absolute -- so any
violation is a real runtime torn-value / cross-fiber-leak bug.

WHICH ORACLE IS LOAD-BEARING, AND WHY (single-owner + closed-form).  Every input
is fiber-local (derived from wid+idx, never shared) and every checked object is
produced and owned by ONE fiber.  The laws are mathematical identities, not
value-equality against a shared expectation, so a correct runtime PASSES 100% of
the time (verified with a plain-threads control: 8 OS threads, GIL on and off,
hammering the same secrets calls -- 0 structural violations).  Because _sysrand is
stateless, there is NO legitimate shared-mutable oracle to relegate; the whole
load-bearing arm is single-owner, and the only shared object touched (the global
_sysrand) has no state that a fiber could corrupt.

ORACLES:
  * LOAD-BEARING -- CSPRNG STRUCTURAL LAWS + compare_digest PURITY (worker, HARD,
    fail-fast).  Each fiber, with fiber-local sizes:
      - token_bytes(n): type is bytes, len == n.
      - token_hex(n): type str, len == 2n, every char in [0-9a-f],
        bytes.fromhex(s) round-trips to n bytes.
      - token_urlsafe(n): urlsafe-b64-decodes (with restored padding) to exactly
        n bytes.
      - randbelow(n): 0 <= r < n.
      - randbits(k): 0 <= r < 2**k and r.bit_length() <= k.
      - choice(seq): result is an element of the fiber-local sequence.
      - compare_digest: reflexive (a,a)->True, (a, equal-copy)->True,
        (a, one-byte-flipped)->False, symmetric.
    It records the pre-yield structural facts + compare_digest booleans, YIELDS
    (runloom.yield_now / sleep) so siblings interleave through the shared _sysrand
    on other hubs, then RE-verifies every stored single-owner object is unchanged
    (len/value/round-trip bit-identical, compare_digest booleans identical) across
    the yield.  A value that changes, or that violates a range/length/round-trip
    law, is the runloom torn-value / cross-fiber-leak bug.

  * MEASURED (report-only, NEVER fails): DISTINCTNESS tally.  Two successive
    token_bytes(n) in the same fiber are compared; a collision is recorded.  For
    n >= 8 (>= 64 bits) a real collision is astronomically improbable, so a nonzero
    rate would flag a stuck/degenerate entropy source -- but we only MEASURE and
    REPORT it (a genuine 1-in-2^64 collision must never fail the run).  It proves
    the generators actually produced VARIED output, so the range/length laws are
    not vacuously satisfied by a constant.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside a
    cooperative os.urandom park never returns; the watchdog + require_no_lost
    catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

FAIL ON: any CSPRNG structural-law violation, a compare_digest result that is
wrong or changes across a yield, or a single-owner value that changed across the
yield.  The distinctness arm is report-only.

Stresses: secrets.token_bytes/token_hex/token_urlsafe/randbelow/randbits/choice
funnelled through the shared stateless SystemRandom + os.urandom getrandom()
syscall under M:N hub migration; secrets.compare_digest constant-time buffer
compare purity; torn int/bytes/str value or cross-fiber value leak across a yield.

Good TSan / controlled-M:N-replay target: os.urandom's syscall + the returned
PyBytes/PyLong construction run concurrently on many hubs through one _sysrand; a
data-race report on a returned object, or a replay that returns an out-of-range
int, localizes the torn value before the structural law even closes.
"""
import base64

import harness
import runloom

# Character set token_hex is allowed to emit.  A char outside this set in a
# token_hex string is a torn/corrupted byte in the produced str.
HEXSET = frozenset("0123456789abcdef")

# randbits widths cycled per check.  Spread across the 1..63 range so the produced
# PyLong crosses the small-int / multi-digit boundary (a torn high digit shows up
# as bit_length() > k).
RANDBITS_WIDTHS = (1, 3, 7, 15, 31, 32, 33, 47, 63)

# Sustained checks per worker, bounded by H.running().  The torn-value / cross-
# fiber hazard only manifests under SUSTAINED churn -- many fibers simultaneously
# pulling from the shared _sysrand / os.urandom while sleep-PARKED across their
# yield, so a sibling reliably interleaves before this fiber resumes.
INNER_CAP = 100000


def urlsafe_decode(s):
    """Restore stripped b64 padding and urlsafe-b64-decode.  token_urlsafe(n)
    is base64.urlsafe_b64encode(token_bytes(n)).rstrip(b'=').decode(), so this
    inverts it exactly -> the decoded length must equal n."""
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def secrets_check(H, wid, idx, secrets_mod, state):
    """One battery of single-owner CSPRNG structural + compare_digest purity
    checks, verified across a yield.  All inputs are fiber-local; all laws are
    closed-form identities that hold on a correct runtime."""
    # Fiber-local sizes (never shared).  n in [8, 32] keeps tokens >= 64 bits so
    # the distinctness arm is meaningful, and crosses several allocation sizes.
    n = 8 + (idx % 25)
    k = RANDBITS_WIDTHS[idx % len(RANDBITS_WIDTHS)]
    bound = n + 1                                  # randbelow needs a positive n
    seq = tuple(range(wid * 100003 + idx, wid * 100003 + idx + 16))

    # ---- generate single-owner values -------------------------------------
    b = secrets_mod.token_bytes(n)
    if type(b) is not bytes or len(b) != n:
        H.fail("token_bytes({0}) returned {1!r}-typed len={2} (expected bytes "
               "len={0}) wid={3} -- torn/corrupted CSPRNG output".format(
                   n, type(b).__name__, len(b) if hasattr(b, "__len__") else "?",
                   wid))
        return

    hx = secrets_mod.token_hex(n)
    if type(hx) is not str or len(hx) != 2 * n or not (set(hx) <= HEXSET):
        H.fail("token_hex({0}) returned bad string len={1} (expected str len={2}, "
               "hex-only) wid={3} -- torn/corrupted CSPRNG hex output".format(
                   n, len(hx), 2 * n, wid))
        return
    hx_decoded = bytes.fromhex(hx)                 # raises on any non-hex char
    if len(hx_decoded) != n:
        H.fail("bytes.fromhex(token_hex({0})) length {1} != {0} wid={2} -- torn "
               "hex round-trip".format(n, len(hx_decoded), wid))
        return

    us = secrets_mod.token_urlsafe(n)
    us_decoded = urlsafe_decode(us)
    if len(us_decoded) != n:
        H.fail("token_urlsafe({0}) urlsafe-b64-decoded to {1} bytes (expected "
               "{0}) wid={2} -- torn/corrupted urlsafe token".format(
                   n, len(us_decoded), wid))
        return

    rb = secrets_mod.randbelow(bound)
    if not (0 <= rb < bound):
        H.fail("randbelow({0}) returned {1} OUT OF [0,{0}) wid={2} -- torn int / "
               "cross-fiber CSPRNG leak".format(bound, rb, wid))
        return

    rk = secrets_mod.randbits(k)
    if not (0 <= rk < (1 << k)) or rk.bit_length() > k:
        H.fail("randbits({0}) returned {1} (bit_length {2}) OUT OF [0,2**{0}) "
               "wid={3} -- torn PyLong / cross-fiber CSPRNG leak".format(
                   k, rk, rk.bit_length(), wid))
        return

    c = secrets_mod.choice(seq)
    if c not in seq:
        H.fail("choice(seq) returned {0!r} NOT IN the fiber-local sequence "
               "[{1}..{2}) wid={3} -- torn index / cross-fiber leak".format(
                   c, seq[0], seq[-1] + 1, wid))
        return

    # compare_digest purity on fiber-local buffers.  b_equal is a distinct object
    # with equal value; b_diff differs from b in exactly its last byte.
    b_equal = bytes(bytearray(b))
    diff = bytearray(b)
    diff[-1] ^= 0xFF
    b_diff = bytes(diff)
    cd_equal = secrets_mod.compare_digest(b, b_equal)
    cd_self = secrets_mod.compare_digest(b, b)
    cd_diff = secrets_mod.compare_digest(b, b_diff)
    cd_sym = secrets_mod.compare_digest(b_equal, b)
    if not (cd_equal is True and cd_self is True and cd_diff is False
            and cd_sym is True):
        H.fail("compare_digest purity violated BEFORE yield: equal={0} self={1} "
               "diff={2} sym={3} (expected True/True/False/True) wid={4}".format(
                   cd_equal, cd_self, cd_diff, cd_sym, wid))
        return

    # MEASURED (report-only): distinctness of two fresh tokens (never fails).
    b2 = secrets_mod.token_bytes(n)
    if b2 == b:
        state["dups"][wid & 1023] += 1

    # ---- YIELD: let siblings churn through the shared _sysrand / os.urandom ----
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    # ---- re-verify EVERY single-owner object is unchanged across the yield ----
    if len(b) != n or b != b_equal:
        H.fail("token_bytes value CHANGED across a yield: len {0} (expected {1}) "
               "or value drifted from its own copy wid={2} -- the single-owner "
               "bytes object was torn / a sibling's value leaked in".format(
                   len(b), n, wid))
        return
    if bytes.fromhex(hx).hex() != hx or bytes.fromhex(hx) != hx_decoded:
        H.fail("token_hex round-trip NOT bit-identical across a yield wid={0} -- "
               "the single-owner hex str/bytes was torn".format(wid))
        return
    if urlsafe_decode(us) != us_decoded:
        H.fail("token_urlsafe re-decode CHANGED across a yield wid={0} -- the "
               "single-owner urlsafe token was torn".format(wid))
        return
    if not (0 <= rb < bound) or not (0 <= rk < (1 << k)) or rk.bit_length() > k:
        H.fail("randbelow/randbits value drifted OUT OF RANGE across a yield "
               "wid={0} (rb={1} bound={2} rk={3} k={4}) -- torn int".format(
                   wid, rb, bound, rk, k))
        return
    if c not in seq:
        H.fail("choice result drifted OUT OF sequence across a yield wid={0} "
               "(c={1!r})".format(wid, c))
        return
    cd_equal2 = secrets_mod.compare_digest(b, b_equal)
    cd_diff2 = secrets_mod.compare_digest(b, b_diff)
    if not (cd_equal2 is True and cd_diff2 is False):
        H.fail("compare_digest purity CHANGED across a yield: equal={0} diff={1} "
               "(expected True/False) wid={2} -- torn buffer compare".format(
                   cd_equal2, cd_diff2, wid))
        return

    state["checks"][wid] += 1                       # single-writer-per-slot, race-free


def worker(H, wid, rng, state):
    secrets_mod = state["secrets"]
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            secrets_check(H, wid, idx, secrets_mod, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    import secrets
    H.state = {
        "secrets": secrets,
        "checks": [0] * H.funcs,          # LOAD-BEARING single-owner checks (per-wid)
        "dups": [0] * 1024,               # MEASURED token collisions (report-only)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    dups = sum(H.state["dups"])
    H.log("secrets[single-owner LOAD-BEARING]: {0} CSPRNG structural + "
          "compare_digest purity checks (all passed fail-fast) | "
          "distinctness[MEASURED]: {1} token collisions (report-only; expected "
          "0 for >=64-bit tokens)".format(checks, dups))
    if dups:
        H.log("note: {0} token_bytes collisions observed -- for n>=8 (>=64 bits) "
              "a real collision is ~1-in-2^64, so a nonzero count hints at a "
              "degenerate entropy source; MEASURED + REPORT ONLY, never a "
              "failure (a genuine astronomically-rare collision must not fail the "
              "run)".format(dups))

    # NON-VACUITY: the load-bearing arm actually exercised the CSPRNG laws.
    H.check(checks > 0,
            "no single-owner secrets checks ran -- the CSPRNG structural / "
            "compare_digest purity hazard was never exercised (oracle vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded in a cooperative
    # os.urandom read).
    H.require_no_lost("secrets CSPRNG purity")


if __name__ == "__main__":
    harness.main(
        "p597_secrets_token_purity", body, setup=setup, post=post,
        default_funcs=8000,
        describe="secrets funnels token_bytes/token_hex/token_urlsafe/randbelow/"
                 "randbits/choice through ONE stateless SystemRandom + os.urandom, "
                 "and compare_digest is a pure constant-time compare.  LOAD-BEARING: "
                 "each fiber checks fiber-local CSPRNG outputs against their "
                 "closed-form range/length/round-trip laws and compare_digest's "
                 "purity, re-verifying every single-owner value is bit-identical "
                 "across a yield.  A randbelow(n)>=n, a token whose len/round-trip "
                 "breaks, a wrong/changed compare_digest, or a value that drifts "
                 "across a yield is the runloom torn-value / cross-fiber-leak bug. "
                 "MEASURED token-distinctness (report-only) proves the generators "
                 "produced varied output so the laws are not vacuous")
