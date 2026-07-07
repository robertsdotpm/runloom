"""big_100 / 612 -- token predicate PURITY + EXACT_TOKEN_TYPES round-trip under M:N.

The `token` module is the stdlib's token-type registry: a set of integer
constants (ENDMARKER=0 .. N_TOKENS, then the NT_OFFSET=256 non-terminal band),
three pure predicate functions, and two read-only lookup tables:

    ISTERMINAL(x)      -> x < NT_OFFSET          (a real token type)
    ISNONTERMINAL(x)   -> x >= NT_OFFSET         (a grammar non-terminal)
    ISEOF(x)           -> x == ENDMARKER (== 0)  (end-of-input marker)
    tok_name[int]      -> "NAME"                 (int -> canonical name)
    EXACT_TOKEN_TYPES  -> {"+": PLUS, ...}       (operator string -> token type)

Every one of these is a PURE, referentially-transparent lookup: the module is
imported once, its constants and tables never mutate, and the three predicates
are single-comparison functions of their argument and a module-global constant.
So for ANY integer x the results are fully determined by a closed form we can
compute independently, and they can NEVER change across a yield.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom multiplexes
tens of thousands of goroutines over a few hubs with the GIL OFF.  A pure
stdlib function is only "pure" if, under that scheduling, (a) the function
OBJECT bound to `token.ISTERMINAL` still resolves to the same code, (b) the
module-global constant it closes over (NT_OFFSET / ENDMARKER) is read intact
(not a torn / half-published int object), and (c) the frame that computes and
compares the result is not corrupted by a sibling resuming on the same hub
mid-call.  If a fiber computes ISTERMINAL(x) == a specific boolean, PARKS across
a yield while thousands of siblings hammer the same functions on other hubs, and
then re-computes and gets a DIFFERENT boolean -- or a boolean that disagrees
with the closed form -- that is a runtime desync (torn constant read, swapped
function object, corrupted return frame), NOT documented Python behavior.

WHY THE ORACLE IS LEGITIMATELY SINGLE-OWNER (verified against plain threads):

  Every input integer is fiber-local (derived from wid via H.derive, built into a
  private list owned by exactly one fiber).  Every computed result is folded into
  a fiber-local integer checksum owned by that fiber.  The token module's tables
  and constants are READ-ONLY shared immutables -- reading them concurrently is
  exactly like reading `math.pi`: no writer exists, so there is no shared-mutable
  race (HARD RULE 2 is about shared MUTABLE containers; a frozen module table is a
  constant).  We verified with a plain-threads control (8 OS threads, GIL on AND
  off, each folding the same closed-form checksum over random integers) that the
  predicate results are 100% deterministic and bit-identical -- 0 disagreements.
  Under a correct runloom the same must hold, so the oracle PASSES on a correct
  runtime (exit 0 when there is no bug).

THE CLOSED FORM (computed independently of the token module, so the oracle is
falsifiable, not self-referential):

    exp_isterminal(x)    = 1 if x < NT_OFFSET else 0
    exp_isnonterminal(x) = 1 if x >= NT_OFFSET else 0           (== 1 - exp_isterminal)
    exp_iseof(x)         = 1 if x == ENDMARKER else 0
    PARTITION invariant  : ISTERMINAL(x) != ISNONTERMINAL(x)  for EVERY x
                           (the two predicates split the integers on NT_OFFSET;
                            both-true or both-false is a torn-constant read)
    ROUND-TRIP invariant : for every (op, t) in EXACT_TOKEN_TYPES,
                           t < NT_OFFSET (exact tokens are terminals),
                           tok_name[t] is a name, and getattr(token, that name) == t.

ORACLES:
  * LOAD-BEARING -- PREDICATE PURITY + ROUND-TRIP (worker, HARD, fail-fast).
    Each fiber owns a private list of test integers spanning the NT_OFFSET
    boundary (and the ENDMARKER / N_TOKENS / non-terminal band).  It folds the
    token module's predicate results AND the EXACT_TOKEN_TYPES round-trip into a
    fiber-local checksum, and independently folds the closed-form checksum.  It
    asserts actual == closed-form (the predicates are correct), snapshots the
    checksum, YIELDS (parks so siblings hammer the same functions), then RE-folds
    and asserts the checksum is (a) bit-identical to the pre-yield snapshot -- the
    stability law -- and (b) still equal to the closed form.  A mismatch means a
    torn constant read, a swapped function object, or a corrupted return frame:
    a runtime bug.  Single-owner: inputs and checksums are fiber-local; the module
    tables are read-only immutables shared like constants.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that parked inside a
    predicate call and never resumed (lost wakeup) is caught by the watchdog.

FAIL ON: a token predicate returning a result that disagrees with the closed
form, the ISTERMINAL/ISNONTERMINAL partition breaking, an EXACT_TOKEN_TYPES
round-trip that does not resolve, or a fiber-local checksum that CHANGES across a
yield.  All inputs are single-owner and all tables are read-only, so any such
observation is a runloom desync (torn read / frame corruption / SIGSEGV), never
documented Python semantics.

Stresses: pure-function purity across hub migration + park/resume, torn read of a
module-global int constant (NT_OFFSET / ENDMARKER) under GIL-off concurrency,
function-object binding stability, read-only dict lookup (tok_name /
EXACT_TOKEN_TYPES) racing thousands of concurrent readers, return-frame integrity
across a yield.
"""
import token

import harness
import runloom

# The load-bearing constants, snapshotted at import into local names so the
# closed form is computed independently of any per-call module attribute lookup.
NT_OFFSET = token.NT_OFFSET            # 256: the terminal / non-terminal split
ENDMARKER = token.ENDMARKER           # 0: the ISEOF marker
N_TOKENS = token.N_TOKENS

# The EXACT_TOKEN_TYPES round-trip, frozen at import into a private tuple of
# (op_string, token_type, canonical_name) so the expected mapping is a constant
# owned by the module load, not re-derived from the live dict every call.  The
# live dict is still exercised per-fiber below; this is the independent oracle.
FROZEN_EXACT = tuple(
    (op, t, token.tok_name[t]) for op, t in sorted(token.EXACT_TOKEN_TYPES.items())
)

# How many fiber-local test integers to fold per check.  Spans the NT_OFFSET
# boundary and the ENDMARKER / non-terminal band so both predicate arms and the
# partition invariant are exercised on every fold.
NPROBE = 48

# Sustained folds per worker, bounded by H.running().  The purity hazard (a torn
# constant read or a swapped function binding) only manifests under SUSTAINED
# churn: many fibers folding the same predicates while park-yielded across their
# checkpoint, so a sibling reliably interleaves before this fiber resumes.  A
# single fold per fiber barely overlaps a sibling's and does NOT reproduce.
INNER_CAP = 100000


def build_probes(rng):
    """One fiber's PRIVATE list of test integers.  Includes the exact boundary
    values (NT_OFFSET-1, NT_OFFSET, NT_OFFSET+1), ENDMARKER, and random integers
    spread across the terminal band, the non-terminal band, and above.  Every
    value is fiber-local; the list is never shared."""
    probes = [ENDMARKER, 0, 1, NT_OFFSET - 1, NT_OFFSET, NT_OFFSET + 1, N_TOKENS]
    while len(probes) < NPROBE:
        # Spread across [0, 3*NT_OFFSET) so both predicate arms fire heavily.
        probes.append(rng.randrange(0, 3 * NT_OFFSET))
    return probes


def fold_closed_form(probes):
    """Independent closed-form checksum over `probes`.  Computed WITHOUT calling
    the token module -- this is the falsifiable expected value the module's
    results must match.  Also folds the frozen EXACT_TOKEN_TYPES round-trip."""
    acc = 0x9E3779B1
    for x in probes:
        it = 1 if x < NT_OFFSET else 0
        nt = 1 if x >= NT_OFFSET else 0
        ie = 1 if x == ENDMARKER else 0
        # Partition is a closed-form identity: it and nt are complementary.
        acc = (acc * 1000003 + (x & 0xFFFFFFFF)) & 0xFFFFFFFFFFFFFFFF
        acc = (acc * 31 + it) & 0xFFFFFFFFFFFFFFFF
        acc = (acc * 31 + nt) & 0xFFFFFFFFFFFFFFFF
        acc = (acc * 31 + ie) & 0xFFFFFFFFFFFFFFFF
    for op, t, name in FROZEN_EXACT:
        acc = (acc * 1000003 + t) & 0xFFFFFFFFFFFFFFFF
        acc = (acc * 31 + (len(name) & 0xFF)) & 0xFFFFFFFFFFFFFFFF
    return acc


def fold_actual(H, wid, probes):
    """Checksum over the SAME probes computed by CALLING the live token module.
    Fail-fast on any per-value disagreement with the closed form (so we localize
    the exact input), then also return the folded checksum for the equality +
    stability laws.  Returns None on failure."""
    acc = 0x9E3779B1
    for x in probes:
        it = 1 if token.ISTERMINAL(x) else 0
        nt = 1 if token.ISNONTERMINAL(x) else 0
        ie = 1 if token.ISEOF(x) else 0

        # PARTITION invariant: exactly one of ISTERMINAL / ISNONTERMINAL holds.
        if it == nt:
            H.fail("token predicate PARTITION broken (wid {0}): ISTERMINAL({1})="
                   "{2} ISNONTERMINAL({1})={3} -- both agree, so the NT_OFFSET "
                   "split ({4}) read torn under GIL-off concurrency".format(
                       wid, x, it, nt, NT_OFFSET))
            return None
        # Closed-form agreement per predicate.
        if it != (1 if x < NT_OFFSET else 0):
            H.fail("token.ISTERMINAL({0})={1} disagrees with closed form (x<{2}) "
                   "(wid {3}) -- a pure predicate returned a wrong result, torn "
                   "NT_OFFSET constant or swapped function object".format(
                       x, it, NT_OFFSET, wid))
            return None
        if ie != (1 if x == ENDMARKER else 0):
            H.fail("token.ISEOF({0})={1} disagrees with closed form (x=={2}) "
                   "(wid {3}) -- torn ENDMARKER constant read".format(
                       x, ie, ENDMARKER, wid))
            return None

        acc = (acc * 1000003 + (x & 0xFFFFFFFF)) & 0xFFFFFFFFFFFFFFFF
        acc = (acc * 31 + it) & 0xFFFFFFFFFFFFFFFF
        acc = (acc * 31 + nt) & 0xFFFFFFFFFFFFFFFF
        acc = (acc * 31 + ie) & 0xFFFFFFFFFFFFFFFF

    # EXACT_TOKEN_TYPES round-trip over the LIVE dict: every exact operator maps
    # to a terminal token whose canonical name resolves back to that same int.
    for op, t, name in FROZEN_EXACT:
        live = token.EXACT_TOKEN_TYPES.get(op)
        if live != t:
            H.fail("token.EXACT_TOKEN_TYPES[{0!r}]={1!r} != frozen {2} (wid {3}) "
                   "-- read-only exact-token table returned a wrong/torn entry".format(
                       op, live, t, wid))
            return None
        if not token.ISTERMINAL(t):
            H.fail("EXACT_TOKEN_TYPES value {0} for {1!r} is not a terminal "
                   "(wid {2}) -- ISTERMINAL disagrees with the round-trip".format(
                       t, op, wid))
            return None
        if token.tok_name[t] != name or getattr(token, name) != t:
            H.fail("EXACT_TOKEN_TYPES round-trip broken for {0!r}: tok_name[{1}]="
                   "{2!r} getattr(token,{3!r})={4!r} (wid {5})".format(
                       op, t, token.tok_name[t], name, getattr(token, name, None), wid))
            return None
        acc = (acc * 1000003 + t) & 0xFFFFFFFFFFFFFFFF
        acc = (acc * 31 + (len(name) & 0xFF)) & 0xFFFFFFFFFFFFFFFF
    return acc


def purity_check(H, wid, idx, rng, state):
    """One single-owner purity fold: build fiber-local probes, verify the live
    predicates match the closed form, snapshot the checksum, YIELD (park so
    siblings hammer the same pure functions), then re-fold and assert the
    checksum is bit-identical across the yield AND still equals the closed form."""
    probes = build_probes(rng)
    expected = fold_closed_form(probes)

    sig1 = fold_actual(H, wid, probes)
    if sig1 is None:
        return
    if sig1 != expected:
        H.fail("token purity checksum {0} != closed form {1} (wid {2}) before "
               "yield -- the live predicates already disagree with the pure "
               "closed form".format(sig1, expected, wid))
        return

    # YIELD: park so thousands of siblings drive the same predicates on other
    # hubs before this fiber's frame resumes.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    sig2 = fold_actual(H, wid, probes)
    if sig2 is None:
        return
    # STABILITY law: a pure fold over fiber-local inputs cannot change across a
    # yield.  A difference is a torn read / corrupted frame / swapped binding.
    if sig2 != sig1:
        H.fail("token purity checksum CHANGED across a yield (wid {0}): {1} -> "
               "{2} -- a pure predicate fold over fiber-local inputs is not "
               "stable, a runtime desync (torn constant, frame corruption)".format(
                   wid, sig1, sig2))
        return
    if sig2 != expected:
        H.fail("token purity checksum {0} != closed form {1} after yield (wid "
               "{2}) -- the live predicates drifted from the pure form while "
               "parked".format(sig2, expected, wid))
        return

    state["checks"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """Sustained single-owner purity folds, fail-fast.  Each fiber owns its probe
    list and checksum; the token module tables are read-only shared immutables."""
    fr = H.derive("token", wid)
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            purity_check(H, wid, idx, fr, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "checks": [0] * 1024,             # LOAD-BEARING single-owner folds (non-vacuity)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("token[single-owner PURITY LOAD-BEARING]: {0} predicate/round-trip "
          "folds (all matched the closed form + were checksum-stable across a "
          "yield, fail-fast); ops={1}".format(checks, H.total_ops()))

    # NON-VACUITY: the load-bearing purity hazard was actually exercised.
    H.check(checks > 0,
            "no single-owner token purity folds ran -- the predicate-purity "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished inside a predicate call.
    H.require_no_lost("token predicate purity")


if __name__ == "__main__":
    harness.main(
        "p612_token_purity", body, setup=setup, post=post,
        default_funcs=8000,
        describe="token exposes pure predicates (ISTERMINAL/ISNONTERMINAL/ISEOF) "
                 "over module-global constants (NT_OFFSET/ENDMARKER) plus read-only "
                 "tables (tok_name/EXACT_TOKEN_TYPES).  LOAD-BEARING: each fiber "
                 "folds the live predicate results + the EXACT_TOKEN_TYPES round-"
                 "trip over a private probe list into a checksum, asserts it equals "
                 "an independent closed form, YIELDS, then re-folds and requires the "
                 "checksum be bit-identical across the yield and still match the "
                 "closed form.  A predicate disagreeing with the closed form, a "
                 "broken ISTERMINAL/ISNONTERMINAL partition (torn NT_OFFSET read), a "
                 "failed round-trip, or a checksum that changes across a yield is a "
                 "runtime desync (torn constant / frame corruption / SIGSEGV)")
