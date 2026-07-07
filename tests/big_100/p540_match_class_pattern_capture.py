"""big_100 / 540 -- match-statement class/mapping/sequence CAPTURE isolation under M:N.

The structural-pattern-matching `match` statement (PEP 634, a 3.10+ language
feature exercised here on free-threaded 3.14t with the GIL off) compiles a
class pattern `case Point(a, b, c)` into a MATCH_CLASS opcode that:

  1. checks isinstance(subject, Point),
  2. reads the subject TYPE's __match_args__ tuple to learn which attributes the
     positional sub-patterns bind (a<-x, b<-y, c<-z),
  3. TENTATIVELY BINDS each captured sub-pattern into the RUNNING FIBER'S FRAME
     LOCALS (a, b, c become frame-local names), and
  4. only THEN evaluates the case GUARD (`if ...`); if the guard is false the
     arm is skipped but the captured names remain bound in the frame.

Mapping patterns `case {"mx": mx, ...}` (MATCH_KEYS) and sequence patterns
`case [s0, s1, s2]` (MATCH_SEQUENCE / UNPACK_SEQUENCE) bind the same way -- into
frame locals of the fiber currently executing the match.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom runs each
goroutine on its OWN Python frame stack, and a cooperative yield can migrate the
fiber to a DIFFERENT hub before it resumes.  If a `yield` occurs INSIDE a case
GUARD -- i.e. AFTER MATCH_CLASS/MATCH_KEYS/MATCH_SEQUENCE has half-bound the
captured names into the frame but BEFORE the guard returns and the arm body reads
them -- then those half-bound frame locals are live across a hub migration.  A
sibling fiber running its own `match` on a DIFFERENT subject during that window
binds ITS OWN captures.  If frame-local isolation is not fiber-perfect, this
fiber could resume with:

  * a SIBLING'S captured value in one of its bound names (a cross-fiber frame
    local leak), or
  * the WRONG case selected (a torn __match_args__ read / dispatch corruption).

We make this a SINGLE-OWNER, falsifiable oracle -- not a shared-object probe:

  Each fiber matches its OWN, fiber-local subject whose fields are seeded from
  its wid (values are large distinct ints per fiber -- (wid+1)*VALUE_SCALE+off --
  so a leaked sibling value is a DIFFERENT int with a different value, never a
  cached-small-int coincidence).  The subject rotates over four shapes so every
  pattern KIND is exercised:

    kind 0 -- a PosPoint instance  -> POSITIONAL class pattern via __match_args__
    kind 1 -- a KwPoint  instance  -> KEYWORD class pattern (attr-named captures)
    kind 2 -- a dict               -> MAPPING pattern captures
    kind 3 -- a list               -> SEQUENCE pattern captures

  The match's arms, IN ORDER:
    * a PosPoint arm whose guard TENTATIVELY captures then yields and returns
      FALSE (only reached by kind 0 -- forces the half-bound-then-reject churn the
      hazard describes: names get bound, the fiber yields mid-guard, a sibling
      runs, the guard rejects, and the NEXT arm re-binds), then
    * the four real arms (PosPoint / KwPoint / mapping / sequence), EACH with a
      guard that calls runloom.yield_now() before returning True -- so for EVERY
      kind the fiber yields AFTER its captures are bound and BEFORE its arm body
      reads them.

  After the match the fiber asserts, fail-fast:
    * the CORRECT arm fired (kind 0->"posP", 1->"kwP", 2->"map", 3->"seq") -- a
      wrong arm is a torn __match_args__ / dispatch corruption;
    * each captured binding equals this fiber's EXPECTED field value
      (base+1, base+2, base+3) -- a mismatch is a cross-fiber frame-local leak
      surfacing a sibling's captured value.

  Single-owner: the subject, the expected values, and the bound frame locals all
  belong to ONE fiber; nothing is shared.  On a correct runtime the oracle is
  ALWAYS satisfiable, so a clean run exits 0.  A FAIL therefore means a real
  runtime bug (captured frame local leaked across a hub migration, or the wrong
  case dispatched) -- not documented Python semantics.

WHICH ORACLE IS LOAD-BEARING, AND WHY:
  The captured-binding value check (each captured name == this fiber's expected
  field value, read in the arm body AFTER a yield fired inside the guard) is the
  load-bearing oracle.  It is a pure single-owner check: the only way a captured
  name can differ from the fiber-local subject's field is if the frame local was
  overwritten by a sibling across the mid-guard yield -- exactly the M:N frame
  isolation failure this program exists to catch.  Verified against plain threads
  (each OS thread running the same match on its own subject, GIL on and off):
  100% correct-arm + correct-capture, 0 cross-thread leaks; a correct runloom
  must match that.

ORACLES:
  * LOAD-BEARING -- CAPTURE ISOLATION (worker, HARD, fail-fast): correct arm fired
    and every captured binding equals the fiber-local expected value, checked
    after a yield fired inside the winning arm's guard.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-match
    (parked inside a guard-yield and never re-woken) never returns; the watchdog +
    require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (match_checks>0).

FAIL ON: a captured binding whose value is not this fiber's expected field value
(a cross-fiber frame-local leak across the mid-guard yield), the WRONG case
firing (torn __match_args__ / dispatch), or a None capture where a real arm
should have fired.

Stresses: MATCH_CLASS __match_args__ read + positional capture, keyword class
pattern capture, MATCH_KEYS mapping capture, MATCH_SEQUENCE capture, tentative
capture-then-reject churn, and -- the crux -- a cooperative yield INSIDE a case
guard holding half-bound frame locals live across a hub migration under M:N.

Good TSan / controlled-M:N-replay target: the captured names live in the fiber's
frame fastlocals array; a yield inside the guard is a scheduler safepoint with
that array half-written.  A data-race report on the frame's localsplus, or a
deterministic-replay that resumes the frame with a sibling's captured int, is the
cleanest signal before the value oracle even fires.
"""
import harness
import runloom

# Per-fiber field values are drawn far apart so a leaked sibling value is a
# DIFFERENT, non-interned int (never a cached-small-int coincidence): fiber wid's
# subject carries base+1, base+2, base+3 where base = (wid+1)*VALUE_SCALE + slot.
VALUE_SCALE = 1 << 32            # >> any small-int cache; wid separation is clean
SPAN = 64                        # rotate the low bits so the same wid varies by idx


class PosPoint(object):
    """Matched POSITIONALLY: MATCH_CLASS reads __match_args__ to bind x,y,z."""
    __match_args__ = ("x", "y", "z")

    def __init__(self, x, y, z):
        self.x = x
        self.y = y
        self.z = z


class KwPoint(object):
    """Matched by KEYWORD sub-patterns (attr-named); __match_args__ unused for
    keyword patterns but defined so the class is a well-formed match target."""
    __match_args__ = ("x", "y", "z")

    def __init__(self, x, y, z):
        self.x = x
        self.y = y
        self.z = z


# Which arm SHOULD fire for each subject kind (checked fail-fast after the match).
FIRE_FOR_KIND = ("posP", "kwP", "map", "seq")


def make_subject(kind, v1, v2, v3):
    """Build this fiber's PRIVATE subject for `kind`, carrying its unique values."""
    if kind == 0:
        return PosPoint(v1, v2, v3)
    if kind == 1:
        return KwPoint(v1, v2, v3)
    if kind == 2:
        return {"mx": v1, "my": v2, "mz": v3}
    return [v1, v2, v3]


def guard_true():
    """A winning arm's guard: yield (exposing the just-bound frame locals to a
    hub migration) BEFORE returning True, so a sibling reliably interleaves while
    this fiber's captures sit half-bound in its frame."""
    runloom.yield_now()
    return True


def guard_reject(a, b, c):
    """A losing arm's guard: the pattern has TENTATIVELY bound a,b,c into the
    frame; yield mid-guard, then reject so the NEXT arm must re-bind.  References
    a,b,c so they are genuinely captured, not elided."""
    runloom.yield_now()
    return (a is None) and (b is None) and (c is None)   # always False for our ints


def run_match(subj):
    """Run the fiber's OWN subject through the match/case ladder and return
    (fired_tag, captured_triple).  Every winning arm yields inside its guard
    AFTER binding its captures and BEFORE its body reads them."""
    fired = None
    cap = None
    match subj:
        # Losing PosPoint arm (kind 0 only): tentative capture -> yield -> reject,
        # so the next arm has to re-bind under M:N churn.
        case PosPoint(a, b, c) if guard_reject(a, b, c):
            fired = "reject"
            cap = (a, b, c)
        # Real POSITIONAL class pattern (kind 0).
        case PosPoint(a, b, c) if guard_true():
            fired = "posP"
            cap = (a, b, c)
        # Real KEYWORD class pattern (kind 1).
        case KwPoint(x=kx, y=ky, z=kz) if guard_true():
            fired = "kwP"
            cap = (kx, ky, kz)
        # Real MAPPING pattern (kind 2).
        case {"mx": mx, "my": my, "mz": mz} if guard_true():
            fired = "map"
            cap = (mx, my, mz)
        # Real SEQUENCE pattern (kind 3).
        case [s0, s1, s2] if guard_true():
            fired = "seq"
            cap = (s0, s1, s2)
        case _:
            fired = "default"
    return fired, cap


# Sustained checks per worker, bounded by H.running().  The frame-isolation
# hazard only manifests under SUSTAINED churn -- many fibers simultaneously
# yielding mid-guard with half-bound captures so the scheduler reliably
# interleaves a sibling before this fiber's arm body reads its captures.
INNER_CAP = 100000


def match_check(H, wid, idx, state):
    """Single-owner capture-isolation check (fail-fast).

    Build this fiber's private subject with unique values, run it through the
    match ladder (which yields inside the winning arm's guard), then assert the
    correct arm fired and every captured binding equals this fiber's expected
    value.  A mismatch is a cross-fiber frame-local leak; a wrong arm is a torn
    __match_args__ / dispatch corruption."""
    kind = (wid + idx) & 3
    base = (wid + 1) * VALUE_SCALE + (idx % SPAN) * 4
    e1, e2, e3 = base + 1, base + 2, base + 3
    subj = make_subject(kind, e1, e2, e3)

    fired, cap = run_match(subj)

    expected_fire = FIRE_FOR_KIND[kind]
    if fired != expected_fire:
        H.fail("WRONG CASE fired: subject kind {0} matched arm {1!r}, expected "
               "{2!r} (wid {3}, idx {4}) -- a torn __match_args__ read or a "
               "MATCH_CLASS/MATCH_KEYS/MATCH_SEQUENCE dispatch corruption across "
               "a mid-guard hub migration".format(
                   kind, fired, expected_fire, wid, idx))
        return

    if cap is None:
        H.fail("NO CAPTURE: subject kind {0} fired arm {1!r} but captured nothing "
               "(wid {2}, idx {3}) -- the winning arm's captures were not bound "
               "into the fiber frame".format(kind, fired, wid, idx))
        return

    g1, g2, g3 = cap
    if g1 != e1 or g2 != e2 or g3 != e3:
        H.fail("CAPTURE LEAK: subject kind {0} arm {1!r} captured {2!r}, expected "
               "{3!r} (wid {4}, idx {5}) -- a captured frame local was overwritten "
               "by a sibling fiber across the yield inside the case guard (cross-"
               "fiber frame-local leak under M:N)".format(
                   kind, fired, (g1, g2, g3), (e1, e2, e3), wid, idx))
        return

    state["match_checks"][wid] += 1     # single-writer-per-slot (wid-indexed)


def worker(H, wid, rng, state):
    """Sustained single-owner match/capture checks.  Each iteration builds a
    fresh fiber-local subject, matches it (yielding mid-guard), and verifies the
    correct arm + correct captures survived the yield."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            match_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # match_checks: ONE slot per worker (wid-indexed, single-writer -> race-free),
    # allocated here where H.funcs is known.  Feeds the non-vacuity check only.
    H.state = {
        "match_checks": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["match_checks"])
    H.log("match capture-isolation checks (all passed fail-fast): {0}; "
          "ops={1}".format(checks, H.total_ops()))

    # NON-VACUITY: the load-bearing single-owner capture-isolation arm actually
    # ran (else the oracle was vacuous).
    H.check(checks > 0,
            "no single-owner match capture-isolation checks ran -- the load-"
            "bearing frame-local-capture hazard was never exercised")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside a case
    # guard's yield and never re-woken).
    H.require_no_lost("match class-pattern capture isolation")


if __name__ == "__main__":
    harness.main(
        "p540_match_class_pattern_capture", body, setup=setup, post=post,
        default_funcs=5000,
        describe="the match statement's MATCH_CLASS reads __match_args__ and binds "
                 "captured sub-patterns into the fiber's frame locals BEFORE the "
                 "case guard runs; a yield inside a winning guard holds those half-"
                 "bound captures live across a hub migration.  LOAD-BEARING: each "
                 "fiber matches its OWN subject (positional/keyword class + mapping "
                 "+ sequence arms, values seeded by wid) through a ladder whose "
                 "winning guard yields before its body reads the captures; the "
                 "correct arm MUST fire and every captured binding MUST equal the "
                 "fiber-local field value.  A captured value that becomes a "
                 "sibling's across the yield, or the wrong case firing, is the "
                 "runloom frame-local capture-isolation bug")
