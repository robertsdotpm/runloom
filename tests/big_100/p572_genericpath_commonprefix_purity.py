"""big_100 / 572 -- genericpath.commonprefix purity across a yield under M:N.

genericpath.commonprefix(m) is the pure string/bytes routine underlying
os.path.commonprefix: given a list of pathnames it returns their longest common
leading component.  Its whole computation is `s1 = min(m); s2 = max(m); return
s1[: first index where s1 and s2 differ]`.  It touches NO module-global mutable
state -- only the argument list and (for os.PathLike inputs) os.fspath -- so it
is mathematically PURE: the same input list must always return the same prefix.

WHERE M:N COULD BREAK IT (the gap this program probes).  Under the pygo runtime a
fiber can be preempted mid-call, migrated across hubs, or parked-and-resumed at a
cooperative yield.  min()/max() over the list build a C comparison loop and the
final `s1[:i]` slice allocates a fresh str/bytes/list in a C scratch buffer.  If
the runtime ever resumed a fiber with a stale frame (lost-wakeup class), let a
sibling's concurrent commonprefix scribble into this fiber's transient
min/max/slice state (cross-fiber scratch leak), or tore the produced object,
then re-running the SAME pure call on the SAME fiber-local list after a yield
would return a DIFFERENT prefix.  Because the inputs are fiber-local constants
and the function is pure, ANY change across the yield is a runtime bug, never a
documented Python semantic.

WHICH ORACLE IS LOAD-BEARING, AND WHY:

  Single-owner closed-form PURITY law.  Each fiber builds its OWN list `m`
  (fiber-local, never shared) with a KNOWN, constructed common prefix P and at
  least two members that diverge exactly at position len(P) -- so the closed-form
  longest common prefix of the whole list is EXACTLY P by construction.  The
  fiber:
    * computes r1 = commonprefix(m) and asserts r1 == P (the closed form) with
      the right type -- catches a value that is WRONG the instant it is produced
      (a torn min/max/slice), which a pure recompute alone could miss if both
      computations tore identically;
    * YIELDS (yield_now / sleep) so siblings interleave their own commonprefix
      churn, possibly on another hub;
    * recomputes r2 = commonprefix(m) and asserts r2 == r1 == P, same type
      -- catches a value that CHANGED across the park (the M:N purity hazard).

  Three fiber-local input shapes are round-robined so the str, bytes, and
  list-of-parts branches of commonprefix (the last SKIPS os.fspath) are all
  exercised: STR (list of str), BYTES (list of bytes), PARTS (list of list-of-str
  "pathname parts", the OS-agnostic sublist form).  For each the expected prefix
  is closed-form P; on a correct runtime every check passes deterministically
  (program exits 0).  A FAIL means a commonprefix result was wrong on production,
  or changed across a yield -- a real runtime purity/isolation bug.

  Note on why NO shared-mutable MEASURED arm: commonprefix is pure and reads only
  its argument; there is no shared container it mutates, so there is no documented
  shared-object race to measure/report (unlike enum's _member_map_ or a shared
  Counter).  The single-owner recompute-across-yield arm IS the hazard test.

ORACLES:
  * LOAD-BEARING -- PURITY (worker, HARD, fail-fast): r1 == P and r2 == r1 == P
    with identical type, on fiber-local single-owner input.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-slice /
    inside min()/max() never returns; the watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

Stresses: genericpath.commonprefix min()/max() comparison loop + trailing slice
over str / bytes / list-of-parts inputs, across a cooperative yield + hub
migration under M:N; purity of a pure stdlib path routine under preemption.
"""
import genericpath

import harness
import runloom

# Fiber-local input alphabet for the constructed common prefix and the random
# tails.  Deliberately EXCLUDES the two divergence markers ('!' and '~') so the
# only place the members can differ at-or-before len(P) is the marker slot -- this
# is what pins the closed-form longest common prefix to exactly P.
ALPHABET = "cdefghijklmnopqrstuvwxyzCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/._-"

# Two markers placed immediately after the prefix; '!' (0x21) < '~' (0x7e) and
# neither is in ALPHABET, so a member built as P + marker + tail diverges from a
# differently-marked sibling EXACTLY at index len(P), fixing the common prefix.
MARK_LO = "!"
MARK_HI = "~"

# Input shape cases, round-robined by (wid + idx) so all three branches of
# commonprefix are exercised regardless of how many rounds a worker does.
CASE_STR = 0     # list of str        -> commonprefix maps os.fspath, returns str
CASE_BYTES = 1   # list of bytes      -> maps os.fspath, returns bytes
CASE_PARTS = 2   # list of [str,...]  -> SKIPS os.fspath (sublist form), returns list
NCASES = 3


def build_str_case(rng):
    """Build a fiber-local list of str with a KNOWN common prefix P.

    Returns (m, expected).  Every member starts with P; at least one member uses
    MARK_LO and at least one uses MARK_HI immediately after P, so they diverge at
    index len(P) and the longest common prefix of the whole list is exactly P."""
    plen = rng.randint(0, 24)
    prefix = "".join(rng.choice(ALPHABET) for _ in range(plen))
    k = rng.randint(2, 8)
    members = []
    for i in range(k):
        # Guarantee both markers appear: force the first two, random after.
        if i == 0:
            mark = MARK_LO
        elif i == 1:
            mark = MARK_HI
        else:
            mark = rng.choice((MARK_LO, MARK_HI))
        tail_len = rng.randint(0, 12)
        tail = "".join(rng.choice(ALPHABET) for _ in range(tail_len))
        members.append(prefix + mark + tail)
    rng.shuffle(members)
    return members, prefix


def build_bytes_case(rng):
    """As build_str_case but bytes members; expected prefix is bytes."""
    members, prefix = build_str_case(rng)
    return [s.encode("latin-1") for s in members], prefix.encode("latin-1")


def build_parts_case(rng):
    """Build a fiber-local list of 'pathname parts' sublists with a KNOWN common
    leading part-sequence P.  commonprefix treats m[0] being a list/tuple as the
    OS-agnostic sublist form and does NOT call os.fspath; min/max compare the
    lists element-wise and the result is a list.

    Returns (m, expected_list).  Every member starts with the same leading parts
    P; at least two diverge at index len(P) via distinct marker parts, so the
    longest common leading part-sequence is exactly P."""
    plen = rng.randint(0, 5)
    prefix = ["".join(rng.choice(ALPHABET) for _ in range(rng.randint(1, 6)))
              for _ in range(plen)]
    k = rng.randint(2, 6)
    members = []
    for i in range(k):
        if i == 0:
            mark = MARK_LO       # "!" sorts below any ALPHABET part
        elif i == 1:
            mark = MARK_HI       # "~" sorts above any ALPHABET part
        else:
            mark = rng.choice((MARK_LO, MARK_HI))
        tail = ["".join(rng.choice(ALPHABET) for _ in range(rng.randint(1, 4)))
                for _ in range(rng.randint(0, 3))]
        members.append(list(prefix) + [mark] + tail)
    rng.shuffle(members)
    return members, list(prefix)


BUILDERS = (build_str_case, build_bytes_case, build_parts_case)


def purity_check(H, wid, idx, state):
    """Single-owner commonprefix purity check on fiber-local input.

    Build a list whose closed-form longest common prefix is a known P, compute it,
    assert it equals P with the right type, yield so siblings interleave, recompute
    and assert it is unchanged and still equals P."""
    rng = H.derive("cp", wid, idx)
    case = (wid + idx) % NCASES
    m, expected = BUILDERS[case](rng)

    # (1) production correctness: the result must be the closed-form prefix now.
    r1 = genericpath.commonprefix(m)
    if type(r1) is not type(expected):
        H.fail("commonprefix TYPE wrong: case={0} wid={1} got type {2!r}, "
               "expected type {3!r} -- a torn/mis-typed result the instant it was "
               "produced".format(case, wid, type(r1).__name__,
                                  type(expected).__name__))
        return
    if r1 != expected:
        H.fail("commonprefix VALUE wrong on production: case={0} wid={1} got "
               "{2!r}, expected closed-form prefix {3!r} -- torn min/max/slice "
               "over fiber-local single-owner input".format(case, wid, r1, expected))
        return

    # YIELD: let siblings run their own commonprefix churn, possibly on another
    # hub, while this fiber is parked holding r1/m/expected.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    # (2) purity across the park: recompute and assert nothing changed.
    r2 = genericpath.commonprefix(m)
    if type(r2) is not type(expected):
        H.fail("commonprefix TYPE CHANGED across a yield: case={0} wid={1} type "
               "{2!r} -> {3!r} -- the pure result's type changed while the fiber "
               "was parked".format(case, wid, type(r1).__name__,
                                   type(r2).__name__))
        return
    if r2 != r1:
        H.fail("commonprefix VALUE CHANGED across a yield: case={0} wid={1} "
               "{2!r} -> {3!r} -- a pure call on fiber-local input returned a "
               "different prefix after the park (lost-wakeup stale frame or "
               "cross-fiber scratch leak)".format(case, wid, r1, r2))
        return
    if r2 != expected:
        H.fail("commonprefix VALUE DRIFTED from closed form across a yield: "
               "case={0} wid={1} got {2!r}, expected {3!r}".format(
                   case, wid, r2, expected))
        return

    state["checks"][wid] += 1        # single-writer-per-slot, race-free (see p405)


# Sustained checks per worker, bounded by H.running().  The purity hazard only
# manifests under SUSTAINED churn -- many fibers simultaneously computing/parking
# across their commonprefix yield so the scheduler reliably interleaves a
# sibling's call before this fiber resumes.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            purity_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # ONE slot per worker (wid-indexed) -> single-writer, race-free non-vacuity
    # tally.  Allocated here where H.funcs is known (see HARD RULE 1 / p405).
    H.state = {
        "checks": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("genericpath.commonprefix purity checks (str/bytes/parts, all passed "
          "fail-fast): {0}; ops={1}".format(checks, H.total_ops()))

    # NON-VACUITY: the load-bearing purity arm actually ran.
    H.check(checks > 0,
            "no commonprefix purity checks ran -- the pure-function purity hazard "
            "was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid commonprefix.
    H.require_no_lost("genericpath.commonprefix purity")


if __name__ == "__main__":
    harness.main(
        "p572_genericpath_commonprefix_purity", body, setup=setup, post=post,
        default_funcs=8000,
        describe="genericpath.commonprefix is a pure longest-common-prefix routine "
                 "(min()/max() + trailing slice, no shared mutable state).  "
                 "LOAD-BEARING: each fiber builds its OWN list (str / bytes / "
                 "list-of-parts) whose closed-form common prefix is a known P, "
                 "computes it (must equal P with the right type), yields so "
                 "siblings interleave their own commonprefix churn on other hubs, "
                 "then recomputes and asserts the prefix is UNCHANGED and still "
                 "equals P.  A value wrong on production, or that changes across "
                 "the yield, is a runtime purity/isolation bug (lost-wakeup stale "
                 "frame, cross-fiber scratch leak, torn slice)")
