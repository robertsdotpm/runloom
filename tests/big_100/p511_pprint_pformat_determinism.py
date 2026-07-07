"""big_100 / 511 -- pprint.pformat determinism / recursion-context isolation under M:N.

pprint.PrettyPrinter renders an object by walking it with a family of _format /
_pprint_* methods that thread a MUTABLE recursion-context down the call tree:

  * a `context` dict mapping id(obj) -> obj for every container currently OPEN on
    the format stack (used to detect and mark true cycles), and
  * a running `indent`/`allowance`/`level` set of positional args.

The context dict is inserted-into on the way DOWN into a container and popped on
the way back UP (``del context[objid]``).  That in-flight dict is the shared
mutable state of a single pformat() call.  A big_100 fiber that PARKS in the
middle of a pformat() (at a cooperative yield we inject between two pformat calls,
or that the runtime inserts at a preemption point) and then RESUMES on a different
hub must find its OWN recursion context intact -- not a sibling's container id
leaked in, not its own visited-id set dropped.  If M:N migration leaked a sibling
fiber's node into this fiber's `context`, a NON-cyclic node whose id() happened to
collide with an open-elsewhere container would be mis-marked as a recursion (a
spurious ``<Recursion on ...>``), or a genuine cycle's marker would be dropped --
either way the rendered text would DRIFT for a fixed, single-owner input.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom gives each fiber
its own Python frame stack, so the pformat() call's C-and-Python locals (including
the `context` dict, which is a fresh per-call local, never shared) should be fully
fiber-private.  This program pins that claim: a fiber formats a FIXED, single-owner
nested literal, PARKS across a yield, formats it AGAIN, and asserts the two
renderings are BYTE-IDENTICAL and that the text eval()s back to a structure EQUAL
to the original.  Because the input is deterministic and owned by exactly one
fiber, ANY drift in the output -- a changed byte, a spurious/missing recursion
marker, a reordered sorted-dict -- can only come from the runtime leaking or
dropping recursion-context / frame state across the park+migration.  On a correct
runtime the oracle PASSES (program exits 0).

WHICH ORACLE IS LOAD-BEARING, AND WHY (holds on plain threads):

  pformat(x, sort_dicts=True) is a PURE FUNCTION of x: for a fixed value it must
  return the same string every time, in any thread, GIL on or off (verified with a
  plain-threads control -- 8 OS threads each formatting their own fixed nested
  literal in a tight loop return byte-identical strings every iteration, 0 drift).
  sort_dicts=True removes insertion-order dependence, so even the dict rendering is
  a pure function of the (key,value) SET.  Under a CORRECT runloom this must also
  hold across a hub migration.  The input is a SINGLE-OWNER value built by, read
  by, and formatted by exactly one fiber; nothing is shared.  So a byte difference
  between the pre-yield and post-yield rendering, or an eval() that no longer
  equals the original, is a runtime frame/recursion-context isolation bug, not a
  documented pprint or shared-object behavior.

ORACLES:
  * LOAD-BEARING -- PFORMAT DETERMINISM (worker, HARD, fail-fast).  Each fiber
    builds its OWN fixed nested literal `x` of hashable literals (dicts with str
    keys, lists, tuples, and int/str/float/bool/None scalars; str tokens contain
    no '<'/'.' so they can never spoof a marker).  The fiber:
      - s1 = pprint.pformat(x, sort_dicts=True)          (baseline render)
      - YIELDS (yield_now / sleep) so a sibling reliably interleaves mid-life and
        the fiber migrates hubs while its render is "open"
      - s2 = pprint.pformat(x, sort_dicts=True)          (re-render)
      - asserts s1 == s2 BYTE-IDENTICAL (determinism across the park)
      - asserts eval(s1, {}, {}) is STRUCTURALLY EQUAL to x (round-trip)
    Single-owner: `x`, `s1`, `s2` are fiber-local; nothing is shared.

  * LOAD-BEARING -- CYCLE-MARKER ISOLATION (worker, HARD, fail-fast).  Each fiber
    builds a FIBER-LOCAL cyclic container (a list/dict holding some safe scalars
    plus a reference to itself -- exactly ONE true cycle).  pprint marks the cycle
    with a single ``<Recursion on TYPE with id=N>`` token.  The fiber renders,
    YIELDS, renders again, and asserts:
      - the marker substring "<Recursion on " appears EXACTLY ONCE (one true
        cycle -> one marker; a spurious extra marker would mean a sibling
        container id leaked into this fiber's recursion context; a missing marker
        would mean the visited-id set was dropped);
      - the two renderings are BYTE-IDENTICAL across the yield (the marker embeds
        id(obj) of the fiber-local object, which is stable across the park).
    Single-owner: the cyclic object is fiber-local, never shared.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-pformat
    (parked inside _format / _safe_repr with its context half-mutated) never
    returns; the watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

There is NO shared-mutable / MEASURED arm here: pformat of a single-owner value is
already a self-contained purity test, and a PrettyPrinter or context dict is a
per-call local, so there is nothing legitimately shared to race.  (Sharing one
PrettyPrinter instance across fibers WOULD race its internal state -- documented
non-thread-safe behavior -- so we deliberately do NOT share one; each pformat()
call in the stdlib builds its own PrettyPrinter, keeping every render single-owner.)

FAIL ON: a single-owner value's pformat output changing across a yield, an eval()
round-trip that no longer equals the original value, a true cycle's recursion
marker appearing zero or more-than-once, or a SIGSEGV inside the C repr path.

Stresses: pprint.pformat / PrettyPrinter._format recursion-context (id-keyed
`context` dict) push/pop across a park+hub-migration, _safe_repr sorted-dict
rendering determinism, cycle detection (visited-id set) isolation per fiber,
eval() round-trip of the rendered literal under M:N churn.

Good TSan / controlled-M:N-replay target: the per-call `context` dict is inserted
and deleted on every container descent; under the single-owner arm it is touched
by exactly one fiber, so a data-race report on that dict -- or a replay in which a
sibling's descent mutates a context this fiber is mid-walk of -- is the cleanest
signal before the byte-identity / eval oracle fires.
"""
import pprint

import harness
import runloom

# Scalar alphabet for random single-owner literals.  Deliberately EXCLUDES any
# character that could spoof pprint output: no '<' (would fake a "<Recursion on")
# and no '.' (keeps float/marker counting unambiguous).  All tokens eval() back to
# themselves and compare equal.
STR_TOKENS = ("alpha", "bravo", "charlie", "delta", "echo", "foxtrot",
              "golf", "hotel", "india", "juliet", "kilo", "lima")
# String KEYS for dicts -- str keys keep sort_dicts=True total-orderable and make
# the eval round-trip trivially equal (no mixed-type ordering ambiguity).
STR_KEYS = ("k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9")

MAX_DEPTH = 4          # nesting depth of the generated literal
MAX_WIDTH = 5          # max children per container

# The recursion marker pprint emits for a true cycle.  A fixed, fiber-local single
# cycle must yield exactly one of these.
RECUR_MARKER = "<Recursion on "


def build_scalar(rng):
    """A leaf literal whose repr eval()s back equal to itself and can never spoof
    a pprint marker (no '<', no '.')."""
    kind = rng.randrange(5)
    if kind == 0:
        return rng.randint(-1000, 1000)
    if kind == 1:
        return rng.choice(STR_TOKENS)
    if kind == 2:
        # A float with a clean, eval-stable repr; '.' inside a float repr is fine
        # for eval but we still must not COUNT it as a marker -- we only ever count
        # the RECUR_MARKER substring, which contains no '.', so floats are safe.
        return float(rng.randint(-500, 500))
    if kind == 3:
        return rng.choice((True, False))
    return None


def build_literal(rng, depth):
    """Build a fixed nested literal of hashable/eval-able values.  Containers are
    dict(str->value) / list / tuple so the whole thing eval()s from its pformat and
    compares structurally equal.  NON-cyclic (build_cycle handles the cycle arm)."""
    if depth <= 0:
        return build_scalar(rng)
    kind = rng.randrange(4)
    if kind == 0:
        return build_scalar(rng)
    width = rng.randint(0, MAX_WIDTH)
    if kind == 1:                                   # list
        return [build_literal(rng, depth - 1) for _ in range(width)]
    if kind == 2:                                   # tuple
        return tuple(build_literal(rng, depth - 1) for _ in range(width))
    # dict with distinct str keys
    keys = list(STR_KEYS)
    rng.shuffle(keys)
    n = min(width, len(keys))
    return {keys[i]: build_literal(rng, depth - 1) for i in range(n)}


def build_cycle(rng):
    """Build a FIBER-LOCAL container with exactly ONE true self-cycle.

    Returns (obj, kind_name).  The container holds a few safe scalars plus a single
    reference back to itself -- one cycle, so pprint emits exactly one recursion
    marker.  Fiber-local: never escapes this fiber."""
    n = rng.randint(1, 3)
    if rng.randrange(2) == 0:
        obj = [build_scalar(rng) for _ in range(n)]
        obj.append(obj)                             # the one true cycle
        return obj, "list"
    obj = {STR_KEYS[i]: build_scalar(rng) for i in range(n)}
    obj["self"] = obj                               # the one true cycle
    return obj, "dict"


def determinism_check(H, wid, rng, state):
    """LOAD-BEARING single-owner pformat determinism + eval round-trip."""
    x = build_literal(rng, MAX_DEPTH)

    s1 = pprint.pformat(x, sort_dicts=True)

    # PARK across the render boundary so a sibling interleaves and this fiber
    # migrates hubs while its recursion-context locals are (were) live.
    runloom.yield_now()
    if wid & 1:
        runloom.sleep(0.0002)

    s2 = pprint.pformat(x, sort_dicts=True)

    # Determinism: pformat of a fixed single-owner value is a pure function; the
    # two renders MUST be byte-identical.  A difference is a frame/recursion-context
    # leak across the park (the input never changed and is owned by one fiber).
    if s1 != s2:
        H.fail("pformat DRIFT across a yield (wid {0}): a fixed single-owner value "
               "rendered to two different strings.\n  pre : {1!r}\n  post: {2!r}\n"
               "-- the input is fiber-local and never mutated, so the recursion "
               "context / frame state leaked or was dropped across the park".format(
                   wid, s1, s2))
        return

    # Round-trip: the rendered text is a literal that must eval() back to a value
    # structurally EQUAL to the original.  Empty globals/locals; the data is pure
    # literals (None/True/False are keywords, not builtins) so eval is safe.
    try:
        back = eval(s1, {"__builtins__": {}}, {})
    except Exception as exc:                         # noqa: BLE001
        H.fail("pformat output failed to eval() back (wid {0}): {1}: {2}\n  text: "
               "{3!r} -- the rendered literal is malformed, a torn repr under "
               "M:N".format(wid, type(exc).__name__, exc, s1))
        return
    if back != x:
        H.fail("pformat round-trip MISMATCH (wid {0}): eval(pformat(x)) != x -- the "
               "rendered text does not reconstruct the original single-owner value.\n"
               "  text: {1!r}".format(wid, s1))
        return

    state["checks"][wid & 1023] += 1


def cycle_marker_check(H, wid, rng, state):
    """LOAD-BEARING single-owner cycle-marker isolation."""
    obj, kind = build_cycle(rng)

    c1 = pprint.pformat(obj)

    runloom.yield_now()

    c2 = pprint.pformat(obj)

    # Byte-identity across the park (the marker embeds id(obj) of a fiber-local
    # object -- stable across the yield).
    if c1 != c2:
        H.fail("cyclic pformat DRIFT across a yield (wid {0}, {1}): fiber-local "
               "cycle rendered to two strings.\n  pre : {2!r}\n  post: {3!r}".format(
                   wid, kind, c1, c2))
        return

    # Exactly one true cycle -> exactly one recursion marker.  Zero means the
    # visited-id set was dropped; more than one means a sibling container id leaked
    # into this fiber's recursion context.
    n_markers = c1.count(RECUR_MARKER)
    if n_markers != 1:
        H.fail("cycle-marker count WRONG (wid {0}, {1}): expected exactly 1 "
               "'<Recursion on ...>' for one true fiber-local cycle, got {2}.\n"
               "  text: {3!r} -- {4}".format(
                   wid, kind, n_markers, c1,
                   "visited-id set was DROPPED across the park" if n_markers == 0
                   else "a sibling's container id LEAKED into this fiber's "
                        "recursion context"))
        return

    state["cyc_checks"][wid & 1023] += 1


# Sustained iterations per worker so many fibers are simultaneously mid-pformat and
# sleep-PARKED across their yields; a single check per fiber barely overlaps a
# sibling's render and does not reliably reproduce a context leak.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            determinism_check(H, wid, rng, state)      # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            cycle_marker_check(H, wid, rng, state)     # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "checks": [0] * 1024,          # non-vacuity tally: determinism checks
        "cyc_checks": [0] * 1024,      # non-vacuity tally: cycle-marker checks
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    cyc = sum(H.state["cyc_checks"])
    H.log("pprint[single-owner LOAD-BEARING]: {0} determinism/round-trip checks + "
          "{1} cycle-marker checks (all passed fail-fast); ops={2}".format(
              checks, cyc, H.total_ops()))

    # NON-VACUITY: the load-bearing arms actually ran.
    H.check(checks > 0,
            "no pformat determinism checks ran -- the load-bearing recursion-"
            "context isolation hazard was never exercised (oracle would be vacuous)")
    H.check(cyc > 0,
            "no cycle-marker checks ran -- the cycle-detection isolation hazard "
            "was never exercised")

    # COMPLETENESS: no fiber parked-then-vanished inside _format / _safe_repr.
    H.require_no_lost("pprint pformat determinism")


if __name__ == "__main__":
    harness.main(
        "p511_pprint_pformat_determinism", body, setup=setup, post=post,
        default_funcs=4000,
        describe="pprint.pformat threads a mutable id-keyed recursion `context` "
                 "dict down its format-stack (push on descent, pop on ascent).  "
                 "Under M:N a fiber that parks mid-pformat and migrates hubs must "
                 "keep its OWN recursion context; a leaked sibling id or a dropped "
                 "visited-id set would drift the output.  LOAD-BEARING: each fiber "
                 "formats a FIXED single-owner nested literal, parks across a "
                 "yield, formats again, and asserts the render is byte-identical "
                 "and eval()s back structurally equal; a fiber-local cyclic value "
                 "must emit exactly one recursion marker.  Any drift of a "
                 "single-owner value's output across the park is a runtime frame / "
                 "recursion-context isolation bug")
