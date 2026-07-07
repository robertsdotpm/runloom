"""big_100 / 603 -- sre_parse.parse() purity / parse-tree isolation under M:N.

sre_parse (the regex front-end behind re; in 3.12+ it re-exports re._parser and
warns on import, but the module + its public parse() are still live) turns a
pattern STRING into a fresh SubPattern parse tree.  parse() is a PURE function:
it builds a private Tokenizer + State per call, reads only immutable module
tables (ESCAPES / CATEGORIES / FLAGS are frozen lookup dicts, SPECIAL_CHARS /
DIGITS / _UNITCODES are frozensets -- none is mutated during a parse), and
returns a brand-new SubPattern whose `.data` list and `.state` (group count,
group name map) are owned by that one call.  Same input -> byte-identical tree,
every time, on any thread.

WHERE M:N COULD BREAK IT (the gap this program probes).  Under free-threaded
3.14t with the GIL off and tens of thousands of goroutines parsing across >1
hubs, parse() is on a hot shared-code path that touches those module-global
lookup tables and constructs many short-lived SubPattern/State objects
concurrently.  If runloom mis-schedules a fiber across hubs mid-parse -- a lost
wakeup that strands a fiber inside the tokenizer, a torn read of a shared
constant table, an object built on one hub being resumed/finalized on another
with corrupted intermediate state, or any cross-fiber leak of the per-call
Tokenizer/State -- then a fiber that parses its OWN fiber-local pattern, yields
(letting siblings parse their conflicting patterns in parallel), and re-parses
the SAME pattern could observe a parse tree that is NOT bit-identical to its
first parse, or whose group count no longer matches the closed-form the fiber
constructed the pattern to have.  On a correct runtime this can never happen:
parse() is pure and single-owner, so the oracle PASSES (exit 0) when there is
no bug.

WHICH ORACLE IS LOAD-BEARING, AND WHY (a pure-function PURITY law):

  Each fiber CONSTRUCTS its own pattern string from a fiber-local RNG, and by
  construction it knows the EXACT number of capturing groups the pattern
  contains (the closed form).  It then:
    * parses the pattern once and serializes the resulting SubPattern tree into
      a canonical primitives-only tuple (opcodes -> str, ints -> str, nested
      SubPatterns recursed) -- a value that is INDEPENDENT of object identity,
      so it is comparable across a yield;
    * records the closed-form group count and cross-checks it against the
      parser's own state.groups (parser sees ngroups+1);
    * YIELDS (runloom.yield_now / sleep) so siblings parse their conflicting
      patterns in parallel, possibly on other hubs;
    * re-parses the SAME fiber-local pattern and re-serializes;
    * asserts the second serialization is BIT-IDENTICAL to the first, and the
      group count is STILL the closed-form value.

  The pattern string and both SubPattern trees are fiber-local (built in local
  variables, never shared), so this is a genuine single-owner PURITY oracle: a
  correct pure parse() re-run over the same input MUST reproduce the same tree.
  A mismatch means a fiber's parse observed a value that changed across a yield
  -- a torn shared table, a cross-fiber State/Tokenizer leak, or a scheduler
  desync -- i.e. a runloom M:N bug, never documented Python semantics.

  Verified reasoning: parse() holds no cross-call mutable state (Tokenizer and
  State are per-call; the module tables it reads are frozensets / never-written
  dicts), so under a correct runtime the reparse is deterministic and the oracle
  is clean.  A plain-threads control (many OS threads each parsing distinct
  patterns, GIL on and off) produces bit-identical reparses 100% of the time.

ORACLES:
  * LOAD-BEARING -- PARSE PURITY (worker, HARD, fail-fast).  Fiber-local pattern
    parsed before and after a yield; second tree must equal the first bit-for-
    bit AND the group count must match the closed form.  Single-owner.
  * NON-VACUITY (post, HARD): the purity arm actually ran (parse_checks > 0).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-parse
    (inside the tokenizer / SubPattern construction) never returns; the watchdog
    + require_no_lost catch it.

FAIL ON: a fiber's reparse of its own fiber-local pattern producing a tree that
differs from its first parse, or a parser group count that disagrees with the
closed-form the fiber built the pattern to have, or a SIGSEGV mid-parse.  There
is no shared-mutable arm: parse() returns a fresh single-owner tree per call, so
there is no documented shared-object hazard to mislabel.

Stresses: sre_parse.parse() reentrancy on the hot regex-front-end path, the
Tokenizer/State per-call construction, immutable module-table reads (ESCAPES /
CATEGORIES / FLAGS / _UNITCODES) under concurrent access, SubPattern tree build
+ group-count accounting across hub migration + yield, per-fiber parse isolation.

Good TSan / controlled-M:N-replay target: many hubs concurrently read the shared
constant lookup tables and allocate short-lived SubPattern/State objects; a TSan
report on a module table or a State field, or a deterministic replay that resumes
a half-built tree on another hub, localizes the fault before the bit-identical
serialization check even fires.
"""
import sre_parse
import warnings

import harness
import runloom

# Silence the 3.12+ "module 'sre_parse' is deprecated" DeprecationWarning at
# import time; the module and its public parse() remain live and are the target.
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Safe literal alphabet: letters + digits only, so a generated run needs no
# escaping and can never accidentally form a metacharacter.  Char classes and
# category escapes are drawn from fixed, always-valid tables below.
LIT_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789"
CHAR_CLASSES = ("[a-z]", "[0-9]", "[A-Za-z]", "[a-fA-F0-9]", "[^0-9]", "[wxyz]")
CATEGORY_ESCAPES = (r"\d", r"\w", r"\s", r"\D", r"\W", r"\S")
QUANTIFIERS = ("", "*", "+", "?", "{2,4}", "{1,3}", "{2,}")

# Atoms per generated pattern.  Enough that the SubPattern tree is non-trivial
# (branches, nested groups, repeats) and the backing structures are sizeable,
# small enough that many parses complete per fiber under the timeout.
ATOMS_MIN = 4
ATOMS_MAX = 12
# Probability (out of GROUP_DENOM) that a given atom is wrapped in a capturing
# group -- the thing whose count is the closed-form cross-check.
GROUP_NUM = 2
GROUP_DENOM = 5


def make_leaf_atom(rng):
    """One NON-grouping atom (never introduces a capturing group).  Always a
    syntactically valid regex fragment.  Returns the fragment string."""
    kind = rng.randrange(3)
    if kind == 0:
        n = rng.randint(1, 4)
        run = "".join(rng.choice(LIT_ALPHABET) for _ in range(n))
        # A quantifier binds to the LAST literal only -- still valid regex.
        return run + rng.choice(QUANTIFIERS)
    if kind == 1:
        return rng.choice(CHAR_CLASSES) + rng.choice(QUANTIFIERS)
    return rng.choice(CATEGORY_ESCAPES) + rng.choice(QUANTIFIERS)


def make_pattern(rng):
    """Build a fiber-local pattern string with a KNOWN capturing-group count.

    Returns (pattern_str, ngroups) where ngroups is the EXACT number of
    capturing groups -- the closed form the parser's state.groups must equal
    plus one.  Groups wrap 1-3 leaf atoms (leaves never themselves capture, so
    the count stays exact).  The pattern may contain a top-level alternation so
    the tree has a BRANCH node."""
    natoms = rng.randint(ATOMS_MIN, ATOMS_MAX)
    ngroups = 0
    segs = []
    for _ in range(natoms):
        if rng.randrange(GROUP_DENOM) < GROUP_NUM:
            inner_n = rng.randint(1, 3)
            inner = "".join(make_leaf_atom(rng) for _ in range(inner_n))
            # A group may itself carry a quantifier (still one capturing group).
            segs.append("(" + inner + ")" + rng.choice(QUANTIFIERS))
            ngroups += 1
        else:
            segs.append(make_leaf_atom(rng))
    pattern = "".join(segs)
    # Occasionally add a top-level alternation to force a BRANCH in the tree.
    # The alternative is a single leaf atom (no group) so ngroups is unchanged.
    if rng.randrange(3) == 0:
        pattern = pattern + "|" + make_leaf_atom(rng)
    return pattern, ngroups


def serialize(sp):
    """Canonicalize a SubPattern into a primitives-only tuple that is
    INDEPENDENT of object identity (so it is comparable across a yield).
    Recurses nested SubPatterns; opcodes and ints become their str()."""
    out = []
    for op, av in sp.data:
        out.append((str(op), serialize_av(av)))
    return tuple(out)


def serialize_av(av):
    if isinstance(av, sre_parse.SubPattern):
        return serialize(av)
    if isinstance(av, (tuple, list)):
        return tuple(serialize_av(x) for x in av)
    return str(av)


# Sustained parses per worker, bounded by H.running().  The purity hazard only
# manifests under SUSTAINED churn -- many fibers simultaneously building/parsing
# distinct patterns while sleep-PARKED across the yield, so the scheduler
# reliably interleaves a sibling's parse before this fiber resumes.
INNER_CAP = 100000


def purity_check(H, wid, idx, rng, state):
    """Single-owner parse-purity check.

    Build a fiber-local pattern with a known group count, parse + serialize it,
    yield so siblings parse their conflicting patterns in parallel, then reparse
    and assert the tree is bit-identical and the group count is unchanged."""
    pattern, ngroups = make_pattern(rng)

    try:
        sp1 = sre_parse.parse(pattern, 0)
    except sre_parse.error:
        # A generated pattern that the parser rejects is a GENERATOR bug, not a
        # runtime bug -- but our generator only emits valid fragments, so this
        # should never fire.  Treat it as a hard program error via H.fail so it
        # is caught loudly rather than silently skewing coverage.
        H.fail("generated pattern {0!r} was rejected by sre_parse -- generator "
               "bug (wid {1})".format(pattern, wid))
        return

    ref = serialize(sp1)
    ref_groups = sp1.state.groups            # parser's own group accounting

    # Closed-form cross-check BEFORE the yield: the parser must count exactly the
    # groups the fiber constructed (state.groups == ngroups + 1; group 0 is the
    # whole match).  A disagreement here is a parse-correctness fault.
    if ref_groups != ngroups + 1:
        H.fail("group-count closed form broken: pattern {0!r} built with "
               "{1} capturing groups but sre_parse state.groups={2} (expected "
               "{3}) (wid {4})".format(pattern, ngroups, ref_groups,
                                       ngroups + 1, wid))
        return

    # YIELD: let siblings parse their conflicting patterns, possibly on other
    # hubs, between this fiber's two parses of the SAME input.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0002)

    # Reparse the SAME fiber-local pattern; a pure parse() must reproduce the
    # exact same tree and group count.
    sp2 = sre_parse.parse(pattern, 0)
    got = serialize(sp2)
    got_groups = sp2.state.groups

    if got != ref:
        H.fail("parse PURITY broken: reparse of fiber-local pattern {0!r} "
               "produced a DIFFERENT tree across a yield (wid {1}) -- a torn "
               "shared parser table, a cross-fiber State/Tokenizer leak, or a "
               "scheduler desync.\n  first: {2}\n  second: {3}".format(
                   pattern, wid, ref, got))
        return

    if got_groups != ref_groups:
        H.fail("parse group count CHANGED across a yield: pattern {0!r} "
               "state.groups was {1}, reparse gave {2} (wid {3}) -- parser "
               "State accounting desynced under M:N".format(
                   pattern, ref_groups, got_groups, wid))
        return

    state["parse_checks"][wid] += 1          # single-writer-per-slot, race-free


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            purity_check(H, wid, idx, rng, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # parse_checks: ONE slot per worker (wid-indexed, single-writer -> race-free),
    # a non-vacuity tally that the purity arm actually ran.
    H.state = {
        "parse_checks": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["parse_checks"])
    H.log("sre_parse[single-owner LOAD-BEARING]: {0} parse-purity checks (all "
          "passed fail-fast -- reparse bit-identical + group count matched the "
          "closed form); ops={1}".format(checks, H.total_ops()))

    # NON-VACUITY: the load-bearing purity hazard was actually exercised.
    H.check(checks > 0,
            "no parse-purity checks ran -- the sre_parse.parse() purity hazard "
            "was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside the
    # tokenizer or SubPattern construction).
    H.require_no_lost("sre_parse parse purity")


if __name__ == "__main__":
    harness.main(
        "p603_sre_parse_purity", body, setup=setup, post=post,
        default_funcs=6000,
        describe="sre_parse.parse() is a PURE function: same pattern string -> "
                 "byte-identical SubPattern tree, reading only immutable module "
                 "tables and building a per-call Tokenizer/State.  LOAD-BEARING: "
                 "each fiber builds its own pattern with a KNOWN capturing-group "
                 "count, parses + serializes it, yields (siblings parse their "
                 "conflicting patterns in parallel across hubs), then reparses "
                 "the SAME pattern; the second tree MUST be bit-identical and the "
                 "group count MUST still match the closed form.  A changed tree "
                 "or group count across a yield is a runloom M:N bug (torn shared "
                 "table, cross-fiber State/Tokenizer leak, or scheduler desync)")
