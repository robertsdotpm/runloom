"""big_100 / 569 -- doctest.DocTestParser example-extraction isolation +
closed-form conservation under M:N.

doctest's front end is a PARSER: DocTestParser scans a docstring and produces a
list of doctest.Example objects (and DocTestParser.get_doctest wraps them in a
single doctest.DocTest object).  Each Example carries the exact text it was cut
from -- `source` (the `>>> ...` line(s), with trailing newline), `want` (the
expected-output block), `lineno` (0-based line of the prompt in the docstring),
`indent`, and any inline option flags.  The extraction is DETERMINISTIC and PURE:
DocTestParser holds no cross-call mutable state (it drives module-level compiled
regexes -- `_EXAMPLE_RE`, `_IS_BLANK_OR_COMMENT` -- which are read-only), so
get_examples(s) over the SAME string MUST return byte-identical Example text every
time, and get_doctest(s, ...) MUST produce a DocTest whose .examples match.

Crucially there is a CLOSED-FORM ground truth: this program BUILDS each docstring
by emitting, for a fiber-private list of (expr, want) specs, one `>>> {expr}` line
immediately followed by one `{want}` line, tracking the line index of each prompt.
So for a correctly-parsed docstring we know EXACTLY, in advance:
    len(examples)      == number of specs
    examples[i].source == specs[i].expr + "\n"
    examples[i].want   == specs[i].want + "\n"
    examples[i].lineno == the recorded prompt line index
No parse is consulted to compute the expectation -- it is derived from how the
string was assembled -- so a mismatch cannot be "the parser is wrong", it can only
be a corrupted parse.

WHERE M:N COULD BREAK IT (the gap this program probes).  Each fiber owns its OWN
DocTestParser instance and its OWN fiber-private docstring, and parses it TWICE
with a yield in between (baseline parse -> yield so a sibling parses ITS OWN string
mid-flight -> re-parse).  runloom gives each fiber its own frame stack; the parser
instance, the docstring, and the produced Example/DocTest objects are all
fiber-local, never shared.  If per-fiber state were NOT isolated -- if a sibling's
parse (its regex match objects, its intermediate Example list, its scan cursor)
bled into this fiber's parse across the yield -- this fiber would resume and
observe a DIFFERENT Example list on the SECOND parse than the FIRST (a torn
source/want string, a shifted lineno, a dropped or doubled Example), or a list that
disagrees with the closed-form expectation.  That is a cross-fiber leak of
single-owner parser state.

WHICH ORACLES ARE LOAD-BEARING, AND WHY:

  Each fiber builds a wid+idx-UNIQUE docstring: a prose header naming (wid, idx),
  then a fiber-private sequence of examples drawn from a pool of pure single-line
  expressions whose repr output is a fixed, known string (`1 + 1` -> `2`,
  `sorted([3, 1, 2])` -> `[1, 2, 3]`, ...), PLUS one per-fiber-unique arithmetic
  example (`{base} + 1` -> `{base+1}` with base tied to wid,idx) so no two fibers'
  docstrings can coincide.  Because the string is assembled from those specs, the
  expected Example list is closed-form.

  * LOAD-BEARING -- EXAMPLE-EXTRACTION STABILITY + CLOSED FORM (worker, HARD,
    fail-fast).  parser.get_examples(src) -> baseline; snapshot each Example as a
    (source, want, lineno, indent) tuple.  YIELD (yield_now / sleep) so siblings
    parse their own strings mid-flight.  parser.get_examples(src) again -> compare:
    the second snapshot MUST equal the first tuple-for-tuple (byte-identical text,
    same linenos), AND both MUST equal the closed-form specs.  Single-owner: the
    parser + string + Example objects are fiber-local.  A mismatch is a runloom
    per-instance parse-isolation bug.

  * LOAD-BEARING -- DocTest OBJECT STABILITY + CLOSED FORM (worker, HARD,
    fail-fast).  parser.get_doctest(src, {}, name, None, base_lineno) -> a single
    DocTest object; snapshot (name, lineno, len(examples), per-example
    source/want/lineno).  YIELD.  Re-parse into a second DocTest; assert the
    snapshots match each other AND the closed form (dt.name == the fiber's unique
    name, dt.lineno == the passed base line, dt.examples reproduce the specs with
    relative linenos).  Single-owner DocTest; no execution, no globals touched, so
    NO process-global state (doctest only mutates sys.stdout when it RUNS examples,
    which this oracle never does).  A divergence is a parse-isolation bug.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-parse
    (parked inside a regex scan on a desynced parser) never returns; the watchdog +
    require_no_lost catch it.

  * NON-VACUITY (post, HARD): both load-bearing arms actually ran (checks > 0),
    tallied sharded by wid (a non-vacuity count, NOT a conservation sum, so the
    sharded `wid & MASK` tally is legitimate -- each law is intra-fiber over a
    single-owner parse, not a cross-fiber sum).

FAIL ON: a second get_examples/get_doctest of a fiber's OWN docstring that differs
from the first parse (a cross-fiber leak of parser state -- torn source/want,
shifted lineno, dropped/doubled Example), or a parse that disagrees with the
closed-form specs the string was built from, or a SIGSEGV mid-scan.  There is NO
shared parser and NO shared string in the load-bearing path, and the oracle never
RUNS an example (so it never touches sys.stdout / sys.displayhook), so a failure
cannot be documented shared-object behavior -- only a runloom per-instance parse-
isolation bug.

Stresses: doctest.DocTestParser.get_examples / get_doctest example extraction
(the _EXAMPLE_RE scan, source/want slicing, lineno accounting, Example/DocTest
construction) driven across a yield midway between two parses of the SAME single-
owner docstring; per-fiber parser-instance isolation under hub migration + sleep-
park.

Good TSan / controlled-M:N-replay target: two fibers each between two parses of
their own docstring, sleep-parked across the midpoint yield -- a data-race report
on a DocTestParser's transient scan state or an Example's source/want string, or a
deterministic-replay in which one fiber's second parse reads a sibling's match
cursor, localizes the leak before the Example-tuple oracle fires.
"""
import doctest

import harness
import runloom

# Pool of pure, single-line expressions whose interactive repr output is a fixed,
# known string.  Every (expr, want) pair is exercised by doctest's example scanner
# and its want-block matcher, and each want is a NON-BLANK single line (a blank
# line terminates a want block, so blank wants would change the parse shape).  No
# expression needs an import or any global -- the oracle never RUNS them, but
# keeping them self-contained makes the docstrings realistic doctest bodies.
ATOMS = (
    ("1 + 1", "2"),
    ("2 * 3", "6"),
    ("10 % 3", "1"),
    ("abs(-5)", "5"),
    ("max(4, 7, 2)", "7"),
    ("'ab' + 'cd'", "'abcd'"),
    ("'x' * 4", "'xxxx'"),
    ("'Hello'.lower()", "'hello'"),
    ("len('hello')", "5"),
    ("sorted([3, 1, 2])", "[1, 2, 3]"),
    ("list(range(3))", "[0, 1, 2]"),
    ("tuple([1, 2])", "(1, 2)"),
    ("divmod(17, 5)", "(3, 2)"),
    ("bool([])", "False"),
    ("bool([0])", "True"),
    ("2 ** 5", "32"),
    ("'a,b,c'.split(',')", "['a', 'b', 'c']"),
    ("min([9, 3, 7])", "3"),
)

# Examples per fiber docstring.  A handful is enough for a real multi-example parse
# with a yield between the two parses; small enough that many checks complete under
# the timeout.  Drawn per (wid, idx) so docstrings are wid-unique.
MIN_EXAMPLES = 3
MAX_EXAMPLES = 8

# Sustained checks per worker, bounded by H.running().  The parse-isolation hazard
# only manifests under SUSTAINED churn: many fibers simultaneously between their
# two parses while sleep-PARKED across the midpoint yield, so the scheduler
# reliably interleaves a sibling's parse before this fiber resumes.  One check per
# fiber barely overlaps a sibling's and does NOT reproduce.
INNER_CAP = 100000


def build_specs(H, wid, idx):
    """Build a fiber-private list of (expr, want) example specs.

    The first N-1 specs are drawn from ATOMS; the LAST spec is a per-fiber-unique
    arithmetic example (base tied to wid,idx) so no two fibers' docstrings can
    coincide.  Returns the specs list (length in [MIN_EXAMPLES, MAX_EXAMPLES])."""
    rng = H.derive("doc", wid, idx)
    n = rng.randint(MIN_EXAMPLES, MAX_EXAMPLES)
    specs = [ATOMS[rng.randrange(len(ATOMS))] for _ in range(n - 1)]
    # Per-fiber-unique example: a big base value tied to (wid, idx).  Its want is
    # the exact decimal repr of base+1, so the closed form is known.
    base = (wid * 100003 + idx) & 0x7FFFFFFF
    specs.append(("{0} + 1".format(base), str(base + 1)))
    return specs


def assemble(wid, idx, specs):
    """Assemble a docstring from `specs` and record each example's closed-form
    expectation.

    Emits a prose header naming (wid, idx), a blank line, then for each spec one
    `>>> {expr}` prompt line immediately followed by one `{want}` line.  Tracks the
    0-based line index of each prompt.  Returns (src, expected) where expected is a
    list of (source_text, want_text, lineno) tuples -- the closed-form ground truth
    derived purely from HOW the string was built, not from any parse."""
    lines = ["Docstring for wid {0} idx {1}.".format(wid, idx), ""]
    expected = []
    for expr, want in specs:
        lineno = len(lines)                 # 0-based index of the >>> prompt line
        lines.append(">>> " + expr)
        lines.append(want)
        # doctest stores source and want each WITH a trailing newline.
        expected.append((expr + "\n", want + "\n", lineno))
    src = "\n".join(lines) + "\n"
    return src, expected


def snapshot(examples):
    """Snapshot a list of doctest.Example objects into plain comparable tuples
    (source, want, lineno, indent) -- so an across-yield comparison is a pure value
    compare that cannot alias any parser-internal object."""
    return [(e.source, e.want, e.lineno, e.indent) for e in examples]


# ---- LOAD-BEARING arm 1: get_examples stability + closed form ----------------
def examples_isolation_check(H, wid, idx, state):
    """Single-owner get_examples stability + closed-form check.

    Parse the fiber's OWN docstring twice with a yield between, and assert the two
    Example snapshots are byte-identical AND match the closed-form specs.  A
    mismatch is a cross-fiber leak of this fiber's parser state across the yield."""
    specs = build_specs(H, wid, idx)
    src, expected = assemble(wid, idx, specs)

    parser = doctest.DocTestParser()        # fiber-local, single-owner

    baseline = snapshot(parser.get_examples(src))

    # YIELD between the two parses: this fiber is between parses of its OWN string.
    # A sibling parsing ITS OWN docstring must not perturb this fiber's next parse.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    again = snapshot(parser.get_examples(src))

    # Closed-form count: exactly one Example per spec.
    if len(baseline) != len(specs):
        H.fail("doctest parse count wrong: get_examples yielded {0} Examples but "
               "the docstring was built from {1} specs (wid {2} idx {3}) -- an "
               "Example was dropped or doubled. src={4!r}".format(
                   len(baseline), len(specs), wid, idx, src))
        return

    # Stability across the yield: second parse byte-identical to the first.
    if again != baseline:
        H.fail("doctest parse NOT STABLE across a yield: second get_examples of "
               "this fiber's OWN docstring differs from the first (wid {0} idx {1}) "
               "-- a sibling's parse corrupted this fiber's single-owner parser "
               "state. first={2!r} second={3!r} src={4!r}".format(
                   wid, idx, baseline, again, src))
        return

    # Closed form: each Example's source/want/lineno matches how the string was
    # assembled (not consulted from any parse).
    for i in range(len(specs)):
        exp_src, exp_want, exp_lineno = expected[i]
        got_src, got_want, got_lineno, _indent = baseline[i]
        if got_src != exp_src or got_want != exp_want or got_lineno != exp_lineno:
            H.fail("doctest Example[{0}] disagrees with closed form (wid {1} idx "
                   "{2}): got (source={3!r}, want={4!r}, lineno={5}) expected "
                   "(source={6!r}, want={7!r}, lineno={8}) -- a torn source/want "
                   "or a shifted lineno from a corrupted parse. src={9!r}".format(
                       i, wid, idx, got_src, got_want, got_lineno,
                       exp_src, exp_want, exp_lineno, src))
            return

    state["example_checks"][wid & 1023] += 1


# ---- LOAD-BEARING arm 2: get_doctest object stability + closed form ----------
def doctest_object_check(H, wid, idx, state):
    """Single-owner get_doctest DocTest-object stability + closed-form check.

    Wrap the fiber's OWN docstring into a DocTest twice (with a yield between) and
    assert the DocTest's name/lineno and its per-Example source/want/lineno are
    stable across the yield AND match the closed form.  No example is ever RUN, so
    this never touches sys.stdout/sys.displayhook (doctest's only process globals);
    everything is fiber-local single-owner."""
    specs = build_specs(H, wid, idx)
    src, expected = assemble(wid, idx, specs)

    name = "fiber_w{0}_i{1}".format(wid, idx)   # per-fiber-unique DocTest name
    base_lineno = (wid & 0xFFFF)                 # arbitrary fiber-local base line
    parser = doctest.DocTestParser()             # fiber-local, single-owner

    def make():
        # globs is a FRESH fiber-local dict; get_doctest copies it and we never run
        # the test, so it is never mutated by anything shared.
        dt = parser.get_doctest(src, {}, name, None, base_lineno)
        return (dt.name, dt.lineno, snapshot(dt.examples))

    baseline_name, baseline_lineno, baseline_exs = make()

    runloom.yield_now()                          # sibling parses mid-flight
    if idx & 1:
        runloom.sleep(0.0003)

    again_name, again_lineno, again_exs = make()

    # Stability across the yield.
    if (again_name, again_lineno, again_exs) != (baseline_name, baseline_lineno,
                                                 baseline_exs):
        H.fail("doctest get_doctest NOT STABLE across a yield: second parse of "
               "this fiber's OWN docstring produced a different DocTest (wid {0} "
               "idx {1}) -- a sibling's parse corrupted this fiber's single-owner "
               "state. first=({2!r},{3},{4!r}) second=({5!r},{6},{7!r})".format(
                   wid, idx, baseline_name, baseline_lineno, baseline_exs,
                   again_name, again_lineno, again_exs))
        return

    # Closed form: DocTest identity + example texts/linenos.
    if baseline_name != name or baseline_lineno != base_lineno:
        H.fail("doctest DocTest identity wrong (wid {0} idx {1}): name={2!r} "
               "lineno={3}, expected name={4!r} lineno={5}".format(
                   wid, idx, baseline_name, baseline_lineno, name, base_lineno))
        return
    if len(baseline_exs) != len(specs):
        H.fail("doctest DocTest example count wrong: {0} examples but {1} specs "
               "(wid {2} idx {3}) -- Example dropped/doubled. src={4!r}".format(
                   len(baseline_exs), len(specs), wid, idx, src))
        return
    for i in range(len(specs)):
        exp_src, exp_want, exp_lineno = expected[i]
        got_src, got_want, got_lineno, _indent = baseline_exs[i]
        if got_src != exp_src or got_want != exp_want or got_lineno != exp_lineno:
            H.fail("doctest DocTest.examples[{0}] disagrees with closed form (wid "
                   "{1} idx {2}): got (source={3!r}, want={4!r}, lineno={5}) "
                   "expected (source={6!r}, want={7!r}, lineno={8}). src={9!r}"
                   .format(i, wid, idx, got_src, got_want, got_lineno,
                           exp_src, exp_want, exp_lineno, src))
            return

    state["object_checks"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """Each fiber runs BOTH load-bearing arms per iteration on its OWN fiber-local
    parser + docstring: get_examples stability (fail-fast) and get_doctest object
    stability (fail-fast).  Nothing is shared, so the mixed churn keeps the hub busy
    without any shared mutation reaching either oracle."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            examples_isolation_check(H, wid, idx, state)    # LOAD-BEARING
            if H.failed:
                return
            doctest_object_check(H, wid, idx, state)        # LOAD-BEARING
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "example_checks": [0] * 1024,   # LOAD-BEARING get_examples checks
        "object_checks": [0] * 1024,    # LOAD-BEARING get_doctest object checks
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    echecks = sum(H.state["example_checks"])
    ochecks = sum(H.state["object_checks"])
    H.log("doctest[get_examples LOAD-BEARING]: {0} stability+closed-form checks "
          "(all passed fail-fast) | doctest[get_doctest object LOAD-BEARING]: {1} "
          "stability+closed-form checks (all passed fail-fast); ops={2}".format(
              echecks, ochecks, H.total_ops()))

    # NON-VACUITY: both load-bearing arms actually exercised the hazard.
    H.check(echecks > 0,
            "no get_examples stability checks ran -- the load-bearing doctest "
            "parse-isolation hazard was never exercised (vacuous)")
    H.check(ochecks > 0,
            "no get_doctest object checks ran -- the load-bearing doctest DocTest-"
            "object stability law was never exercised (vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-parse.
    H.require_no_lost("doctest parse isolation")


if __name__ == "__main__":
    harness.main(
        "p569_doctest_parse_isolation", body, setup=setup, post=post,
        default_funcs=8000,
        describe="doctest.DocTestParser extracts a list of doctest.Example objects "
                 "(and get_doctest wraps them in a DocTest) from a docstring -- a "
                 "deterministic, PURE parse (no cross-call mutable state).  Under "
                 "M:N, each fiber parses its OWN fiber-private docstring TWICE with "
                 "a yield between; if per-instance parser state is not fiber-"
                 "isolated a sibling parse could corrupt this fiber's result. "
                 "LOAD-BEARING 1: get_examples across a yield is byte-identical AND "
                 "matches the closed-form specs the docstring was assembled from. "
                 "LOAD-BEARING 2: get_doctest's DocTest object (name, lineno, "
                 "examples) is stable across the yield AND closed-form correct -- "
                 "no example is ever RUN, so doctest's sys.stdout/displayhook "
                 "globals are never touched.  Nothing is shared, so a mismatch is a "
                 "runloom per-instance parse-isolation bug, never shared-object "
                 "doctest semantics")
