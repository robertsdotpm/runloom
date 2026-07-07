"""big_100 / 601 -- sre_compile regex-bytecode PURITY under M:N.

sre_compile turns a parsed regular expression (an sre_parse SubPattern) into the
flat list of integer opcodes the _sre C engine executes, and sre_compile.compile()
wraps that with _sre.compile() to hand back a re.Pattern.  The whole pipeline is a
PURE FUNCTION of (pattern-string, flags): parse -> _optimize_charset / _compile /
_compile_info -> a deterministic list of ints -> a Pattern whose match behaviour is
fixed.  Nothing about a given pattern's compiled form depends on wall-clock, on
another pattern compiled before it, or on any per-thread state -- the same pattern
compiles to the byte-identical opcode list every single time (verified below).

The module DOES lean on process-global read-only tables during a compile:
_LITERAL_CODES / _REPEATING_CODES / _SUCCESS_CODES / _UNIT_CODES / _CODEBITS /
MAXCODE / _BITS_TRANS, the CATEGORY_* opcode maps, and _optimize_charset's bitmap
scratch.  Those are built once at import and only READ while compiling.  If, under
M:N with the GIL off, a fiber's compile were to observe one of those shared tables
mid-mutation, or if sre_compile kept any hidden per-thread/per-hub scratch that a
sibling fiber could stomp across a hub migration, then a fiber that compiles its
OWN fiber-local pattern, yields, and recompiles would get a DIFFERENT opcode list
the second time -- or an opcode list that disagrees with the closed-form reference
computed single-threaded before the hubs ever started.  That divergence is the
runloom bug this program hunts.

WHERE M:N BREAKS IT (the gap this program probes).  runloom runs tens of thousands
of goroutines across >1 hubs with the GIL off.  Each fiber owns a fiber-local
pattern string (drawn deterministically by wid from a fixed corpus).  It compiles
that pattern to its opcode list, PARKS across a cooperative yield so siblings
compile their OWN (different) patterns on the same and other hubs, then recompiles.
The compiled opcode list -- and the Pattern's match span on a fiber-local subject
-- MUST be bit-identical across the yield AND equal to the reference precomputed in
setup().  A single differing int, a wrong/None match span, or a crash mid-compile
is a corruption of sre_compile's compile pipeline under M:N.

WHICH ORACLE IS LOAD-BEARING, AND WHY.

  sre_compile.compile(pattern, flags) and the internal parse+_code(...) pipeline
  are documented PURE functions of their inputs.  Their output is fully determined
  by (pattern, flags); we precompute the exact expected opcode list and match span
  for every corpus pattern ONCE, single-threaded, in setup() before any hub is
  live -- that precomputed value is the closed-form reference.  Under a CORRECT
  runloom, every fiber recomputing its own pattern (before and after a yield) must
  reproduce that reference bit-for-bit.  This mirrors a plain-threads control (N OS
  threads each compiling patterns from the same corpus, GIL on AND off) which
  returns byte-identical opcode lists 100% of the time -- 0 divergences.  So on a
  correct runtime the single-owner load-bearing oracle PASSES (exit 0) and only a
  real runtime desync (torn read of a shared compile table, cross-fiber scratch
  leak, hub-migration corruption of the in-flight compile) can make it FAIL.

  Single-owner: the pattern string is an immutable fiber-local input; the parsed
  SubPattern, the opcode list, and the compiled Pattern are all built FRESH inside
  the fiber and never shared.  The reference opcode tuple / span in state is an
  immutable tuple, read-only -- reading a shared immutable object is not a race.

ORACLES:
  * LOAD-BEARING -- BYTECODE PURITY (worker, HARD, fail-fast).  For a fiber-local
    (pattern, flags, subject):
      - code0  = tuple(sre_compile._code(parse(pattern, flags), flags))  [baseline]
      - span0  = sre_compile.compile(pattern, flags).search(subject) span/None
      - YIELD (runloom.yield_now / sleep) so siblings compile their own patterns.
      - code1  = tuple(sre_compile._code(parse(pattern, flags), flags))  [recompute]
      - span1  = sre_compile.compile(pattern, flags).search(subject) span/None
      - assert code1 == code0 == reference_code   (bit-identical opcode list)
      - assert span1 == span0 == reference_span    (compiled Pattern matches same)
    A mismatch means sre_compile produced a different compilation for the same
    fiber-local input across a yield, or diverged from the single-threaded
    reference -- a runloom corruption of the compile pipeline.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-compile
    (parked inside _optimize_charset / _compile over a torn shared table) never
    returns; the watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (compile_checks>0).

FAIL ON: an opcode-list int that differs across a yield or from the precomputed
reference, a match span that differs, or a crash inside the compile pipeline.
There is NO shared-mutable arm here: sre_compile has no user-visible shared
mutable state to hammer (its tables are read-only), so the whole probe is a clean
single-owner PURITY law -- every observation is load-bearing.

Stresses: sre_compile.compile / _code / parse+_optimize_charset+_compile_info
opcode generation under M:N hub migration + yield, read-only shared compile-table
integrity (torn read), hidden per-thread/per-hub compile scratch leak across
fibers, compiled re.Pattern match-behaviour stability.

Good TSan / controlled-M:N-replay target: the compile pipeline reads several
process-global tables (_LITERAL_CODES, CATEGORY_* maps, _optimize_charset bitmap
scratch) while building each fiber's opcode list; a TSan report on one of those
reads, or a deterministic-replay that diverges one int under concurrent compiles,
localizes the corruption before the opcode-list / span equality oracle even fires.
"""
import warnings

import harness
import runloom

# sre_compile (and the sre_parse module it aliases as _parser) emit a
# DeprecationWarning on import/use; silence it so the soak log stays clean.  The
# module is still the live implementation behind re.compile in 3.14t.
warnings.filterwarnings("ignore", category=DeprecationWarning)
import sre_compile                     # noqa: E402


# Fiber-local pattern CORPUS.  Each entry is (pattern, flags, subject): a real
# regex spanning literals, alternation, greedy/lazy/possessive repeats, character
# sets + negation + ranges, named groups + backrefs, look-around, anchors, and the
# category escapes -- so the compile pipeline exercises _optimize_charset,
# _compile_info (prefix/charset info), BIGCHARSET bitmaps, and the REPEAT/BRANCH
# jump patching.  Flags are 0 (any case-insensitivity is inline (?i)) so the corpus
# is self-describing and reference-stable.  Subjects are chosen so search() returns
# a fixed span (or None), giving the functional arm a closed-form expected value.
CORPUS = [
    (r"(a|b)+c*\d{2,4}", 0, "aabccc12"),
    (r"[A-Za-z0-9_]+@\w+\.\w{2,}", 0, "user_1@example.com "),
    (r"(?P<x>\d+)-(?P=x)", 0, "id 42-42 end"),
    (r"a.*?b(?:cd|ef)$", 0, "aXXbef"),
    (r"\b\w+\b\s+", 0, "hello world"),
    (r"(?i)Hello|World", 0, "say hELLo now"),
    (r"[^\x00-\x1f]{1,10}", 0, "abcDEF!?"),
    (r"(?:ab){3,}c?d+", 0, "zzababababcdd"),
    (r"\d{3}-\d{4}", 0, "call 123-4567"),
    (r"colou?r", 0, "the color red"),
    (r"(foo|bar|baz)+", 0, "foobarbaz!"),
    (r"^\s*#.*$", 0, "   # a comment"),
    (r"[0-9a-fA-F]+", 0, "0xdeadBEEF00"),
    (r"(?=\d)\w+", 0, "1abc"),
    (r"(?!\d)\w+", 0, "abc1"),
    (r"a{2,5}?b", 0, "zaaab"),
    (r"(?:x(?:y(?:z)?)?)?end", 0, "xyzend"),
    (r"[\w.\-]+", 0, "a.b-c_d"),
    (r"\A\d+\Z", 0, "12345"),
    (r"cat|category|catalog", 0, "catalog"),
    (r"(.)\1+", 0, "aabbb"),
    (r"[^aeiou]+", 0, "xyzq"),
    (r"\bword\b", 0, "a word here"),
    (r"(?s)a.b", 0, "a\nb"),
    (r"(?:\d{1,3}\.){3}\d{1,3}", 0, "ip 10.0.0.1 x"),
    (r"(?i)[a-z]+\d*", 0, "AbC123"),
    (r"\s+|\S+", 0, "  x"),
    (r"(?P<w>\w+)\s+(?P=w)", 0, "go go"),
    (r"a(?:bc|b)c", 0, "abcc"),
    (r"[-+]?\d+(?:\.\d+)?", 0, "-3.14"),
]


def compile_code(pattern, flags):
    """The load-bearing PURE result: the flat integer opcode list sre_compile
    produces for (pattern, flags).  Reparse each call (parse consumes nothing
    reusable) and return an immutable tuple so it can be compared/stored as a
    reference.  This is the exact bytecode re.compile would hand the _sre engine."""
    parsed = sre_compile._parser.parse(pattern, flags)
    return tuple(sre_compile._code(parsed, flags))


def compile_span(pattern, flags, subject):
    """The functional companion: compile via the full sre_compile.compile() path
    (which also runs _sre.compile over the same opcode list) and return the
    search() span on the fiber-local subject, or None.  A fixed closed-form value
    for each corpus entry."""
    pat = sre_compile.compile(pattern, flags)
    m = pat.search(subject)
    return m.span() if m is not None else None


# Sustained compiles per worker, bounded by H.running().  The torn-read / scratch-
# leak hazard only manifests under SUSTAINED churn: many fibers simultaneously
# driving the compile pipeline while sleep-PARKED across their yield, so the
# scheduler reliably interleaves a sibling's compile before this fiber resumes.
INNER_CAP = 100000


def compile_check(H, wid, idx, state):
    """Single-owner bytecode-purity check for this fiber's pattern.

    The pattern/flags/subject are an immutable fiber-local input; the parsed
    SubPattern, opcode list, and compiled Pattern are built FRESH here and never
    shared.  Compile, yield so siblings compile their own patterns, recompile, and
    assert bit-identical output that also matches the single-threaded reference."""
    pattern, flags, subject = state["corpus"][wid % len(state["corpus"])]
    ref_code = state["ref_code"][wid % len(state["corpus"])]
    ref_span = state["ref_span"][wid % len(state["corpus"])]

    # Baseline: compile BEFORE the yield.
    code0 = compile_code(pattern, flags)
    span0 = compile_span(pattern, flags, subject)

    # YIELD: siblings compile their (different) patterns on this and other hubs,
    # racing the shared read-only compile tables while this fiber is parked.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    # Recompute AFTER the yield.
    code1 = compile_code(pattern, flags)
    span1 = compile_span(pattern, flags, subject)

    # Check 1: opcode list stable across the yield (a differing int == a torn
    # compile-table read or cross-fiber scratch leak corrupted this compile).
    if code1 != code0:
        H.fail("sre_compile OPCODE LIST CHANGED across a yield for pattern "
               "{0!r} (flags {1}): {2} ints before, {3} ints after; first diff at "
               "{4} (wid {5}) -- a sibling fiber's compile corrupted the shared "
               "compile tables or leaked per-hub scratch into this compile".format(
                   pattern, flags, len(code0), len(code1),
                   _first_diff(code0, code1), wid))
        return

    # Check 2: opcode list matches the single-threaded reference (not just self-
    # consistent across the yield, but the CORRECT compilation).
    if code0 != ref_code:
        H.fail("sre_compile OPCODE LIST WRONG for pattern {0!r} (flags {1}): got "
               "{2} ints, reference (single-threaded) has {3}; first diff at {4} "
               "(wid {5}) -- this fiber's compile diverged from the pure closed-"
               "form result".format(
                   pattern, flags, len(code0), len(ref_code),
                   _first_diff(code0, ref_code), wid))
        return

    # Check 3: compiled Pattern match span stable across the yield.
    if span1 != span0:
        H.fail("sre_compile compiled-Pattern SPAN CHANGED across a yield for "
               "pattern {0!r} on subject {1!r}: {2} before, {3} after (wid {4}) -- "
               "the full compile+_sre.compile path produced a different matcher "
               "across a hub migration".format(
                   pattern, subject, span0, span1, wid))
        return

    # Check 4: match span equals the single-threaded reference span.
    if span0 != ref_span:
        H.fail("sre_compile compiled-Pattern SPAN WRONG for pattern {0!r} on "
               "subject {1!r}: got {2}, reference {3} (wid {4}) -- the compiled "
               "matcher disagrees with the pure closed-form result".format(
                   pattern, subject, span0, ref_span, wid))
        return

    state["compile_checks"][wid & 1023] += 1


def _first_diff(a, b):
    """Index of the first differing element (or the length where one runs out).
    Diagnostic only."""
    n = min(len(a), len(b))
    for i in range(n):
        if a[i] != b[i]:
            return i
    return n


def worker(H, wid, rng, state):
    """Sustained single-owner compile-purity churn.  Each iteration compiles this
    fiber's fiber-local pattern twice across a yield and checks the four purity
    laws fail-fast."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            compile_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # Precompute the closed-form reference opcode list + match span for every
    # corpus pattern ONCE, single-threaded, before any hub is live.  These
    # immutable tuples are the reference the fibers must reproduce bit-for-bit.
    ref_code = [compile_code(p, f) for (p, f, s) in CORPUS]
    ref_span = [compile_span(p, f, s) for (p, f, s) in CORPUS]
    H.state = {
        "corpus": CORPUS,               # immutable fiber-local inputs
        "ref_code": ref_code,           # immutable reference opcode tuples
        "ref_span": ref_span,           # immutable reference match spans
        "compile_checks": [0] * 1024,   # LOAD-BEARING purity checks (non-vacuity)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["compile_checks"])
    H.log("sre_compile[single-owner LOAD-BEARING]: {0} bytecode-purity checks "
          "(opcode-list + match-span, all passed fail-fast); ops={1}".format(
              checks, H.total_ops()))

    # NON-VACUITY: the load-bearing arm actually exercised the compile pipeline.
    H.check(checks > 0,
            "no sre_compile bytecode-purity checks ran -- the compile-under-M:N "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-compile.
    H.require_no_lost("sre_compile bytecode purity")


if __name__ == "__main__":
    harness.main(
        "p601_sre_compile_bytecode_purity", body, setup=setup, post=post,
        default_funcs=8000,
        describe="sre_compile.compile / _code turn a parsed regex into a "
                 "deterministic integer opcode list -- a PURE function of "
                 "(pattern, flags).  LOAD-BEARING: each fiber compiles its own "
                 "fiber-local pattern to its opcode list + a compiled-Pattern "
                 "match span, yields so siblings compile their own, then "
                 "recompiles; the opcode list AND the match span MUST be bit-"
                 "identical across the yield and equal the single-threaded "
                 "reference precomputed before the hubs started.  A differing "
                 "opcode int, a changed/wrong match span, or a crash inside the "
                 "compile pipeline is a runloom corruption (torn read of a shared "
                 "compile table or a cross-fiber scratch leak).  No shared-mutable "
                 "arm: sre_compile's tables are read-only, so the whole probe is a "
                 "clean single-owner purity law")
