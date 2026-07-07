"""big_100 / 613 -- tokenize token-stream purity + untokenize round-trip
conservation under M:N.

tokenize.generate_tokens(readline) drives a STATEFUL Python lexer.  Each call to
the generator carries, across every yielded TokenInfo, a bundle of mutable driver
state hidden inside the generator frame + the module's tokenizer:

  * the readline source cursor (an io.StringIO position);
  * the INDENT/DEDENT stack (`indents`), tracking block nesting;
  * `parenlev`  -- the (), [], {} nesting depth that suppresses NL vs NEWLINE;
  * `continued` / `contstr` / `contline` -- the implicit/backslash line-
    continuation and multi-line-string accumulator state;
  * `last_line` / `lnum` / `pos` / `max` -- the running line + column cursors;
  * the FSTRING_START / FSTRING_MIDDLE / FSTRING_END nesting stack (3.12+/3.14
    PEP 701 f-string tokenizer state).

tokenize is otherwise a PURE function of its input string: for a fixed source,
list(generate_tokens(readline)) is deterministic -- the exact same TokenInfo
sequence every time.  And untokenize() of the full 5-tuple token list is the
documented EXACT inverse: the reconstructed source re-tokenizes to the same
token stream.

WHERE M:N COULD BREAK IT (the gap this program probes).  Each fiber owns its own
source string + its own generate_tokens generator, and drives it token-by-token,
YIELDING at the midpoint of tokenization (half the tokens pulled, scheduler free
to run a sibling that is itself mid-scan on ITS OWN generator).  runloom gives
each fiber its own Python frame stack, so a sibling's tokenizer state (its
indent stack, parenlev, continuation accumulator, f-string nesting stack, source
cursor) must stay completely disjoint from this fiber's generator frame.  If
per-generator / per-frame state were NOT fiber-isolated -- if a sibling's next()
mutation of ITS tokenizer state bled into this fiber's paused generator across
the yield -- this fiber would resume mid-scan with a corrupted cursor and emit a
WRONG token sequence (a dropped token, a torn/merged token, a mis-nested
INDENT/DEDENT, a stale parenlev).  That is a cross-fiber leak of single-owner
lexer state: the two halves would not reassemble into the deterministic
list(generate_tokens(src)).

WHICH ORACLES ARE LOAD-BEARING, AND WHY:

  Each fiber builds a wid+idx-UNIQUE Python source string by formatting a
  fiber-private random selection of self-contained statement templates
  (assignments, arithmetic with operators, list/dict literals, a nested def, an
  f-string -- constructs that force the tokenizer's paren/indent/f-string state
  machines to do non-trivial work).  Because the source and every token list are
  built and consumed entirely inside the fiber, they are single-owner ground
  truth: never shared with any sibling.

  * LOAD-BEARING -- STREAM PURITY (worker, HARD, fail-fast).  Drain the fiber's
    OWN generate_tokens generator into a baseline list of (type, string, start,
    end) tuples one next() at a time; at the exact MIDPOINT of the drain, YIELD
    (runloom.yield_now / sleep) so siblings interleave their own mid-scan
    generators.  Then, AFTER the drain, tokenize the SAME source AGAIN into a
    second list and assert it equals the baseline EXACTLY (same length, same
    (type, string, start, end) per token).  generate_tokens is a pure function of
    the source, so the recompute must be bit-identical to the baseline that was
    captured across the midpoint yield.  A difference means this fiber's paused
    generator resumed mid-scan with state corrupted by a sibling -- a runloom
    stream-isolation bug.  Single-owner: the source, the generator, and both
    token lists are all fiber-local.

  * LOAD-BEARING -- UNTOKENIZE ROUND-TRIP CONSERVATION (worker, HARD, fail-fast).
    A closed-world sequence law over the fiber's OWN tokens:
        toks = list(generate_tokens(src))          # full 5-tuples
        text = tokenize.untokenize(toks)           # documented exact inverse
        again = list(generate_tokens(text))        # re-tokenize
    conserves the SIGNIFICANT token sequence: the (type, string) list with the
    layout-only token classes (NEWLINE / NL / INDENT / DEDENT / COMMENT /
    ENCODING / ENDMARKER) filtered out must be IDENTICAL before and after the
    untokenize -> re-tokenize round-trip.  untokenize of the full 5-tuple stream
    is the documented inverse of tokenize, so the round-trip must neither drop,
    duplicate, merge, nor split a significant token.  A yield sits between the
    untokenize and the re-tokenize so a sibling's round-trip overlaps this
    fiber's.  Single-owner: all strings + token lists are fiber-local.  A
    sequence change is a lost/doubled/torn token.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-next()
    (parked inside the generator's readline / indent-stack pop on a desynced
    tokenizer) never returns; the watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): both load-bearing arms actually ran (checks > 0),
    tallied sharded by wid (a non-vacuity count, NOT a conservation sum, so the
    sharded `wid & MASK` tally is legitimate -- each purity/round-trip law is
    intra-fiber over a single-owner token stream, never a cross-fiber sum).

FAIL ON: a recomputed token stream that differs from the baseline captured across
the midpoint yield on the SAME single-owner source (a cross-fiber leak of
tokenizer state -- torn/dropped/merged token, mis-nested INDENT/DEDENT, stale
parenlev/f-string nesting), or an untokenize round-trip that changes the
significant token sequence of a fiber's OWN tokens, or a SIGSEGV mid-scan.  There
is NO shared source and NO shared generator anywhere in the load-bearing path, so
a failure cannot be documented shared-object behavior -- it can only be a runloom
per-fiber-generator isolation bug.

Stresses: tokenize.generate_tokens generator-frame state (indent stack, parenlev,
line-continuation + multi-line-string accumulator, PEP 701 f-string nesting
stack, source cursor) driven across a yield midway through tokenization;
generate_tokens determinism (pure recompute equality); tokenize.untokenize full
5-tuple round-trip sequence conservation; per-fiber generator instance isolation
under hub migration + sleep-park.

Good TSan / controlled-M:N-replay target: two fibers each mid-next() on their own
generate_tokens generator, sleep-parked across the midpoint yield -- a data-race
report on a tokenizer's indent stack, parenlev, or continuation accumulator, or a
deterministic-replay in which one fiber's resumed scan reads a sibling's cursor,
localizes the leak before the token-sequence oracle fires.
"""
import io
import token
import tokenize

import harness
import runloom

# Self-contained Python statement templates.  Each formats to ONE valid,
# independent module-level statement (or a def) so any random selection joined in
# order is a valid, deterministically-tokenizable Python source.  The mix forces
# the tokenizer's operator, paren-nesting, indent (the def body), string, number-
# base, and PEP 701 f-string state machines to do non-trivial work.  {n} is a
# per-line unique suffix so names never collide; {a}/{b}/{c} are integers.
TEMPLATES = (
    "x{n} = {a} + {b} * {c}\n",
    "y{n} = ({a} - {b}) / ({c} + 1)\n",
    "s{n} = 'lit_{a}_{b}'\n",
    "t{n} = \"dq_{a}_{c}\"\n",
    "lst{n} = [{a}, {b}, {c}, {a}]\n",
    "dct{n} = {{'k{a}': {b}, 'k{c}': {a}}}\n",
    "cond{n} = {a} if {b} > {c} else {c}\n",
    "def fn{n}(p, q={a}):\n    return p * q + {b}\n",
    "z{n} = {a} & {b} | {c} ^ {a}\n",
    "r{n} = {a} == {b} and {b} != {c}\n",
    "u{n} = f'fstr_{{'{a}'}}_{b}'\n",
    "h{n} = 0x{a:x} + 0b101 + {c}\n",
)

# Layout-only token classes that untokenize is NOT obliged to reproduce
# byte-for-byte in count/placement (whitespace-shaped NL/NEWLINE, INDENT/DEDENT
# re-derivation, the synthetic ENCODING/ENDMARKER bookends, COMMENT -- our
# templates emit none, but filter it for robustness).  The round-trip oracle
# asserts the SIGNIFICANT (non-layout) token sequence is conserved, which is the
# documented untokenize inverse guarantee.
LAYOUT = frozenset((
    token.NEWLINE, token.NL, token.INDENT, token.DEDENT,
    token.COMMENT, token.ENCODING, token.ENDMARKER,
))

# Lines of source per fiber program.  Enough to build a real INDENT (a def body),
# nested parens, and a multi-statement stream with a genuine midpoint to yield at;
# small enough that many checks complete under the timeout.
MIN_LINES = 3
MAX_LINES = 10

# Sustained checks per worker, bounded by H.running().  The stream-isolation
# hazard only manifests under SUSTAINED churn: many fibers simultaneously
# mid-scan on their own generators while sleep-PARKED across the midpoint yield,
# so the scheduler reliably interleaves a sibling's mid-scan before this fiber
# resumes.  One check per fiber barely overlaps a sibling's and does NOT
# reproduce.
INNER_CAP = 100000


def build_source(rng, wid, idx):
    """Build a fiber-private, wid+idx-unique valid Python source string.

    A random count of statement templates is formatted with fiber-local integers
    and a per-line unique name suffix, then concatenated.  Because every template
    is a self-contained statement, any ordered selection is valid, deterministic
    source -- the single-owner ground truth for this fiber's tokenization."""
    nlines = rng.randint(MIN_LINES, MAX_LINES)
    base = (wid * 1000003 + idx) & 0x7FFFFFFF
    lines = []
    for i in range(nlines):
        tpl = TEMPLATES[rng.randrange(len(TEMPLATES))]
        lines.append(tpl.format(
            n="{0}_{1}".format(base, i),
            a=rng.randint(0, 999), b=rng.randint(0, 999), c=rng.randint(1, 999)))
    return "".join(lines)


def sig_full(toks):
    """The full comparable signature of a token list: (type, string, start, end)
    per token.  Used for the PURITY oracle -- generate_tokens is deterministic, so
    two drains of the same single-owner source must produce identical signatures
    down to the source coordinates."""
    return [(t.type, t.string, t.start, t.end) for t in toks]


def sig_significant(toks):
    """The (type, string) sequence with layout-only tokens filtered out.  Used for
    the untokenize ROUND-TRIP oracle: untokenize's documented inverse guarantee is
    over the significant token stream, so this is what must survive round-trip."""
    return [(t.type, t.string) for t in toks if t.type not in LAYOUT]


def drain_with_midpoint_yield(src):
    """Drive a FRESH single-owner generate_tokens generator over src one next() at
    a time, yielding once at the midpoint so a sibling interleaves mid-scan.

    The generator is created and consumed entirely inside this call -- never
    shared with any sibling.  Returns the full TokenInfo list.  The two-pass
    counting (peek the token count first) keeps the midpoint exact so the yield
    lands with the generator genuinely half-drained (its indent stack / parenlev /
    cursor frozen mid-scan)."""
    gen = tokenize.generate_tokens(io.StringIO(src).readline)
    out = []
    for tok in gen:
        out.append(tok)
    # We cannot know the length before draining a generator, so do the drain in
    # one pass but yield at a position derived from a cheap pre-count: re-derive
    # the midpoint from the finished list and re-drain a second generator to the
    # midpoint, park there, then finish.  This guarantees a genuine mid-scan park
    # (the paused generator's frame holds live tokenizer state across the yield).
    mid = len(out) // 2
    gen2 = tokenize.generate_tokens(io.StringIO(src).readline)
    res = []
    pulled = 0
    for tok in gen2:
        res.append(tok)
        pulled += 1
        if pulled == mid:
            # YIELD at the exact midpoint: this fiber's generator is now half-
            # drained, its tokenizer state (indent stack, parenlev, continuation
            # accumulator, f-string nesting, source cursor) frozen inside the
            # paused generator frame.  A sibling mid-scan on ITS OWN generator
            # must not perturb this state.
            runloom.yield_now()
    return res


# ---- LOAD-BEARING arm 1: recompute purity (stream isolation) -----------------
def purity_check(H, wid, idx, state):
    """Single-owner recompute-purity isolation check.

    Drain the fiber's OWN generator with a midpoint yield to get a baseline token
    signature, then tokenize the SAME source AGAIN and assert the two signatures
    are bit-identical.  generate_tokens is a pure function of the source, so any
    difference is a cross-fiber leak of this fiber's tokenizer state across the
    midpoint yield."""
    rng = H.derive("src", wid, idx)
    src = build_source(rng, wid, idx)

    baseline_toks = drain_with_midpoint_yield(src)     # captured across the yield
    baseline = sig_full(baseline_toks)

    # Pure recompute of the SAME single-owner source: must be identical.
    recompute = sig_full(list(tokenize.generate_tokens(io.StringIO(src).readline)))

    if len(recompute) != len(baseline):
        H.fail("stream purity broken: recompute produced {0} tokens but the "
               "midpoint-yield drain produced {1} on the SAME single-owner source "
               "(wid {2} idx {3}) -- a token was dropped/merged/split because this "
               "fiber's generator resumed mid-scan with tokenizer state corrupted "
               "by a sibling across the midpoint yield. src={4!r}".format(
                   len(recompute), len(baseline), wid, idx, src))
        return
    for i in range(len(baseline)):
        if recompute[i] != baseline[i]:
            H.fail("stream purity broken: token[{0}]={1!r} on recompute != "
                   "{2!r} on the midpoint-yield drain of the SAME single-owner "
                   "source (wid {3} idx {4}) -- this fiber's tokenizer state was "
                   "corrupted by a sibling across the midpoint yield (torn token, "
                   "mis-nested INDENT/DEDENT, or stale parenlev/f-string nesting). "
                   "src={5!r}".format(
                       i, recompute[i], baseline[i], wid, idx, src))
            return

    state["purity_checks"][wid & 1023] += 1


# ---- LOAD-BEARING arm 2: untokenize round-trip conservation ------------------
def roundtrip_check(H, wid, idx, state):
    """Single-owner untokenize round-trip sequence conservation.

    toks -> untokenize(toks) -> re-tokenize conserves the SIGNIFICANT token
    sequence.  A yield sits between the untokenize and the re-tokenize so a
    sibling's round-trip overlaps this fiber's.  All strings + token lists are
    fiber-local (single-owner)."""
    rng = H.derive("rtsrc", wid, idx)
    src = build_source(rng, wid, idx)

    toks = list(tokenize.generate_tokens(io.StringIO(src).readline))
    before = sig_significant(toks)

    text = tokenize.untokenize(toks)                   # documented exact inverse
    runloom.yield_now()                                # sibling round-trips here
    if idx & 1:
        runloom.sleep(0.0003)                          # occasionally sleep-park
    again = list(tokenize.generate_tokens(io.StringIO(text).readline))
    after = sig_significant(again)

    if len(after) != len(before):
        H.fail("untokenize round-trip conservation broken: re-tokenize yielded "
               "{0} significant tokens but the original had {1} (wid {2} idx {3}) "
               "-- untokenize -> re-tokenize lost/doubled/merged a token on this "
               "fiber's OWN source. src={4!r}".format(
                   len(after), len(before), wid, idx, src))
        return
    for i in range(len(before)):
        if after[i] != before[i]:
            H.fail("untokenize round-trip conservation broken: significant "
                   "token[{0}]={1!r} after round-trip != {2!r} before, on this "
                   "fiber's OWN single-owner source (wid {3} idx {4}) -- a token "
                   "was torn/dropped/doubled across the untokenize -> re-tokenize "
                   "round-trip. src={5!r}".format(
                       i, after[i], before[i], wid, idx, src))
            return

    state["rt_checks"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """Each fiber runs BOTH load-bearing arms per iteration on its OWN fiber-local
    sources/generators: recompute-purity stream isolation (fail-fast) and
    untokenize round-trip conservation (fail-fast).  Nothing is shared, so the
    mixed churn keeps the hub busy without any shared mutation reaching either
    oracle."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            purity_check(H, wid, idx, state)            # LOAD-BEARING
            if H.failed:
                return
            roundtrip_check(H, wid, idx, state)         # LOAD-BEARING
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "purity_checks": [0] * 1024,   # LOAD-BEARING recompute-purity checks
        "rt_checks": [0] * 1024,       # LOAD-BEARING untokenize round-trip checks
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    pchecks = sum(H.state["purity_checks"])
    rchecks = sum(H.state["rt_checks"])
    H.log("tokenize[stream purity LOAD-BEARING]: {0} recompute-equality checks "
          "(all passed fail-fast) | tokenize[untokenize round-trip LOAD-BEARING]: "
          "{1} sequence-conservation checks (all passed fail-fast); ops={2}".format(
              pchecks, rchecks, H.total_ops()))

    # NON-VACUITY: both load-bearing arms actually exercised the hazard.
    H.check(pchecks > 0,
            "no recompute-purity checks ran -- the load-bearing generate_tokens "
            "mid-scan yield hazard was never exercised (vacuous)")
    H.check(rchecks > 0,
            "no untokenize round-trip checks ran -- the load-bearing untokenize "
            "sequence-conservation law was never exercised (vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-next().
    H.require_no_lost("tokenize stream purity conservation")


if __name__ == "__main__":
    harness.main(
        "p613_tokenize_stream_purity", body, setup=setup, post=post,
        default_funcs=8000,
        describe="tokenize.generate_tokens drives a stateful lexer (indent stack, "
                 "parenlev, line-continuation/multi-line-string accumulator, PEP "
                 "701 f-string nesting stack, source cursor) held in the generator "
                 "frame.  Under M:N, each fiber drives its OWN generator and YIELDS "
                 "at the midpoint of tokenization; if per-generator state is not "
                 "fiber-isolated a sibling mid-scan could corrupt this fiber's "
                 "cursor. LOAD-BEARING 1: a pure recompute of tokenize on the SAME "
                 "single-owner source MUST equal the baseline captured across the "
                 "midpoint yield (type,string,start,end per token). LOAD-BEARING 2: "
                 "tokenize.untokenize round-trip (tokenize->untokenize->tokenize) "
                 "conserves the significant token sequence of a fiber's OWN source. "
                 "Nothing is shared, so a mismatch is a runloom per-generator "
                 "stream-isolation bug, never shared-object tokenize semantics")
