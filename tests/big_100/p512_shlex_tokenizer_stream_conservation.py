"""big_100 / 512 -- shlex.shlex incremental tokenization isolation + quote
round-trip conservation under M:N.

shlex.shlex is a STATEFUL streaming lexer.  A live shlex.shlex instance carries,
across every get_token() call, a bundle of mutable per-instance state:

  * `pushback`     -- a collections.deque of tokens pushed back (peek/unget);
  * `pushback_chars` -- a deque of characters pushed back mid-scan;
  * `state`        -- the single-character lexer state machine cursor
                      (' ', 'a', quote char, escape char, or None at EOF);
  * `token`        -- the partially-accumulated current token string;
  * `lineno`       -- the running line counter;
  * `instream` / the char-source iterator it reads from;
  * `whitespace_split`, `posix`, and the character-class tables.

shlex.split(s) is literally: build one shlex(s, posix=True), set
whitespace_split=True, and drain it with `list(lex)`.  So an incremental
get_token() loop over the SAME string, with the SAME flags, MUST reproduce the
EXACT token sequence shlex.split(s) returns -- the lexer is deterministic.

WHERE M:N COULD BREAK IT (the gap this program probes).  Each fiber owns its own
shlex.shlex instance and drives it get_token()-by-get_token(), YIELDING at the
midpoint of tokenization (half the tokens pulled, scheduler free to run a sibling
that is mid-scan on ITS OWN lexer).  runloom gives each fiber its own frame stack,
so a sibling's lexer state (its pushback deque, its `state` cursor, its `token`
accumulator, its char-source iterator) must stay completely disjoint from this
fiber's.  If instance state were NOT fiber-isolated -- if a sibling's get_token()
mutation of ITS deque/state/token bled into this fiber's lexer across the yield --
this fiber would resume mid-scan with a corrupted cursor and emit a WRONG token
sequence (a dropped token, a torn/merged token, a mis-split at a quote boundary,
or a stale pushback char).  That is a cross-fiber leak of single-owner lexer
state: the halves would not reassemble into shlex.split(s).

WHICH ORACLES ARE LOAD-BEARING, AND WHY:

  Each fiber builds a wid+idx-UNIQUE command line by shlex.quote()-joining a
  fiber-private list of raw token strings (spaces, embedded quotes, shell
  metacharacters, escapes -- tokens that FORCE real quoting so the lexer's quote
  state machine does non-trivial work).  Because the command line is built by
  quote-joining that private list, shlex.split(cmdline) == that exact list is the
  ground truth for this fiber.

  * LOAD-BEARING -- STREAM ISOLATION (worker, HARD, fail-fast).  Drain the fiber's
    OWN shlex.shlex incrementally: pull the first half of the tokens with
    get_token(), YIELD (runloom.yield_now / sleep) so siblings interleave their
    own mid-scan lexers, then pull the remaining tokens.  Assert the concatenated
    incremental sequence equals shlex.split(cmdline) EXACTLY (same length, same
    tokens in order) -- which in turn equals the fiber's private ground-truth
    list.  Single-owner: the lexer, the command line, and the token list are all
    fiber-local, never shared.  A mismatch means this fiber's lexer resumed with
    state corrupted by a sibling -- a runloom stream-isolation bug.

  * LOAD-BEARING -- QUOTE ROUND-TRIP CONSERVATION (worker, HARD, fail-fast).  A
    closed-world multiset law over the fiber's OWN tokens:
        toks  = shlex.split(cmdline)
        rebar = ' '.join(shlex.quote(t) for t in toks)   # re-quote each token
        again = shlex.split(rebar)                        # re-split
    conserves the token MULTISET: sorted(again) == sorted(toks), and
    len(again) == len(toks).  shlex.quote is the documented inverse of the
    posix split for a single field, so quote->join->split must neither drop,
    duplicate, merge, nor split a token.  A yield sits between the split and the
    re-split so a sibling's re-quoting overlaps this fiber's.  Single-owner: all
    strings are fiber-local.  A multiset change is a lost/doubled/torn token.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-get_token()
    (parked inside the char-source iterator / deque pop on a desynced lexer) never
    returns; the watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arms actually ran (checks > 0),
    tallied sharded by wid (a non-vacuity count, NOT a conservation sum, so the
    sharded `wid & MASK` tally is legitimate -- the conservation law itself is
    intra-fiber over a single-owner token multiset, not a cross-fiber sum).

FAIL ON: an incremental get_token() sequence that differs from shlex.split() on
the SAME single-owner string (a cross-fiber leak of lexer state -- torn/dropped/
merged token, mis-split quote boundary), or a quote round-trip that changes the
token multiset of a fiber's OWN tokens, or a SIGSEGV mid-scan.  There is NO shared
lexer and NO shared string anywhere in the load-bearing path, so a failure cannot
be documented shared-object shlex behavior -- it can only be a runloom
per-fiber-instance isolation bug.

Stresses: shlex.shlex.get_token() state machine (state cursor, pushback/
pushback_chars deques, token accumulator, char-source iterator) driven across a
yield midway through tokenization; shlex.split C-ish drain vs incremental drain
equivalence; shlex.quote round-trip multiset conservation; per-fiber lexer
instance isolation under hub migration + sleep-park.

Good TSan / controlled-M:N-replay target: two fibers each mid-get_token() on
their own shlex.shlex, sleep-parked across the midpoint yield -- a data-race
report on a shlex instance's `pushback` deque, `state` char, or `token` string,
or a deterministic-replay in which one fiber's resumed scan reads a sibling's
pushback char, localizes the leak before the token-sequence oracle fires.
"""
import shlex

import harness
import runloom

# Raw token "atoms": a mix of plain words, words with embedded spaces, embedded
# quotes, shell metacharacters, and escape-worthy characters.  Each forces
# shlex.quote() to wrap/escape the token, so the lexer's quote + escape state
# machine does non-trivial work when the command line is re-parsed.  Every atom
# is a NON-EMPTY string (an empty field quotes to '' and round-trips, but we keep
# atoms non-empty so the half-and-half midpoint split is unambiguous).
ATOMS = (
    "foo", "bar", "baz", "--flag=value", "/a/b/c",
    "with space", "two  spaces", "tab\tinside",
    "single'quote", 'double"quote', "both'\"mixed",
    "meta$var", "pipe|semi;amp&", "glob*?[x]", "paren(y)",
    "back\\slash", "new\nline", "hash#mark", "eq=sign", "dash-dash",
    "unicode_snowman", "trailing ", " leading", "127.0.0.1:8000",
)

# Tokens per fiber command line.  A handful is enough for a real half/half split
# with a yield between the halves; small enough that many checks complete under
# the timeout.  Drawn per (wid, idx) so command lines are wid-unique.
MIN_TOKENS = 5
MAX_TOKENS = 14

# Sustained checks per worker, bounded by H.running().  The stream-isolation
# hazard only manifests under SUSTAINED churn: many fibers simultaneously
# mid-scan on their own lexers while sleep-PARKED across the midpoint yield, so
# the scheduler reliably interleaves a sibling's mid-scan before this fiber
# resumes.  One check per fiber barely overlaps a sibling's and does NOT
# reproduce.
INNER_CAP = 100000


def build_tokens(rng):
    """Build a fiber-private list of raw token strings drawn from ATOMS.

    Returns the list (length in [MIN_TOKENS, MAX_TOKENS]).  Order is preserved
    when the command line is quote-joined, so this list is the ground truth for
    shlex.split(cmdline)."""
    n = rng.randint(MIN_TOKENS, MAX_TOKENS)
    return [ATOMS[rng.randrange(len(ATOMS))] for _ in range(n)]


def make_cmdline(tokens):
    """Quote-join the fiber-private token list into ONE command line.

    Because each token is shlex.quote()'d and space-joined, shlex.split() of the
    result yields EXACTLY `tokens` (order preserved) -- the posix-split inverse of
    quote-join.  This is the fiber's single-owner ground-truth string."""
    return " ".join(shlex.quote(t) for t in tokens)


def drain_incremental(cmdline, midpoint):
    """Drive a FRESH single-owner shlex.shlex over cmdline get_token()-by-token,
    yielding once after `midpoint` tokens so a sibling interleaves mid-scan.

    Uses the SAME configuration shlex.split() uses (posix=True,
    whitespace_split=True) so the incremental drain must reproduce
    shlex.split(cmdline) exactly on a correct runtime.  The lexer is created and
    consumed entirely inside this call -- never shared with any sibling."""
    lex = shlex.shlex(cmdline, posix=True)
    lex.whitespace_split = True
    out = []
    pulled = 0
    while True:
        tok = lex.get_token()          # posix: None at EOF
        if tok is None:
            break
        out.append(tok)
        pulled += 1
        if pulled == midpoint:
            # YIELD at the exact midpoint of tokenization: this fiber's lexer is
            # now HALF-drained (its pushback/state/token/char-source frozen).  A
            # sibling mid-scan on ITS OWN lexer must not perturb this state.
            runloom.yield_now()
    return out


# ---- LOAD-BEARING arm 1: incremental vs one-shot (stream isolation) ----------
def stream_isolation_check(H, wid, idx, state):
    """Single-owner incremental-drain isolation check.

    Build a wid+idx-unique command line, drain it incrementally with a midpoint
    yield, and assert the token sequence equals shlex.split() of the SAME string
    (== the fiber's private ground-truth list).  A mismatch is a cross-fiber leak
    of this fiber's lexer state across the yield."""
    rng = H.derive("cmd", wid, idx)
    tokens = build_tokens(rng)
    cmdline = make_cmdline(tokens)

    # One-shot ground truth (shlex.split builds+drains its own throwaway lexer).
    oneshot = shlex.split(cmdline)

    # Sanity: quote-join really is the split inverse for this fiber's tokens.
    # (If this ever fails it is a construction bug in THIS test, not runloom, so
    # it is caught here and reported distinctly.)
    if oneshot != tokens:
        H.fail("test-construction error: shlex.split(quote-join) != tokens for "
               "wid {0} idx {1} -- got {2!r} expected {3!r} (NOT a runloom bug; "
               "the atom set produced a non-round-tripping command line)".format(
                   wid, idx, oneshot, tokens))
        return

    midpoint = len(tokens) // 2
    incremental = drain_incremental(cmdline, midpoint)

    # Load-bearing assertion: incremental drain (across a midpoint yield) ==
    # one-shot drain.  Both are over this fiber's OWN single-owner string.
    if len(incremental) != len(oneshot):
        H.fail("stream isolation broken: incremental get_token() drain yielded "
               "{0} tokens but shlex.split() yielded {1} on the SAME single-owner "
               "command line (wid {2} idx {3}) -- a token was dropped/merged/"
               "split because this fiber's lexer resumed mid-scan with state "
               "corrupted by a sibling across the midpoint yield. cmdline={4!r} "
               "incremental={5!r} oneshot={6!r}".format(
                   len(incremental), len(oneshot), wid, idx, cmdline,
                   incremental, oneshot))
        return
    for i in range(len(oneshot)):
        if incremental[i] != oneshot[i]:
            H.fail("stream isolation broken: incremental token[{0}]={1!r} != "
                   "shlex.split token[{0}]={2!r} on the SAME single-owner command "
                   "line (wid {3} idx {4}) -- this fiber's lexer state was "
                   "corrupted by a sibling across the midpoint yield (torn token "
                   "or mis-split quote boundary). cmdline={5!r}".format(
                       i, incremental[i], oneshot[i], wid, idx, cmdline))
            return

    state["stream_checks"][wid & 1023] += 1


# ---- LOAD-BEARING arm 2: quote round-trip conservation -----------------------
def quote_conservation_check(H, wid, idx, state):
    """Single-owner quote round-trip multiset conservation.

    toks -> re-quote each -> join -> re-split conserves the token MULTISET.  A
    yield sits between the split and the re-split so a sibling's re-quoting
    overlaps this fiber's.  All strings are fiber-local (single-owner)."""
    rng = H.derive("qcmd", wid, idx)
    tokens = build_tokens(rng)
    cmdline = make_cmdline(tokens)

    toks = shlex.split(cmdline)
    runloom.yield_now()                # sibling re-quotes during our round-trip
    rebar = " ".join(shlex.quote(t) for t in toks)
    if idx & 1:
        runloom.sleep(0.0003)          # occasionally sleep-park across the trip
    again = shlex.split(rebar)

    # Length conservation: no token dropped, doubled, merged, or split.
    if len(again) != len(toks):
        H.fail("quote round-trip conservation broken: re-split yielded {0} "
               "tokens but the original had {1} (wid {2} idx {3}) -- quote->join->"
               "split lost/doubled/merged a token. toks={4!r} again={5!r}".format(
                   len(again), len(toks), wid, idx, toks, again))
        return
    # Multiset conservation: exact same tokens (order may legitimately differ?
    # no -- quote-join preserves order too, but we assert the MULTISET so the law
    # is robust to any legal reordering while still catching a torn token).
    if sorted(again) != sorted(toks):
        H.fail("quote round-trip conservation broken: token MULTISET changed "
               "across quote->join->split on this fiber's OWN tokens (wid {0} idx "
               "{1}) -- a token was torn/dropped/doubled. sorted(toks)={2!r} "
               "sorted(again)={3!r}".format(
                   wid, idx, sorted(toks), sorted(again)))
        return

    state["quote_checks"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """Each fiber runs BOTH load-bearing arms per iteration on its OWN fiber-local
    lexers/strings: incremental-vs-oneshot stream isolation (fail-fast) and quote
    round-trip conservation (fail-fast).  Nothing is shared, so the mixed churn
    keeps the hub busy without any shared mutation reaching either oracle."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            stream_isolation_check(H, wid, idx, state)      # LOAD-BEARING
            if H.failed:
                return
            quote_conservation_check(H, wid, idx, state)    # LOAD-BEARING
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "stream_checks": [0] * 1024,   # LOAD-BEARING incremental-isolation checks
        "quote_checks": [0] * 1024,    # LOAD-BEARING quote round-trip checks
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    schecks = sum(H.state["stream_checks"])
    qchecks = sum(H.state["quote_checks"])
    H.log("shlex[stream isolation LOAD-BEARING]: {0} incremental-vs-oneshot "
          "checks (all passed fail-fast) | shlex[quote round-trip LOAD-BEARING]: "
          "{1} multiset-conservation checks (all passed fail-fast); ops={2}".format(
              schecks, qchecks, H.total_ops()))

    # NON-VACUITY: both load-bearing arms actually exercised the hazard.
    H.check(schecks > 0,
            "no incremental stream-isolation checks ran -- the load-bearing "
            "shlex.shlex mid-scan yield hazard was never exercised (vacuous)")
    H.check(qchecks > 0,
            "no quote round-trip conservation checks ran -- the load-bearing "
            "shlex.quote multiset law was never exercised (vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-get_token().
    H.require_no_lost("shlex tokenizer stream conservation")


if __name__ == "__main__":
    harness.main(
        "p512_shlex_tokenizer_stream_conservation", body, setup=setup, post=post,
        default_funcs=8000,
        describe="shlex.shlex is a stateful streaming lexer (pushback deques, "
                 "state cursor, token accumulator, char-source iterator).  Under "
                 "M:N, each fiber drives its OWN lexer incrementally and YIELDS at "
                 "the midpoint of tokenization; if per-instance lexer state is not "
                 "fiber-isolated a sibling mid-scan could corrupt this fiber's "
                 "cursor. LOAD-BEARING 1: incremental get_token() drain across a "
                 "midpoint yield MUST equal shlex.split() of the SAME single-owner "
                 "string. LOAD-BEARING 2: shlex.quote round-trip (split->quote-"
                 "join->split) conserves the token MULTISET of a fiber's OWN "
                 "tokens.  Nothing is shared, so a mismatch is a runloom per-"
                 "instance stream-isolation bug, never shared-object shlex "
                 "semantics")
