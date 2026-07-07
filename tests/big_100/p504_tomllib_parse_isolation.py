"""big_100 / 504 -- tomllib recursive-descent parse-state isolation under M:N.

tomllib.loads() runs a hand-written recursive-descent parser (tomllib._parser)
that threads a bundle of MUTABLE per-parse state through the whole parse: a
`src`/`pos` cursor over the document text, a `_current_table` pointer into the
output dict being built, a set of "flags" recording which tables/keys have been
explicitly defined (to reject redefinition), and an implicit stack of nested
array / inline-table contexts as the descent recurses.  All of that lives on the
CALL STACK and in a per-parse Output/Flags object created fresh inside loads().

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom gives every
fiber its own Python frame stack, so a parse's cursor/flags/current-table live in
that fiber's own frames and should be untouchable by a sibling.  But the whole
point of a stress test is to falsify that: if a hub migration mid-parse were to
cross this fiber's parser cursor (`pos`), its `_current_table` pointer, or its
flags set with a SIBLING fiber's concurrent parse, the parser would resume
reading the wrong table / at the wrong offset and produce a dict that either
differs from a clean re-parse of the SAME text, or holds values belonging to
another fiber's document.  Because each fiber parses text seeded uniquely by its
own wid, any such cross-fiber bleed is DETECTABLE as a value that is not the
closed-form wid-derived expected.

WHICH ORACLE IS LOAD-BEARING, AND WHY.

  tomllib.loads(text) is a PURE FUNCTION of `text`: for a fixed valid document it
  must return an equal dict every time, and for a malformed document it must raise
  TOMLDecodeError, regardless of what any other thread/fiber is parsing.  We
  verified against a plain-threads control (8 OS threads, GIL on AND off, each
  looping loads() over its own wid-seeded doc): 100% of parses return the exact
  wid-derived dict and every malformed doc raises -- 0 cross-thread bleed.  Under
  a CORRECT runloom the same must hold.  If a fiber's parse of its OWN fiber-local
  text returns a dict that differs across a yield, or holds a value that is not
  its wid-derived expected (a sibling's value bled through the shared C-less-but-
  stateful parser), that is a runloom parse-state-isolation bug, and this single-
  owner oracle PASSES on a correct runtime (program exits 0 when there is no bug).

  SINGLE-OWNER: the document TEXT (a str built deterministically from wid), and
  both parsed dicts d0/d1, are created inside the fiber and never shared.  No
  shared mutable container is asserted on, so a FAIL cannot be documented
  shared-object M:N behavior -- only a real runtime desync.

ORACLES:
  * LOAD-BEARING -- PARSE ISOLATION (worker, HARD, fail-fast).  Each fiber:
      - synthesizes a fiber-local TOML doc with top-level keys, a nested table
        ([server] / [server.meta]), an integer array, an inline table (with a
        nested string array), and an array-of-tables ([[items]]), every scalar
        value derived from wid;
      - d0 = tomllib.loads(text);
      - yields (yield_now + occasional sleep) so a sibling reliably parses in the
        window;
      - d1 = tomllib.loads(SAME text);
      - asserts d0 == d1 (re-parse determinism across the yield), and walks the
        full nested structure asserting EVERY scalar equals its wid-derived
        closed-form expected (no cross-fiber value bleed);
      - parses a fiber-local MALFORMED doc (unterminated array, seeded by wid) and
        asserts tomllib.TOMLDecodeError is raised -- the parser's error path must
        also stay fiber-isolated (a swapped cursor could mask the error).
    A failure here is a runloom parse-state desync, not Python semantics.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-parse
    (spinning inside the recursive descent on a crossed cursor) never returns; the
    watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (parse_checks > 0).

FAIL ON: d0 != d1 across a yield, a nested scalar that is not the wid-derived
expected (cross-fiber value bleed), a malformed fiber-local doc that does NOT
raise TOMLDecodeError (error-path desync), or a SIGSEGV/torn structure mid-parse.

Stresses: tomllib._parser recursive descent under M:N, per-parse cursor/pos +
_current_table pointer + flags-set + nested inline-table/array-of-tables stack
isolation across hub migration + yield, re-parse determinism, error-path
(TOMLDecodeError) isolation.
"""
import tomllib

import harness
import runloom


# Each wid seeds a distinct base value so every scalar in a fiber's document is
# unique to that fiber; a sibling's value bleeding through the parser is therefore
# a detectable out-of-band number.  Widely spaced so wid N's band never overlaps
# wid N+1's across the ~dozen scalars per doc.
VALUE_SCALE = 100000

# Number of [[items]] array-of-tables entries and integer-array ports per doc.
# Enough nesting/length that the recursive descent does real stack work and the
# backing dicts/lists grow, without making the text huge.
N_ITEMS = 4
N_PORTS = 3


def build_doc(wid):
    """Build a fiber-local VALID TOML document string, every scalar derived from
    wid, plus the closed-form `expected` dict describing exactly what loads() must
    return.  Text and expected are both pure functions of wid -- no sharing."""
    base = wid * VALUE_SCALE
    ports = [base + 10 + i for i in range(N_PORTS)]
    region = wid % 8
    lines = []
    lines.append('title = "doc_{0}"'.format(wid))
    lines.append('magic = {0}'.format(base + 7))
    lines.append('point = {{ x = {0}, y = {1}, tags = ["t{2}", "u{2}"] }}'.format(
        base + 1, base + 2, wid))
    lines.append('')
    lines.append('[server]')
    lines.append('id = {0}'.format(wid))
    lines.append('name = "srv_{0}"'.format(wid))
    lines.append('ports = [{0}]'.format(', '.join(str(p) for p in ports)))
    lines.append('')
    lines.append('[server.meta]')
    lines.append('region = "r{0}"'.format(region))
    lines.append('weight = {0}'.format(base + 3))
    lines.append('')
    for i in range(N_ITEMS):
        lines.append('[[items]]')
        lines.append('k = {0}'.format(i))
        lines.append('v = {0}'.format(base + 1000 + i))
        lines.append('')
    text = '\n'.join(lines)

    expected = {
        'title': 'doc_{0}'.format(wid),
        'magic': base + 7,
        'point': {'x': base + 1, 'y': base + 2, 'tags': ['t{0}'.format(wid),
                                                         'u{0}'.format(wid)]},
        'server': {
            'id': wid,
            'name': 'srv_{0}'.format(wid),
            'ports': ports,
            'meta': {'region': 'r{0}'.format(region), 'weight': base + 3},
        },
        'items': [{'k': i, 'v': base + 1000 + i} for i in range(N_ITEMS)],
    }
    return text, expected


def build_bad_doc(wid):
    """Build a fiber-local MALFORMED TOML document (unterminated array), seeded by
    wid so it is not shared.  tomllib.loads() MUST raise TOMLDecodeError on it."""
    base = wid * VALUE_SCALE
    return 'bad_{0} = [{1}, {2},'.format(wid, base, base + 1)


def verify(H, wid, tag, d, expected):
    """Structurally assert d equals the wid-derived expected.  Any mismatch is a
    cross-fiber value bleed or a torn parse -- fail-fast.  Returns True on match."""
    if d != expected:
        H.fail("tomllib parse mismatch ({0}, wid {1}): loads() returned {2!r} but "
               "the wid-derived closed-form expected is {3!r} -- a cross-fiber "
               "parse-state bleed or torn structure".format(tag, wid, d, expected))
        return False
    # Spot-check a few load-bearing nested scalars explicitly (beyond ==) so a
    # torn int that happens to hash-equal is still caught by identity of value.
    base = wid * VALUE_SCALE
    if d['magic'] != base + 7:
        H.fail("tomllib magic wrong ({0}, wid {1}): {2} != {3}".format(
            tag, wid, d['magic'], base + 7))
        return False
    if d['server']['id'] != wid:
        H.fail("tomllib server.id wrong ({0}, wid {1}): {2} != {3}".format(
            tag, wid, d['server']['id'], wid))
        return False
    if d['items'][-1]['v'] != base + 1000 + (N_ITEMS - 1):
        H.fail("tomllib items[-1].v wrong ({0}, wid {1}): {2} != {3}".format(
            tag, wid, d['items'][-1]['v'], base + 1000 + (N_ITEMS - 1)))
        return False
    return True


# Sustained parse churn per worker.  The cross-parse hazard only manifests under
# many fibers simultaneously descending their parsers while sleep-PARKED across
# the yield, so the scheduler reliably interleaves a sibling's parse before this
# fiber resumes.  A single parse per fiber barely overlaps a sibling's.
INNER_CAP = 100000


def parse_check(H, wid, state):
    """Single-owner parse-isolation check (LOAD-BEARING, fail-fast)."""
    text, expected = build_doc(wid)

    d0 = tomllib.loads(text)
    if not verify(H, wid, "d0", d0, expected):
        return

    # YIELD: allow siblings to parse in the window.  If parse state (cursor,
    # _current_table, flags) is not fiber-isolated, a sibling's concurrent parse
    # could corrupt this fiber's re-parse.
    runloom.yield_now()
    if wid & 1:
        runloom.sleep(0.0003)

    d1 = tomllib.loads(text)
    if not verify(H, wid, "d1", d1, expected):
        return

    # Re-parse determinism across the yield: the SAME text must parse equal.
    if d0 != d1:
        H.fail("tomllib re-parse changed across a yield (wid {0}): d0 != d1 -- "
               "the parser produced different output for identical input, a "
               "cross-fiber parse-state desync".format(wid))
        return

    # Error-path isolation: a malformed fiber-local doc must raise TOMLDecodeError
    # even while siblings parse valid docs (a crossed cursor could mask the error).
    bad = build_bad_doc(wid)
    try:
        tomllib.loads(bad)
    except tomllib.TOMLDecodeError:
        pass
    else:
        H.fail("tomllib malformed doc did NOT raise TOMLDecodeError (wid {0}): "
               "{1!r} parsed without error -- the parser's error path desynced "
               "under M:N (a crossed cursor masked the malformation)".format(
                   wid, bad))
        return

    state["parse_checks"][wid & 1023] += 1


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            parse_check(H, wid, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "parse_checks": [0] * 1024,        # LOAD-BEARING single-owner parse checks
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    pchecks = sum(H.state["parse_checks"])
    H.log("tomllib[single-owner LOAD-BEARING]: {0} parse-isolation checks (each: "
          "loads() -> yield -> loads(same) -> d0==d1 + full wid-derived value "
          "walk + malformed-doc raises); ops={1}".format(pchecks, H.total_ops()))

    # NON-VACUITY: the load-bearing parse-isolation hazard was actually exercised.
    H.check(pchecks > 0,
            "no single-owner tomllib parse-isolation checks ran -- the load-"
            "bearing parse-state hazard was never exercised (oracle vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside the
    # recursive descent on a crossed cursor).
    H.require_no_lost("tomllib parse isolation")


if __name__ == "__main__":
    harness.main(
        "p504_tomllib_parse_isolation", body, setup=setup, post=post,
        default_funcs=6000,
        describe="tomllib.loads() runs a recursive-descent parser threading mutable "
                 "per-parse state (cursor pos, _current_table pointer, flags set, "
                 "nested array/inline-table stack) through the descent.  Under M:N, "
                 "a hub migration mid-parse could cross this fiber's cursor/flags "
                 "with a sibling parse.  LOAD-BEARING: each fiber builds a fiber-"
                 "local wid-seeded TOML doc, loads() to d0, yields, loads() the same "
                 "text to d1, asserts d0==d1 and every nested scalar equals its wid-"
                 "derived expected, and a malformed fiber-local doc raises "
                 "TOMLDecodeError.  A parsed value that is not the wid-derived "
                 "expected, a re-parse that changes across the yield, or a masked "
                 "error is the runloom parse-state-isolation bug")
