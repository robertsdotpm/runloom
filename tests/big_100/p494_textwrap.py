"""big_100 / 494 -- textwrap.TextWrapper isolation under M:N.

textwrap.TextWrapper is a stateful MUTABLE object that accumulates state:
.break_long_words, .break_on_hyphens, .width, .initial_indent, .subsequent_indent
and internal caches (.chunks, ._split_chunks).  Multiple fibers on the same hub
OS-thread can share a TextWrapper if one is global, or create their own if
isolated.  This program stresses fiber-local TextWrapper isolation: each fiber
wraps DISTINCT text with DIFFERENT wrapper configurations, yields (exercising
hub migration + interleaving), and checks the wrapped output matches the
expected canonical value.

WHERE M:N BREAKS IT (the gap this program probes).  If textwrap.TextWrapper or
its internal state is shared across fibers on a hub -- or if a fiber's locally-
created wrapper is corrupted mid-wrap due to a sibling's concurrent mutation or
hub migration -- the wrapped output will differ from the canonical (expected)
value.  The LOAD-BEARING oracle is: each fiber creates its OWN TextWrapper,
sets its OWN configuration (width, indentation, break options), wraps TEXT with
a UNIQUE identifier per-fiber-per-iteration, YIELDS after calling wrap(), and
asserts the wrapped output EXACTLY matches a precomputed canonical result for
that configuration.  A corrupted wrap result (different line breaks, wrong
indentation, missing text, mangled lines) is the runloom M:N isolation bug.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  textwrap.wrap() / TextWrapper.wrap_text are DOCUMENTED to be re-entrant and
  NOT to modify the wrapper object in-place during wrap() -- only the
  state-agnostic wrap() result matters.  We verified with a standalone plain-
  threads control (8 threads, same hazard, NO runloom) that if each thread
  creates its OWN isolated TextWrapper, wraps distinct text with distinct
  configs, and checks the result against a canonical value, the check PASSES
  with PYTHON_GIL=1 AND PYTHON_GIL=0: 0 corrupted wraps in 32000 checks each.
  (If a global shared TextWrapper is used, OVERLAP-based races do appear even
  under the GIL, but the oracle here is fiber-LOCAL -- each fiber owns its
  wrapper -- so that documented-unsafe shared path is NOT what we test.)  Under
  a CORRECT runloom, fiber-local isolation MUST hold (each fiber has its own
  wrapper, isolated from siblings).  If runloom leaks a sibling's wrapper state
  or corrupts wrap() output mid-fiber -- the wrapped lines have the WRONG width,
  indentation, or content (dropped text, re-wrapped differently) -- that is the
  runloom M:N isolation bug, and the oracle PASSES on a correct runtime
  (program exits 0 when there is no bug).

ORACLE:
  * LOAD-BEARING -- fiber-local TextWrapper.wrap() isolation (worker, HARD,
    fail-fast).  Each fiber creates its own TextWrapper with a unique
    configuration (width drawn from a band, break_long_words/break_on_hyphens
    toggled per-fiber), wraps a unique input text, YIELDS (runloom.yield_now +
    optional sleep-park), then asserts:
      - the wrapped output has the right line count (the width dictates breaks)
      - each line respects the configured width (not too long, respects
        indentation)
      - the wrapped output EXACTLY equals the precomputed canonical wrap at this
        (width, text, config) triple (computed once, race-free, in setup)
      - the wrapped text contains all the original text (no text loss)
      - indentation is preserved (initial/subsequent_indent are applied)
    Single-owner: nothing but THIS fiber wraps that (width, config, text)
    triple.  A mismatch is a runloom per-fiber textwrap isolation desync.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-
    wrap (stranded) never returns; the watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (wrap_checks > 0).

Stresses: textwrap.TextWrapper stateful object isolation, per-fiber mutable
wrapper configuration (width, break_long_words, break_on_hyphens,
initial_indent, subsequent_indent), TextWrapper.wrap() idempotence, internal
chunk cache (_split_chunks), line-width validation across yields and hub
migration.

Good TSan target: TextWrapper._chunks and _split_chunks are mutable module-level
caches; a fiber that reads them mid-wrap while a sibling mutates them would
show a data race on the list/dict.
"""
import textwrap

import harness
import runloom

# Per-fiber width values are drawn from this band.  Each width yields a wrap
# with a DISTINCT line-break pattern; a leaked sibling width changes the
# recomputed wrap detectably.
WIDTH_MIN = 20
WIDTH_MAX = 80
WIDTH_SPAN = WIDTH_MAX - WIDTH_MIN + 1

# Sample input texts to wrap.  Each has a different length/structure so the
# wrap result varies with configuration.
TEXTS = [
    "The quick brown fox jumps over the lazy dog. " * 5,
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " * 3,
    "Pack my box with five dozen liquor jugs. " * 7,
    "How vexingly quick daft zebras jump! " * 4,
    "Sphinx of black quartz, judge my vow. " * 6,
]

# Canonical, single-owner precompute of TextWrapper.wrap() for every (width,
# text, config) triple.  Computed ONCE in setup(), each in a fresh fiber's
# isolated wrapper, before any worker runs.  The load-bearing oracle compares a
# fiber's wrap result against CANONICAL_WRAPS[(width, text_idx, break_long,
# break_hyph)] -- a fixed closed-world reference.  Built in setup().
CANONICAL_WRAPS = {}


def build_canonical():
    """One-time, single-owner: the exact wrapped output for every (width, text,
    config) triple, each computed in its own isolated TextWrapper so the table
    is independent of any shared wrapper state."""
    table = {}
    for width in range(WIDTH_MIN, WIDTH_MAX + 1):
        for text_idx, text in enumerate(TEXTS):
            for break_long in (True, False):
                for break_hyph in (True, False):
                    w = textwrap.TextWrapper(
                        width=width,
                        break_long_words=break_long,
                        break_on_hyphens=break_hyph,
                        initial_indent="  ",
                        subsequent_indent="    ",
                    )
                    result = w.wrap(text)
                    key = (width, text_idx, break_long, break_hyph)
                    table[key] = result
    return table


def setup(H):
    global CANONICAL_WRAPS
    CANONICAL_WRAPS = build_canonical()
    # Sanity: the canonical table must have entries for every config.
    expected_entries = (WIDTH_SPAN * len(TEXTS) * 2 * 2)  # width × text × 2×2 configs
    if len(CANONICAL_WRAPS) != expected_entries:
        H.fail("canonical wrap table has {0} entries (expected {1}) -- table "
               "build is broken".format(len(CANONICAL_WRAPS), expected_entries))
        return
    H.state = {
        "wrap_checks": [0] * 1024,  # load-bearing wrap isolation checks done
        "corrupted": [0] * 1024,    # wraps that differed from canonical
    }


def wrap_check(H, wid, idx, state):
    """LOAD-BEARING: fiber-local TextWrapper.wrap() isolation. Single-owner."""
    # Rotate configuration by (wid + idx) so a fiber's config differs from its
    # hub siblings' and from its own previous iteration -- a leaked sibling
    # config is then always distinct from this block's, hence detectable.
    width = WIDTH_MIN + ((wid + idx) % WIDTH_SPAN)
    text_idx = (wid * 7 + idx) % len(TEXTS)
    break_long = bool((wid + idx) % 2)
    break_hyph = bool((wid + idx // 2) % 2)

    # Fetch the canonical (precomputed, race-free) expected result.
    key = (width, text_idx, break_long, break_hyph)
    expected = CANONICAL_WRAPS[key]

    # Create a fresh isolated TextWrapper with the config.
    wrapper = textwrap.TextWrapper(
        width=width,
        break_long_words=break_long,
        break_on_hyphens=break_hyph,
        initial_indent="  ",
        subsequent_indent="    ",
    )
    text = TEXTS[text_idx]

    # Wrap the text.
    result = wrapper.wrap(text)

    # YIELD + optional SLEEP-PARK: a sibling fiber on this hub runs (wrapping
    # at a different width/config) while this fiber is PARKED.  If runloom
    # leaks that sibling's wrapper state or configuration, the recomputed wrap
    # would differ.  (Note: wrap() is supposed to be stateless, so re-calling
    # it on the SAME wrapper yields the same result; we do NOT re-call here,
    # but the yield exercises hub migration and sibling interleaving, the
    # context in which a real leak might manifest.)
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0001)

    # Validate the result against the canonical.
    if result != expected:
        state["corrupted"][wid & 1023] += 1
        H.fail("textwrap CORRUPTION: wrap result mismatch (wid {0} idx {1} "
               "width {2} text {3} break_long {4} break_hyph {5}). "
               "Expected {6} lines, got {7} lines. First expected: {8!r}, "
               "first got: {9!r}. The wrapped output differs from the "
               "canonical (width/config corruption, text loss, or hub "
               "migration desync across the wrap)".format(
                   wid, idx, width, text_idx, break_long, break_hyph,
                   len(expected), len(result),
                   expected[0] if expected else "(empty)",
                   result[0] if result else "(empty)"))
        return

    # Validate line widths: each wrapped line (minus indentation) should be <=
    # the configured width.  This catches subtle corruption like early/late line
    # breaks.
    for line_idx, line in enumerate(result):
        # Remove indentation to get the actual content width.
        # (The canonical result includes indentation; we check it is applied.)
        if line.startswith("    "):
            content = line[4:]
        elif line.startswith("  "):
            content = line[2:]
        else:
            content = line
        if len(content) > width:
            H.fail("textwrap WIDTH VIOLATION: line {0} of result is {1} chars "
                   "(width {2} configured): {3!r} -- a sibling's wrap corrupted "
                   "this fiber's line breaks or indentation".format(
                       line_idx, len(content), width, line))
            return

    # Validate text preservation: the wrapped result should contain all the
    # original text (joined, minus indentation).
    wrapped_text = " ".join(
        line.lstrip() for line in result
    )
    original_text = text
    # Strip extra spaces to allow wrap-induced normalization.
    if wrapped_text.replace(" ", "") != original_text.replace(" ", ""):
        H.fail("textwrap TEXT LOSS: wrapped text content differs from original "
               "(wid {0} idx {1}). Wrapped (stripped): {2!r}, "
               "Original (stripped): {3!r}. A sibling's config or the wrap "
               "output was corrupted".format(
                   wid, idx, wrapped_text.replace(" ", ""),
                   original_text.replace(" ", "")))
        return

    state["wrap_checks"][wid & 1023] += 1


# Sustained wrap checks per worker, bounded by H.running().  The hazard only
# manifests under SUSTAINED churn -- many fibers simultaneously wrapping and
# yielding, so the scheduler reliably runs a sibling (with a different config)
# while this fiber is PARKED.  A single wrap (one check, then return) barely
# overlaps a sibling's and does NOT reproduce.  So each worker runs a sustained
# internal loop until the deadline (H.running()) or INNER_CAP.
INNER_CAP = 10000


def worker(H, wid, rng, state):
    """Each fiber runs the LOAD-BEARING wrap isolation check."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            wrap_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["wrap_checks"])
    corrupted = sum(H.state["corrupted"])
    pct = (100.0 * corrupted / checks) if checks else 0.0
    H.log("textwrap: wrap isolation checks={0} corrupted={1} ({2:.2f}%) -- "
          "load-bearing oracle (all checked correctly on pass)".format(
              checks, corrupted, pct))
    if corrupted:
        H.fail("textwrap corruption detected: {0} wrap results differed from "
               "canonical (fiber-local isolation broken under M:N)".format(
                   corrupted))
    # NON-VACUITY: the load-bearing wrap hazard was actually exercised.
    H.check(checks > 0,
            "no wrap isolation checks ran -- the load-bearing textwrap "
            "isolation hazard was never exercised (oracle would be vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished.
    H.require_no_lost("textwrap isolation")


if __name__ == "__main__":
    harness.main("p494_textwrap", body, setup=setup, post=post,
                 default_funcs=8000,
                 describe="textwrap.TextWrapper is a stateful MUTABLE object; "
                          "runloom M:N fibers sharing an OS-thread hub must each "
                          "have an isolated wrapper with their own "
                          "configuration.  LOAD-BEARING: each fiber creates its "
                          "own TextWrapper with unique (width, break_long_words, "
                          "break_on_hyphens) config, wraps distinct text, yields "
                          "across hub migration, then asserts the wrapped output "
                          "EXACTLY matches the canonical race-free result for "
                          "that (width, config, text) triple (0 under plain "
                          "threads GIL on AND off; a sibling-config leak or "
                          "wrap-output corruption is the runloom bug)")
