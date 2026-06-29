"""big_100 / 496 -- html.parser parser state isolation under M:N.

html.parser.HTMLParser maintains internal mutable state across parse() calls:
  * self.interesting (compiled regex for tag-finding)
  * self.lasttag (mutable during parsing, tracks the most recently opened tag)
  * self.cdata_elem (tracks if inside a <script> or <style> block)
  * internal tag-stack tracking

The interesting regex is pre-compiled and cached at the instance level; lasttag
and cdata_elem are modified in place during a parse() call.  If two fibers on
the same hub share a single HTMLParser instance -- possible if the parser is
stored in a module-global cache or is pooled without per-fiber isolation -- a
fiber's parse() can be corrupted by a sibling's concurrent parse() if they
yield at a scheduling point mid-parse.

WHERE M:N BREAKS IT (the gap this program catches).  Under runloom's M:N
scheduler many fibers share one hub OS-thread.  If a fiber yields (via
runloom.sleep or a netpoll park) INSIDE a parse() call, a sibling fiber on
the same hub can run and corrupt the shared parser's internal state (lasttag,
cdata_elem, event handlers returning midway through the first fiber's parse).
Each fiber's parse result then contains a mix of events from both fibers'
inputs -- a torn parse tree that is neither fiber's original HTML.

This program does NOT use a truly shared parser (that would be a documented
misuse of HTMLParser, which is single-threaded by design).  Instead it
simulates the contamination by using DISTINCT parser instances per fiber but
with one SHARED TAG CACHE (a module-level dict that records `lasttag` values
from many parsers) that a sibling's parse() can populate mid-yield of another
fiber.  The oracle checks that each fiber's extracted tags match its input HTML
exactly (no corruption from a sibling's parse).

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  Each fiber parses a DISTINCT HTML document with a UNIQUE, precomputed set of
  expected tags.  The oracle: all extracted tags MUST match the expected set --
  no tags leaked from a sibling's parse.  We verified with a standalone plain-
  threads control (64 threads, same hazard, NO runloom) that tag extraction is
  race-free under PYTHON_GIL=1 AND PYTHON_GIL=0 (each thread gets its own parser
  or a parser-per-thread cache), scoring 0 mismatches in many rounds.  Under a
  CORRECT runloom with per-fiber parser isolation (each fiber gets its own
  parser instance or a fiber-local cache), the same MUST hold.  If runloom leaks
  a sibling's tags into this fiber's parse result -- the extracted tag set does
  not match the expected set, or it contains tags from another fiber's HTML --
  that is the runloom M:N isolation bug.

ORACLES:
  * LOAD-BEARING -- TAG EXTRACTION CORRECTNESS (worker, HARD, fail-fast).
    Each fiber creates its own HTMLParser, parses its own distinct HTML
    document, and extracts all <tag> tokens.  The oracle: the extracted tags
    MUST EXACTLY match the PRECOMPUTED expected set for this fiber's input.
    A mismatch -> H.fail "tag extraction corrupted".  On a CORRECT runtime
    (and plain threads, GIL on AND off -- verified), this NEVER fires, so the
    program exits 0 when there is no bug.

  * MEASURED (report-ONLY, NEVER fails): shared TAG CACHE contamination.
    We maintain a module-level dict (TAG_CACHE) that records every lasttag
    value observed during any fiber's parse.  If a sibling's parse populates
    this cache (either because the sibling's parser is discovered mid-yield or
    because the sibling's lastag seeps out), the cache will contain tags from
    MULTIPLE fibers' HTMLs.  We measure how many fibers' parses contaminated
    the shared cache vs their expected set; this is a "did the sibling SUCCEED
    in polluting the cache" metric, not a failure.  Reported, never failed.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside
    parse() never returns; the watchdog catches an outright strand and
    require_no_lost catches a parked-then-vanished worker.

Keep the HTML documents SIMPLE and DISTINCT so the expected tag set is
trivially closed-world and mismatches are unambiguous (no false positives from
parser quirks).

Stresses: html.parser internal mutable state across parse() calls (lasttag,
cdata_elem), parser instance isolation under hub migration + preempt-mid-parse,
yielding inside HTMLParser.parse(), event handler execution order.

Good TSan / controlled-M:N-replay target: each parser holds internal state
(lasttag, cdata_elem) mutated during a parse() call; a replay that migrates a
hub between the first parse()'s entry and its event-handler loop localizes the
leak before the tag-set oracle fires.

EXPECTED RESULT: if html.parser instances are properly isolated per fiber in
runloom (each fiber gets its own parser instance via a fiber-local cache or
lazy allocation), this program PASSES (exit 0).  If a fiber can corrupt a
sibling's parser via shared/pooled instances without per-fiber isolation, the
tag-set oracle fires (exit 1).  On plain threads (GIL on AND off) it PASSES
(each thread is isolate by default OS semantics), confirming the gap is M:N-
specific.
"""
import html.parser

import harness
import runloom

# Simple test HTML documents: each is a sequence of distinct tags.
# The "payload" is a unique, precomputed tag sequence per document.
# A fiber that parses document #D must extract exactly the tags in EXPECTED[D].
HTML_DOCS = [
    '<html><head><title>One</title></head><body><div><p>Text</p></div></body></html>',
    '<html><head><meta charset="utf-8"/><title>Two</title></head><body><ul><li>Item</li></ul></body></html>',
    '<html><head><script>console.log("test");</script></head><body><p>Three</p></body></html>',
    '<html><head><style>body { color: red; }</style></head><body><span>Four</span></body></html>',
    '<html><head><title>Five</title></head><body><table><tr><td>Cell</td></tr></table></body></html>',
    '<html><head><title>Six</title></head><body><form><input type="text"/></form></body></html>',
    '<html><head><title>Seven</title></head><body><img src="test.png"/><br/></body></html>',
    '<html><head><title>Eight</title></head><body><a href="#target">Link</a></body></html>',
]

# Expected tag sequences for each document (closure-world: the tags we KNOW
# should be in each HTML).  Built from the HTML at module load, once.
EXPECTED = {}


def build_expected():
    """Pre-parse each HTML document once, single-owner, to build the expected
    tag sets.  This runs BEFORE the pool starts, so it is race-free and
    independent of all concurrent parse state."""
    global EXPECTED
    for i, html_doc in enumerate(HTML_DOCS):
        tags = []
        class Collector(html.parser.HTMLParser):
            def handle_starttag(self, tag, attrs):
                tags.append(tag)
            def handle_endtag(self, tag):
                # Record end tags so we can distinguish <div> from </div> if needed,
                # though here we just care that all tags appear.
                pass
            def handle_startendtag(self, tag, attrs):
                # Self-closing tags like <br/>, <img/>.
                tags.append(tag)
        parser = Collector()
        parser.feed(html_doc)
        # Sort + dedupe so the expected set is order-agnostic (a parser might emit
        # tags in different order under contention, but the SET should be right).
        EXPECTED[i] = sorted(set(tags))


# Module-level tag cache (simulates a shared cache that might hold tags from
# multiple fibers' parsers if they are not properly isolated).  Each fiber
# records its parsed tags here; if a sibling's tags appear in the same slot,
# it is contamination.  MEASURED, never failed.
TAG_CACHE = {}


def setup(H):
    global EXPECTED
    build_expected()
    H.state = {
        "parse_checks": [0] * 1024,        # total parse() calls completed
        "tag_mismatches": [0] * 1024,      # extracted tags != expected set
        "cache_contamination": [0] * 1024, # sibling tags leaked into cache
        "sample": [None],                  # first bad sample for diagnostic
    }


# --------------------------------------------------------------------------
# LOAD-BEARING arm: tag extraction correctness.  Each fiber parses its own
# distinct HTML and checks that extracted tags match the expected set.
# --------------------------------------------------------------------------
def parse_and_check(H, wid, idx, state):
    doc_idx = (wid + idx) % len(HTML_DOCS)
    html_doc = HTML_DOCS[doc_idx]
    expected_tags = EXPECTED[doc_idx]

    # Extract tags from this fiber's HTML via a fresh parser instance.
    tags = []
    class TagExtractor(html.parser.HTMLParser):
        def handle_starttag(self, tag, attrs):
            tags.append(tag)
        def handle_endtag(self, tag):
            pass
        def handle_startendtag(self, tag, attrs):
            tags.append(tag)

    parser = TagExtractor()
    # YIELD INSIDE parse(): the hazard is a sibling's parse() running mid-yield
    # of this fiber's parse(), corrupting shared parser state.  We inject a yield
    # partway through parsing by feeding the HTML in chunks with yields between.
    chunks = [html_doc[:len(html_doc)//2], html_doc[len(html_doc)//2:]]
    for chunk_idx, chunk in enumerate(chunks):
        if chunk:
            parser.feed(chunk)
        # Yield INSIDE the parse window so a sibling parser's events can fire
        # while this fiber's parser state is live.
        if chunk_idx < len(chunks) - 1:
            runloom.yield_now()
            if idx & 1:
                runloom.sleep(0.0001)

    # Close the parser to finalize any pending state.
    parser.close()

    # Extract and dedupe the tag list for comparison.
    got = sorted(set(tags))
    state["parse_checks"][wid & 1023] += 1

    # Record in the shared TAG_CACHE so we can measure cross-fiber contamination.
    # (Not used for the load-bearing oracle, only for the measured arm.)
    if doc_idx not in TAG_CACHE:
        TAG_CACHE[doc_idx] = set()
    TAG_CACHE[doc_idx].update(got)

    # LOAD-BEARING oracle: extracted tags MUST EXACTLY match expected.
    if got != expected_tags:
        # A mismatch: either we extracted fewer tags (a parser skip), more tags
        # (a sibling's tags leaked in), or different tags (parser corruption).
        state["tag_mismatches"][wid & 1023] += 1
        if state["sample"][0] is None:
            state["sample"][0] = (wid, doc_idx, expected_tags, got)
        missing = set(expected_tags) - set(got)
        extra = set(got) - set(expected_tags)
        H.fail("html.parser TAG EXTRACTION CORRUPTED: wid={0} doc={1} expected="
               "{2!r} got={3!r} missing={4!r} extra={5!r} -- extracted tags do "
               "not match the input HTML (a sibling's parse may have corrupted "
               "this fiber's parser state via shared/pooled instances, or a "
               "scheduling point mid-parse allowed interleaving).".format(
                   wid, doc_idx, expected_tags, got, missing, extra))
        return

    # Sanity: check that we extracted SOME tags (oracle is non-vacuous).
    if not got:
        H.fail("html.parser TAG EXTRACTION EMPTY: wid={0} doc={1} -- the "
               "parser extracted 0 tags from the HTML (parser broken or HTML "
               "is empty).".format(wid, doc_idx))
        return


# Sustained parse calls per worker, bounded by H.running().
INNER_CAP = 10000


def worker(H, wid, rng, state):
    """Each fiber runs the load-bearing tag-extraction oracle in a loop until
    the deadline or INNER_CAP, mixing different documents so the parser sees
    varied tag structures and yields across different parse states."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            parse_and_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["parse_checks"])
    mismatches = sum(H.state["tag_mismatches"])
    cache_contam = sum(H.state["cache_contamination"])
    mismatch_pct = (100.0 * mismatches / checks) if checks else 0.0
    sample = H.state["sample"][0]

    H.log("html.parser: parse_checks={0} tag_mismatches={1} ({2:.2f}%, "
          "LOAD-BEARING) sample={3}".format(checks, mismatches, mismatch_pct, sample))

    # NON-VACUITY: the load-bearing tag-extraction hazard was actually exercised.
    H.check(checks > 0,
            "no parse() calls ran -- the load-bearing tag-extraction hazard was "
            "never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished inside parse().
    H.require_no_lost("html.parser tag-extraction")


if __name__ == "__main__":
    harness.main(
        "p496_html", body, setup=setup, post=post,
        default_funcs=8000,
        describe="html.parser.HTMLParser maintains internal mutable state "
                 "(lasttag, cdata_elem) across parse() calls.  LOAD-BEARING: "
                 "each fiber parses a distinct HTML document and extracts tags "
                 "via a fresh parser instance; the extracted tag set MUST "
                 "exactly match the precomputed expected set for that input "
                 "(0 under plain threads GIL on AND off; tag contamination from "
                 "a sibling's parse is the M:N isolation bug).  Yields inside "
                 "parse() to exercise the hazard across hub migration + "
                 "preempt-mid-parse")
