"""big_100 / 527 -- xml.sax push-parse event conservation under M:N.

xml.sax is a PUSH parser: xml.sax.make_parser() builds an IncrementalParser
(an ExpatParser wrapping a raw C expat reader) and you register a
ContentHandler on it.  As bytes are fed in with parser.feed(chunk), the C
reader DISPATCHES events -- startElement / characters / endElement -- by
CALLING BACK into whatever ContentHandler is bound to that reader.  The
event->handler binding is per-reader state: parser.setContentHandler(h)
stores `h` on the ExpatParser, and the C expat callbacks trampoline through
the ExpatParser to `h.startElement(...)` etc.

WHERE M:N COULD BREAK IT (the gap this program probes).  We drive the feed in
SEVERAL chunks with a runloom yield BETWEEN each feed() so a sibling fiber
reliably interleaves mid-parse.  Each fiber owns its OWN parser and its OWN
handler (single-owner), but the C expat reader keeps a mutable dispatch cursor
and a pointer back to "the current handler".  If runloom's M:N scheduling did
not keep that per-reader handler binding fiber-isolated -- e.g. if a global or
per-OS-thread "current SAX handler" pointer were shared, or if resuming this
fiber on a DIFFERENT hub picked up a sibling's reader/handler binding -- then a
sibling's startElement/characters could be delivered into THIS fiber's handler
across the yield, or this fiber's next feed() could dispatch into a sibling's
handler.  Either way the closed-world per-handler event tally would not match
the document this fiber actually fed.

WHICH ORACLE IS LOAD-BEARING, AND WHY (a closed-world conservation law):

  Each fiber builds a DETERMINISTIC XML document whose shape it knows exactly:
  one <d> root wrapping N <i> elements, NO whitespace between any tags, and each
  <i> holding a text run that EMBEDS THIS FIBER'S wid (e.g. "w{wid}i{k};").
  Because there is no inter-tag whitespace, the only characters() the reader can
  emit are the item texts, so the concatenation of every characters() chunk this
  handler receives MUST equal the exact wid-embedded string this fiber built.

  Feeding that document (split into chunks, yielding between feeds) into a
  fresh single-owner ExpatParser + single-owner CountingHandler, then close(),
  the CLOSED-WORLD law for this fiber's handler is:

    * handler.starts == N + 1                (one startElement per element)
    * handler.ends   == N + 1                (one endElement per element)
    * handler.starts == handler.ends         (balanced -- no stray/dropped event)
    * "".join(handler.chars) == expected_text (the wid-embedded string EXACTLY;
      a sibling's text leaking in, or one of ours going to a sibling's handler,
      breaks this byte-for-byte -- and the embedded wid makes a cross-fiber leak
      unmistakable rather than a lucky value match)

  Single-owner: the parser, the handler, the document string, and the expected
  values are all created inside the fiber and never shared.  On a CORRECT
  runtime this law holds for every parse (the oracle PASSES, exit 0).  A count
  mismatch, an unbalanced start/end, or a text that carries a foreign wid is a
  runloom SAX-dispatch isolation bug (a cross-fiber event leak).

  WHY NOT A SHARED HANDLER (the discipline the contract demands).  A SINGLE
  ContentHandler (or a single ExpatParser) fed by many fibers at once would mix
  events EXACTLY as it would across OS threads -- that is documented
  shared-object behavior and, worse, feeding one C expat reader from two fibers
  concurrently is C-level undefined behavior (torn reader state / SIGSEGV), not a
  runloom bug.  So the fail-fast oracle is strictly SINGLE-OWNER; there is no
  shared-parser arm.  The interleave that would expose a real binding bug comes
  from the yield BETWEEN feeds while THOUSANDS of sibling fibers are mid-parse on
  their own readers, not from sharing one reader.

ORACLES:
  * LOAD-BEARING -- SAX EVENT CONSERVATION (worker, HARD, fail-fast).  Per fiber:
    build a known wid-embedded document, feed it chunked across yields into a
    fresh single-owner parser+handler, close(), and assert the closed-world law
    above.  A SAXParseException or any exception from feed()/close() on a
    well-formed document (single-owner) is also a hard fault -- it means the C
    reader's state was corrupted by a foreign fiber -- and is surfaced by the
    pool's worker wrapper as a failure.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-feed
    (parked inside the C dispatch on a desynced reader) never returns; the
    watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually parsed documents
    (parse_count > 0), else the conservation law was never exercised.

FAIL ON: a per-handler start/end count that does not equal the fiber's known
element count, an unbalanced start/end, characters() text that does not equal
the fiber's exact wid-embedded string (a cross-fiber event leak), or an
exception parsing a well-formed single-owner document (torn reader state).

Stresses: xml.sax IncrementalParser feed()/close() dispatch, ExpatParser ->
ContentHandler callback trampolining, per-reader handler-binding isolation
across a yield + hub migration, C expat reader state under M:N.  Distinct layer
from pyexpat (raw C callbacks) and xml.etree (tree build): this probes the SAX
push-handler dispatch binding specifically.

Good TSan / controlled-M:N-replay target: the ExpatParser's `_cont_handler`
pointer and the C reader's dispatch cursor are written once per fiber but read
by the C callback on every event; under the single-owner arm only one fiber
touches a given reader, so a data-race report on that reader object -- or a
replay that dispatches an event into the wrong handler across the inter-feed
yield -- localizes a binding leak before the count/text law even closes.
"""
import xml.sax
from xml.sax.handler import ContentHandler

import harness
import runloom

# Number of <i> child elements per document.  Total elements = N_ITEMS + 1
# (the <d> root).  Big enough that the document spans several feed chunks (so
# the inter-feed yield lands mid-parse, mid-dispatch) and that a leaked or
# dropped event moves a count by a detectable amount; small enough that many
# thousands of fibers each complete many parses under the timeout.
N_ITEMS = 24

# Split each document into this many feed() chunks, yielding between each so a
# sibling fiber reliably interleaves while this fiber's reader is mid-parse.
FEED_CHUNKS = 4

# Sustained parses per worker, bounded by H.running().  The handler-binding
# hazard only manifests under SUSTAINED churn: thousands of fibers each mid-feed
# on their own reader, yielding across the dispatch boundary, so the scheduler
# interleaves a sibling's dispatch before this fiber resumes.  A single parse
# per fiber barely overlaps a sibling's and does not reproduce a binding leak.
INNER_CAP = 100000


class CountingHandler(ContentHandler):
    """Single-owner SAX ContentHandler: accumulates the closed-world event tally
    for ONE fiber's ONE document into instance attrs.  Never shared."""

    def __init__(self):
        # ContentHandler.__init__ is a no-op in the stdlib, but call it for
        # forward-compat / correctness.
        ContentHandler.__init__(self)
        self.starts = 0
        self.ends = 0
        self.chars = []

    def startElement(self, name, attrs):
        self.starts += 1

    def endElement(self, name):
        self.ends += 1

    def characters(self, content):
        # The reader may split a single text run across several characters()
        # calls (especially since we feed in chunks); we accumulate and join, so
        # the concatenation is exact regardless of how the reader chunks it.
        self.chars.append(content)


def item_text(wid, k):
    """The text run for the k-th <i> element in wid's document.  Embeds wid so a
    cross-fiber leak (a sibling's text delivered to this handler) is unmistakable
    -- the concatenation would carry a FOREIGN wid, not merely a wrong value."""
    return "w{0}i{1};".format(wid, k)


def build_document(wid):
    """Build wid's deterministic XML document and its expected character string.

    The document is <d> wrapping N_ITEMS <i> elements with NO whitespace between
    any tags, so the ONLY characters() the reader can emit are the item texts.
    Returns (doc_str, expected_text, expected_elements)."""
    parts = ["<d>"]
    expected_chars = []
    for k in range(N_ITEMS):
        t = item_text(wid, k)
        parts.append("<i>")
        parts.append(t)
        parts.append("</i>")
        expected_chars.append(t)
    parts.append("</d>")
    doc = "".join(parts)
    return doc, "".join(expected_chars), N_ITEMS + 1


def chunk_document(doc, nchunks):
    """Split `doc` into (up to) nchunks contiguous byte slices for feed()."""
    n = len(doc)
    if nchunks <= 1 or n <= nchunks:
        return [doc]
    step = (n + nchunks - 1) // nchunks
    return [doc[i:i + step] for i in range(0, n, step)]


def parse_check(H, wid, state):
    """Single-owner SAX event-conservation check.

    Build wid's known document, feed it chunked across yields into a FRESH
    single-owner parser + handler, close(), and assert the closed-world law.
    A cross-fiber event leak (foreign start/end into this handler, or this
    fiber's text into a sibling's handler) breaks the counts or the exact
    wid-embedded text."""
    doc, expected_text, expected_elements = build_document(wid)

    parser = xml.sax.make_parser()
    handler = CountingHandler()
    parser.setContentHandler(handler)

    chunks = chunk_document(doc, FEED_CHUNKS)
    for ci, chunk in enumerate(chunks):
        parser.feed(chunk)
        # YIELD across the dispatch boundary: while this reader sits between
        # feeds, thousands of sibling fibers dispatch events on their own
        # readers.  If the handler binding is not fiber-isolated, a sibling's
        # event could land in `handler` or ours in a sibling's before we resume.
        runloom.yield_now()
        if ci & 1:
            runloom.sleep(0.0002)
    parser.close()

    # ---- closed-world conservation law for THIS fiber's handler -------------
    # Balanced + exact element count: one start and one end per element.
    if handler.starts != expected_elements:
        H.fail("SAX event leak: handler.starts={0} but this fiber fed a "
               "document with {1} elements (wid {2}) -- a foreign startElement "
               "was dispatched into this single-owner handler, or one of ours "
               "was lost across the inter-feed yield".format(
                   handler.starts, expected_elements, wid))
        return
    if handler.ends != expected_elements:
        H.fail("SAX event leak: handler.ends={0} but this fiber fed a document "
               "with {1} elements (wid {2}) -- a foreign endElement was "
               "dispatched into this single-owner handler, or one of ours was "
               "lost across the inter-feed yield".format(
                   handler.ends, expected_elements, wid))
        return
    if handler.starts != handler.ends:
        H.fail("SAX event imbalance: starts={0} != ends={1} (wid {2}) -- the "
               "reader dispatched an unbalanced start/end sequence into this "
               "single-owner handler under M:N".format(
                   handler.starts, handler.ends, wid))
        return

    # Exact text: the concatenation of every characters() chunk MUST equal the
    # wid-embedded string this fiber built.  A foreign wid in here is a
    # cross-fiber event leak; a missing/extra fragment is a dropped/doubled
    # dispatch.
    got_text = "".join(handler.chars)
    if got_text != expected_text:
        H.fail("SAX character leak: handler received text {0!r} but this fiber "
               "(wid {1}) fed text {2!r} -- a sibling's characters() was "
               "dispatched into this single-owner handler (foreign wid), or a "
               "text fragment was dropped/doubled across the inter-feed "
               "yield".format(got_text, wid, expected_text))
        return

    state["parses"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """Each fiber repeatedly parses its OWN deterministic wid-embedded document
    through a FRESH single-owner parser+handler, yielding between feed chunks so
    siblings interleave mid-dispatch.  The load-bearing oracle is fail-fast."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            parse_check(H, wid, state)          # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        # Sharded non-vacuity tally (ONLY for non-vacuity, per contract rule 1):
        # the load-bearing oracle is per-fiber single-owner, not a cross-fiber
        # exact-sum law, so a wid&1023 shard is the right structure here.
        "parses": [0] * 1024,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    parses = sum(H.state["parses"])
    H.log("SAX single-owner event-conservation parses: {0} (each start==end=="
          "element-count + exact wid-embedded characters() checked fail-fast); "
          "ops={1}".format(parses, H.total_ops()))

    # NON-VACUITY: the load-bearing single-owner parse hazard was exercised.
    H.check(parses > 0,
            "no SAX parses completed -- the single-owner push-parse event "
            "conservation law was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-feed (stranded inside the
    # C dispatch on a desynced reader).
    H.require_no_lost("sax handler event conservation")


if __name__ == "__main__":
    harness.main(
        "p527_sax_handler_event_conservation", body, setup=setup, post=post,
        default_funcs=6000,
        describe="xml.sax push-parses bytes through an ExpatParser (C expat "
                 "reader) that DISPATCHES startElement/characters/endElement "
                 "into the bound ContentHandler.  Under M:N, if that per-reader "
                 "handler binding is not fiber-isolated, a sibling's event "
                 "could be dispatched into this fiber's handler across the "
                 "inter-feed yield.  LOAD-BEARING: each fiber feeds its OWN "
                 "deterministic wid-embedded document (chunked, yielding "
                 "between feeds) into a FRESH single-owner parser+handler; the "
                 "closed-world law is start==end==element-count and the "
                 "concatenated characters() equals the exact wid-embedded "
                 "string -- a foreign wid, a count mismatch, or a parse "
                 "exception on a well-formed single-owner document is a "
                 "runloom SAX-dispatch isolation bug")
