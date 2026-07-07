"""big_100 / 525 -- pyexpat StartElement/EndElement/CharData count conservation
under M:N, with the raw C expat parser struct held ACROSS a yield mid-parse.

pyexpat.ParserCreate() wraps a raw C `XML_Parser` struct.  Feeding it a document
in SLICES -- Parse(chunk, False), Parse(chunk, False), ..., Parse(b'', True) --
keeps that C struct plus its bound handler pointers (StartElementHandler,
EndElementHandler, CharacterDataHandler) LIVE across every gap between chunks.
In this program each gap is a runloom yield: the fiber parks with a half-consumed
XML document sitting inside its parser, and a sibling fiber on the same (or a
different) hub runs -- itself parked mid-parse inside ITS OWN parser.

WHERE M:N COULD BREAK IT (the gap this program probes).  Each parser's handlers
close over that parser's OWN single-owner counters (1-element lists: starts,
ends, and a char accumulator).  Handler dispatch is: expat's C code, on hitting a
start/end tag or a run of character data, calls back into the Python callable
stored on THIS parser.  If a broken M:N runtime were to fire a handler bound to
the WRONG fiber's parser after a hub migration -- e.g. the handler-dispatch
trampoline captured a stale per-thread parser pointer, or the fiber resumed on a
hub whose tstate still referenced a sibling's parser -- then this fiber's start
count, end count, or char accumulator would be corrupted by a sibling's document.
Because each fiber embeds its OWN wid into the character data it feeds, a cross-
fiber handler firing is DIRECTLY OBSERVABLE: the accumulator would contain another
fiber's wid-tagged text, or the element counts would drift.

WHICH ORACLE IS LOAD-BEARING, AND WHY (a closed-world counting law):

  Each fiber creates its OWN pyexpat parser (single owner, never shared), and
  feeds it a KNOWN document whose element count and full character-data payload
  are computed up front:

     <root><item>W{wid}I0;</item><item>W{wid}I1;</item>...</root>

  The document has exactly ELEMENT_COUNT = 1 + NITEMS elements and a known
  concatenated character payload EXPECTED_TEXT that embeds wid in every chunk.
  The parser is driven in small BYTE SLICES with a runloom yield between each
  slice, then closed with Parse(b'', True).  After the final Parse:

    * starts[0] == ELEMENT_COUNT      (every StartElement callback fired, once)
    * ends[0]   == ELEMENT_COUNT      (every EndElement callback fired, once)
    * starts[0] == ends[0]            (well-formed nesting conservation)
    * chars accumulator == EXPECTED_TEXT   (every CharacterData callback fired on
                                            THIS parser, delivering THIS fiber's
                                            wid-tagged text, none dropped/doubled,
                                            none leaked in from a sibling)

  Single-owner: the parser and its three counters are fiber-local, created inside
  the worker and never shared.  On a CORRECT runtime this closed-world law holds
  with probability 1 (verified: a single-threaded run of the same doc yields
  starts==ends==ELEMENT_COUNT and the exact EXPECTED_TEXT).  A FAIL therefore
  means the C parser struct's handler dispatch fired against the wrong fiber's
  counters across a yield -- a real runloom M:N bug (cross-fiber leak of single-
  owner C-object state, dropped/doubled callback, or torn accumulator).

ORACLES:
  * LOAD-BEARING -- COUNT + TEXT CONSERVATION (worker, HARD, fail-fast).  The
    per-parser closed-world law above.  Any deviation -> H.fail, return.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (parse_ok > 0),
    tallied sharded by wid&1023 (this is a NON-conservation tally, so sharding is
    fine -- it only proves work happened, it is not the counting law).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-Parse
    inside the C expat callback (parked with a half-consumed document) never
    returns; the watchdog + require_no_lost catch it.

Note on why the counts are single-owner and NOT sharded: starts[0]/ends[0]/the
char accumulator are the per-parser 1-element lists owned by exactly ONE fiber
(the one that created that parser).  There is exactly one writer per counter, so
these are race-free by construction -- the conservation law is per-parser, not a
global sum, so no [0]*H.funcs slot table is needed for the LAW itself; the only
cross-fiber tally (parse_ok) is a non-vacuity counter and may be sharded.

Stresses: pyexpat XML_Parser C struct held live across a cooperative yield mid-
document; StartElement/EndElement/CharacterData handler dispatch from C back into
per-parser single-owner Python callables under hub migration; sliced Parse() with
yields between chunks so a sibling reliably interleaves while this parser is
parked half-consumed; closed-world element-count + char-payload conservation.

Good TSan / controlled-M:N-replay target: the parser's handler-pointer dispatch
and the Python callback's list mutation happen while a sibling fiber is suspended
mid-parse in ITS parser; a data race on a parser's handler slot, or a replay that
delivers a callback against a sibling's counters, localizes the leak before the
count/text law even closes.
"""
import pyexpat

import harness
import runloom

# Items per document.  ELEMENT_COUNT = 1 (root) + NITEMS (each <item>).  Sized so
# the document spans many small byte-slices (many yields per parse) while staying
# cheap enough that thousands of fibers each run many parses under the timeout.
NITEMS = 6
ELEMENT_COUNT = 1 + NITEMS

# Byte-slice size for feeding Parse(chunk, False).  Small so the C parser struct
# is repeatedly parked mid-document with a runloom yield between every slice --
# maximizing the window in which a sibling runs while this parser is half-consumed.
SLICE = 5

# Sustained parses per worker, bounded by H.running().  A single parse per fiber
# barely overlaps a sibling's; the cross-fiber handler-dispatch hazard only
# manifests under SUSTAINED churn (many parsers parked mid-document at once).
INNER_CAP = 100000


def build_doc(wid):
    """Build this fiber's KNOWN document plus its element count and full character
    payload.  Every item's text embeds `wid`, so a cross-fiber handler firing on
    this parser would deliver another fiber's wid-tagged text into the accumulator
    (directly observable).  Uses only XML-safe characters (letters, digits, ';').

    Returns (doc_bytes, element_count, expected_text)."""
    parts = ["<root>"]
    expected = []
    for i in range(NITEMS):
        txt = "W{0}I{1};".format(wid, i)     # wid-tagged, XML-safe payload
        expected.append(txt)
        parts.append("<item>")
        parts.append(txt)
        parts.append("</item>")
    parts.append("</root>")
    doc = "".join(parts).encode("ascii")
    return doc, ELEMENT_COUNT, "".join(expected)


def parse_once(H, wid, state):
    """Single-owner closed-world parse.  Create a fresh parser owned only by this
    fiber, drive it in byte-slices with a yield between each (parser struct parked
    mid-document), then assert the count + text conservation law.  A deviation is a
    cross-fiber handler-dispatch leak in the runtime."""
    doc, element_count, expected_text = build_doc(wid)

    # Per-parser single-owner counters: exactly one writer (this fiber).
    starts = [0]
    ends = [0]
    chunks = []                              # char-data pieces, order-preserving

    parser = pyexpat.ParserCreate()
    parser.StartElementHandler = lambda name, attrs: starts.__setitem__(0, starts[0] + 1)
    parser.EndElementHandler = lambda name: ends.__setitem__(0, ends[0] + 1)
    parser.CharacterDataHandler = lambda data: chunks.append(data)

    # Feed the document in small slices, parking the C parser struct across a
    # runloom yield between each slice so a sibling reliably interleaves while this
    # parser sits half-consumed.
    pos = 0
    n = len(doc)
    while pos < n:
        end = pos + SLICE
        parser.Parse(doc[pos:end], False)
        pos = end
        runloom.yield_now()                  # parser parked mid-document here
    parser.Parse(b"", True)                   # finalize

    # ---- closed-world count + text conservation law -----------------------------
    if starts[0] != element_count:
        H.fail("pyexpat START count wrong: parser saw {0} StartElement callbacks, "
               "expected {1} for a {2}-element doc (wid {3}) -- a start-tag "
               "callback was dropped/doubled or fired against another fiber's "
               "parser across a yield".format(
                   starts[0], element_count, element_count, wid))
        return
    if ends[0] != element_count:
        H.fail("pyexpat END count wrong: parser saw {0} EndElement callbacks, "
               "expected {1} for a {2}-element doc (wid {3}) -- an end-tag "
               "callback was dropped/doubled or cross-fiber-dispatched across a "
               "yield".format(ends[0], element_count, element_count, wid))
        return
    if starts[0] != ends[0]:
        H.fail("pyexpat START/END nesting conservation broken: {0} starts != {1} "
               "ends (wid {2}) -- the C parser's handler dispatch is desynced "
               "across a yield".format(starts[0], ends[0], wid))
        return

    got_text = "".join(chunks)
    if got_text != expected_text:
        # Either a CharacterData callback was dropped/doubled on this parser, or a
        # sibling fiber's wid-tagged text leaked in via a cross-fiber handler fire.
        H.fail("pyexpat CHAR-DATA conservation broken (wid {0}): accumulator is "
               "{1!r} but this fiber's doc payload is {2!r} -- a character-data "
               "callback was dropped/doubled, or a sibling's parser handler fired "
               "against this fiber's accumulator across a yield".format(
                   wid, got_text, expected_text))
        return

    state["parse_ok"][wid & 1023] += 1        # NON-VACUITY tally (sharded OK)


def worker(H, wid, rng, state):
    """Sustained single-owner parses.  Each parse builds a fresh fiber-local parser
    and drives it sliced-with-yields so the parser struct is repeatedly parked
    mid-document while siblings run their own parked parsers."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            parse_once(H, wid, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "parse_ok": [0] * 1024,               # sharded NON-VACUITY tally
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    ok = sum(H.state["parse_ok"])
    H.log("pyexpat single-owner conservation parses: {0} (each verified "
          "starts==ends=={1}==element-count and the exact wid-tagged char "
          "payload, fail-fast); ops={2}".format(ok, ELEMENT_COUNT, H.total_ops()))

    # NON-VACUITY: the load-bearing closed-world parse actually ran.
    H.check(ok > 0,
            "no pyexpat conservation parses completed -- the parser-parked-mid-"
            "document hazard was never exercised (the oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside a C expat
    # callback with a half-consumed document).
    H.require_no_lost("pyexpat count/text conservation")


if __name__ == "__main__":
    harness.main(
        "p525_pyexpat_start_end_count_conservation", body, setup=setup, post=post,
        default_funcs=6000,
        describe="each fiber owns a pyexpat.ParserCreate() and feeds a KNOWN "
                 "document in byte-slices with a runloom yield between slices, so "
                 "the raw C XML_Parser struct + its StartElement/EndElement/"
                 "CharacterData handler pointers stay live while the fiber is "
                 "parked mid-document.  LOAD-BEARING closed-world law: after the "
                 "final Parse(b'',True), StartElement count == EndElement count == "
                 "1+NITEMS element count, and the char-data accumulator equals the "
                 "fiber's exact wid-tagged payload.  A dropped/doubled callback, a "
                 "desynced start/end count, or a sibling's wid-tagged text leaking "
                 "into this parser's accumulator across a yield is the runloom "
                 "cross-fiber C-handler-dispatch bug")
