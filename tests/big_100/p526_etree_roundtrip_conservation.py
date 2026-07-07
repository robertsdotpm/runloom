"""big_100 / 526 -- xml.etree.ElementTree chunked-feed round-trip conservation under M:N.

xml.etree.ElementTree.fromstring() (and the lower-level ET.XMLParser) drives a C
`expat` XMLParser into a TreeBuilder: as expat emits start/end/data events it
pushes and pops elements on the TreeBuilder's INTERNAL ELEMENT STACK, splicing each
finished child onto its parent.  The interesting M:N hazard is the INCREMENTAL feed
path: `parser.feed(chunk)` processes a partial document and LEAVES the TreeBuilder
mid-way through its element stack (parent open, waiting for the next chunk's child
events).  If you feed a document in 2-3 slices with a yield BETWEEN feed() calls, a
sibling fiber runs on the same hub in the gap.  Were any of expat's per-parse state,
or the TreeBuilder's element-stack cursor, NOT isolated per fiber (e.g. a shared
scratch buffer, a thread-ID-keyed parser handle, or a contextvar-bound builder that
leaked across the runloom hub switch), a sibling's start/end events could be spliced
onto THIS fiber's half-built tree -- a cross-tree node graft.

WHERE M:N COULD BREAK IT (the gap this program probes).  Each fiber owns its OWN
ET.XMLParser (hence its own expat parser handle and its own TreeBuilder + element
stack) -- a single-owner object, never shared.  It feeds a document it built itself,
one whose EVERY element carries this fiber's `wid` as an attribute, in equal chunks
with runloom.yield_now() between each feed() so a sibling reliably interleaves at the
exact moment the TreeBuilder's element stack is non-empty (parent open, mid-parse).
Under a CORRECT runtime the parser is fully fiber-local: expat state and the element
stack belong to this fiber alone, the yield merely parks it, and the finished tree is
EXACTLY the document fed in -- same tags, attribs, texts, in the same document order,
and every `wid` marker is this fiber's own.  A cross-fiber splice would surface as a
foreign `wid` attribute, a wrong element count, or a structural mismatch.

WHICH ORACLE IS LOAD-BEARING, AND WHY.  This is a CONSERVATION program: the parse is
a closed-world transform whose output must be an exact, structure-preserving copy of
the known input.  Two conservation laws, both fail-fast, on a SINGLE-OWNER parser:

  * STRUCTURAL CONSERVATION (chunked feed).  Build a known tree T of E elements, each
    element tagged with this fiber's wid.  Serialize T to bytes, feed those bytes to
    a FRESH per-fiber ET.XMLParser in 2-3 chunks with a yield between chunks, close()
    to get the parsed tree P.  Then, in DOCUMENT ORDER (root.iter()):
      - len(flatten(P)) == E                        (element count conserved: no node
                                                       dropped, doubled, or spliced in)
      - flatten(P) == flatten(T) elementwise, where flatten(el) = (tag, sorted(attrib
        .items()), text)                            (tag/attrib/text preserved exactly)
      - every element's `wid` attribute == this fiber's wid  (NO cross-fiber node
                                                       graft from a sibling's parser)
    A ParseError on a well-formed document the fiber built and fed to its OWN parser
    is itself a hard fault (corruption of the single-owner parse buffer).

  * ROUND-TRIP CONSERVATION.  Re-serialize P with ET.tostring() and re-parse with
    ET.fromstring(); the twice-parsed tree R must flatten() identically to P (a
    tostring->fromstring round-trip is an identity on structure), and E is conserved
    again.  This exercises the ONE-SHOT fromstring() path (which also drives expat +
    TreeBuilder, but with no yield-interrupted feed) as an independent confirmation.

  The per-iteration element count E feeds a CONSERVATION counter: elems[wid] (one
  slot per worker, single writer -- race-free, allocated in setup() where H.funcs is
  known).  post() sums it for the global element-conservation tally and NON-VACUITY.

  These oracles are single-owner (each fiber's parser, TreeBuilder, and both trees
  are fiber-local, never shared), so on a correct runtime they PASS (program exits 0).
  A shared parser or a shared tree would race exactly like shared-across-threads --
  documented Python behavior, NOT a runloom bug -- so nothing here is shared.

ORACLES:
  * LOAD-BEARING -- STRUCTURAL + ROUND-TRIP CONSERVATION (worker, HARD, fail-fast).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-feed() (parked
    with the TreeBuilder's element stack non-empty and never resumed) never returns;
    the watchdog + require_no_lost catch the lost-wakeup.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (elements parsed > 0).

FAIL ON: a foreign wid attribute in a fiber's parsed tree, a mismatched element count,
a structural (tag/attrib/text/order) divergence between fed and parsed trees, a
round-trip that changes structure, or a ParseError on self-fed well-formed XML -- any
of which is a cross-fiber TreeBuilder splice or torn expat parse under M:N.

Stresses: ET.XMLParser incremental feed() across a hub-yield, TreeBuilder element-
stack isolation per fiber, expat C-parser state isolation, ET.tostring/fromstring
round-trip identity, structural conservation under sustained concurrent parsing.

Good TSan / controlled-M:N-replay target: the TreeBuilder element stack and expat's
parse buffers are mutated on every feed(); under the single-owner arm they are touched
by one fiber only, so a data-race report on a TreeBuilder/parser buffer -- or a replay
that splices a sibling's start-tag mid-feed -- is the cleanest signal before the
structural flatten() comparison even fires.
"""
import xml.etree.ElementTree as ET

import harness
import runloom


# Sustained parses per worker, bounded by H.running().  The mid-feed splice hazard
# only manifests under SUSTAINED churn -- many fibers simultaneously feeding chunked
# documents while PARKED across the inter-chunk yield, so the scheduler reliably
# interleaves a sibling's feed() before this fiber resumes.  A single parse per fiber
# barely overlaps a sibling's mid-feed window and does NOT reproduce.
INNER_CAP = 100000


def build_doc(wid, seq, rng):
    """Build a KNOWN tree whose every element carries this fiber's wid as an
    attribute.  Returns (root, element_count).  The structure is deterministic given
    (wid, seq, rng): a `doc` root with a random number of `item` children, some of
    which carry a nested `note`, every text a wid+seq marker.  Element count is exact
    and used by the conservation oracle."""
    wids = str(wid)
    marker = "W{0}S{1}".format(wid, seq)
    root = ET.Element("doc", {"wid": wids, "seq": str(seq), "marker": marker})
    root.text = marker + "-root"
    count = 1
    nitems = rng.randint(3, 12)
    for k in range(nitems):
        item = ET.SubElement(root, "item", {"i": str(k), "wid": wids, "m": marker})
        item.text = "{0}-item-{1}".format(marker, k)
        count += 1
        if k % 3 == 0:
            note = ET.SubElement(item, "note", {"wid": wids, "k": str(k)})
            note.text = "{0}-note-{1}".format(marker, k)
            count += 1
    return root, count


def flatten(root):
    """Document-order structural fingerprint: a list of (tag, sorted-attrib, text)
    tuples, one per element (root.iter() yields in document order).  Two trees are
    structurally identical iff their flatten() lists are equal; the list LENGTH is the
    element count (conserved by the parse)."""
    out = []
    for el in root.iter():
        out.append((el.tag, tuple(sorted(el.attrib.items())), el.text))
    return out


def chunked_parse(data, nchunks):
    """Feed `data` (bytes) into a FRESH per-fiber ET.XMLParser in `nchunks` roughly-
    equal slices, yielding BETWEEN feed() calls so a sibling interleaves while this
    fiber's TreeBuilder element stack is mid-parse.  Returns the closed root.  The
    parser (expat handle + TreeBuilder) is created here and never escapes this call --
    strictly single-owner."""
    parser = ET.XMLParser()
    n = len(data)
    step = max(1, n // nchunks)
    pos = 0
    while pos < n:
        parser.feed(data[pos:pos + step])
        pos += step
        runloom.yield_now()               # sibling runs while our element stack is open
    return parser.close()


def parse_check(H, wid, seq, rng, state):
    """One single-owner structural + round-trip conservation check.  Fail-fast."""
    expected_root, ecount = build_doc(wid, seq, rng)
    expected_flat = flatten(expected_root)
    data = ET.tostring(expected_root)          # bytes; the exact document to conserve
    wids = str(wid)
    nchunks = 2 + (seq & 1)                     # 2 or 3 chunks

    # ---- STRUCTURAL CONSERVATION: chunked feed across yields --------------------
    try:
        parsed_root = chunked_parse(data, nchunks)
    except ET.ParseError as exc:
        H.fail("chunked feed of self-built well-formed XML raised ParseError "
               "(wid {0} seq {1}): {2!r} -- the single-owner expat/TreeBuilder parse "
               "buffer was corrupted, a sibling's feed() spliced into this fiber's "
               "mid-parse element stack".format(wid, seq, exc))
        return

    parsed_flat = flatten(parsed_root)

    # element count conserved (no node dropped, doubled, or grafted in)
    if len(parsed_flat) != ecount:
        H.fail("element count NOT conserved: fed a {0}-element doc but parsed {1} "
               "elements (wid {2} seq {3}) -- a node was dropped, doubled, or a "
               "sibling's node was spliced into this fiber's TreeBuilder stack "
               "mid-feed".format(ecount, len(parsed_flat), wid, seq))
        return

    # exact structural identity in document order (tag / sorted-attrib / text)
    if parsed_flat != expected_flat:
        # locate the first divergent element for a precise message
        for i, (pe, ee) in enumerate(zip(parsed_flat, expected_flat)):
            if pe != ee:
                H.fail("structural conservation broken at doc-order index {0} "
                       "(wid {1} seq {2}): parsed {3!r} != fed {4!r} -- a cross-fiber "
                       "TreeBuilder splice or torn expat parse under M:N".format(
                           i, wid, seq, pe, ee))
                return
        H.fail("structural conservation broken (wid {0} seq {1}): parsed flatten "
               "differs from fed flatten".format(wid, seq))
        return

    # NO cross-fiber node graft: every element's wid attribute is THIS fiber's wid.
    for el in parsed_root.iter():
        got = el.attrib.get("wid")
        if got != wids:
            H.fail("cross-fiber node graft: parsed element <{0}> carries wid={1!r}, "
                   "expected this fiber's wid={2!r} (seq {3}) -- a sibling fiber's "
                   "element was spliced onto this fiber's tree during the "
                   "yield-interrupted chunked feed".format(el.tag, got, wids, seq))
            return

    # ---- ROUND-TRIP CONSERVATION: tostring -> fromstring identity --------------
    try:
        redata = ET.tostring(parsed_root)
        reparsed_root = ET.fromstring(redata)     # one-shot fromstring path
    except ET.ParseError as exc:
        H.fail("round-trip fromstring of self-serialized tree raised ParseError "
               "(wid {0} seq {1}): {2!r} -- ET.tostring produced malformed output "
               "or the one-shot parse was torn under M:N".format(wid, seq, exc))
        return

    reparsed_flat = flatten(reparsed_root)
    if len(reparsed_flat) != ecount:
        H.fail("round-trip element count NOT conserved: {0}-element tree round-"
               "tripped to {1} elements (wid {2} seq {3})".format(
                   ecount, len(reparsed_flat), wid, seq))
        return
    if reparsed_flat != parsed_flat:
        for i, (re, pe) in enumerate(zip(reparsed_flat, parsed_flat)):
            if re != pe:
                H.fail("round-trip conservation broken at doc-order index {0} "
                       "(wid {1} seq {2}): reparsed {3!r} != parsed {4!r} -- "
                       "tostring/fromstring is not structure-preserving here, a "
                       "torn serialize/parse under M:N".format(i, wid, seq, re, pe))
                return
        H.fail("round-trip conservation broken (wid {0} seq {1})".format(wid, seq))
        return

    # CONSERVATION counter: elements parsed this iteration, single-writer-per-slot.
    state["elems"][wid] += ecount


def worker(H, wid, rng, state):
    """Each fiber repeatedly builds a wid-marked doc, feeds it chunked (with a yield
    between chunks so a sibling interleaves while the TreeBuilder stack is open), and
    verifies the parsed + round-tripped trees are exact structure-preserving copies.
    Single-owner throughout: the parser and both trees are fiber-local."""
    seq = 0
    for _ in H.round_range():
        if not H.running():
            break
        inner = 0
        while H.running() and inner < INNER_CAP:
            parse_check(H, wid, seq, rng, state)
            if H.failed:
                return
            H.op(wid)
            seq += 1
            inner += 1
        H.task_done(wid)


def setup(H):
    # elems is the CONSERVATION counter: ONE slot per worker (wid-indexed, single
    # writer -- race-free GIL-off), allocated here where H.funcs is known.
    H.state = {
        "elems": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    total_elems = sum(H.state["elems"])
    H.log("xml.etree chunked-feed round-trip conservation: {0} elements parsed and "
          "structurally conserved across single-owner chunked feeds (every per-"
          "iteration structural + round-trip check passed fail-fast); ops={1}".format(
              total_elems, H.total_ops()))

    # NON-VACUITY: the load-bearing single-owner parse arm actually ran.
    H.check(total_elems > 0,
            "no elements were parsed -- the chunked-feed structural-conservation "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded mid-feed() with the
    # TreeBuilder element stack open and never resumed).
    H.require_no_lost("etree round-trip conservation")


if __name__ == "__main__":
    harness.main(
        "p526_etree_roundtrip_conservation", body, setup=setup, post=post,
        default_funcs=6000,
        describe="xml.etree.ElementTree.fromstring / ET.XMLParser drive a C expat "
                 "parser into a TreeBuilder whose element stack is left mid-parse "
                 "between incremental feed() calls.  LOAD-BEARING (single-owner, "
                 "CONSERVATION): each fiber feeds a wid-marked document it built "
                 "itself into its OWN parser in 2-3 chunks with a yield between "
                 "chunks (so a sibling interleaves while the element stack is open), "
                 "then asserts the parsed tree is an EXACT structure-preserving copy "
                 "(element count conserved, tag/attrib/text identical in document "
                 "order, every wid marker its own) and that a tostring->fromstring "
                 "round-trip conserves it again.  A foreign wid attribute, a wrong "
                 "element count, a structural divergence, or a ParseError on self-fed "
                 "XML is a cross-fiber TreeBuilder splice under M:N")
