"""big_100 / 528 -- xml.dom.minidom Node identity + toxml round-trip under M:N.

xml.dom.minidom.parseString() drives the C expat parser to build a PURE-PYTHON
Node tree (Document -> Element -> Text ...).  Expat is a stateful streaming
parser: as it fires StartElement / CharacterData / EndElement / entity-expansion
callbacks, minidom's ExpatBuilder mutates a builder cursor and appends freshly
allocated Element/Text nodes.  The entity-expansion machinery in particular
carries scratch state (the current character-data buffer, the entity stack) that
lives on the parser object across callbacks.

WHERE M:N COULD BREAK IT (the gap this program probes).  Each fiber parses its
OWN document string (single-owner Document, never shared) and then HOLDS one
Element node live across a park (runloom.yield_now / sleep).  While this fiber is
parked, a SIBLING fiber on the same hub is inside its own parseString(), driving
ITS expat instance through the very same StartElement/CharacterData/entity
callbacks.  If any expat/entity scratch state, or minidom's builder cursor, were
process-global instead of per-parser -- or if a runloom hub migration reused a C
buffer across the two live parses -- the parked fiber's held Element could come
back with a MUTATED .firstChild.data (its text overwritten by the sibling's
character-data buffer), a changed attribute, or a different identity.  A correct
runtime keeps every parse's node tree wholly private to its owning fiber.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  minidom's documented contract is that a parsed Document is an ordinary Python
  object graph owned by its caller; parseString() allocates a fresh ExpatBuilder
  and expat parser per call (expat parser instances are independent).  A held
  Element's id(), its .firstChild.data, and its getAttribute() are therefore
  stable for as long as the owner holds the tree and does not mutate it -- a
  yield in between changes nothing.  We verified with a plain-threads control (8
  OS threads, GIL on AND off, each parsing its own wid-marked doc, parking on a
  threading.Barrier mid-hold, then re-reading) that 100% of held nodes are byte-
  identical before and after the park and that toxml()->re-parse reproduces the
  wid-marked text and attribute exactly -- 0 cross-thread bleed.  Under a CORRECT
  runloom it must also hold, so this single-owner oracle PASSES on a correct
  runtime (program exits 0 when there is no bug).

ORACLES:
  * LOAD-BEARING -- NODE IDENTITY STABILITY (worker, HARD, fail-fast).  Each fiber
    parses its OWN unique document (marker element with a wid-unique attribute and
    wid-unique text containing entity references &amp; &lt; &gt; so expat's entity-
    expansion path is exercised).  It grabs the marker Element, RECORDS id(marker),
    id(marker.firstChild), marker.getAttribute("tag") and marker.firstChild.data.
    It YIELDS (parks so a sibling's parse interleaves), then re-reads the SAME
    references and asserts:
      - id(marker) and id(marker.firstChild) unchanged across the park (the held
        node was not replaced / a foreign parse did not swap the object),
      - getAttribute("tag") unchanged and equal to the wid-unique expected value,
      - firstChild.data unchanged and equal to the entity-decoded expected text.
    Single-owner: the Document is created inside the fiber, stored in a local,
    unlink()ed in finally, never shared.  A failure is a runloom parse-isolation
    desync -- a real torn/leaked-node bug.

  * LOAD-BEARING -- toxml ROUND-TRIP CONSERVATION (worker, HARD, fail-fast).  After
    the identity check the fiber serializes its OWN Document with toxml(), re-parses
    the serialized bytes into a SECOND private Document, and asserts the re-parsed
    marker's attribute and text equal the wid-unique originals EXACTLY.  This is the
    conservation arm: the wid-marked unit of information survives a full
    build->serialize->rebuild cycle bit-for-bit even while siblings churn expat on
    the same hub.  A dropped/rewritten character or attribute is a hard fault.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside expat
    (parked mid-callback on a desynced parser) never returns; watchdog +
    require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arms actually ran (checks > 0).

FAIL ON: a held Element's identity/attribute/text changing across a yield, or a
toxml()->re-parse round-trip that does not reproduce the wid-unique attribute and
text.  There is NO shared-mutable arm here -- every Document is single-owner, so a
failure cannot be documented shared-object semantics; it can only be a real
runtime bug (torn node, cross-fiber expat-buffer bleed, lost wakeup, SIGSEGV).

Stresses: xml.dom.minidom.parseString / ExpatBuilder node construction, expat
StartElement / CharacterData / entity-expansion callbacks under concurrent live
parses on the same hub, Element/Text node identity across a park, getAttribute()
and firstChild.data stability, toxml() serialization racing a sibling's parse,
Document.unlink() cycle-breaking cleanup under M:N churn.

Good TSan / controlled-M:N-replay target: the expat parser's character-data
buffer and minidom's builder cursor are mutated on every callback; a data-race
report on a C buffer shared between two concurrently-live parses, or a
deterministic replay that reads a held node's .data mid-callback of a sibling's
parse, localizes the bleed before the identity/round-trip oracle even fires.
"""
import xml.dom.minidom as minidom

import harness
import runloom

# Number of sibling <item> elements around the marker so expat builds a real,
# non-trivial tree (several StartElement/CharacterData/EndElement cycles) rather
# than a one-node document -- more callbacks = a wider window for any shared
# builder/entity state to bleed while this fiber is parked mid-hold.
SIBLINGS = 6

# Sustained checks per worker, bounded by H.running().  The bleed hazard only
# manifests under SUSTAINED churn -- many fibers simultaneously parsing while
# sleep-PARKED across their yield, so the scheduler reliably interleaves a
# sibling's parse before this fiber resumes.  A single parse per fiber barely
# overlaps a sibling's and does NOT reproduce.
INNER_CAP = 100000


def build_doc_string(wid, idx):
    """Build a UNIQUE per-(wid, idx) XML document string.

    The marker element carries:
      - a wid-unique attribute  tag="tag-W{wid}-I{idx}-K"
      - wid-unique text with ENTITY REFERENCES (&amp; &lt; &gt;) so expat's
        entity-expansion callback path is exercised; the decoded text is unique.

    Returns (doc_string, expected_attr, expected_text)."""
    expected_attr = "tag-W{0}-I{1}-K".format(wid, idx)
    # Raw (escaped) marker text and its expat-DECODED form.  Keep them distinct
    # per wid/idx so a leaked sibling buffer would produce a visible mismatch.
    raw_text = "unit-{0}-{1}&amp;x&lt;{0}&gt;end".format(wid, idx)
    expected_text = "unit-{0}-{1}&x<{0}>end".format(wid, idx)

    parts = ['<root wid="{0}" idx="{1}">'.format(wid, idx)]
    # Some leading siblings to give expat a real callback stream before the marker.
    for s in range(SIBLINGS // 2):
        parts.append('<item n="{0}">pre-{1}-{0}</item>'.format(s, wid))
    parts.append('<marker tag="{0}">{1}</marker>'.format(expected_attr, raw_text))
    # Trailing siblings after the held node.
    for s in range(SIBLINGS - SIBLINGS // 2):
        parts.append('<item n="{0}">post-{1}-{0}</item>'.format(s, wid))
    parts.append('</root>')
    return "".join(parts), expected_attr, expected_text


def get_marker(doc):
    """Return the single <marker> Element, or None if the tree is malformed."""
    markers = doc.getElementsByTagName("marker")
    if len(markers) != 1:
        return None
    return markers[0]


def identity_and_roundtrip_check(H, wid, idx, state):
    """One LOAD-BEARING iteration: parse a private doc, hold the marker node across
    a park and assert identity/attr/text stability, then toxml()->re-parse and
    assert the wid-unique attr/text are conserved exactly.  Single-owner: both
    Documents are local and unlink()ed in finally."""
    doc_str, expected_attr, expected_text = build_doc_string(wid, idx)
    doc = None
    doc2 = None
    try:
        doc = minidom.parseString(doc_str)
        marker = get_marker(doc)
        if marker is None:
            H.fail("parseString produced a tree with != 1 <marker> element "
                   "(wid {0}, idx {1}) -- expat/minidom built a malformed tree "
                   "under concurrent parses".format(wid, idx))
            return
        if marker.firstChild is None:
            H.fail("marker element has NO firstChild text node (wid {0}, idx {1}) "
                   "-- the character-data callback dropped this fiber's text under "
                   "concurrent parses".format(wid, idx))
            return

        # ---- baseline BEFORE the park ------------------------------------
        base_marker_id = id(marker)
        text_node = marker.firstChild
        base_text_id = id(text_node)
        base_attr = marker.getAttribute("tag")
        base_text = text_node.data

        # Sanity: the freshly parsed values must already be the wid-unique ones
        # (a mismatch here would be a same-fiber parse corruption, still a bug).
        if base_attr != expected_attr:
            H.fail("fresh parse attribute WRONG: marker tag={0!r}, expected {1!r} "
                   "(wid {2}, idx {3}) -- expat produced the wrong attribute".format(
                       base_attr, expected_attr, wid, idx))
            return
        if base_text != expected_text:
            H.fail("fresh parse text WRONG: marker text={0!r}, expected {1!r} "
                   "(wid {2}, idx {3}) -- expat entity-expansion produced the "
                   "wrong character data".format(
                       base_text, expected_text, wid, idx))
            return

        # ---- PARK: let a sibling drive its own expat parse on this hub ----
        runloom.yield_now()
        if idx & 1:
            runloom.sleep(0.0003)

        # ---- re-read the SAME held references AFTER the park -------------
        if id(marker) != base_marker_id:
            H.fail("held marker Element IDENTITY CHANGED across a park: id {0} -> "
                   "{1} (wid {2}, idx {3}) -- a sibling's parse replaced this "
                   "fiber's held node".format(
                       base_marker_id, id(marker), wid, idx))
            return
        if id(marker.firstChild) != base_text_id:
            H.fail("held marker firstChild(Text) IDENTITY CHANGED across a park: "
                   "id {0} -> {1} (wid {2}, idx {3}) -- the text node was swapped "
                   "under a concurrent parse".format(
                       base_text_id, id(marker.firstChild), wid, idx))
            return
        now_attr = marker.getAttribute("tag")
        if now_attr != expected_attr:
            H.fail("held marker attribute CHANGED across a park: tag {0!r} -> "
                   "{1!r}, expected {2!r} (wid {3}, idx {4}) -- a sibling's expat "
                   "state bled into this fiber's held Element".format(
                       base_attr, now_attr, expected_attr, wid, idx))
            return
        now_text = marker.firstChild.data
        if now_text != expected_text:
            H.fail("held marker text CHANGED across a park: data {0!r} -> {1!r}, "
                   "expected {2!r} (wid {3}, idx {4}) -- a sibling's character-data "
                   "buffer overwrote this fiber's held Text node".format(
                       base_text, now_text, expected_text, wid, idx))
            return

        # ---- toxml() -> re-parse ROUND-TRIP conservation ----------------
        serialized = doc.toxml()
        doc2 = minidom.parseString(serialized)
        marker2 = get_marker(doc2)
        if marker2 is None or marker2.firstChild is None:
            H.fail("round-trip re-parse lost the <marker> element or its text "
                   "(wid {0}, idx {1}) -- toxml()/re-parse did not conserve the "
                   "tree under concurrent parses".format(wid, idx))
            return
        rt_attr = marker2.getAttribute("tag")
        rt_text = marker2.firstChild.data
        if rt_attr != expected_attr:
            H.fail("round-trip attribute NOT CONSERVED: after toxml()->re-parse "
                   "tag={0!r}, expected {1!r} (wid {2}, idx {3})".format(
                       rt_attr, expected_attr, wid, idx))
            return
        if rt_text != expected_text:
            H.fail("round-trip text NOT CONSERVED: after toxml()->re-parse "
                   "text={0!r}, expected {1!r} (wid {2}, idx {3}) -- a character "
                   "was dropped/rewritten across the serialize/rebuild cycle".format(
                       rt_text, expected_text, wid, idx))
            return

        # NON-VACUITY tally (sharded wid & 1023 -- report/tally only, NOT a
        # conservation counter; a lost tally increment cannot cause a false FAIL).
        state["checks"][wid & 1023] += 1
    finally:
        # unlink() breaks the parent<->child reference cycles minidom builds, so
        # repeated parses under sustained churn do not leak the whole tree until GC.
        if doc is not None:
            doc.unlink()
        if doc2 is not None:
            doc2.unlink()


def worker(H, wid, rng, state):
    """Each fiber sustains the single-owner parse/hold/round-trip check.  Both arms
    operate ONLY on Documents this fiber created (never shared), so any observed
    identity/attr/text change or round-trip loss is a real runtime bug, not
    documented shared-object semantics."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            identity_and_roundtrip_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "checks": [0] * 1024,        # LOAD-BEARING single-owner checks (tally)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("minidom[single-owner LOAD-BEARING]: {0} node-identity + toxml round-trip "
          "checks (all passed fail-fast); ops={1}".format(checks, H.total_ops()))

    # NON-VACUITY: the load-bearing hold-across-park + round-trip hazard actually ran.
    H.check(checks > 0,
            "no single-owner minidom identity/round-trip checks ran -- the load-"
            "bearing parse-isolation hazard was never exercised (oracle vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside expat).
    H.require_no_lost("minidom node identity + round-trip")


if __name__ == "__main__":
    harness.main(
        "p528_minidom_node_identity_roundtrip", body, setup=setup, post=post,
        default_funcs=5000,
        describe="xml.dom.minidom.parseString drives expat to build a pure-Python "
                 "Node tree.  Under M:N, each fiber parses its OWN wid-marked "
                 "document, HOLDS the marker Element across a park while a sibling "
                 "drives its own expat parse on the same hub, then re-reads.  LOAD-"
                 "BEARING: the held Element's id(), getAttribute() and "
                 "firstChild.data are stable across the yield, AND toxml()->re-parse "
                 "conserves the wid-unique attribute and entity-decoded text "
                 "exactly.  Single-owner throughout (Document unlink()ed in finally) "
                 "-- a torn node, cross-fiber expat-buffer bleed, or round-trip loss "
                 "is the runloom bug")
