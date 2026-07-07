"""big_100 / 594 -- pydoc documentation rendering stability under M:N.

pydoc turns a live Python object (module / class / function) into a formatted
documentation string.  The workhorse is a renderer instance -- pydoc.TextDoc
(plain text) or pydoc.HTMLDoc -- whose .docclass()/.docroutine()/.document()
methods walk the object with inspect (getmembers, signature, getdoc,
classify_class_attrs) and assemble a string.  The render is a PURE FUNCTION of
the object: for a fixed object, TextDoc().docclass(obj) returns byte-identical
output every call (verified: two back-to-back docclass() calls on the same class
compare equal).

WHERE M:N COULD BREAK IT (the gap this program probes).  A fiber builds its OWN
distinct class (fiber-local, never shared) with a docstring, class attribute, and
several methods whose docstrings all embed the fiber's unique wid sentinel.  It
renders that class through a fiber-local pydoc.TextDoc() (single-owner) to a
baseline string, then YIELDS so siblings run on other hubs -- each rendering its
own distinct class through pydoc's C-recursing inspect walk + the shared reprlib
TextRepr config object (pydoc.Doc._repr_instance is a class attribute shared by
every renderer).  On resume the fiber RE-renders the SAME class and asserts the
output is byte-identical to the baseline and still carries ONLY its own wid
sentinel.  If the render machinery -- inspect's member walk, pydoc's attribute
classification, or the shared reprlib recursion-guard -- is not properly isolated
per fiber, the second render could differ from the first (torn output) or splice
in a sibling's sentinel (cross-fiber leak).  Neither may happen on a correct
runtime: the object is single-owner and the render is a pure function of it.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against a plain re-render):
  pydoc.render_doc / TextDoc().docclass on a FIXED object is deterministic --
  re-rendering the identical class yields the identical string.  We verified two
  successive renders of the same dynamically-built class compare byte-equal, and
  that the output contains exactly the fiber's own sentinel markers (class name
  Widget_<wid>, the "widget number <wid>" body, and each method's per-wid
  docstring).  Under a CORRECT runloom that determinism must survive a yield that
  interleaves siblings rendering their own classes: the re-render must equal the
  baseline and carry no foreign wid.  The load-bearing single-owner oracle PASSES
  on a correct runtime (exit 0 when there is no bug).

ORACLES:
  * LOAD-BEARING -- RENDER STABILITY + CONTENT (worker, HARD, fail-fast).  Each
    fiber:
      - builds its OWN class Widget_<wid> (exec into a private namespace; the
        class object is a fiber-local variable, never shared) whose class doc,
        class attribute value, and per-method docstrings all embed the unique
        token "WIDMARK<wid>Z";
      - renders it via a fiber-local pydoc.TextDoc() to a baseline string, and
        asserts the baseline actually carries the fiber's own markers (guards
        against a vacuous render);
      - YIELDs (yield_now + occasional tiny sleep) so siblings render on other
        hubs;
      - RE-renders the SAME class and asserts (a) byte-identical to the baseline
        (no torn output across the yield), and (b) the fiber's own token appears
        the expected number of times and NO OTHER fiber's token "WIDMARK<other>Z"
        appears (no cross-fiber splice).
    Single-owner: the class and the TextDoc renderer are fiber-local.  A failure
    is a pydoc/inspect render-isolation desync in runloom.

  * PURITY SIDE-CHECK (worker, HARD, fail-fast).  pydoc.splitdoc(getdoc(cls)) and
    pydoc.getdoc(cls) are recomputed across the same yield on the fiber-local
    class and must return the identical (synopsis, description) and docstring --
    a pure-function identity law on single-owner input.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (renders > 0).

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-render
    (inside inspect.getmembers / reprlib recursion) never returns; the watchdog +
    require_no_lost catch it.

FAIL ON: a re-render that differs from the baseline across a yield, a fiber's own
sentinel token missing or mis-counted, a foreign fiber's token appearing in this
fiber's render, or getdoc/splitdoc returning a different result across the yield.
All are single-owner / pure-function violations -- a real runtime bug (torn
object, cross-fiber leak, or corrupted shared reprlib recursion state), never
documented Python semantics.

Stresses: pydoc.TextDoc.docclass / render_doc over inspect.getmembers +
classify_class_attrs + inspect.signature + reprlib TextRepr recursion, per-fiber
renderer isolation vs the shared Doc._repr_instance config object, string
assembly determinism across hub migration + yield.

Good TSan / controlled-M:N-replay target: pydoc's render recurses through the
shared reprlib.Repr instance (Doc._repr_instance) and inspect's member walk; under
the single-owner arm the class is touched by one fiber only, so a data-race report
on the shared Repr recursion state, or a replay that renders mid-mutation of a
sibling's inspect cache, is the cleanest signal before the byte-equality oracle
fires.
"""
import pydoc

import harness
import runloom

# Source template for each fiber's private class.  Every docstring + the class
# attribute value embeds the fiber's unique sentinel token WIDMARK<wid>Z so a
# cross-fiber splice (another fiber's rendered text bleeding into this one) is
# detectable, and a torn re-render moves the byte content.  Several methods give
# pydoc's docroutine + signature walk real work per render.
CLASS_SRC = '''
class Widget_{w}:
    """Synopsis for widget {w} sentinel WIDMARK{w}Z.

    Longer description body for widget number {w}; carries the marker
    WIDMARK{w}Z again so the rendered class doc has two occurrences.
    """
    ATTR = "attrval_WIDMARK{w}Z"

    def compute(self, x, y=0):
        """Return x plus y plus {w}; marker WIDMARK{w}Z."""
        return x + y + {w}

    def label(self, prefix="w"):
        """Return the widget label for {w}; marker WIDMARK{w}Z."""
        return prefix + str({w})

    def describe(self):
        """Describe widget {w}; marker WIDMARK{w}Z."""
        return "widget {w}"
'''

# Expected number of times the fiber's own token WIDMARK<wid>Z appears in the
# rendered class doc.  Counting the template above: class-doc synopsis (1) +
# class-doc body (1) + ATTR value (1) + 3 method docstrings (3) = 6.  We assert
# the exact count so a DROPPED member (a torn inspect walk) or a DOUBLED one is
# caught, not just presence.
EXPECTED_TOKEN_COUNT = 6

# Sustained render churn per worker.  The render-isolation hazard only manifests
# under many fibers simultaneously walking inspect + the shared reprlib recursion
# state while sleep-parked across their yield, so a sibling reliably interleaves a
# render before this fiber resumes.  A single render per fiber barely overlaps and
# does not reproduce.
INNER_CAP = 100000


def build_class(wid):
    """Build this fiber's PRIVATE class in a fresh namespace (never shared)."""
    ns = {}
    exec(CLASS_SRC.format(w=wid), ns)
    return ns["Widget_{0}".format(wid)]


def render_check(H, wid, other_token, state):
    """Single-owner pydoc render stability + content oracle (fail-fast).

    Builds the fiber's own class, renders it via a fiber-local TextDoc, yields,
    re-renders, and asserts byte-identity + correct sentinel content with no
    foreign token spliced in."""
    cls = build_class(wid)
    my_token = "WIDMARK{0}Z".format(wid)
    td = pydoc.TextDoc()

    # Baseline render (single-owner renderer, single-owner class).
    baseline = td.docclass(cls)

    # Guard against a vacuous render: the fiber's own markers MUST be present now.
    base_count = baseline.count(my_token)
    if base_count != EXPECTED_TOKEN_COUNT:
        H.fail("baseline render for wid {0} has {1} occurrences of own token "
               "{2!r}, expected {3} -- the render dropped/doubled a member BEFORE "
               "any yield (a torn inspect walk on a single-owner class)".format(
                   wid, base_count, my_token, EXPECTED_TOKEN_COUNT))
        return

    # Pure-function side inputs recomputed after the yield.
    base_getdoc = pydoc.getdoc(cls)
    base_split = pydoc.splitdoc(base_getdoc)

    # YIELD: siblings render their own distinct classes on other hubs, driving the
    # shared reprlib recursion state + inspect walks concurrently.
    runloom.yield_now()
    if wid & 1:
        runloom.sleep(0.0003)

    # RE-render the SAME class; must be byte-identical to the baseline.
    again = td.docclass(cls)
    if again != baseline:
        # Locate the first divergence for the message.
        n = min(len(again), len(baseline))
        diff = next((i for i in range(n) if again[i] != baseline[i]), n)
        H.fail("render for wid {0} CHANGED across a yield: byte {1} differs "
               "(len {2} vs baseline {3}) -- pydoc.TextDoc.docclass is a pure "
               "function of a single-owner class, so a differing re-render is a "
               "torn render / cross-fiber render-state corruption".format(
                   wid, diff, len(again), len(baseline)))
        return

    # Own token still present the expected number of times.
    again_count = again.count(my_token)
    if again_count != EXPECTED_TOKEN_COUNT:
        H.fail("re-render for wid {0} has {1} occurrences of own token {2!r}, "
               "expected {3} -- a member was dropped/doubled across the yield "
               "(torn inspect walk under M:N)".format(
                   wid, again_count, my_token, EXPECTED_TOKEN_COUNT))
        return

    # No FOREIGN fiber's token spliced into this fiber's render (cross-fiber leak).
    if other_token in again:
        H.fail("render for wid {0} contains FOREIGN token {1!r} -- a sibling "
               "fiber's rendered documentation leaked into this single-owner "
               "render (cross-fiber pydoc render-state corruption)".format(
                   wid, other_token))
        return

    # Pure-function identity: getdoc / splitdoc unchanged across the yield.
    if pydoc.getdoc(cls) != base_getdoc:
        H.fail("pydoc.getdoc(cls) for wid {0} CHANGED across a yield -- a pure "
               "function of a single-owner class must be identity-stable".format(
                   wid))
        return
    if pydoc.splitdoc(pydoc.getdoc(cls)) != base_split:
        H.fail("pydoc.splitdoc(getdoc(cls)) for wid {0} CHANGED across a yield "
               "-- pure-function identity broken on single-owner input".format(
                   wid))
        return

    state["renders"][wid] += 1        # single-writer-per-slot (wid-indexed)


def worker(H, wid, rng, state):
    # A fixed foreign token from a DIFFERENT wid, used only to assert it never
    # appears in this fiber's own render.  wid+1 (or wid-1 for the top slot) is a
    # real sibling in the pool, so its token is one that IS being produced
    # concurrently -- the sharpest cross-fiber-leak probe.
    other_wid = wid + 1 if wid + 1 < H.funcs else wid - 1
    if other_wid < 0:
        other_wid = wid + 1
    other_token = "WIDMARK{0}Z".format(other_wid)

    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            render_check(H, wid, other_token, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # renders[] is the non-vacuity tally, ONE slot per worker (wid-indexed,
    # single-writer -> race-free).  Allocated here where H.funcs is known.
    H.state = {
        "renders": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    renders = sum(H.state["renders"])
    H.log("pydoc[single-owner LOAD-BEARING]: {0} render-stability checks (each: "
          "baseline docclass == re-render across a yield, own sentinel present "
          "x{1}, no foreign sentinel, getdoc/splitdoc identity-stable) -- all "
          "passed fail-fast; ops={2}".format(
              renders, EXPECTED_TOKEN_COUNT, H.total_ops()))

    # NON-VACUITY: the load-bearing render hazard was actually exercised.
    H.check(renders > 0,
            "no pydoc render-stability checks ran -- the load-bearing render-"
            "isolation hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside
    # inspect.getmembers / reprlib recursion during a render).
    H.require_no_lost("pydoc render stability")


if __name__ == "__main__":
    harness.main(
        "p594_pydoc_render", body, setup=setup, post=post,
        default_funcs=6000,
        describe="pydoc.TextDoc.docclass / render_doc is a PURE FUNCTION of the "
                 "object it documents (byte-identical output every call).  Under "
                 "M:N, each fiber builds its OWN class Widget_<wid> whose "
                 "docstrings embed a unique sentinel, renders it via a fiber-local "
                 "TextDoc, yields while siblings render their own classes through "
                 "inspect's member walk + the shared reprlib recursion state, then "
                 "RE-renders the SAME class.  LOAD-BEARING: the re-render MUST be "
                 "byte-identical to the baseline, carry the fiber's own sentinel "
                 "the exact expected number of times, splice in NO foreign "
                 "sentinel, and getdoc/splitdoc must be identity-stable -- a "
                 "differing re-render or a leaked foreign token is the runloom "
                 "render-isolation bug")
