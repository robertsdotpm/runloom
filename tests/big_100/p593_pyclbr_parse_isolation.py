"""big_100 / 593 -- pyclbr class/function tree parse ISOLATION under M:N.

pyclbr parses a Python module's source with `ast` and walks it with an
`_ModuleBrowser(ast.NodeVisitor)` to build a dict tree of top-level names ->
`Class` / `Function` (`_Object` subclasses) objects, each carrying `lineno`,
`super`, `methods`, `children`, `is_async`, etc.  The public entry points
`readmodule_ex` / `readmodule` wrap `_readmodule`, which layers a process-global
`_modules` cache + importlib file lookup around the parse core `_create_tree`.

The CACHE (`pyclbr._modules`) is a shared, process-global dict -- NOT a single-
owner object -- so it is NOT the oracle here (per the big_100 shared-mutable
rule).  What IS single-owner is the parse core itself: `_create_tree(module,
path, file, source, tree, inpackage)` builds a BRAND-NEW `tree={}` dict of
BRAND-NEW `Class`/`Function` objects from the in-memory `source` string, using a
freshly-constructed `_ModuleBrowser` whose only mutable state (its `.stack` and
the `tree` it fills) is local to that one call.  When the source contains NO
top-level imports and NO dotted (`module.Class`) base classes, `_create_tree`
never reads or writes the shared `_modules` cache at all (verified by reading
Lib/pyclbr.py: `_modules` is touched only in `visit_Import`/`visit_ImportFrom`
and the dotted-base branch of `visit_ClassDef`).  So each `_create_tree` call is
a pure, self-contained, single-owner parse -- ideal for an M:N isolation oracle.

WHERE M:N COULD BREAK IT (the gap this program probes).  Tens of thousands of
fibers across >1 hubs each parse their OWN fiber-local source (its class/function
NAMES encode the fiber's wid) into their OWN fresh tree, yield mid-flight so
siblings interleave on the same hubs, then re-parse the identical source.  If the
runtime leaks one fiber's parse state into another's (a torn/shared `_ModuleBrowser`
stack, a cross-fiber object identity mix-up, a lost-wakeup stranding a fiber inside
the visitor, or a SIGSEGV in the ast walk under GIL-off contention), a fiber would
observe a tree whose NAMES are not its own wid's names, or whose STRUCTURE differs
between two parses of byte-identical source.  On a correct runtime, parsing fixed
source is a pure function: the tree's shape is a deterministic constant.

WHICH ORACLE IS LOAD-BEARING, AND WHY:

  Parsing a FIXED source string is referentially transparent -- `ast.parse` +
  the `_ModuleBrowser` walk must yield the SAME tree structure every time,
  independent of any other fiber, because nothing shared is read.  We verified
  with a plain-threads control (8 OS threads each `_create_tree`-parsing their own
  wid-tagged source in a tight loop, GIL on AND off) that 100% of parses produce
  the closed-form-expected tree with 0 cross-thread name leaks and 0 structural
  drift.  Under a correct runloom it must also hold.  A parse that returns another
  fiber's names, a missing/extra top-level name, a changed lineno/methods/super/
  children set, or a structure that differs across a yield, is a runtime parse-
  isolation bug -- and on a correct runtime the oracle PASSES (exit 0).

ORACLES:
  * LOAD-BEARING -- PARSE ISOLATION (worker, HARD, fail-fast).  Each fiber owns a
    fiber-local source string whose Base/Derived class names, methods, nested
    Inner class, and top-level (async) functions all embed its wid.  It computes
    the CLOSED-FORM expected tree (name set + per-name kind/super/methods/children
    /is_async) directly from the generator -- NOT from a parse -- so a consistent
    corruption cannot hide.  Each round it:
      - parses the source into a fresh tree via `_create_tree` (no cache, no I/O),
      - asserts the tree matches the closed-form expectation (every top-level name
        is present, no extra/foreign name, every key carries THIS wid's tag, each
        object's kind/super-names/methods/children/is_async are exactly right),
      - snapshots the tree structure, YIELDS (so siblings parse/interleave),
      - parses AGAIN into another fresh tree and asserts its snapshot is byte-
        identical to the pre-yield snapshot AND still matches the closed form.
    Single-owner: every tree, every `Class`/`Function`, and the `_ModuleBrowser`
    are created and read by ONE fiber; nothing is shared.  A failure is a runloom
    parse-isolation desync (cross-fiber name leak / torn tree / structural drift).

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside the ast
    walk or the visitor (parked-then-vanished) never returns; the watchdog +
    require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (parse_checks > 0).

FAIL ON: a fiber's fresh parse of its own fixed source yielding a wrong/foreign
name set, a mismatched kind/super/methods/children/is_async, or a tree structure
that differs between two parses of identical source across a yield.  There is NO
report-only shared arm: `_create_tree` on import-free source touches nothing
shared, so every observation is load-bearing.

Stresses: pyclbr `_create_tree` / `_ModuleBrowser(ast.NodeVisitor)` tree build,
`Class`/`Function`/`_Object` construction (lineno/super/methods/children/is_async
wiring, `parent.children`/`parent.methods` back-links), `ast.parse` +
generic_visit recursion under GIL-off M:N churn + hub migration across a yield.

Good TSan / controlled-M:N-replay target: the `_ModuleBrowser.stack` push/pop
around `generic_visit` and the `parent.children[name] = self` back-link are the
per-call mutable state; a data-race report on a tree/stack entry, or a replay
that returns a foreign name mid-walk, localizes a parse-isolation break before
the closed-form structural oracle even fires.
"""
import ast

import harness
import runloom
import pyclbr


def gen_source(tag):
    """Build a fiber-local Python SOURCE string whose class/function structure
    embeds `tag`, plus the CLOSED-FORM expected tree description.

    The source has NO top-level imports and NO dotted base classes, so parsing it
    with `pyclbr._create_tree` never touches the shared `pyclbr._modules` cache.

    Returns (source_text, expected_names, expected_meta, expected_snapshot).
      expected_names    -- set of top-level names the tree must have (exactly)
      expected_meta     -- name -> (kind, is_async_or_None, frozenset(super_names),
                                     frozenset(method_names), frozenset(child_names))
      expected_snapshot -- the canonical structural tuple a correct parse yields
                           (used for cross-yield stability comparison)
    """
    lines = []

    def emit(s):
        lines.append(s)
        return len(lines)               # 1-based lineno of the line just added

    base = "Base_{0}".format(tag)
    m_a = "m_a_{0}".format(tag)
    m_b = "m_b_{0}".format(tag)
    der = "Derived_{0}".format(tag)
    am = "am_{0}".format(tag)
    inner = "Inner_{0}".format(tag)
    im = "im_{0}".format(tag)
    fn = "func_{0}".format(tag)
    afn = "afunc_{0}".format(tag)

    ln_base = emit("class {0}:".format(base))
    ln_ma = emit("    def {0}(self):".format(m_a))
    emit("        return {0}".format(tag))
    ln_mb = emit("    def {0}(self, x):".format(m_b))
    emit("        return x")

    ln_der = emit("class {0}({1}):".format(der, base))
    ln_am = emit("    async def {0}(self):".format(am))
    emit("        return {0}".format(tag + 1))
    ln_inner = emit("    class {0}:".format(inner))
    ln_im = emit("        def {0}(self):".format(im))
    emit("            return 0")

    ln_fn = emit("def {0}():".format(fn))
    emit("    return {0}".format(tag))
    ln_afn = emit("async def {0}():".format(afn))
    emit("    return {0}".format(tag))

    source = "\n".join(lines) + "\n"

    # ---- closed-form expected structure (computed, NOT parsed) --------------
    #  Base: plain class, two direct methods (also its children).
    #  Derived(Base): known super (in tree) -> super name {Base}; one async method
    #    'am' (a method AND a child); nested Inner class is a child but NOT a
    #    method.  Inner's own method 'im' belongs to Inner, not Derived.
    #  func: sync top-level function.  afunc: async top-level function.
    expected_names = {base, der, fn, afn}
    expected_meta = {
        base: ("class", None, frozenset(), frozenset((m_a, m_b)),
               frozenset((m_a, m_b))),
        der:  ("class", None, frozenset((base,)), frozenset((am,)),
               frozenset((am, inner))),
        fn:   ("func", False, frozenset(), frozenset(), frozenset()),
        afn:  ("func", True, frozenset(), frozenset(), frozenset()),
    }

    # Expected canonical snapshot (mirrors snapshot() below, incl. exact linenos).
    expected_snapshot = tuple(sorted((
        ("class", base, ln_base, (), tuple(sorted(((m_a, ln_ma), (m_b, ln_mb)))),
         tuple(sorted((m_a, m_b)))),
        ("class", der, ln_der, (base,), ((am, ln_am),),
         tuple(sorted((am, inner)))),
        ("func", fn, ln_fn, False, ()),
        ("func", afn, ln_afn, True, ()),
    )))
    # linenos referenced so a generator edit that desyncs them is obvious.
    assert ln_im > ln_inner and ln_afn > ln_fn, "source generator lineno desync"
    return source, expected_names, expected_meta, expected_snapshot


def snap_obj(name, obj):
    """Canonical, hashable structural fingerprint of one top-level tree object."""
    if isinstance(obj, pyclbr.Class):
        sup = tuple(s.name if isinstance(s, pyclbr._Object) else s
                    for s in obj.super)
        meth = tuple(sorted(obj.methods.items()))
        children = tuple(sorted(obj.children.keys()))
        return ("class", name, obj.lineno, sup, meth, children)
    if isinstance(obj, pyclbr.Function):
        children = tuple(sorted(obj.children.keys()))
        return ("func", name, obj.lineno, bool(obj.is_async), children)
    return ("other", name, repr(obj))


def snapshot(tree):
    """Deterministic canonical snapshot of the whole top-level tree."""
    return tuple(sorted(snap_obj(n, o) for n, o in tree.items()))


def parse_tree(module_name, source):
    """Drive pyclbr's parse CORE directly on in-memory source -> a fresh, single-
    owner tree (no `_modules` cache, no file I/O).  Mirrors what readmodule_ex
    does internally after it has fetched the source, minus the shared cache."""
    tree = {}
    # (fullmodule, path, file, source, tree, inpackage); path=[]/inpackage=None
    # is the top-level (non-package) case.  A synthetic file name is fine: it is
    # only stored on the objects, never opened.
    pyclbr._create_tree(module_name, [], module_name + ".py", source, tree, None)
    return tree


def check_closed_form(H, wid, tree, expected_names, expected_meta):
    """Assert `tree` matches the CLOSED-FORM expectation.  Returns True on match;
    calls H.fail and returns False on any deviation (foreign/missing name, wrong
    kind/super/methods/children/is_async, or a key missing this wid's tag)."""
    tag = "_{0}".format(wid)
    got_names = set(tree.keys())
    if got_names != expected_names:
        H.fail("pyclbr parse NAME-SET mismatch (wid {0}): got {1!r}, expected "
               "{2!r} -- a cross-fiber name leak, missing, or extra top-level "
               "object in a single-owner parse".format(
                   wid, sorted(got_names), sorted(expected_names)))
        return False
    for name, obj in tree.items():
        if not name.endswith(tag):
            H.fail("pyclbr parse FOREIGN name (wid {0}): top-level name {1!r} does "
                   "not carry this fiber's tag {2!r} -- another fiber's parse state "
                   "leaked into this tree".format(wid, name, tag))
            return False
        kind, is_async, exp_super, exp_meth, exp_children = expected_meta[name]
        if kind == "class":
            if not isinstance(obj, pyclbr.Class):
                H.fail("pyclbr parse KIND mismatch (wid {0}): {1!r} expected Class, "
                       "got {2!r}".format(wid, name, type(obj).__name__))
                return False
            got_super = frozenset(
                s.name if isinstance(s, pyclbr._Object) else s for s in obj.super)
            if got_super != exp_super:
                H.fail("pyclbr parse SUPER mismatch (wid {0}): {1!r} super {2!r} != "
                       "expected {3!r}".format(wid, name, sorted(got_super),
                                               sorted(exp_super)))
                return False
            if frozenset(obj.methods.keys()) != exp_meth:
                H.fail("pyclbr parse METHODS mismatch (wid {0}): {1!r} methods {2!r} "
                       "!= expected {3!r}".format(wid, name,
                                                  sorted(obj.methods.keys()),
                                                  sorted(exp_meth)))
                return False
            if frozenset(obj.children.keys()) != exp_children:
                H.fail("pyclbr parse CHILDREN mismatch (wid {0}): {1!r} children "
                       "{2!r} != expected {3!r}".format(
                           wid, name, sorted(obj.children.keys()),
                           sorted(exp_children)))
                return False
        else:  # function
            if not isinstance(obj, pyclbr.Function):
                H.fail("pyclbr parse KIND mismatch (wid {0}): {1!r} expected "
                       "Function, got {2!r}".format(wid, name, type(obj).__name__))
                return False
            if bool(obj.is_async) != is_async:
                H.fail("pyclbr parse IS_ASYNC mismatch (wid {0}): {1!r} is_async "
                       "{2!r} != expected {3!r}".format(
                           wid, name, bool(obj.is_async), is_async))
                return False
            if frozenset(obj.children.keys()) != exp_children:
                H.fail("pyclbr parse CHILDREN mismatch (wid {0}): {1!r} function "
                       "children {2!r} != expected {3!r}".format(
                           wid, name, sorted(obj.children.keys()),
                           sorted(exp_children)))
                return False
    return True


def worker(H, wid, rng, state):
    """Each fiber parses its OWN wid-tagged source twice per round, across a yield,
    asserting closed-form correctness and cross-yield structural stability of the
    single-owner tree.  Everything is fiber-local -- no shared object is read."""
    module_name = "big100_pyclbr_w{0}".format(wid)
    source, expected_names, expected_meta, expected_snapshot = gen_source(wid)

    # Sanity: the very first parse must already match the closed form (a
    # generator/version desync surfaces here, before the concurrency loop).
    first = parse_tree(module_name, source)
    if not check_closed_form(H, wid, first, expected_names, expected_meta):
        return
    if snapshot(first) != expected_snapshot:
        H.fail("pyclbr parse SNAPSHOT != closed-form snapshot (wid {0}): the "
               "parse core produced an unexpected structure for fixed source -- "
               "generator/version desync or torn parse".format(wid))
        return

    checks = state["parse_checks"]
    jitter = bool(wid & 1)

    for _ in H.round_range():
        if not H.running():
            break
        # Parse #1 into a fresh single-owner tree.
        t1 = parse_tree(module_name, source)
        if not check_closed_form(H, wid, t1, expected_names, expected_meta):
            return
        s1 = snapshot(t1)
        if s1 != expected_snapshot:
            H.fail("pyclbr parse #1 SNAPSHOT drift (wid {0}): parse of fixed "
                   "source diverged from the closed-form structure under M:N "
                   "contention".format(wid))
            return

        # YIELD at the hazard boundary so siblings parse/interleave on this hub.
        runloom.yield_now()
        if jitter:
            runloom.sleep(0.0002)

        # Parse #2 (byte-identical source) -- must be structurally identical.
        t2 = parse_tree(module_name, source)
        s2 = snapshot(t2)
        if s2 != s1:
            H.fail("pyclbr parse STABILITY broken (wid {0}): two parses of the "
                   "SAME source across a yield produced different trees -- a "
                   "cross-fiber parse-state leak or torn ast walk".format(wid))
            return
        if s2 != expected_snapshot:
            H.fail("pyclbr parse #2 SNAPSHOT drift (wid {0}): post-yield parse "
                   "diverged from the closed-form structure".format(wid))
            return
        if not check_closed_form(H, wid, t2, expected_names, expected_meta):
            return

        checks[wid & 1023] += 1        # sharded NON-VACUITY tally (report-only)
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {
        "parse_checks": [0] * 1024,    # sharded non-vacuity tally (not a law)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    pchecks = sum(H.state["parse_checks"])
    H.log("pyclbr[single-owner LOAD-BEARING]: {0} closed-form parse-isolation "
          "rounds (each = 2 fresh _create_tree parses across a yield, all passed "
          "fail-fast); ops={1}".format(pchecks, H.total_ops()))

    # NON-VACUITY: the load-bearing parse-isolation hazard actually ran.
    H.check(pchecks > 0,
            "no single-owner pyclbr parse rounds ran -- the parse-isolation "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished inside the ast walk / visitor.
    H.require_no_lost("pyclbr parse isolation")


if __name__ == "__main__":
    harness.main(
        "p593_pyclbr_parse_isolation", body, setup=setup, post=post,
        default_funcs=4000,
        describe="pyclbr builds a Class/Function tree by walking ast with an "
                 "_ModuleBrowser NodeVisitor.  We drive the parse CORE "
                 "(_create_tree) directly on fiber-local, import-free source so "
                 "each parse is a pure single-owner build that never touches the "
                 "shared _modules cache.  LOAD-BEARING: each fiber parses its own "
                 "wid-tagged source into a fresh tree, checks it against the "
                 "closed-form expected structure (names/super/methods/children/"
                 "is_async/lineno), yields, and re-parses -- the two trees must be "
                 "structurally identical and match the closed form.  A foreign/"
                 "missing name, wrong structure, or cross-yield drift is the "
                 "runloom parse-isolation bug")
