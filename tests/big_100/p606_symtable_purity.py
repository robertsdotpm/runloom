"""big_100 / 606 -- symtable.symtable() parse PURITY + single-owner object stability under M:N.

symtable.symtable(source, filename, compile_type) runs the CPython symbol-table
pass (the compiler's scope-analysis phase, implemented in C in the _symtable
module + Python wrappers in Lib/symtable.py) over `source` and returns a
SymbolTable OBJECT tree: each table exposes get_type()/get_name()/
get_identifiers()/get_symbols()/get_children(), and each Symbol exposes a fixed
set of scope flags (is_parameter / is_global / is_nonlocal / is_local / is_free /
is_referenced / is_assigned / is_namespace).  For a FIXED source string the whole
tree -- every table, every identifier set, every symbol's flag vector -- is a
PURE FUNCTION of the source: two independent parses of the same bytes must be
bit-identical, and a single parse's object must not mutate after it is produced.

WHERE M:N COULD BREAK IT (the gap this program probes).  The symbol-table pass
threads a fair amount of C state through the compiler (the `struct symtable`, its
`st_cur`/`st_stack`, the interned-name dict, the per-block symbol dicts) while it
recursively descends the AST.  Under free-threaded 3.14t with the GIL off and
runloom fibers migrating across hubs mid-parse, if any of that build-time state
were shared/global rather than per-call, a fiber parsing SOURCE_A that yields (or
is preempted) while a sibling parses SOURCE_B on another hub could observe a
CORRUPTED table: a scope flag from the sibling's parse, an identifier that
belongs to the sibling's source, a child table grafted from the wrong AST, or a
torn Symbol.  Each fiber here parses a source whose every user identifier is
tagged with its OWN wid ("vw<wid>_..."), so a cross-fiber leak is not only a
fingerprint mismatch but a directly-nameable FOREIGN identifier (one carrying a
DIFFERENT wid's tag) appearing in this fiber's table.

WHICH ORACLE IS LOAD-BEARING, AND WHY.  symtable.symtable() is a pure analyzer:
the produced SymbolTable is OWNED by the single fiber that called it (never
shared), and the mapping source -> table is deterministic.  Two laws follow and
are both fail-fast:

  * PURITY (identity across a yield): the fiber fingerprints its table, yields to
    let siblings parse conflicting sources on other hubs, then RE-PARSES the SAME
    source and fingerprints again -- the two fingerprints MUST be byte-identical
    (a pure function of identical input).  A mismatch means the second parse saw
    corrupted build-time state.
  * OBJECT STABILITY (torn single-owner object): the fiber ALSO re-fingerprints
    the FIRST table object (held across the yield) after the yield -- a live
    SymbolTable is immutable once built, so its fingerprint must not change.  A
    change means the produced object was mutated/torn by a concurrent parse.
  * CLOSED-WORLD IDENTIFIER OWNERSHIP: every user identifier in the fiber's table
    that matches the tag pattern "vw<n>_" MUST have n == this fiber's wid.  A
    foreign-wid identifier is a direct cross-fiber leak.  (Untagged names --
    builtins like `range`, compiler-generated `.format` / `__classdict__` /
    `__annotate__` -- are ignored: they belong to no fiber.)

All three test an object created and owned by ONE fiber from a fiber-local source
string; nothing is shared between fibers, so a violation is a genuine runtime
fault (cross-fiber leak of build-time compiler state, a torn table object, or an
identity/value change across a yield), never documented Python semantics.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside the C
    symbol-table pass (parked mid-parse and never re-woken) never returns; the
    watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

Stresses: the _symtable C symbol-table pass (scope analysis, interned-name
handling, per-block symbol dicts, nested-function/class/comprehension descent)
run concurrently across hubs with the GIL off; per-call build-state isolation;
SymbolTable/Symbol object stability across fiber yield + hub migration; pure-
function determinism of source -> symbol-table under M:N churn.

Good TSan / controlled-M:N-replay target: if the symbol-table build state is not
fully per-call, a data race on the shared compiler struct or a replay that
descends a sibling's AST mid-parse surfaces as a fingerprint mismatch or a
foreign-wid identifier before anything else.
"""
import re

import symtable

import harness
import runloom

# User identifiers in every generated source are tagged "vw<wid>_...".  A table
# identifier matching this pattern whose captured wid != the owning fiber's wid is
# a cross-fiber leak.  Untagged names (builtins, compiler-generated dunder/dotted
# names) never match and are correctly ignored.
TAG_RE = re.compile(r"vw(\d+)_")

# Sustained checks per worker.  The cross-parse corruption hazard only manifests
# under SUSTAINED churn: many fibers simultaneously running the C symbol-table
# pass over DIFFERENT sources while sleep-PARKED across their yield, so a sibling's
# parse reliably interleaves before this fiber's second parse.  A single check per
# fiber barely overlaps and does not reproduce.
INNER_CAP = 100000


def gen_source(wid, variant):
    """Build a fiber-local Python source string whose every USER identifier is
    tagged with `wid` ("vw<wid>_...").  `variant` (0..V-1) shifts the shape --
    number of module globals, functions, and whether a nested closure / class /
    comprehension is present -- so sibling fibers parse structurally DIFFERENT
    sources (more chance for a shared-build-state leak to manifest), while the
    source remains a deterministic function of (wid, variant) so re-parsing it is
    bit-identical.

    Exercised scope features: module globals, `global`/`nonlocal` statements,
    function params + locals, a nested closure (free variables), a list
    comprehension (its own scope), and a class body with a method."""
    t = "vw{0}_".format(wid)
    nglob = 2 + (variant % 3)
    nfunc = 1 + (variant % 2)
    lines = []
    for gi in range(nglob):
        lines.append("{0}G{1} = {2}".format(t, gi, gi + 1))
    for fi in range(nfunc):
        lines.append("def {0}fn{1}({0}a, {0}b):".format(t, fi))
        lines.append("    global {0}G0".format(t))
        lines.append("    {0}c = {0}a + {0}b".format(t))
        # nested closure with a free variable + nonlocal
        lines.append("    def {0}inner{1}({0}d):".format(t, fi))
        lines.append("        nonlocal {0}c".format(t))
        lines.append("        return {0}c + {0}d + {0}a".format(t))
        # comprehension: its own scope with an iteration variable
        lines.append("    {0}e = [{0}z for {0}z in range({0}c)]".format(t))
        lines.append("    return {0}inner{1}, {0}e".format(t, fi))
    # a class body with a method referencing a class attribute
    lines.append("class {0}K:".format(t))
    lines.append("    {0}attr = {0}G0".format(t))
    lines.append("    def {0}m(self, {0}x):".format(t))
    lines.append("        return self.{0}attr + {0}x".format(t))
    return "\n".join(lines) + "\n"


def fingerprint(table):
    """Canonical, order-stable tuple fingerprint of a SymbolTable subtree.

    Captures everything a correct parse determines: table type + name, the sorted
    identifier set, each Symbol's full scope-flag vector (sorted by name), and the
    fingerprints of all child tables (recursively).  Two parses of the same source
    MUST produce equal fingerprints; a live table's fingerprint MUST NOT change
    after it is built."""
    idents = tuple(sorted(table.get_identifiers()))
    syms = []
    for s in sorted(table.get_symbols(), key=lambda x: x.get_name()):
        syms.append((
            s.get_name(),
            s.is_parameter(),
            s.is_global(),
            s.is_nonlocal(),
            s.is_local(),
            s.is_free(),
            s.is_referenced(),
            s.is_assigned(),
            s.is_namespace(),
        ))
    children = tuple(fingerprint(c) for c in table.get_children())
    return (table.get_type(), table.get_name(), idents, tuple(syms), children)


def foreign_ident(table, wid):
    """Walk the whole table tree; return the first identifier whose "vw<n>_" tag
    carries a DIFFERENT wid than `wid` (a direct cross-fiber leak), or None."""
    stack = [table]
    while stack:
        t = stack.pop()
        for ident in t.get_identifiers():
            m = TAG_RE.match(ident)
            if m is not None and int(m.group(1)) != wid:
                return ident
        stack.extend(t.get_children())
    return None


def one_check(H, wid, idx, state):
    """Single-owner symtable purity + stability check.

    Parse a fiber-local source, fingerprint the produced (single-owner) table,
    yield to let siblings parse conflicting sources on other hubs, then verify:
    (1) the SAME table object is unchanged (not torn), (2) a fresh re-parse of the
    same source is bit-identical (purity), (3) no foreign-wid identifier leaked in
    from a sibling's source (closed-world ownership)."""
    variant = idx & 7
    src = gen_source(wid, variant)
    fname = "<w{0}_{1}>".format(wid, variant)

    st1 = symtable.symtable(src, fname, "exec")
    fp1 = fingerprint(st1)

    # Closed-world ownership BEFORE the yield: the parse we just did must contain
    # only this fiber's tagged identifiers (plus untagged builtins/dunders).
    leak = foreign_ident(st1, wid)
    if leak is not None:
        H.fail("symtable produced FOREIGN identifier {0!r} in wid {1}'s table "
               "(tagged for a different fiber) -- a cross-fiber leak of the "
               "symbol-table build state".format(leak, wid))
        return

    # YIELD: let siblings run the C symbol-table pass over DIFFERENT sources on
    # other hubs while this fiber's table object is live and about to be re-parsed.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    # (1) OBJECT STABILITY: the first table (held across the yield) is immutable
    # once built; its fingerprint must be unchanged (no torn/mutated object).
    fp1_after = fingerprint(st1)
    if fp1_after != fp1:
        H.fail("SymbolTable OBJECT CHANGED across a yield (wid {0}): the live "
               "single-owner table's fingerprint mutated -- a concurrent parse on "
               "another hub tore this fiber's table object".format(wid))
        return

    # (2) PURITY: an independent re-parse of the SAME source must be bit-identical.
    st2 = symtable.symtable(src, fname, "exec")
    fp2 = fingerprint(st2)
    if fp2 != fp1:
        H.fail("symtable PURITY VIOLATION (wid {0}): re-parsing the identical "
               "source produced a DIFFERENT symbol table across a yield -- the "
               "second parse observed corrupted build-time compiler state from a "
               "sibling's concurrent parse".format(wid))
        return

    # (3) closed-world ownership on the second parse too.
    leak2 = foreign_ident(st2, wid)
    if leak2 is not None:
        H.fail("symtable re-parse produced FOREIGN identifier {0!r} in wid {1}'s "
               "table -- a cross-fiber leak of the symbol-table build state".format(
                   leak2, wid))
        return

    state["checks"][wid] += 1


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            one_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # checks[] is ONE slot per worker (wid-indexed, single-writer -> race-free);
    # allocated here where H.funcs is known.  Feeds the non-vacuity law only.
    H.state = {
        "checks": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("symtable single-owner PURITY + STABILITY checks: {0} (every parse "
          "bit-identical across a yield, no torn table, no foreign-wid identifier "
          "-- all passed fail-fast); ops={1}".format(checks, H.total_ops()))

    # NON-VACUITY: the load-bearing purity/stability hazard was actually exercised.
    H.check(checks > 0,
            "no symtable purity/stability checks ran -- the load-bearing "
            "symbol-table-parse hazard was never exercised (oracle would be "
            "vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished inside the C symbol-table pass.
    H.require_no_lost("symtable purity")


if __name__ == "__main__":
    harness.main(
        "p606_symtable_purity", body, setup=setup, post=post,
        default_funcs=6000,
        describe="symtable.symtable() is a PURE analyzer producing a single-owner "
                 "SymbolTable tree.  Under M:N (GIL off, fibers migrating mid-parse "
                 "across hubs), if the C symbol-table build state is not per-call, "
                 "a fiber parsing one source could observe a table corrupted by a "
                 "sibling's concurrent parse.  LOAD-BEARING: each fiber parses a "
                 "source whose every identifier is tagged with its own wid, "
                 "fingerprints the table, yields, then asserts (1) the same table "
                 "object is unchanged, (2) a re-parse of the same source is bit-"
                 "identical, (3) no foreign-wid identifier leaked in.  A fingerprint "
                 "mismatch, torn table, or foreign identifier is the runloom bug")
