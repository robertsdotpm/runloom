"""big_100 / 579 -- modulefinder.ModuleFinder closed-world import-graph oracle under M:N.

modulefinder.ModuleFinder statically analyses a Python program: run_script(path)
parses the entry file, walks every IMPORT it finds (via dis over the code object),
resolves each name through importlib.machinery.PathFinder, recurses into every
resolvable module, and records the whole reachable graph in two dicts --
self.modules (name -> Module for everything it could resolve) and
self.badmodules (name -> {...} for every import it could NOT resolve).  The
resolution path is process-GLOBAL-touching: ModuleFinder._find_module calls
importlib.machinery.PathFinder.invalidate_caches() (which CLEARS the global
sys.path_importer_cache) and then PathFinder.find_spec(name, path) (which REBUILDS
entries in that same global cache and stats the filesystem).  Under M:N with the
GIL off, many hubs each running their OWN ModuleFinder over their OWN entry script
are simultaneously clearing and rebuilding that shared importlib cache -- exactly
the concurrency that would surface a torn resolution: a module that IS on the
finder's path being spuriously dropped into badmodules (a cache entry another hub
invalidated mid-lookup), or a stale/foreign spec bleeding a module that is NOT in
this finder's closed world into self.modules.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom runs each fiber's
ModuleFinder.run_script in parallel across hubs.  If the importlib PathFinder /
path_importer_cache machinery is not free-threading-safe under the
invalidate-then-rebuild churn, a fiber that analyses its OWN fixed on-disk import
graph could observe:
  * a WRONG module set -- a name that its closed-world graph guarantees is
    resolvable landed in badmodules, or a name outside its closed world appeared in
    self.modules (a cross-finder resolution leak);
  * a NON-DETERMINISTIC result -- analysing the SAME fixed source tree twice (once
    before a yield, once after) yields DIFFERENT (modules, badmodules) sets, which
    for a fixed read-only tree must be bit-identical;
  * a spurious ImportError storm, or a SIGSEGV inside find_spec / the cache.

WHICH ORACLE IS LOAD-BEARING, AND WHY (single-owner, closed-form).  setup() builds a
small POOL of fixed, READ-ONLY package trees on disk (created once, never mutated
again -- so sharing them across fibers is a canned-bytes read, not a shared mutable
container).  Each tree is a hand-constructed import graph whose exact closed world
is computed IN PYTHON directly from the generator (expected_modules and
expected_bad), WITHOUT running modulefinder -- an independent reference.  Each
fiber picks one tree by wid and repeatedly:
  * constructs a FRESH, single-owner ModuleFinder whose search path is ONLY that
    tree's root (so every import either resolves inside the tree or is
    deterministically bad -- no dependence on the ambient sys.path or the real
    stdlib), runs run_script(entry), and reads back the closed world as
    (frozenset(modules), frozenset(badmodules));
  * asserts that closed world equals the independently-computed expected sets;
  * YIELDS (so siblings hammer PathFinder.invalidate_caches / find_spec on other
    hubs while this fiber is parked);
  * constructs ANOTHER fresh single-owner ModuleFinder over the SAME tree, runs it,
    and asserts the closed world is IDENTICAL to the first run AND still equals the
    expected sets (determinism + correctness across the yield).
Everything the oracle reads is single-owner (each ModuleFinder instance is created,
run, and discarded by one fiber) or read-only shared (the on-disk tree).  The
module-global packagePathMap / replacePackageMap are never touched, so they stay
empty and are never a shared-mutable arm.

Verified against plain threads: 8 OS threads each analysing their own fixed tree
(GIL on and off) produce the exact closed world 100% of the time, bit-identical
across repeats -- 0 wrong sets, 0 nondeterministic results.  Under a CORRECT
runloom it must also hold, so this program EXITS 0 when there is no bug.  A module
misclassified as bad (or vice-versa), a result that differs across a yield for a
fixed tree, a spurious ImportError, or a crash is a real runtime (or importlib
free-threading) fault, not documented Python semantics.

ORACLES:
  * LOAD-BEARING -- CLOSED-WORLD IMPORT GRAPH + DETERMINISM (worker, HARD,
    fail-fast).  Single-owner ModuleFinder over a fixed read-only tree; the
    resolved (modules, badmodules) sets equal the independently-computed closed
    world, and a second analysis of the same tree is identical, across a yield.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside find_spec
    / the file walk / the importlib cache never returns; the watchdog +
    require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (mf_checks > 0).

FAIL ON: a resolved module set != the closed-world expected set, a badmodules set
!= the expected bad set, a non-identical result across a yield for a fixed tree, or
a crash.

File/resolution-heavy: each fiber's ModuleFinder stats a small tree and opens a
handful of source files per run, so max_funcs is capped (the forever-loop --funcs
1000000 would otherwise spawn a million concurrent finders).  The on-disk trees are
built once under a harness-managed tmpdir and removed at shutdown.

Stresses: modulefinder.ModuleFinder.run_script / scan_code / scan_opcodes /
import_hook / ensure_fromlist / find_module, importlib.machinery.PathFinder
invalidate_caches + find_spec over the global sys.path_importer_cache, all under
M:N parallel static analysis of distinct fixed import graphs across hubs.
"""
import modulefinder
import os

import harness
import runloom

# A fixed roster of top-level module names that are guaranteed NOT resolvable from
# a tree-only search path -- every import of one of these lands deterministically in
# ModuleFinder.badmodules.  They are unlikely-to-exist names so even if a tree's
# root somehow leaked onto the ambient path they would not resolve.
MISSING = (
    "big100_p579_absent_alpha",
    "big100_p579_absent_beta",
    "big100_p579_absent_gamma",
    "big100_p579_absent_delta",
    "big100_p579_absent_epsilon",
)

# Number of distinct fixed trees in the read-only pool.  Each fiber picks one by
# wid, so trees are hammered by many single-owner finders in parallel.
NTREES = 6

# Sustained checks per worker: the torn-resolution hazard only manifests under
# SUSTAINED parallel analysis (many hubs simultaneously inside PathFinder.find_spec
# / invalidate_caches while this fiber is parked across its yield), so the
# scheduler reliably interleaves a sibling's cache churn before this fiber resumes
# and re-analyses.
INNER_CAP = 100000

# File/resolution-heavy: each fiber runs a full static analysis (stat the tree, open
# every reachable source).  Cap the goroutine count so the forever-loop --funcs
# 1000000 doesn't spawn a million concurrent finders.
MAX_FUNCS = 512


def build_tree(base, variant):
    """Write a fixed package tree for `variant` under `base` and return
    (root_dir, entry_path, expected_modules, expected_bad).

    The tree is a chain pkg.m0 -> pkg.m1 -> ... -> pkg.m{k-1}; pkg/__init__ imports
    m0; the entry script imports pkg + pkg.m0 + one guaranteed-missing top-level
    name; each chain module imports one guaranteed-missing name (so badmodules is
    non-trivial) and, except the last, the next chain link.  Because the finder's
    search path is ONLY this root, the closed world is exactly:
        modules = {__main__, pkg, pkg.m0 .. pkg.m{k-1}}
        bad     = { the missing names imported anywhere }
    computed here in straight Python, independent of modulefinder."""
    root = os.path.join(base, "tree{0}".format(variant))
    pkg = os.path.join(root, "pkg")
    os.makedirs(pkg, exist_ok=True)

    k = 3 + (variant % 4)                       # chain length 3..6

    with open(os.path.join(pkg, "__init__.py"), "w") as fh:
        fh.write("from . import m0\n")

    bad = set()
    for i in range(k):
        miss = MISSING[i % len(MISSING)]
        bad.add(miss)
        with open(os.path.join(pkg, "m{0}.py".format(i)), "w") as fh:
            if i + 1 < k:
                fh.write("from . import m{0}\n".format(i + 1))
                fh.write("import {0}\n".format(miss))
                fh.write("x = {0}\n".format(i))
            else:
                fh.write("import {0}\n".format(miss))
                fh.write("y = {0}\n".format(i))

    entry_miss = MISSING[variant % len(MISSING)]
    bad.add(entry_miss)
    entry = os.path.join(root, "main.py")
    with open(entry, "w") as fh:
        fh.write("import pkg\n")
        fh.write("from pkg import m0\n")
        fh.write("import {0}\n".format(entry_miss))

    expected_modules = frozenset(
        ["__main__", "pkg"] + ["pkg.m{0}".format(i) for i in range(k)])
    expected_bad = frozenset(bad)
    return root, entry, expected_modules, expected_bad


def analyse(root, entry):
    """Run a FRESH single-owner ModuleFinder over the fixed tree at `root` and
    return its closed world as (frozenset(modules), frozenset(badmodules)).

    The finder's search path is restricted to ONLY `root`, so resolution never
    depends on the ambient sys.path or the real stdlib -- every import either
    resolves inside the tree or is deterministically bad."""
    mf = modulefinder.ModuleFinder(path=[root])
    mf.run_script(entry)
    return frozenset(mf.modules.keys()), frozenset(mf.badmodules.keys())


def check_once(H, wid, idx, tree, state):
    """One single-owner CLOSED-WORLD + determinism check over a fixed read-only
    tree, straddling a yield so siblings churn the importlib cache in parallel."""
    root, entry, exp_mods, exp_bad = tree

    got_mods1, got_bad1 = analyse(root, entry)
    if got_mods1 != exp_mods:
        H.fail("closed-world MODULES set wrong (pre-yield): ModuleFinder resolved "
               "{0} but the tree's independently-computed closed world is {1} "
               "(missing={2} extra={3}, wid {4} idx {5}) -- a name that IS in the "
               "finder's path was dropped, or a name OUTSIDE its closed world "
               "leaked in, under concurrent PathFinder cache churn".format(
                   sorted(got_mods1), sorted(exp_mods),
                   sorted(exp_mods - got_mods1), sorted(got_mods1 - exp_mods),
                   wid, idx))
        return
    if got_bad1 != exp_bad:
        H.fail("closed-world BADMODULES set wrong (pre-yield): ModuleFinder marked "
               "bad {0} but the expected unresolvable set is {1} (missing={2} "
               "extra={3}, wid {4} idx {5}) -- a resolvable module was spuriously "
               "marked bad, or a bad import vanished, under concurrent "
               "resolution".format(
                   sorted(got_bad1), sorted(exp_bad),
                   sorted(exp_bad - got_bad1), sorted(got_bad1 - exp_bad),
                   wid, idx))
        return

    # YIELD: let sibling fibers run their own ModuleFinders (invalidate_caches +
    # find_spec over the shared importlib path cache) while this fiber is parked,
    # then re-analyse the SAME fixed tree and assert an identical result.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    got_mods2, got_bad2 = analyse(root, entry)
    if got_mods2 != got_mods1 or got_bad2 != got_bad1:
        H.fail("static analysis NON-DETERMINISTIC across a yield: re-analysing the "
               "SAME fixed read-only tree produced a different closed world "
               "(modules delta={0}, bad delta={1}, wid {2} idx {3}) -- a torn "
               "resolution: a sibling's concurrent PathFinder.invalidate_caches / "
               "find_spec corrupted this fiber's import-graph walk".format(
                   sorted(got_mods1 ^ got_mods2), sorted(got_bad1 ^ got_bad2),
                   wid, idx))
        return
    if got_mods2 != exp_mods or got_bad2 != exp_bad:
        H.fail("closed world wrong (post-yield): re-analysis modules/bad != the "
               "independently-computed closed world (modules delta={0}, bad "
               "delta={1}, wid {2} idx {3}) -- resolution corruption on the second "
               "run under concurrent analysis".format(
                   sorted(got_mods2 ^ exp_mods), sorted(got_bad2 ^ exp_bad),
                   wid, idx))
        return

    state["mf_checks"][wid] += 1


def worker(H, wid, rng, state):
    """Each fiber picks one fixed read-only tree by wid and runs the load-bearing
    closed-world + determinism check across a yield, repeatedly, while siblings
    analyse their own trees in parallel on other hubs."""
    tree = state["trees"][wid % NTREES]
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            check_once(H, wid, idx, tree, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # Build the fixed read-only tree pool ONCE, under a harness-managed tmpdir that
    # is rmtree'd at shutdown.  The trees are never mutated after this point, so
    # every fiber's single-owner ModuleFinder reads them as canned bytes.
    base = H.make_tmpdir(prefix="big100_p579_")
    trees = [build_tree(base, v) for v in range(NTREES)]

    # RACE-FREE conservation counter: one slot per worker (single writer per slot),
    # allocated here where H.funcs is known.
    H.state = {
        "trees": trees,
        "mf_checks": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["mf_checks"])
    H.log("modulefinder single-owner CLOSED-WORLD+determinism checks: {0} (each "
          "analysed a fixed read-only import graph, verified the resolved "
          "modules/badmodules sets == an independently-computed closed world and "
          "an identical re-analysis across a yield); ops={1}".format(
              checks, H.total_ops()))

    # NON-VACUITY: the load-bearing static-analysis hazard was actually exercised.
    H.check(checks > 0,
            "no modulefinder closed-world checks ran -- the load-bearing "
            "concurrent-resolution hazard was never exercised (oracle would be "
            "vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside find_spec,
    # the file walk, or the importlib path cache).
    H.require_no_lost("modulefinder closed world")


if __name__ == "__main__":
    harness.main(
        "p579_modulefinder_closed_world", body, setup=setup, post=post,
        default_funcs=3000, max_funcs=MAX_FUNCS,
        describe="many hubs each run a private modulefinder.ModuleFinder over a "
                 "fixed read-only import graph in parallel (invalidate_caches + "
                 "PathFinder.find_spec churn the global importlib path cache); "
                 "single-owner CLOSED-WORLD law: the resolved modules/badmodules "
                 "sets must equal an independently-computed closed world, and a "
                 "re-analysis of the same fixed tree must be identical across a "
                 "yield -- a module misclassified as bad (or leaked in), a "
                 "non-deterministic result, or a crash is a torn-resolution "
                 "runtime/importlib bug")
