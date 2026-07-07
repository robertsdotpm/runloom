"""big_100 / 584 -- pkgutil.iter_modules() closed-world module-discovery law under M:N.

pkgutil.iter_modules(path) is the heart of pkgutil: it walks the importers for a
path list and yields a ModuleInfo(module_finder, name, ispkg) namedtuple for every
importable module/package it finds on disk.  Under the hood it drives the import
machinery's PROCESS-GLOBAL caches -- sys.path_importer_cache (the path->finder
map that pkgutil.get_importer reads/writes) and each importlib FileFinder's own
directory-listing cache -- and then builds a fresh, single-owner list of
ModuleInfo tuples for THIS call.  The global hook is NOT single-owner (it is a
process-wide dict shared by every fiber), so per the big_100 contract we do NOT
put the oracle on the global cache.  Instead we put it on the SINGLE-OWNER object
the module PRODUCES: the set of ModuleInfo tuples returned for a fiber-LOCAL
directory that this fiber, and only this fiber, populated.

WHERE M:N COULD BREAK IT (the gap this program probes).  Every fiber scans its
OWN private temp directory (a unique path -> its own sys.path_importer_cache slot,
its own fresh FileFinder), but ALL fibers hammer the SAME process-global
path_importer_cache dict and the SAME importlib FileFinder machinery concurrently
with the GIL off.  If a concurrent insert/lookup on that shared dict, or a torn
FileFinder directory-cache read, corrupted the ModuleInfo list handed back to a
fiber -- a dropped module, an EXTRA/foreign module leaked from a sibling's scan, a
torn ispkg flag, a None/garbage module_finder, or a name from outside this fiber's
own on-disk universe -- the closed-world discovery law below would catch it.

CLOSED-WORLD DISCOVERY LAW (single-owner, fail-fast).  Each fiber, per round:
  * builds a KNOWN random multiset of on-disk entries in its OWN fresh temp dir:
      - a random subset of MODULE_POOL as bare `<name>.py` files      (ispkg False)
      - a random subset of PKG_POOL   as `<name>/__init__.py` packages (ispkg True)
      - fixed DISTRACTORS that iter_modules MUST ignore: non-module data
        files (.txt/.dat) and a plain sub-directory with NO __init__.py
    and records the EXACT expected frozenset {(name, ispkg), ...} (single-owner,
    fiber-local -- never shared);
  * calls pkgutil.iter_modules([mydir]) and asserts the returned ModuleInfo set
    equals `expected` EXACTLY -- every produced tuple is well-formed
    (module_finder not None, name a str inside the fiber's UNIVERSE, ispkg a
    bool), nothing dropped, nothing extra, no distractor leaked in;
  * YIELDs (yield_now / tiny sleep) so siblings run their own scans and pound the
    shared import caches on other hubs;
  * re-scans the SAME unchanged directory (this second call hits the now-cached
    FileFinder) and asserts the set is STILL exactly `expected` -- stable across
    the yield, not corrupted by a sibling's concurrent scan.

Single-owner: the directory, its contents, the expected set, and the returned
ModuleInfo lists are all fiber-local; the only shared thing touched is the import
machinery's global caches, which is the runtime surface under test.  The unique
per-fiber path means each fiber owns a distinct path_importer_cache KEY (one
writer per key), so the cache write is not a contended-slot RMW -- a corruption
would be a genuine shared-dict / FileFinder concurrency bug, not documented
shared-container semantics.  After each round the fiber pops its own key back out
of sys.path_importer_cache (bounding the global cache to the live fiber set) and
rmtree's its directory.

Why a FAIL here is a REAL bug (not documented Python semantics): iter_modules over
a fiber-private, unchanging directory is a pure function of that directory's
contents; two OS threads each scanning their own private dir get exactly their own
contents (verified with a plain-threads control, GIL on and off: 0 cross-thread
leaks, every scan == its own expected set).  So under a correct runtime the law
holds and the program exits 0.  A dropped/extra/torn ModuleInfo, an
out-of-universe name, or a cross-fiber leak is a runloom / free-threaded
import-machinery concurrency bug.

NON-VACUITY (post): the discovery arm actually ran (scans > 0).
COMPLETENESS (post): require_no_lost -- a fiber stranded inside iter_modules /
FileFinder / a parked cache lookup never returns; the watchdog + require_no_lost
catch it.

Resource-capped (max_funcs): each round mkdtemp's a dir and writes ~10 files, so
this is filesystem-heavy -- max_funcs bounds the forever-loop's --funcs 1000000 to
keep inode/dir churn sane.

Stresses: pkgutil.iter_modules / iter_importers / get_importer,
sys.path_importer_cache concurrent distinct-key insert+lookup+pop, importlib
FileFinder directory-listing cache reads racing across hubs, ModuleInfo namedtuple
construction under M:N, and closed-world module-discovery conservation.
"""
import os
import shutil
import sys
import tempfile

import pkgutil

import harness
import runloom

# Valid module base names (no leading underscore; all valid identifiers) used as
# bare `<name>.py` files -> ModuleInfo(ispkg=False).  Disjoint from PKG_POOL so a
# name is unambiguously either a module or a package.
MODULE_POOL = (
    "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
    "iota", "kappa", "mu", "nu", "omicron", "sigma", "tau", "omega",
)

# Package base names used as `<name>/__init__.py` sub-packages ->
# ModuleInfo(ispkg=True).  Disjoint from MODULE_POOL.
PKG_POOL = (
    "pkgone", "pkgtwo", "pkgthree", "pkgfour",
    "pkgfive", "pkgsix", "pkgseven", "pkgeight",
)

# The finite UNIVERSE of names iter_modules may legitimately return for our dirs.
# A returned name outside this set is a torn/corrupt/leaked entry -> hard fault.
UNIVERSE = frozenset(MODULE_POOL) | frozenset(PKG_POOL)

# Fixed distractors that iter_modules MUST ignore: non-module data files and a
# plain directory with NO __init__.py.  If any of these ever surfaces as a
# ModuleInfo it is either a leaked/torn entry (out-of-universe -> fail) or a real
# discovery bug.
DISTRACTOR_FILES = ("readme.txt", "data.dat", "config.ini")
PLAIN_DIR = "plainfolder"                  # sub-dir without __init__.py; ignored


def build_dir(rng):
    """Create a fresh private temp dir populated with a KNOWN random set of
    modules + packages + ignored distractors.  Returns (mydir, expected) where
    expected is the frozenset of (name, ispkg) iter_modules MUST return."""
    mydir = tempfile.mkdtemp(prefix="big100_p584_")

    nmods = rng.randint(1, len(MODULE_POOL))
    npkgs = rng.randint(0, len(PKG_POOL))
    mods = rng.sample(MODULE_POOL, nmods)
    pkgs = rng.sample(PKG_POOL, npkgs)

    expected = set()
    for name in mods:
        with open(os.path.join(mydir, name + ".py"), "w") as f:
            f.write("x = 1\n")
        expected.add((name, False))
    for name in pkgs:
        pdir = os.path.join(mydir, name)
        os.mkdir(pdir)
        with open(os.path.join(pdir, "__init__.py"), "w") as f:
            f.write("")
        expected.add((name, True))

    # Distractors iter_modules must NOT report.
    for fn in DISTRACTOR_FILES:
        with open(os.path.join(mydir, fn), "w") as f:
            f.write("not a module\n")
    plain = os.path.join(mydir, PLAIN_DIR)
    os.mkdir(plain)
    with open(os.path.join(plain, "stray.py"), "w") as f:
        f.write("x = 1\n")   # a .py INSIDE a non-package dir must stay invisible

    return mydir, frozenset(expected)


def scan_set(H, wid, mydir, expected):
    """Call pkgutil.iter_modules([mydir]) and return the frozenset of
    (name, ispkg), fail-fast on any malformed / out-of-universe / unexpected
    ModuleInfo.  Returns None on failure (caller must check H.failed)."""
    got = set()
    for mi in pkgutil.iter_modules([mydir]):
        # ModuleInfo(module_finder, name, ispkg) must be well-formed.
        if mi.module_finder is None:
            H.fail("iter_modules yielded ModuleInfo with module_finder=None for "
                   "name {0!r} (wid {1}) -- a torn/half-built ModuleInfo under "
                   "concurrent import-machinery access".format(mi.name, wid))
            return None
        if not isinstance(mi.name, str):
            H.fail("iter_modules yielded non-str name {0!r} (wid {1}) -- torn "
                   "ModuleInfo field under M:N".format(mi.name, wid))
            return None
        if not isinstance(mi.ispkg, bool):
            H.fail("iter_modules yielded non-bool ispkg {0!r} for {1!r} (wid "
                   "{2}) -- torn ModuleInfo field under M:N".format(
                       mi.ispkg, mi.name, wid))
            return None
        if mi.name not in UNIVERSE:
            H.fail("iter_modules yielded OUT-OF-UNIVERSE name {0!r} (ispkg={1}) "
                   "for a fiber-private dir (wid {2}) -- a leaked/torn entry, or "
                   "a sibling fiber's module bled through the shared import "
                   "cache".format(mi.name, mi.ispkg, wid))
            return None
        key = (mi.name, mi.ispkg)
        if key in got:
            H.fail("iter_modules yielded DUPLICATE entry {0!r} (wid {1}) -- the "
                   "single-call dedup set lost track under concurrent "
                   "access".format(key, wid))
            return None
        got.add(key)
    return frozenset(got)


def check_scan(H, wid, mydir, expected, phase):
    """Scan mydir and assert the produced ModuleInfo set == expected exactly."""
    got = scan_set(H, wid, mydir, expected)
    if H.failed:
        return False
    if got != expected:
        missing = expected - got
        extra = got - expected
        H.fail("pkgutil.iter_modules discovery MISMATCH ({0} scan, wid {1}): "
               "missing={2} extra={3} -- a module was DROPPED, an EXTRA/foreign "
               "entry leaked in, or an ispkg flag tore under GIL-off concurrent "
               "import-machinery access on a fiber-private directory".format(
                   phase, wid, sorted(missing), sorted(extra)))
        return False
    return True


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        mydir, expected = build_dir(rng)
        try:
            # First scan: builds/populates this path's FileFinder in the global
            # cache, produces the single-owner ModuleInfo set.
            if not check_scan(H, wid, mydir, expected, "first"):
                return

            # YIELD at the hazard boundary so siblings run their own scans and
            # pound the shared path_importer_cache / FileFinder machinery on
            # other hubs before this fiber re-reads its own directory.
            runloom.yield_now()
            if wid & 1:
                runloom.sleep(0.0003)

            # Second scan of the UNCHANGED dir: hits the now-cached FileFinder.
            # Must still be exactly `expected` (stable, uncorrupted by siblings).
            if not check_scan(H, wid, mydir, expected, "second"):
                return

            state["scans"][wid] += 2       # one slot per worker -> race-free
        finally:
            # Pop our own unique key so the process-global cache stays bounded to
            # the live fiber set, then remove the directory.  Distinct-key pop is
            # safe under 3.14t's dict critical-section audit; no other fiber ever
            # touches this key.
            try:
                sys.path_importer_cache.pop(mydir, None)
            except Exception:
                pass
            shutil.rmtree(mydir, ignore_errors=True)
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # One race-free slot per worker for the non-vacuity tally (single writer).
    H.state = {"scans": [0] * H.funcs}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    scans = sum(H.state["scans"])
    H.log("pkgutil.iter_modules closed-world scans (all == expected, fail-fast): "
          "{0}; ops={1}".format(scans, H.total_ops()))
    # NON-VACUITY: the single-owner discovery law was actually exercised.
    H.check(scans > 0,
            "no pkgutil.iter_modules discovery scans ran -- the closed-world "
            "module-discovery oracle was vacuous")
    # COMPLETENESS: no fiber parked-then-vanished inside iter_modules / the
    # import machinery.
    H.require_no_lost("pkgutil iter_modules discovery")


if __name__ == "__main__":
    harness.main(
        "p584_pkgutil_iter_modules", body, setup=setup, post=post,
        default_funcs=2000,
        max_funcs=1000,
        describe="pkgutil.iter_modules() closed-world module-discovery law under "
                 "M:N: each fiber populates its OWN private temp dir with a known "
                 "random set of `<name>.py` modules + `<name>/__init__.py` "
                 "packages plus ignored distractors, then asserts the returned "
                 "ModuleInfo set == expected EXACTLY, twice across a yield.  All "
                 "fibers pound the process-global sys.path_importer_cache / "
                 "importlib FileFinder machinery concurrently (GIL off); a "
                 "dropped/extra/torn ModuleInfo, an out-of-universe name, a "
                 "leaked sibling module, or a torn ispkg/module_finder fails.  The "
                 "single-owner object is the ModuleInfo set pkgutil PRODUCES for a "
                 "fiber-private directory -- not the shared global cache (its "
                 "unique per-fiber key is a single-writer slot)")
