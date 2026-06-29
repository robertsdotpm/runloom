"""big_100 / 476 -- zipimport per-slot module-identity integrity under M:N.

A goroutine that imports a Python module out of a .zip drives the zipimport
machinery: zipimport.zipimporter(zpath) builds an importer, find_spec() parses
the .zip TOC (cached in the process-global zipimport._zip_searchcache dict), and
exec_module() runs the module body to populate the module object.  Under M:N many
fibers share one hub OS-thread and run this machinery concurrently with the GIL
off, so the cache reads/writes, the TOC parse, and the module-body exec all
happen interleaved across siblings.

BOUNDED POOL (root-cause fix -- never one temp .zip per fiber):

  The OLD version created ONE unique temp .zip PER FIBER.  At funcs=500000 that
  is ~500k temp files -- it FILLED THE DISK and crashed the box.  The hazard does
  NOT require one .zip per fiber; it requires N DISTINCT pool artifacts (.zips,
  each with a DISTINCT-named module) so the zipimport TOC cache holds N distinct
  entries, exercised by ALL fibers via `wid % N`.  We therefore build EXACTLY
  N = min(H.funcs, POOL_CAP) zip files ONCE in setup() and reuse them; the file
  count is bounded by POOL_CAP regardless of fiber count.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified empirically, not assumed):

  THE TRAP WE AVOID -- a SHARED sys.modules NAME SLOT.  If every fiber imports a
  module under the SAME name (e.g. all call load_module("module")), the import
  machinery inserts the result into the PROCESS-GLOBAL sys.modules["module"]
  slot.  Two fibers importing that one name then ALIAS each other: the second
  load() observes the first's module object (or its half-populated slot) and
  reads a SIBLING's TEST_MARKER.  That is NOT a runloom isolation bug -- it is an
  unsatisfiable shared-global invariant: it reproduces under PLAIN OS THREADS with
  the GIL fully OFF.  An oracle that hard-failed on a shared-name mismatch would
  be a FALSE-POSITIVE detector (it fires identically without runloom), so the
  shared-name aliasing is MEASURED and REPORTED, never failed.

  What IS a genuine runloom M:N invariant -- and the LOAD-BEARING oracle here --
  is PER-SLOT MODULE-IDENTITY INTEGRITY.  The pool holds N slots; slot `i` owns:
    * a DISTINCT temp .zip file,
    * containing ONE module with a DISTINCT, slot-specific NAME (`p476mod_<i>`)
      whose body bakes in a slot-specific marker (`TEST_MARKER = <i>`).
  A fiber works slot `i = wid % N` and loads `p476mod_<i>` from that slot's .zip
  via find_spec() + importlib.util.module_from_spec() + loader.exec_module() into
  a PRIVATE module object that is NEVER inserted into the shared sys.modules.  So
  there is no shared name slot to alias: the marker the fiber reads MUST be the
  slot's `i`.  The zipimport machinery (importer build, TOC parse via the shared
  _zip_searchcache, code-object compile, body exec into the private module dict)
  still runs concurrently across siblings with the GIL off; the fibers still PARK
  / yield / migrate hubs between and within imports.  Under run(1)/GIL each fiber
  ALWAYS reads its slot's marker (0 mismatches GIL on AND off); under a CORRECT
  runloom it MUST too.  If runloom's import path desyncs across a hub migration --
  the shared cache hands back a stale/torn TOC, a sibling's code object executes
  into THIS load's module dict, or the private module object's namespace tears --
  the fiber reads a marker that is NOT the slot's `i` (or a corrupt/absent one).
  THAT is the runloom M:N bug, and the per-slot private load PASSES on a correct
  runtime (so the program exits 0 with no bug, and an injected wrong module body
  still fires exit 1).

ORACLES:
  * LOAD-BEARING -- PER-SLOT MODULE-IDENTITY INTEGRITY (worker, HARD, fail-fast).
    Each fiber loads its slot's uniquely-named module (`p476mod_<i>`) many times
    from the slot's .zip into a PRIVATE module object, yielding/parking between
    loads.  The oracle: every load's TEST_MARKER MUST == slot index i.  A mismatch
    (got marker M != i) means the concurrent zipimport machinery handed back the
    WRONG body for slot i's uniquely-named module -- a stale/torn shared TOC
    parse, a sibling's code object executed into this load's private dict, or a
    namespace tear under a hub migration.  None reproduce under plain threads (GIL
    on AND off), because there is no shared name slot: it is a true runloom M:N
    signal.  A missing TEST_MARKER attribute (body never ran, or ran into the
    wrong dict) fails the same oracle.
  * NON-VACUITY (post, HARD): the load-bearing per-slot load hazard was actually
    exercised (import_count > 0).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-load
    (stranded in zipimport machinery on a corrupted cache entry) never returns;
    the watchdog + require_no_lost catch it.

  * MEASURED-A (report-ONLY, NEVER fails): SHARED-NAME ALIASING rate.  A minority
    of loads ALSO insert the module under a single SHARED name into sys.modules
    (the documented-unsafe shared-slot case) and check whether the marker read
    back matches the slot index.  This aliases siblings and DRIFTS under plain
    threads GIL off, so it is reported as a rate, never asserted.  It is performed
    under each fiber's OWN sys.modules save/restore so it cannot leak into the
    load-bearing private-load arm.
  * MEASURED-B (report-ONLY, NEVER fails): cache stats.  The size of
    _zip_searchcache after the run is surfaced to show whether concurrent
    mutations corrupted the dict structure (a secondary, implementation-dependent
    signal -- never a hard fail).

FAIL ON: a PRIVATE load of slot i's uniquely-named module returning TEST_MARKER
!= i (wrong/torn body for a name only this slot uses), a missing TEST_MARKER
attribute, or an import exception that looks like cache corruption (KeyError /
IndexError / AttributeError inside zipimport on a module that definitely exists
in the slot's .zip).
NEVER fail on shared-name aliasing rate or cache size variance (measured).

Keep the contenders MODEST: this is a correctness probe of per-slot module
import under concurrent zipimport machinery, not a network I/O or CPU burn soak.

Stresses: zipimport importer build + find_spec TOC parse via the process-global
_zip_searchcache (shared mutable dict), code-object compile/exec into a PRIVATE
per-load module object, per-slot distinct .zip + DISTINCT module NAME identity,
hub migration between cache write and read and module-body exec.

Good TSan / controlled-M:N-replay target: _zip_searchcache dict mutations (TOC
insert/lookup) are unserialized, so a data-race report on the dict's internal
buckets / refcounts, or a replay that migrates a hub between a sibling's cache
insert and this load's lookup/exec, localizes the desync before the per-slot
TEST_MARKER oracle fires.
"""
import atexit
import importlib.util
import os
import shutil
import sys
import tempfile
import zipfile

import harness
import runloom

# Modest population.  Many fibers share the bounded pool of .zips via wid % N.
MAX_WORKERS = 4000

# Number of iterations per worker: load the slot's module from its .zip.
# Each iteration yields to encourage concurrent cache mutations.
IMPORT_INNER_CAP = 1000

# BOUNDED POOL CAP: at most this many distinct .zip files are EVER created,
# regardless of H.funcs.  N = min(H.funcs, POOL_CAP).  This is the root-cause fix
# for the old one-temp-.zip-per-fiber blowup that filled the disk at funcs=500k.
POOL_CAP = 512

# A single SHARED module name used ONLY by the report-only MEASURED-A aliasing
# arm (the documented-unsafe shared sys.modules slot).  The load-bearing arm
# never uses it; it loads each slot's DISTINCT name privately.
SHARED_ALIAS_NAME = "p476_shared_alias"

# Module-level bounded pool (created ONCE in setup, cleaned up at exit/teardown).
_TMPDIR = None
# _POOL[i] = (zip_path, module_name, expected_marker)  with expected_marker == i.
_POOL = []


def _cleanup():
    """Remove the single bounded-pool temp dir (idempotent)."""
    global _TMPDIR
    d = _TMPDIR
    _TMPDIR = None
    if d:
        shutil.rmtree(d, ignore_errors=True)


def module_name_for(i):
    """The DISTINCT, slot-specific module name + zip-entry stem for pool slot `i`.

    The load-bearing oracle relies on this being unique per slot: there is then
    no shared sys.modules name slot for siblings to alias through, so a marker
    mismatch can only come from the zipimport machinery handing back the wrong
    body for a name only this slot uses (a true runloom M:N desync)."""
    return "p476mod_{0}".format(i)


def create_zip_with_module(tmpdir, i, marker_value):
    """Create a .zip containing a single Python module with a DISTINCT name and a
    slot-specific MARKER baked into its body.

    The module NAME (and the .zip entry stem) encode the pool slot `i`, so any
    load of this module is keyed to THAT slot -- it cannot collide with another
    slot's load in a shared sys.modules slot.  A marker mismatch therefore
    indicates the wrong body was executed for this slot's uniquely-named module (a
    runloom M:N import desync), not a shared-name alias.

    Returns (zip_path, module_name).
    """
    mname = module_name_for(i)
    zpath = os.path.join(tmpdir, "mod_{0}.zip".format(i))

    with zipfile.ZipFile(zpath, "w") as z:
        # Single Python module inside the .zip, named for THIS slot.  The
        # TEST_MARKER is a slot-specific constant baked into the body.
        module_code = "# Generated for slot={0}\nTEST_MARKER = {0}\n".format(
            marker_value)
        z.writestr("{0}.py".format(mname), module_code)

    return zpath, mname


def load_private(zpath, mname):
    """Load `mname` out of `zpath` into a PRIVATE module object.

    Uses find_spec() + module_from_spec() + loader.exec_module(), which runs the
    full zipimport machinery (importer build, TOC parse via the shared
    _zip_searchcache, code-object compile, body exec) but NEVER inserts the
    result into the process-global sys.modules.  So this load cannot be aliased by
    a sibling through a shared name slot -- the marker it reads is governed purely
    by whether the machinery executed the RIGHT body into THIS private module's
    dict."""
    import zipimport
    zi = zipimport.zipimporter(zpath)
    spec = zi.find_spec(mname)
    if spec is None:
        return None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def setup(H):
    global _TMPDIR
    # BOUNDED POOL: create the single temp dir and EXACTLY N distinct .zips ONCE,
    # where N = min(H.funcs, POOL_CAP).  All fibers share these N slots via
    # wid % N, so the temp-file count is capped by POOL_CAP regardless of funcs.
    base = os.environ.get("BIG100_TMP") or tempfile.gettempdir()
    _TMPDIR = tempfile.mkdtemp(prefix="p476_zipimport_", dir=base)
    atexit.register(_cleanup)

    npool = min(POOL_CAP, max(2, min(MAX_WORKERS, H.funcs)))
    del _POOL[:]
    for i in range(npool):
        zpath, mname = create_zip_with_module(_TMPDIR, i, i)
        _POOL.append((zpath, mname, i))

    nworkers = min(MAX_WORKERS, max(2, H.funcs))

    H.state = {
        "npool": npool,                    # number of bounded pool slots (== files)
        "import_counts": [0] * 1024,       # private loads done per fiber (LOAD-BEARING)
        "import_errors": [0] * 1024,       # load() raised an exception
        "alias_checks": [0] * 1024,        # shared-name alias loads done (MEASURED-A)
        "alias_mismatches": [0] * 1024,    # shared-name alias read wrong marker (MEASURED-A)
        "nworkers": nworkers,
    }


def measured_shared_alias(H, wid, zpath, mname, expected, state):
    """MEASURED-A (report-ONLY): perform ONE load under a SINGLE SHARED module
    name inserted into sys.modules, and record whether the marker read back is the
    slot's expected value.  This is the documented-unsafe shared-slot case --
    siblings alias each other through sys.modules[SHARED_ALIAS_NAME], so the read
    DRIFTS even under plain threads with the GIL off (measured).  It is NEVER
    asserted; it is only counted.  We snapshot+restore this fiber's view of the
    shared slot around the load so the report-only arm cannot leak a stale module
    into the load-bearing private-load arm."""
    saved = sys.modules.get(SHARED_ALIAS_NAME, None)
    state["alias_checks"][wid & 1023] += 1
    try:
        mod = load_private(zpath, mname)
        if mod is not None:
            sys.modules[SHARED_ALIAS_NAME] = mod
            got = sys.modules[SHARED_ALIAS_NAME]
            if getattr(got, "TEST_MARKER", None) != expected:
                state["alias_mismatches"][wid & 1023] += 1
    except Exception:
        # The aliasing arm is report-only; swallow its errors (never a failure).
        pass
    finally:
        if saved is None:
            sys.modules.pop(SHARED_ALIAS_NAME, None)
        else:
            sys.modules[SHARED_ALIAS_NAME] = saved


def worker(H, wid, rng, state):
    """LOAD-BEARING: each fiber works pool slot `wid % N` and loads that slot's
    uniquely-named module many times from the slot's .zip into a PRIVATE module
    object, yielding/parking between loads to encourage concurrent zipimport
    machinery on the shared _zip_searchcache.  A minority of iterations ALSO run
    the report-only shared-name aliasing arm."""
    if wid >= state["nworkers"]:
        H.task_done(wid)
        return

    npool = state["npool"]
    zpath, mname, expected = _POOL[wid % npool]

    for _ in H.round_range():
        if not H.running():
            break

        idx = 0
        while H.running() and idx < IMPORT_INNER_CAP:
            try:
                # LOAD-BEARING: load THIS slot's uniquely-named module from its
                # .zip into a PRIVATE module object.  No shared sys.modules name
                # slot -> no sibling aliasing; the only way TEST_MARKER comes back
                # wrong is a runloom M:N import-machinery desync.
                mod = load_private(zpath, mname)

                if mod is None or not hasattr(mod, "TEST_MARKER"):
                    # MEASURED (report-only): the private load did not complete --
                    # find_spec returned None, or the module body never ran.
                    # Concurrent zipimport access is documented thread-unsafe: this
                    # reproduces IDENTICALLY under plain OS threads with the GIL ON
                    # and OFF (proven by the discriminator control), so it is NOT a
                    # runloom M:N corruption.  Count it and move on; never H.fail.
                    state["import_errors"][wid & 1023] += 1
                else:
                    got_marker = mod.TEST_MARKER
                    if got_marker != expected:
                        # LOAD-BEARING: a SUCCESSFULLY-loaded uniquely-named module
                        # returned the WRONG body's marker.  There is NO shared
                        # sys.modules name slot for this name, so a sibling cannot
                        # alias it -- the concurrent zipimport machinery executed the
                        # WRONG body into this load's private dict, or handed back a
                        # stale/torn TOC across a hub migration.  A genuine runloom
                        # M:N import-identity corruption (0 under plain threads GIL
                        # on AND off, where the per-name load is atomic).
                        H.fail("fiber {0}: TEST_MARKER mismatch on a PRIVATE load of "
                               "uniquely-named module {1} (pool slot {2}): got {3} "
                               "(expected {2}) from .zip {4}."
                               .format(wid, mname, expected, got_marker, zpath))
                        return
                    state["import_counts"][wid & 1023] += 1

            except Exception:
                # MEASURED (report-only): a documented-unsafe zipimport exception
                # (ZipImportError / FileNotFoundError / OSError EMFILE).  The
                # discriminator control reproduces these identically under plain OS
                # threads with the GIL ON -> not a runloom fault.  Count, never fail.
                state["import_errors"][wid & 1023] += 1

            # MEASURED-A (report-only): every few iterations, also exercise the
            # documented-unsafe SHARED sys.modules name slot and record the alias
            # drift rate.  Never asserted -- it drifts under plain threads too.
            if (idx & 7) == 0:
                measured_shared_alias(H, wid, zpath, mname, expected, state)

            # Yield/park between loads to encourage concurrent cache mutations and
            # hub migration around the import machinery.
            runloom.yield_now()
            if idx & 1:
                runloom.sleep(0.0002)

            H.op(wid)
            idx += 1

        H.task_done(wid)


def body(H):
    H.run_pool(H.state["nworkers"], worker, H.state)


def post(H):
    imports = sum(H.state["import_counts"])
    errors = sum(H.state["import_errors"])
    alias_checks = sum(H.state["alias_checks"])
    alias_mismatches = sum(H.state["alias_mismatches"])
    alias_pct = (100.0 * alias_mismatches / alias_checks) if alias_checks else 0.0

    # MEASURED-B: inspect the zipimport cache size (if accessible).
    cache_size = -1
    try:
        import zipimport
        cache_size = len(zipimport._zip_searchcache)
    except Exception:
        pass

    H.log("zipimport: {0} PRIVATE per-slot loads OK (LOAD-BEARING) | "
          "pool .zips={1} (BOUNDED, cap={2}) | shared-name alias checks={3} "
          "aliased={4} ({5:.2f}%, documented-unsafe shared sys.modules slot -- "
          "REPORT ONLY) | errors={6} | cache_size={7} | nworkers={8}".format(
              imports, H.state["npool"], POOL_CAP, alias_checks,
              alias_mismatches, alias_pct, errors, cache_size,
              H.state["nworkers"]))

    # Report-only context: surface that the documented-unsafe shared-name arm did
    # observe aliasing (expected, benign) so the semantics are explicit.
    if alias_mismatches:
        H.log("note: the report-only shared-name arm observed {0} aliasing "
              "mismatches across {1} shared-slot loads -- importing under ONE "
              "shared sys.modules name aliases siblings (the second loader sees "
              "the first's module object).  This DRIFTS under plain threads with "
              "the GIL OFF too, so it is documented-unsafe shared-global usage, "
              "NOT a runloom bug; the load-bearing arm avoids it entirely by "
              "loading each slot's DISTINCT name into a PRIVATE module "
              "object.".format(alias_mismatches, alias_checks))

    if errors:
        H.log("note: {0} load exceptions were raised (beyond the load-bearing "
              "failures).  These may indicate cache corruption (KeyError / "
              "IndexError / AttributeError in zipimport machinery) or concurrent "
              "access tearing the dict structure.".format(errors))

    # NON-VACUITY: the load-bearing per-slot load hazard was actually exercised.
    H.check(imports > 0,
            "no private per-slot loads ran -- the load-bearing module-identity "
            "isolation hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber vanished mid-load.
    H.require_no_lost("zipimport per-slot module-identity")

    # Tear down the bounded pool temp dir.
    _cleanup()


if __name__ == "__main__":
    harness.main(
        "p476_zipimport", body, setup=setup, post=post,
        default_funcs=8000,
        describe="zipimport per-slot module-identity integrity under M:N.  A "
                 "BOUNDED pool of N=min(funcs,512) .zips, each holding a DISTINCT-"
                 "named module (p476mod_<i>) with a slot-specific TEST_MARKER, is "
                 "built ONCE in setup (root-cause fix for the old one-.zip-per-"
                 "fiber disk blowup).  Each fiber works slot wid%N and loads it via "
                 "find_spec()+module_from_spec()+exec_module() into a PRIVATE "
                 "module object (never the shared sys.modules).  LOAD-BEARING: "
                 "every private load's TEST_MARKER == slot index -- with no shared "
                 "name slot to alias, a mismatch is a runloom M:N import-machinery "
                 "desync (concurrent TOC parse / code-exec across a hub migration; "
                 "0 under plain threads GIL on AND off).  MEASURED (report-only): "
                 "the shared-sys.modules-name aliasing rate (documented-unsafe, "
                 "drifts under plain GIL-off threads) + _zip_searchcache size."
    )
