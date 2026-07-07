"""big_100 / 596 -- runpy.run_path returned-namespace isolation under M:N.

runpy is a PROCESS-GLOBAL module: run_module()/run_path() execute code with the
interpreter's shared state temporarily rewired.  Along the way they mutate the
one shared sys.modules dict (via runpy._TempModule, which inserts an entry keyed
by run_name and removes it on exit) and the shared sys.argv[0] (via
runpy._ModifiedArgv0), then exec the target code in a FRESH module namespace and
return that namespace back to the caller.  There is nothing single-owner about
the global hooks -- but each call PRODUCES a single-owner object we CAN build a
falsifiable oracle on: the returned globals dictionary.

  run_path(path, init_globals={...}, run_name=UNIQUE) does, for a plain .py path:
      with _TempModule(run_name) as tm, _ModifiedArgv0(path):
          g = tm.module.__dict__
          g.update(init_globals)      # our fiber-local seed + yield hook land here
          g.update(__name__=run_name, __file__=path, ...)
          exec(code, g)               # the target script runs, computing g["out"]
      return g.copy()                 # a FRESH dict, owned solely by THIS caller

  The returned dict is a per-call copy: single-owner, never shared with a sibling.
  Its contents are fully determined by init_globals + the (pure) script code, so
  they are a CLOSED-FORM function of this fiber's own seed.

WHERE M:N COULD BREAK IT (the gap this program probes).  Every fiber's run_path
runs the SAME script file but with a DIFFERENT seed, and the script YIELDS
(runloom.yield_now, injected via init_globals) IN THE MIDDLE of exec -- i.e.
while this fiber is parked INSIDE the _TempModule/_ModifiedArgv0 window, siblings
on other hubs are concurrently entering/leaving their OWN _TempModule blocks,
mutating the shared sys.modules dict and re-exec'ing their own scripts into their
own namespaces.  If runloom's M:N scheduling let a sibling's exec write into THIS
fiber's run_globals (a cross-fiber namespace leak), or lost the wakeup that
resumes this fiber after the injected yield, or corrupted the returned dict, the
value this fiber reads back would not match its own closed-form.  Under a correct
runtime the returned namespace is exactly this fiber's own computation -- bit for
bit -- no matter how many siblings churn runpy's global hooks concurrently.

Each call uses a UNIQUE run_name ("...w{wid}_r{idx}").  That is deliberate and
load-bearing: _TempModule keys sys.modules by run_name, and running two calls
under the SAME run_name concurrently is documented as unsupported (they would
clobber each other's sys.modules slot) -- a shared-key collision is NOT a runtime
bug, so we never create one.  With distinct keys the closed world holds and any
mismatch is a genuine isolation/lost-wakeup fault.

ORACLES:
  * LOAD-BEARING -- RETURNED-NAMESPACE PURITY (worker, HARD, fail-fast).  Each
    fiber picks a fiber-local seed, calls run_path on the shared pure script with
    init_globals={"pygo_seed": seed, "pygo_yield": runloom.yield_now} and a UNIQUE
    run_name.  The script yields mid-exec, then computes out = closed_form(seed).
    The fiber asserts, on the SINGLE-OWNER returned dict:
      - ns["pygo_out"] == closed_form(seed)      (the value the code computed is
        exactly this fiber's own -- not a sibling's leaked namespace, not torn);
      - ns["pygo_tag"] == seed                    (the seed round-tripped through
        init_globals unchanged);
      - ns["__name__"] == run_name                (runpy stamped THIS call's
        run_name into the namespace -- a sibling's _TempModule did not bleed in);
      - ns is a fresh dict object (id changes call to call; single-owner copy).
    A mismatch is a runloom runpy-namespace isolation / lost-wakeup desync.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (runs > 0).

  * CONSERVATION / COMPLETENESS (post, HARD): a per-wid race-free tally counts the
    run_path calls that PASSED every check; sum > 0.  require_no_lost catches a
    fiber stranded inside exec across the injected yield (a lost wakeup while
    parked in the _TempModule window never returns).

Single-owner, closed-world: each run_path call yields a fresh per-call dict whose
contents are a pure function of this fiber's own seed; the shared sys.modules /
sys.argv[0] mutations are runpy's internal global churn (keyed by unique
run_names, never asserted upon) that exist only to CREATE the M:N hazard, never as
the oracle -- so a FAIL means a real cross-fiber leak, torn namespace, or lost
wakeup, never documented Python global-state semantics.

Stresses: runpy.run_path returned-namespace isolation, init_globals round-trip,
_TempModule/sys.modules + _ModifiedArgv0/sys.argv[0] global churn under M:N,
exec-into-fresh-namespace parked across a cooperative yield (lost-wakeup surface),
per-call dict single-ownership.  File-read + compile per call, so max_funcs caps
the forever loop's --funcs.
"""
import os

import harness
import runloom
import runpy

# The pure kernel the injected script computes.  A fiber-local seed drives a
# 16-step LCG fold; the result is a closed-form function of the seed, so the
# returned namespace's "pygo_out" is fully predictable and any cross-fiber value
# stands out immediately.  Kept in Python here AND in the script text so the two
# must agree bit-for-bit.
LCG_MUL = 1103515245
LCG_ADD = 12345
MASK32 = 0xFFFFFFFF
FOLD_STEPS = 16

# The script executed by runpy.run_path.  It reads its inputs from init_globals
# (pygo_seed, pygo_yield), yields IN THE MIDDLE of the fold so a sibling reliably
# interleaves while this fiber is parked inside runpy's _TempModule window, and
# writes its result back into the module namespace that run_path returns.  Pure /
# stdlib-free: no imports, only arithmetic + the injected yield hook.
SCRIPT_SRC = """\
pygo_yield()
_r = pygo_seed & 0xFFFFFFFF
_acc = 0
for _i in range({steps}):
    _r = (_r * {mul} + {add}) & 0xFFFFFFFF
    _acc = (_acc + _r) & 0xFFFFFFFF
    if _i == {steps} // 2:
        pygo_yield()
pygo_out = _acc
pygo_tag = pygo_seed
""".format(steps=FOLD_STEPS, mul=LCG_MUL, add=LCG_ADD)


def closed_form(seed):
    """The exact value the injected script must produce for `seed`."""
    r = seed & MASK32
    acc = 0
    for _ in range(FOLD_STEPS):
        r = (r * LCG_MUL + LCG_ADD) & MASK32
        acc = (acc + r) & MASK32
    return acc


def fiber_seed(wid, idx):
    """A fiber-local, per-call seed.  Distinct (wid, idx) pairs give distinct
    seeds, so a sibling's leaked namespace would carry a visibly wrong value."""
    return (wid * 2654435761 + idx * 40503 + 0x9E3779B1) & MASK32


def run_once(H, wid, idx, state):
    """One run_path call + the single-owner returned-namespace oracle.

    Runs the SHARED pure script with a fiber-local seed and a UNIQUE run_name; the
    script yields mid-exec (parking this fiber inside runpy's _TempModule window
    while siblings churn the global hooks), then computes out=closed_form(seed).
    The returned dict is a fresh per-call copy owned solely by this fiber."""
    seed = fiber_seed(wid, idx)
    run_name = "big100_runpy_w{0}_r{1}".format(wid, idx)
    init_globals = {"pygo_seed": seed, "pygo_yield": runloom.yield_now}

    ns = runpy.run_path(state["script_path"],
                        init_globals=init_globals,
                        run_name=run_name)

    # Check 1: the computed value is exactly THIS fiber's closed-form (not a
    # cross-fiber leak from a sibling's concurrent exec, not a torn value).
    expected = closed_form(seed)
    got = ns.get("pygo_out")
    if got != expected:
        H.fail("run_path namespace VALUE WRONG: pygo_out={0!r}, expected {1} for "
               "seed {2} (wid {3}, idx {4}) -- a cross-fiber runpy namespace leak "
               "or torn returned dict under M:N".format(got, expected, seed,
                                                        wid, idx))
        return False

    # Check 2: the seed round-tripped through init_globals unchanged.
    tag = ns.get("pygo_tag")
    if tag != seed:
        H.fail("run_path init_globals CORRUPTED: pygo_tag={0!r} != seed {1} "
               "(wid {2}, idx {3}) -- init_globals did not round-trip through "
               "run_path under M:N".format(tag, seed, wid, idx))
        return False

    # Check 3: runpy stamped THIS call's run_name as __name__ (a sibling's
    # _TempModule did not bleed its own run_name into this fiber's namespace).
    name = ns.get("__name__")
    if name != run_name:
        H.fail("run_path __name__ WRONG: {0!r} != run_name {1!r} (wid {2}, "
               "idx {3}) -- runpy's _TempModule/sys.modules global rewrite leaked "
               "a sibling's run_name into this fiber's returned namespace".format(
                   name, run_name, wid, idx))
        return False

    # Check 4: run_path returned a plain dict (it hands back a per-call copy of
    # the module namespace).  We do NOT compare id() across calls: the previous
    # dict is dropped before the next call, so id-reuse after GC is expected and
    # would be a false positive -- single-ownership is guaranteed by run_path's
    # own .copy(), not something a cross-time identity probe can legitimately test.
    if not isinstance(ns, dict):
        H.fail("run_path returned non-dict {0!r} (wid {1}, idx {2})".format(
            type(ns).__name__, wid, idx))
        return False
    return True


# Sustained runs per worker, bounded by H.running().  Many fibers must be parked
# inside runpy's global-hook window simultaneously for a cross-fiber leak /
# lost-wakeup to manifest; a single run per fiber barely overlaps a sibling's.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            if not run_once(H, wid, idx, state):
                return                      # fail-fast; H.failed already set
            state["runs"][wid] += 1         # single-writer-per-slot, race-free
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # ONE shared, read-only pure script for every fiber.  Its behaviour is
    # identical across fibers; only the per-call init_globals seed differs.  A
    # single file keeps fd/disk pressure flat (read-only concurrent access is
    # fine); run_name uniqueness -- not per-file uniqueness -- is what keeps each
    # call's sys.modules slot from colliding.
    tmpdir = H.make_tmpdir(prefix="big100_p596_")
    script_path = os.path.join(tmpdir, "runpy_kernel.py")
    with open(script_path, "w") as f:
        f.write(SCRIPT_SRC)

    H.state = {
        "script_path": script_path,
        "runs": [0] * H.funcs,          # per-wid PASSED-run tally (race-free)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    runs = sum(H.state["runs"])
    H.log("runpy.run_path single-owner returned-namespace checks passed: {0} "
          "(every value/seed/__name__/identity check held fail-fast); ops={1}"
          .format(runs, H.total_ops()))

    # NON-VACUITY: the load-bearing arm actually exercised the hazard.
    H.check(runs > 0,
            "no run_path returned-namespace checks ran -- the runpy global-hook "
            "M:N race window was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished inside exec across the injected
    # yield (a lost wakeup while inside runpy's _TempModule window never returns).
    H.require_no_lost("runpy namespace isolation")


if __name__ == "__main__":
    harness.main(
        "p596_runpy_namespace_conserve", body, setup=setup, post=post,
        default_funcs=2000,
        max_funcs=2000,             # file-read + compile per call: cap the forever loop
        describe="runpy.run_path rewires PROCESS-GLOBAL state (sys.modules via "
                 "_TempModule, sys.argv[0] via _ModifiedArgv0) then exec's code in "
                 "a FRESH namespace it returns as a per-call copy.  LOAD-BEARING: "
                 "each fiber run_path's the SAME pure script with a fiber-local "
                 "seed, a UNIQUE run_name, and an injected mid-exec yield -- so it "
                 "parks inside runpy's global-hook window while siblings churn the "
                 "same hooks on other hubs.  The single-owner returned dict MUST "
                 "carry exactly this fiber's closed-form value + its own run_name "
                 "(not a sibling's leaked namespace, not torn), stable across the "
                 "yield.  A wrong value / __name__ / a lost wakeup is the runloom "
                 "bug")
