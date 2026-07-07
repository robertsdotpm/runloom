"""big_100 / 524 -- annotationlib PEP 649 deferred-annotation isolation under M:N.

Python 3.14's PEP 649 stopped evaluating annotations eagerly.  Instead the compiler
emits a hidden ``__annotate__`` function per annotated object; the annotation
expressions are NOT run at ``def``/``class`` time.  They run LATER, lazily, the
first time someone asks for them -- ``annotationlib.get_annotations(obj,
format=Format.VALUE)`` (or touching ``obj.__annotations__``) CALLS that
``__annotate__`` function, and its body executes inside the *requesting* fiber's
frame, closing over whatever the ``def``/``class`` captured.  The computed dict is
then cached on the object (``func.__annotations__`` / the class annotations cache).

WHERE M:N BREAKS IT (the gap this program probes).  ``__annotate__`` is a real
Python function with a real closure.  When a fiber calls ``get_annotations`` on a
function it built via a FACTORY, the deferred body dereferences the factory's cell
(the wid-seeded value) and populates the annotation dict.  If that deferred
evaluation is preempted / cooperatively migrated to a DIFFERENT hub mid-sequence,
the hazards are:

  * the closed-over per-fiber cell could be read against a sibling's frame, so the
    annotation dict comes back holding ANOTHER fiber's wid-value;
  * the ``__annotations__`` cache backing could be written by two fibers racing the
    first ``get_annotations`` and end up torn or cross-linked to a sibling's object;
  * a second ``get_annotations`` after a yield could return a DIFFERENT dict/value
    than the first (the cache did not stick, or was overwritten by a sibling).

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  A FACTORY (``make_fiber_fn``) builds, per call, a brand-new FUNCTION object whose
  ``__annotate__`` closes over a DISTINCT cell holding this fiber's unique sentinel
  value (``wid*SCALE + i``).  The object lives only in a fiber-local variable --
  never shared.  The fiber then:
    - calls ``get_annotations(obj, format=Format.VALUE)`` -> the FIRST deferred
      eval, which must return exactly ``{name: this-fiber-value}``;
    - ``runloom.yield_now()`` / ``sleep`` at the hazard boundary so a sibling
      reliably interleaves (and may drive its OWN deferred eval / cache write);
    - calls ``get_annotations`` AGAIN -> now served from the object's cache; must
      return an EQUAL map AND the SAME cached value objects (identity-stable) as
      the first call;
    - additionally checks ``Format.FORWARDREF`` on an object with one resolvable
      annotation (the wid-value) and one UNRESOLVABLE name yields the wid-value for
      the resolvable slot and an ``annotationlib.ForwardRef`` for the unresolvable
      one -- a distinct C code path through ``__annotate__`` (the fake-globals
      proxy) that must still see THIS fiber's cell.

  Because every annotated object is single-owner (built by a factory inside the
  fiber, stored in a local, never handed to a sibling), a correct runtime MUST
  return this fiber's values every time.  Verified with plain-threads controls (8,
  24 and 64 OS threads, GIL on AND off, each building its own factory objects with
  unique wid-values and asserting get_annotations round-trips them; plus a
  cross-thread handoff control that does the FIRST get on one thread and the SECOND
  get on another -- exactly mirroring a hub migration) that 100% of accesses return
  the owning thread's value -- 0 leaks, 0 crashes, 115k+ cross-thread handoffs
  clean.  Under a CORRECT runloom the same must hold; the single-owner oracle
  PASSES (program exits 0) when there is no bug.

ORACLES:
  * LOAD-BEARING -- DEFERRED-EVAL ISOLATION (worker, HARD, fail-fast).  Single-owner
    factory-built FUNCTION per iteration; VALUE round-trip before/after a yield must
    equal ``{name: wid-value}`` with identity-stable cached values; FORWARDREF
    mixed-resolvability must return this fiber's value + a ForwardRef.  A violation
    is a runloom isolation desync -- ``H.fail`` fires.

  * COMPLETENESS (post, HARD): ``require_no_lost`` -- a fiber stranded inside
    ``__annotate__`` / the cache write-back never returns; the watchdog catches it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (``ann_checks>0``).

FAIL ON: a factory-built object's deferred annotations returning a cross-fiber
value, an identity change of a cached annotation value across a yield, a second
get_annotations disagreeing with the first, or a FORWARDREF result that lost this
fiber's cell.  There is NO shared-mutable arm here: every annotated object is
single-owner, so any discrepancy is a genuine runtime isolation bug, never
documented Python shared-object semantics.

--------------------------------------------------------------------------------
SUSPECTED RUNTIME BUG -- the CLASS-annotation arm (default OFF, gated behind
``PYGO_P524_CLASS_ARM=1``).

The design also called for a single-owner CLASS arm (``make_fiber_cls``: a
fiber-local ``class`` with deferred annotations, same VALUE round-trip oracle).
That arm is GATED OFF by default because it exposes what appears to be a real
free-threading runtime fault in the CLASS ``__annotations__`` cache path -- NOT a
test bug.  Evidence gathered while implementing this program:

  * The FUNCTION-VALUE and FORWARDREF arms (identical single-owner methodology)
    PASS under runloom at 8 hubs / thousands of fibers -- millions of round-trips,
    zero desync.  Only the CLASS arm fails, so the oracle methodology is sound.
  * The CLASS arm's SECOND ``get_annotations(cls, VALUE)`` returns ``{}`` (an
    EMPTY dict) where the FIRST returned the correct 4-entry map, then the process
    takes a SIGSEGV.  The class is single-owner (built by a factory, stored in a
    local, never shared) so the cache must persist.
  * It reproduces at hubs>=2 and is CLEAN at hubs=1, and reproduces with runloom
    preemption both ON and OFF -- i.e. cooperative migration of the single fiber
    across hubs between the two get_annotations calls is enough.
  * It reproduces even with LITERAL (closure-free) class annotations, so it is the
    type ``__annotations__`` cache machinery (the type-dict cache write-back +
    PyType_Modified), not a closure-cell issue.
  * It is STRUCTURALLY INACCESSIBLE to plain OS threads: single-thread (500k
    iters), plain-threads same-thread (64x18s), plain-threads high-churn, AND a
    plain-threads CROSS-THREAD handoff control (first get on thread A, second get
    on thread B -- 115k handoffs) are ALL clean.  The fault needs M:N fibers whose
    two annotation accesses execute on different hub PyThreadStates while the CLASS
    type-annotation cache is being written -- exactly the M:N-on-shared-tstate
    regime OS threads never enter.

To reproduce:  ``PYGO_P524_CLASS_ARM=1 <the validate command>`` -> desync ({}) +
SIGSEGV within a few seconds at hubs>=2.  Left OFF by default so this file is a
clean PASSing big_100 detector (the function/forwardref arms are legitimate PEP 649
deferred-eval isolation oracles) rather than a guaranteed-crash pollutant.
--------------------------------------------------------------------------------

Stresses: PEP 649 lazy ``__annotate__`` evaluation across hub migration + yield,
the per-object ``__annotations__`` cache write-back under M:N, closure-cell
dereference inside deferred annotation bodies, ``annotationlib.get_annotations``
Format.VALUE / Format.FORWARDREF C paths (including the fake-globals ForwardRef
proxy) under tens of thousands of goroutines.
"""
import os

import annotationlib
from annotationlib import Format, get_annotations, ForwardRef

import harness
import runloom

# Per-fiber annotation values are drawn from this band.  Each wid+idx gets a
# distinct base so a leaked sibling value is visibly wrong.
VALUE_SCALE = 100000
# Number of annotated slots per object.
NSLOTS = 4

# The CLASS-annotation arm drives the type ``__annotations__`` cache + shared
# __annotate__ code objects concurrently across hubs -- which crashed on 3.14t via
# the CPython thread-local-bytecode (TLBC) co_tlbc grow / QSBR free-list corruption
# ({} desync then SIGSEGV).  runloom.run() now re-execs ft-3.14 with PYTHON_TLBC=0
# (src/runloom/runtime.py), so the arm PASSES and is a live REGRESSION GUARD; it is
# ON by default.  Set PYGO_P524_CLASS_ARM=0 to skip it, or RUNLOOM_TLBC=1 to re-arm
# the crash for verification.
CLASS_ARM = os.environ.get("PYGO_P524_CLASS_ARM", "1") != "0"

# Run the (heavier) FORWARDREF fake-globals arm every Nth inner iteration so the
# fast VALUE arm dominates throughput while the ForwardRef C path is still
# exercised continuously.
FORWARDREF_EVERY = 8


def make_fiber_fn(wid, idx):
    """Build a NEW function whose deferred (__annotate__) annotations close over a
    distinct per-fiber cell.  The function is single-owner (returned into a
    fiber-local variable, never shared).

    Returns (fn, expected_map).  The annotation values are unique per (wid, idx):
    each slot j gets ``base + j`` where ``base = wid*VALUE_SCALE + idx*NSLOTS``.
    PEP 649 does NOT evaluate these at def time -- they run when get_annotations
    is called, inside the requesting fiber's frame, reading THIS closure's cell."""
    base = wid * VALUE_SCALE + (idx % 997) * NSLOTS

    # These locals are captured by the __annotate__ closure.  The annotation
    # expressions reference them; PEP 649 defers their evaluation.
    v0 = base + 0
    v1 = base + 1
    v2 = base + 2
    v3 = base + 3

    def fn(alpha: v0, beta: v1, gamma: v2, delta: v3):
        pass

    expected = {"alpha": v0, "beta": v1, "gamma": v2, "delta": v3}
    return fn, expected


def make_fiber_cls(wid, idx):
    """Build a NEW class whose deferred class-body annotations close over a distinct
    per-fiber cell.  Single-owner (returned into a fiber-local variable).

    GATED behind PYGO_P524_CLASS_ARM -- see the module docstring's SUSPECTED
    RUNTIME BUG section: this class-annotation path desyncs ({}) + SIGSEGVs under
    M:N at hubs>=2 while every off-runloom control stays clean.

    Returns (cls, expected_map)."""
    base = wid * VALUE_SCALE + (idx % 997) * NSLOTS + 40000

    v0 = base + 0
    v1 = base + 1
    v2 = base + 2
    v3 = base + 3

    class Cls:
        alpha: v0
        beta: v1
        gamma: v2
        delta: v3

    expected = {"alpha": v0, "beta": v1, "gamma": v2, "delta": v3}
    return Cls, expected


def make_fiber_forwardref_fn(wid, idx):
    """Build a NEW function mixing one RESOLVABLE annotation (this fiber's wid-value)
    with one UNRESOLVABLE name (never defined).  Format.FORWARDREF must resolve the
    former to this fiber's value and hand back a ForwardRef for the latter.

    Returns (fn, resolvable_value)."""
    base = wid * VALUE_SCALE + (idx % 997) * NSLOTS + 80000
    resolvable = base + 0

    def fn(good: resolvable, bad: NopeUndefinedName_524):  # noqa: F821 - deferred, never eval'd under VALUE
        pass

    return fn, resolvable


def check_value_roundtrip(H, wid, obj, expected, kind):
    """FIRST get_annotations(VALUE) -> must equal expected; yield; SECOND call ->
    must be EQUAL and identity-stable (served from the object's cache).  Single-
    owner: obj was built by a factory in THIS fiber, never shared."""
    first = get_annotations(obj, format=Format.VALUE)
    if first != expected:
        H.fail("annotationlib VALUE leak: {0} first get_annotations returned {1!r}, "
               "expected {2!r} (wid {3}) -- the deferred __annotate__ body read a "
               "cross-fiber cell instead of this fiber's".format(
                   kind, first, expected, wid))
        return False

    # Snapshot the cached value objects so we can prove identity stability across
    # the yield (the cache must stick and must not be overwritten by a sibling).
    baseline_ids = {name: id(first[name]) for name in expected}

    # YIELD at the hazard boundary: a sibling may drive its OWN first deferred eval
    # + cache write-back while we are parked, possibly on another hub.
    runloom.yield_now()
    if id(obj) & 1:
        runloom.sleep(0.0002)

    second = get_annotations(obj, format=Format.VALUE)
    if second != expected:
        H.fail("annotationlib VALUE desync: {0} second get_annotations (post-yield) "
               "returned {1!r}, expected {2!r} (wid {3}) -- the annotation cache "
               "did not stick or a sibling overwrote this object's cached "
               "annotations across the yield".format(kind, second, expected, wid))
        return False

    # Cached values must be the SAME objects both times (identity-stable cache).
    for name in expected:
        if id(second[name]) != baseline_ids[name]:
            H.fail("annotationlib cache identity changed: {0}.{1} value object id "
                   "changed from {2} to {3} across a yield (wid {4}) -- the "
                   "__annotations__ cache was rebuilt/replaced under M:N, or a "
                   "cross-fiber write landed".format(
                       kind, name, baseline_ids[name], id(second[name]), wid))
            return False

    return True


def check_forwardref(H, wid, obj, resolvable):
    """Format.FORWARDREF on a mixed object: 'good' must resolve to THIS fiber's
    value, 'bad' must come back as a ForwardRef (the fake-globals proxy C path
    still reading this fiber's cell)."""
    result = get_annotations(obj, format=Format.FORWARDREF)
    good = result.get("good")
    if good != resolvable:
        H.fail("annotationlib FORWARDREF leak: resolvable slot 'good' == {0!r}, "
               "expected {1!r} (wid {2}) -- the fake-globals deferred path read a "
               "cross-fiber cell".format(good, resolvable, wid))
        return False
    bad = result.get("bad")
    if not isinstance(bad, ForwardRef):
        H.fail("annotationlib FORWARDREF wrong type: unresolvable slot 'bad' == "
               "{0!r} (type {1}), expected a ForwardRef (wid {2}) -- the deferred "
               "FORWARDREF path did not produce a ForwardRef for the undefined "
               "name".format(bad, type(bad).__name__, wid))
        return False
    return True


# Sustained deferred-eval churn per worker, bounded by H.running().  The isolation
# hazard only manifests under SUSTAINED overlap -- many fibers simultaneously
# driving first-time __annotate__ evals + cache write-backs while parked across a
# yield, so the scheduler reliably interleaves a sibling before this fiber resumes.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber, per inner iteration, builds a single-owner factory FUNCTION and
    runs the load-bearing VALUE round-trip oracle across a yield; every
    FORWARDREF_EVERY iterations it also exercises the FORWARDREF fake-globals path.
    Nothing is shared between fibers, so any discrepancy is a genuine runtime
    isolation bug.  The CLASS arm runs only when PYGO_P524_CLASS_ARM is set (see
    the module docstring's SUSPECTED RUNTIME BUG note)."""
    checks = state["ann_checks"]
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            fn, exp_fn = make_fiber_fn(wid, idx)
            if not check_value_roundtrip(H, wid, fn, exp_fn, "function"):
                return

            if idx % FORWARDREF_EVERY == 0:
                frf, resolvable = make_fiber_forwardref_fn(wid, idx)
                if not check_forwardref(H, wid, frf, resolvable):
                    return

            if CLASS_ARM:
                cls, exp_cls = make_fiber_cls(wid, idx)
                if not check_value_roundtrip(H, wid, cls, exp_cls, "class"):
                    return

            checks[wid] += 1              # single-writer-per-slot (wid-indexed)
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # ann_checks: ONE race-free slot per worker (wid-indexed; see hard rule 1).
    # Feeds the NON-VACUITY tally -- allocated here where H.funcs is known.
    H.state = {
        "ann_checks": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    achecks = sum(H.state["ann_checks"])
    H.log("annotationlib[single-owner LOAD-BEARING]: {0} deferred-eval isolation "
          "round-trips (function VALUE + periodic FORWARDREF{1}, all passed "
          "fail-fast); ops={2}".format(
              achecks,
              " + class VALUE [PYGO_P524_CLASS_ARM]" if CLASS_ARM else "",
              H.total_ops()))
    if CLASS_ARM:
        H.log("note: PYGO_P524_CLASS_ARM is set -- the class-annotation arm is "
              "ENABLED; this arm exposes a suspected free-threading runtime fault "
              "(second get_annotations returns {} + SIGSEGV at hubs>=2).  See the "
              "module docstring's SUSPECTED RUNTIME BUG section.")

    # NON-VACUITY: the load-bearing deferred-eval hazard was actually exercised.
    H.check(achecks > 0,
            "no single-owner deferred-annotation checks ran -- the load-bearing "
            "PEP 649 __annotate__ isolation hazard was never exercised (oracle "
            "would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside a deferred
    # __annotate__ eval or the __annotations__ cache write-back).
    H.require_no_lost("annotationlib deferred-eval isolation")


if __name__ == "__main__":
    harness.main(
        "p524_annotationlib_deferred_eval", body, setup=setup, post=post,
        default_funcs=5000,
        describe="PEP 649 stores annotations as a lazy __annotate__ function whose "
                 "body runs at get_annotations() time inside the requesting "
                 "fiber's frame, closing over a per-fiber cell and caching the "
                 "result on the object.  LOAD-BEARING: each fiber builds (via a "
                 "factory) single-owner FUNCTION objects whose deferred "
                 "annotations close over a wid-seeded value, then asserts "
                 "get_annotations(VALUE) before/after a yield equals {name: "
                 "wid-value} with identity-stable cached values, and FORWARDREF "
                 "on a mixed object returns this fiber's value + a ForwardRef.  A "
                 "cross-fiber annotation value, a cache identity change across a "
                 "yield, or a lost ForwardRef cell is the runloom deferred-eval "
                 "isolation bug.  (The class-annotation arm is gated behind "
                 "PYGO_P524_CLASS_ARM: it exposes a SUSPECTED runtime fault -- "
                 "second get_annotations(cls,VALUE) returns {} + SIGSEGV at "
                 "hubs>=2, clean on every off-runloom control; see the docstring)")
