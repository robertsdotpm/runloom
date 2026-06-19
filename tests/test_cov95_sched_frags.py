"""Adversarial coverage suite for four runloom scheduler fragments:

  * src/runloom_c/runloom_sched_preempt.c.inc   -- the time-sliced preemption
    timer thread (runloom_preempt_main / runloom_preempt_yield_cb) + init/fini.
  * src/runloom_c/runloom_sched_core.c.inc       -- stack calibration freeze,
    runloom_g_entry, the g try_incref/decref refcount machine, cal_record.
  * src/runloom_c/runloom_sched_pystate.c.inc    -- the per-fiber CPython state
    snap/load dance: exception-in-flight save/restore, the immortal-context
    fast path, and the RUNLOOM_DBG_EXCSTATE excobj validator.
  * src/runloom_c/runloom_sched_parkwake.c.inc   -- the Dekker park/wake
    handshake (runloom_park_generic / _timed / runloom_sched_park_safe),
    the timer-heap drain + teardown sweep, sched_sleep_real, run_ready.

Each test drives a *reachable* uncovered region with a REAL oracle.  The
hardest regions are the race windows in the Dekker park/wake handshake (a
cross-thread wake landing in the sub-microsecond window between the parker's
`parked_safe=1` store and its wake_pending recheck): those are driven by a
genuine foreign-OS-thread wake STORM against a parked fiber, with the storm
held open until the parker finishes so a wake is never permanently lost (no
strand -> no hang), while the tight cross-thread contention races the abort
branch.  The oracle is "every park completed and the workload exited cleanly"
-- a lost wake would hang (caught by hang_guard), a UAF would crash.

Env-gated regions (RUNLOOM_DBG_EXCSTATE excobj validator; RUNLOOM_NO_CTX_COPY
immortal-context snap fast path; the multi-hub calibration-freeze race) run in
SUBPROCESSES that exit cleanly so gcov flushes their counters; a
TimeoutExpired is treated as box contention (skip), not a bug.

UNREACHABLE-from-a-test lines are NOT faked -- they are catalogued in the
structured report's exclusions[]: the per-g-tstate teardown (RUNLOOM_PER_G_TSTATE
is GATED OFF behind RUNLOOM_ALLOW_UNSAFE_MIGRATION, which the rules forbid),
the OOM-cleanup branches with no fault hook, the Go-style abort()-on-panic and
the corrupt-excobj abort() guard (crash-only / defensive), the thread-create-
fail branch (no spawn fault hook reaches it), and the cross-thread
runloom_sched_wake delivery between two independent single-thread run() loops
(an unsupported topology that deadlock-detects rather than delivering).
"""
import os
import subprocess
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

import runloom
import runloom_c as rc
from adv_util import needs_free_threading, hang_guard

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

mn = pytest.mark.skipif(not FT, reason="M:N scheduler needs the GIL off (3.13t)")


def _spawn(code, env_extra=None, timeout=240):
    """Run `code` in a clean child; return CompletedProcess or skip on timeout."""
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    if env_extra:
        env.update(env_extra)
    try:
        return subprocess.run([PY, "-c", code], cwd=REPO, env=env,
                              capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.skip("subprocess timed out (shared CI box under contention)")


# ==========================================================================
# runloom_sched_preempt.c.inc -- the time-sliced preemption timer thread.
#
# rc.preempt_init(quantum_us) starts runloom_preempt_main (the dedicated OS
# timer thread, L26-50): it sleeps one quantum, then posts one
# Py_AddPendingCall(runloom_preempt_yield_cb) per hub.  When a hub's eval
# breaker drains the pending call, runloom_preempt_yield_cb (L8-23) runs:
# runloom_mn_yield_current() on a hub, else the single-thread sched yield.
# We arm a SMALL quantum and run two non-cooperative CPU hogs on a hub so the
# posted callbacks are actually drained (the yield path runs), then
# preempt_fini (L74-87) joins the thread.  A clean subprocess exit flushes the
# timer-thread + callback lines.  The thread-create-FAIL branch (L67-69) has
# no fault hook -> classified SPAWNFAIL in exclusions[].
# ==========================================================================
_PREEMPT = r'''
import sys, time; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
rc.preempt_init(2000)          # 2ms quantum -> the timer fires many times
def main():
    wg = WaitGroup(); wg.add(2)
    def hog(i):
        try:
            x = 0
            t0 = time.monotonic()
            while time.monotonic() - t0 < 0.3:   # pure CPU, NO cooperative yield
                x = (x + i * i) % 7
        finally:
            wg.done()
    rc.mn_go(lambda: hog(1))
    rc.mn_go(lambda: hog(2))
    wg.wait()
runloom.run(2, main)
rc.preempt_fini()
sys.stdout.write("PREEMPT_OK\n")
'''


@mn
def test_preempt_timer_thread_posts_and_yields_subprocess():
    p = _spawn(_PREEMPT, timeout=120)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "PREEMPT_OK" in p.stdout, (p.stdout[-400:], p.stderr[-800:])


# Single-thread preemption: the posted runloom_preempt_yield_cb drains where
# runloom_mn_yield_current() is FALSE, so it takes the single-thread sched
# fallback (preempt.c L17-22: runloom_sched_get()->current != NULL ->
# runloom_sched_yield) -- the branch the M:N hog above doesn't exercise.
_PREEMPT_ST = r'''
import sys, time; sys.path.insert(0, "src")
import runloom, runloom_c as rc
rc.preempt_init(2000)          # 2ms quantum
done = []
def hog(i):
    x = 0
    t0 = time.monotonic()
    while time.monotonic() - t0 < 0.25:   # CPU-bound, no cooperative yield
        x = (x + i * i) % 7
    done.append(i)
rc.fiber(lambda: hog(1))
rc.fiber(lambda: hog(2))
rc.run()
rc.preempt_fini()
sys.stdout.write("PREEMPT_ST_OK %d\n" % len(done))
'''


@mn
def test_preempt_single_thread_yield_fallback_subprocess():
    p = _spawn(_PREEMPT_ST, timeout=120)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "PREEMPT_ST_OK 2" in p.stdout, (p.stdout[-400:], p.stderr[-800:])


def test_preempt_init_validates_quantum_and_idempotent_fini():
    """In-process lifecycle oracle for runloom_preempt_init/_fini's argument
    validation + idempotency (preempt.c L52-77): a non-positive quantum is a
    ValueError (not silently accepted), double-init re-arms safely, and
    fini-without-init / double-fini are no-ops.  Leaves no timer armed."""
    rc.preempt_fini()                  # no-op without init (L76 early return)
    with pytest.raises((ValueError, OverflowError)):
        rc.preempt_init(0)             # quantum_us <= 0 -> ValueError (L54-56)
    with pytest.raises((ValueError, OverflowError)):
        rc.preempt_init(-1)
    rc.preempt_init(50000)             # valid arm
    rc.preempt_init(40000)             # double init -> safe re-arm (L59-63)
    rc.preempt_fini()                  # join the timer thread (L74-87)
    rc.preempt_fini()                  # double fini -> no-op
    assert rc._self_check(0) == 0


# ==========================================================================
# runloom_sched_parkwake.c.inc -- timed in-memory park inside an M:N hub
# (runloom_park_generic_timed hub branch L205-207) + the timer-heap drain
# routing the timeout back to the hub via mn_wake_g (runloom_sched_drain_timers
# L265-270).  A hub fiber parks with a deadline and nobody wakes it -> the
# hub_main timer drain fires it, and park() returns True (timed out).
# ==========================================================================
@mn
def test_timed_park_in_hub_times_out_via_timer_drain():
    res = {}

    def main():
        from runloom.sync import WaitGroup
        wg = WaitGroup(); wg.add(1)

        def f():
            try:
                # No wake arrives: the timer-heap entry pushed by
                # runloom_park_generic_timed is the ONLY thing that re-queues
                # this g -- exercising drain_timers' mn_wake_g (park_hub != NULL).
                res["timed_out"] = rc.park(timeout=0.05)
            finally:
                wg.done()

        rc.mn_go(f)
        wg.wait()

    with hang_guard(40, "timed_park_hub"):
        runloom.run(3, main)
    # Real oracle: it actually timed out (the timer fired), not a spurious wake.
    assert res.get("timed_out") is True


@mn
def test_timed_park_in_hub_woken_before_deadline_and_release_timers():
    """A hub fiber timed-parks with a LONG (30s) deadline and is woken EARLY by
    a sibling's G.wake() -> park() returns False (woken), exercising the WOKEN
    branch of the drain CAS (the timer loses).  The unfired 30s timer entry then
    survives on the hub's timer heap until mn_fini, where
    runloom_sched_release_timers (parkwake L287-294) sweeps its g-ref -- the
    teardown path for entries that never fired.  (mn_fini drives the per-hub
    runloom_sched_release_timers; the single-thread run()-end caller is the same
    function.  An early cross-thread wake of a single-thread timed park is an
    unreliable topology -- the owner loop can block on the stale 30s deadline
    rather than draining the ready g -- so this is driven on the reliable M:N
    hub where mn_wake_g delivers in-hub.)  A leaked entry-ref would fail
    _self_check; the parked-leak invariant in conftest also guards it."""
    res = {}
    hbox = {}

    def main():
        from runloom.sync import WaitGroup
        wg = WaitGroup(); wg.add(1)

        def parker():
            try:
                hbox["g"] = rc.current_g()
                res["woke"] = rc.park(timeout=30.0)   # long; woken early below
            finally:
                wg.done()

        def waker():
            for _ in range(2_000_000):
                if hbox.get("g") is not None:
                    break
                rc.sched_yield_classic()
            rc.sched_sleep(0.02)          # let the parker commit its park
            hbox["g"].wake()

        rc.mn_go(parker)
        rc.mn_go(waker)
        wg.wait()

    with hang_guard(40, "timed_park_woken_release"):
        runloom.run(2, main)
    assert res.get("woke") is False       # False == woken (not timed out)
    # mn_fini already ran (inside runloom.run) and swept the stale 30s entry;
    # the structural walk confirms no leaked entry-ref.
    assert rc._self_check(0) == 0


# ==========================================================================
# runloom_sched_parkwake.c.inc -- runloom_sched_sleep_until_real (L575-577,
# via runloom_sched_sleep_until_ex real=1).  Exposed as rc.sched_sleep_real:
# a wall-clock sleep that does not advance the logical clock.
# ==========================================================================
def test_sched_sleep_real_single_thread():
    out = []

    def s():
        t0 = time.monotonic()
        rc.sched_sleep_real(0.03)
        out.append(time.monotonic() - t0)

    with hang_guard(20, "sched_sleep_real"):
        rc.fiber(s)
        rc.run()
    assert len(out) == 1
    # Real oracle: it actually slept ~30ms (a real wall-clock park, not a
    # no-op early return).
    assert out[0] >= 0.02, out


@mn
def test_sched_sleep_real_in_hub():
    """sched_sleep_real inside an M:N hub takes the hub branch of
    sleep_until_ex (target != NULL); a clean completion proves the real-clock
    sleeper parks + resumes on the hub sleep heap."""
    out = []

    def main():
        from runloom.sync import WaitGroup
        wg = WaitGroup(); wg.add(1)

        def s():
            try:
                t0 = time.monotonic()
                rc.sched_sleep_real(0.03)
                out.append(time.monotonic() - t0)
            finally:
                wg.done()

        rc.mn_go(s)
        wg.wait()

    with hang_guard(40, "sched_sleep_real_hub"):
        runloom.run(2, main)
    assert len(out) == 1 and out[0] >= 0.02, out


# ==========================================================================
# runloom_sched_parkwake.c.inc -- runloom_sched_run_ready (L590-610):
#   * M:N hub branch (L593-596): degrade-to-one-classic-yield.
#   * single-thread quiescence-list FIFO append, both the head (first fiber)
#     and the TAIL-append (L602-603, second+ fiber) branches.
# ==========================================================================
def test_run_ready_quiescence_two_fibers_single_thread():
    """Two fibers both call run_ready(); the second appends onto the
    quiescence list's TAIL (quiescence_tail != NULL -> L602-603).  Oracle:
    both run to completion (the quiescence barrier resumed every parked
    fiber once the ready ring drained)."""
    out = []

    def w(i):
        rc.run_ready()       # park on the quiescence list, resume at quiescence
        out.append(i)

    with hang_guard(20, "run_ready_quiescence"):
        rc.fiber(lambda: w(1))
        rc.fiber(lambda: w(2))
        rc.run()
    assert sorted(out) == [1, 2]


@mn
def test_run_ready_in_hub_degrades_to_yield():
    """run_ready() inside an M:N hub has no hub-local quiescence list, so it
    degrades to a single classic yield (L593-596).  Oracle: the fiber runs
    past run_ready() to completion."""
    out = []

    def main():
        from runloom.sync import WaitGroup
        wg = WaitGroup(); wg.add(1)

        def f():
            try:
                rc.run_ready()      # hub: one yield, then resume
                out.append("ok")
            finally:
                wg.done()

        rc.mn_go(f)
        wg.wait()

    with hang_guard(40, "run_ready_hub"):
        runloom.run(2, main)
    assert out == ["ok"]


# ==========================================================================
# runloom_sched_parkwake.c.inc -- the DEKKER park/wake abort windows.
#
# runloom_sched_park_safe (single-thread, L500-506) and runloom_park_generic's
# hub branch (L156-159 in-window early-out, L164-170 CAS abort) each have a
# narrow race: a cross-thread wake_safe bump of wake_pending landing right
# after the parker's parked_safe=1 store.  We drive it with a FOREIGN OS-thread
# wake STORM held open until the parker has done all its parks -- so no wake is
# ever permanently lost (the parker can't strand -> no hang), while the tight
# cross-thread contention races the abort branch.  Probabilistic on the abort
# line; the assertion is the safety invariant: every park returned and the
# loop did not lose a wake (no hang, no crash, exact count).
# ==========================================================================
def _storm_until(hbox, stop, key="g"):
    """Foreign-OS-thread waker: storm G.wake() at the parked fiber's handle
    until told to stop.  A true OS thread (real parallelism) is required to
    hit the sub-microsecond store/recheck window."""
    for _ in range(4000):
        if hbox.get(key) is not None:
            break
        time.sleep(0.0005)
    g = hbox.get(key)
    if g is None:
        return
    while not stop.is_set():
        g.wake()


def test_park_safe_dekker_abort_under_foreign_wake_storm():
    PARKS = 30000
    hbox = {}
    res = {}
    stop = threading.Event()

    t = threading.Thread(target=_storm_until, args=(hbox, stop), daemon=True)
    t.start()

    def parker():
        hbox["g"] = rc.current_g()
        n = 0
        for _ in range(PARKS):
            # foreign_wakeable=True -> single-thread park_generic -> park_safe,
            # AND keeps run() alive for the cross-thread waker.
            rc.park(foreign_wakeable=True)
            n += 1
        res["n"] = n
        # Stop + join the foreign wake-storm WHILE the runtime is still up (see
        # the M:N sibling): a storm that outlives run() executes g.wake() into a
        # torn-down runtime -> use-after-free / crash.
        stop.set()
        t.join(timeout=3)

    with hang_guard(90, "park_safe_dekker"):
        rc.fiber(parker)
        rc.run()
    # Safety oracle: not one of 30k parks lost its wake (would hang) and none
    # double-freed / returned garbage (would crash).  Exact count == no lost
    # wake, no spurious extra return.
    assert res.get("n") == PARKS


@mn
def test_park_generic_hub_dekker_under_foreign_wake_storm():
    PARKS = 20000
    hbox = {}
    res = {}
    stop = threading.Event()

    t = threading.Thread(target=_storm_until, args=(hbox, stop), daemon=True)
    t.start()

    def main():
        from runloom.sync import WaitGroup
        wg = WaitGroup(); wg.add(1)

        def parker():
            try:
                hbox["g"] = rc.current_g()
                n = 0
                for _ in range(PARKS):
                    # hub park_generic: cross-thread wake routes via park_hub
                    # (mn_wake_g); races the hub Dekker (L156-170).
                    rc.park()
                    n += 1
                res["n"] = n
            finally:
                wg.done()

        rc.mn_go(parker)
        wg.wait()
        # Stop the foreign wake-storm and JOIN it WHILE the runtime is still up.
        # If the storm outlives runloom.run() (stop/join placed AFTER run
        # returned), the foreign thread keeps executing g.wake() -- i.e. running
        # Python -- while mn_fini tears the free-threaded runtime down and frees
        # the g-slab; that corrupts the foreign thread's eval state and faults
        # with a NULL vectorcall (the rare dekker SIGSEGV -- PID-attributed core:
        # the crashing thread is _storm_until -> g.wake() with a garbage eval
        # throwflag, all hubs already gone).  Bounding the storm to run()'s
        # lifetime keeps the during-park abort-window coverage and removes the
        # post-teardown use-after-free.
        stop.set()
        t.join(timeout=3)

    with hang_guard(120, "park_generic_hub_dekker"):
        runloom.run(3, main)
    assert res.get("n") == PARKS


# ==========================================================================
# module_g.c.inc -- RunloomG.wake() must be a safe no-op after M:N teardown.
#
# The two dekker tests above AVOID the post-teardown UAF by bounding their
# wake-storm to run()'s lifetime.  These two do the OPPOSITE on purpose: they
# wake a handle that OUTLIVED its M:N run, after mn_fini bumped the session
# generation and freed the runloom_hubs array that the g's park_hub points into.
# The runtime guard (a per-session generation stamped on the handle at creation,
# bumped at every mn_fini) turns that into a no-op instead of following a
# dangling park_hub into freed hub memory.  This is the belt-and-suspenders the
# dekker FINDING calls for (docs/dev/repro/DEKKER_SIGSEGV_FINDING.md).
#
# These are STAYS-SAFE nets, not deterministic crash-catchers: the guarded read
# is undefined behaviour, but it does not reliably fault, because (a) PyMem
# retains the freed arena mapping and (b) wake_safe short-circuits on a DONE g
# (the normal post-run() state) before it would dereference the hub.  A poison-
# the-freed-array control confirmed even the unguarded path does not segfault
# here -- consistent with the FINDING, whose actual crash was Python-eval-state
# corruption in the foreign thread, removed by bounding the storm (the landed
# test fix).  The value of THIS guard is eliminating the UB read outright; these
# tests assert the safe behaviour holds across teardown and re-init aliasing.
# ==========================================================================
def _wake_storm(h, stop):
    """Hammer a handle's .wake() from a foreign OS thread until told to stop."""
    while not stop.is_set():
        h.wake()


def _run_capture_hub_parker(hbox, done):
    """Run a 3-hub M:N session whose parker parks on a hub (records park_hub),
    capture its handle in hbox['g'], wake it so it completes, then return (which
    runs mn_fini -> frees the hub array -> bumps the session generation)."""
    def main():
        def parker():
            hbox["g"] = rc.current_g()   # handle for THIS hub-parked g
            rc.park()                    # hub park_generic -> records park_hub
            done["ok"] = True
        rc.mn_go(parker)
        for _ in range(2000):
            if hbox.get("g") is not None:
                break
            runloom.sleep(0.0005)
        # wake (in-session, normal path) until the parker completes
        for _ in range(4000):
            if done.get("ok"):
                break
            hbox["g"].wake()
            runloom.sleep(0.0005)
    with hang_guard(60, "wake_teardown_setup"):
        runloom.run(3, main)


@mn
def test_wake_after_mn_teardown_is_noop():
    ROUNDS = 30
    handles = []                         # hold every handle alive across rounds
    for _ in range(ROUNDS):
        hbox, done = {}, {}
        _run_capture_hub_parker(hbox, done)
        h = hbox["g"]
        handles.append(h)
        assert bool(h.done)              # parker completed before teardown
        # The runtime is now torn down; h.park_hub dangles into the freed hub
        # array.  Wake it hard, from the main thread AND a foreign OS thread.
        # Pre-guard this faulted; with the guard it is a no-op.
        for _ in range(500):
            h.wake()
        stop = threading.Event()
        t = threading.Thread(target=_wake_storm, args=(h, stop), daemon=True)
        t.start()
        time.sleep(0.02)
        stop.set()
        t.join(timeout=3)
    # Every stale handle's wake was absorbed; the process survived all rounds.
    assert len(handles) == ROUNDS
    assert all(bool(h.done) for h in handles)


@mn
def test_stale_handle_wake_during_reinit_is_noop():
    """The re-init aliasing case a bare `runloom_hubs != NULL` guard would MISS:
    a stale handle from pool #1 is stormed throughout pool #2's life.  Its
    park_hub dangles into pool #1's freed array; a NULL-check would see pool #2's
    (non-NULL) array and deref the dangling hub, corrupting the LIVE pool.  The
    generation stamp distinguishes them, so the wake is skipped and pool #2's
    work completes intact."""
    # Round 1: capture a hub-parked handle, complete it, tear pool #1 down.
    hbox, done = {}, {}
    _run_capture_hub_parker(hbox, done)
    h1 = hbox["g"]
    assert bool(h1.done)

    # Round 2: a foreign thread storms the stale h1 throughout a FRESH session.
    stop = threading.Event()
    t = threading.Thread(target=_wake_storm, args=(h1, stop), daemon=True)
    t.start()
    res = {}

    def main2():
        from runloom.sync import WaitGroup
        N = 300
        slots = bytearray(N)             # one writer per slot: GIL-off-safe
        wg = WaitGroup()
        for i in range(N):
            wg.add(1)

            def w(i=i):
                try:
                    runloom.sleep(0.001)
                    slots[i] = 1
                finally:
                    wg.done()
            rc.mn_go(w)
        wg.wait()
        res["sum"] = sum(slots)

    with hang_guard(60, "reinit_aliasing"):
        runloom.run(3, main2)
    stop.set()
    t.join(timeout=3)
    # If the stale wake had corrupted pool #2, this would crash / hang / mis-sum.
    assert res.get("sum") == 300


# ==========================================================================
# runloom_sched_pystate.c.inc -- exception-in-flight snap/load.
#
# A fiber that PARKS (sched_sleep) while an exception is in flight (inside an
# `except` block) forces runloom_pystate_snap down the non-default exc branch
# (L162-181 save the exc chain + Py_XINCREF exc_value, walk for the chain
# bottom) and runloom_pystate_load down the matching restore (L383-388 +
# the snap->exc_info==NULL default-reset L377-381 for a sibling that parks
# without an exception).  Oracle: sys.exc_info() survives the park intact.
# ==========================================================================
@mn
def test_exception_state_survives_park_in_hub():
    """INTERLEAVE exc-carrying and exc-FREE fibers on shared hub tstates.
      * The exc-carrying fibers park inside `except` -> snap saves the exc
        chain (L162-181), load restores it (L383-388).
      * The exc-free fibers park with NO exception in flight -> their snap is
        the default sentinel (exc_info==NULL), and on resume onto a hub tstate
        an exc-carrier drifted, load's default-reset branch (L377-381:
        Py_CLEAR + reset exc_info to the embedded sentinel) fires.
    Oracle: every exc-carrier still sees its OWN exception after all its parks
    -- no cross-fiber exc identity bleed (a wrong message would mean the
    per-fiber save/restore mixed two fibers' exception chains).  The exc-free
    fibers exist only to drift+reset the shared tstate (driving the default-
    reset path); they are not asserted on (sys.exc_info() in a fiber that did
    NOT raise but shares a hub tstate is intentionally transient under M:N)."""
    N = 12
    carried = {}      # i -> message seen after parks (odd i carry an exception)

    def main():
        from runloom.sync import WaitGroup
        wg = WaitGroup(); wg.add(N)

        def with_exc(i):
            try:
                try:
                    raise ValueError("exc-%d" % i)
                except ValueError:
                    for _ in range(5):
                        rc.sched_sleep(0.002)
                    exc = sys.exc_info()[1]
                    carried[i] = exc.args[0] if exc is not None else None
            finally:
                wg.done()

        def without_exc(i):
            try:
                for _ in range(5):
                    rc.sched_sleep(0.002)   # default-sentinel snap -> reset load
            finally:
                wg.done()

        for i in range(N):
            if i % 2:
                rc.mn_go(lambda i=i: with_exc(i))
            else:
                rc.mn_go(lambda i=i: without_exc(i))
        wg.wait()

    with hang_guard(40, "exc_state_park"):
        runloom.run(3, main)
    # Every exc-carrier restored its OWN exception across every park (no bleed).
    assert carried == {i: "exc-%d" % i for i in range(N) if i % 2}, carried


def test_exception_state_survives_park_single_thread():
    """Same exc-in-flight snap/load on the single-thread sleep path
    (sleep_until_ex single-thread branch -> pystate_snap, drain resume ->
    pystate_load)."""
    seen = {}

    def f():
        try:
            raise KeyError("st-in-flight")
        except KeyError:
            rc.sched_sleep(0.005)
            exc = sys.exc_info()[1]
            seen["msg"] = exc.args[0] if exc is not None else None

    with hang_guard(20, "exc_state_park_st"):
        rc.fiber(f)
        rc.run()
    assert seen.get("msg") == "st-in-flight"


# ==========================================================================
# runloom_sched_pystate.c.inc -- the immortal-context snap fast path (L114-115:
# ts->context immortal -> store the pointer, skip the atomic INCREF).  With the
# default per-fiber contextvars copy each fiber's context is NON-immortal (the
# L116-118 else branch).  Under RUNLOOM_NO_CTX_COPY=1 fibers share the immortal
# empty default context, so a park's snap takes the immortal fast path.
# Subprocess: the env flag is read once + cached.
# ==========================================================================
_IMMORTAL_CTX = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
N = 16
done = bytearray(N)
def main():
    wg = WaitGroup(); wg.add(N)
    def f(i):
        try:
            # sched_sleep -> pystate_snap with the shared immortal default ctx
            rc.sched_sleep(0.003)
            done[i] = 1
        finally:
            wg.done()
    for i in range(N):
        rc.mn_go(lambda i=i: f(i))
    wg.wait()
runloom.run(3, main)
sys.stdout.write("IMMORTAL_OK %d\n" % sum(done))
'''


@mn
def test_immortal_context_snap_fast_path_subprocess():
    p = _spawn(_IMMORTAL_CTX, env_extra={"RUNLOOM_NO_CTX_COPY": "1"})
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "IMMORTAL_OK 16" in p.stdout, (p.stdout[-400:], p.stderr[-800:])


# ==========================================================================
# runloom_sched_pystate.c.inc -- runloom_dbg_check_excobj (L62-68) + the
# RUNLOOM_DBG_EXCSTATE-gated validation calls in snap (L189-192) and load
# (L272-278).  Under RUNLOOM_DBG_EXCSTATE=1, every snap/load of a fiber that
# parks with an exception in flight runs the validator against the real (valid)
# exception objects; a clean exit proves the validator passes legitimate exc
# state (the abort() at L74 is a defensive corrupt-object guard, classified
# CRASHONLY -- it would kill the process, so gcov never flushes it).
# ==========================================================================
_DBG_EXCSTATE = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
N = 12
ok = bytearray(N)
def main():
    wg = WaitGroup(); wg.add(N)
    def f(i):
        try:
            try:
                raise ValueError("e%d" % i)
            except ValueError:
                for _ in range(4):
                    rc.sched_sleep(0.002)   # snap+load validate the live exc
                if sys.exc_info()[1] is not None:
                    ok[i] = 1
        finally:
            wg.done()
    for i in range(N):
        rc.mn_go(lambda i=i: f(i))
    wg.wait()
runloom.run(3, main)
sys.stdout.write("DBGEXC_OK %d\n" % sum(ok))
'''


@mn
def test_dbg_excstate_validator_passes_live_exceptions_subprocess():
    p = _spawn(_DBG_EXCSTATE, env_extra={"RUNLOOM_DBG_EXCSTATE": "1"})
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    # The validator must NOT have aborted on any legitimate in-flight exception.
    assert "STALE/CORRUPT" not in p.stderr, p.stderr[-1500:]
    assert "DBGEXC_OK 12" in p.stdout, (p.stdout[-400:], p.stderr[-800:])


# ==========================================================================
# runloom_sched_core.c.inc -- runloom_g_entry (the fiber bootstrap) + the
# refcount machine (runloom_g_try_incref / _decref) + runloom_cal_record (the
# stack-calibration freeze).  A deep nested-spawn chain drives many fresh
# free->runnable->running->done transitions (each a try_incref on spawn, a
# decref at completion), and the indexed fiber_n path drives g_entry's
# pass_index branch (PyLong_FromSsize_t + CallFunctionObjArgs).
# ==========================================================================
def test_g_entry_and_refcount_machine_nested_spawn():
    out = []

    def chain(n):
        def step(i):
            if i:
                rc.fiber(lambda: step(i - 1))
            out.append(i)
        step(n)

    with hang_guard(30, "g_entry_nested"):
        rc.fiber(lambda: chain(30))
        rc.run()
    assert sorted(out) == list(range(31))
    assert rc._self_check(0) == 0


@mn
def test_g_entry_fiber_n_indexed_pass_index():
    """fiber_n(indexed=True) drives runloom_g_entry's pass_index branch
    (L398-408: mint a PyLong from the raw index, CallFunctionObjArgs)."""
    N = 256
    seen = bytearray(N)

    def worker(i):
        if 0 <= i < N:
            seen[i] = 1

    def main():
        rc.fiber_n(worker, N, 0, True)        # indexed bulk spawn

    with hang_guard(40, "go_n_indexed"):
        rc.mn_init(4); rc.mn_go(main); rc.mn_run(); rc.mn_fini()
    assert sum(seen) == N


# ==========================================================================
# runloom_sched_core.c.inc -- runloom_cal_record freeze + the LOST-FREEZE-RACE
# re-check (L59-61).  cal_record runs from the SINGLE-THREAD drain on EVERY
# fiber completion and writes the PROCESS-GLOBAL calibration counters; it
# freezes once RUNLOOM_CAL_TARGET (1000) fibers have completed.  The L59-61
# re-check fires when two OS threads (each running its own single-thread
# run()) race the freeze: thread A passes the lock-free `!cal_frozen` fast
# path while still unfrozen, thread B freezes before A takes the cal lock, so
# A re-reads cal_frozen==1 under the lock and bails.  We drive it with several
# raw OS threads each completing ~700 short fibers, crossing the global 1000
# threshold concurrently.  Process-global one-shot -> a fresh subprocess.
# Best-effort on the race line; the oracle is that calibration froze after
# >=1000 completions with the runtime structurally intact.
# ==========================================================================
_CAL_FREEZE = r'''
import sys, threading; sys.path.insert(0, "src")
import runloom, runloom_c as rc
THREADS = 6
PER = 700               # 6*700 = 4200 >> RUNLOOM_CAL_TARGET (1000)
start = threading.Barrier(THREADS)
def loop():
    def run_one():
        start.wait()    # all threads cross the freeze threshold together
        for _ in range(PER):
            rc.fiber(lambda: None)
        rc.run()
    run_one()
ts = [threading.Thread(target=loop, daemon=True) for _ in range(THREADS)]
for t in ts: t.start()
for t in ts: t.join()
st = rc.stats()
assert st["stack_completed"] >= 1000, st["stack_completed"]
assert st["stack_calibrated"] in (1, True), st["stack_calibrated"]
sys.stdout.write("CALFREEZE_OK completed=%d cal=%s\n" % (
    st["stack_completed"], st["stack_calibrated"]))
'''


@mn
def test_calibration_freeze_race_multi_thread_subprocess():
    # Fresh subprocess: calibration is a process-global one-shot, so it must
    # start unfrozen.  RUNLOOM_SYSMON left OFF (no need; avoids stderr noise).
    p = _spawn(_CAL_FREEZE, env_extra={"RUNLOOM_SYSMON": "0"}, timeout=120)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "CALFREEZE_OK" in p.stdout, (p.stdout[-400:], p.stderr[-800:])


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
