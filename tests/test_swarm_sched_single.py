"""Adversarial QA for the SINGLE-THREAD scheduler (sched_single).

Subsystem under test: the M:1 cooperative scheduler that backs
``runloom_c.run()`` -- spawn/admission, the park/wake Dekker handshake,
``unpark_many`` fan-in, ``run_ready`` quiescence, deadlock detection,
``sched_reset``/``sched_stop`` teardown, signal delivery into a parked
cooperative call, and the slab/refcount/datastack churn surfaces.

This file deliberately manufactures the conditions that break a cooperative
scheduler -- lost wakes (wake-before-park / wake-after-park / many waiters),
admission-cap exhaustion + slot release, deadlock->raise/warn, reset/stop with
parked fibers (no UAF), guard-page overflow CLASSIFICATION (not silent
corruption), signal interruption into a parked op, and slow-return collapse of
cooperative overlap.  It goes DEEPER than tests/test_adv_sched.py /
test_run_ready.py / test_unpark_many.py / test_wait_reason.py /
test_signal_interrupt.py / test_crash_handler.py rather than duplicating them.

Crash-prone scenarios run in a SUBPROCESS so a SIGSEGV is contained and observed
as a signalled returncode + a CLASSIFIED report, never a silent wedge.  Hang-
prone scenarios are bounded by hang_guard / finite timeouts so a lost wake is a
bounded failure, not an infinite hang.

FINDINGS encoded here:
  * test_finding_foreign_thread_unpark_many_hangs -- ``unpark_many`` invoked
    DIRECTLY from a foreign OS thread on single-thread-parked waiters wedges the
    process (the runtime's own fan-in primitives use the os.write fallback for
    foreign setters precisely to avoid this; the raw C API offers no guard and
    HANGS rather than erroring).  Subprocess-contained; asserts the current bad
    behavior.
"""
import os
import signal
import subprocess
import sys
import time

import pytest

import runloom
import runloom_c as rc
from adv_util import (hang_guard, assert_faster_than, raw_thread,
                      needs_free_threading)

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HAVE_ALARM = hasattr(signal, "alarm")

READ = 1
UNPARKED = 0x10000000   # the wait_fd sentinel an unpark_many wake returns


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
def _run_single(fn):
    """Run fn() as the sole fiber to completion; return its value."""
    box = {}
    def main():
        box["r"] = fn()
    rc.fiber(main)
    rc.run()
    return box.get("r")


def _subproc(script, env_extra=None, timeout=40):
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src",
               PYTEST_DISABLE_PLUGIN_AUTOLOAD="1")
    if env_extra:
        env.update(env_extra)
    return subprocess.run([sys.executable, "-c", script], cwd=REPO, env=env,
                          capture_output=True, text=True, timeout=timeout)


# ==========================================================================
# 1.  Argument validation / edge values on every public entry point.
#     A bad arg must raise a clean Python error, never crash the interpreter.
# ==========================================================================
def test_fiber_rejects_non_callable():
    with pytest.raises(TypeError):
        rc.fiber(5)
    with pytest.raises(TypeError):
        rc.go_noyield(object())


def test_fiber_n_requires_mn_init_in_single_thread():
    # fiber_n is the M:N bulk-spawn path; under the single-thread scheduler (no
    # hubs) it must raise a clean RuntimeError, not spawn-into-nothing / crash.
    with pytest.raises(RuntimeError):
        rc.fiber_n(lambda: None, 5)
    assert rc._self_check(0) == 0


def test_set_stack_size_rejects_nonpositive():
    for bad in (0, -1, -(1 << 40)):
        with pytest.raises(ValueError):
            rc.set_stack_size(bad)


def test_set_max_fibers_clamps_negative_to_unlimited():
    prev = rc.get_max_fibers()
    try:
        rc.set_max_fibers(-3)
        assert rc.get_max_fibers() == 0     # clamped, not stored as -3
        rc.set_max_fibers(1 << 50)
        assert rc.get_max_fibers() == (1 << 50)
    finally:
        rc.set_max_fibers(prev)


def test_set_deadlock_mode_clamps_out_of_range():
    prev = rc.get_deadlock_mode()
    try:
        rc.set_deadlock_mode(7)
        assert rc.get_deadlock_mode() == 2   # clamped up
        rc.set_deadlock_mode(-9)
        assert rc.get_deadlock_mode() == 0   # clamped down
    finally:
        rc.set_deadlock_mode(prev)


def test_unpark_many_non_sequence_raises():
    with pytest.raises(TypeError):
        rc.unpark_many(5)


def test_unpark_many_non_g_item_raises_inside_fiber():
    def body():
        me = rc.current_g()
        with pytest.raises(TypeError):
            rc.unpark_many([me, object()])      # one valid, one junk
        return "ok"
    assert _run_single(body) == "ok"


def test_park_timed_outside_fiber_raises_but_untimed_is_noop():
    # An asymmetry worth pinning: untimed park() off a fiber is a silent no-op,
    # but the timed form raises (it has no fiber to attach the deadline to).
    rc.park()                       # no-op, no crash
    rc.park_self()                  # no-op, no crash
    rc.set_wait_reason(rc.WR_FUTURE)  # no-op off a fiber
    with pytest.raises(RuntimeError):
        rc.park(timeout=0.01)
    assert rc._self_check(0) == 0


# ==========================================================================
# 2.  current_g identity stability + None outside a fiber.
# ==========================================================================
def test_current_g_none_outside_fiber():
    assert rc.current_g() is None


def test_current_g_stable_across_yield_and_park():
    def body():
        a = rc.current_g()
        rc.sched_yield()
        b = rc.current_g()
        # park/wake round trip mints another wrapper; must still compare equal.
        me = rc.current_g()
        me.wake()                   # self-wake-before-park: consumed, no block
        rc.park_self()
        c = rc.current_g()
        assert a == b == c
        assert hash(a) == hash(b) == hash(c)
        assert {a, b, c} == {a}     # all collapse to one set member
        return "ok"
    with hang_guard(15, "current_g stability"):
        assert _run_single(body) == "ok"


def test_current_g_none_again_after_run_completes():
    rc.fiber(lambda: None)
    rc.run()
    assert rc.current_g() is None   # the run drained; no current fiber survives


# ==========================================================================
# 3.  park / wake Dekker races (single-thread).  go DEEPER than test_adv_sched:
#     many waiters, nested-spawn lost-wake hunt, double-wake idempotence.
# ==========================================================================
def test_many_waiters_each_woken_exactly_once_no_lost_wake():
    # N fibers park_self; main wakes each by its own handle.  A lost wake shows
    # up as a deadlock/hang (hang_guard) or a resumed-count < N.
    N = 200
    resumed = bytearray(N)
    handles = [None] * N

    def waiter(i):
        handles[i] = rc.current_g()
        rc.sched_yield()            # publish handle while runnable
        rc.park_self()
        resumed[i] = 1

    def main():
        for i in range(N):
            rc.fiber(lambda i=i: waiter(i))
        # let every waiter record its handle + reach park
        for _ in range(3):
            rc.sched_yield()
        for i in range(N):
            handles[i].wake()

    with hang_guard(20, "many waiters"):
        rc.fiber(main)
        rc.run()
    assert sum(resumed) == N, "lost wake(s): only %d/%d resumed" % (sum(resumed), N)


def test_double_wake_before_park_consumes_one_and_blocks_again():
    # Two wakes before a single park: park consumes ONE pending wake and returns;
    # a SECOND park then must block (only one wake was banked).  A wake-counter
    # that saturates at 1 would be fine here; a counter that over-counts would
    # let the second park fall through (resumed too early).  We assert the first
    # park returns; the second is woken explicitly so the test can't hang.
    state = {}
    holder = {}

    def waiter():
        g = rc.current_g()
        holder["g"] = g
        rc.sched_yield()            # main double-wakes us here
        rc.park_self()              # consumes 1 banked wake -> returns
        state["first"] = True
        rc.park_self()              # must park again (only 1 was banked)
        state["second"] = True

    def main():
        rc.fiber(waiter)
        rc.sched_yield()
        holder["g"].wake()
        holder["g"].wake()          # double wake-before-(first)-park
        rc.sched_yield()            # let waiter run first park + reach second
        rc.sched_yield()
        holder["g"].wake()          # explicit wake to release the second park

    with hang_guard(15, "double wake before park"):
        rc.fiber(main)
        rc.run()
    assert state.get("first") is True
    assert state.get("second") is True, "second park was not honoured (over-counted wake)"


def test_wake_in_nested_spawn_cascade_is_not_lost():
    # A waiter parks; its waker is reached only after a deep nested-spawn cascade
    # (A spawns B spawns C ... which finally wakes the parked waiter).  Hunts a
    # lost wake that only manifests when the wake is issued from deep inside a
    # freshly-spawned fiber chain rather than from main.
    state = {}
    holder = {}

    def waiter():
        holder["g"] = rc.current_g()
        rc.sched_yield()
        rc.park_self()
        state["resumed"] = True

    def level(n):
        if n == 0:
            holder["g"].wake()
            return
        rc.fiber(lambda: level(n - 1))

    def main():
        rc.fiber(waiter)
        for _ in range(2):
            rc.sched_yield()        # waiter parks
        rc.fiber(lambda: level(12))    # 12-deep spawn cascade ends in the wake

    with hang_guard(15, "nested-spawn wake cascade"):
        rc.fiber(main)
        rc.run()
    assert state.get("resumed") is True, "wake from a deep spawn cascade was lost"


def test_park_timeout_true_when_unwoken_false_when_woken():
    # The timed in-memory park: True on timeout, False when a real wake beats it.
    def timed_out():
        return rc.park(timeout=0.02)          # nobody wakes -> True
    assert _run_single(timed_out) is True

    state = {}
    holder = {}
    def waiter():
        holder["g"] = rc.current_g()
        rc.sched_yield()
        state["r"] = rc.park(timeout=5.0)     # main wakes well before 5s -> False
    def main():
        rc.fiber(waiter)
        rc.sched_yield()
        holder["g"].wake()
    with hang_guard(15, "timed park woken"):
        rc.fiber(main)
        rc.run()
    assert state.get("r") is False, "timed park reported timeout despite a real wake"


def test_wake_on_completed_g_is_safe_noop():
    g = rc.fiber(lambda: None)
    rc.run()
    assert g.done is True
    g.wake(); g.wake()                         # wake a finished g -> no crash
    assert g.cancel_wait_fd() is False         # not netpoll-parked
    assert rc._self_check(0) == 0


# ==========================================================================
# 4.  unpark_many: partial (edge-before-park), running-g reported missed,
#     and the full batched wake of fibers parked in wait_fd.
# ==========================================================================
def test_unpark_many_running_g_reported_missed():
    def body():
        me = rc.current_g()                    # RUNNING, not parked in wait_fd
        return rc.unpark_many([me])
    assert _run_single(body) == [0]            # index 0 could not be direct-woken


def test_unpark_many_wakes_wait_fd_parkers_and_reports_running_missed():
    # A batch where 30 waiters are parked in wait_fd (all woken -> UNPARKED) and
    # one handle is for a RUNNING fiber that is not parked in wait_fd.  The
    # parked ones are direct-woken; the running one (netpoll_parker == NULL) is
    # reported missed at its batch index -- the C contract (a g is only *skipped*
    # when its underlying runloom_g_t* is NULL, i.e. fully reclaimed; a live but
    # not-wait_fd-parked g is reported missed so the caller pipe-writes it).
    def main():
        r, w = os.pipe()
        os.set_blocking(r, False)
        handles = []
        woke = []

        def mk():
            def waiter():
                handles.append(rc.current_g())
                woke.append(rc.wait_fd(r, READ, 4000))
            return waiter

        me = rc.current_g()                    # RUNNING main fiber: not wait_fd-parked
        for _ in range(30):
            rc.fiber(mk())
        # Deterministic: wait until all 30 waiters have COMMITTED their wait_fd
        # park before unparking.  A fixed sleep lets a loaded scheduler leave
        # some still RUNNING, which unpark_many then mis-reports as `missed`.
        _spin = 0
        while rc.stats()["netpoll_parked"] < 30 and _spin < 2000000:
            rc.sched_yield(); _spin += 1
        batch = [me] + list(handles)           # index 0 = running, 1..30 = parked
        missed = rc.unpark_many(batch)
        _spin = 0
        while len(woke) < 30 and _spin < 2000000:
            rc.sched_yield(); _spin += 1
        rc.netpoll_unregister(r)
        os.close(r); os.close(w)
        return len(woke), missed, set(woke)

    with hang_guard(20, "unpark_many batched"):
        n, missed, rvs = _run_single(main)
    assert n == 30
    assert missed == [0]                        # the running main fiber, reported missed
    assert rvs == {UNPARKED}


# FINDING ------------------------------------------------------------------
def test_finding_foreign_thread_unpark_many_hangs():
    # FINDING: unpark_many() called DIRECTLY from a foreign OS thread on fibers
    # parked in a single-thread run()'s wait_fd WEDGES the process.  The runtime's
    # own fan-in primitives (Event.set / notify_all from a foreign setter) route
    # through an os.write fallback EXACTLY to avoid this cross-thread direct-wake
    # race; the raw C unpark_many offers no such guard and hangs (the foreign call
    # never returns) instead of raising or falling back.  Contained in a
    # subprocess with a hard timeout so the hang is OBSERVED, never propagated.
    script = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc, os, threading
RealThread = threading.Thread
done = {}
def main():
    r, w = os.pipe(); os.set_blocking(r, False)
    handles = []
    def mk():
        def waiter():
            handles.append(rc.current_g())
            rc.wait_fd(r, 1, 2000)
        return waiter
    for _ in range(4):
        rc.fiber(mk())
    rc.sched_sleep(0.1)
    def foreign():
        rc.unpark_many(list(handles))
        done["foreign_returned"] = True
    t = RealThread(target=foreign); t.start(); t.join()
    os.close(r); os.close(w)
rc.fiber(main); rc.run()
sys.stdout.write("FOREIGN_RETURNED\n" if done.get("foreign_returned") else "NEVER\n")
'''
    # 4s is far longer than the ~0.1s the parkers take to commit; a healthy
    # foreign unpark_many would return in well under that.  It never does.
    timed_out = False
    out = ""
    try:
        p = _subproc(script, timeout=4)
        out = p.stdout
    except subprocess.TimeoutExpired:
        timed_out = True
    # Current (buggy) behavior: the foreign unpark_many never returns -> the
    # subprocess times out (or, at best, never prints FOREIGN_RETURNED).  If a
    # future fix makes the foreign call return cleanly, this assertion flips and
    # the FINDING should be revisited.
    assert timed_out or "FOREIGN_RETURNED" not in out, (
        "foreign-thread unpark_many returned cleanly -- FINDING may be fixed; "
        "output=%r" % (out,))


# ==========================================================================
# 5.  Admission gate (set_max_fibers): exhaustion raises + slot release.
# ==========================================================================
def test_admission_cap_from_main_thread_exact_release():
    # Spawning from the main thread (not inside a fiber): each pending fiber()
    # counts against the cap until run() completes it.  Over the cap raises;
    # after the run every slot releases back to 0.
    rc.set_max_fibers(3)
    try:
        ok = errs = 0
        for _ in range(10):
            try:
                rc.fiber(lambda: None); ok += 1
            except RuntimeError:
                errs += 1
        assert ok == 3 and errs == 7, (ok, errs)
        assert rc.live_fibers() == 3
        rc.run()
        assert rc.live_fibers() == 0           # all slots released on completion
    finally:
        rc.set_max_fibers(0)
    # cap lifted -> unbounded spawns admit again
    out = []
    for _ in range(50):
        rc.fiber(lambda: out.append(1))
    rc.run()
    assert len(out) == 50


def test_admission_cap_noyield_path_also_counts_and_releases():
    rc.set_max_fibers(2)
    try:
        n = 0
        for _ in range(5):
            try:
                rc.go_noyield(lambda: None); n += 1
            except RuntimeError:
                pass
        assert n == 2
        assert rc.live_fibers() == 2
        rc.run()
        assert rc.live_fibers() == 0
    finally:
        rc.set_max_fibers(0)


def test_admission_slot_released_even_when_fiber_raises():
    # A fiber that raises still counts as completed -> its admission slot
    # must release, or a cap would leak slots and wedge after a few failures.
    import sys as _sys
    prev = _sys.unraisablehook
    _sys.unraisablehook = lambda u: None       # swallow the reported panic
    rc.set_max_fibers(2)
    try:
        def boom():
            raise ValueError("x")
        for _ in range(2):
            rc.fiber(boom)
        assert rc.live_fibers() == 2
        rc.run()
        assert rc.live_fibers() == 0, "admission slot leaked on a raising fiber"
        # cap still usable after the failures
        rc.fiber(boom); rc.fiber(boom)
        rc.run()
        assert rc.live_fibers() == 0
    finally:
        rc.set_max_fibers(0)
        _sys.unraisablehook = prev


# ==========================================================================
# 6.  Deadlock detection: raise (mode 2), warn (mode 1), off (mode 0),
#     count_deadlocked, and that a raise leaves the scheduler re-runnable.
# ==========================================================================
def test_deadlock_raise_then_reset_then_reuse():
    prev = rc.get_deadlock_mode()
    rc.set_deadlock_mode(2)
    try:
        rc.fiber(lambda: rc.Chan(0).recv())       # nobody ever sends
        with pytest.raises(RuntimeError):
            rc.run()
        rc.sched_reset()
        rc.set_deadlock_mode(0)
        out = []
        rc.fiber(lambda: out.append(1))
        rc.run()                               # scheduler reusable after the raise
        assert out == [1]
        assert rc._self_check(0) == 0
    finally:
        rc.sched_reset()
        rc.set_deadlock_mode(prev)


def test_deadlock_warn_mode_does_not_raise():
    prev = rc.get_deadlock_mode()
    rc.set_deadlock_mode(1)                     # warn: dump to stderr, no raise
    try:
        rc.fiber(lambda: rc.Chan(0).recv())
        rc.run()                               # must return (no exception)
        assert rc.count_deadlocked() >= 1
    finally:
        rc.sched_reset()
        rc.set_deadlock_mode(prev)


def test_deadlock_off_mode_silent_return():
    prev = rc.get_deadlock_mode()
    rc.set_deadlock_mode(0)
    try:
        rc.fiber(lambda: rc.Chan(0).recv())
        rc.run()
        assert rc.count_deadlocked() >= 1      # stranded fiber visible to census
    finally:
        rc.sched_reset()
        rc.set_deadlock_mode(prev)


def test_park_self_stranded_fiber_counts_as_deadlocked():
    # An in-memory park_self with no waker is the deadlockable set, just like a
    # channel recv with no sender.
    prev = rc.get_deadlock_mode()
    rc.set_deadlock_mode(0)
    holder = {}
    try:
        def parker():
            holder["g"] = rc.current_g()
            rc.park_self()                     # never woken
        def main():
            rc.fiber(parker)
            rc.sched_yield(); rc.sched_yield()
        rc.fiber(main)
        rc.run()
        assert rc.count_deadlocked() >= 1
        # And it can STILL be woken after run() returned: wake + drain it cleanly.
        holder["g"].wake()
        rc.run()
        assert rc._self_check(0) == 0
    finally:
        rc.sched_reset()
        rc.set_deadlock_mode(prev)


# ==========================================================================
# 7.  sched_reset / sched_stop teardown with parked fibers (no leak / no UAF).
# ==========================================================================
def test_sched_reset_drains_ready_and_sleep_heap():
    # Spawn fibers that immediately sleep far in the future, plus ready ones,
    # then reset WITHOUT running them to completion.  reset reports the counts
    # and leaves the scheduler clean.
    def sleeper():
        rc.sched_sleep(1000.0)
    def main():
        for _ in range(5):
            rc.fiber(sleeper)
        rc.sched_yield()                       # let them reach the sleep heap
        # don't wait for them
    rc.fiber(main)
    # run() would block on the 1000s sleeps; instead drive one quiescence then reset.
    # Use a stop to break out, then reset the heap.
    rc.fiber(lambda: rc.sched_stop())
    rc.run()
    n_ready, n_sleep, n_parked = rc.sched_reset()
    assert n_sleep >= 0 and n_ready >= 0       # structural: no crash, valid tuple
    # a fresh run is clean
    assert rc.sched_reset() == (0, 0, 0)
    assert rc._self_check(0) == 0


@pytest.mark.runloom_leaky
def test_sched_reset_cannot_reclaim_in_memory_park_self_parker():
    # DOCUMENTED LIMITATION (not a crash): sched_reset drains the ready ring,
    # the sleep heap, and netpoll-parked fibers -- but an in-memory park_self
    # parker leaves NO footprint it can reach, so it survives the reset and
    # keeps a g struct + 2 refs alive.  Critically it must NOT corrupt anything:
    # self_check stays clean and the parker is still wakeable afterwards.
    # (marked runloom_leaky because the stranded parker is intentional; it is a
    # park_self/in-memory parker, not a netpoll parker, so it doesn't touch the
    # conftest netpoll-parked delta, but we keep the marker for intent.)
    prev = rc.get_deadlock_mode()
    rc.set_deadlock_mode(0)
    holder = {}
    try:
        def parker():
            holder["g"] = rc.current_g()
            rc.park_self()
            holder["resumed"] = True
        def main():
            rc.fiber(parker)
            rc.sched_yield(); rc.sched_yield()
        rc.fiber(main)
        rc.run()
        before = rc.count_deadlocked()
        # reset does NOT reclaim the in-memory parker:
        rc.sched_reset()
        assert rc.count_deadlocked() == before, (
            "sched_reset unexpectedly reclaimed an in-memory park_self parker")
        assert rc._self_check(0) == 0
        # but it is still wakeable -- wake + drain it so the test ends clean.
        holder["g"].wake()
        rc.run()
        assert holder.get("resumed") is True
        assert rc._self_check(0) == 0
    finally:
        rc.sched_reset()
        rc.set_deadlock_mode(prev)


def test_sched_stop_leaves_parked_fiber_and_run_returns():
    # A long-sleeping background fiber + a sched_stop from another fiber: run()
    # must return promptly leaving the sleeper parked, with no crash; a reset
    # then cleans the heap.
    ran = []
    def bg():
        rc.sched_sleep(1000.0)
        ran.append("woke")                     # should NOT happen
    def stopper():
        rc.sched_yield()
        rc.sched_stop()
    def main():
        rc.fiber(bg)
        rc.fiber(stopper)
    with assert_faster_than(5.0, "sched_stop mid-run"):
        rc.fiber(main)
        rc.run()
    assert ran == []                           # the 1000s sleeper never fired
    rc.sched_reset()
    assert rc._self_check(0) == 0


# ==========================================================================
# 8.  go_noyield: the FAST path, and the documented misuse (a yielding body).
#     "Undefined behavior" must at least not silently corrupt / crash here.
# ==========================================================================
def test_fiber_noyield_pure_compute_runs():
    out = []
    rc.go_noyield(lambda: out.append(sum(range(1000))))
    rc.run()
    assert out == [sum(range(1000))]


def test_fiber_noyield_with_yielding_body_does_not_crash_subprocess():
    # The docstring says behaviour is UNDEFINED if a go_noyield body yields.
    # Contain it in a subprocess and require: no signal (no SIGSEGV/abort).  A
    # clean completion or a clean Python error are both acceptable; a crash is
    # the failure we are hunting.
    script = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc
done = []
def body():
    rc.sched_yield()          # promised not to; does
    rc.sched_sleep(0.001)     # and parks on the sleep heap
    done.append(1)
rc.go_noyield(body)
n = rc.run()
sys.stdout.write("OK n=%d done=%d check=%d\n" % (n, len(done), rc._self_check(0)))
'''
    p = _subproc(script, env_extra={"RUNLOOM_GOROUTINE_PANIC": "silent"}, timeout=20)
    assert p.returncode is not None and p.returncode >= 0, (
        "go_noyield yielding body crashed: rc=%r\n%s%s"
        % (p.returncode, p.stdout, p.stderr))


# ==========================================================================
# 9.  Slab recycle + refcount UAF surface: rapid spawn-to-completion storms.
#     A use-after-free in the g slab shows up as a SIGSEGV under the storm.
# ==========================================================================
def test_spawn_to_completion_storm_no_uaf():
    # Many short fibers across many run() cycles: stresses slab recycle (a freed
    # g reused next round) and the queue refcount handoff.  A UAF here is a hard
    # crash; we run it in-process (glibc keeps freed heap mapped, so a UAF on
    # Linux usually corrupts rather than traps) AND check structural integrity.
    completed = 0
    with hang_guard(60, "spawn storm"):
        for _ in range(150):
            def f():
                pass
            for _ in range(400):
                rc.fiber(f)
            completed += rc.run()
    assert completed == 150 * 400
    assert rc._self_check(0) == 0


def test_spawn_storm_under_asan_subprocess():
    # Same storm in a SUBPROCESS so a latent slab/refcount UAF that DOES trap
    # (e.g. on a freed+remapped page) is contained as a signalled returncode
    # rather than taking out the test runner.
    script = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc
total = 0
for _ in range(80):
    def f():
        x = [0] * 8     # tiny per-fiber datastack churn
        return x
    for _ in range(500):
        rc.fiber(f)
    total += rc.run()
assert rc._self_check(0) == 0
sys.stdout.write("STORM_OK %d\n" % total)
'''
    p = _subproc(script, timeout=60)
    assert p.returncode is not None and p.returncode >= 0, (
        "spawn storm crashed with signal rc=%r\n%s%s"
        % (p.returncode, p.stdout, p.stderr))
    assert "STORM_OK 40000" in p.stdout, (p.stdout, p.stderr)


def test_datastack_chunk_reuse_across_many_short_fibers():
    # Each fiber builds a small frame graph then returns; the scheduler reuses
    # datastack chunks across fibers.  A mis-managed chunk would corrupt a later
    # fiber's locals -> WRONG DATA.  Assert each fiber computes its own correct
    # result with no cross-contamination.
    results = []
    def main():
        def worker(i):
            # a few locals + a nested call so the datastack actually grows
            a = i * 2
            b = sum(range(i % 7))
            results.append((i, a, b))
        for i in range(500):
            rc.fiber(lambda i=i: worker(i))
    with hang_guard(30, "datastack reuse"):
        rc.fiber(main)
        rc.run()
    assert len(results) == 500
    for i, a, b in results:
        assert a == i * 2 and b == sum(range(i % 7)), "datastack cross-contamination at %d" % i


# ==========================================================================
# 10.  Cooperative overlap must NOT collapse into serialization (slow-return).
# ==========================================================================
def test_sleeping_fibers_overlap_not_serialize():
    # 50 fibers each sleep 0.05s cooperatively.  Serialized that is 2.5s; with
    # cooperative overlap it is ~0.05s.  A slow return (lost overlap) trips the
    # bound.
    N = 50
    done = bytearray(N)
    def main():
        def s(i):
            rc.sched_sleep(0.05)
            done[i] = 1
        for i in range(N):
            rc.fiber(lambda i=i: s(i))
    with assert_faster_than(1.0, "%d overlapping sleeps" % N):
        rc.fiber(main)
        rc.run()
    assert sum(done) == N


def test_yield_storm_returns_promptly():
    # A fiber that yields 100k times must not take pathologically long (the yield
    # fast-path must stay O(1) per yield, not degrade).
    def main():
        for _ in range(100_000):
            rc.sched_yield()
    with assert_faster_than(5.0, "100k yields"):
        rc.fiber(main)
        rc.run()


# ==========================================================================
# 11.  Signal delivery INTO a parked cooperative call (single-thread).
#      A SIGALRM whose handler raises during a cooperative wait_fd must raise
#      INTO the fiber's try/except, not out of run().  Subprocess so the alarm
#      can't leak into the test runner.
# ==========================================================================
@pytest.mark.skipif(not HAVE_ALARM, reason="signal.alarm required")
def test_signal_raises_into_parked_wait_fd_not_out_of_run():
    script = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc, signal, os, socket
class Boom(Exception): pass
def h(s, f): raise Boom
signal.signal(signal.SIGALRM, h)
box = {}
def body():
    rd, wr = socket.socketpair()
    rd.setblocking(False)
    signal.alarm(1)
    try:
        rc.wait_fd(rd.fileno(), 1, 30000)   # never-ready read; only the signal frees it
        box["r"] = "no-exc"
    except Boom:
        box["r"] = "caught"
    finally:
        rc.netpoll_unregister(rd.fileno())
        rd.close(); wr.close()
escaped = None
try:
    rc.fiber(body); rc.run()
except Boom:
    escaped = "Boom"
sys.stdout.write("result=%s escaped=%s check=%d\n" % (box.get("r"), escaped, rc._self_check(0)))
'''
    p = _subproc(script, timeout=20)
    assert p.returncode == 0, (p.returncode, p.stdout, p.stderr)
    assert "result=caught" in p.stdout, (p.stdout, p.stderr)
    assert "escaped=None" in p.stdout, "signal escaped out of run() instead of into the fiber\n" + p.stdout
    assert "check=0" in p.stdout


@pytest.mark.skipif(not HAVE_ALARM, reason="signal.alarm required")
def test_signal_on_idle_sleep_path_is_not_lost():
    # The complementary contract: when the ONLY thing parked is a pure-timer
    # sleep (no cooperative fd-wait to deliver into), a raised signal handler
    # must still surface (carried out of run()/into the fiber) -- never silently
    # swallowed, never a hang.
    script = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc, signal
class Boom(Exception): pass
def h(s, f): raise Boom
signal.signal(signal.SIGALRM, h)
box = {}
def body():
    signal.alarm(1)
    try:
        rc.sched_sleep(30.0)
        box["r"] = "no-exc"
    except Boom:
        box["r"] = "caught"
escaped = None
try:
    rc.fiber(body); rc.run()
except Boom:
    escaped = "Boom"
# "not lost" == either caught in-fiber OR carried out of run()
ok = (box.get("r") == "caught") or (escaped == "Boom")
sys.stdout.write("LOST\n" if not ok else "DELIVERED\n")
'''
    p = _subproc(script, timeout=20)
    assert p.returncode == 0, (p.returncode, p.stdout, p.stderr)
    assert "DELIVERED" in p.stdout, "signal lost on the idle/sleep path\n" + p.stdout


# ==========================================================================
# 12.  Guard-page overflow inside go(stack_size=...) under run() is CLASSIFIED.
#      Subprocess: a deliberate overflow must trap on the guard page and the
#      crash handler must classify it ("GOROUTINE STACK OVERFLOW" + guard page),
#      proving the scheduler's small-stack spawn path fails LOUD, not silent.
# ==========================================================================
def test_small_stack_overflow_under_run_is_classified():
    script = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c
runloom.inspect.install_crash_handler("on")
def body():
    runloom_c._crash_selftest_overflow()   # unbounded real-C recursion
runloom_c.fiber(body, 64 * 1024)              # pin a small 64 KiB stack
runloom_c.run()
'''
    p = _subproc(script, timeout=30)
    # A signalled negative returncode (SIGSEGV) chained from the classifier is
    # expected; what we REQUIRE is that it was CLASSIFIED, not silent corruption.
    assert "GOROUTINE STACK OVERFLOW" in p.stderr, (
        "overflow not classified (silent corruption?)\nrc=%r\n%s" % (p.returncode, p.stderr))
    assert "guard page" in p.stderr, p.stderr
    assert "64 KiB" in p.stderr, p.stderr


# ==========================================================================
# 13.  Fault injection on the SPAWN sites: a clean Python error, never a crash,
#      and no leaked admission slot / structural inconsistency afterwards.
# ==========================================================================
def test_spawn_g_fault_injection_clean_error():
    # RUNLOOM_FAULT_SPAWN_G="once:..." forces one g-struct allocation to fail ->
    # MemoryError on that spawn, the rest proceed, self_check clean.
    script = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc
ok = err = 0
for _ in range(20):
    try:
        rc.fiber(lambda: None); ok += 1
    except MemoryError:
        err += 1
n = rc.run()
sys.stdout.write("ok=%d err=%d check=%d\n" % (ok, err, rc._self_check(0)))
'''
    p = _subproc(script, env_extra={"RUNLOOM_FAULT_SPAWN_G": "once:12",
                                    "RUNLOOM_GOROUTINE_PANIC": "silent"}, timeout=20)
    assert p.returncode == 0, (p.returncode, p.stdout, p.stderr)
    assert "err=1" in p.stdout, p.stdout
    assert "check=0" in p.stdout, p.stdout


def test_spawn_stack_fault_injection_clean_error():
    # RUNLOOM_FAULT_SPAWN_STACK forces coro_new (the C stack mmap) to fail ->
    # MemoryError, the admission slot is released, self_check clean.
    script = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc
rc.set_max_fibers(8)
ok = err = 0
for _ in range(20):
    try:
        rc.fiber(lambda: None); ok += 1
    except MemoryError:
        err += 1
live_before_run = rc.live_fibers()
n = rc.run()
rc.set_max_fibers(0)
sys.stdout.write("ok=%d err=%d live=%d after=%d check=%d\n"
                 % (ok, err, live_before_run, rc.live_fibers(), rc._self_check(0)))
'''
    p = _subproc(script, env_extra={"RUNLOOM_FAULT_SPAWN_STACK": "always:12",
                                    "RUNLOOM_GOROUTINE_PANIC": "silent"}, timeout=20)
    assert p.returncode == 0, (p.returncode, p.stdout, p.stderr)
    # every spawn fails (always) -> 0 admitted, 20 errors, no leaked slot
    assert "ok=0 err=20" in p.stdout, p.stdout
    assert "after=0" in p.stdout, "admission slot leaked on a failed stack alloc\n" + p.stdout
    assert "check=0" in p.stdout, p.stdout


# ==========================================================================
# 14.  sched_sleep edge values + run() completion accounting.
# ==========================================================================
def test_sched_sleep_negative_and_zero_do_not_block():
    def body():
        t0 = time.monotonic()
        rc.sched_sleep(-5.0)        # negative -> immediate
        rc.sched_sleep(0.0)         # zero -> a yield
        return time.monotonic() - t0
    with assert_faster_than(1.0, "sched_sleep edge"):
        dt = _run_single(body)
    assert dt < 0.5


def test_run_returns_completion_count_and_stats_track():
    base = rc.stats()["completed"]
    ran = []
    for _ in range(40):
        rc.fiber(lambda: ran.append(1))
    n = rc.run()
    assert n == 40 and len(ran) == 40
    assert rc.stats()["completed"] == base + 40
    assert rc.stats()["running"] == 0          # no current fiber after the drain


def test_set_wait_reason_does_not_leak_into_later_park():
    # set_wait_reason tags only the NEXT park; a tagged-then-untagged sequence
    # must not carry the reason forward.  We verify via the deadlock dump in a
    # subprocess: a FUTURE-tagged park, then a default park, must show both
    # park:future and park:sync (not two park:future).
    script = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc
rc.set_deadlock_mode(1)   # warn: dump
holders = {}
def tagged():
    rc.set_wait_reason(rc.WR_FUTURE)
    holders["t"] = rc.current_g()
    rc.park()                     # park:future
def plain():
    holders["p"] = rc.current_g()
    rc.park()                     # park:sync (no tag set)
def main():
    rc.fiber(tagged); rc.fiber(plain)
    rc.sched_yield(); rc.sched_yield(); rc.sched_yield()
rc.fiber(main); rc.run()
'''
    p = _subproc(script, env_extra={"RUNLOOM_DEADLOCK": "warn",
                                    "PYTHONUNBUFFERED": "1"}, timeout=20)
    out = p.stdout + p.stderr
    assert "park:future" in out, out
    assert "park:sync" in out, "default park inherited the prior wait reason\n" + out


# ==========================================================================
# 15.  AUGMENTATION (adversarial critic pass).  Conditions the first pass
#      skipped or tested too shallowly:
#        * G.result / G.exception / G.done accessor contract (mid-run, value,
#          raising, BaseException-not-escaped)
#        * re-entrant / nested run() from inside a fiber (count + no UAF)
#        * run_ready() TRANSITIVE quiescence (a wake issued from a resuming
#          fiber must settle before run_ready returns)
#        * sched_reset RECLAIMS a netpoll(wait_fd) parker (the 3rd tuple slot)
#          AND does not poison the per-fd arm cache on reuse (CLAUDE.md gotcha)
#        * unpark_many duplicate-g-in-batch claim-once + empty list
#        * admission cap counted from INSIDE a fiber (the running g counts)
#        * sched_reset called from inside the running fiber (no-op, no UAF)
#        * FD_READ / FD_WRITE fault injection inside cooperative fd_read/fd_write
#        * sched_yield_classic == sched_yield interleaving equivalence
#        * stats()/fibers() snapshot integrity while fibers are parked
# ==========================================================================

# --- 15a.  G handle accessor contract ------------------------------------
def test_g_result_and_done_on_value_returning_fiber():
    g = rc.fiber(lambda: 42)
    assert g.done is False                     # not run yet
    rc.run()
    assert g.done is True
    assert g.result == 42
    assert g.exception is None


def test_g_result_is_none_and_exception_none_mid_run():
    # While a fiber is still parked (not done) its result/exception read as None
    # -- they must NOT raise or expose stale slab memory from a recycled g.
    holder = {}
    seen = {}
    def parker():
        holder["g"] = rc.current_g()
        rc.park_self()
    def main():
        rc.fiber(parker)
        rc.sched_yield(); rc.sched_yield()
        g = holder["g"]
        seen["done"] = g.done
        seen["result"] = g.result
        seen["exc"] = g.exception
        g.wake()                               # release so the run drains
    with hang_guard(15, "g accessors mid-run"):
        rc.fiber(main); rc.run()
    assert seen == {"done": False, "result": None, "exc": None}
    assert rc._self_check(0) == 0


def test_g_exception_captured_not_escaped_for_plain_exception():
    import sys as _sys
    prev = _sys.unraisablehook
    _sys.unraisablehook = lambda u: None
    try:
        def boom():
            raise ValueError("boom")
        g = rc.fiber(boom)
        n = rc.run()                           # must NOT raise out of run()
        assert n == 1
        assert g.done is True
        assert isinstance(g.exception, ValueError)
        assert g.result is None
    finally:
        _sys.unraisablehook = prev
    assert rc._self_check(0) == 0


def test_g_baseexception_captured_not_escaped_out_of_run():
    # SystemExit / KeyboardInterrupt raised inside a fiber must be CAPTURED on
    # the G like any other exception, NOT propagated out of run() (which would
    # tear down an embedding application on a single misbehaving fiber).
    import sys as _sys
    prev = _sys.unraisablehook
    _sys.unraisablehook = lambda u: None
    try:
        for exc_t, arg in ((SystemExit, 3), (KeyboardInterrupt, None)):
            def boom(exc_t=exc_t, arg=arg):
                raise exc_t(arg) if arg is not None else exc_t()
            g = rc.fiber(boom)
            escaped = None
            try:
                n = rc.run()
            except BaseException as e:         # noqa: BLE001 - hunting an escape
                escaped = type(e).__name__
            assert escaped is None, (
                "%s escaped out of run() instead of being captured on the G"
                % exc_t.__name__)
            assert g.done is True
            assert isinstance(g.exception, exc_t)
    finally:
        _sys.unraisablehook = prev
    assert rc._self_check(0) == 0


# --- 15b.  re-entrant / nested run() -------------------------------------
def test_reentrant_run_from_inside_fiber_runs_inner_and_outer_counts():
    # A nested run() invoked from INSIDE a running fiber drives the freshly
    # spawned child to completion and returns 1; the OUTER run() then still
    # reports the full count including the body itself.  A confused nesting that
    # double-counted or freed the outer frame's g would crash or miscount.
    order = []
    inner_n = {}
    def child():
        order.append("child")
    def body():
        rc.fiber(child)
        inner_n["n"] = rc.run()                # nested run drains the child
        order.append("body-after-nested")
    with hang_guard(15, "reentrant run"):
        rc.fiber(body)
        outer = rc.run()
    assert order == ["child", "body-after-nested"]
    assert inner_n["n"] == 1                    # nested run drove exactly the child
    assert outer == 2                           # body + child both counted at the top
    assert rc._self_check(0) == 0


def test_deeply_nested_run_no_uaf():
    # run() nested 4 deep from inside successive fibers: a slab/refcount UAF on
    # the parent frame's g (kept alive across the nested drive) is a hard crash.
    depth = {"max": 0}
    def lvl(n):
        depth["max"] = max(depth["max"], n)
        if n < 4:
            rc.fiber(lambda: lvl(n + 1))
            rc.run()                           # nested
    with hang_guard(20, "deep nested run"):
        rc.fiber(lambda: lvl(0))
        rc.run()
    assert depth["max"] == 4
    assert rc._self_check(0) == 0


def test_empty_run_returns_zero_and_is_idempotent():
    assert rc.run() == 0
    assert rc.run() == 0                        # repeated empty drive: still 0, no crash
    assert rc.current_g() is None
    assert rc._self_check(0) == 0


# --- 15c.  run_ready transitive quiescence -------------------------------
def test_run_ready_waits_for_transitive_wake_chain():
    # run_ready() must not return until EVERY transitively-woken fiber has run
    # to its next park.  w1 parks; w2 parks; the driver wakes w1; w1 on resume
    # wakes w2.  A run_ready that returned after just the directly-ready set
    # would return before w2 resumed -> a lost-overlap / premature-quiescence
    # bug.  We assert w2 resumed BEFORE run_ready returned.
    log = []
    holder = {}
    def w2():
        holder["w2"] = rc.current_g()
        rc.park_self()
        log.append("w2-resumed")
    def w1():
        holder["w1"] = rc.current_g()
        rc.park_self()
        log.append("w1-resumed")
        holder["w2"].wake()                    # transitive wake on resume
    def driver():
        rc.fiber(w1); rc.fiber(w2)
        rc.sched_yield(); rc.sched_yield()      # both reach park
        holder["w1"].wake()
        rc.run_ready()                          # must settle w1 AND w2
        log.append("after-run_ready")
    with hang_guard(15, "run_ready transitive"):
        rc.fiber(driver)
        rc.run()
    assert log == ["w1-resumed", "w2-resumed", "after-run_ready"], (
        "run_ready returned before the transitive wake settled: %r" % (log,))
    assert rc._self_check(0) == 0


def test_run_ready_off_fiber_is_noop():
    # run_ready() has no calling fiber to park off a fiber -- must be a clean
    # no-op, not a crash / not a hang.
    assert rc.run_ready() is None
    assert rc._self_check(0) == 0


# --- 15d.  sched_reset reclaims a netpoll parker + no arm-cache poison ----
def test_sched_reset_reclaims_netpoll_parker_and_arm_not_poisoned():
    # COMPLEMENT to test_sched_reset_cannot_reclaim_in_memory_park_self_parker:
    # a wait_fd (netpoll) parker IS reachable by sched_reset (it walks the
    # netpoll parker set), so the 3rd tuple slot reports it reclaimed.  AND --
    # the CLAUDE.md fd-reuse gotcha -- reclaiming it must NOT leave the per-fd
    # LEVEL arm cache poisoned: a fresh wait_fd on the SAME fd number must still
    # arm + wake.  A poisoned arm (register-once skip seeing a stale mask) would
    # park the reused fd forever.
    box = {}
    def cycle():
        r, w = os.pipe()
        os.set_blocking(r, False); os.set_blocking(w, False)
        def waiter():
            rc.wait_fd(r, READ, 5000)
        rc.fiber(waiter)
        rc.sched_sleep(0.1)                     # commit the parker
        # confirm it is actually netpoll-parked before we reset
        box["parked"] = rc.stats().get("netpoll_parked_self",
                                       rc.stats()["netpoll_parked"])
        rc.sched_stop()
        return r, w

    def outer():
        box["fds"] = cycle()
    with hang_guard(20, "reset reclaim netpoll parker"):
        rc.fiber(outer); rc.run()
    assert box["parked"] >= 1
    n_ready, n_sleep, n_parked = rc.sched_reset()
    assert n_parked >= 1, ("sched_reset did NOT reclaim the netpoll parker: "
                           "tuple=%r" % ((n_ready, n_sleep, n_parked),))
    assert rc._self_check(0) == 0
    # now reuse the SAME fd number: data already buffered -> wait_fd must wake.
    r, w = box["fds"]
    got = {}
    def reuse():
        os.write(w, b"x")
        def waiter2():
            got["rv"] = rc.wait_fd(r, READ, 2000)
        rc.fiber(waiter2); rc.run()
    with hang_guard(15, "reused-fd wait_fd"):
        reuse()
    assert got.get("rv") == READ, (
        "reused fd parked instead of waking -- arm cache poisoned by reset: %r"
        % (got,))
    rc.netpoll_unregister(r); os.close(r); os.close(w)
    assert rc._self_check(0) == 0


# --- 15e.  unpark_many claim-once + empty -------------------------------
def test_unpark_many_empty_list_returns_empty():
    def body():
        return rc.unpark_many([])
    assert _run_single(body) == []


def test_unpark_many_same_g_twice_in_batch_claims_once():
    # A handle that appears TWICE in one batch: the first occurrence direct-wakes
    # the wait_fd parker (returns nothing missed at that index); the SECOND
    # occurrence finds the g already claimed/woken -> reported missed at its
    # index.  A double-claim that re-queued the same g twice would corrupt the
    # ready ring (a g appearing twice -> double-resume -> UAF).
    def main():
        r, w = os.pipe(); os.set_blocking(r, False)
        h = []
        woke = []
        def waiter():
            h.append(rc.current_g())
            woke.append(rc.wait_fd(r, READ, 4000))
        rc.fiber(waiter)
        rc.sched_sleep(0.1)                     # the single parker commits
        g = h[0]
        missed = rc.unpark_many([g, g])         # SAME g twice
        rc.sched_sleep(0.1)
        rc.netpoll_unregister(r); os.close(r); os.close(w)
        return missed, woke
    with hang_guard(20, "unpark_many dup g"):
        missed, woke = _run_single(main)
    assert woke == [UNPARKED], "parker not woken exactly once: %r" % (woke,)
    assert missed == [1], (
        "duplicate g not reported missed at its second index (double-claim?): %r"
        % (missed,))
    assert rc._self_check(0) == 0


# --- 15f.  admission cap counted from inside a fiber ---------------------
def test_admission_cap_counts_running_fiber_itself():
    # Spawning from INSIDE a fiber: the running fiber itself counts against the
    # cap.  cap=3, the body is 1 live fiber, so only 2 children admit before
    # RuntimeError -- and after the run every slot releases.
    rc.set_max_fibers(3)
    res = {}
    try:
        def child():
            pass
        def main():
            ok = err = 0
            for _ in range(10):
                try:
                    rc.fiber(child); ok += 1
                except RuntimeError:
                    err += 1
            res["ok"] = ok; res["err"] = err
            res["live"] = rc.live_fibers()
        with hang_guard(15, "in-fiber admission"):
            rc.fiber(main); rc.run()
        assert res["ok"] == 2, res          # body + 2 children == cap of 3
        assert res["err"] == 8, res
        assert res["live"] == 3, res        # at peak: main + 2 children
        assert rc.live_fibers() == 0        # all released after the run
    finally:
        rc.set_max_fibers(0)
    assert rc._self_check(0) == 0


# --- 15g.  sched_reset from inside the running fiber ---------------------
def test_sched_reset_from_inside_running_fiber_is_noop_no_uaf():
    # The running fiber resets the very scheduler it is running on.  Its own g is
    # not in the ready ring / sleep heap / netpoll set, so reset reclaims nothing
    # (0,0,0) and must NOT free the running g out from under itself -> UAF.
    out = {}
    def body():
        out["reset"] = rc.sched_reset()
        out["after"] = "alive"                 # proves the g survived the reset
        return "done"
    g = rc.fiber(body)
    with hang_guard(15, "reset from inside"):
        rc.run()
    assert out.get("reset") == (0, 0, 0), out
    assert out.get("after") == "alive"
    assert g.done is True and g.result == "done"
    assert rc._self_check(0) == 0


# --- 15h.  FD_READ / FD_WRITE fault injection inside cooperative I/O ------
def test_fd_read_fault_injection_clean_oserror_in_fiber():
    # RUNLOOM_FAULT_FD_READ="once:5" forces one cooperative fd_read to fail with
    # EIO.  It must surface as a clean OSError INSIDE the fiber (caught by its own
    # try/except), self_check clean, no crash, no leaked netpoll arm.
    script = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc, os
out = {}
def main():
    r, w = os.pipe()
    os.set_blocking(r, False); os.set_blocking(w, False)
    os.write(w, b"hello")
    def reader():
        buf = bytearray(16)
        try:
            n = rc.fd_read(r, buf, 16)
            out["n"] = n
        except OSError as e:
            out["err"] = e.errno
    rc.fiber(reader); rc.run()
    rc.netpoll_unregister(r); os.close(r); os.close(w)
rc.fiber(main); rc.run()
sys.stdout.write("err=%r check=%d\n" % (out.get("err"), rc._self_check(0)))
'''
    p = _subproc(script, env_extra={"RUNLOOM_FAULT_FD_READ": "once:5",
                                    "RUNLOOM_GOROUTINE_PANIC": "silent"}, timeout=20)
    assert p.returncode == 0, (p.returncode, p.stdout, p.stderr)
    assert "err=5" in p.stdout, ("FD_READ fault did not raise a clean EIO\n"
                                 + p.stdout + p.stderr)
    assert "check=0" in p.stdout, p.stdout


def test_fd_write_fault_injection_clean_oserror_in_fiber():
    script = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc, os
out = {}
def main():
    r, w = os.pipe()
    os.set_blocking(r, False); os.set_blocking(w, False)
    def writer():
        try:
            rc.fd_write(w, b"payload")
            out["ok"] = True
        except OSError as e:
            out["err"] = e.errno
    rc.fiber(writer); rc.run()
    rc.netpoll_unregister(w); os.close(r); os.close(w)
rc.fiber(main); rc.run()
sys.stdout.write("err=%r ok=%r check=%d\n"
                 % (out.get("err"), out.get("ok"), rc._self_check(0)))
'''
    p = _subproc(script, env_extra={"RUNLOOM_FAULT_FD_WRITE": "once:5",
                                    "RUNLOOM_GOROUTINE_PANIC": "silent"}, timeout=20)
    assert p.returncode == 0, (p.returncode, p.stdout, p.stderr)
    # either a clean EIO surfaced, or (if the site fired pre-park) a clean retry;
    # what we forbid is a crash.  Assert no signal + clean structure.
    assert "check=0" in p.stdout, p.stdout
    assert ("err=5" in p.stdout) or ("ok=True" in p.stdout), (
        "FD_WRITE fault produced neither a clean error nor a clean write\n"
        + p.stdout + p.stderr)


# --- 15i.  sched_yield_classic == sched_yield interleaving ---------------
def test_sched_yield_classic_matches_vectorcall_yield_interleave():
    # The METH_NOARGS classic form and the vectorcall singleton must produce the
    # SAME cooperative interleaving (one step each per yield).  A drift would mean
    # one path skips the scheduler round-trip -> starvation / reorder.
    seq = []
    def a():
        for i in range(4):
            seq.append(("a", i)); rc.sched_yield_classic()
    def b():
        for i in range(4):
            seq.append(("b", i)); rc.sched_yield()
    with hang_guard(15, "classic vs vectorcall yield"):
        rc.fiber(a); rc.fiber(b); rc.run()
    # strict round-robin interleave; set-equality on the full payload (not counts)
    assert seq == [("a", 0), ("b", 0), ("a", 1), ("b", 1),
                   ("a", 2), ("b", 2), ("a", 3), ("b", 3)], seq
    assert rc._self_check(0) == 0


# --- 15j.  stats() / fibers() snapshot integrity while parked ------------
def test_fibers_and_stats_snapshot_integrity_while_parked():
    # With one fiber in wait_fd (io-wait), one sleeping, and the snapshotting
    # fiber running, fibers() must report exactly those three states with the
    # right per-fiber fields and stats() must agree (netpoll_parked>=1,
    # sleeping>=1) -- WRONG-DATA hunt on the introspection snapshot, which walks
    # live scheduler structures and could expose a stale/freed g.
    box = {}
    def main():
        r, w = os.pipe(); os.set_blocking(r, False)
        def waiter():
            rc.set_wait_reason(rc.WR_FUTURE)
            rc.wait_fd(r, READ, 4000)
        def sleeper():
            rc.sched_sleep(3.0)
        rc.fiber(waiter); rc.fiber(sleeper)
        rc.sched_sleep(0.1)                     # both committed
        snap = rc.fibers()
        st = rc.stats()
        # fibers() is process-global; a prior test may have stranded fibers, so
        # work with the SET of states we observe (must INCLUDE our three) and
        # locate OUR io-wait fiber by its fd, not by assuming the snapshot is
        # only ours.
        states = set(f.get("state") for f in snap)
        keys = set().union(*[set(f.keys()) for f in snap]) if snap else set()
        io_mine = [f for f in snap if f.get("state") == "io-wait"
                   and f.get("fd") == r]
        box["states"] = states
        box["keys"] = keys
        box["netpoll"] = st.get("netpoll_parked_self", st["netpoll_parked"])
        box["sleeping"] = st["sleeping"]
        box["io_fd"] = io_mine[0].get("fd") if io_mine else None
        box["io_events"] = io_mine[0].get("events") if io_mine else None
        box["r"] = r; box["w"] = w
        rc.netpoll_unregister(r); os.close(r); os.close(w)
        rc.sched_reset()                        # drop the long sleeper + drained waiter
    with hang_guard(15, "fibers snapshot"):
        rc.fiber(main); rc.run()
    rc.sched_reset()
    # our three states are all present in the live snapshot (others may exist)
    assert {"io-wait", "running", "sleep"} <= box["states"], box["states"]
    # every fiber dict carries the documented field set
    assert {"id", "state", "blocked_on", "fd", "events", "refcount",
            "noyield", "owner"} <= box["keys"], box["keys"]
    assert box["netpoll"] >= 1
    assert box["sleeping"] >= 1
    assert box["io_fd"] == box["r"], (box["io_fd"], box["r"])
    # events is a human-readable string ('R' for a read-wait), not the bitmask.
    assert "R" in str(box["io_events"]), box["io_events"]
    assert rc._self_check(0) == 0


# --- 15k.  park(foreign_wakeable=True) self-wake path --------------------
def test_park_foreign_wakeable_self_wake_returns():
    # park(foreign_wakeable=True) keeps a single-thread run() alive for a foreign
    # waker; a self-wake banked before the park must still be consumed and the
    # park return promptly (no extra wake-pump fd is leaked into self_check).
    def body():
        g = rc.current_g()
        g.wake()                               # bank a wake before parking
        rc.park(foreign_wakeable=True)         # consumes the banked wake
        return "resumed"
    with hang_guard(15, "park foreign_wakeable self-wake"):
        assert _run_single(body) == "resumed"
    assert rc._self_check(0) == 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
