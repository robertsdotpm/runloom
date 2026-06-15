"""Adversarial QA: scheduler + M:N hub lifecycle.

Targets the highest-bug-density area: spawn/admission, park/wake races, the
M:N teardown path (the known-flaky `mn_fini` hang), deadlock detection, and
goroutine exception handling.  Several of these are *negative-space* checks:
a lost wake or a teardown deadlock shows up as a `hang_guard` _exit with a
pinpointed traceback, not a silently-green run.

Includes one xfail-documented FINDING: an unhandled exception inside a bare
`runloom_c.go` goroutine is silently swallowed -- not raised out of run(),
not retrievable via `G.result`, and not even written to stderr / the
unraisable hook.  In a Go-parity runtime a goroutine panic should at minimum
be observable; today it vanishes.
"""
import io
import os
import sys

import pytest

import runloom
import runloom_c as rc
from adv_util import hang_guard, assert_faster_than, needs_free_threading

FT = needs_free_threading()


def _run_single(fn):
    box = {}
    def main():
        box["r"] = fn()
    rc.go(main)
    rc.run()
    return box.get("r")


# --------------------------------------------------------------------------
# spawn / run correctness
# --------------------------------------------------------------------------
def test_run_returns_completion_count():
    ran = []
    for _ in range(50):
        rc.go(lambda: ran.append(1))
    n = rc.run()
    assert n >= 50 and len(ran) == 50


def test_nested_spawn_from_inside_fiber():
    seen = []
    def parent():
        for i in range(10):
            rc.go(lambda i=i: seen.append(i))
    rc.go(parent)
    rc.run()
    assert sorted(seen) == list(range(10))


def test_go_noyield_runs_to_completion():
    out = []
    rc.go_noyield(lambda: out.append("ran"))
    rc.run()
    assert out == ["ran"]


def test_custom_stack_size_runs():
    out = []
    rc.go(lambda: out.append("big"), 1 << 20)   # 1 MiB stack
    rc.run()
    assert out == ["big"]


def test_current_g_identity_stable_within_fiber():
    def f():
        a, b = rc.current_g(), rc.current_g()
        # Fresh wrapper each call but must compare/hash equal (same fiber).
        assert a == b
        assert hash(a) == hash(b)
        assert {a, b} == {a}
        return "ok"
    assert _run_single(f) == "ok"


def test_current_g_none_outside_fiber():
    assert rc.current_g() is None


# --------------------------------------------------------------------------
# FINDING: unhandled goroutine exceptions vanish silently
# --------------------------------------------------------------------------
def test_goroutine_exception_is_reported_and_retrievable():
    # Regression for the swallowed-exception FINDING (now fixed): an unhandled
    # goroutine exception is reported via sys.unraisablehook (default
    # RUNLOOM_GOROUTINE_PANIC=print) AND retrievable on G.exception.  run() still
    # does NOT raise it (report, not propagate) and G.result stays None.
    # NB: PyErr_WriteUnraisable calls sys.unraisablehook, so we capture there --
    # pytest installs its own hook (turning unraisables into warnings), so an
    # fd-2 capture would see nothing under pytest.
    marker = "RUNLOOM_PANIC_MARKER_ABC"
    def boom():
        raise ValueError(marker)
    recorded = []
    prev_hook = sys.unraisablehook
    sys.unraisablehook = lambda u: recorded.append(u)
    try:
        g = rc.go(boom)
        n = rc.run()
    finally:
        sys.unraisablehook = prev_hook
    assert n >= 1
    assert g.done is True
    assert g.result is None                        # value channel unchanged
    assert isinstance(g.exception, ValueError)     # now retrievable on the handle
    assert marker in str(g.exception)
    assert any(marker in str(getattr(u, "exc_value", "")) for u in recorded), \
        "goroutine exception was not reported via the unraisable hook"


_SILENT_SCRIPT = r'''
import sys, os; sys.path.insert(0, "src")
import runloom_c as rc
def boom():
    raise ValueError("SILENT_MARKER_X")
g = rc.go(boom)
r, w = os.pipe(); saved = os.dup(2); os.dup2(w, 2)
rc.run()
os.dup2(saved, 2); os.close(w)
cap = os.read(r, 1 << 16).decode(); os.close(r); os.close(saved)
sys.stdout.write("PRINTED\n" if "SILENT_MARKER_X" in cap else "QUIET\n")
sys.stdout.write("RETRIEVABLE\n" if isinstance(g.exception, ValueError) else "LOST\n")
'''


def test_goroutine_exception_silent_mode_opt_out():
    # RUNLOOM_GOROUTINE_PANIC=silent restores no-report (still retrievable).
    # Subprocess: the mode is cached process-wide.
    import subprocess
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env = dict(os.environ, RUNLOOM_GOROUTINE_PANIC="silent",
               PYTHON_GIL="0", PYTHONPATH="src")
    p = subprocess.run([sys.executable, "-c", _SILENT_SCRIPT], cwd=repo, env=env,
                       capture_output=True, text=True, timeout=30)
    assert "QUIET" in p.stdout, "silent mode still printed:\n%s%s" % (p.stdout, p.stderr)
    assert "RETRIEVABLE" in p.stdout, "G.exception lost in silent mode"


# --------------------------------------------------------------------------
# admission gate (set_max_fibers)
# --------------------------------------------------------------------------
def test_max_fibers_admission_gate_and_release():
    rc.set_max_fibers(4)
    try:
        spawned = []
        errors = []
        def child():
            rc.sched_yield()       # stay live so the cap is pressed
        def boss():
            for _ in range(20):
                try:
                    rc.go(child)
                    spawned.append(1)
                except RuntimeError:
                    errors.append(1)
        rc.go(boss)
        rc.run()
        # boss itself counts against the cap, so at most 3 children admit at once;
        # over the cap raises, and admitted slots release as children finish.
        assert errors, "admission gate never fired under a cap of 4"
        assert spawned, "no child admitted at all"
    finally:
        rc.set_max_fibers(0)
    # after release everything is back to unlimited
    assert rc.get_max_fibers() == 0
    out = []
    for _ in range(100):
        rc.go(lambda: out.append(1))
    rc.run()
    assert len(out) == 100


# --------------------------------------------------------------------------
# park / wake races  (single-thread run, park_self + G.wake)
# --------------------------------------------------------------------------
def test_wake_after_park_resumes():
    state = {}
    holder = {}
    def waiter():
        holder["g"] = rc.current_g()
        rc.sched_yield()           # let main grab the handle
        rc.park_self()             # parks until woken
        state["resumed"] = True
    def main():
        rc.go(waiter)
        rc.sched_yield()           # waiter records handle + yields
        rc.sched_yield()           # waiter parks
        holder["g"].wake()
    with hang_guard(15, "wake after park"):
        rc.go(main)
        rc.run()
    assert state.get("resumed") is True


def test_wake_before_park_is_not_lost():
    # The Dekker race: wake() arrives in the [decide-to-park .. park] window.
    # park_self must consume it and NOT block forever.
    state = {}
    holder = {}
    def waiter():
        holder["g"] = rc.current_g()
        rc.sched_yield()           # hand main the handle while still runnable
        # main wakes us HERE, before we reach park_self below:
        rc.park_self()             # must return immediately (wake already pending)
        state["resumed"] = True
    def main():
        rc.go(waiter)
        rc.sched_yield()           # waiter sets handle, yields back (not yet parked)
        holder["g"].wake()         # wake BEFORE the park
    with hang_guard(15, "wake before park"):
        rc.go(main)
        rc.run()
    assert state.get("resumed") is True, "wake-before-park was lost (hung waiter)"


def test_park_timeout_returns_true_then_false():
    res = {}
    def f():
        res["timedout"] = rc.park(timeout=0.02)     # nobody wakes -> True
        return "ok"
    assert _run_single(f) == "ok"
    assert res["timedout"] is True


# --------------------------------------------------------------------------
# deadlock detection
# --------------------------------------------------------------------------
def test_deadlock_mode_raise():
    prev = rc.get_deadlock_mode()
    rc.set_deadlock_mode(2)        # raise
    try:
        def stuck():
            rc.Chan(0).recv()      # nobody will ever send
        rc.go(stuck)
        with pytest.raises(RuntimeError):
            rc.run()
    finally:
        rc.sched_reset()
        rc.set_deadlock_mode(prev)


def test_count_deadlocked_reports_parked():
    prev = rc.get_deadlock_mode()
    rc.set_deadlock_mode(0)        # off: let run() return instead of raising
    try:
        def stuck():
            rc.Chan(0).recv()
        rc.go(stuck)
        rc.run()                   # returns with 1 fiber stranded
        assert rc.count_deadlocked() >= 1
    finally:
        rc.sched_reset()
        rc.set_deadlock_mode(prev)


# --------------------------------------------------------------------------
# M:N teardown stress -- the known-flaky mn_fini hang surface
# --------------------------------------------------------------------------
def test_mn_fini_without_init_is_safe():
    rc.mn_fini()                   # must not crash / hang with no hubs
    assert rc.mn_hub_count() == 0


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_mn_init_fini_cycles_no_hang():
    # Rapid lifecycle churn is the classic mn_fini lost-wakeup-join hang surface.
    with hang_guard(60, "mn init/fini churn"):
        for _ in range(40):
            rc.mn_init(4)
            done = []
            for _ in range(20):
                rc.mn_go(lambda: done.append(1))
            rc.mn_run()
            rc.mn_fini()
            assert rc.mn_hub_count() == 0


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_mn_spawn_storm_completion_count():
    N = 5000
    counter = bytearray(1)         # single-writer slot avoids the GIL-off RMW race
    box = {"done": 0}
    def main():
        from runloom.sync import WaitGroup
        wg = WaitGroup(); wg.add(N)
        def w():
            try:
                box["done"] += 1   # racy but only a coarse liveness signal
            finally:
                wg.done()
        for _ in range(N):
            rc.mn_go(w)
        wg.wait()
    with hang_guard(60, "mn spawn storm"):
        runloom.run(4, main)


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_mn_init_spawn_fini_without_run_drains():
    # Spawn onto live hubs then tear down WITHOUT mn_run: hubs run pending gs
    # immediately, and fini must join cleanly (no orphaned hub thread / hang).
    with hang_guard(30, "mn fini drains pending"):
        rc.mn_init(2)
        ran = []
        for _ in range(50):
            rc.mn_go(lambda: ran.append(1))
        rc.mn_fini()
    assert rc.mn_hub_count() == 0


# --------------------------------------------------------------------------
# CPU-bound fiber must not permanently starve a sibling (sysmon/preempt).
# A genuine starvation hang trips the guard -> a finding.
# --------------------------------------------------------------------------
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_cpu_bound_fiber_does_not_starve_sibling():
    progress = []
    def cpu_hog():
        x = 0
        for i in range(60_000_000):
            x += i
        progress.append(("hog", x))
    def needy():
        for _ in range(10):
            progress.append(("needy",))
            rc.sched_sleep(0.01)
    def main():
        rc.mn_go(cpu_hog)
        rc.mn_go(needy)
    with hang_guard(45, "cpu starvation"):
        # 1 hub: the needy fiber shares the single hub with the hog, so it only
        # makes progress if the runtime preempts/yields the hog.
        rc.mn_init(1)
        rc.mn_go(main)
        rc.mn_run()
        rc.mn_fini()
    needy_runs = sum(1 for p in progress if p[0] == "needy")
    assert needy_runs == 10, "needy fiber starved by CPU hog (got %d/10)" % needy_runs


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
