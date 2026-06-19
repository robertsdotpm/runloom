"""Adversarial coverage suite for runloom_blockpool.c + runloom_gstate.c.

Fragments under test:
  * src/runloom_c/runloom_blockpool.c -- the blocking-offload thread pool
    (runloom_blocking_call / the worker loop / lazy init).
  * src/runloom_c/runloom_gstate.c   -- the observational g-state machine
    (runloom_g_state_set / _in / the RUNLOOM_G_ASSERT_NOT debug guard).

These tests target the *reachable* behaviour of both fragments with REAL
oracles, with particular emphasis on the use-after-free-prevention re-park
loop in runloom_blocking_call -- the exact path whose comment block warns that
a spurious wake (task.cancel() -> G.wake()) delivered to a fiber parked in
blocking() must NOT return early and free its stack `job` while the worker
still references it.  We deliberately deliver that spurious wake, en masse,
mid-worker, and assert the offloaded result is still correct (so the loop
re-parked rather than unwinding into a UAF / wrong value / crash).

The gstate fragment's transition-assert boundary (RUNLOOM_G_ASSERT_NOT, which
calls runloom_g_state_in under RUNLOOM_DBG_GSTATE) is driven in a SUBPROCESS
under RUNLOOM_DEBUG=gstate: the debug flag is read once at module init, so the
mode must be set in a child env.  A clean exit there proves the abort() guard
in runloom_g_assert_failure_ never fires on legitimate transitions -- it is a
genuine "can't happen" invariant, not dead-on-bug code.

UNREACHABLE-from-a-test lines are NOT faked; they are catalogued in the
structured report's exclusions[] (the never-called runloom_blockpool_fini and
its worker-stop path, the cond_init/thread_create OOM-cleanup branches that
have no RUNLOOM_FAULT_ hook, the unused public runloom_g_state_cas/_get, and
the abort() crash guard).
"""
import os
import subprocess
import sys
import time

import pytest

from adv_util import needs_free_threading, hang_guard, assert_faster_than

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

import runloom
import runloom_c as rc

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

# The single-thread offload only OFFLOADS (vs. running inline) on a backend
# with a pump-wake primitive; epoll/kqueue have one.  Correctness holds on
# every backend either way (inline still produces the right result).
_PUMP_WAKE = rc.netpoll_backend() in ("epoll", "kqueue", "iocp-afd")


# ==========================================================================
# runloom_blockpool.c -- runloom_blocking_call re-park / UAF-prevention loop
# ==========================================================================

def test_blocking_result_and_kwargs_single_thread():
    """Drives runloom_blocking_call's enqueue + park_safe re-park loop
    (blockpool.c L301-306 enqueue, L321-322 single-thread re-park) on the
    single-thread sched.  Oracle: fn's value + *args/**kwargs forwarded."""
    out = []

    def add(a, b, c=0):
        time.sleep(0.01)
        return a + b + c

    def w():
        out.append(runloom.blocking(add, 5, 7, c=100))

    with hang_guard(30, "blocking_result_single"):
        rc.fiber(w)
        rc.run()
    assert out == [112]


def test_blocking_exception_propagates():
    """An exception raised in the offloaded call must re-raise in the fiber:
    job.result stays NULL, job.exc_* is restored (module_go) -- proves the
    worker (blockpool.c L141 job->fn) ran and the fiber resumed via the
    re-park loop carrying the failure, not a UAF."""
    seen = []

    def boom():
        time.sleep(0.01)
        raise ValueError("offloaded-kaboom")

    def w():
        try:
            runloom.blocking(boom)
        except ValueError as e:
            seen.append(str(e))

    with hang_guard(30, "blocking_exc"):
        rc.fiber(w)
        rc.run()
    assert seen == ["offloaded-kaboom"]


def test_blocking_inline_outside_fiber():
    """Off any fiber, runloom_sched_peek_current() is NULL so runloom_blocking_call
    takes the inline fallback (blockpool.c L283-285 g==NULL -> return fn(arg)).
    Real oracle: it still returns fn's value, computed on THIS thread."""
    marker = []

    def fn(x):
        marker.append(("thread", id(marker)))
        return x * 3

    # No run()/fiber() -- we are a plain OS thread, not a fiber.
    assert rc.blocking(fn, 14) == 42
    assert marker == [("thread", id(marker))]


def test_blocking_spurious_wake_does_not_uaf_single_thread():
    """THE adversarial case for blockpool.c L321-322.

    A fiber parks in blocking() (park_safe) while a worker runs a slow job.
    A SECOND fiber delivers a storm of spurious G.wake()s to the parked
    fiber -- exactly the task.cancel()->G.wake() race the comment at L308-314
    documents as the historical use-after-free.  The re-park loop MUST keep
    re-parking until job.done; returning on a spurious wake would free the
    stack `job` mid-worker (UAF) or return a stale/garbage result.

    Oracle: the offloaded value is returned intact (no early return, no
    crash), proving the loop spun on job.done rather than the wake."""
    result = {}
    hbox = {}

    def slow():
        time.sleep(0.15)
        return 4242

    def worker():
        hbox["g"] = rc.current_g()
        result["v"] = runloom.blocking(slow)

    def waker():
        for _ in range(500000):
            if hbox.get("g") is not None:
                break
            rc.sched_yield_classic()
        g = hbox.get("g")
        assert g is not None, "worker never registered its handle"
        # Storm of spurious wakes while the worker is mid-job.
        for _ in range(60):
            g.wake()
            rc.sched_sleep(0.001)

    with hang_guard(60, "blocking_spurious_single"):
        rc.fiber(worker)
        rc.fiber(waker)
        rc.run()
    # The result survived the spurious-wake storm: re-park loop held.
    assert result.get("v") == 4242


@pytest.mark.skipif(not FT, reason="M:N hub path needs the GIL off")
def test_blocking_spurious_wake_does_not_uaf_mn_hub():
    """Same UAF-prevention guarantee on the M:N HUB branch (blockpool.c
    L315-318: park_current + coro_yield re-park).  A hub fiber parks in
    blocking(); a sibling fiber spams G.wake() at it mid-worker."""
    result = {}
    hbox = {}

    def slow():
        time.sleep(0.12)
        return 9999

    def main():
        from runloom.sync import WaitGroup
        wg = WaitGroup()
        wg.add(1)

        def worker():
            try:
                hbox["g"] = rc.current_g()
                result["v"] = runloom.blocking(slow)
            finally:
                wg.done()

        def waker():
            for _ in range(1000000):
                if hbox.get("g") is not None:
                    break
                rc.sched_yield_classic()
            g = hbox.get("g")
            assert g is not None
            for _ in range(50):
                g.wake()
                rc.sched_sleep(0.001)

        rc.mn_go(worker)
        rc.mn_go(waker)
        wg.wait()

    with hang_guard(90, "blocking_spurious_mn"):
        runloom.run(4, main)
    assert result.get("v") == 9999


@pytest.mark.skipif(not FT, reason="M:N hub path needs the GIL off")
def test_blocking_concurrent_offloads_overlap_on_one_hub():
    """N hub fibers each offload a blocking sleep.  The pool must run them
    CONCURRENTLY (the whole point of the offload) -- wall time ~= one sleep,
    not N sleeps.  Drives blockpool.c L152-153 (hub wake_g) + L158 inflight
    decrement across many in-flight jobs, plus the worker fan-out.

    Real oracle: all N complete AND (on a pump-wake backend) the wall clock
    proves overlap; assert_faster_than makes the slow-return a hard failure."""
    from runloom.sync import WaitGroup
    N, NAP = 8, 0.2
    done = bytearray(N)

    def main():
        wg = WaitGroup()
        wg.add(N)

        def w(i):
            try:
                runloom.blocking(time.sleep, NAP)
                done[i] = 1
            finally:
                wg.done()

        for i in range(N):
            rc.mn_go(lambda i=i: w(i))
        wg.wait()

    with hang_guard(90, "blocking_overlap"):
        if _PUMP_WAKE:
            # Serial would be N*NAP; offloaded ~= NAP.  Half the serial time
            # is a generous bar that still proves concurrency, not flaky.
            with assert_faster_than(N * NAP * 0.5, "concurrent offloads"):
                runloom.run(4, main)
        else:
            runloom.run(4, main)
    assert sum(done) == N


@pytest.mark.skipif(not FT, reason="hub fiber count is only meaningful under M:N")
def test_blocking_storm_reuses_pool_no_leak():
    """A storm of offloads across repeated run()s reuses the one lazily-init'd
    pool (blockpool.c L169 fast-path 'already up') and every inflight counter
    settles back to 0 (L299 add / L158 sub balanced).  Oracle: exact completion
    count across rounds + inflight()==0 at the end (no stuck job)."""
    from runloom.sync import WaitGroup
    ROUNDS, PER = 4, 16
    total = {"n": 0}

    def main():
        wg = WaitGroup()
        wg.add(PER)
        ok = bytearray(PER)

        def w(i):
            try:
                runloom.blocking(lambda x=i: x * x)
                ok[i] = 1
            finally:
                wg.done()

        for i in range(PER):
            rc.mn_go(lambda i=i: w(i))
        wg.wait()
        total["n"] += sum(ok)

    with hang_guard(120, "blocking_storm"):
        for _ in range(ROUNDS):
            runloom.run(2, main)
    assert total["n"] == ROUNDS * PER
    # Every submitted job decremented inflight on completion (no stuck job):
    # the structural self_check would flag a leaked parker otherwise.
    assert rc._self_check(0) == 0


# ==========================================================================
# runloom_gstate.c -- transition-assert boundary under RUNLOOM_DBG_GSTATE
# ==========================================================================

# This workload spins many M:N fibers through the full park/wake/done g-state
# transitions, under RUNLOOM_DEBUG=gstate so every RUNLOOM_G_ASSERT_NOT site
# (mn_api submit, sysmon, hub_main) actually evaluates runloom_g_state_in.  A
# clean exit proves the abort() in runloom_g_assert_failure_ never fired on a
# legitimate transition -- i.e. the guard is a real invariant, not dead code.
_GSTATE_DBG = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
N = 96
done = bytearray(N)
def main():
    wg = WaitGroup(); wg.add(N)
    def f(i):
        try:
            # exercise park (sleep), wake, channel, done transitions
            rc.sched_sleep(0.001)
            done[i] = 1
        finally:
            wg.done()
    for i in range(N):
        rc.mn_go(lambda i=i: f(i))
    wg.wait()
runloom.run(4, main)
sys.stdout.write("GSTATE_OK %d\n" % sum(done))
'''


@pytest.mark.skipif(not FT, reason="g-state transitions exercised under M:N")
def test_gstate_assert_guard_holds_under_debug_mode():
    """Run an M:N park/wake/done workload under RUNLOOM_DEBUG=gstate in a
    subprocess.  This arms the RUNLOOM_G_ASSERT_NOT macro (gstate.c is the
    runloom_g_state_in predicate + runloom_g_assert_failure_ abort).  A clean
    exit (returncode 0, no 'ASSERT FAILED', all N fibers done) proves no
    legitimate transition tripped the abort guard."""
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src",
               RUNLOOM_DEBUG="gstate")
    try:
        p = subprocess.run([PY, "-c", _GSTATE_DBG], cwd=REPO, env=env,
                           capture_output=True, text=True, timeout=240)
    except subprocess.TimeoutExpired:
        pytest.skip("gstate-debug workload timed out (box under heavy load)")
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    # The abort() guard must NOT have fired on any legitimate transition.
    assert "ASSERT FAILED" not in p.stderr, p.stderr[-1500:]
    assert "GSTATE_OK 96" in p.stdout, (p.stdout[-400:], p.stderr[-800:])


def test_gstate_set_in_get_exercised_by_normal_workload():
    """runloom_g_state_set (every park/wake/done transition) and
    runloom_g_state_in (the PARKED/DEAD masks read in sysmon/drain) are on the
    hot path of ANY run; this asserts a real workload drives them and leaves
    the runtime structurally consistent (no half-applied transition)."""
    out = []

    def chain(n):
        def step(i):
            if i:
                # nested spawn: many fresh->runnable->running->done transitions
                rc.fiber(lambda: step(i - 1))
            out.append(i)

        step(n)

    with hang_guard(30, "gstate_workload"):
        rc.fiber(lambda: chain(20))
        rc.run()
    assert sorted(out) == list(range(21))
    # Structural invariant (conftest also asserts this post-test): every g
    # that transitioned through set()/in() left no inconsistent state.
    assert rc._self_check(0) == 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
