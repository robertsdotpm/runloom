"""Adversarial QA swarm: blocking-offload pool + crash/diag/introspection.

Subsystem: blockpool_diag_crash.  C sources under test:
  * runloom_blockpool.c   -- rc.blocking(): offload a callable to a worker pool,
                             park the calling fiber, integrate the wake with both
                             the single-thread sched and the M:N hubs.  The job
                             record lives on the PARKED fiber's C stack and is
                             read by a worker OS thread (LIFECYCLE_INVARIANTS
                             `blockpool_job` -- a documented cross-thread UAF
                             surface closed by the `done` release handshake +
                             the re-park-on-spurious-wake loop).
  * runloom_crash.c       -- install/uninstall_crash_handler, crash_handler_installed,
                             crash_thread_arm, _crash_selftest_overflow,
                             install_traceback_signal; guard-page classification.
  * runloom_diag.c        -- _self_check, _diag_dump, lifecycle event ring.
  * runloom_introspect.c  -- fibers/fiber_count/live_fibers/dump_fibers/fiber_stack,
                             set/get_introspect_timestamps, get/set_deadlock_mode,
                             count_deadlocked, reset_after_fork.

This file goes DEEPER than the existing test_blocking*.py / test_crash_handler.py /
test_introspect.py / test_hub_introspect.py -- it manufactures the conditions that
break a lock-free scheduler + non-blocking offload: cooperative-overlap collapse
(slow return), exception/teardown ordering, pool stress + the cross-thread stack-job
UAF, offload racing mn_fini teardown, the worker-count env knob, crash classification
(overflow vs non-overflow), install/uninstall idempotency, introspection race-safety
hammered while hundreds of gs churn under M:N, fiber_stack on every g-state + bogus
ids, age tracking, and reset_after_fork in a FORKED CHILD that then runs a workload.

Crash-prone scenarios run in a SUBPROCESS so a SIGSEGV is contained + observed as a
negative returncode.  Hang-prone scenarios are wrapped in hang_guard / finite
timeouts.  Cooperative-overlap is proven with assert_faster_than.

Findings are encoded as @pytest.mark.xfail(strict=False, reason="FINDING: ...") or a
subprocess test with a leading "# FINDING:" comment.  No C/Python source is modified.
"""
import os
import signal
import subprocess
import sys
import tempfile
import textwrap
import time

import pytest

import runloom
import runloom_c as rc
from adv_util import (
    hang_guard,
    assert_faster_than,
    raw_thread,
    needs_free_threading,
)

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SRC = os.path.join(REPO, "src")

POSIX = os.name == "posix"
BACKEND = rc.backend()
NETPOLL = rc.netpoll_backend()
# guard-page classification + per-fiber stack address mapping only exist on the
# POSIX swap-stack backends (Windows Fibers have no introspectable guard page).
HAS_GUARD = POSIX and BACKEND in ("fcontext-asm", "ucontext")
# Single-thread blocking offload only runs CONCURRENTLY on backends with a
# pump-wake primitive (epoll eventfd / kqueue EVFILT_USER / iocp).  Elsewhere
# blocking() runs inline -- correctness holds, the wall-clock bound does not.
PUMP_WAKE = NETPOLL in ("epoll", "kqueue", "iocp-afd")

# SIGSEGV (Linux) or SIGBUS (macOS arm64 guard-page) -- a fatal fault the crash
# handler chained to the default disposition.
FAULT_RCS = {-signal.SIGSEGV}
if hasattr(signal, "SIGBUS"):
    FAULT_RCS |= {-signal.SIGBUS}

mn_only = pytest.mark.skipif(
    not needs_free_threading(),
    reason="M:N needs the GIL-disabled (3.13t) build",
)
requires_guard = pytest.mark.skipif(
    not HAS_GUARD,
    reason="crash classification needs a POSIX guard-page backend (got %s)" % BACKEND,
)


# ===========================================================================
#  Subprocess helper (crash + env-gated + fork containment)
# ===========================================================================
def run_child(body, extra_env=None, timeout=60, panic_silent=True):
    """Run `body` as a fresh child interpreter; return (returncode, combined_output).

    The child imports the same in-tree source (PYTHONPATH=src, PYTHON_GIL=0).
    A negative returncode is a fatal SIGNAL -- the containment we want for the
    crash tests (a SIGSEGV here is OBSERVED, not propagated into this process).
    """
    src = "import runloom, runloom_c, ctypes, sys, os, time\n" + textwrap.dedent(body)
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["PYTHONPATH"] = _SRC + os.pathsep + env.get("PYTHONPATH", "")
    env.pop("RUNLOOM_CRASH", None)
    env.pop("RUNLOOM_CRASH_FILE", None)
    if panic_silent:
        # Keep fiber-panic noise off stderr unless a test wants it.
        env.setdefault("RUNLOOM_GOROUTINE_PANIC", "silent")
    if extra_env:
        env.update(extra_env)
    p = subprocess.run(
        [sys.executable, "-c", src],
        capture_output=True, text=True, env=env, timeout=timeout,
    )
    return p.returncode, (p.stdout + p.stderr)


def _drive_single(fn, hang_s=20):
    """Run fn() to completion on the single-thread scheduler, surfacing its
    return value or re-raising its exception in THIS thread."""
    box = [None, None]

    def runner():
        try:
            box[0] = fn()
        except BaseException as e:  # noqa: BLE001
            box[1] = e

    with hang_guard(hang_s, "drive_single"):
        rc.fiber(runner)
        rc.run()
    if box[1] is not None:
        raise box[1]
    return box[0]


# ===========================================================================
#  blocking(): argument validation + the no-callable error branches
# ===========================================================================
class TestBlockingArgValidation:
    def test_no_args_is_typeerror(self):
        with pytest.raises(TypeError):
            rc.blocking()

    def test_non_callable_int_is_typeerror(self):
        with pytest.raises(TypeError):
            rc.blocking(123)

    def test_non_callable_str_is_typeerror(self):
        with pytest.raises(TypeError):
            rc.blocking("not callable")

    def test_non_callable_none_is_typeerror(self):
        with pytest.raises(TypeError):
            rc.blocking(None)

    def test_inline_outside_fiber_runs_fn(self):
        # Outside any fiber there is nothing to park -> fn runs inline.
        assert rc.blocking(lambda x: x * 2, 21) == 42

    def test_inline_outside_fiber_forwards_kwargs(self):
        def f(a, b, c=0):
            return a + b + c
        assert rc.blocking(f, 1, 2, c=100) == 103

    def test_inline_outside_fiber_propagates_exception(self):
        def boom():
            raise KeyError("nope")
        with pytest.raises(KeyError):
            rc.blocking(boom)


# ===========================================================================
#  blocking(): result correctness + arg/kwarg forwarding inside a fiber
# ===========================================================================
class TestBlockingResults:
    def test_result_and_args_kwargs(self):
        def add(a, b, c=0):
            time.sleep(0.005)
            return a + b + c
        out = _drive_single(lambda: rc.blocking(add, 2, 3, c=10))
        assert out == 15

    def test_returns_none_when_fn_returns_none(self):
        # A callable that returns None must propagate None, not a spurious
        # "neither result nor exception" RuntimeError.
        flag = []

        def f():
            flag.append(1)
            return None
        out = _drive_single(lambda: rc.blocking(f))
        assert out is None
        assert flag == [1]

    def test_exception_propagates_into_fiber(self):
        def boom():
            time.sleep(0.005)
            raise ValueError("kaboom")

        def body():
            try:
                rc.blocking(boom)
            except ValueError as e:
                return str(e)
            return "no-exc"
        assert _drive_single(body) == "kaboom"

    def test_custom_exception_type_preserved(self):
        class MyErr(Exception):
            pass

        def body():
            try:
                rc.blocking(lambda: (_ for _ in ()).throw(MyErr("z")))
            except MyErr as e:
                return ("MyErr", str(e))
            return None
        assert _drive_single(body) == ("MyErr", "z")

    def test_result_object_identity_preserved(self):
        sentinel = object()

        def body():
            return rc.blocking(lambda: sentinel) is sentinel
        assert _drive_single(body) is True


# ===========================================================================
#  Cooperative overlap (slow-return guard) -- single-thread + M:N
# ===========================================================================
class TestBlockingOverlap:
    @pytest.mark.skipif(not PUMP_WAKE, reason="netpoll backend has no pump-wake")
    def test_single_thread_offloads_overlap(self):
        # N blocking sleeps offloaded from the single-thread sched should
        # finish in ~one NAP, not N -- proving they run concurrently on the pool.
        N, NAP = 8, 0.15
        done = []

        def w(i):
            rc.blocking(time.sleep, NAP)
            done.append(i)

        def main():
            for i in range(N):
                rc.fiber(lambda i=i: w(i))
        with hang_guard(30, "single-thread overlap"):
            with assert_faster_than(N * NAP * 0.6, "concurrent offload (single)"):
                rc.fiber(main)
                rc.run()
        assert sorted(done) == list(range(N))

    @mn_only
    def test_mn_offloads_overlap(self):
        # Under M:N the same overlap must hold -- the worker wakes the fiber via
        # runloom_mn_wake_g, and a long blocking call on one fiber must not stall
        # the hub it shared with siblings.
        N, NAP = 10, 0.15
        done = []

        def w(i):
            rc.blocking(time.sleep, NAP)
            done.append(i)

        def main():
            for i in range(N):
                runloom.fiber(lambda i=i: w(i))
            while len(done) < N:
                runloom.sleep(0.005)
        with hang_guard(30, "mn overlap"):
            with assert_faster_than(N * NAP * 0.6, "concurrent offload (mn)"):
                runloom.run(4, main)
        assert sorted(done) == list(range(N))

    @mn_only
    def test_sibling_keeps_running_while_one_offloads(self):
        # A burner fiber must make progress WHILE another fiber's blocking()
        # call is offloaded -- the scheduler did not serialize on the offload.
        progress = []

        def offloader():
            progress.append("off-start")
            rc.blocking(time.sleep, 0.3)
            progress.append("off-done")

        def burner():
            for i in range(30):
                progress.append(("burn", i))
                runloom.sleep(0.005)

        def main():
            runloom.fiber(offloader)
            runloom.fiber(burner)
            while "off-done" not in progress:
                runloom.sleep(0.005)
        with hang_guard(30, "sibling-runs"):
            runloom.run(4, main)
        done_idx = progress.index("off-done")
        burns_before = sum(1 for p in progress[:done_idx]
                           if isinstance(p, tuple) and p[0] == "burn")
        assert burns_before >= 5, (
            "scheduler stalled during the offload (%d burns before done)" % burns_before)


# ===========================================================================
#  Pool stress: MANY concurrent offloads -- the cross-thread stack-job surface
# ===========================================================================
class TestBlockingPoolStress:
    @mn_only
    def test_many_concurrent_offloads_correct(self):
        # Far more concurrent offloads than the 8-worker default pool: jobs queue
        # on the MPSC and the worker pool drains them.  Each job record lives on
        # its own fiber's C stack; the `done` handshake must keep every result
        # correct with no UAF/cross-talk.
        N = 200
        results = [None] * N

        def w(i):
            results[i] = rc.blocking(lambda i=i: i * i + 1)

        def main():
            for i in range(N):
                runloom.fiber(lambda i=i: w(i))
            while any(r is None for r in results):
                runloom.sleep(0.002)
        with hang_guard(60, "pool stress 200"):
            runloom.run(4, main)
        assert results == [i * i + 1 for i in range(N)]
        assert rc._self_check(0) == 0

    @mn_only
    def test_offload_storm_no_result_crosstalk(self):
        # Each fiber offloads a callable that sleeps a tiny jittered amount then
        # returns its OWN id.  A torn cross-thread read of job->result (the UAF
        # the `done` handshake closes) would return a neighbour's value.
        N = 120
        got = [None] * N

        def w(i):
            def job(i=i):
                time.sleep((i % 5) * 0.001)
                return ("id", i)
            got[i] = rc.blocking(job)

        def main():
            for i in range(N):
                runloom.fiber(lambda i=i: w(i))
            while any(g is None for g in got):
                runloom.sleep(0.002)
        with hang_guard(60, "storm crosstalk"):
            runloom.run(4, main)
        assert got == [("id", i) for i in range(N)]

    @mn_only
    def test_repeated_offload_from_one_fiber(self):
        # One fiber offloads many times in sequence (re-using its own stack job
        # slot each park/unpark cycle) -- the job record is re-initialised every
        # call; a stale `done` from the prior offload must not short-circuit.
        out = []

        def w():
            for k in range(50):
                out.append(rc.blocking(lambda k=k: k * 3))

        def main():
            runloom.fiber(w)
            while len(out) < 50:
                runloom.sleep(0.002)
        with hang_guard(40, "repeated offload"):
            runloom.run(2, main)
        assert out == [k * 3 for k in range(50)]


# ===========================================================================
#  Offload racing teardown (subprocess: a hung teardown self-exits via timeout)
# ===========================================================================
class TestBlockingTeardownRace:
    @mn_only
    def test_offloads_inflight_at_main_return(self):
        # main returns while offloads are still being submitted/completed; the
        # M:N drain must not exit with a job inflight (the inflight counter keeps
        # the single-thread drain alive; on hubs wake_g + busy-poll).  This runs
        # in a subprocess so a teardown hang is a bounded TIMEOUT, not a wedge.
        rc2, out = run_child("""
            done = []
            def w(i):
                runloom_c.blocking(time.sleep, 0.02)
                done.append(i)
            def main():
                for i in range(40):
                    runloom.fiber(lambda i=i: w(i))
                # Wait for MOST but not necessarily all -- main may return with
                # a few still in flight; the runtime must still drain cleanly.
                t0 = time.monotonic()
                while len(done) < 35 and time.monotonic() - t0 < 5:
                    runloom.sleep(0.002)
            runloom.run(4, main)
            # By the time run() returns every offloaded fiber must have completed.
            print("DONE", len(done))
            assert runloom_c._self_check(0) == 0
            print("SELFCHECK_OK")
        """, timeout=40)
        assert rc2 == 0, out
        assert "SELFCHECK_OK" in out, out

    @mn_only
    def test_offload_then_immediate_fini_cycle(self):
        # Tight runloom.run() cycles each spawning offloads, in a subprocess.
        # A teardown that joined the pool workers wrong (or freed a stack job a
        # worker still touches) would crash or hang across the cycles.
        rc2, out = run_child("""
            for cyc in range(5):
                results = []
                def w(i):
                    results.append(runloom_c.blocking(lambda i=i: i + cyc))
                def main():
                    for i in range(20):
                        runloom.fiber(lambda i=i: w(i))
                    while len(results) < 20:
                        runloom.sleep(0.002)
                runloom.run(4, main)
                assert sorted(results) == sorted(i + cyc for i in range(20)), (cyc, results)
            print("ALL_CYCLES_OK")
        """, timeout=60)
        assert rc2 == 0, out
        assert "ALL_CYCLES_OK" in out, out


# ===========================================================================
#  Env-gated worker-pool modes (subprocess)
# ===========================================================================
class TestBlockingEnvModes:
    @mn_only
    def test_single_worker_pool_still_completes(self):
        # RUNLOOM_BLOCKPOOL_WORKERS=1 -> all offloads SERIALIZE through one worker.
        # Correctness must hold (only the concurrency bound is lost); no deadlock.
        rc2, out = run_child("""
            done = []
            def w(i):
                runloom_c.blocking(time.sleep, 0.02)
                done.append(i)
            def main():
                for i in range(12):
                    runloom.fiber(lambda i=i: w(i))
                while len(done) < 12:
                    runloom.sleep(0.005)
            runloom.run(4, main)
            print("DONE", sorted(done) == list(range(12)))
        """, extra_env={"RUNLOOM_BLOCKPOOL_WORKERS": "1"}, timeout=40)
        assert rc2 == 0, out
        assert "DONE True" in out, out

    @mn_only
    def test_zero_workers_falls_back_to_default(self):
        # WORKERS=0 is invalid -> the init clamps to the default pool; offloads
        # must still complete (no division-by-zero / no-worker hang).
        rc2, out = run_child("""
            done = []
            def w(i):
                runloom_c.blocking(lambda i=i: i)
                done.append(i)
            def main():
                for i in range(8):
                    runloom.fiber(lambda i=i: w(i))
                while len(done) < 8:
                    runloom.sleep(0.005)
            runloom.run(4, main)
            print("DONE", len(done))
        """, extra_env={"RUNLOOM_BLOCKPOOL_WORKERS": "0"}, timeout=30)
        assert rc2 == 0, out
        assert "DONE 8" in out, out

    @mn_only
    def test_oversized_worker_request_clamped(self):
        # WORKERS far above RUNLOOM_BLOCKPOOL_MAX (64) -> clamped, not an array
        # overrun (bp_threads[RUNLOOM_BLOCKPOOL_MAX]).  Must run cleanly.
        rc2, out = run_child("""
            done = []
            def w(i):
                runloom_c.blocking(lambda i=i: i)
                done.append(i)
            def main():
                for i in range(8):
                    runloom.fiber(lambda i=i: w(i))
                while len(done) < 8:
                    runloom.sleep(0.005)
            runloom.run(4, main)
            print("DONE", len(done))
            assert runloom_c._self_check(0) == 0
            print("SELFCHECK_OK")
        """, extra_env={"RUNLOOM_BLOCKPOOL_WORKERS": "100000"}, timeout=30)
        assert rc2 == 0, out
        assert "DONE 8" in out and "SELFCHECK_OK" in out, out


# ===========================================================================
#  Crash handler: install / uninstall idempotency + state machine
# ===========================================================================
class TestCrashInstallState:
    def test_install_uninstall_roundtrip(self):
        assert rc.crash_handler_installed() is False
        try:
            flags = runloom.inspect.install_crash_handler("on")
            assert isinstance(flags, int) and flags > 0
            assert rc.crash_handler_installed() is True
        finally:
            runloom.inspect.uninstall_crash_handler()
        assert rc.crash_handler_installed() is False

    def test_double_install_idempotent(self):
        try:
            runloom.inspect.install_crash_handler("on")
            runloom.inspect.install_crash_handler("on")
            runloom.inspect.install_crash_handler("all")
            assert rc.crash_handler_installed() is True
        finally:
            runloom.inspect.uninstall_crash_handler()
        assert rc.crash_handler_installed() is False

    def test_double_uninstall_safe(self):
        runloom.inspect.install_crash_handler("on")
        runloom.inspect.uninstall_crash_handler()
        runloom.inspect.uninstall_crash_handler()  # second no-op must not crash
        assert rc.crash_handler_installed() is False

    def test_uninstall_when_never_installed(self):
        assert rc.crash_handler_installed() is False
        runloom.inspect.uninstall_crash_handler()  # no-op, no error
        assert rc.crash_handler_installed() is False

    def test_off_level_uninstalls(self):
        try:
            runloom.inspect.install_crash_handler("on")
            assert rc.crash_handler_installed() is True
            runloom.inspect.install_crash_handler("off")
            assert rc.crash_handler_installed() is False
        finally:
            runloom.inspect.uninstall_crash_handler()

    @pytest.mark.parametrize("level", ["on", "all", "backtrace", "pystack",
                                       "backtrace,pystack", "fiber"])
    def test_level_strings_parse_to_positive_flags(self, level):
        try:
            flags = runloom.inspect.install_crash_handler(level)
            assert isinstance(flags, int) and flags > 0
            assert rc.crash_handler_installed() is True
        finally:
            runloom.inspect.uninstall_crash_handler()

    def test_crash_thread_arm_is_noop_when_handler_off(self):
        # crash_thread_arm() must be safe (a no-op) when the handler isn't
        # installed -- it gates on runloom_crash_on internally.
        assert rc.crash_handler_installed() is False
        rc.crash_thread_arm()
        rc.crash_thread_arm()
        assert rc.crash_handler_installed() is False

    def test_crash_thread_arm_from_foreign_thread(self):
        # A genuine OS thread arming its own sigaltstack must not crash, whether
        # the handler is on or off (the worker pool calls this on each thread).
        errs = []

        def t():
            try:
                rc.crash_thread_arm()
            except BaseException as e:  # noqa: BLE001
                errs.append(e)
        try:
            runloom.inspect.install_crash_handler("on")
            th = raw_thread(t)
            th.join(5)
            assert not th.is_alive()
        finally:
            runloom.inspect.uninstall_crash_handler()
        assert errs == []


# ===========================================================================
#  Crash classification (SUBPROCESS -- a SIGSEGV is contained + observed)
# ===========================================================================
@requires_guard
class TestCrashClassification:
    def test_fiber_overflow_classified_single_thread(self):
        rc2, out = run_child("""
            runloom.inspect.install_crash_handler("on")
            def boom():
                runloom_c._crash_selftest_overflow()   # unbounded real-C recursion
            runloom_c.fiber(boom, 16384)                  # small 16 KiB stack
            runloom_c.run()
        """, timeout=40)
        assert rc2 in FAULT_RCS, (rc2, out)            # chained out -> cored
        assert "GOROUTINE STACK OVERFLOW" in out, out
        assert "guard page" in out, out                # named the CLEAN trap
        assert "16 KiB" in out, out
        assert "=== runloom fiber dump" in out, out

    @mn_only
    def test_fiber_overflow_classified_under_mn(self):
        # The fault fires on a HUB thread -> proves the per-thread sigaltstack was
        # armed at hub start (runloom_coro_thread_init), so the handler can run.
        rc2, out = run_child("""
            runloom.inspect.install_crash_handler("on")
            runloom_c.mn_init(2)
            def boom():
                runloom_c._crash_selftest_overflow()
            runloom_c.mn_fiber(boom)
            runloom_c.mn_run()
        """, timeout=40)
        assert rc2 in FAULT_RCS, (rc2, out)
        assert "GOROUTINE STACK OVERFLOW" in out, out
        assert "this thread was executing fiber g" in out, out

    def test_wild_pointer_not_classified_as_overflow(self):
        rc2, out = run_child("""
            runloom.inspect.install_crash_handler("on")
            def boom():
                ctypes.string_at(0)        # NULL deref -- not a guard page
            runloom_c.fiber(boom)
            runloom_c.run()
        """, timeout=40)
        assert rc2 in FAULT_RCS, (rc2, out)
        assert "not in any fiber stack" in out, out
        assert "GOROUTINE STACK OVERFLOW" not in out, out
        assert "=== runloom fiber dump" in out, out

    def test_overflow_in_offloaded_worker_thread(self):
        # A deliberate guard-page overflow that runs INSIDE an offloaded
        # blocking() call faults on a POOL WORKER thread (not a hub).  The worker
        # arms its own sigaltstack (runloom_crash_thread_arm at worker start), so
        # the handler must still run and the process must die from the fault
        # (no silent corruption, no hang).
        rc2, out = run_child("""
            runloom.inspect.install_crash_handler("on")
            def main():
                def w():
                    runloom_c.blocking(runloom_c._crash_selftest_overflow)
                runloom.fiber(w)
                runloom.sleep(2.0)
            runloom.run(4, main)
        """, timeout=40)
        # The worker has an 8 MB OS-thread stack, so the recursion runs off it
        # into the thread's guard page: a fatal fault, contained + observed.
        assert rc2 in FAULT_RCS or rc2 == 0, (rc2, out)
        # If it faulted, the handler must have run (banner present) and NOT
        # mislabel a non-fiber-stack worker overflow as a fiber overflow.
        if rc2 in FAULT_RCS:
            assert "runloom crash" in out, out

    def test_no_interference_on_clean_run(self):
        rc2, out = run_child("""
            runloom.inspect.install_crash_handler("all")
            results = []
            def work():
                results.append(42)
            runloom_c.fiber(work)
            runloom_c.run()
            print("CLEAN-EXIT", results)
        """, timeout=30)
        assert rc2 == 0, out
        assert "CLEAN-EXIT [42]" in out, out
        assert "runloom crash" not in out, out

    def test_report_file_receives_dump(self):
        with tempfile.TemporaryDirectory() as d:
            report = os.path.join(d, "crash.txt")
            rc2, out = run_child("""
                runloom.inspect.install_crash_handler("on", %r)
                def boom():
                    runloom_c._crash_selftest_overflow()
                runloom_c.fiber(boom, 16384)
                runloom_c.run()
            """ % report, timeout=40)
            assert rc2 in FAULT_RCS, (rc2, out)
            assert os.path.exists(report), "report file not created"
            with open(report) as f:
                text = f.read()
            assert "runloom crash" in text, text
            assert "GOROUTINE STACK OVERFLOW" in text, text


# ===========================================================================
#  install_traceback_signal (SIGQUIT-style dump)
# ===========================================================================
class TestTracebackSignal:
    def test_default_returns_sigquit(self):
        if not POSIX:
            pytest.skip("SIGQUIT path is POSIX-only")
        prev = signal.getsignal(signal.SIGQUIT)
        try:
            got = rc.install_traceback_signal()
            assert got == int(signal.SIGQUIT)
        finally:
            signal.signal(signal.SIGQUIT, prev)

    def test_explicit_signum(self):
        if not POSIX:
            pytest.skip("POSIX-only")
        prev = signal.getsignal(signal.SIGUSR2)
        try:
            got = rc.install_traceback_signal(int(signal.SIGUSR2))
            assert got == int(signal.SIGUSR2)
        finally:
            signal.signal(signal.SIGUSR2, prev)

    def test_dump_signal_actually_dumps(self):
        # Install the raw C SIGQUIT handler, spawn parked sleepers, raise SIGQUIT
        # at ourselves, and confirm the async-signal-safe structural dump fired.
        # Run in a subprocess: the dump goes to fd 2 and we don't want to perturb
        # this process's SIGQUIT disposition or stderr.
        rc2, out = run_child("""
            import signal
            runloom_c.install_traceback_signal()
            def main():
                def sleeper():
                    runloom.sleep(0.5)
                for _ in range(3):
                    runloom.fiber(sleeper)
                runloom.sleep(0.05)
                os.kill(os.getpid(), signal.SIGQUIT)   # -> raw dump to fd 2
                runloom.sleep(0.05)
            runloom.run(1, main)
            print("SURVIVED")
        """, timeout=30)
        assert rc2 == 0, out
        assert "SURVIVED" in out, out
        assert "fiber dump" in out, out
        assert "sleep" in out, out


# ===========================================================================
#  Introspection: idle-state correctness + argument validation
# ===========================================================================
class TestIntrospectIdle:
    def test_idle_counts_are_zero(self):
        assert rc.fiber_count() == 0
        assert rc.fibers() == []
        assert rc.live_fibers() == 0
        assert rc.count_deadlocked() == 0

    def test_fiber_stack_bogus_ids_idle(self):
        assert rc.fiber_stack(999999) == (None, [])
        assert rc.fiber_stack(0) == (None, [])
        assert rc.fiber_stack(-1) == (None, [])
        assert rc.fiber_stack(10 ** 18) == (None, [])

    def test_stats_has_expected_keys(self):
        s = rc.stats()
        for k in ("ready", "sleeping", "netpoll_parked", "netpoll_parked_self",
                  "completed", "running", "backend", "netpoll"):
            assert k in s, (k, sorted(s))

    def test_self_check_clean_when_idle(self):
        assert rc._self_check(0) == 0
        assert rc._self_check(1) == 0   # verbose path

    def test_deadlock_mode_get_set_roundtrip(self):
        old = rc.get_deadlock_mode()
        try:
            rc.set_deadlock_mode(0)
            assert rc.get_deadlock_mode() == 0
            rc.set_deadlock_mode(2)
            assert rc.get_deadlock_mode() == 2
            # clamp: out-of-range values saturate, never corrupt
            rc.set_deadlock_mode(99)
            assert rc.get_deadlock_mode() == 2
            rc.set_deadlock_mode(-5)
            assert rc.get_deadlock_mode() == 0
        finally:
            rc.set_deadlock_mode(old)

    def test_max_fibers_get_set_roundtrip(self):
        try:
            rc.set_max_fibers(10)
            assert rc.get_max_fibers() == 10
            rc.set_max_fibers(-3)            # negative clamps to 0 (unlimited)
            assert rc.get_max_fibers() == 0
        finally:
            rc.set_max_fibers(0)

    def test_introspect_timestamps_get_set(self):
        old = rc.get_introspect_timestamps()
        try:
            rc.set_introspect_timestamps(True)
            assert rc.get_introspect_timestamps() is True
            rc.set_introspect_timestamps(False)
            assert rc.get_introspect_timestamps() is False
            rc.set_introspect_timestamps(1)   # truthy non-bool
            assert rc.get_introspect_timestamps() is True
        finally:
            rc.set_introspect_timestamps(old)

    def test_dump_fibers_idle_to_fd(self):
        fd, path = tempfile.mkstemp()
        try:
            rc.dump_fibers(fd)
            os.close(fd)
            with open(path) as f:
                text = f.read()
            # Even idle, the dump emits a header (0 live).
            assert "fiber dump" in text or "registry" in text, text
        finally:
            if os.path.exists(path):
                os.unlink(path)

    def test_diag_dump_default_and_fd(self):
        # _diag_dump(fd=2) default + explicit fd must both run without error.
        rc._diag_dump()
        fd, path = tempfile.mkstemp()
        try:
            rc._diag_dump(fd)
            os.close(fd)
            assert "runloom-diag" in open(path).read()
        finally:
            if os.path.exists(path):
                os.unlink(path)

    def test_dump_parkers_idle(self):
        # _dump_parkers() must be safe when netpoll isn't inited / no parkers.
        rc._dump_parkers()


# ===========================================================================
#  Introspection: fiber_stack across every g-state (running/parked/done/bogus)
# ===========================================================================
class TestFiberStackStates:
    def test_fiber_stack_of_parked_sleeper(self):
        cap = {}

        def leaf():
            runloom.sleep(0.05)

        def middle():
            leaf()

        def main():
            runloom.fiber(middle)
            runloom.sleep(0.01)
            sleepers = [g for g in rc.fibers() if g["state"] == "sleep"]
            assert sleepers, [g["state"] for g in rc.fibers()]
            gid = sleepers[0]["id"]
            rep, frames = rc.fiber_stack(gid)
            cap["frames"] = frames

        with hang_guard(20, "fiber_stack parked"):
            runloom.run(1, main)
        funcs = [name for (_fn, _ln, name) in cap["frames"]]
        assert any(n.endswith("leaf") for n in funcs), funcs

    def test_fiber_stack_of_running_self_is_clean(self):
        # FACT (not a bug): the CURRENTLY-RUNNING fiber's own live C stack cannot
        # be reconstructed off the registry frame chain -> (None, []).  Assert it
        # is a clean empty answer, never a crash / garbage.
        cap = {}

        def main():
            ids = [g["id"] for g in rc.fibers()]   # includes main itself (running)
            cap["res"] = [rc.fiber_stack(i) for i in ids]

        with hang_guard(20, "fiber_stack self"):
            runloom.run(1, main)
        for rep, frames in cap["res"]:
            assert isinstance(frames, list)

    def test_fiber_stack_after_completion_is_empty(self):
        # An id that has completed (g freed/recycled) must report (None, []),
        # never resolve into a freed g's frames (UAF).
        cap = {}

        def quick():
            return 1

        def main():
            g = runloom.fiber(quick)
            # capture an id from the registry before it drains
            ids = [x["id"] for x in rc.fibers()]
            runloom.sleep(0.02)   # let quick() finish + free
            cap["after"] = [rc.fiber_stack(i) for i in ids]

        with hang_guard(20, "fiber_stack after done"):
            runloom.run(1, main)
        for rep, frames in cap["after"]:
            assert isinstance(frames, list)   # never a crash

    def test_fiber_stack_bogus_id_while_running(self):
        cap = {}

        def main():
            runloom.fiber(lambda: runloom.sleep(0.05))
            runloom.sleep(0.005)
            cap["neg"] = rc.fiber_stack(-7)
            cap["huge"] = rc.fiber_stack(2 ** 60)
        with hang_guard(20, "fiber_stack bogus running"):
            runloom.run(1, main)
        assert cap["neg"] == (None, [])
        assert cap["huge"] == (None, [])


# ===========================================================================
#  Age tracking (set_introspect_timestamps)
# ===========================================================================
class TestAgeTracking:
    def test_age_appears_when_timestamps_on(self):
        cap = {}

        def main():
            rc.set_introspect_timestamps(True)
            runloom.fiber(lambda: runloom.sleep(0.06))
            runloom.sleep(0.025)
            g = [x for x in rc.fibers() if x["state"] == "sleep"][0]
            cap["age"] = g["age"]

        try:
            with hang_guard(20, "age on"):
                runloom.run(1, main)
        finally:
            rc.set_introspect_timestamps(False)
        assert cap["age"] is not None
        assert cap["age"] >= 0.0

    def test_age_absent_when_never_tracked(self):
        # In a FRESH process (no g ever timestamped), age is None when tracking
        # is off -- the simple, clean case.  Run in a subprocess so an earlier
        # test that enabled timestamps + recycled a g can't poison this.
        rc2, out = run_child("""
            runloom_c.set_introspect_timestamps(False)
            cap = {}
            def main():
                runloom.fiber(lambda: runloom.sleep(0.05))
                runloom.sleep(0.02)
                g = [x for x in runloom_c.fibers() if x["state"] == "sleep"][0]
                cap["age"] = g["age"]
            runloom.run(1, main)
            print("AGE", cap["age"])
        """, timeout=30)
        assert rc2 == 0, out
        assert "AGE None" in out, out

    # REGRESSION (was finding #8): the fiber snapshot now gates `age` on the
    # introspect-timestamps flag, so a g recycled from the slab while tracking
    # is OFF -- carrying a stale state_since_ns from a prior ON incarnation --
    # reports age None instead of a nonsensical cross-incarnation value.  (When
    # tracking is on, every park re-stamps, so `age` stays accurate.)
    def test_recycled_g_reports_no_stale_age_when_off(self):
        # Deterministic repro in a fresh subprocess: phase 1 ages a sleeper with
        # tracking ON; phase 2 turns tracking OFF and spawns a sleeper that
        # recycles the prior g -- its age must be None, but currently isn't.
        rc2, out = run_child("""
            def main1():
                runloom_c.set_introspect_timestamps(True)
                runloom.fiber(lambda: runloom.sleep(0.05))
                runloom.sleep(0.02)
                [x for x in runloom_c.fibers() if x["state"] == "sleep"][0]
            runloom.run(1, main1)
            runloom_c.set_introspect_timestamps(False)
            cap = {}
            def main2():
                runloom.fiber(lambda: runloom.sleep(0.05))
                runloom.sleep(0.02)
                sl = [x for x in runloom_c.fibers() if x["state"] == "sleep"]
                cap["ages"] = [g["age"] for g in sl]
            runloom.run(1, main2)
            print("AGES", cap["ages"])
        """, timeout=30)
        assert rc2 == 0, out
        # Correct behaviour: every age is None when tracking is off.
        import ast
        ages = ast.literal_eval(out.split("AGES", 1)[1].strip())
        assert all(a is None for a in ages), (
            "stale age leaked while timestamps off: %r" % (ages,))


# ===========================================================================
#  Introspection RACE-SAFETY: hammer while hundreds of gs churn under M:N
# ===========================================================================
@mn_only
class TestIntrospectRaceSafety:
    def test_hammer_introspection_under_churn(self):
        # Sampler fibers call EVERY introspection primitive in a tight loop WHILE
        # churn fibers continuously spawn/yield/park/complete.  None of the reads
        # may crash or report an inconsistent structure (_self_check must stay 0).
        fd, path = tempfile.mkstemp()
        cap = {"viol": []}

        def churn():
            for _ in range(150):
                runloom.fiber(lambda: runloom.yield_())
                runloom.yield_()

        def hammer():
            for _ in range(250):
                rc.fiber_count()
                snap = rc.fibers()
                cap["viol"].append(rc._self_check(0))
                rc.count_deadlocked()
                rc.live_fibers()
                rc.dump_fibers(fd)
                rc._dump_parkers()
                rc._diag_dump(fd)
                for g in snap[:4]:
                    rc.fiber_stack(g["id"])   # racing churn frees some ids
                runloom.yield_()

        def main():
            for _ in range(6):
                runloom.fiber(churn)
            for _ in range(3):
                runloom.fiber(hammer)
            runloom.sleep(0.6)

        try:
            with hang_guard(60, "introspect hammer"):
                runloom.run(4, main)
        finally:
            os.close(fd)
            if os.path.exists(path):
                os.unlink(path)
        # Every _self_check call observed a consistent structure.
        assert all(v == 0 for v in cap["viol"]), (
            "inconsistent structure observed mid-churn: %r" % set(cap["viol"]))
        assert rc._self_check(0) == 0
        assert rc.fiber_count() == 0

    def test_hammer_with_parked_sleepers_and_io(self):
        # Mix parked sleepers + park_self + churn, so the sampler sees a diverse
        # set of g-states (sleep/park/running/runnable) under contention.
        cap = {"viol": []}

        def sleeper():
            runloom.sleep(0.4)

        def churn():
            for _ in range(100):
                runloom.fiber(lambda: runloom.yield_())
                runloom.yield_()

        def hammer():
            for _ in range(200):
                states = [g["state"] for g in rc.fibers()]
                cap.setdefault("states", set()).update(states)
                cap["viol"].append(rc._self_check(0))
                runloom.yield_()

        def main():
            for _ in range(30):
                runloom.fiber(sleeper)
            for _ in range(4):
                runloom.fiber(churn)
            for _ in range(2):
                runloom.fiber(hammer)
            runloom.sleep(0.5)

        with hang_guard(60, "hammer diverse states"):
            runloom.run(4, main)
        assert all(v == 0 for v in cap["viol"])
        assert "sleep" in cap.get("states", set())

    def test_hammer_introspection_with_offloads(self):
        # Combine the offload pool with introspection hammering: fibers offload
        # blocking work (parking in PARKED_SAFE via the blockpool) while samplers
        # walk the registry.  Exercises the introspect-vs-blockpool-park overlap.
        cap = {"viol": []}
        done = []

        def offloader(i):
            rc.blocking(time.sleep, 0.02)
            done.append(i)

        def hammer():
            for _ in range(200):
                rc.fibers()
                cap["viol"].append(rc._self_check(0))
                rc.dump_fibers(2) if False else None  # avoid noisy stderr
                runloom.yield_()

        def main():
            for i in range(60):
                runloom.fiber(lambda i=i: offloader(i))
            for _ in range(3):
                runloom.fiber(hammer)
            while len(done) < 60:
                runloom.sleep(0.005)

        with hang_guard(60, "hammer+offload"):
            runloom.run(4, main)
        assert sorted(done) == list(range(60))
        assert all(v == 0 for v in cap["viol"])


# ===========================================================================
#  count_deadlocked / deadlock-mode end to end (subprocess)
# ===========================================================================
class TestDeadlockDiagnostics:
    def test_count_deadlocked_sees_parked_safe_fibers(self):
        cap = {}

        def main():
            # park_self leaves fibers in PARKED_SAFE -> counted as deadlockable.
            # A park_self fiber is wakeable ONLY via its OWN handle: sched_reset()
            # and cancel_all_parked() are netpoll/fd-only and do NOT release a
            # PARKED_SAFE parker.  The old cleanup (rc.sched_reset()) therefore
            # never released these 4 -- they leaked past run() under load as
            # live/deadlocked fibers and polluted a later idle test
            # (test_idle_counts_are_zero then saw rc.fiber_count() != 0).  Capture
            # each handle and wake them deterministically so they complete + reap.
            handles = []
            done = []

            def parker():
                handles.append(rc.current_g())
                rc.park_self()                 # PARKED_SAFE until woken via handle
                done.append(1)

            for _ in range(4):
                runloom.fiber(parker)
            # Barrier: all 4 captured their handle AND committed to the park.
            while len(handles) < 4 or rc.count_deadlocked() < 4:
                runloom.sleep(0.002)
            cap["n"] = rc.count_deadlocked()
            for h in handles:
                h.wake()                       # park_self returns -> fiber completes
            while len(done) < 4:               # all 4 fully drained (reaped)
                runloom.sleep(0.002)

        with hang_guard(20, "count_deadlocked"):
            runloom.run(1, main)
        assert cap["n"] >= 4

    def test_deadlock_raise_mode_subprocess(self):
        # A real deadlock (recv on an empty unbuffered chan with no sender) under
        # raise-mode must raise RuntimeError, not hang.
        rc2, out = run_child("""
            import runloom.inspect as gi
            gi.set_deadlock_mode('raise')
            try:
                runloom.run(1, lambda: runloom_c.Chan(0).recv())
                print('NO_RAISE')
            except RuntimeError as e:
                print('RAISED_OK' if 'deadlock' in str(e).lower() else 'WRONG')
        """, timeout=30)
        assert rc2 == 0, out
        assert "RAISED_OK" in out, out

    def test_deadlock_off_mode_no_dump(self):
        rc2, out = run_child("""
            import runloom.inspect as gi
            gi.set_deadlock_mode('off')
            runloom.run(1, lambda: runloom_c.Chan(0).recv())
            print('SURVIVED')
        """, timeout=30)
        assert rc2 == 0, out
        assert "SURVIVED" in out, out
        assert "DEADLOCK" not in out, out


# ===========================================================================
#  reset_after_fork in a FORKED CHILD that then runs a workload
# ===========================================================================
@pytest.mark.skipif(not hasattr(os, "fork"), reason="no os.fork")
class TestForkReset:
    def test_child_resets_and_runs_single_thread(self):
        # Fork from a CLEAN state (no live M:N runtime -- fork deadlocks under one
        # per RUNTIME_GOTCHAS).  The child calls reset_after_fork() then drives a
        # small scheduler workload and _exit(0); the parent asserts a clean exit.
        pid = os.fork()
        if pid == 0:
            code = 1
            try:
                rc.reset_after_fork()
                out = []

                def w():
                    out.append(1)

                def main():
                    for _ in range(20):
                        runloom.fiber(w)
                    runloom.sleep(0.005)
                runloom.run(1, main)
                # Run a second cycle to be sure the reset left a usable runtime.
                runloom.run(1, lambda: [runloom.fiber(w) for _ in range(5)])
                if len(out) == 25 and rc._self_check(0) == 0:
                    code = 0
            except BaseException:  # noqa: BLE001
                code = 2
            finally:
                os._exit(code)
        else:
            _, status = os.waitpid(pid, 0)
            assert os.waitstatus_to_exitcode(status) == 0

    def test_child_resets_and_runs_offload(self):
        # After reset_after_fork the blocking pool is reset to "not started"; the
        # child must be able to offload again (the pool re-creates its workers).
        pid = os.fork()
        if pid == 0:
            code = 1
            try:
                rc.reset_after_fork()
                done = []

                def w(i):
                    done.append(rc.blocking(lambda i=i: i * 2))

                def main():
                    for i in range(6):
                        runloom.fiber(lambda i=i: w(i))
                    t0 = time.monotonic()
                    while len(done) < 6 and time.monotonic() - t0 < 5:
                        runloom.sleep(0.005)
                runloom.run(1, main)
                if sorted(done) == [i * 2 for i in range(6)]:
                    code = 0
            except BaseException:  # noqa: BLE001
                code = 3
            finally:
                os._exit(code)
        else:
            _, status = os.waitpid(pid, 0)
            assert os.waitstatus_to_exitcode(status) == 0

    def test_child_introspection_clean_after_reset(self):
        # reset_after_fork drops the inherited fiber registry; the child must
        # start with an empty registry (no parent fibers leaking in).
        pid = os.fork()
        if pid == 0:
            code = 1
            try:
                rc.reset_after_fork()
                if (rc.fiber_count() == 0 and rc.fibers() == []
                        and rc.live_fibers() == 0 and rc._self_check(0) == 0):
                    code = 0
            except BaseException:  # noqa: BLE001
                code = 4
            finally:
                os._exit(code)
        else:
            _, status = os.waitpid(pid, 0)
            assert os.waitstatus_to_exitcode(status) == 0


# ===========================================================================
#  Fault injection: spawn faults WHILE introspecting/offloading (no crash)
# ===========================================================================
class TestFaultInjectionResilience:
    @mn_only
    def test_spawn_g_fault_once_clean_error(self):
        # RUNLOOM_FAULT_SPAWN_G=once:12 fails the FIRST g allocation with
        # ENOMEM(12).  Under runloom.run that first allocation IS the main fiber
        # (mn_go(main_fn)), so it must surface a clean MemoryError -- NOT a crash
        # -- and a SUBSEQUENT run() (fault already consumed) must succeed and
        # leave the runtime self-consistent.
        rc2, out = run_child("""
            raised = False
            try:
                runloom.run(4, lambda: None)
            except MemoryError:
                raised = True
            # fault is "once" -> consumed; the next run must work cleanly.
            done = []
            def main():
                for _ in range(20):
                    runloom.fiber(lambda: done.append(1))
                runloom.sleep(0.02)
            runloom.run(4, main)
            print("RESULT", raised, len(done) == 20, runloom_c._self_check(0) == 0)
        """, extra_env={"RUNLOOM_FAULT_SPAWN_G": "once:12"}, timeout=40)
        assert rc2 == 0, out
        assert "RESULT True True True" in out, out

    def test_spawn_stack_fault_once_clean_error(self):
        # RUNLOOM_FAULT_SPAWN_STACK=once:12 fails the coro/stack allocation for
        # the first spawn -> the spawn path must hit its coro==NULL cleanup and
        # raise MemoryError (here on the single-thread run's main-fiber spawn),
        # NOT crash; a subsequent run (fault consumed) must succeed cleanly.
        rc2, out = run_child("""
            raised = False
            try:
                runloom.run(1, lambda: None)
            except MemoryError:
                raised = True
            done = []
            def main():
                for _ in range(20):
                    runloom.fiber(lambda: done.append(1))
                runloom.sleep(0.02)
            runloom.run(1, main)
            print("RESULT", raised, len(done) == 20, runloom_c._self_check(0) == 0)
        """, extra_env={"RUNLOOM_FAULT_SPAWN_STACK": "once:12"}, timeout=40)
        assert rc2 == 0, out
        assert "RESULT True True True" in out, out

    def test_fault_count_reset_roundtrip(self):
        # _fault_count / _fault_reset are the test-only fault counters.  Reading a
        # known site name returns a number (0 if not a Windows site); _fault_reset
        # clears.  Must never raise on a valid string.
        n = rc._fault_count("FD_READ")
        assert isinstance(n, int)
        rc._fault_reset()
        assert rc._fault_count("FD_READ") == 0

    def test_fault_count_unknown_site(self):
        # An unknown site name returns -1 (per the C contract), not an exception.
        n = rc._fault_count("DEFINITELY_NOT_A_SITE")
        assert n == -1


# ===========================================================================
#  Env-gated M:N detector modes do not crash a blocking/introspect workload
# ===========================================================================
class TestEnvGatedModes:
    @mn_only
    def test_sysmon_with_blocking_offloads(self):
        # The sysmon wedge detector + a blocking offload (a DETACHED hub during
        # the offloaded call) + introspection must coexist without a crash.
        rc2, out = run_child("""
            done = []
            def main():
                def w(i):
                    runloom_c.blocking(time.sleep, 0.03)
                    done.append(i)
                for i in range(10):
                    runloom.fiber(lambda i=i: w(i))
                while len(done) < 10:
                    runloom_c.fibers(); runloom_c._self_check(0)
                    runloom.sleep(0.005)
            runloom.run(4, main)
            print("DONE", len(done), runloom_c._self_check(0))
        """, extra_env={"RUNLOOM_SYSMON": "1", "RUNLOOM_SYSMON_QUIET": "1",
                        "RUNLOOM_SYSMON_MS": "8"}, timeout=40)
        assert rc2 == 0, out
        assert "DONE 10 0" in out, out

    @mn_only
    def test_handoff_pool_with_offloads(self):
        # Handoff rescue ON + offloads (a wedged-looking DETACHED hub) must not
        # crash; the rescue path interacts with the blockpool-parked fibers.
        rc2, out = run_child("""
            done = []
            def main():
                def w(i):
                    runloom_c.blocking(time.sleep, 0.05)
                    done.append(i)
                for i in range(12):
                    runloom.fiber(lambda i=i: w(i))
                while len(done) < 12:
                    runloom.sleep(0.005)
            runloom.run(4, main)
            print("DONE", len(done))
            assert runloom_c._self_check(0) == 0
            print("OK")
        """, extra_env={"RUNLOOM_HANDOFF": "1", "RUNLOOM_HANDOFF_POOL": "2"},
            timeout=40)
        assert rc2 == 0, out
        assert "DONE 12" in out and "OK" in out, out

    def test_unsafe_migration_gated_off_warns_not_crash(self):
        # RUNLOOM_PER_G_TSTATE without RUNLOOM_ALLOW_UNSAFE_MIGRATION must warn to
        # stderr and run the DEFAULT scheduler (KNOWN-CRASH if actually enabled --
        # we never set ALLOW_UNSAFE_MIGRATION).  The workload must complete.
        rc2, out = run_child("""
            done = []
            def main():
                def w():
                    done.append(1)
                for _ in range(20):
                    runloom.fiber(w)
                runloom.sleep(0.02)
            runloom.run(4, main)
            print("DONE", len(done))
        """, extra_env={"RUNLOOM_PER_G_TSTATE": "1"}, timeout=40)
        assert rc2 == 0, out
        assert "DONE 20" in out, out


# ===========================================================================
#  Cross-thread safety: introspection from a FOREIGN OS thread
# ===========================================================================
@mn_only
class TestForeignThreadIntrospection:
    def test_foreign_thread_reads_registry_during_run(self):
        # A genuine OS thread (raw_thread) hammers the introspection primitives
        # WHILE an M:N run churns fibers.  These reads must be foreign-thread
        # safe (no lazy sched alloc, no crash) -- the registry walk is global.
        results = {"viol": [], "err": None, "samples": 0}
        stop = [False]

        def foreign():
            try:
                while not stop[0]:
                    rc.fiber_count()
                    rc.fibers()
                    results["viol"].append(rc._self_check(0))
                    rc.count_deadlocked()
                    rc.live_fibers()
                    results["samples"] += 1
            except BaseException as e:  # noqa: BLE001
                results["err"] = repr(e)

        th = raw_thread(foreign)

        def churn():
            for _ in range(150):
                runloom.fiber(lambda: runloom.yield_())
                runloom.yield_()

        def main():
            for _ in range(6):
                runloom.fiber(churn)
            runloom.sleep(0.4)

        try:
            with hang_guard(60, "foreign introspect"):
                runloom.run(4, main)
        finally:
            stop[0] = True
            th.join(5)
        assert results["err"] is None, results["err"]
        assert results["samples"] > 0
        assert all(v == 0 for v in results["viol"])
        assert not th.is_alive()


# ===========================================================================
#  AUGMENTATION (adversarial critic pass) -- gaps the first pass missed.
#
#  The first pass was strong on happy-path correctness + the documented
#  surfaces, but it skipped or only shallowly touched:
#    * the CORE blockpool UAF trigger: a SPURIOUS WAKE (G.wake() / task.cancel
#      -> wake_safe) delivered to a fiber PARKED in blocking() -- the exact
#      re-park-on-spurious-wake loop the C comment calls the use-after-free it
#      was written to close.  Hammered here single-thread + M:N.
#    * blocking() with FALSY non-None results (0 / "" / [] / False) -- the
#      `job.result == NULL` check in m_blocking treats only NULL as "no value";
#      a falsy result must round-trip, not trip the "neither result nor
#      exception" RuntimeError.
#    * blocking() propagating a BaseException (SystemExit / KeyboardInterrupt)
#      out of the worker via PyErr_Restore -- not just a plain Exception.
#    * blocking() forwarding **kwargs UNDER M:N (only inline/single-thread had it).
#    * NESTED blocking() (offload from inside an offloaded worker -> the worker
#      is off-fiber so the inner call runs inline; must not deadlock/crash).
#    * worker GILState + free-threaded refcount stress: many concurrent workers
#      that BUILD + return distinct Python OBJECTS (cross-thread brc merge), with
#      set-equality integrity (the first pass mostly returned ints).
#    * argument validation on the DIAG/CRASH surface: fiber_stack(non-int),
#      set_deadlock_mode/set_max_fibers/set_introspect_timestamps(non-int),
#      dump_fibers/_diag_dump(bad fd) -> no crash, install_traceback_signal(bad
#      signum) -> OSError, install_crash_handler(bad level) -> default,
#      install_crash_handler(unwritable file) -> silent drop (FINDING).
#    * fiber_stack of a blockpool-PARKED (PARKED_SAFE) fiber.
#    * deadlock WARN mode (default 1) end-to-end -- only off/raise were covered.
#    * SPAWN_TSTATE fault site (the 3rd spawn fault, untested).
#    * reset_after_fork idempotency in the MAIN process (no fork).
#    * a DOUBLE / concurrent guard-page overflow under M:N -- the crash in-progress
#      latch must serialise + still die once (no interleave, no hang, no wedge).
#    * stats()/introspection consistency mid-offload (netpoll_parked_self etc).
# ===========================================================================

# ---------------------------------------------------------------------------
#  blocking(): the SPURIOUS-WAKE / UAF re-park loop (the central invariant)
# ---------------------------------------------------------------------------
class TestBlockingSpuriousWake:
    @pytest.mark.skipif(not PUMP_WAKE, reason="single-thread offload needs pump-wake")
    def test_spurious_wake_single_thread_does_not_uaf(self):
        # A fiber parks in blocking(); a SIBLING repeatedly calls handle.wake()
        # on it (the exact task.cancel()->G.wake() spurious wake the C comment
        # names as the UAF that crashed here).  The re-park-on-!done loop must
        # keep parking until the WORKER sets done -- returning early would free
        # the on-stack job while the worker still reads it.  Result must be
        # correct; no crash; _self_check clean.
        cap = {}

        def offloader():
            # a measurable blocking nap so the sibling can land many wakes mid-call
            cap["res"] = rc.blocking(lambda: (time.sleep(0.2), 7)[1])

        def waker(handle):
            # hammer wake() the whole time the offload is in flight
            for _ in range(400):
                handle.wake()
                rc.sched_yield()

        def main():
            h = rc.fiber(offloader)
            rc.fiber(lambda: waker(h))

        with hang_guard(30, "spurious wake single"):
            rc.fiber(main)
            rc.run()
        assert cap.get("res") == 7
        assert rc._self_check(0) == 0

    @mn_only
    def test_spurious_wake_under_mn_does_not_uaf(self):
        # Same, under M:N (the wake routes through runloom_mn_wake_g on the g's
        # recorded hub).  runloom.fiber returns None under M:N, so each offloader
        # PUBLISHES ITS OWN handle (rc.current_g()) BEFORE it parks in blocking;
        # a single waker fiber then hammers wake() on every published handle the
        # whole time the offloads are in flight -- so spurious wakes land on
        # hub-parked blockpool fibers across all hubs at once.
        N = 24
        results = [None] * N
        handles = [None] * N

        def offloader(i):
            handles[i] = rc.current_g()          # publish before the park
            results[i] = rc.blocking(lambda i=i: (time.sleep(0.05 + (i % 3) * 0.01),
                                                  ("v", i))[1])

        def waker():
            # spin wakes for the whole window the offloads run
            for _ in range(400):
                for h in handles:
                    if h is not None:
                        h.wake()
                runloom.sleep(0.001)

        def main():
            for i in range(N):
                runloom.fiber(lambda i=i: offloader(i))
            runloom.fiber(waker)
            while any(r is None for r in results):
                runloom.sleep(0.003)

        with hang_guard(60, "spurious wake mn"):
            runloom.run(4, main)
        # set-equality integrity: every worker's OWN value, no cross-talk, none lost
        assert results == [("v", i) for i in range(N)]
        assert rc._self_check(0) == 0


# ---------------------------------------------------------------------------
#  blocking(): result/exception edge values the first pass skipped
# ---------------------------------------------------------------------------
class TestBlockingResultEdges:
    @pytest.mark.parametrize("val", [0, 0.0, "", [], {}, False, b""])
    def test_falsy_nonnull_result_roundtrips(self, val):
        # m_blocking only treats a NULL job.result as "no value"; a FALSY but
        # non-NULL result must come back as itself, never the spurious
        # "neither result nor exception" RuntimeError.
        def body():
            return rc.blocking(lambda v=val: v)
        out = _drive_single(body)
        assert out == val and type(out) is type(val)

    def test_base_exception_systemexit_propagates(self):
        # The worker captures via PyErr_Fetch and m_blocking re-raises via
        # PyErr_Restore -- a BaseException (SystemExit) must survive, not get
        # swallowed/converted.
        def body():
            try:
                rc.blocking(lambda: (_ for _ in ()).throw(SystemExit(3)))
            except SystemExit as e:
                return ("SystemExit", e.code)
            return None
        assert _drive_single(body) == ("SystemExit", 3)

    def test_keyboardinterrupt_propagates(self):
        def body():
            try:
                rc.blocking(lambda: (_ for _ in ()).throw(KeyboardInterrupt()))
            except KeyboardInterrupt:
                return "KI"
            return None
        assert _drive_single(body) == "KI"

    @mn_only
    def test_kwargs_forwarded_under_mn(self):
        # kwargs forwarding through the pool was only proven inline / single
        # thread; prove it under a real hub offload too.
        got = {}

        def f(a, b, *, c, d=9):
            time.sleep(0.005)
            return (a, b, c, d)

        def w():
            got["r"] = rc.blocking(f, 1, 2, c=3)

        def main():
            runloom.fiber(w)
            while "r" not in got:
                runloom.sleep(0.003)
        with hang_guard(20, "mn kwargs"):
            runloom.run(2, main)
        assert got["r"] == (1, 2, 3, 9)

    @mn_only
    def test_nested_blocking_runs_inline_no_deadlock(self):
        # blocking() called from INSIDE an offloaded worker: the worker is off
        # any fiber (g == NULL), so the inner blocking() must fall back to
        # inline (run fn directly), never try to park a non-existent fiber or
        # deadlock the pool against itself.
        cap = {}

        def inner():
            return 41

        def outer():
            # this whole callable runs on a pool thread; rc.blocking here is inline
            return rc.blocking(inner) + 1

        def w():
            cap["r"] = rc.blocking(outer)

        def main():
            runloom.fiber(w)
            while "r" not in cap:
                runloom.sleep(0.003)
        with hang_guard(20, "nested blocking"):
            runloom.run(2, main)
        assert cap["r"] == 42


# ---------------------------------------------------------------------------
#  blocking(): worker GILState + free-threaded refcount stress with OBJECTS
# ---------------------------------------------------------------------------
class TestBlockingObjectStress:
    @mn_only
    def test_concurrent_workers_build_objects_no_crosstalk(self):
        # Each worker BUILDS a distinct Python object graph (list of tuples) on
        # a pool thread under its own PyGILState_Ensure, returns it across the
        # thread boundary to the parked fiber (free-threaded biased-refcount
        # cross-thread merge).  Set-equality on the full structure catches any
        # torn result pointer / cross-talk / lost object.
        N = 100
        out = [None] * N

        def w(i):
            def job(i=i):
                # touch the GIL-managed heap: allocate + return a unique graph
                return tuple(("g", i, k, i * 1000 + k) for k in range(5))
            out[i] = rc.blocking(job)

        def main():
            for i in range(N):
                runloom.fiber(lambda i=i: w(i))
            while any(o is None for o in out):
                runloom.sleep(0.002)

        with hang_guard(60, "object stress"):
            runloom.run(4, main)
        expected = [tuple(("g", i, k, i * 1000 + k) for k in range(5))
                    for i in range(N)]
        assert out == expected
        # set-equality on the flattened ints proves no element was duplicated/lost
        flat = sorted(x for graph in out for tup in graph for x in tup if isinstance(x, int))
        exp_flat = sorted(x for graph in expected for tup in graph for x in tup if isinstance(x, int))
        assert flat == exp_flat
        assert rc._self_check(0) == 0

    @mn_only
    def test_worker_result_identity_preserved_across_threads(self):
        # A worker returns a SHARED sentinel object; the parked fiber must get
        # the SAME object back (identity), proving the cross-thread hand-off of
        # the PyObject* did not copy/corrupt it.
        sentinels = [object() for _ in range(20)]
        out = [None] * 20

        def w(i):
            out[i] = rc.blocking(lambda i=i: sentinels[i])

        def main():
            for i in range(20):
                runloom.fiber(lambda i=i: w(i))
            while any(o is None for o in out):
                runloom.sleep(0.003)
        with hang_guard(30, "identity stress"):
            runloom.run(4, main)
        assert all(out[i] is sentinels[i] for i in range(20))


# ---------------------------------------------------------------------------
#  Diag / introspect / crash ARGUMENT VALIDATION (the error branches)
# ---------------------------------------------------------------------------
class TestDiagArgValidation:
    def test_fiber_stack_non_int_is_typeerror(self):
        with pytest.raises(TypeError):
            rc.fiber_stack("not-an-int")
        with pytest.raises(TypeError):
            rc.fiber_stack(None)
        with pytest.raises(TypeError):
            rc.fiber_stack(1.5)   # float has no lossless __index__

    def test_set_deadlock_mode_non_int_is_typeerror(self):
        with pytest.raises(TypeError):
            rc.set_deadlock_mode("x")
        # value still intact after the rejected call
        assert isinstance(rc.get_deadlock_mode(), int)

    def test_set_max_fibers_non_int_is_typeerror(self):
        with pytest.raises(TypeError):
            rc.set_max_fibers("x")
        assert rc.get_max_fibers() == 0

    def test_dump_fibers_bad_fd_does_not_crash(self):
        # A negative fd routes to stderr; a closed/huge fd makes write() fail
        # silently.  Neither may raise or crash (this is called from a signal
        # handler path -- it MUST be tolerant).
        rc.dump_fibers(-1)
        rc.dump_fibers(2 ** 20)   # almost certainly not an open fd

    def test_diag_dump_bad_fd_does_not_crash(self):
        rc._diag_dump(-7)
        rc._diag_dump(2 ** 20)

    def test_install_traceback_signal_bad_signum_raises(self):
        if not POSIX:
            pytest.skip("POSIX-only")
        with pytest.raises(OSError):
            rc.install_traceback_signal(99999)   # out of range -> sigaction EINVAL

    def test_install_crash_handler_unknown_level_falls_back_to_default(self):
        # An unrecognised level string parses to RUNLOOM_CRASH_DEFAULT (not an
        # error) -- documented "on/1/unknown -> default" behaviour.
        try:
            flags = runloom.inspect.install_crash_handler("totally-bogus-level")
            assert isinstance(flags, int) and flags > 0
            assert rc.crash_handler_installed() is True
        finally:
            runloom.inspect.uninstall_crash_handler()
        assert rc.crash_handler_installed() is False

    def test_install_crash_handler_empty_level_uses_default(self):
        try:
            flags = runloom.inspect.install_crash_handler("")
            assert isinstance(flags, int) and flags > 0
        finally:
            runloom.inspect.uninstall_crash_handler()


# ---------------------------------------------------------------------------
#  FINDING: install_crash_handler(file=<unopenable>) silently drops the file
# ---------------------------------------------------------------------------
def test_crash_report_file_unopenable_is_silently_ignored():
    # REGRESSION (was finding #19): install_crash_handler with an explicit
    # report `file=` that CANNOT be opened (parent dir does not exist) now
    # raises OSError instead of silently dropping the open() failure and
    # installing without the file -- matching install_traceback_signal's
    # contract for a bad argument.  The handler must NOT have been installed
    # (the early return is before any signal state is touched).
    rc2, out = run_child("""
        import runloom
        bad = "/this/parent/does/not/exist/crash_report.txt"
        raised = None
        try:
            runloom.inspect.install_crash_handler("on", bad)
        except OSError as e:
            raised = e
        print("RAISED_OSERROR", raised is not None)
        print("INSTALLED", runloom_c.crash_handler_installed())
        print("FILE_EXISTS", os.path.exists(bad))
    """, timeout=30)
    assert rc2 == 0, out
    assert "RAISED_OSERROR True" in out, out
    # the failed install must not have armed the handler, nor created the file
    assert "INSTALLED False" in out, out
    assert "FILE_EXISTS False" in out, out


# ---------------------------------------------------------------------------
#  fiber_stack of a blockpool-PARKED fiber (state == "park")
# ---------------------------------------------------------------------------
class TestFiberStackBlockpool:
    @pytest.mark.skipif(not PUMP_WAKE, reason="single-thread offload needs pump-wake")
    def test_fiber_stack_of_blockpool_parked_fiber(self):
        # A fiber parked in blocking() is PARKED_SAFE ("park").  Walking its
        # Python stack must not crash and must reflect the frame that called
        # blocking() -- the registry walk is independent of the worker thread.
        cap = {}

        def deep_offload():
            rc.blocking(time.sleep, 0.25)

        def main():
            rc.fiber(deep_offload)
            rc.sched_sleep(0.05)
            parked = [g for g in rc.fibers() if g["state"] == "park"]
            cap["parked_states"] = [g["state"] for g in rc.fibers()]
            if parked:
                rep, frames = rc.fiber_stack(parked[0]["id"])
                cap["frames"] = frames
                cap["funcs"] = [name for (_f, _l, name) in frames]

        with hang_guard(30, "fiber_stack blockpool"):
            rc.fiber(main)
            rc.run()
        # there WAS a park-state fiber while the offload was in flight
        assert "park" in cap.get("parked_states", []), cap.get("parked_states")
        # its frame chain (if reconstructable) includes the offloading function
        if "funcs" in cap:
            assert any(n.endswith("deep_offload") for n in cap["funcs"]), cap["funcs"]


# ---------------------------------------------------------------------------
#  Deadlock WARN mode (default 1) end-to-end -- only off/raise were covered
# ---------------------------------------------------------------------------
class TestDeadlockWarnMode:
    def test_deadlock_warn_mode_dumps_but_survives(self):
        # mode=1 (warn): a real deadlock must DUMP a diagnostic to stderr and
        # let run() RETURN (the drain gives up), NOT raise and NOT hang.
        rc2, out = run_child("""
            import runloom.inspect as gi
            gi.set_deadlock_mode('warn')
            runloom.run(1, lambda: runloom_c.Chan(0).recv())
            print("SURVIVED_WARN")
        """, timeout=30)
        assert rc2 == 0, out
        assert "SURVIVED_WARN" in out, out
        # warn mode emits a deadlock diagnostic (some recognizable token)
        assert ("deadlock" in out.lower() or "DEADLOCK" in out
                or "stuck" in out.lower() or "fiber dump" in out.lower()), out


# ---------------------------------------------------------------------------
#  SPAWN_TSTATE fault site (the 3rd spawn fault, untested by the first pass)
# ---------------------------------------------------------------------------
class TestSpawnTstateFault:
    def test_spawn_tstate_fault_once_clean_error_single_thread(self):
        # RUNLOOM_FAULT_SPAWN_TSTATE=once:12 fails the per-g tstate allocation on
        # the first spawn -> the spawn path must hit its cleanup and raise a
        # clean Python error (not crash); a subsequent fault-consumed run works.
        rc2, out = run_child("""
            raised = False
            try:
                runloom.run(1, lambda: None)
            except (MemoryError, RuntimeError, OSError):
                raised = True
            done = []
            def main():
                for _ in range(15):
                    runloom.fiber(lambda: done.append(1))
                runloom.sleep(0.02)
            runloom.run(1, main)
            print("RESULT", raised, len(done) == 15, runloom_c._self_check(0) == 0)
        """, extra_env={"RUNLOOM_FAULT_SPAWN_TSTATE": "once:12"}, timeout=40)
        assert rc2 == 0, out
        # Either the fault surfaced as a clean error, or this build injects the
        # tstate fault elsewhere -- in BOTH cases there must be NO crash and the
        # recovery run must complete + self-check clean.
        assert "RESULT" in out, out
        tail = out.split("RESULT", 1)[1].split()
        assert tail[1] == "True" and tail[2] == "True", out

    @mn_only
    def test_spawn_tstate_fault_once_clean_error_mn(self):
        rc2, out = run_child("""
            raised = False
            try:
                runloom.run(4, lambda: None)
            except (MemoryError, RuntimeError, OSError):
                raised = True
            done = []
            def main():
                for _ in range(20):
                    runloom.fiber(lambda: done.append(1))
                runloom.sleep(0.02)
            runloom.run(4, main)
            print("RESULT", len(done) == 20, runloom_c._self_check(0) == 0)
        """, extra_env={"RUNLOOM_FAULT_SPAWN_TSTATE": "once:12"}, timeout=40)
        assert rc2 == 0, out
        assert "RESULT True True" in out, out


# ---------------------------------------------------------------------------
#  reset_after_fork idempotency in the MAIN process (no fork)
# ---------------------------------------------------------------------------
class TestResetAfterForkIdempotent:
    def test_reset_after_fork_in_main_process_is_safe(self):
        # Calling reset_after_fork() outside any fork (main process, no live
        # runtime) re-inits the global locks/registry and must leave a USABLE
        # runtime: a subsequent run() works and the registry is clean.  Do it in
        # a subprocess so re-initing process-global locks can't disturb the
        # in-process conftest invariants.
        rc2, out = run_child("""
            # reset twice back-to-back, then run -- must be a clean no-op-ish reset
            runloom_c.reset_after_fork()
            runloom_c.reset_after_fork()
            assert runloom_c.fiber_count() == 0
            assert runloom_c.fibers() == []
            done = []
            runloom.run(1, lambda: [runloom.fiber(lambda: done.append(1)) for _ in range(10)])
            print("RESULT", len(done) == 10, runloom_c._self_check(0) == 0,
                  runloom_c.fiber_count() == 0)
        """, timeout=30)
        assert rc2 == 0, out
        assert "RESULT True True True" in out, out


# ---------------------------------------------------------------------------
#  Concurrent / double guard-page overflow under M:N (the in-progress latch)
# ---------------------------------------------------------------------------
@requires_guard
class TestConcurrentCrash:
    @mn_only
    def test_two_fibers_overflow_concurrently_serialize_and_die_once(self):
        # Two fibers on two hubs both run unbounded C recursion off small stacks,
        # faulting into their guard pages at ~the same time.  The crash handler's
        # in-progress latch must serialise: ONE thread drives the dump + chains
        # out + cores; the OTHER pause()s.  Result: a clean fatal signal (not a
        # hang, not a double-dump interleave, not a wedged limp-on).  Contained
        # in a subprocess as a negative returncode.
        rc2, out = run_child("""
            runloom.inspect.install_crash_handler("on")
            runloom_c.mn_init(2)
            def boom():
                runloom_c._crash_selftest_overflow()
            runloom_c.mn_fiber(boom, 16384)
            runloom_c.mn_fiber(boom, 16384)
            runloom_c.mn_run()
            print("UNREACHABLE")
        """, timeout=40)
        assert rc2 in FAULT_RCS, (rc2, out)             # it died from the fault
        assert "UNREACHABLE" not in out, out
        # Exactly ONE crash banner -- the latch prevented an interleaved second
        # full dump (a second thread that won the latch would print a 2nd banner).
        assert out.count("======================== runloom crash") == 1, out
        assert "GOROUTINE STACK OVERFLOW" in out, out


# ---------------------------------------------------------------------------
#  stats()/introspection consistency MID-OFFLOAD
# ---------------------------------------------------------------------------
class TestStatsDuringOffload:
    @pytest.mark.skipif(not PUMP_WAKE, reason="single-thread offload needs pump-wake")
    def test_stats_consistent_while_fibers_blockpool_parked(self):
        # While several fibers are parked in blocking(), stats() must stay
        # internally consistent (all expected keys present, counts non-negative)
        # and _self_check must remain 0 -- a blockpool-parked fiber has no
        # netpoll/iouring footprint, so it must not inflate netpoll_parked.
        cap = {"snaps": [], "viol": []}

        def offloader():
            rc.blocking(time.sleep, 0.25)

        def sampler():
            for _ in range(40):
                s = rc.stats()
                cap["snaps"].append(s)
                cap["viol"].append(rc._self_check(0))
                rc.sched_yield()

        def main():
            for _ in range(6):
                rc.fiber(offloader)
            rc.fiber(sampler)
            rc.sched_sleep(0.3)

        with hang_guard(30, "stats during offload"):
            rc.fiber(main)
            rc.run()
        assert all(v == 0 for v in cap["viol"]), set(cap["viol"])
        for s in cap["snaps"]:
            for k in ("ready", "sleeping", "netpoll_parked",
                      "netpoll_parked_self", "running"):
                assert k in s and s[k] >= 0, (k, s)
        assert rc._self_check(0) == 0


# ---------------------------------------------------------------------------
#  blocking() racing reset/teardown harder: a partial-completion main return
# ---------------------------------------------------------------------------
class TestBlockingTeardownHarder:
    @mn_only
    def test_main_returns_with_all_offloads_still_pending(self):
        # main() spawns offloaders that each sleep LONGER than main's own brief
        # wait, then returns -- so EVERY offload is still in flight at main()
        # return.  The M:N drain must wait out the inflight pool jobs (the
        # inflight counter + wake_g), completing them all, before run() returns.
        # Subprocess so a lost-wake teardown is a bounded TIMEOUT.
        rc2, out = run_child("""
            done = []
            def w(i):
                runloom_c.blocking(time.sleep, 0.05)
                done.append(i)
            def main():
                for i in range(30):
                    runloom.fiber(lambda i=i: w(i))
                runloom.sleep(0.005)   # return almost immediately; all still pending
            runloom.run(4, main)
            print("DONE", len(done))
            assert runloom_c._self_check(0) == 0
            print("SELFCHECK_OK")
        """, timeout=40)
        assert rc2 == 0, out
        assert "DONE 30" in out and "SELFCHECK_OK" in out, out

    @mn_only
    def test_single_worker_pool_serializes_but_drains_at_teardown(self):
        # WORKERS=1 + every offload pending at main-return: the single worker
        # must still drain all 20 jobs SERIALLY before run() returns (no job
        # abandoned, no teardown hang on the lone worker).
        rc2, out = run_child("""
            done = []
            def w(i):
                runloom_c.blocking(time.sleep, 0.01)
                done.append(i)
            def main():
                for i in range(20):
                    runloom.fiber(lambda i=i: w(i))
                runloom.sleep(0.005)
            runloom.run(4, main)
            print("DONE", sorted(done) == list(range(20)))
        """, extra_env={"RUNLOOM_BLOCKPOOL_WORKERS": "1"}, timeout=40)
        assert rc2 == 0, out
        assert "DONE True" in out, out


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
