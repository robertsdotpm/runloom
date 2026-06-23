"""Adversarial coverage suite for src/runloom_c/mn_sched_init_fini.c.inc.

This fragment is the M:N scheduler's init / fini / spawn-core / fiber_n-bulk /
deadlock-census code.  Most of its UNCOVERED lines live behind error-cleanup,
teardown-drain, and bulk-arena-fallback branches the normal corpus never trips.
Each test below drives ONE such dark region with a real adverse condition and
asserts the behaviour that line implements -- not just that it executed.

Almost every test runs in a SUBPROCESS, because:
  * env-gated modes (RUNLOOM_GON_BULK, RUNLOOM_STACK_ARENA_N, the SPAWN_G fault,
    RUNLOOM_GILSTATE_DELETE_ON_MAIN) are read once at import / first-run, so the
    parent pytest's already-imported runloom_c has them fixed; and
  * the error/teardown paths call mn_init/mn_fini directly and/or shrink process
    rlimits, which must not perturb the parent runtime or the autouse
    self-check / parked-leak fixture.
For gcov to count a subprocess's lines it MUST exit cleanly (a crash/_exit does
not flush counters), so every worker prints a unique stdout marker and we assert
both the marker AND returncode == 0.

Regions driven (uncovered line -> how):
  L146-172  thread_create failure cleanup  -> shrink RLIMIT_NPROC so hub
            pthread_creates EAGAIN partway -> mn_init raises OSError.
  L241-242  ready-ring leftover drain at fini -> forever-yielders + mn_fini
            WITHOUT mn_run (hubs stopped mid-cycle, ready ring non-empty).
  L249-250  sleep-heap leftover drain at fini -> long-sleepers + mn_fini
            WITHOUT mn_run (gs sitting in the sleep heap when the hub stops).
  L333-335  hub-tstate delete on the MAIN thread (negative control) ->
            RUNLOOM_GILSTATE_DELETE_ON_MAIN leaves h->tstate live for fini.
  L480-491  runloom_mn_fiber_core coro==NULL cleanup -> RLIMIT_AS capped just above
            VmSize + an 8 MiB explicit stack so coro_new's mmap fails; asserts
            the admission slot was RELEASED (set_max_fibers -> limit_counted).
  L694-698  fiber_n bulk-arena coro_init FAILURE fallback -> RUNLOOM_GON_BULK=1 +
            RUNLOOM_STACK_ARENA_N=1 (1-slot arena) so the bulk path falls back
            to the per-g mn_fiber_core loop; asserts all (indexed) fibers still ran.
  L742-744  fiber_n bulk splice signalling an IDLE hub's cond -> GON_BULK + let the
            hubs idle, then a bulk fiber_n.
  L764      fiber_n non-bulk loop spawn-failure return -1 -> RUNLOOM_FAULT_SPAWN_G
            forces the first slab alloc to fail; fiber_n raises.

See the module docstring's `unreachable` notes in the structured report for the
per-g-tstate / OOM-only / lost-wakeup lines that have no SAFE trigger.
"""
import os
import subprocess
import sys
import textwrap

import pytest

from adv_util import needs_free_threading

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

pytestmark = pytest.mark.skipif(not FT, reason="M:N scheduler needs GIL-disabled build")

# The thread-spawn-failure and coro-alloc-failure coverage drivers below inject
# their adverse condition via Linux-only mechanisms: RLIMIT_NPROC (which caps
# THREADS per-uid on Linux but only fork()'d PROCESSES on Darwin/BSD, so
# pthread_create never EAGAINs there -> mn_init succeeds), and /proc/self/status
# (absent on macOS). There is no in-process knob to trip the mn_init
# thread_create cleanup branch on macOS, so these are Linux-specific gcov drivers.
_LINUX_ONLY = pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="RLIMIT_NPROC thread cap + /proc/self/status are Linux-specific "
           "(Darwin RLIMIT_NPROC limits fork() not threads; no /proc)")


def _run_worker(body, env_extra=None, timeout=60):
    """Run a worker snippet in a fresh subprocess; return CompletedProcess.

    `body` is dedented and prefixed with the standard src-on-path + import so
    each test only writes the adversarial part.
    """
    src = "import sys\nsys.path.insert(0, 'src')\nimport runloom_c as rc\n" + textwrap.dedent(body)
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    if env_extra:
        env.update(env_extra)
    return subprocess.run([PY, "-c", src], cwd=REPO, env=env,
                          capture_output=True, text=True, timeout=timeout)


def _assert_clean(p, marker):
    assert p.returncode == 0, (
        "worker crashed (rc=%d)\nstdout=%s\nstderr=%s"
        % (p.returncode, p.stdout[-1500:], p.stderr[-1500:]))
    assert marker in p.stdout, (
        "worker did not reach %r\nstdout=%s\nstderr=%s"
        % (marker, p.stdout[-1500:], p.stderr[-1500:]))


# --------------------------------------------------------------------------
# L146-172 : runloom_thread_create failure -> mn_init partial-cleanup + OSError
# --------------------------------------------------------------------------
@_LINUX_ONLY
def test_mn_init_thread_spawn_failure_first_hub():
    """All hub pthread_creates fail (RLIMIT_NPROC=1) -> mn_init marks every hub
    stopping, restores the saved tstate, frees runloom_hubs, sets OSError, and
    returns -1.  Drives L146-148, L153-160, L164-172 (the i==0 variant: the
    j<i join loop runs zero times).  Asserts mn_init raised OSError with the
    exact message and the worker exits cleanly so gcov flushes.

    NOTE: a SECOND mn_init() AFTER a spawn failure SIGSEGVs in this build (the
    cleanup path leaves some global state inconsistent), so we deliberately do
    NOT attempt recovery -- a crash would prevent the just-run cleanup lines
    from flushing their counters.  See the structured-report notes."""
    body = """
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NPROC)
        resource.setrlimit(resource.RLIMIT_NPROC, (1, hard))   # no new threads
        try:
            n = rc.mn_init(48)
            print("UNEXPECTED_OK", n)
        except OSError as e:
            print("OSERROR", str(e))
        finally:
            resource.setrlimit(resource.RLIMIT_NPROC, (soft, hard))
        print("SPAWNFAIL_FIRST_OK")
    """
    p = _run_worker(body)
    _assert_clean(p, "SPAWNFAIL_FIRST_OK")
    assert "OSERROR thread spawn failed" in p.stdout, (
        "mn_init did not raise OSError('thread spawn failed') on spawn failure\n"
        + p.stdout)
    assert "UNEXPECTED_OK" not in p.stdout


@_LINUX_ONLY
def test_mn_init_thread_spawn_failure_partial():
    """A FEW hub threads spawn, then EAGAIN -> mn_init also runs the j<i join
    loop (L149-152) over the already-spawned hubs before unwinding.  We grant
    exactly ~3 threads of headroom over the current thread count so the failure
    lands partway through the spawn loop (i > 0).  As above, NO recovery is
    attempted (a post-failure mn_init crashes), so the worker exits cleanly."""
    body = """
        import resource
        def cur_threads():
            with open("/proc/self/status") as f:
                for line in f:
                    if line.startswith("Threads:"):
                        return int(line.split()[1])
            return -1
        base = cur_threads()
        soft, hard = resource.getrlimit(resource.RLIMIT_NPROC)
        resource.setrlimit(resource.RLIMIT_NPROC, (base + 3, hard))   # a few succeed
        raised = False
        try:
            rc.mn_init(32)
            print("UNEXPECTED_OK")
        except OSError:
            raised = True
        finally:
            resource.setrlimit(resource.RLIMIT_NPROC, (soft, hard))
        assert raised, "partial spawn did not raise"
        print("SPAWNFAIL_PARTIAL_OK")
    """
    p = _run_worker(body)
    _assert_clean(p, "SPAWNFAIL_PARTIAL_OK")


# --------------------------------------------------------------------------
# L249-250 : sleep-heap leftover drain in runloom_mn_hub_drain_leftovers
# --------------------------------------------------------------------------
def test_fini_drains_sleep_heap_leftovers():
    """Fibers parked in a long sched_sleep are sitting in their hub's sleep heap
    when mn_fini() runs WITHOUT mn_run().  hub_drain_leftovers must pop + decref
    them (L248-251).  We prove the drain was sound by checking _self_check stays
    at 0 violations after fini (a leaked / double-freed g would trip it)."""
    body = """
        import time
        rc.mn_init(3)
        def sleeper():
            rc.sched_sleep(1000.0)   # effectively forever -> stuck in sleep heap
        for _ in range(8):
            rc.mn_fiber(sleeper)
        time.sleep(0.25)             # let the hubs consume + park them in sleep heap
        assert rc._self_check(0) == 0
        rc.mn_fini()                 # drains the sleep heap leftovers
        assert rc._self_check(0) == 0, "self-check tripped after sleep-heap drain"
        print("SLEEP_DRAIN_OK")
    """
    p = _run_worker(body)
    _assert_clean(p, "SLEEP_DRAIN_OK")


# --------------------------------------------------------------------------
# L241-242 (+ L244-247) : ready-ring / deque leftover drain at fini
# --------------------------------------------------------------------------
def test_fini_drains_ready_ring_leftovers():
    """Forever-yielding fibers keep cycling through their hub's ready ring (and
    steal deque).  mn_fini() WITHOUT mn_run() stops the hubs mid-cycle, so the
    ready ring / deque hold runnable gs that hub_drain_leftovers must pop +
    decref (L240-247).  With many spinners per hub the ready ring is virtually
    never empty at the stop instant.  _self_check == 0 proves the drain freed
    them without corrupting the structures."""
    body = """
        import time
        rc.mn_init(2)
        def spinner():
            while True:
                rc.sched_yield()    # re-queues self onto the ready ring forever
        for _ in range(48):
            rc.mn_fiber(spinner)
        time.sleep(0.25)            # hubs pick them up and start cycling
        rc.mn_fini()               # stops hubs mid-cycle -> ready/deque drain
        assert rc._self_check(0) == 0, "self-check tripped after ready-ring drain"
        print("READY_DRAIN_OK")
    """
    p = _run_worker(body)
    _assert_clean(p, "READY_DRAIN_OK")


# --------------------------------------------------------------------------
# L333-335 : hub tstate deleted on the MAIN thread (negative control)
# --------------------------------------------------------------------------
def test_fini_deletes_hub_tstate_on_main():
    """RUNLOOM_GILSTATE_DELETE_ON_MAIN makes each hub LEAVE its tstate alive
    (the pre-c28e5ca bug path) instead of self-deleting it, so h->tstate is
    non-NULL at fini and the main-thread sweep clears + deletes it
    (L328-336: the gilstate trace + PyThreadState_Clear/Delete).  On this
    non-pydebug build that is benign; we assert a full mn lifecycle completes
    cleanly under the knob and the runtime is reusable afterwards."""
    body = """
        ran = []
        def main():
            rc.mn_fiber(lambda: ran.append(1))
        rc.mn_init(3); rc.mn_fiber(main); rc.mn_run(); rc.mn_fini()
        assert ran, "fiber did not run"
        # Reusable after the main-thread tstate delete path:
        rc.mn_init(2); rc.mn_fiber(lambda: None); rc.mn_run(); rc.mn_fini()
        print("DELETE_ON_MAIN_OK")
    """
    p = _run_worker(body, env_extra={"RUNLOOM_GILSTATE_DELETE_ON_MAIN": "1"})
    _assert_clean(p, "DELETE_ON_MAIN_OK")


# --------------------------------------------------------------------------
# L480-491 : runloom_mn_fiber_core coro==NULL cleanup (stack mmap fails)
# --------------------------------------------------------------------------
@_LINUX_ONLY
def test_mn_fiber_core_coro_alloc_failure_releases_admission():
    """Cap RLIMIT_AS just above the current VmSize, then mn_fiber() an 8 MiB stack:
    the fresh-size stack mmap inside runloom_coro_new fails -> coro == NULL ->
    mn_fiber_core runs its cleanup (Py_DECREF callable, PyErr_NoMemory, release the
    admission slot, slab_free, errno=ENOMEM, return -1).  With set_max_fibers
    active the admission slot was COUNTED (limit_counted==1), so the cleanup MUST
    decrement live_fibers back -- we assert that (L485-487) and that mn_fiber raised
    MemoryError (L483)."""
    body = """
        import resource
        def vmsize():
            with open("/proc/self/status") as f:
                for line in f:
                    if line.startswith("VmSize:"):
                        return int(line.split()[1]) * 1024
            return -1
        rc.set_max_fibers(100000)        # admit() returns 2 -> limit_counted=1
        rc.mn_init(2)
        live0 = rc.live_fibers()
        soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        cap = vmsize() + 8 * 1024 * 1024 # ~8 MiB headroom: a fresh 8 MiB stack won't fit
        resource.setrlimit(resource.RLIMIT_AS, (cap, hard))
        raised = False
        try:
            rc.mn_fiber(lambda: None, 8 * 1024 * 1024)
            print("UNEXPECTED_SPAWN_OK")
        except MemoryError:
            raised = True
        finally:
            resource.setrlimit(resource.RLIMIT_AS, (soft, hard))
        live1 = rc.live_fibers()
        rc.mn_run(); rc.mn_fini(); rc.set_max_fibers(0)
        assert raised, "coro_new failure did not raise MemoryError"
        assert live1 == live0, ("admission slot leaked: live %d -> %d" % (live0, live1))
        print("CORO_ALLOC_FAIL_OK")
    """
    p = _run_worker(body)
    _assert_clean(p, "CORO_ALLOC_FAIL_OK")


# --------------------------------------------------------------------------
# L764 : runloom_mn_fiber_n non-bulk loop -- a mid-loop spawn failure returns -1
# --------------------------------------------------------------------------
def test_fiber_n_loop_spawn_failure_returns_error():
    """RUNLOOM_FAULT_SPAWN_G=always:12 forces every g slab alloc to fail.  In the
    non-bulk fiber_n loop (GON_BULK unset), the first mn_fiber_core fails -> fiber_n
    returns -1 with a Python error set (L761-765, the L764 return).  Asserts
    fiber_n raised."""
    body = """
        rc.mn_init(2)
        raised = False
        try:
            rc.fiber_n(lambda: None, 4, 0, False)
            print("UNEXPECTED_GON_OK")
        except (MemoryError, RuntimeError):
            raised = True
        rc.mn_run(); rc.mn_fini()
        assert raised, "fiber_n did not raise on a spawn failure"
        print("GON_LOOP_FAIL_OK")
    """
    p = _run_worker(body, env_extra={"RUNLOOM_FAULT_SPAWN_G": "always:12"})
    _assert_clean(p, "GON_LOOP_FAIL_OK")


# --------------------------------------------------------------------------
# L694-698 : fiber_n bulk-arena path falls back to the per-g loop on arena failure
# --------------------------------------------------------------------------
def test_fiber_n_bulk_arena_failure_falls_back_to_per_g_loop():
    """RUNLOOM_GON_BULK=1 takes the bulk-arena spawn path; RUNLOOM_STACK_ARENA_N=1
    makes the stack arena hold a single slot, so runloom_arena_alloc(n>1) fails
    and runloom_coro_bulk_init returns -1.  go_n_bulk then frees its arenas and
    FALLS BACK to the per-g mn_fiber_core loop (L694-698), re-spawning each fiber
    with its index.  We assert every indexed fiber still ran with the CORRECT
    index (the fallback passes `indexed ? i : -1`)."""
    body = """
        from runloom.sync import WaitGroup
        N = 8
        seen = bytearray(N)
        def main():
            wg = WaitGroup(); wg.add(N)
            def w(i):
                if 0 <= i < N:
                    seen[i] = 1
                wg.done()
            rc.fiber_n(lambda i: w(i), N, 0, True)   # bulk -> arena fail -> per-g fallback
            wg.wait()
        rc.mn_init(3); rc.mn_fiber(main); rc.mn_run(); rc.mn_fini()
        assert sum(seen) == N, ("only %d/%d fibers ran via the fallback" % (sum(seen), N))
        print("GON_BULK_FALLBACK_OK")
    """
    p = _run_worker(body, env_extra={"RUNLOOM_GON_BULK": "1",
                                     "RUNLOOM_STACK_ARENA_N": "1"})
    _assert_clean(p, "GON_BULK_FALLBACK_OK")


# --------------------------------------------------------------------------
# L742-744 : fiber_n bulk splice signals an IDLE hub's condvar
# --------------------------------------------------------------------------
def test_fiber_n_bulk_wakes_idle_hubs():
    """RUNLOOM_GON_BULK=1 with a real (large) arena -> the bulk path SUCCEEDS and
    splices each hub's whole batch under one lock.  We first let the hubs settle
    into their idle condvar wait, then issue the bulk fiber_n; the per-hub splice
    finds idle_waiting set and signals idle_cond (L741-744) to drain the batch
    promptly.  Asserts every fiber ran within a generous bound (a missed wake
    would strand the batch until idle_ns expiry / hang)."""
    body = """
        import time
        from runloom.sync import WaitGroup
        N = 32
        seen = bytearray(N)
        def main():
            rc.sched_sleep(0.06)     # let the OTHER hubs reach their idle condvar wait
            wg = WaitGroup(); wg.add(N)
            def w(i):
                if 0 <= i < N:
                    seen[i] = 1
                wg.done()
            t0 = time.monotonic()
            rc.fiber_n(lambda i: w(i), N, 0, True)   # bulk splice across now-idle hubs
            wg.wait()
            # The whole batch should drain quickly via the idle-cond signal.
            assert time.monotonic() - t0 < 5.0, "bulk batch was slow to wake"
        rc.mn_init(4); rc.mn_fiber(main); rc.mn_run(); rc.mn_fini()
        assert sum(seen) == N, ("only %d/%d bulk fibers ran" % (sum(seen), N))
        print("GON_BULK_IDLEWAKE_OK")
    """
    p = _run_worker(body, env_extra={"RUNLOOM_GON_BULK": "1"})
    _assert_clean(p, "GON_BULK_IDLEWAKE_OK")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
