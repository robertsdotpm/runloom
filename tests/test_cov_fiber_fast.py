"""Gated functional coverage for runloom.fiber_fast / runloom_c.fiber_fast.

fiber_fast is the raw-throughput spawn entry (the M:N fire-and-forget fast path
that bypasses the grow-down auto-sizer and the Python _fiber_full frame).  It had
ZERO gated functional coverage.  This file pins its two documented behaviours:

  * under run(1) (single-thread / M:1): mn_hub_count()==0, so fiber_fast has no
    hub to fire onto and DELEGATES to the registered _fiber_full wrapper -- it
    spawns on this thread's scheduler and returns a working ``runloom.Goroutine``
    handle (``.done`` / ``.result`` / ``.exception`` all reflect the fiber).

  * under run(n>1) (M:N): mn_hub_count()>0, so fiber_fast takes the C fast path
    (runloom_mn_fiber) -- a fire-and-forget spawn that round-robins onto the hubs
    and returns None (no join handle in M:N v1).  Correctness oracle: every one
    of N goroutines runs to completion, checked with a race-free ``bytearray(N)``
    (one slot per goroutine -- a shared ``+= 1`` would lose increments GIL-off).

Each workload runs in a FRESH SUBPROCESS (like tests/test_mn.py), for the same
two reasons: mn_init/mn_fini install process-global hub threads (a clean process
per test avoids cross-test contamination), and a scheduler regression can SIGSEGV
or hang -- a subprocess turns that into a clean test failure, not a dead pytest
run.  Subprocesses run free-threaded (PYTHON_GIL=0) so run(n>1) genuinely spreads
across hubs, and PYTHON_TLBC=0 (set at launch) both disables the CPython 3.14t
TLBC crash and makes runloom.run()'s re-exec-with-TLBC-off a no-op -- so the
high-level runloom.run() we exercise here runs in-process, not via a re-exec.
"""
import os
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def run_snippet(code, timeout=60):
    """Run a runloom snippet in a fresh free-threaded subprocess.
    Returns (returncode, stdout, stderr).  The snippet prints 'PASS' on
    success."""
    preamble = (
        "import sys; sys.path.insert(0, %r)\n"
        "import runloom, runloom_c, threading\n" % os.path.join(REPO, "src")
    )
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"            # force GIL off: real parallel hubs
    env["RUNLOOM_GIL"] = "0"
    env["PYTHON_TLBC"] = "0"           # 3.14t TLBC off + run() re-exec becomes a no-op
    env["RUNLOOM_TLBC_REEXEC"] = "1"   # belt-and-suspenders: never re-exec pytest
    env["RUNLOOM_GOROUTINE_PANIC"] = "silent"  # a deliberately-raising fiber shouldn't spam stderr
    try:
        p = subprocess.run(
            [sys.executable, "-c", preamble + code],
            cwd=REPO, env=env, timeout=timeout,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except subprocess.TimeoutExpired as e:
        # A wedge / lost-wake hang: surface as rc=124 (like coreutils timeout)
        # rather than letting TimeoutExpired escape as a test error.
        out = e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or "")
        err = e.stderr.decode() if isinstance(e.stderr, bytes) else (e.stderr or "")
        return 124, out, err + "\n[run_snippet: timed out after {0}s]".format(timeout)
    return p.returncode, p.stdout, p.stderr


def assert_pass(code, timeout=60):
    rc, out, err = run_snippet(code, timeout=timeout)
    assert rc == 0 and "PASS" in out, (
        "rc={0}\n--- stdout ---\n{1}\n--- stderr ---\n{2}".format(rc, out, err))
    return out


# ---------------------------------------------------------------------------
# run(1): fiber_fast delegates and returns a WORKING Goroutine.
# ---------------------------------------------------------------------------
def test_run1_returns_working_goroutine():
    """Under run(1), fiber_fast(fn) spawns on the single-thread scheduler and
    returns a runloom.Goroutine whose .done/.result track the fiber: fn runs to
    completion and its return value lands in g.result."""
    assert_pass(r"""
ran = []
def child():
    ran.append('x')
    return 4242
g = runloom.fiber_fast(child)
assert isinstance(g, runloom.Goroutine), ('not a Goroutine', type(g))
# Single-thread: nothing is drained until run(), so the fiber is queued not done.
assert g.done is False, ('done before run', g.done)
completed = runloom.run(1)
assert g.done is True, ('fiber not done after run(1)', g.done)
assert g.result == 4242, ('return value not captured in .result', g.result)
assert g.exception is None, ('unexpected exception', g.exception)
assert ran == ['x'], ('child did not run exactly once', ran)
assert completed >= 1, ('run(1) reported nothing completed', completed)
assert runloom_c._self_check(0) == 0
print('PASS')
""", timeout=30)


def test_run1_c_entry_symbol_identity():
    """runloom.fiber_fast IS runloom_c.fiber_fast, and calling the C entry
    directly under run(1) also returns a working Goroutine (the delegation to the
    registered _fiber_full wrapper is wired at import runloom time)."""
    assert_pass(r"""
assert runloom.fiber_fast is runloom_c.fiber_fast
box = []
def child():
    box.append(7)
    return 'ok'
g = runloom_c.fiber_fast(child)          # the raw C entry, not the re-export
assert isinstance(g, runloom.Goroutine), ('not a Goroutine', type(g))
runloom.run(1)
assert g.done is True, ('not done', g.done)
assert g.result == 'ok', ('bad result', g.result)
assert box == [7], ('child did not run', box)
assert runloom_c._self_check(0) == 0
print('PASS')
""", timeout=30)


def test_run1_nested_spawn_and_exception_capture():
    """fiber_fast called from INSIDE a run(1) main is still a single-thread spawn
    (returns a Goroutine, not None), and the returned handle faithfully reports
    both a normal return (.result) and an escaped exception (.exception)."""
    assert_pass(r"""
holder = {}
def good():
    return 11
def bad():
    raise ValueError('boom')
def main():
    holder['ok'] = runloom.fiber_fast(good)
    holder['bad'] = runloom.fiber_fast(bad)
    # Spawned from inside run(1) -> still single-thread -> real Goroutine handles.
    assert isinstance(holder['ok'], runloom.Goroutine), type(holder['ok'])
    assert isinstance(holder['bad'], runloom.Goroutine), type(holder['bad'])
runloom.run(1, main)
g_ok, g_bad = holder['ok'], holder['bad']
assert g_ok.done and g_ok.result == 11, ('good fiber', g_ok.done, g_ok.result)
assert g_bad.done, ('bad fiber not done', g_bad.done)
assert isinstance(g_bad.exception, ValueError), ('exception not captured', repr(g_bad.exception))
assert g_bad.result is None, ('raising fiber has no result', g_bad.result)
assert runloom_c._self_check(0) == 0
print('PASS')
""", timeout=30)


# ---------------------------------------------------------------------------
# run(n>1): fiber_fast is fire-and-forget onto the hubs; every goroutine runs.
# ---------------------------------------------------------------------------
def test_mn_fire_and_forget_all_run():
    """Under run(n>1), fiber_fast round-robins N goroutines onto the hubs
    fire-and-forget: each returns None (no join handle), every one runs to
    completion (race-free bytearray(N) oracle, one slot per goroutine), and the
    work genuinely spreads across more than one hub OS thread."""
    assert_pass(r"""
N = 2000
H = 4
oracle = bytearray(N)          # one race-free slot per goroutine (no shared += 1)
tids = [0] * N                 # each child records its hub's OS-thread id (own slot)
rets = []                      # written only by the single main fiber -> no race
def mk(k):
    def w():
        for _ in range(3):
            runloom.yield_now()          # force real hub interleave / work-stealing
        tids[k] = threading.get_ident()
        oracle[k] = 1
    return w
def main():
    for k in range(N):
        rets.append(runloom.fiber_fast(mk(k)))
completed = runloom.run(H, main)
assert sum(oracle) == N, ('not every goroutine ran', sum(oracle), N)
assert oracle == bytearray([1]) * N, 'a goroutine slot was missed'
assert set(rets) == {None}, ('M:N fiber_fast must be fire-and-forget (None)', set(rets) - {None})
assert completed >= N, ('run(H) under-counted completions', completed, N)
distinct = len(set(tids))
assert distinct >= 2, ('round-robin did not spread across hubs', distinct, H)
assert runloom_c._self_check(0) == 0
print('PASS distinct_hubs=%d completed=%d' % (distinct, completed))
""", timeout=60)


if __name__ == "__main__":
    test_run1_returns_working_goroutine()
    test_run1_c_entry_symbol_identity()
    test_run1_nested_spawn_and_exception_capture()
    test_mn_fire_and_forget_all_run()
    print("all ok")
