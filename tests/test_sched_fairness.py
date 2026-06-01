"""Scheduler fairness / parallelism tests, ported in spirit from Go's
runtime/proc_test.go.

pygo IS a Go-style M:N scheduler (single sched + N OS-thread hubs, a Chase-Lev
work-stealing deque per hub, a ready ring for woken/yielded gs).  Go's
proc_test.go has explicit guards for exactly the failure modes this scheduler
can have -- and one of them (yield starvation, TestYieldProgress) is the class
of bug just fixed in 7a93a3e.  These are the standing regressions for it.

Each workload runs in a fresh free-threaded subprocess (PYTHON_GIL=0) so the
hubs run in genuine parallel.  The dominant failure signal is the subprocess
TIMEOUT: mn_run() only returns when every goroutine has finished, so a starved
or never-scheduled goroutine wedges mn_run forever -> rc 124 -> clean failure
(not a hung pytest).

Go originals (golang/go, src/runtime/proc_test.go):
  TestYieldProgress / TestYieldLocked, TestGoroutineParallelism{,2},
  the work-stealing runq tests, and async preemption (TestPreemption).

What is asserted vs. what Go asserts:
  * Fairness/no-starvation -> completion (no timeout), same as Go.
  * Parallelism -> work observably runs on >1 hub OS-thread (Go uses a tighter
    in-loop check; the OS-thread spread is the robust, non-flaky proxy).
  * Preemption -> a goroutine in a tight loop with NO explicit yield still lets
    a sibling run.  Requires pygo's eval-wrapper preemption (default-on);
    skipped only if PYGO_PREEMPT=0 is set explicitly.
"""
import os
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def run_mn(code, timeout=30):
    preamble = (
        "import sys; sys.path.insert(0, %r)\n"
        "import pygo_core\n" % os.path.join(REPO, "src")
    )
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["PYGO_GIL"] = "0"
    try:
        p = subprocess.run(
            [sys.executable, "-c", preamble + code],
            cwd=REPO, env=env, timeout=timeout,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except subprocess.TimeoutExpired as e:
        out = e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or "")
        err = e.stderr.decode() if isinstance(e.stderr, bytes) else (e.stderr or "")
        return 124, out, err + "\n[run_mn: timed out after {0}s]".format(timeout)
    return p.returncode, p.stdout, p.stderr


def assert_pass(code, timeout=30):
    rc, out, err = run_mn(code, timeout=timeout)
    assert rc == 0 and "PASS" in out, (
        "rc={0}\n--- stdout ---\n{1}\n--- stderr ---\n{2}".format(rc, out, err))
    return out


# ---------------------------------------------------------------------------
# TestYieldProgress -- many goroutines all yielding must each make full
# progress; none is starved.  (mn_run only returns when ALL finish, so a hang
# == starvation.)  Single- and multi-hub.
# ---------------------------------------------------------------------------
def test_yield_round_robin_all_progress():
    """N goroutines each loop `incr; sched_yield` for R rounds.  All must
    complete and each must have run exactly R times.  Exercises the optimized
    yield fast path (the one the fairness fix bounded) under heavy contention,
    at H=1 (pure round-robin) and H=4 (cross-hub)."""
    assert_pass(r"""
def run(nhubs, n, rounds):
    counts = [0] * n
    done = pygo_core.Chan(n)
    def mk(k):
        def w():
            for _ in range(rounds):
                counts[k] += 1
                pygo_core.sched_yield()
            done.send(1)
        return w
    pygo_core.mn_init(nhubs)
    for k in range(n):
        pygo_core.mn_go(mk(k))
    pygo_core.mn_run()
    fin = 0
    for _ in range(n):
        if done.try_recv() is None: break
        fin += 1
    pygo_core.mn_fini()
    assert fin == n, ("not all finished", fin, n)
    assert all(c == rounds for c in counts), "uneven progress"
    assert pygo_core._self_check(0) == 0

run(1, 64, 200)
run(4, 256, 200)
print("PASS")
""", timeout=30)


# ---------------------------------------------------------------------------
# Anti-starvation: spinners that only sched_yield must not starve real workers,
# even when the spinners are spawned first and monopolize the hubs.  (General
# N-worker form of test_mn.test_sched_yield_no_starvation_*.)
# ---------------------------------------------------------------------------
def test_spinners_dont_starve_workers():
    """8 goroutines busy-looping on sched_yield, spawned FIRST, must not
    prevent a batch of real workers spawned afterward from running to
    completion.  A starved worker => done barrier never fills => mn_run hangs
    => timeout.  This is the multi-worker generalization of the sched_yield
    fairness regression (7a93a3e)."""
    assert_pass(r"""
NWORK = 64
stop = [False]
done = pygo_core.Chan(NWORK)
def spinner():
    while not stop[0]:
        pygo_core.sched_yield()
def worker(k):
    def w():
        s = 0
        for i in range(100):
            s += i
            pygo_core.sched_yield_classic()
        done.send(s)
    return w
pygo_core.mn_init(4)
for _ in range(8):                 # spinners monopolize the hubs first
    pygo_core.mn_go(spinner)
for k in range(NWORK):             # then the workers they could starve
    pygo_core.mn_go(worker(k))
def stopper():                     # workers done -> release the spinners
    for _ in range(NWORK):
        done.recv()
    stop[0] = True
pygo_core.mn_go(stopper)
pygo_core.mn_run()
pygo_core.mn_fini()
assert stop[0]
assert pygo_core._self_check(0) == 0
print("PASS")
""", timeout=25)


# ---------------------------------------------------------------------------
# TestGoroutineParallelism -- goroutines genuinely run in parallel across hubs.
# Robust proxy: record which OS-thread (hub) each goroutine ran on; with 4 hubs
# the work must land on >1 thread, and no single hub may hog nearly all of it.
# ---------------------------------------------------------------------------
def test_work_distributed_across_hubs():
    """A burst of CPU-bound goroutines under 4 hubs must execute on more than
    one hub OS-thread (genuine parallelism, not a silently-serialized M:N),
    and the busiest hub must not run ~everything (real distribution / work
    stealing, not one hub draining its deque while three idle)."""
    assert_pass(r"""
import threading
NHUBS, N = 4, 400
seen = {}
lock = threading.Lock()
done = pygo_core.Chan(N)
def mk(k):
    def w():
        s = 0
        for i in range(3000):
            s += i
        tid = threading.get_ident()
        with lock:
            seen[tid] = seen.get(tid, 0) + 1
        done.send(1)
    return w
pygo_core.mn_init(NHUBS)
for k in range(N):
    pygo_core.mn_go(mk(k))
pygo_core.mn_run()
fin = 0
for _ in range(N):
    if done.try_recv() is None: break
    fin += 1
pygo_core.mn_fini()
assert fin == N, (fin, N)
assert len(seen) >= 2, ("work did not spread across hubs", seen)
assert max(seen.values()) <= int(N * 0.9), ("one hub hogged the work", seen)
assert pygo_core._self_check(0) == 0
print("PASS", len(seen), max(seen.values()))
""", timeout=25)


# ---------------------------------------------------------------------------
# TestPreemption -- a goroutine in a tight loop with NO explicit yield point
# still yields the hub so a sibling can run.  Relies on pygo's eval-wrapper
# preemption (default-on).  Failure => infinite busy loop => timeout.
# ---------------------------------------------------------------------------
@pytest.mark.skipif(os.environ.get("PYGO_PREEMPT") == "0",
                    reason="preemption explicitly disabled (PYGO_PREEMPT=0)")
def test_preemption_busy_loop_yields_to_sibling():
    """A goroutine running `while not flag: pass` with NO sched_yield must
    still be preempted so the sibling that sets `flag` gets to run.  Tested at
    H=1 (the hardest: one hub, the busy loop owns it) and H=2.  No escape valve
    in the loop -- if preemption is broken this hangs and the subprocess times
    out, which is exactly the failure we want to catch."""
    assert_pass(r"""
def run(nhubs):
    flag = [False]
    def waiter():
        while not flag[0]:     # NO yield -- pure busy loop; needs preemption
            pass
    def setter():
        flag[0] = True
    pygo_core.mn_init(nhubs)
    pygo_core.mn_go(waiter)
    pygo_core.mn_go(setter)
    pygo_core.mn_run()
    pygo_core.mn_fini()
    assert flag[0]
    assert pygo_core._self_check(0) == 0

run(1)
run(2)
print("PASS")
""", timeout=25)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
