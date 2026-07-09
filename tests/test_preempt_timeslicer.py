"""Time-sliced preemption (preempt_init) -- liveness AND in-dealloc safety,
ISOLATED from the sysmon eval-wrapper.

runloom has TWO preemption mechanisms:
  * the sysmon eval-frame wrapper (RUNLOOM_PREEMPT, default ON) -- covered by
    test_sched_fairness.test_preemption_busy_loop_yields_to_sibling (which is
    SKIPPED when RUNLOOM_PREEMPT=0);
  * the explicit time-slicer: preempt_init(quantum_us) starts an OS timer thread
    that posts Py_AddPendingCall(runloom_preempt_yield_cb) every quantum
    (runloom_sched_preempt.c.inc).

The time-slicer had only the cov95 "posts and yields" tests, whose hogs are
TIME-BOUNDED (`while monotonic() < t0 + 0.3`) -- they finish whether or not a
preemption ever fired, so they don't assert the slicer actually preempts.  These
run with RUNLOOM_PREEMPT=0 so the ONLY preemption source is the time-slicer, and
assert it positively:

  1. LIVENESS (single-thread + M:N): a hog with NO cooperative yield spins until a
     sibling has advanced a counter N times; each of the sibling's N steps needs
     the hog preempted, so completion PROVES ~N preemptions happened while the hog
     span.  A dead slicer => the counter never advances => the hog spins forever
     => subprocess TIMEOUT (rc 124), a clean failure.
  2. SAFETY (in-dealloc gate, runloom_sched_preempt.c.inc:25): the time-slicer
     reaches its yield via a path SEPARATE from the M:N sysmon gates; it must
     defer while a tstate is mid object-destruction, else a concurrent
     stop-the-world gc.collect() reclaims a half-destroyed object -> UAF.  Under an
     aggressive slicer, heavy __del__s race a foreign STW thread; the invariant is
     no crash (rc 0) + self_check clean (this is a no-UAF safety oracle, the
     correct shape for this invariant; the defer is not separately counted).
"""
import os
import subprocess
import sys

import pytest

from adv_util import needs_free_threading

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

pytestmark = pytest.mark.skipif(
    not needs_free_threading(),
    reason="preempt_init (time-slicer) requires free-threaded Python")


def _run(code, timeout=30):
    """Run a snippet in a fresh subprocess with the sysmon eval-wrapper DISABLED
    (RUNLOOM_PREEMPT=0), so the time-slicer is the only preemption source."""
    preamble = "import sys; sys.path.insert(0, %r)\nimport runloom_c as rc\n" % (
        os.path.join(REPO, "src"))
    env = dict(os.environ, PYTHON_GIL="0", RUNLOOM_PREEMPT="0")
    try:
        p = subprocess.run([sys.executable, "-c", preamble + code],
                           cwd=REPO, env=env, timeout=timeout,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except subprocess.TimeoutExpired as e:
        return 124, (e.stdout or b"").decode() if isinstance(e.stdout, bytes) else (e.stdout or ""), \
               "TIMEOUT after {0}s (preemption never fired?)".format(timeout)
    return p.returncode, p.stdout, p.stderr


def _assert_ok(code, marker, timeout=30):
    rc, out, err = _run(code, timeout=timeout)
    assert rc == 0 and marker in out, \
        "rc={0}\n--- stdout ---\n{1}\n--- stderr ---\n{2}".format(rc, out, err[-1500:])


class TestTimeSlicerLiveness:
    def test_single_thread_slicer_preempts_unbounded_hog(self):
        """Single-thread run(): a hog with no yield spins until the sibling has
        stepped a counter N times; each step needs the hog preempted, so reaching
        N proves the time-slicer fired ~N times while the hog span."""
        _assert_ok(r"""
N = 100
progress = [0]
done = [False]
def hog():
    while progress[0] < N:   # NO cooperative yield -- only the time-slicer frees us
        pass
    done[0] = True
def sibling():
    for _ in range(N):
        progress[0] += 1
        rc.sched_yield()     # each resume of sibling required a preemption of hog
rc.preempt_init(1000)        # 1ms quantum
rc.fiber(hog)
rc.fiber(sibling)
rc.run()
rc.preempt_fini()
assert done[0] and progress[0] == N, (progress[0], done[0])
assert rc._self_check(0) == 0
sys.stdout.write("SLICER_ST_OK %d\n" % progress[0])
""", "SLICER_ST_OK 100", timeout=30)

    def test_mn_slicer_preempts_unbounded_hog(self):
        """M:N mn_run at H=2: same unbounded-hog liveness through the hub yield
        path (runloom_mn_yield_current) of the time-slicer's callback."""
        _assert_ok(r"""
N = 100
progress = [0]
def hog():
    while progress[0] < N:
        pass
def sibling():
    for _ in range(N):
        progress[0] += 1
        rc.sched_yield()
rc.preempt_init(1000)
rc.mn_init(2)
rc.mn_fiber(hog)
rc.mn_fiber(sibling)
rc.mn_run()
rc.mn_fini()
rc.preempt_fini()
assert progress[0] == N, progress[0]
assert rc._self_check(0) == 0
sys.stdout.write("SLICER_MN_OK %d\n" % progress[0])
""", "SLICER_MN_OK 100", timeout=30)


class TestTimeSlicerInDeallocSafety:
    def test_slicer_defers_yield_mid_dealloc_no_uaf(self):
        """An aggressive time-slicer fires WHILE heavy __del__s run and a foreign
        OS thread does stop-the-world gc.collect().  The slicer's yield_cb must
        defer while in_destruction (runloom_sched_preempt.c.inc:25); a yield
        mid-dealloc would freeze a half-destroyed object for the STW reclaim ->
        UAF/crash.  Invariant: clean exit, destructors ran, self_check clean."""
        _assert_ok(r"""
import gc
import _thread
import time as _t
finalized = [0]
stop = [False]
class Heavy:
    __slots__ = ("bucket", "peer")
    def __init__(self):
        self.bucket = [bytearray(8) for _ in range(200)]   # deep teardown in __del__
        self.peer = None
    def __del__(self):
        s = 0
        for ba in self.bucket:      # touch every element -> a long-ish destructor
            s += len(ba)
        if self.peer is not None:
            try: self.peer.bucket
            except Exception: pass
        self.bucket = None
        finalized[0] += 1
def gc_thread():
    while not stop[0]:
        gc.collect()                # cross-thread STW racing the destructors
        _t.sleep(0.001)
def churn():
    for _ in range(400):
        a = Heavy(); b = Heavy()
        a.peer = b; b.peer = a      # cycle -> trashcan unwind inside __del__
        del a, b
        rc.sched_yield()
    stop[0] = True
_thread.start_new_thread(gc_thread, ())
rc.preempt_init(300)                # 0.3ms: fire aggressively into the dtors
rc.fiber(churn)
rc.run()
rc.preempt_fini()
stop[0] = True
gc.collect()
assert finalized[0] > 0, "no destructors ran"
assert rc._self_check(0) == 0
sys.stdout.write("SLICER_DEALLOC_OK %d\n" % finalized[0])
""", "SLICER_DEALLOC_OK", timeout=40)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
