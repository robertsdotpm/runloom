"""Bounded gap-fill coverage for the never-executed lines of netpoll.c.

Each test names the exact uncovered region it drives and the mechanism the
classifier proposed.  Every test is BOUNDED (a few seconds, watchdog or a
subprocess timeout) and exits CLEANLY so gcov flushes -- a crash/_exit would
lose the counters, so we always assert returncode==0 + a stdout marker for the
subprocess drives, and a real observable effect (the cancelled fiber's
sentinel, the graceful io_uring fallback) for the in-process ones.

Mechanisms used (all from the project's existing harness toolbox):

  cancel_all_parked body (netpoll_wake_iouring.c.inc L256-294)
      In-process M:N run(2): a fiber wait_fd-parks READ on one end of an
      idle socketpair (never written, never closed), the main fiber waits
      until netpoll_parked>=1 then calls runloom_c.cancel_all_parked(); the
      walk claims+unlinks+wakes the parked g with the CANCELLED sentinel.
      Hub parker (p->hub != NULL) -> the mn_wake_g branch (L283).

  add_iouring_eventfd / add_iouring_ring / wake_pump_arm epoll_ctl failures
      tools/faultinj/faultinj.so LD_PRELOAD (FAULTINJ_TARGET=epoll_ctl) forces
      the chosen epoll_ctl ADD to fail with a chosen errno so the clean
      io_uring-disable / hub-ring-discard / pump-arm-degrade fallbacks run.
      None of these touch the io_uring RECV path -> no backpressure deadlock.

  reset_after_fork memsets (netpoll_init.c.inc L409, L428)
      A real fd park in the PARENT allocates the pool by_fd[] + the
      registration bitmap, then os.fork(); the register_at_fork after-in-child
      handler runs runloom_netpoll_reset_after_fork, taking both memsets.

  64-consecutive-EINTR backoff (netpoll_pump_helpers.c.inc L200)
      strace -e inject=epoll_wait:error=EINTR:when=1+ on the netpoll timeout
      workload: >64 sustained EINTRs from epoll_wait trip the throttle reset
      and fall through to the 1ms backoff; the run exits cleanly on its
      timeout.

  fd_cap_target rlim_cur fallback (netpoll_diag_fd.c.inc L233-234)
      A tiny getrlimit64 LD_PRELOAD shim (the classifier's stated alternative
      to an unprivileged prlimit) reports rlim_max==RLIM_INFINITY + a finite
      rlim_cur, so fd_cap_target takes the rlim_cur branch.  Built at runtime;
      the test self-skips if no cc is present.

NOT covered here (reported as BLOCKED, no bounded clean-exit driver):
  add_iouring_ring idempotent re-register (L468-470) -- fires only if the
  SAME hub eventfd is registered twice, which never happens in normal
  operation and has no Python-reachable entry point.
"""
import errno
import os
import shutil
import subprocess
import sys
import textwrap
import time

import pytest

import runloom
import runloom_c as rc
from adv_util import hang_guard, needs_free_threading

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
FAULTINJ_SO = os.path.join(REPO, "tools", "faultinj", "faultinj.so")
STRACE = shutil.which("strace")
CC = os.environ.get("CC") or shutil.which("cc") or shutil.which("gcc")

IS_LINUX = sys.platform.startswith("linux")
IS_EPOLL = rc.netpoll_backend() == "epoll"


def _run_py(src, env_extra=None, timeout=60, preload=None):
    """Run a python snippet in a clean subprocess with the runloom env set.

    The snippet must print its own success marker and exit 0 -- a crash/_exit
    does NOT flush gcov, so we always assert returncode==0 + the marker.
    """
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    if preload:
        env["LD_PRELOAD"] = preload
    if env_extra:
        env.update(env_extra)
    return subprocess.run([PY, "-c", textwrap.dedent(src)],
                          cwd=REPO, env=env, capture_output=True, text=True,
                          timeout=timeout)


def _strace_supports_inject():
    if not STRACE:
        return False
    try:
        p = subprocess.run(
            [STRACE, "-e", "inject=epoll_wait:error=EBADF:when=1", "true"],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=15)
        return p.returncode == 0 and b"invalid" not in p.stderr.lower()
    except Exception:
        return False


HAVE_FAULTINJ = os.path.exists(FAULTINJ_SO)
HAVE_STRACE_INJECT = _strace_supports_inject()

faultinj_only = pytest.mark.skipif(
    not (IS_LINUX and IS_EPOLL and HAVE_FAULTINJ),
    reason="needs Linux epoll + tools/faultinj/faultinj.so (run `make` in tools/faultinj)")
strace_only = pytest.mark.skipif(
    not (IS_LINUX and IS_EPOLL and HAVE_STRACE_INJECT),
    reason="needs Linux epoll + strace with -e inject=")


# ---------------------------------------------------------------------------
# netpoll_wake_iouring.c.inc L256-294: runloom_netpoll_cancel_all_parked.
#   L256        function entry
#   L263-269    lock_inited gate, RLOCK, by_fd walk, save next_by_fd
#   L272-273    if cur==WOKEN break; the claim commit-CAS
#   L276        break after a winning CAS
#   L278-284    ready_out=CANCELLED, parker_unlink, mn_wake_g(L283)/sched_wake
#   L286-287    PARKER_FORCE event + n++
#   L289        p = next bucket-walk advance
#   L292-294    per-pool RUNLOCK + return n
# A fiber parks READ on an idle socketpair (never written/closed) so ONLY the
# cancel can wake it; the main fiber polls netpoll_parked until it is parked,
# then calls cancel_all_parked().  The parked fiber resumes with the CANCELLED
# sentinel and the call returns n==1 -- observable proof the whole body ran.
# Under run(2) the parker's p->hub != NULL, so the wake takes the mn_wake_g
# branch (L283).
# ---------------------------------------------------------------------------
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_cancel_all_parked_wakes_idle_fd_parker():
    import socket
    res = {}

    def main():
        a, b = socket.socketpair()
        a.setblocking(False)
        b.setblocking(False)

        def parker():
            # idle fd: nothing writes or closes it, so only the cancel wakes us.
            res["r"] = rc.wait_fd(a.fileno(), 1, 5000)

        rc.mn_go(parker)
        # Wait until the parker is genuinely parked (cross-hub safe via a real
        # logical sleep rather than a same-hub sched_yield).
        t0 = time.monotonic()
        while rc.stats()["netpoll_parked"] < 1 and time.monotonic() - t0 < 3.0:
            runloom.sleep(0.005)
        res["parked_before"] = rc.stats()["netpoll_parked"]
        res["n"] = rc.cancel_all_parked()      # drives L256-294
        # Let the woken parker resume and record its result.
        t0 = time.monotonic()
        while "r" not in res and time.monotonic() - t0 < 3.0:
            runloom.sleep(0.005)
        rc.netpoll_unregister(a.fileno())
        a.close()
        rc.netpoll_unregister(b.fileno())
        b.close()

    with hang_guard(30, "cancel_all_parked"):
        runloom.run(2, lambda: rc.mn_go(main))

    assert res.get("parked_before") == 1, res          # the parker really parked
    assert res.get("n") == 1, res                       # exactly one cancelled
    # The C fast path returns the CANCELLED sentinel on a force-wake.
    assert res.get("r") == rc.WAIT_FD_CANCELLED, res


def test_cancel_all_parked_single_thread_sched_wake():
    # netpoll_wake_iouring.c.inc L284: the OTHER arm of the L283/L284 wake
    # branch -- a SINGLE-THREAD parked g has p->hub == NULL, so cancel takes
    # runloom_sched_wake(p->g) (L284) instead of mn_wake_g (L283).  Two
    # cooperative fibers on one thread: a parker on an idle socketpair, and a
    # canceller that sched_yields until the parker is parked then calls
    # cancel_all_parked().  The parker resumes with the CANCELLED sentinel.
    import socket
    res = {}
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)
    res["a"], res["b"] = a, b

    def parker():
        res["r"] = rc.wait_fd(res["a"].fileno(), 1, 5000)

    def canceller():
        for _ in range(50):
            if rc.stats()["netpoll_parked"] >= 1:
                break
            rc.sched_yield()
        res["parked"] = rc.stats()["netpoll_parked"]
        res["n"] = rc.cancel_all_parked()        # single-thread -> L284 sched_wake

    rc.fiber(parker)
    rc.fiber(canceller)
    with hang_guard(20, "cancel_all_parked single-thread"):
        rc.run()                                  # single-thread scheduler
    rc.netpoll_unregister(a.fileno())
    a.close()
    rc.netpoll_unregister(b.fileno())
    b.close()
    assert res.get("parked") == 1, res
    assert res.get("n") == 1, res
    assert res.get("r") == rc.WAIT_FD_CANCELLED, res


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_cancel_all_parked_empty_is_clean_noop():
    # The common clean-drain case: nothing parked -> walk finds no bucket
    # entries, returns 0.  Exercises the lock_inited gate + empty by_fd walk +
    # per-pool unlock (L263, L292-294) with n==0, and proves it is a cheap
    # idempotent no-op (the function's documented common path).
    res = {}

    def main():
        res["n0"] = rc.cancel_all_parked()
        res["n1"] = rc.cancel_all_parked()

    with hang_guard(20, "cancel_all_parked noop"):
        runloom.run(2, lambda: rc.mn_go(main))
    assert res.get("n0") == 0, res
    assert res.get("n1") == 0, res


# ---------------------------------------------------------------------------
# A global io_uring file_read is the simplest way to drive the GLOBAL ring's
# add_iouring_eventfd (it registers the ring CQE eventfd into the epoll pump).
# When io_uring is the first epoll user, the epoll_ctl ADD of that eventfd is
# epoll_ctl call #1, so faultinj can target it precisely.  On failure io_uring
# disables itself and file_read falls back to pread -- the SAME data, no crash,
# no recv-backpressure deadlock.
# ---------------------------------------------------------------------------
_IOURING_FILEREAD = r"""
    import os, sys, tempfile
    import runloom_c as rc
    PAYLOAD = b"netpoll cover payload " * 32
    path = tempfile.mktemp()
    with open(path, "wb") as f:
        f.write(PAYLOAD)
    out = {}
    def worker():
        fd = os.open(path, os.O_RDONLY)
        try:
            buf = bytearray(len(PAYLOAD))
            try:
                n = rc.file_read(fd, buf, len(PAYLOAD), 0)
                out["ok"] = (n == len(PAYLOAD) and bytes(buf) == PAYLOAD)
            except OSError as e:
                out["errno"] = e.errno
        finally:
            os.close(fd)
    rc.fiber(worker); rc.run()
    os.unlink(path)
    # io_uring must have disabled itself after the injected ADD failure.
    sys.stdout.write("AVAIL=%d OK=%r ERRNO=%r\n" % (
        1 if rc.iouring_available() else 0, out.get("ok"), out.get("errno")))
"""


@faultinj_only
@pytest.mark.skipif(not rc.iouring_available(), reason="needs io_uring")
def test_add_iouring_eventfd_outer_add_nonexist_returns_minus1():
    # netpoll_wake_iouring.c.inc L315-316: the outer epoll_ctl ADD of the
    # io_uring eventfd fails with a NON-EINVAL, NON-EEXIST errno (EPERM=1):
    # errno!=EINVAL skips the fallback, L315 errno!=EEXIST is true -> L316
    # return -1.  io_uring init aborts, file_read falls back to pread.
    p = _run_py(_IOURING_FILEREAD, preload=FAULTINJ_SO,
                env_extra={"FAULTINJ_TARGET": "epoll_ctl",
                           "FAULTINJ_NTH": "1", "FAULTINJ_ERRNO": "1"})
    assert p.returncode == 0, p.stderr[-1500:]
    assert "AVAIL=0" in p.stdout, (p.stdout, p.stderr[-800:])   # io_uring disabled
    assert "OK=True" in p.stdout, (p.stdout, p.stderr[-800:])   # pread fallback data


@faultinj_only
@pytest.mark.skipif(not rc.iouring_available(), reason="needs io_uring")
def test_add_iouring_eventfd_retry_add_einval_returns_minus1():
    # netpoll_wake_iouring.c.inc L314: the outer ADD fails EINVAL -> the
    # EINVAL-fallback retry ADD (non-EPOLLEXCLUSIVE) ALSO fails (FAULTINJ_ALL=1,
    # same EINVAL) and EINVAL!=EEXIST -> L314 return -1.  Two epoll_ctl calls
    # both injected; io_uring disables itself, file_read falls back to pread.
    p = _run_py(_IOURING_FILEREAD, preload=FAULTINJ_SO,
                env_extra={"FAULTINJ_TARGET": "epoll_ctl", "FAULTINJ_NTH": "1",
                           "FAULTINJ_ALL": "1", "FAULTINJ_ERRNO": "22"})
    assert p.returncode == 0, p.stderr[-1500:]
    assert "AVAIL=0" in p.stdout, (p.stdout, p.stderr[-800:])
    assert "OK=True" in p.stdout, (p.stdout, p.stderr[-800:])


# ---------------------------------------------------------------------------
# Per-hub io_uring rings are created UNCONDITIONALLY on every M:N hub thread
# (mn_sched_hub_main: ring_create -> add_iouring_ring), so a plain run(2)
# already drives add_iouring_ring.  Each hub's ring-eventfd ADD is an
# EPOLLEXCLUSIVE epoll_ctl; faultinj forces EINVAL on it to drive the
# EPOLLEXCLUSIVE fallback / table-undo branches.  A hub that loses its ring
# discards it and falls back to the epoll pump (mn_sched_hub_main L227-233) --
# no recv, no deadlock, bounded sched_yield workload.
# ---------------------------------------------------------------------------
_MN_RING = r"""
    import sys
    import runloom, runloom_c as rc
    def worker():
        for _ in range(40):
            rc.sched_yield()
    def drv():
        for _ in range(6):
            rc.mn_go(worker)
    runloom.run(2, drv)
    sys.stdout.write("MN_RING_OK\n")
"""


@faultinj_only
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
@pytest.mark.skipif(not rc.iouring_available(), reason="needs io_uring")
def test_add_iouring_ring_epollexclusive_einval_retry():
    # netpoll_wake_iouring.c.inc L491-493: the EPOLLEXCLUSIVE ADD of a hub
    # ring's eventfd fails EINVAL (epoll_ctl call #1) -> the non-exclusive
    # retry ADD (call #2) succeeds and registration proceeds.  Only the FIRST
    # epoll_ctl is injected (no FAULTINJ_ALL) so the retry succeeds.
    p = _run_py(_MN_RING, preload=FAULTINJ_SO,
                env_extra={"FAULTINJ_TARGET": "epoll_ctl",
                           "FAULTINJ_NTH": "1", "FAULTINJ_ERRNO": "22"})
    assert p.returncode == 0, p.stderr[-1500:]
    assert "MN_RING_OK" in p.stdout, (p.stdout, p.stderr[-1200:])


@faultinj_only
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
@pytest.mark.skipif(not rc.iouring_available(), reason="needs io_uring")
def test_add_iouring_ring_undo_table_insert_returns_minus1():
    # netpoll_wake_iouring.c.inc L495-509: BOTH the EPOLLEXCLUSIVE ADD and the
    # EINVAL-fallback retry ADD fail EINVAL (FAULTINJ_ALL=1) -> EINVAL!=EEXIST
    # -> the undo block re-locks, swap-removes the just-inserted ring entry,
    # decrements the count, unlocks, returns -1 (L497-509).  Each hub then
    # discards its ring and falls back to the epoll pump.  The bounded
    # sched_yield workload (no fd parking) exits cleanly.
    p = _run_py(_MN_RING, preload=FAULTINJ_SO,
                env_extra={"FAULTINJ_TARGET": "epoll_ctl", "FAULTINJ_NTH": "1",
                           "FAULTINJ_ALL": "1", "FAULTINJ_ERRNO": "22"})
    assert p.returncode == 0, p.stderr[-1500:]
    assert "MN_RING_OK" in p.stdout, (p.stdout, p.stderr[-1200:])


# ---------------------------------------------------------------------------
# netpoll_wake_iouring.c.inc L365-367: runloom_netpoll_wake_pump_arm's
# epoll_ctl ADD of the (level-triggered, non-exclusive) pump-wake eventfd fails
# with a non-EEXIST errno -> close(fd), unlock, return -1.  Driven by a
# single-thread runloom.blocking() offload (runloom_blockpool.c arms the
# pump-wake eventfd); the ADD is epoll_ctl call #1.  On failure the blockpool
# degrades to no-offload but the blocking call still completes cleanly.
# ---------------------------------------------------------------------------
_BLOCKING_ARM = r"""
    import sys, time
    import runloom, runloom_c as rc
    def slow():
        time.sleep(0.05)
        return 42
    def main():
        r = runloom.blocking(slow)        # arms the pump-wake eventfd
        assert r == 42, r
    rc.fiber(main); rc.run()
    sys.stdout.write("BLOCK_OK\n")
"""


@faultinj_only
def test_wake_pump_arm_epoll_ctl_fail_degrades_cleanly():
    p = _run_py(_BLOCKING_ARM, preload=FAULTINJ_SO,
                env_extra={"FAULTINJ_TARGET": "epoll_ctl",
                           "FAULTINJ_NTH": "1", "FAULTINJ_ERRNO": "1"})
    assert p.returncode == 0, p.stderr[-1500:]
    assert "BLOCK_OK" in p.stdout, (p.stdout, p.stderr[-1200:])   # call still completed


# ---------------------------------------------------------------------------
# netpoll_init.c.inc L409 + L428: runloom_netpoll_reset_after_fork's two
# memsets -- p->by_fd (L409, per pool whose by_fd[] was allocated) and
# runloom_fd_registered_bm (L428, if the registration bitmap was allocated).
# Both guards are non-NULL only after real park activity.  A genuine fd park in
# the PARENT allocates both; os.fork()'s register_at_fork(after_in_child)
# handler runs reset_after_fork in the child, taking both memsets.  The child
# re-parks to prove the reset left a working runtime; child exit 0 == both ran.
# ---------------------------------------------------------------------------
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_reset_after_fork_memsets():
    p = _run_py(r"""
        import glob, os, socket, sys
        import runloom, runloom_c as rc

        def gcov_dump():
            # The grandchild exits via os._exit (fork-in-M:N safety), which does
            # NOT flush gcov.  Under an instrumented build the memsets ran in the
            # child but their counters would be lost; flush them explicitly.
            # No-op on a normal build (the symbol simply isn't there).
            try:
                import ctypes
                so = glob.glob(os.path.join("src", "runloom_c*.so"))
                if so:
                    ctypes.CDLL(so[0]).__gcov_dump()
            except Exception:
                pass

        def park_once(tag):
            a, b = socket.socketpair()
            a.setblocking(False); b.setblocking(False)
            def w():
                rc.sched_yield()
                rc.tcp_send(b.fileno(), tag)
            rc.mn_go(w)
            r = rc.wait_fd(a.fileno(), 1, 5000)   # epoll park+wake: alloc by_fd + reg bitmap
            assert r == 1, (tag, r)
            buf = bytearray(1); rc.tcp_recv(a.fileno(), buf, 1)
            rc.netpoll_unregister(a.fileno()); a.close()
            rc.netpoll_unregister(b.fileno()); b.close()

        runloom.run(2, lambda: rc.mn_go(lambda: park_once(b"P")))  # parent: by_fd[]+bitmap now set

        pid = os.fork()
        if pid == 0:
            try:
                runloom.run(2, lambda: rc.mn_go(lambda: park_once(b"C")))  # reset ran at fork
                gcov_dump()
                os._exit(0)
            except BaseException as e:
                sys.stderr.write("child: %r\n" % e); os._exit(7)
        else:
            _, st = os.waitpid(pid, 0)
            sys.stdout.write("CHILD:%d\n" % os.waitstatus_to_exitcode(st))
    """, timeout=45)
    assert p.returncode == 0, p.stderr[-1500:]
    assert "CHILD:0" in p.stdout, (p.stdout, p.stderr[-1200:])


# ---------------------------------------------------------------------------
# netpoll_pump_helpers.c.inc L200: __atomic_store(&eintr_run, 0) after 64
# CONSECUTIVE EINTRs from epoll_wait, then a fall-through to the 1ms backoff
# (clean throttle, no abort).  strace forces a sustained EINTR storm on
# epoll_wait; the timeout workload parks on a never-ready fd, so the pump spins
# on EINTR > 64 times, trips the reset+backoff, and exits cleanly on timeout.
# ---------------------------------------------------------------------------
@strace_only
def test_epoll_wait_64_consecutive_eintr_backoff():
    workload = os.path.join(REPO, "tests", "netpoll_fault_workload.py")
    env = dict(os.environ, PYTHON_GIL="0")
    cmd = [STRACE, "-f", "-e", "signal=none",
           "-e", "inject=epoll_wait,epoll_pwait,epoll_pwait2:error=EINTR:when=1+",
           PY, workload, "timeout"]
    p = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True,
                       timeout=40)
    assert p.returncode == 0, (p.stdout, p.stderr[-1500:])
    # The timeout workload reaches its clean deadline through the backoff.
    assert "DONE" in p.stdout, (p.stdout, p.stderr[-1200:])


# ---------------------------------------------------------------------------
# netpoll_diag_fd.c.inc L233-234: runloom_fd_cap_target's rlim_cur fallback,
# reached only when getrlimit reports rlim_max==RLIM_INFINITY (and rlim_cur>0)
# -- it then sizes the fd arrays from rlim_cur.  Raising the HARD NOFILE limit
# to unlimited needs privilege, so (per the classifier's stated alternative) a
# tiny getrlimit64 LD_PRELOAD shim reports rlim_max==INF + a finite rlim_cur.
# The extension imports getrlimit64 (the LFS alias), so that is what we hook.
# Any socket program then drives fd_cap_target through the fallback; we assert
# a real park still works (the fd arrays were sized correctly).
# ---------------------------------------------------------------------------
_RLIMSHIM_C = r"""
#define _GNU_SOURCE
#include <dlfcn.h>
#include <sys/resource.h>
static int (*real_getrlimit64)(__rlimit_resource_t, struct rlimit64 *);
int getrlimit64(__rlimit_resource_t resource, struct rlimit64 *rl) {
    if (!real_getrlimit64) real_getrlimit64 = dlsym(RTLD_NEXT, "getrlimit64");
    int r = real_getrlimit64 ? real_getrlimit64(resource, rl) : 0;
    if (resource == RLIMIT_NOFILE && rl) {
        rl->rlim_cur = 50000;          /* finite, > RUNLOOM_FD_CAP_MIN */
        rl->rlim_max = RLIM64_INFINITY;/* forces the rlim_cur fallback */
    }
    return r;
}
"""


@pytest.mark.skipif(not (IS_LINUX and IS_EPOLL), reason="Linux epoll only")
@pytest.mark.skipif(not CC, reason="needs a C compiler to build the getrlimit shim")
def test_fd_cap_target_rlim_cur_fallback(tmp_path):
    src = tmp_path / "rlimshim.c"
    so = tmp_path / "rlimshim.so"
    src.write_text(_RLIMSHIM_C)
    build = subprocess.run([CC, "-shared", "-fPIC", "-O2", "-o", str(so),
                            str(src), "-ldl"],
                           capture_output=True, text=True, timeout=60)
    if build.returncode != 0:
        pytest.skip("getrlimit shim did not build: %s" % build.stderr[-400:])
    p = _run_py(r"""
        import socket, sys
        import runloom, runloom_c as rc
        # A real park drives netpoll init -> runloom_fd_cap_target (rlim_cur branch).
        def park_once():
            a, b = socket.socketpair()
            a.setblocking(False); b.setblocking(False)
            def w():
                rc.sched_yield(); rc.tcp_send(b.fileno(), b"x")
            rc.mn_go(w)
            r = rc.wait_fd(a.fileno(), 1, 5000)
            assert r == 1, r
            buf = bytearray(1); rc.tcp_recv(a.fileno(), buf, 1)
            rc.netpoll_unregister(a.fileno()); a.close()
            rc.netpoll_unregister(b.fileno()); b.close()
        runloom.run(2, lambda: rc.mn_go(park_once))
        sys.stdout.write("RLIM_OK\n")
    """, preload=str(so), timeout=45)
    assert p.returncode == 0, p.stderr[-1500:]
    assert "RLIM_OK" in p.stdout, (p.stdout, p.stderr[-1200:])


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
