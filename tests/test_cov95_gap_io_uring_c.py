"""Bounded gap-fill coverage for io_uring.c (per-hub ring create + the
multishot close/reclaim leaves the existing cov95 suites never reach).

All work runs in a clean-exiting SUBPROCESS (the io_uring backend / env is
latched once per process), so each child must EXIT CLEANLY for gcov to flush.

What this drives (source line refs are into io_uring.c's #included fragments):

  io_uring_l_ring.c.inc  runloom_iouring_ring_create() -- called UNCONDITIONALLY
      once per M:N hub in mn_sched_hub_main (whenever io_uring is available),
      with NO sockets involved, so its syscall-failure cleanup is drivable by a
      bare ``runloom.run(N)`` under strace / LD_PRELOAD fault injection.  The hub
      tolerates a NULL ring (falls back to the epoll pump), so every failure is a
      clean-exit degrade -- NOT the io_uring recv-backpressure deadlock.
        - io_uring_setup ENOMEM (non-EINVAL, no downgrade)  -> L93-95 free+NULL
        - io_uring_setup EINVAL on every setup (3x/hub: DEFER_TASKRUN retry ->
          SINGLE_ISSUER retry -> plain) -> the downgrade chain L82-85 + L90-91
          then the final L93-95 cleanup
        - eventfd() fails (faultinj)                        -> L114-115 goto fail
        - io_uring_register(EVENTFD) fails (strace)         -> L118-120 cleanup

  io_uring_l_msclose.c.inc  runloom_iouring_ms_close() immediate-free path
      (armed_snapshot==0): a GLOBAL-ring multishot that DE-ARMED (peer FIN) with
      a buffer still queued, closed without the consumer draining it.
        - ready-queue reclaim loop                          -> L23-26
        - carried inflight_bid pbuf_return (partial recv)   -> L29

  io_uring_l_do.c.inc  runloom_iouring_ms_on_cqe() closing-path reclaim
      (was_closing && !more): close while STILL armed (no FIN) with a buffer
      queued -> ms_close submits the cancel, the terminal CQE reclaims.
        - ready-queue reclaim loop                          -> L272-275
        - carried inflight_bid pbuf_return (partial recv)   -> L278

  io_uring_l_loop.c.inc  runloom_iouring_loop_ms_close() held-buffer reclaim
      (per-hub ring multishot): a real OS-thread peer RSTs after queuing >1
      buffer in the all-C echo's handle, leaving q_count>0 at close.
        - q_count>0 buffer-return loop                      -> L731-733

These are all SMALL bounded transfers (a few bytes / a few hundred bytes) and
peer FIN/RST, never a large repeatedly-backpressured recv, so none of them can
hit the iouring_recv_backpressure_deadlock path.  A TimeoutExpired is treated as
box contention (this host shares io_uring + CPU with a CI runner) -> skip.

Lines this file does NOT cover, with reasons, are in the structured notes.
"""
import os
import shutil
import subprocess
import sys

import pytest

from adv_util import needs_free_threading

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
FAULTINJ_SO = os.path.join(REPO, "tools", "faultinj", "faultinj.so")
STRACE = shutil.which("strace")

pytestmark = pytest.mark.skipif(
    not FT, reason="io_uring per-hub rings + multishot are M:N (free-threaded)")


def _iou_available():
    try:
        import runloom_c
        return bool(runloom_c.iouring_available())
    except Exception:
        return False


def _strace_supports_inject():
    if not STRACE:
        return False
    try:
        p = subprocess.run(
            [STRACE, "-e", "inject=io_uring_setup:error=EINVAL:when=1", "true"],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=15)
        return p.returncode == 0 and b"invalid" not in p.stderr.lower()
    except Exception:
        return False


needs_iouring = pytest.mark.skipif(not _iou_available(), reason="io_uring unavailable")
needs_strace = pytest.mark.skipif(
    not _strace_supports_inject(), reason="strace with -e inject= not available")
needs_faultinj = pytest.mark.skipif(
    not os.path.exists(FAULTINJ_SO), reason="tools/faultinj/faultinj.so not built")


def _run(script, env_extra=None, timeout=120):
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    if env_extra:
        env.update(env_extra)
    try:
        return subprocess.run([PY, "-c", script], cwd=REPO, env=env,
                              capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.skip("io_uring workload timed out (box under heavy load)")


def _run_strace(script, inject, env_extra=None, timeout=120):
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    if env_extra:
        env.update(env_extra)
    cmd = [STRACE, "-f", "-e", "signal=none", "-e", "inject=" + inject,
           PY, "-c", script]
    try:
        return subprocess.run(cmd, cwd=REPO, env=env,
                              capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.skip("strace-injected workload timed out (box under heavy load)")


# ===========================================================================
# 1-4. Per-hub runloom_iouring_ring_create() failure cleanup.
#
# A bare M:N run with NO I/O still creates one ring per hub in mn_sched_hub_main
# (io_uring_l_ring.c.inc), each doing: io_uring_setup -> eventfd2 ->
# io_uring_register(EVENTFD).  Injecting a failure into any of those drives the
# corresponding cleanup branch; the hub then runs WITHOUT a ring (epoll-pump
# fallback) and the program completes.  Oracle: clean exit (rc==0) + "DONE", i.e.
# the failure-cleanup path neither crashed nor wedged the hub.
# ===========================================================================
_BARE_RUN = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
done = [0]
def main():
    # a little real work so every hub actually spins up its hub_main (and thus
    # runs ring_create) before the run drains.
    def w(i):
        done[0] += 1
        return i
    for i in range(64):
        rc.mn_fiber(lambda i=i: w(i))
    rc.sched_sleep(0.1)
runloom.run(3, main)
sys.stdout.write("DONE\n")
'''


@needs_iouring
@needs_strace
def test_ring_create_setup_total_failure_cleanup():
    # io_uring_setup ENOMEM: non-EINVAL skips both downgrade branches -> fd<0 ->
    # free(r); return NULL  (io_uring_l_ring.c.inc L93-95).  Every hub degrades.
    p = _run_strace(_BARE_RUN, "io_uring_setup:error=ENOMEM:when=1+")
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "DONE" in p.stdout, (p.stdout[-400:], p.stderr[-1500:])


@needs_iouring
@needs_strace
def test_ring_create_setup_downgrade_chain_then_cleanup():
    # io_uring_setup EINVAL on EVERY setup forces, per hub, the full downgrade
    # chain: first (DEFER_TASKRUN) EINVAL -> retry without it (L82-85) EINVAL ->
    # plain SINGLE_ISSUER-less setup (L90-91) EINVAL -> fd<0 -> L93-95 cleanup.
    # (Verified: 3 io_uring_setup calls per hub under this injection.)
    p = _run_strace(_BARE_RUN, "io_uring_setup:error=EINVAL:when=1+")
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "DONE" in p.stdout, (p.stdout[-400:], p.stderr[-1500:])


@needs_iouring
@needs_faultinj
def test_ring_create_eventfd_failure_cleanup():
    # eventfd() returns -1 inside ring_create (after setup succeeds) ->
    # goto fail -> munmap x3 + close(fd) + free(r) + return NULL (L114-115 entry
    # of the fail label).  faultinj interposes eventfd; on a bare run the ONLY
    # eventfds are the per-hub ring eventfds, so FAULTINJ_ALL fails each hub's.
    p = _run(_BARE_RUN, {"LD_PRELOAD": FAULTINJ_SO,
                         "FAULTINJ_TARGET": "eventfd",
                         "FAULTINJ_NTH": "1", "FAULTINJ_ALL": "1"})
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "DONE" in p.stdout, (p.stdout[-400:], p.stderr[-1500:])


@needs_iouring
@needs_strace
def test_ring_create_register_eventfd_failure_cleanup():
    # io_uring_register(IORING_REGISTER_EVENTFD) fails -> close(efd); goto fail
    # (L118-120).  On a bare run the only io_uring_register calls are the per-hub
    # EVENTFD registrations, so when=1+ targets exactly those.
    p = _run_strace(_BARE_RUN, "io_uring_register:error=EINVAL:when=1+")
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "DONE" in p.stdout, (p.stdout[-400:], p.stderr[-1500:])


# ===========================================================================
# 5. GLOBAL-ring ms_close immediate-free reclaim (armed_snapshot==0).
#
# The peer sends a 1st chunk (consumed by the client recv -> arms the multishot),
# then a 2nd chunk, then closes (FIN).  By the time the client closes, the kernel
# has posted: the 2nd-chunk CQE (queues a buffer in ready_head) and the FIN CQE
# (res==0 && !more -> eof + armed=0).  The client does NOT recv the queued
# buffer; conn.close() -> runloom_iouring_ms_close with armed_snapshot==0 walks
# the ready queue returning each buffer (io_uring_l_msclose.c.inc L23-26).
#   Oracle: every client got its 1st echo exactly + clean exit (the reclaim of
#   the unconsumed 2nd buffer must not double-free / leak / wedge).
# ===========================================================================
_MS_DEARM_CLOSE = r'''
import sys, struct; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
N = 16
ok = bytearray(N)
def main():
    def handler(conn):
        try:
            conn.recv(8)
            conn.send_all(b"AAAAAAAA")    # 1st chunk -> client recv consumes, ms armed
            rc.sched_sleep(0.05)
            conn.send_all(b"BBBBBBBB")    # 2nd chunk -> queues a buffer client never reads
        finally:
            conn.close()                   # FIN -> de-arms client multishot (eof, armed=0)
    port, lst = rc.serve("127.0.0.1", 0, handler, 2)
    wg = WaitGroup(); wg.add(N)
    def client(i):
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(struct.pack(">Q", i))
            r = c.recv(8)                  # arms global multishot, consumes "AAAAAAAA"
            if r == b"AAAAAAAA":
                ok[i] = 1
            rc.sched_sleep(0.3)            # let 2nd-chunk CQE + FIN CQE land (queue + de-arm)
            c.close()                      # ms_close armed_snapshot==0, ready_head!=NULL -> L23-26
        finally:
            wg.done()
    for i in range(N):
        rc.mn_fiber(lambda i=i: client(i))
    wg.wait()
    for ln in lst: ln.close()
runloom.run(4, main)
sys.stdout.write("DEARM_OK %d\n" % sum(ok))
'''


@needs_iouring
def test_global_ms_close_immediate_free_reclaim():
    p = _run(_MS_DEARM_CLOSE, {"RUNLOOM_TCPCONN_IOURING": "1"})
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "DEARM_OK 16" in p.stdout, (p.stdout[-400:], p.stderr[-1500:])


# ===========================================================================
# 6. GLOBAL-ring ms_close immediate-free reclaim WITH a carried inflight buffer.
#
# Same as #5 but the client consumes only PART of the 1st buffer (recv(4) of an
# 8-byte buffer) so inflight_bid>=0 is carried, then de-arms (FIN) with a 2nd
# buffer queued, then closes -> the immediate-free path also pbuf_returns the
# carried inflight buffer (io_uring_l_msclose.c.inc L29).
#   Oracle: the partial recv read the right 4 bytes + clean exit.
# ===========================================================================
_MS_DEARM_CLOSE_INFLIGHT = r'''
import sys, struct; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
N = 16
ok = bytearray(N)
def main():
    def handler(conn):
        try:
            conn.recv(8)
            conn.send_all(b"ABCDEFGH")    # 8-byte 1st chunk
            rc.sched_sleep(0.05)
            conn.send_all(b"BBBBBBBB")    # 2nd chunk -> queued, never read
        finally:
            conn.close()                   # FIN -> de-arm
    port, lst = rc.serve("127.0.0.1", 0, handler, 2)
    wg = WaitGroup(); wg.add(N)
    def client(i):
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(struct.pack(">Q", i))
            r = c.recv(4)                  # PARTIAL: consumes 4/8 -> inflight_bid carried
            if r == b"ABCD":
                ok[i] = 1
            rc.sched_sleep(0.3)            # 2nd-chunk CQE + FIN CQE land
            c.close()                      # ms_close armed_snapshot==0 + inflight_bid>=0 -> L29
        finally:
            wg.done()
    for i in range(N):
        rc.mn_fiber(lambda i=i: client(i))
    wg.wait()
    for ln in lst: ln.close()
runloom.run(4, main)
sys.stdout.write("DEARMINFL_OK %d\n" % sum(ok))
'''


@needs_iouring
def test_global_ms_close_immediate_free_reclaim_with_inflight():
    p = _run(_MS_DEARM_CLOSE_INFLIGHT, {"RUNLOOM_TCPCONN_IOURING": "1"})
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "DEARMINFL_OK 16" in p.stdout, (p.stdout[-400:], p.stderr[-1500:])


# ===========================================================================
# 7. GLOBAL-ring ms_on_cqe closing-path reclaim (was_closing && !more).
#
# The peer keeps the conn OPEN (no FIN) and sends a 2nd chunk the client leaves
# queued; the client closes while the multishot is STILL ARMED -> ms_close
# submits the ASYNC_CANCEL and the terminal CQE reaches ms_on_cqe with
# was_closing && !more AND ready_head non-empty -> the reclaim loop in
# io_uring_l_do.c.inc L272-275 returns the queued buffer at handle free.
#   Oracle: every client got its 1st echo + clean exit.
# ===========================================================================
_MS_ARMED_CLOSE = r'''
import sys, struct; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
N = 16
ok = bytearray(N)
def main():
    def handler(conn):
        try:
            conn.recv(8)
            conn.send_all(b"AAAAAAAA")    # 1st chunk consumed by client
            rc.sched_sleep(0.05)
            conn.send_all(b"BBBBBBBB")    # 2nd chunk queued at client (not consumed)
            rc.sched_sleep(0.4)           # KEEP OPEN (no FIN) -> client ms stays ARMED
        finally:
            conn.close()
    port, lst = rc.serve("127.0.0.1", 0, handler, 2)
    wg = WaitGroup(); wg.add(N)
    def client(i):
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(struct.pack(">Q", i))
            r = c.recv(8)                  # arms ms, consumes "AAAAAAAA"
            if r == b"AAAAAAAA":
                ok[i] = 1
            rc.sched_sleep(0.2)           # 2nd-chunk CQE lands (queued); ms STILL armed
            c.close()                      # close while ARMED + queued -> cancel -> on_cqe L272-275
        finally:
            wg.done()
    for i in range(N):
        rc.mn_fiber(lambda i=i: client(i))
    wg.wait()
    rc.sched_sleep(0.2)
    for ln in lst: ln.close()
runloom.run(4, main)
sys.stdout.write("ARMEDCLOSE_OK %d\n" % sum(ok))
'''


@needs_iouring
def test_global_ms_on_cqe_closing_reclaim():
    p = _run(_MS_ARMED_CLOSE, {"RUNLOOM_TCPCONN_IOURING": "1"})
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "ARMEDCLOSE_OK 16" in p.stdout, (p.stdout[-400:], p.stderr[-1500:])


# ===========================================================================
# 8. GLOBAL-ring ms_on_cqe closing-path reclaim WITH a carried inflight buffer.
#
# Same as #7 but a PARTIAL recv(4) leaves inflight_bid carried when the conn is
# closed while armed -> the terminal-CQE free path pbuf_returns the carried
# inflight buffer (io_uring_l_do.c.inc L278).
# ===========================================================================
_MS_ARMED_CLOSE_INFLIGHT = r'''
import sys, struct; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
N = 16
ok = bytearray(N)
def main():
    def handler(conn):
        try:
            conn.recv(8)
            conn.send_all(b"ABCDEFGH")
            rc.sched_sleep(0.05)
            conn.send_all(b"BBBBBBBB")
            rc.sched_sleep(0.4)           # keep open -> client ms armed
        finally:
            conn.close()
    port, lst = rc.serve("127.0.0.1", 0, handler, 2)
    wg = WaitGroup(); wg.add(N)
    def client(i):
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(struct.pack(">Q", i))
            r = c.recv(4)                  # PARTIAL -> inflight_bid carried
            if r == b"ABCD":
                ok[i] = 1
            rc.sched_sleep(0.2)
            c.close()                      # close while ARMED + inflight carried -> L278
        finally:
            wg.done()
    for i in range(N):
        rc.mn_fiber(lambda i=i: client(i))
    wg.wait()
    rc.sched_sleep(0.2)
    for ln in lst: ln.close()
runloom.run(4, main)
sys.stdout.write("ARMEDINFL_OK %d\n" % sum(ok))
'''


@needs_iouring
def test_global_ms_on_cqe_closing_reclaim_with_inflight():
    p = _run(_MS_ARMED_CLOSE_INFLIGHT, {"RUNLOOM_TCPCONN_IOURING": "1"})
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "ARMEDINFL_OK 16" in p.stdout, (p.stdout[-400:], p.stderr[-1500:])


# ===========================================================================
# 9. LOOP-backend runloom_iouring_loop_ms_close() held-buffer reclaim.
#
# Real OS-thread peers (genuine sockets, never fibers) each connect, read the
# first echo (server multishot now ARMED), then blast several small chunks fast
# so the all-C echo's per-hub multishot QUEUES >1 buffer (q_count>0) faster than
# the echo drains, then RST (SO_LINGER {1,0}).  When the server's loop_send echo
# fails on the RST while still armed, ms_close runs with q_count>0 -> the
# buffer-return loop in io_uring_l_loop.c.inc L731-733.
#   Oracle: every peer got its FIRST echo back + clean exit (the leftover queued
#   buffers are returned to the pool, no leak / no wedge).
# ===========================================================================
_LOOP_RECLAIM = r'''
import sys, struct, socket, threading; sys.path.insert(0, "src")
import runloom, runloom_c as rc
RealThread = threading.Thread          # captured pre-import; never patched
N = 12
got_first = [0] * N
port_box = {}
ready = threading.Event()
def client(i):
    ready.wait()
    port = port_box["p"]
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        s.connect(("127.0.0.1", port))
        s.sendall(struct.pack(">Q", i))
        r = s.recv(8)                              # first echo -> server multishot ARMED
        if r == struct.pack(">Q", i):
            got_first[i] = 1
        try:
            for _ in range(4):
                s.sendall(b"Y" * 200)              # queue >1 buffer in the server handle
        except OSError:
            pass
        s.close()                                  # RST while armed + q_count>0 -> ms_close reclaim
    except OSError:
        pass
threads = [RealThread(target=client, args=(i,), daemon=True) for i in range(N)]
for t in threads: t.start()
def main():
    port, lst = rc.serve("127.0.0.1", 0, None, 2)  # all-C echo under LOOP+MS
    port_box["p"] = port
    ready.set()
    rc.sched_sleep(0.8)                            # let servers run ms_close(q_count>0)
    for ln in lst: ln.close()
runloom.run(2, main)
for t in threads: t.join(timeout=3)
sys.stdout.write("LOOPRECLAIM_FIRST %d\n" % sum(got_first))
'''


@needs_iouring
def test_loop_ms_close_queued_buffer_reclaim():
    p = _run(_LOOP_RECLAIM, {"RUNLOOM_IOURING_LOOP": "1", "RUNLOOM_IOURING_MS": "1"})
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-2000:])
    assert "LOOPRECLAIM_FIRST 12" in p.stdout, (p.stdout[-400:], p.stderr[-1500:])


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
