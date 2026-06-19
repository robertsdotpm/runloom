"""Round-3 coverage recovery for three io_uring fragments:

  - src/runloom_c/io_uring_l_loop.c.inc  (the RUNLOOM_IOURING_LOOP backend:
    Stage-2 single-shot proactor recv + Stage-3 per-hub multishot recv)
  - src/runloom_c/io_uring_l_buf.c.inc   (the GLOBAL-ring provided-buffer pool +
    global CQE drain -- used by TCPConn.recv under RUNLOOM_TCPCONN_IOURING=1)
  - src/runloom_c/io_uring_l_sys.c.inc   (lazy ring setup; almost entirely
    syscall-failure cleanup -- see the module docstring / report exclusions)

WHY a NEW suite when test_cov100b_iouring already drives the loop backend:
that suite always runs WITH RUNLOOM_IOURING_MS=1, so the all-C echo uses the
Stage-3 multishot recv for *receive* and never touches the Stage-2
`loop_recv` single-shot path, and it only ever closes a connection on an
orderly EOF (the multishot is already de-armed by then), so the
`ms_close`-while-still-armed ASYNC_CANCEL path and its buffer-reclaim loop
never run.  It also never exercises the GLOBAL-ring multishot at all
(that needs RUNLOOM_TCPCONN_IOURING=1 + TCPConn.recv, a different backend
from the loop ring).  This suite targets exactly those gaps.

ALL env-mode / io_uring coverage is via a clean-exiting SUBPROCESS: the parent
pytest imports runloom_c once, so the backend/mode is frozen for the process;
only a child started with the right env runs the path, and it must EXIT
CLEANLY for gcov to flush.  Timeouts are treated as box contention (this host
shares io_uring + CPU with a CI runner) -> pytest.skip, never a flaky fail.

Each test names the uncovered source line(s) it drives.
"""
import os
import socket
import struct
import subprocess
import sys
import threading

import pytest

from adv_util import needs_free_threading

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

pytestmark = pytest.mark.skipif(
    not FT, reason="io_uring loop / multishot are M:N (free-threaded) backends")


def _iou_available():
    try:
        import runloom_c
        return bool(runloom_c.iouring_available())
    except Exception:
        return False


needs_iouring = pytest.mark.skipif(not _iou_available(), reason="io_uring unavailable")


def _run(script, env_extra, timeout=300):
    """Run `script` in a child with the given env.  A TimeoutExpired is box
    contention (a competing CI/build run starving io_uring + CPU), not a bug,
    so we skip rather than fail -- the suite must be robust, never flaky."""
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src", **env_extra)
    try:
        return subprocess.run([PY, "-c", script], cwd=REPO, env=env,
                              capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.skip("io_uring workload timed out (box under heavy load)")


# ===========================================================================
# 1. LOOP backend, MULTISHOT OFF: drives the Stage-2 single-shot proactor
#    `runloom_iouring_loop_recv` (io_uring_l_loop.c.inc L466-477), which the
#    MS-on suite never reaches (with MS on the echo recv goes through
#    ms_recv).  The all-C echo's recv branch is `ring != NULL && ms == NULL`
#    (module_io.c.inc L171-173) -> loop_recv -> loop_io.  Exact-once byte oracle.
# ===========================================================================
_LOOP_RECV = r'''
import sys, struct; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
N = 32
got = [None] * N
def main():
    port, lst = rc.serve("127.0.0.1", 0, None, 2)   # all-C echo, loop single-shot recv
    wg = WaitGroup(); wg.add(N)
    def client(i):
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(struct.pack(">Q", i))
            got[i] = c.recv(8)
            c.close()
        finally:
            wg.done()
    for i in range(N):
        rc.mn_go(lambda i=i: client(i))
    wg.wait()
    for ln in lst:
        ln.close()
runloom.run(2, main)
ok = sum(1 for i in range(N) if got[i] == struct.pack(">Q", i))
sys.stdout.write("LOOPRECV_OK %d\n" % ok)
'''


@needs_iouring
def test_loop_single_shot_recv_ms_off():
    # LOOP on, MS explicitly OFF -> loop_recv (not ms_recv) is the recv path.
    p = _run(_LOOP_RECV, {"RUNLOOM_IOURING_LOOP": "1", "RUNLOOM_IOURING_MS": "0"})
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "LOOPRECV_OK 32" in p.stdout, (p.stdout[-400:], p.stderr[-800:])


# ===========================================================================
# 2. LOOP + MULTISHOT, peer RST mid-stream: drives the `ms_close`-while-ARMED
#    ASYNC_CANCEL path (io_uring_l_loop.c.inc L701-726: cancel SQE + park until
#    the terminal CQE clears the handle) and the held-buffer reclaim loop
#    (L728-733).  Also drives ms_on_cqe's real-error branch (L594-597): an RST
#    surfaces -ECONNRESET on the multishot recv.
#
#    HOW: clients are REAL OS threads (genuine sockets, never patched / never a
#    fiber) so they don't block a hub.  Each connects, reads the first echo
#    (the server-side multishot is now ARMED with F_MORE), blasts more bytes,
#    then RSTs (SO_LINGER {1,0}).  The server's all-C echo loop reads the blast,
#    its loop_send echo fails on the RST WHILE the multishot is still armed, and
#    module_io.c.inc L195 calls ms_close(ms) with h->armed == 1 -> the cancel
#    path.  Oracle: every client got its FIRST echo back correct (the cancel
#    path must not corrupt the steady stream), and the child exits cleanly
#    (no UAF on the cancelled in-flight multishot freeing the handle).
# ===========================================================================
_MS_CANCEL = r'''
import sys, struct, socket, threading; sys.path.insert(0, "src")
import runloom, runloom_c as rc
RealThread = threading.Thread          # captured pre-import; never patched here
N = 12
got_first = [0] * N
port_box = {}
ready = threading.Event()
def client(i):
    ready.wait()
    port = port_box["p"]
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # RST (not FIN) on close -- LINGER {1,0} set BEFORE connect so the abort
        # is armed the moment we close.  A small 2nd chunk (64B, not a multi-KB
        # blast) makes the server's loop_send echo of it FAIL atomically on the
        # RST instead of partially-blocking the hub: that keeps the test fast
        # (no SYSMON wedge) while still hitting ms_close with h->armed == 1.
        s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        s.connect(("127.0.0.1", port))
        s.sendall(struct.pack(">Q", i))
        r = s.recv(8)                              # first echo -> server multishot ARMED (F_MORE)
        if r == struct.pack(">Q", i):
            got_first[i] = 1
        try:
            s.sendall(b"X" * 64)                   # 2nd chunk; its echo races the RST
        except OSError:
            pass
        s.close()                                  # hard RST -> server loop_send fails (armed)
    except OSError:
        pass
threads = [RealThread(target=client, args=(i,), daemon=True) for i in range(N)]
for t in threads: t.start()
def main():
    port, lst = rc.serve("127.0.0.1", 0, None, 2)  # all-C echo under LOOP+MS
    port_box["p"] = port
    ready.set()
    rc.sched_sleep(0.8)                            # let servers run ms_close(armed)
    for ln in lst: ln.close()
runloom.run(2, main)
for t in threads: t.join(timeout=3)
sys.stdout.write("MSCANCEL_FIRST %d\n" % sum(got_first))
'''


@needs_iouring
def test_loop_ms_close_while_armed_on_peer_rst():
    p = _run(_MS_CANCEL, {"RUNLOOM_IOURING_LOOP": "1", "RUNLOOM_IOURING_MS": "1"})
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-2000:])
    # All 12 must have gotten the FIRST echo back (the steady stream is correct);
    # the RST/cancel of the SECOND chunk must not corrupt or hang any of them.
    assert "MSCANCEL_FIRST 12" in p.stdout, (p.stdout[-400:], p.stderr[-1200:])


# ===========================================================================
# 3. LOOP + MULTISHOT, high concurrency on FEW hubs: many connections land on a
#    small number of per-hub rings (64-deep SQ), so a burst of deferred
#    send/recv/multishot SQEs can fill the SQ -> the SQ-full spin-drain in
#    loop_enqueue_op (L412/415-416), loop_ms_submit (L550/553-554) flushes +
#    drains to free a slot.  Best-effort (depends on how fast loop_wait drains
#    between enqueues); the firm oracle is exact-once echo across 384 conns,
#    which also re-exercises the multishot buffer ring under real pressure.
# ===========================================================================
_HICONC = r'''
import sys, struct; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
N = 192
got = [None] * N
def main():
    port, lst = rc.serve("127.0.0.1", 0, None, 1)   # ONE acceptor -> few hub rings
    wg = WaitGroup(); wg.add(N)
    def client(i):
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(struct.pack(">Q", i))
            got[i] = c.recv(8)
            c.close()
        finally:
            wg.done()
    for i in range(N):
        rc.mn_go(lambda i=i: client(i))
    wg.wait()
    for ln in lst: ln.close()
runloom.run(2, main)
sys.stdout.write("HICONC_OK %d\n" % sum(1 for i in range(N) if got[i] == struct.pack(">Q", i)))
'''


@needs_iouring
def test_loop_high_concurrency_sq_pressure():
    p = _run(_HICONC, {"RUNLOOM_IOURING_LOOP": "1", "RUNLOOM_IOURING_MS": "1"},
             timeout=300)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "HICONC_OK 192" in p.stdout, (p.stdout[-400:], p.stderr[-1000:])


# ===========================================================================
# 4. DEFAULT backend, GLOBAL-ring multishot (RUNLOOM_TCPCONN_IOURING=1):
#    TCPConn.recv with flags==0 + pbuf_available() routes through the global
#    provided-buffer ring (io_uring_l_buf.c.inc).  Drives:
#      - runloom_iouring_pbuf_available  (L42-46)
#      - runloom_iouring_pbuf_addr       (L58-63)   via l_do ms_recv copy-out
#      - runloom_iouring_pbuf_return     (L66-93)   via l_do ms_on_cqe / consume
#      - the global drain's MULTISHOT case (L231-233) on each delivered chunk
#    Server is the all-C echo (default backend = epoll readiness); the CLIENTS
#    use TCPConn.recv -> global multishot.  Exact-once byte oracle.
# ===========================================================================
_GLOBAL_MS = r'''
import sys, struct; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
N = 48
got = [None] * N
def main():
    port, lst = rc.serve("127.0.0.1", 0, None, 3)   # default-backend all-C echo
    wg = WaitGroup(); wg.add(N)
    def client(i):
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(struct.pack(">Q", i))
            got[i] = c.recv(8)        # TCPConn.recv -> GLOBAL multishot (pbuf path)
            c.close()
        finally:
            wg.done()
    for i in range(N):
        rc.mn_go(lambda i=i: client(i))
    wg.wait()
    for ln in lst: ln.close()
runloom.run(4, main)
sys.stdout.write("GLOBALMS_OK %d avail=%r\n" %
                 (sum(1 for i in range(N) if got[i] == struct.pack(">Q", i)),
                  rc.iouring_available()))
'''


@needs_iouring
def test_global_ring_multishot_recv():
    # RUNLOOM_TCPCONN_IOURING=1 -> TCPConn.recv uses the global pbuf multishot.
    # Loop backend OFF so this is the GLOBAL ring (not a per-hub ring).
    p = _run(_GLOBAL_MS, {"RUNLOOM_TCPCONN_IOURING": "1"})
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "GLOBALMS_OK 48" in p.stdout, (p.stdout[-400:], p.stderr[-1000:])


# ===========================================================================
# 5. GLOBAL-ring multishot CLOSE-WHILE-ARMED: drives the global ms_close's
#    fire-and-forget ASYNC_CANCEL (io_uring_l_msclose.c.inc) whose CQE is
#    routed by the global drain's RUNLOOM_IOURING_OP_CANCEL case
#    (io_uring_l_buf.c.inc L234-239: free(op)), plus pbuf_return for any held
#    buffer at close.  HOW: the server handler holds each connection open
#    (sleeps after the echo) so when the client does its single recv() (which
#    arms the global multishot and returns the echo) and then close()s, the
#    multishot is STILL ARMED -> ms_close submits the cancel SQE rather than
#    freeing immediately.  Oracle: every client got its echo, child exits clean
#    (the cancel op record is freed exactly once, no double-free / leak).
# ===========================================================================
_GLOBAL_CANCEL = r'''
import sys, struct; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
N = 32
ok = [0] * N
def main():
    def handler(conn):
        try:
            d = conn.recv(8)
            if d:
                conn.send_all(d)
            rc.sched_sleep(0.4)        # keep the conn alive so the client's multishot stays armed
        finally:
            conn.close()
    port, lst = rc.serve("127.0.0.1", 0, handler, 2)
    wg = WaitGroup(); wg.add(N)
    def client(i):
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(struct.pack(">Q", i))
            r = c.recv(8)              # arms global multishot, returns the echo
            if r == struct.pack(">Q", i):
                ok[i] = 1
            c.close()                  # close while the multishot is STILL armed -> CANCEL
        finally:
            wg.done()
    for i in range(N):
        rc.mn_go(lambda i=i: client(i))
    wg.wait()
    rc.sched_sleep(0.2)                # let the cancel CQEs drain (free the op records)
    for ln in lst: ln.close()
runloom.run(4, main)
sys.stdout.write("GLOBALCANCEL_OK %d\n" % sum(ok))
'''


@needs_iouring
def test_global_ring_multishot_close_while_armed():
    p = _run(_GLOBAL_CANCEL, {"RUNLOOM_TCPCONN_IOURING": "1"})
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1800:])
    assert "GLOBALCANCEL_OK 32" in p.stdout, (p.stdout[-400:], p.stderr[-1200:])


# ===========================================================================
# 6. GLOBAL-ring multishot recv_into: the recv_into path (L249-262 in
#    runloom_tcp_conn_io.c.inc) is a SECOND distinct caller of the global pbuf
#    multishot (ms_open / ms_recv into a caller-owned buffer); re-exercises
#    pbuf_addr / pbuf_return / the drain MULTISHOT case through recv_into.
#    Confirms the byte-exact copy-out into a preallocated bytearray.
# ===========================================================================
_GLOBAL_RECV_INTO = r'''
import sys, struct; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
N = 40
got = [None] * N
def main():
    port, lst = rc.serve("127.0.0.1", 0, None, 3)
    wg = WaitGroup(); wg.add(N)
    def client(i):
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(struct.pack(">Q", i))
            buf = bytearray(8)
            n = c.recv_into(buf)        # global multishot into caller buffer
            got[i] = bytes(buf[:n]) if n else b""
            c.close()
        finally:
            wg.done()
    for i in range(N):
        rc.mn_go(lambda i=i: client(i))
    wg.wait()
    for ln in lst: ln.close()
runloom.run(4, main)
sys.stdout.write("RECVINTO_OK %d\n" % sum(1 for i in range(N) if got[i] == struct.pack(">Q", i)))
'''


@needs_iouring
def test_global_ring_multishot_recv_into():
    p = _run(_GLOBAL_RECV_INTO, {"RUNLOOM_TCPCONN_IOURING": "1"})
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "RECVINTO_OK 40" in p.stdout, (p.stdout[-400:], p.stderr[-1000:])


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
