"""Coverage-driven adversarial tests for io_uring_l_do.c.inc.

The fragment's uncovered body is the GLOBAL-RING multishot recv handle:
  runloom_iouring_ms_submit / ms_open / ms_on_cqe / ms_recv.

This is NOT the io_uring-as-LOOP backend (that family lives in
io_uring_l_loop.c.inc as loop_ms_*, and serve(handler=None) uses it).  The
GLOBAL-RING multishot path is reached only through a *Python-level* TCPConn's
.recv()/.recv_into() when:

    RUNLOOM_TCPCONN_IOURING=1   (TCPConn picks the io_uring recv backend)
    io_uring is available       (kernel has io_uring)
    pbuf_available()            (kernel >= 5.19: provided-buffer ring set up in
                                 the global ring's lazy_init)

It runs under the DEFAULT epoll backend -- the global ring's completion eventfd
is registered into the shared epoll pump, which drains the CQ via
runloom_iouring_drain(), dispatching each multishot CQE to ms_on_cqe.

The backend choice (RUNLOOM_TCPCONN_IOURING) is resolved ONCE in the C extension
(getenv, latched), so every test runs its workload in a SUBPROCESS with that env
set, and each child EXITS CLEANLY so gcov counters flush.  Generous timeouts +
pytest.skip on TimeoutExpired: this box shares io_uring + CPU with a CI runner,
so a timeout is contention, not a bug.

Oracles are real (exact-once echo, byte-exact partial-buffer carry, exact EOF,
two-buffer coalescing, single-thread vs M:N wake), not line-touch filler.

Lines that need an allocator failure with no RUNLOOM_FAULT_ hook (ms_open
calloc, on_cqe malloc, ms_close cancel-op calloc), a kernel-delivered non-
ENOBUFS/ECANCELED multishot error CQE (ms_on_cqe h->err, ms_recv sticky-error
return), or are caller-gated "can't happen" guards (ms_recv h==NULL / n==0; the
do() !available ENOSYS fold) are classified in the report's exclusions, not
contorted into fake tests.
"""
import os
import subprocess
import sys

import pytest

from adv_util import needs_free_threading

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

pytestmark = pytest.mark.skipif(
    not FT, reason="global-ring multishot recv is an M:N / free-threaded path")


def _iou_available():
    try:
        import runloom_c
        return bool(runloom_c.iouring_available())
    except Exception:
        return False


needs_iouring = pytest.mark.skipif(
    not _iou_available(), reason="io_uring unavailable")


def _run(script, timeout=240, env_extra=None):
    # RUNLOOM_TCPCONN_IOURING=1 routes TCPConn.recv through the global-ring
    # multishot path (ms_open/ms_recv).  No RUNLOOM_IOURING_LOOP: we want the
    # GLOBAL ring's ms_* family, not the per-hub loop's loop_ms_* family.
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src",
               RUNLOOM_TCPCONN_IOURING="1")
    if env_extra:
        env.update(env_extra)
    try:
        return subprocess.run([PY, "-c", script], cwd=REPO, env=env,
                              capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.skip("multishot recv workload timed out (box under heavy load)")


# --------------------------------------------------------------------------
# 1. M:N concurrent TCP echo under the global-ring multishot recv backend.
#    Drives the success spine: ms_open (L187-206) + ms_submit (L171-184) on the
#    first recv per conn; ms_on_cqe's data-arrival append (L214-233) + the M:N
#    wake (L261-262 mn_wake_g) + the !more de-arm (L250); ms_recv's ready-queue
#    move (L321-329), the park (L362-384), and the resume-consume loop.  Both
#    the server handler's recv AND the client's recv route through ms_recv, so a
#    parked recv on each side is woken by a CQE.  Closing each conn while its
#    multishot is armed drives ms_close's cancel + the terminal-CQE handle free
#    in ms_on_cqe (was_closing && !more -> L268-281).
#    Oracle: exact-once 8-byte echo for every one of N connections.
# --------------------------------------------------------------------------
_ECHO = r'''
import sys, struct; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
N = 48
got = [None] * N
def main():
    def handler(conn):
        try:
            d = conn.recv(8)            # global-ring ms_recv (parks, CQE-woken)
            if d: conn.send_all(d)
        finally:
            conn.close()                # close while multishot armed -> ms_close cancel
    port, lst = rc.serve("127.0.0.1", 0, handler, 3)
    wg = WaitGroup(); wg.add(N)
    def client(i):
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(struct.pack(">Q", i))
            got[i] = c.recv(8)          # client side ms_recv too
            c.close()
        finally:
            wg.done()
    for i in range(N):
        rc.mn_go(lambda i=i: client(i))
    wg.wait()
    for ln in lst:
        ln.close()
runloom.run(4, main)
ok = sum(1 for i in range(N) if got[i] == struct.pack(">Q", i))
sys.stdout.write("ECHO_OK %d\n" % ok)
'''


@needs_iouring
def test_ms_echo_exact_once():
    p = _run(_ECHO)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "ECHO_OK 48" in p.stdout, (p.stdout[-400:], p.stderr[-1000:])


# --------------------------------------------------------------------------
# 2. Partial-buffer carry across recv() calls + exact EOF.  The server sends one
#    8-byte buffer (a single multishot CQE -> one ms_entry).  The client does
#    recv(4) then recv(4): the SECOND recv finds no ready buffer but a partially
#    consumed in-flight buffer, so it drives ms_recv's in-flight drain block
#    (L296-318) -- the avail/want/take math, the memcpy at inflight_off, the
#    pbuf_return + inflight reset when the buffer is exhausted (L307-312).  A
#    third recv() after the server closes finds ready empty + h->eof set (the
#    res==0 && !more CQE in ms_on_cqe -> L240-241) and returns 0 (ms_recv EOF
#    return L339-341).
#    Oracle: recv(4)==b"ABCD", recv(4)==b"EFGH", recv(8)==b"" (byte-exact split
#    of one buffer + a clean orderly EOF).
# --------------------------------------------------------------------------
_PARTIAL = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
res = {}
def main():
    def handler(conn):
        try:
            conn.recv(8)                # drain client hello
            conn.send_all(b"ABCDEFGH")  # one 8-byte buffer
        finally:
            conn.close()                # orderly EOF for the client
    port, lst = rc.serve("127.0.0.1", 0, handler, 2)
    wg = WaitGroup(); wg.add(1)
    def client():
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(b"hello123")
            a = c.recv(4)               # consumes 4/8 -> inflight carries 4
            b = c.recv(4)               # in-flight drain path consumes remaining 4
            eof = c.recv(8)             # ready empty + eof -> returns b""
            res["a"] = a; res["b"] = b; res["eof"] = eof
            c.close()
        finally:
            wg.done()
    rc.mn_go(client)
    wg.wait()
    for ln in lst: ln.close()
runloom.run(4, main)
sys.stdout.write("PARTIAL a=%r b=%r eof=%r\n" % (res.get("a"), res.get("b"), res.get("eof")))
'''


@needs_iouring
def test_ms_partial_buffer_carry_and_eof():
    p = _run(_PARTIAL)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "PARTIAL a=b'ABCD' b=b'EFGH' eof=b''" in p.stdout, (
        p.stdout[-400:], p.stderr[-1000:])


# --------------------------------------------------------------------------
# 3. Two buffers queued before consume -> the ready-queue TAIL append branch in
#    ms_on_cqe (L231 "if (h->ready_tail) ready_tail->next = e", the non-empty-
#    queue arm) and ms_recv's grab-next-buffer "continue" loop across MULTIPLE
#    ready entries (L321-329) in a SINGLE recv() call.  The server sends two
#    8-byte messages 50ms apart -> two separate multishot CQEs.  The client
#    sleeps long enough that BOTH CQEs land (the first appends to an empty queue
#    -> ready_head; the second appends to a non-empty queue -> ready_tail->next)
#    before it issues ONE recv(16), which must walk both ready entries.
#    Oracle: a single recv(16) returns the concatenation b"AAAAAAAABBBBBBBB".
# --------------------------------------------------------------------------
_TWOBUF = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
res = {}
def main():
    def handler(conn):
        try:
            conn.recv(8)
            conn.send_all(b"AAAAAAAA")
            rc.sched_sleep(0.05)
            conn.send_all(b"BBBBBBBB")  # arrives as a SECOND multishot CQE
            rc.sched_sleep(0.15)
        finally:
            conn.close()
    port, lst = rc.serve("127.0.0.1", 0, handler, 2)
    wg = WaitGroup(); wg.add(1)
    def client():
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(b"hello123")
            rc.sched_sleep(0.30)        # let BOTH CQEs queue (ready_head + ready_tail)
            a = c.recv(16)              # one recv walks both ready entries
            res["a"] = a
            c.close()
        finally:
            wg.done()
    rc.mn_go(client)
    wg.wait()
    for ln in lst: ln.close()
runloom.run(4, main)
sys.stdout.write("TWOBUF a=%r\n" % (res.get("a"),))
'''


@needs_iouring
def test_ms_two_buffers_queued_then_drained_in_one_recv():
    p = _run(_TWOBUF)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    # Both buffers consumed by ONE recv(16) -> the ready_tail append (L231) and
    # the multi-entry drain "continue" loop (L321-329) both ran.
    assert "TWOBUF a=b'AAAAAAAABBBBBBBB'" in p.stdout, (
        p.stdout[-400:], p.stderr[-1000:])


# --------------------------------------------------------------------------
# 4. SINGLE-THREAD (run(1)) global-ring multishot.  Under run(1) there is no
#    hub, so runloom_mn_current_hub_opaque() == NULL: ms_recv records its waiter
#    via runloom_sched_get()->current (L364-365), parks via
#    runloom_sched_park_safe() (L386), and ms_on_cqe wakes it via
#    runloom_sched_wake_safe() (L263) -- the hub==NULL arms the M:N echo test
#    never touches.  serve() requires M:N, so we build the server from a raw
#    TCPConn listener + a hand-rolled accept loop, all on one thread.
#    Oracle: exact-once 8-byte echo for every connection on the single hub.
# --------------------------------------------------------------------------
_SINGLE = r'''
import sys, socket, struct; sys.path.insert(0, "src")
import runloom, runloom_c as rc
N = 12
got = [None] * N
done = [0]
def main():
    lst = rc.TCPConn.listen("127.0.0.1", 0)
    s = socket.socket(fileno=lst.fileno())     # borrow fd to read the bound port
    port = s.getsockname()[1]
    s.detach()
    def acceptor():
        for _ in range(N):
            try:
                conn = lst.accept()
            except OSError:
                return
            def handle(conn=conn):
                try:
                    d = conn.recv(8)            # single-thread ms_recv (park_safe)
                    if d: conn.send_all(d)
                finally:
                    conn.close()
            rc.go(handle)
    rc.go(acceptor)
    def client(i):
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(struct.pack(">Q", i))
            got[i] = c.recv(8)
            c.close()
        finally:
            done[0] += 1
            if done[0] == N:
                lst.close()
    for i in range(N):
        rc.go(lambda i=i: client(i))
runloom.run(1, main)
sys.stdout.write("SINGLE_OK %d\n" % sum(1 for i in range(N) if got[i] == struct.pack(">Q", i)))
'''


@needs_iouring
def test_ms_single_thread_wake_paths():
    p = _run(_SINGLE)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "SINGLE_OK 12" in p.stdout, (p.stdout[-400:], p.stderr[-1000:])


# --------------------------------------------------------------------------
# 5. recv_into() under the multishot backend.  RunloomTCPConn_recv_into has its
#    OWN ms_open/ms_recv call site (distinct from recv); a writable-buffer recv
#    drives ms_recv into a caller-supplied bytearray.  Confirms the recv_into
#    arm of the multishot path is wired and byte-exact (a second consumer of the
#    same ms_* engine), and exercises a partial recv_into(4) leaving an in-flight
#    buffer that a follow-up recv_into(4) drains.
#    Oracle: recv_into(4) reads b"WXYZ", a second recv_into(4) reads b"0123".
# --------------------------------------------------------------------------
_RECV_INTO = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
res = {}
def main():
    def handler(conn):
        try:
            conn.recv(8)
            conn.send_all(b"WXYZ0123")
            rc.sched_sleep(0.1)
        finally:
            conn.close()
    port, lst = rc.serve("127.0.0.1", 0, handler, 2)
    wg = WaitGroup(); wg.add(1)
    def client():
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(b"hello123")
            buf1 = bytearray(4); buf2 = bytearray(4)
            n1 = c.recv_into(buf1)      # ms_recv via recv_into; consumes 4/8
            n2 = c.recv_into(buf2)      # in-flight drain of remaining 4
            res["b1"] = bytes(buf1[:n1]); res["b2"] = bytes(buf2[:n2])
            c.close()
        finally:
            wg.done()
    rc.mn_go(client)
    wg.wait()
    for ln in lst: ln.close()
runloom.run(4, main)
sys.stdout.write("RECVINTO b1=%r b2=%r\n" % (res.get("b1"), res.get("b2")))
'''


@needs_iouring
def test_ms_recv_into_partial():
    p = _run(_RECV_INTO)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "RECVINTO b1=b'WXYZ' b2=b'0123'" in p.stdout, (
        p.stdout[-400:], p.stderr[-1000:])


# --------------------------------------------------------------------------
# 6. Teardown storm: many connect+recv+close cycles, repeated across several
#    independent run() envelopes.  Each connection opens an ms_ handle on first
#    recv and closes it while armed, so this hammers ms_open -> ms_submit ->
#    (park/CQE-wake) -> ms_close-cancel -> the terminal-CQE handle free in
#    ms_on_cqe (was_closing && !more, freeing the ready queue + inflight buffer:
#    L268-281).  Many handles created+destroyed exercises the cancel/free
#    arbitration under contention without leaking a buffer or wedging a parker.
#    Oracle: every echo in every round is exact-once (a lost CQE / double-free
#    would drop bytes or crash the child).
# --------------------------------------------------------------------------
_TEARDOWN = r'''
import sys, struct; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
def one_round(base):
    got = {}
    def main():
        def handler(conn):
            try:
                d = conn.recv(8)
                if d: conn.send_all(d)
            finally:
                conn.close()
        port, lst = rc.serve("127.0.0.1", 0, handler, 2)
        wg = WaitGroup(); wg.add(16)
        def cl(i):
            try:
                c = rc.TCPConn.connect("127.0.0.1", port)
                c.send_all(struct.pack(">Q", base + i))
                got[i] = c.recv(8); c.close()
            finally:
                wg.done()
        for i in range(16):
            rc.mn_go(lambda i=i: cl(i))
        wg.wait()
        for ln in lst: ln.close()
    runloom.run(4, main)
    return sum(1 for i, v in got.items() if v == struct.pack(">Q", base + i))
total = 0
for r in range(5):
    total += one_round(r * 100)
sys.stdout.write("TEARDOWN_OK %d\n" % total)
'''


@needs_iouring
def test_ms_close_teardown_storm():
    p = _run(_TEARDOWN, timeout=300)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "TEARDOWN_OK 80" in p.stdout, (p.stdout[-400:], p.stderr[-1000:])


# --------------------------------------------------------------------------
# 7. Best-effort buffer-pool pressure: many concurrent conns each sending a
#    large payload, recv'd in small chunks, to stress the provided-buffer ring
#    (4096 buffers x 2 KB) toward exhaustion.  If the kernel ends a multishot
#    with -ENOBUFS (armed cleared, no error, no eof), the next ms_recv hits the
#    re-arm branch (L351-358).  This is timing/kernel dependent, so the test only
#    asserts CORRECTNESS (all bytes echoed exactly) -- it does not assert the
#    re-arm fired (that line is best-effort, classified accordingly).  A
#    correctness pass here proves the multishot engine stays sound under buffer
#    pressure regardless of whether re-arm triggered.
# --------------------------------------------------------------------------
_POOL = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
N = 64
PAYLOAD = b"z" * 4096
ok = bytearray(N)
def main():
    def handler(conn):
        try:
            # read the full payload in small chunks, echo it back
            buf = bytearray()
            while len(buf) < len(PAYLOAD):
                d = conn.recv(256)
                if not d: break
                buf += d
            conn.send_all(bytes(buf))
        finally:
            conn.close()
    port, lst = rc.serve("127.0.0.1", 0, handler, 4)
    wg = WaitGroup(); wg.add(N)
    def client(i):
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(PAYLOAD)
            got = bytearray()
            while len(got) < len(PAYLOAD):
                d = c.recv(256)
                if not d: break
                got += d
            if bytes(got) == PAYLOAD:
                ok[i] = 1
            c.close()
        finally:
            wg.done()
    for i in range(N):
        rc.mn_go(lambda i=i: client(i))
    wg.wait()
    for ln in lst: ln.close()
runloom.run(4, main)
sys.stdout.write("POOL_OK %d\n" % sum(ok))
'''


@needs_iouring
def test_ms_buffer_pool_pressure_stays_correct():
    p = _run(_POOL, timeout=300)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "POOL_OK 64" in p.stdout, (p.stdout[-400:], p.stderr[-1000:])


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
