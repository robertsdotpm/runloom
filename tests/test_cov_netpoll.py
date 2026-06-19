"""Coverage-driven unit tests for the Linux default backend -- epoll netpoll
(src/runloom_c/netpoll.c + netpoll_*.c.inc).

Targets the lines the normal corpus misses: the EPOLLHUP/EPOLLERR error-folding
in the pump, register MOD-widen (a second direction on a live fd), the
arm-cache release/unregister/cancel paths, the deadline heap (many timed
parks), per-fd array growth (many fds), the io_uring global-ring eventfd drain
in the pump, and the diag/dump surface (_dump_parkers / dump_fibers / _diag_dump
/ verbose _self_check, plus RUNLOOM_NETPOLL_MAXFD).
"""
import os
import socket
import sys

import pytest

import runloom
import runloom_c as rc
from adv_util import hang_guard, needs_free_threading

READ, WRITE = 1, 2
FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEVNULL = os.open(os.devnull, os.O_WRONLY)

pytestmark = pytest.mark.skipif(rc.netpoll_backend() != "epoll",
                                reason="epoll-backend coverage")


def _drop(fd):
    try:
        rc.netpoll_unregister(fd); os.close(fd)
    except OSError:
        pass


# --------------------------------------------------------------------------
# pump: EPOLLHUP / EPOLLERR folding (an event with no IN/OUT bit)
# --------------------------------------------------------------------------
def test_epoll_hup_wakes_reader():
    # Closing a pipe's write end gives the read end EPOLLHUP (no EPOLLIN); the
    # pump must fold HUP into READ and wake the parked reader.
    res = {}
    hold = {}
    def reader():
        r = hold["r"]
        res["rv"] = rc.wait_fd(r, READ, 3000)
    def closer():
        rc.sched_yield(); rc.sched_yield()
        os.close(hold["w"])                  # -> EPOLLHUP on r
    r, w = os.pipe()
    hold["r"], hold["w"] = r, w
    with hang_guard(15, "epoll hup"):
        rc.fiber(reader); rc.fiber(closer); rc.run()
    _drop(r)
    assert res.get("rv", 0) & READ, "HUP did not wake the reader (rv=%r)" % res.get("rv")


def test_epoll_err_wakes_on_socket_reset():
    res = {}
    hold = {}
    def reader():
        res["rv"] = rc.wait_fd(hold["a"].fileno(), READ, 3000)
    def resetter():
        rc.sched_yield(); rc.sched_yield()
        b = hold["b"]
        b.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b"\1\0\0\0\0\0\0\0")  # RST on close
        b.close()
    a, b = socket.socketpair()
    hold["a"], hold["b"] = a, b
    with hang_guard(15, "epoll err"):
        rc.fiber(reader); rc.fiber(resetter); rc.run()
    _drop(a.fileno())
    assert res.get("rv", 0) != 0, "RST/ERR did not wake the reader"


# --------------------------------------------------------------------------
# register: MOD-widen a live fd to a second direction
# --------------------------------------------------------------------------
def test_epoll_mod_widen_second_direction():
    res = {}
    hold = {}
    def write_waiter():
        res["w"] = bool(rc.wait_fd(hold["fd"], WRITE, 2000) & WRITE)   # arms OUT (ADD)
    def read_waiter():
        res["r"] = bool(rc.wait_fd(hold["fd"], READ, 3000) & READ)     # widens to IN|OUT (MOD)
    def sender():
        rc.sched_yield(); rc.sched_yield()
        hold["b"].send(b"z")
    a, b = socket.socketpair()
    hold["fd"], hold["b"] = a.fileno(), b
    with hang_guard(15, "epoll mod widen"):
        rc.fiber(write_waiter); rc.fiber(read_waiter); rc.fiber(sender); rc.run()
    _drop(a.fileno()); b.close()
    assert res.get("w") is True and res.get("r") is True


# --------------------------------------------------------------------------
# deadline heap: many timed parks expiring (pump_drain_expired)
# --------------------------------------------------------------------------
def test_epoll_many_timed_parks_expire():
    N = 40
    fired = bytearray(N)
    pipes = [os.pipe() for _ in range(N)]
    def waiter(i):
        rv = rc.wait_fd(pipes[i][0], READ, 30 + i)   # staggered short deadlines
        if rv == 0:
            fired[i] = 1
    def main():
        for i in range(N):
            rc.fiber(lambda i=i: waiter(i))
    with hang_guard(20, "epoll deadline heap"):
        rc.fiber(main); rc.run()
    for r, w in pipes:
        _drop(r); os.close(w)
    assert sum(fired) == N, "%d/%d timed parks expired" % (sum(fired), N)


# --------------------------------------------------------------------------
# per-fd array growth + release/cancel paths
# --------------------------------------------------------------------------
def test_epoll_release_if_idle_and_cancel():
    res = {}
    def f():
        r, w = os.pipe()
        rc.wait_fd(r, READ, 5)                 # arm + time out
        rc.netpoll_release_if_idle(r)          # DEL + clear (no parker)
        os.close(r); os.close(w)
        # cancel a live parker
        r2, w2 = os.pipe()
        def parker():
            res["rv"] = rc.wait_fd(r2, READ, -1)
        g = rc.fiber(parker)
        rc.sched_yield(); rc.sched_yield()
        res["cancel"] = g.cancel_wait_fd()
        _drop(r2); os.close(w2)
    with hang_guard(15, "epoll release/cancel"):
        rc.fiber(f); rc.run()
    assert res.get("cancel") is True
    assert res.get("rv") == rc.WAIT_FD_CANCELLED


def test_epoll_many_distinct_fds_grow_by_fd_array():
    N = 200
    woke = bytearray(N)
    pairs = [socket.socketpair() for _ in range(N)]
    def reader(i):
        rd = pairs[i][0]
        if rc.wait_fd(rd.fileno(), READ, 5000) & READ and rd.recv(1) == b"!":
            woke[i] = 1
    def main():
        for i in range(N):
            rc.fiber(lambda i=i: reader(i))
        rc.sched_yield()
        for i in range(N):
            pairs[i][1].send(b"!")
    with hang_guard(40, "epoll many fds"):
        rc.fiber(main); rc.run()
    for rd, wr in pairs:
        _drop(rd.fileno()); wr.close()
    assert sum(woke) == N


# --------------------------------------------------------------------------
# io_uring global-ring eventfd path (the ring's eventfd is in the epoll set;
# the pump drains it -> netpoll_wake_iouring)
# --------------------------------------------------------------------------
@pytest.mark.skipif(not rc.iouring_available(), reason="io_uring not available")
def test_epoll_iouring_file_io_drains_eventfd():
    import tempfile
    ok = bytearray(16)
    def main():
        for i in range(16):
            def one(i=i):
                fd, path = tempfile.mkstemp()
                rc.file_write(fd, b"x" * 4096, 0)
                buf = bytearray(4096)
                if rc.file_read(fd, buf, 4096, 0) == 4096:
                    ok[i] = 1
                os.close(fd); os.unlink(path)
            rc.fiber(one)
    with hang_guard(30, "iouring eventfd"):
        rc.fiber(main); rc.run()
    assert sum(ok) == 16


# --------------------------------------------------------------------------
# diag / dump surface (netpoll_diag_fd.c.inc + introspection)
# --------------------------------------------------------------------------
def test_epoll_diag_dump_while_parked():
    snap = {}
    def f():
        socks = [socket.socketpair() for _ in range(10)]
        for a, b in socks:
            rc.fiber(lambda a=a: rc.wait_fd(a.fileno(), READ, -1))
        rc.sched_yield()
        snap["parked"] = rc.stats().get("netpoll_parked_self", 0)
        rc.dump_fibers(_DEVNULL)
        rc._dump_parkers()
        rc._diag_dump(_DEVNULL)
        snap["self_check"] = rc._self_check(1)         # verbose walk
        for a, b in socks:
            rc.netpoll_cancel_fd(a.fileno())            # wake them to drain cleanly
        rc.sched_yield()
        for a, b in socks:
            _drop(a.fileno()); b.close()
    with hang_guard(20, "epoll diag dump"):
        rc.fiber(f); rc.run()
    assert snap.get("parked", 0) >= 10
    assert snap.get("self_check") == 0


def test_epoll_netpoll_maxfd_env_subprocess():
    # RUNLOOM_NETPOLL_MAXFD caps the diag fd scan (netpoll_diag_fd.c.inc).
    script = (
        "import sys,os; sys.path.insert(0,'src');"
        "import runloom_c as rc;"
        "rc.fiber(lambda: rc.stats());"
        "rc.run();"
        "rc.dump_fibers(os.open(os.devnull,os.O_WRONLY));"
        "rc._dump_parkers();"
        "sys.stdout.write('MAXFD_OK\\n')")
    import subprocess
    env = dict(os.environ, RUNLOOM_NETPOLL_MAXFD="64", PYTHON_GIL="0", PYTHONPATH="src")
    p = subprocess.run([sys.executable, "-c", script], cwd=REPO, env=env,
                       capture_output=True, text=True, timeout=30)
    assert "MAXFD_OK" in p.stdout, (p.stdout, p.stderr[-500:])


# --------------------------------------------------------------------------
# cross-hub pump wake (wake_pump eventfd) under M:N
# --------------------------------------------------------------------------
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_epoll_cross_hub_pump_wake():
    from runloom.sync import WaitGroup
    N = 60
    ch = rc.Chan(0)
    got = [0]
    mu = rc.Mutex()
    def main():
        wg = WaitGroup(); wg.add(N)
        def producer():
            try:
                ch.send(1)
            finally:
                wg.done()
        def consumer():
            while True:
                v, ok = ch.recv()
                if not ok:
                    break
                mu.lock(); got[0] += 1; mu.unlock()
        for _ in range(4):
            rc.mn_fiber(consumer)
        for _ in range(N):
            rc.mn_fiber(producer)
        wg.wait()
        ch.close()
    with hang_guard(40, "cross-hub pump wake"):
        runloom.run(4, main)
    assert got[0] == N


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
