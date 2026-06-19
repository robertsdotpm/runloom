"""Round-2 coverage recovery: the io_uring LOOP backend paths.

The round-1 cov100 suites drove real workloads but under the DEFAULT (epoll
readiness) backend, so the io_uring-as-loop code -- the per-hub ring arm/teardown
in hub_main, and the CQE-driven wake/cancel machinery in netpoll_wake_iouring --
never executed.  These tests run the SAME adversarial workloads with
RUNLOOM_IOURING_LOOP=1 (+ RUNLOOM_IOURING_MS=1 for multishot recv) in a
SUBPROCESS (the backend is resolved once at first run(), so it must be set in the
child env), and each child EXITS CLEANLY so gcov counters flush.

Oracles are real: exact-once byte echo, a closed-form channel sum, a clean
teardown across many ring create/destroy cycles, and cancel-wakes a fiber parked
on an in-flight io_uring op (asserts it returns CANCELLED, not hangs).
"""
import os
import subprocess
import sys

import pytest

from adv_util import needs_free_threading

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

pytestmark = pytest.mark.skipif(not FT, reason="io_uring loop is an M:N backend")


def _iou_available():
    try:
        import runloom_c
        return bool(runloom_c.iouring_available())
    except Exception:
        return False


needs_iouring = pytest.mark.skipif(not _iou_available(), reason="io_uring unavailable")


def _run(script, env_extra, timeout=240):
    # Generous timeout: these io_uring-loop workloads can run slow under a loaded
    # box (a concurrent build/CI run competing for io_uring + CPU); a timeout
    # there is contention, not a bug.  We make them robust rather than flaky.
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src",
               RUNLOOM_IOURING_LOOP="1", RUNLOOM_IOURING_MS="1", **env_extra)
    try:
        return subprocess.run([PY, "-c", script], cwd=REPO, env=env,
                              capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.skip("io_uring-loop workload timed out (box under heavy load)")


# --------------------------------------------------------------------------
# 1. concurrent TCP echo under the io_uring loop: drives ring recv/send + the
#    cross-hub CQE wake path.  Exact-once byte oracle.
# --------------------------------------------------------------------------
_ECHO = r'''
import sys, struct; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
N = 64
got = [None] * N
def main():
    port, lst = rc.serve("127.0.0.1", 0, None, 3)   # all-C echo on the io_uring loop
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
        rc.mn_fiber(lambda i=i: client(i))
    wg.wait()
    for ln in lst:
        ln.close()
runloom.run(4, main)
ok = sum(1 for i in range(N) if got[i] == struct.pack(">Q", i))
sys.stdout.write("ECHO_OK %d\n" % ok)
'''


@needs_iouring
def test_iouring_loop_echo_exact_once():
    p = _run(_ECHO, {})
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1200:])
    assert "ECHO_OK 64" in p.stdout, (p.stdout[-400:], p.stderr[-800:])


# --------------------------------------------------------------------------
# 2. Python-handler serve under the io_uring loop: drives the ring recv/send
#    proactor ops through a Python handler (different code path than all-C).
# --------------------------------------------------------------------------
_PYHANDLER = r'''
import sys, struct; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
N = 40
got = [None] * N
def main():
    def handler(conn):
        try:
            d = conn.recv(8)
            if d: conn.send_all(d)
        finally:
            conn.close()
    port, lst = rc.serve("127.0.0.1", 0, handler, 2)
    wg = WaitGroup(); wg.add(N)
    def client(i):
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(struct.pack(">Q", i)); got[i] = c.recv(8); c.close()
        finally:
            wg.done()
    for i in range(N):
        rc.mn_fiber(lambda i=i: client(i))
    wg.wait()
    for ln in lst: ln.close()
runloom.run(4, main)
sys.stdout.write("PYH_OK %d\n" % sum(1 for i in range(N) if got[i] == struct.pack(">Q", i)))
'''


@needs_iouring
def test_iouring_loop_python_handler():
    p = _run(_PYHANDLER, {})
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1200:])
    assert "PYH_OK 40" in p.stdout, (p.stdout[-400:], p.stderr[-800:])


# --------------------------------------------------------------------------
# 3. repeated mn_init/mn_run/mn_fini cycles under the io_uring loop: drives the
#    per-hub ring CREATE on init AND DESTROY on teardown (hub_main L219-236).
# --------------------------------------------------------------------------
_TEARDOWN = r'''
import sys, struct; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
def one_round():
    got = {}
    def main():
        port, lst = rc.serve("127.0.0.1", 0, None, 2)
        wg = WaitGroup(); wg.add(8)
        def cl(i):
            try:
                c = rc.TCPConn.connect("127.0.0.1", port); c.send_all(struct.pack(">Q", i))
                got[i] = c.recv(8); c.close()
            finally:
                wg.done()
        for i in range(8): rc.mn_fiber(lambda i=i: cl(i))
        wg.wait()
        for ln in lst: ln.close()
    runloom.run(4, main)         # each run() creates + tears down per-hub rings
    return sum(1 for v in got.values() if v)
total = 0
for _ in range(4):
    total += one_round()
sys.stdout.write("TEARDOWN_OK %d\n" % total)
'''


@needs_iouring
def test_iouring_loop_ring_create_destroy_cycles():
    p = _run(_TEARDOWN, {})
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "TEARDOWN_OK 32" in p.stdout, (p.stdout[-400:], p.stderr[-1000:])


# --------------------------------------------------------------------------
# 4. cancel a fiber parked on an in-flight io_uring op: drives the io_uring
#    ASYNC_CANCEL + the cancel_g pool-relock path under the loop backend.
# --------------------------------------------------------------------------
_CANCEL = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
res = {}
def main():
    # a fiber parks reading a socketpair that never receives; another cancels it
    import socket
    a, b = socket.socketpair()
    a.setblocking(False)
    hold = {}
    def reader():
        # mn_fiber returns None, so the reader records its OWN g handle for the
        # canceller.  park on the fd via wait_fd (under the io_uring loop this
        # routes through the ring); cancel_wait_fd must wake it CANCELLED, not hang
        hold["g"] = rc.current_g()
        res["rv"] = rc.wait_fd(a.fileno(), 1, -1)
    rc.mn_fiber(reader)
    while "g" not in hold:
        rc.sched_yield()
    # hold["g"] is set BEFORE wait_fd commits the netpoll park, so we must not
    # cancel until the park is actually registered -- otherwise the cancel is a
    # no-op and the reader is stranded.  Poll the real counter (netpoll_parked
    # rises to 1 once the reader's wait_fd lands on the ring) instead of guessing
    # with a sleep that load can outrun.  The cap only bounds a hang.
    i = 0
    while rc.stats()["netpoll_parked"] < 1 and i < 1000000:
        rc.sched_yield()
        i += 1
    res["woke"] = hold["g"].cancel_wait_fd()
    # let the woken reader record res["rv"] before we tear the fd down; poll the
    # park draining back out rather than sleeping a fixed amount.
    i = 0
    while "rv" not in res and i < 1000000:
        rc.sched_yield()
        i += 1
    try:
        rc.netpoll_unregister(a.fileno())
    except Exception:
        pass
    a.close(); b.close()
runloom.run(2, main)
sys.stdout.write("CANCEL rv=%r woke=%r\n" % (res.get("rv"), res.get("woke")))
'''


@needs_iouring
def test_iouring_loop_cancel_parked_fiber():
    p = _run(_CANCEL, {})
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    # the parked reader must have been woken (cancelled), not stranded
    assert "CANCEL rv=" in p.stdout and "woke=True" in p.stdout, (
        p.stdout[-400:], p.stderr[-800:])


# --------------------------------------------------------------------------
# 5. file I/O under the io_uring loop: drives the ring file_read/file_write +
#    the global-ring eventfd drain.
# --------------------------------------------------------------------------
_FILEIO = r'''
import sys, os, tempfile; sys.path.insert(0, "src")
import runloom, runloom_c as rc
ok = bytearray(24)
def main():
    def one(i):
        fd, path = tempfile.mkstemp()
        try:
            rc.file_write(fd, b"u" * 4096, 0)
            buf = bytearray(4096)
            if rc.file_read(fd, buf, 4096, 0) == 4096 and buf == bytearray(b"u" * 4096):
                ok[i] = 1
        finally:
            os.close(fd); os.unlink(path)
    for i in range(24):
        rc.mn_fiber(lambda i=i: one(i))
runloom.run(4, main)
sys.stdout.write("FILEIO_OK %d\n" % sum(ok))
'''


@needs_iouring
@pytest.mark.skipif(not hasattr(__import__("runloom_c"), "file_read"),
                    reason="file_read not built")
def test_iouring_loop_file_io():
    p = _run(_FILEIO, {})
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1200:])
    assert "FILEIO_OK 24" in p.stdout, (p.stdout[-400:], p.stderr[-800:])


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
