"""Regression: io_uring recv must not deadlock on a backpressured transfer.

History: forcing recv through the opt-in io_uring backend
(RUNLOOM_TCPCONN_IOURING=1) used to DEADLOCK a large loopback transfer -- under
backpressure the kernel CQ ring overflows, excess completions go to the kernel's
overflow backlog and do NOT re-signal the registered eventfd, so the scheduler
slept forever in epoll_wait waiting for an edge that never came, stranding the
receiver whose completion was in overflow (CQ empty + IORING_SQ_CQ_OVERFLOW set).

Fix: the scheduler/hub idle paths now drain io_uring (which flushes the CQ
overflow backlog) before blocking -- runloom_sched_drain.c.inc and
mn_sched_hub_main.c.inc. This test drives a 4 MiB single-connection transfer
through io_uring on BOTH the single-thread scheduler and the M:N scheduler and
asserts it completes byte-exact within a bounded time (a regression re-hang fails
the test via the subprocess timeout / the child's own watchdog, never hangs the
suite).

NB: io_uring multishot recv across MANY concurrent connections under M:N is a
separate known issue (data loss, not a hang) and is intentionally not covered
here.
"""
import os
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="io_uring is Linux-only")


def _iouring_available():
    try:
        out = subprocess.run(
            [PY, "-c", "import sys;sys.path.insert(0,'src');import runloom_c;"
                       "print(runloom_c.iouring_available())"],
            cwd=REPO, env=dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src"),
            capture_output=True, text=True, timeout=30)
        return "True" in out.stdout
    except Exception:
        return False


requires_iouring = pytest.mark.skipif(
    not _iouring_available(), reason="io_uring not available on this kernel")


_TRANSFER = r"""
import socket, sys, zlib, faulthandler
sys.path.insert(0, "src")
import runloom_c
faulthandler.dump_traceback_later({wd}, exit=True)   # re-hang -> die, never wedge
SIZE = 4 * 1024 * 1024
def bound_port(l):
    fd = l.fileno(); sk = socket.socket(fileno=socket.dup(fd))
    p = sk.getsockname()[1]; sk.close(); return p
payload = (bytes(range(256)) * ((SIZE + 255)//256))[:SIZE]
want = zlib.crc32(payload)
ph = [None]; got = [None]
def server():
    l = runloom_c.TCPConn.listen("127.0.0.1", 0); ph[0] = bound_port(l)
    c = l.accept(); crc = 0; tot = 0
    while True:
        ch = c.recv(65536)
        if not ch: break
        crc = zlib.crc32(ch, crc); tot += len(ch)
    got[0] = (tot, crc); c.close(); l.close()
def client():
    while ph[0] is None: runloom_c.sched_yield()
    c = runloom_c.TCPConn.connect("127.0.0.1", ph[0]); c.send_all(payload); c.close()
{drive}
assert got[0] == (SIZE, want), ("incomplete/corrupt transfer", got[0], (SIZE, want))
print("TRANSFER_OK")
"""

_DRIVE_ST = "runloom_c.go(server); runloom_c.go(client); runloom_c.run()"
_DRIVE_MN = ("runloom_c.mn_init(2); runloom_c.mn_go(server); runloom_c.mn_go(client); "
             "runloom_c.mn_run(); runloom_c.mn_fini()")


def _run(drive, wd=20):
    body = _TRANSFER.format(wd=wd, drive=drive)
    try:
        p = subprocess.run(
            [PY, "-c", body], cwd=REPO,
            env=dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src",
                     RUNLOOM_TCPCONN_IOURING="1"),
            capture_output=True, text=True, timeout=wd + 25)
    except subprocess.TimeoutExpired:
        pytest.fail("io_uring backpressure transfer HUNG (deadlock regression)")
    assert p.returncode == 0, (
        "io_uring backpressure transfer failed rc=%d (negative => watchdog "
        "killed a hang)\nstdout=%s\nstderr=%s"
        % (p.returncode, p.stdout[-800:], p.stderr[-1500:]))
    assert "TRANSFER_OK" in p.stdout, (p.stdout, p.stderr[-800:])


@requires_iouring
def test_single_thread_io_uring_backpressure_transfer_completes():
    _run(_DRIVE_ST)


@requires_iouring
def test_mn_io_uring_backpressure_transfer_completes():
    _run(_DRIVE_MN)
