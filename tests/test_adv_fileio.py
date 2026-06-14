"""Adversarial QA: file + fd I/O.

  * file_read / file_write -- the io_uring path on Linux (pread/pwrite fallback
    elsewhere): round-trip, offsets, n-out-of-range, concurrent ops;
  * fd_read / fd_write -- POSIX read/write with cooperative netpoll parking on a
    NON-BLOCKING fd.

Round-A finding (encoded xfail): fd_read/fd_write rely on EAGAIN to park, but do
NOT set O_NONBLOCK themselves.  On a BLOCKING fd, read() blocks the whole
scheduler OS thread instead of cooperatively parking -- a silent wedge, not an
error.  monkey's os.read patch sets non-blocking first; a raw fd_read caller who
forgets gets a hung scheduler.
"""
import os
import subprocess
import sys
import tempfile

import pytest

import runloom_c as rc
from adv_util import hang_guard

POSIX = sys.platform != "win32"


def _run_single(fn):
    box = {}
    def main():
        box["r"] = fn()
    rc.go(main)
    rc.run()
    return box.get("r")


# --------------------------------------------------------------------------
# file_read / file_write (io_uring on Linux)
# --------------------------------------------------------------------------
def test_file_write_read_roundtrip_with_offset():
    def f():
        fd, path = tempfile.mkstemp()
        try:
            assert rc.file_write(fd, b"hello world", 0) == 11
            buf = bytearray(11)
            assert rc.file_read(fd, buf, 11, 0) == 11
            assert bytes(buf) == b"hello world"
            buf2 = bytearray(5)
            assert rc.file_read(fd, buf2, 5, 6) == 5   # offset read -> "world"
            assert bytes(buf2) == b"world"
            return "ok"
        finally:
            os.close(fd); os.unlink(path)
    with hang_guard(15, "file roundtrip"):
        assert _run_single(f) == "ok"


def test_file_read_n_out_of_range_raises():
    def f():
        fd, path = tempfile.mkstemp()
        try:
            buf = bytearray(4)
            with pytest.raises(ValueError):
                rc.file_read(fd, buf, 99, 0)           # n > buffer
            return "ok"
        finally:
            os.close(fd); os.unlink(path)
    assert _run_single(f) == "ok"


def test_many_concurrent_file_ops():
    N = 24
    results = bytearray(N)
    def main():
        from runloom.sync import WaitGroup
        wg = WaitGroup(); wg.add(N)
        def worker(i):
            try:
                fd, path = tempfile.mkstemp()
                payload = ("data-%05d-payload" % i).encode()
                rc.file_write(fd, payload, 0)
                buf = bytearray(len(payload))
                rc.file_read(fd, buf, len(payload), 0)
                if bytes(buf) == payload:
                    results[i] = 1
                os.close(fd); os.unlink(path)
            finally:
                wg.done()
        for i in range(N):
            rc.go(lambda i=i: worker(i))
        wg.wait()
    with hang_guard(40, "concurrent file ops"):
        rc.go(main); rc.run()
    assert sum(results) == N, "%d/%d concurrent file ops correct" % (sum(results), N)


# --------------------------------------------------------------------------
# fd_read / fd_write on a NON-BLOCKING pipe (the correct, cooperative usage)
# --------------------------------------------------------------------------
@pytest.mark.skipif(not POSIX, reason="POSIX pipe fd model")
def test_fd_read_write_nonblocking_pipe_cooperative():
    out = {}
    hold = {}
    def main():
        r, w = os.pipe()
        os.set_blocking(r, False)
        os.set_blocking(w, False)
        hold["fds"] = (r, w)
        def writer():
            for _ in range(3):
                rc.sched_yield()
            rc.fd_write(w, b"pipe!")
        def reader():
            buf = bytearray(5)
            n = rc.fd_read(r, buf, 5)
            out["data"] = bytes(buf[:n])
        rc.go(reader)
        rc.go(writer)
    with hang_guard(15, "fd nonblocking pipe"):
        rc.go(main); rc.run()
    r, w = hold["fds"]
    rc.netpoll_unregister(r); os.close(r)
    rc.netpoll_unregister(w); os.close(w)
    assert out.get("data") == b"pipe!"


# --------------------------------------------------------------------------
# FINDING: fd_read on a BLOCKING fd wedges the scheduler (no O_NONBLOCK -> no
# EAGAIN -> read() blocks the OS thread).  Demonstrated in a subprocess (it would
# hang the suite); encoded as the ideal (should cooperate or raise EWOULDBLOCK).
# --------------------------------------------------------------------------
_WEDGE_SCRIPT = r'''
import sys, os; sys.path.insert(0, "src")
import runloom_c as rc
def main():
    r, w = os.pipe()                 # BLOCKING by default (no O_NONBLOCK)
    def sib():
        # a sibling that would run IF fd_read parked cooperatively
        for _ in range(5): rc.sched_yield()
        os.write(w, b"X")
    rc.go(sib)
    buf = bytearray(1)
    rc.fd_read(r, buf, 1)            # blocks the OS thread -> sib never runs
    sys.stdout.write("READ_RETURNED\n")
rc.go(main); rc.run()
sys.stdout.write("DONE\n")
'''


@pytest.mark.skipif(not POSIX, reason="POSIX fd model")
@pytest.mark.xfail(strict=False, reason=(
    "FINDING: fd_read/fd_write rely on EAGAIN to park but never set O_NONBLOCK. "
    "On a BLOCKING fd, read() blocks the whole scheduler OS thread instead of "
    "cooperatively parking -- a silent wedge, not an error. It should either set "
    "non-blocking, or raise/detect a blocking fd, rather than deadlock the "
    "scheduler. (monkey's os.read patch sets non-blocking first, so high-level "
    "users are safe; the raw primitive is the footgun.)"))
def test_fd_read_on_blocking_fd_should_not_wedge():
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    # If fd_read cooperates/returns, the subprocess prints READ_RETURNED quickly.
    # If it wedges (current behaviour), subprocess.run raises TimeoutExpired,
    # which xfail records as the expected failure.
    p = subprocess.run([sys.executable, "-c", _WEDGE_SCRIPT], cwd=repo, env=env,
                       capture_output=True, text=True, timeout=5)
    assert "READ_RETURNED" in p.stdout, (
        "fd_read on a blocking fd did not cooperate (out=%r)" % p.stdout)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
