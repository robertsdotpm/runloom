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
    rc.fiber(main)
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
            rc.fiber(lambda i=i: worker(i))
        wg.wait()
    with hang_guard(40, "concurrent file ops"):
        rc.fiber(main); rc.run()
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
        rc.fiber(reader)
        rc.fiber(writer)
    with hang_guard(15, "fd nonblocking pipe"):
        rc.fiber(main); rc.run()
    r, w = hold["fds"]
    rc.netpoll_unregister(r); os.close(r)
    rc.netpoll_unregister(w); os.close(w)
    assert out.get("data") == b"pipe!"


# --------------------------------------------------------------------------
# Regression (was a finding): fd_read/fd_write on a BLOCKING fd used to wedge the
# whole scheduler -- they park on EAGAIN but never set O_NONBLOCK, so read()/
# write() blocked the OS thread.  They now set the fd non-blocking themselves.
# --------------------------------------------------------------------------
@pytest.mark.skipif(not POSIX, reason="POSIX fd model")
def test_fd_read_on_blocking_fd_cooperates():
    out = {}
    hold = {}
    def main():
        r, w = os.pipe()                       # BLOCKING by default (no O_NONBLOCK)
        hold["fds"] = (r, w)
        def sib():
            for _ in range(5):
                rc.sched_yield()               # only runs if fd_read PARKED
            os.write(w, b"X")
        rc.fiber(sib)
        def reader():
            buf = bytearray(1)
            out["n"] = rc.fd_read(r, buf, 1)   # must park cooperatively, not wedge
            out["buf"] = bytes(buf)
        rc.fiber(reader)
    with hang_guard(10, "fd_read blocking fd"):
        rc.fiber(main); rc.run()
    r, w = hold["fds"]
    rc.netpoll_unregister(r); os.close(r); os.close(w)
    assert out.get("buf") == b"X", "fd_read wedged or lost data on a blocking fd"


# --------------------------------------------------------------------------
# Regression (lens error-cancel-path-untested, module_fdio.c.inc:73/:118):
# m_fd_read / m_fd_write parked on a pipe called the RAW runloom_netpoll_wait_fd
# and tested `< 0`.  A cancel (netpoll_cancel_fd / cancel_all_parked / G.cancel
# _wait_fd) wakes with the POSITIVE RUNLOOM_NETPOLL_CANCELLED sentinel, so the
# `< 0` test MISSED it and the loop re-parked FOREVER -- a hang + fiber leak.
# The fix routes through runloom_netpoll_wait_fd_coop (CANCELLED -> ECANCELED/-1),
# so a cancel surfaces as OSError(ECANCELED), like every sibling cooperative path.
# --------------------------------------------------------------------------
@pytest.mark.skipif(not POSIX, reason="POSIX fd model")
def test_fd_read_cancel_unblocks_not_hang():
    import errno as _errno
    out = {}
    hold = {}
    def main():
        r, w = os.pipe()
        os.set_blocking(r, False)
        hold["fds"] = (r, w)
        def reader():
            buf = bytearray(4)
            try:
                rc.fd_read(r, buf, 4)          # empty pipe -> parks READ
                out["result"] = ("returned",)
            except OSError as e:
                out["result"] = ("oserror", e.errno)
        def canceller():
            for _ in range(6):
                rc.sched_yield()               # let the reader commit to the park
            rc.netpoll_cancel_fd(r)            # cancel it: must NOT re-park forever
        rc.fiber(reader)
        rc.fiber(canceller)
    with hang_guard(10, "fd_read cancel"):     # bug => faulthandler _exit => fail
        rc.fiber(main); rc.run()
    r, w = hold["fds"]
    rc.netpoll_unregister(r); os.close(r); os.close(w)
    assert out.get("result", ("hang",))[0] == "oserror", (
        "fd_read ignored the cancel and re-parked (result=%r)" % (out.get("result"),))
    assert out["result"][1] == _errno.ECANCELED


@pytest.mark.skipif(not POSIX, reason="POSIX fd model")
def test_fd_write_cancel_unblocks_not_hang():
    import errno as _errno
    out = {}
    hold = {}
    def main():
        r, w = os.pipe()
        os.set_blocking(w, False)
        hold["fds"] = (r, w)
        try:                                   # fill the pipe so fd_write parks WRITE
            while True:
                os.write(w, b"x" * 65536)
        except OSError:
            pass                               # EAGAIN: pipe full
        def writer():
            try:
                rc.fd_write(w, b"y" * 65536)   # full pipe -> parks WRITE
                out["result"] = ("returned",)
            except OSError as e:
                out["result"] = ("oserror", e.errno)
        def canceller():
            for _ in range(6):
                rc.sched_yield()
            rc.netpoll_cancel_fd(w)
        rc.fiber(writer)
        rc.fiber(canceller)
    with hang_guard(10, "fd_write cancel"):
        rc.fiber(main); rc.run()
    r, w = hold["fds"]
    rc.netpoll_unregister(w); os.close(r); os.close(w)
    assert out.get("result", ("hang",))[0] == "oserror", (
        "fd_write ignored the cancel and re-parked (result=%r)" % (out.get("result"),))
    assert out["result"][1] == _errno.ECANCELED


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
