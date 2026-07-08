"""Coverage: default-offset semantics for file_read/file_write and the EOF
contract for fd_read.

Two gaps the existing fdio tests (test_adv_fileio.py always passes an EXPLICIT
offset 0; test_fd_io_faultinject.py only faults the retry paths) never exercise:

  (1) file_read / file_write with the DEFAULT offset (sentinel -1) must use +
      ADVANCE the current fd position, exactly like read(2)/write(2):
        * two default-offset file_write calls append (AAAA then BBBB -> AAAABBBB);
        * two default-offset file_read calls on a seeded file return advancing,
          non-equal chunks (not both restarting at offset 0).
      On Linux this rides the io_uring off==(u64)-1 "current position" feature
      (kernel 5.17+); the pread/pwrite fallback uses read()/write() for offset<0.

  (2) fd_read returns EXACTLY 0 at EOF: a fiber writes N bytes to a non-blocking
      pipe then closes the write end; the reader loops fd_read until it returns 0
      and asserts total==N and the final call was 0 (the r==0 EOF break in
      m_fd_read, module_fdio.c.inc:67).

no-gil, POSIX (the io_uring / pipe fd model).  A hang fails as a watchdog _exit,
never a wedged suite.
"""
import os
import sys
import tempfile

import pytest

import runloom_c as rc
from adv_util import hang_guard

POSIX = sys.platform != "win32"

pytestmark = pytest.mark.skipif(not POSIX, reason="POSIX fd / io_uring model")


def _run_single(fn):
    box = {}

    def main():
        box["r"] = fn()

    rc.fiber(main)
    rc.run()
    return box.get("r")


# --------------------------------------------------------------------------
# Gap 1a: file_write with the DEFAULT offset appends (advances the position).
# --------------------------------------------------------------------------
def test_file_write_default_offset_appends():
    def f():
        fd, path = tempfile.mkstemp()
        try:
            # No offset arg -> sentinel -1 -> "use + advance current position".
            assert rc.file_write(fd, b"AAAA") == 4
            assert rc.file_write(fd, b"BBBB") == 4
            # Read the file fresh from the start: sequential default-offset
            # writes must have landed at 0 then 4, not both clobbering 0.
            rfd = os.open(path, os.O_RDONLY)
            try:
                data = os.read(rfd, 32)
            finally:
                os.close(rfd)
            return data
        finally:
            os.close(fd)
            os.unlink(path)
    with hang_guard(15, "file_write default offset append"):
        data = _run_single(f)
    assert data == b"AAAABBBB", (
        "default-offset file_write did not advance the position: %r "
        "(expected b'AAAABBBB')" % (data,))


# --------------------------------------------------------------------------
# Gap 1b: two default-offset file_read calls return advancing, non-equal chunks.
# --------------------------------------------------------------------------
def test_file_read_default_offset_advances():
    # Seed a file whose two leading 4-byte chunks DIFFER, so "advanced" vs
    # "restarted at 0" is observable (both-at-0 would give equal chunks).
    seed = b"ABCDEFGH-and-more-tail"
    fd, path = tempfile.mkstemp()
    os.write(fd, seed)
    os.close(fd)

    def f():
        rfd = os.open(path, os.O_RDONLY)
        try:
            b1 = bytearray(4)
            b2 = bytearray(4)
            n1 = rc.file_read(rfd, b1, 4)      # default offset -> "ABCD", pos->4
            n2 = rc.file_read(rfd, b2, 4)      # default offset -> "EFGH", pos->8
            return n1, bytes(b1), n2, bytes(b2)
        finally:
            os.close(rfd)
    try:
        with hang_guard(15, "file_read default offset advance"):
            n1, c1, n2, c2 = _run_single(f)
    finally:
        os.unlink(path)
    assert (n1, n2) == (4, 4), "short read: n1=%d n2=%d" % (n1, n2)
    assert c1 != c2, (
        "default-offset file_read did NOT advance: both chunks are %r "
        "(second read restarted at offset 0)" % (c1,))
    assert c1 == b"ABCD", "first default-offset chunk %r != b'ABCD'" % (c1,)
    assert c2 == b"EFGH", "second default-offset chunk %r != b'EFGH'" % (c2,)
    assert c1 + c2 == seed[:8]


# --------------------------------------------------------------------------
# Gap 2: fd_read returns EXACTLY 0 at EOF once the write end is closed and the
# pipe is drained.  Loop until the 0 sentinel; assert total==N and last==0.
# --------------------------------------------------------------------------
def test_fd_read_returns_zero_at_eof():
    N = 5000                     # < pipe capacity (64 KiB) so the writer never parks
    CHUNK = 1000                 # < N so the reader loops several times before EOF
    out = {}
    hold = {}

    def main():
        r, w = os.pipe()
        os.set_blocking(r, False)
        os.set_blocking(w, False)
        hold["fds"] = (r, w)

        def writer():
            payload = bytes(bytearray((i % 251) for i in range(N)))
            wrote = rc.fd_write(w, payload)
            out["wrote"] = wrote
            os.close(w)          # close write end -> reader's next drained read == EOF(0)

        def reader():
            buf = bytearray(CHUNK)
            total = 0
            last = None
            calls = 0
            while True:
                calls += 1
                n = rc.fd_read(r, buf, CHUNK)
                last = n
                if n == 0:
                    break        # EOF sentinel
                total += n
            out["total"] = total
            out["last"] = last
            out["calls"] = calls

        # writer first: it never parks (N < capacity), writes all + closes, so the
        # reader then drains N bytes and observes a clean EOF.
        rc.fiber(writer)
        rc.fiber(reader)

    with hang_guard(15, "fd_read EOF"):
        rc.fiber(main)
        rc.run()

    r, w = hold["fds"]
    rc.netpoll_unregister(r)
    os.close(r)                  # w already closed by the writer fiber

    assert out.get("wrote") == N, "writer wrote %r of %d" % (out.get("wrote"), N)
    assert out.get("total") == N, (
        "reader accumulated %r bytes, expected %d" % (out.get("total"), N))
    assert out.get("last") == 0, (
        "fd_read did not return the 0 EOF sentinel (last=%r)" % (out.get("last"),))
    assert out.get("calls", 0) >= 2, "expected multiple fd_read calls, got %r" % (
        out.get("calls"),)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
