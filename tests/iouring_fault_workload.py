"""Subprocess workload for the io_uring fault-injection harness.

Run STANDALONE (not collected by pytest) under ``strace -e inject=`` by
test_iouring_faultinject.py.  Exercises the io_uring file-I/O path
(pygo_core.file_read / file_write) from inside a goroutine so an error injected
into io_uring_setup / io_uring_enter hits a real submit + completion path.

Modes (argv[1]):
  fileread -- write known bytes, then file_read them back from a goroutine;
              must round-trip and print "OK <n>".
  badfd    -- file_read on a closed fd; the CQE must complete with res<0 and
              surface as a clean OSError (print "OSERROR errno=<e>").

Prints one status line; exit 0 = expected success, 42 = clean OSError, other =
unexpected.  iouring_available() is checked first: prints "NOIOURING" / exit 7
so the harness can skip when io_uring is gone (e.g. setup injected to fail and
NO fallback is exercised in this mode).
"""
import os
import sys
import tempfile
import threading

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "src"))
import pygo_core

PAYLOAD = b"io_uring fault workload payload " * 64


def mode_fileread():
    # file_read transparently falls back to pread when io_uring is unavailable,
    # so this mode is valid whether or not setup succeeded -- it asserts the
    # DATA is correct either way.
    path = tempfile.mktemp()
    with open(path, "wb") as f:
        f.write(PAYLOAD)
    out = {}

    def worker():
        fd = os.open(path, os.O_RDONLY)
        try:
            buf = bytearray(len(PAYLOAD))
            try:
                n = pygo_core.file_read(fd, buf, len(PAYLOAD), 0)
                out["n"] = n
                out["ok"] = (bytes(buf) == PAYLOAD)
            except OSError as e:
                out["errno"] = e.errno
        finally:
            os.close(fd)

    pygo_core.go(worker)
    pygo_core.run()
    os.unlink(path)
    if "errno" in out:
        print("OSERROR errno=%d" % out["errno"])
        return 42
    if out.get("ok") and out.get("n") == len(PAYLOAD):
        print("OK %d" % out["n"])
        return 0
    print("FAIL out=%r" % out)
    return 1


def mode_badfd():
    out = {}

    def worker():
        fd = os.open(os.devnull, os.O_RDONLY)
        os.close(fd)                      # now stale -> read must fail EBADF
        buf = bytearray(16)
        try:
            pygo_core.file_read(fd, buf, 16, 0)
            out["unexpected"] = True
        except OSError as e:
            out["errno"] = e.errno

    pygo_core.go(worker)
    pygo_core.run()
    if out.get("unexpected"):
        print("FAIL read on closed fd did not error")
        return 1
    print("OSERROR errno=%d" % out.get("errno", -1))
    return 42


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "fileread"
    print("IOURING_AVAIL=%d" % (1 if pygo_core.iouring_available() else 0),
          file=sys.stderr)
    if mode == "fileread":
        return mode_fileread()
    if mode == "badfd":
        return mode_badfd()
    print("BADMODE %r" % mode)
    return 2


if __name__ == "__main__":
    sys.exit(main())
