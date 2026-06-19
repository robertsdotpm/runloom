"""Subprocess workload for the fd_read/fd_write (module.c) fault harness.

Drives cooperative runloom_c.fd_read / fd_write over an os.pipe() as fibers,
so an injected read()/write() error lands on the live EINTR-continue /
EAGAIN-park / surface-OSError loop.  Modes:
  echo      -- writer sends b"ping", reader reads it; must print "OK ping".
  readfail  -- data pre-written; a single fd_read with an injected hard error
               must surface OSError (no writer fiber, so it can't hang).
  writefail -- a single fd_write with an injected hard error must surface
               OSError (no reader fiber).
Prints one status line (exit 0=OK, 42=clean OSError) + FAULTS=<n> when
FAULT_SITE is set.  no-gil only.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "src"))
import runloom_c


def _drive(*fibers):
    box = []

    def wrap(fn):
        def runner():
            try:
                fn()
            except BaseException as e:   # noqa: BLE001 - reported to caller
                box.append(e)
        return runner

    for g in fibers:
        runloom_c.fiber(wrap(g))
    runloom_c.run()
    return box


def _pipe():
    r, w = os.pipe()
    os.set_blocking(r, False)
    os.set_blocking(w, False)
    return r, w


def mode_echo():
    r, w = _pipe()
    out = {}

    def writer():
        runloom_c.fd_write(w, b"ping")

    def reader():
        buf = bytearray(64)
        n = runloom_c.fd_read(r, buf, 64)
        out["data"] = bytes(buf[:n])

    errs = _drive(writer, reader)
    os.close(r); os.close(w)
    if errs:
        e = errs[0]
        if isinstance(e, OSError):
            print("OSERROR errno=%s" % e.errno); return 42
        print("FAIL exc=%r" % e); return 1
    if out.get("data") == b"ping":
        print("OK ping"); return 0
    print("FAIL data=%r" % out.get("data")); return 1


def mode_readfail():
    r, w = _pipe()
    os.write(w, b"ping")          # data present; only the injection fails the read
    box = {}

    def reader():
        try:
            buf = bytearray(64)
            n = runloom_c.fd_read(r, buf, 64)
            box["data"] = bytes(buf[:n])
        except OSError as e:
            box["errno"] = e.errno

    _drive(reader)
    os.close(r); os.close(w)
    if "errno" in box:
        print("OSERROR errno=%s" % box["errno"]); return 42
    print("DATA=%r" % box.get("data")); return 0


def mode_writefail():
    r, w = _pipe()
    box = {}

    def writer():
        try:
            runloom_c.fd_write(w, b"ping")
            box["wrote"] = True
        except OSError as e:
            box["errno"] = e.errno

    _drive(writer)
    os.close(r); os.close(w)
    if "errno" in box:
        print("OSERROR errno=%s" % box["errno"]); return 42
    print("WROTE=%s" % box.get("wrote")); return 0


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "echo"
    dispatch = {"echo": mode_echo, "readfail": mode_readfail,
                "writefail": mode_writefail}
    fn = dispatch.get(mode)
    if fn is None:
        print("BADMODE %r" % mode); return 2
    rc = fn()
    site = os.environ.get("FAULT_SITE")
    if site:
        try:
            print("FAULTS=%d" % runloom_c._fault_count(site))
        except Exception:
            pass
    return rc


if __name__ == "__main__":
    sys.exit(main())
