"""Work-curve server: ONE program, one knob (--work N = FNV-1a passes over the
payload), three handler implementations spanning interpreted -> state-of-the-art:

  --handler py      interpreted Python: a `def` doing conn.recv_into / py_fnv /
                    fold / conn.send_all -- every step in the interpreter.
  --handler cython  the FASTEST / properly-optimized path: the zero-PyObject
                    Cython handler (handler_cy) with the FNV INLINE -- capi recv,
                    native FNV, fold, capi send. No Python def wrapper, no
                    per-call boxing; the whole request path is native. This is
                    runloom's state of the art (the line that competes with Go).
  --handler cdef    handler_cdef: the same native work but on the tstate-free
                    c_entry path (no Python frame at all) -- the extreme.

--work 0 is a pure echo in every mode. The work is pure inline arithmetic (no
stdlib, no blockpool offload), so it runs on the fiber's hub -- a valid per-hub
measurement. The compiled handlers (cython/cdef) set their work knob via
set_work() before serve() spawns any fiber.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # find handler_cy/cdef *.so

import runloom
import runloom_c

CHUNK = 65536
FNV_OFF = 2166136261        # 0x811c9dc5
FNV_PRIME = 16777619        # 0x01000193


def py_fnv(buf, n, passes):
    """Pure-Python FNV-1a -- the interpreted twin of the inline FNV in the
    compiled handlers (handler_cy / handler_cdef)."""
    h = FNV_OFF
    for _ in range(passes):
        for i in range(n):
            h = ((h ^ buf[i]) * FNV_PRIME) & 0xffffffff
    return h


def make_handle(work_fn, passes):
    def handle(conn):
        buf = bytearray(CHUNK)
        mv = memoryview(buf)
        try:
            if passes == 0:
                while True:                       # --work 0 == echo
                    n = conn.recv_into(buf)
                    if not n:
                        break
                    conn.send_all(mv[:n])
            else:
                while True:
                    n = conn.recv_into(buf)
                    if not n:
                        break
                    h = work_fn(buf, n, passes)   # the only variable: py vs compiled
                    buf[0] = (buf[0] ^ (h & 0xff)) & 0xff   # fold in -> no dead-code elision
                    conn.send_all(mv[:n])
        except OSError:
            pass
    return handle


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="10.99.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--hubs", type=int, default=int((os.cpu_count() or 1) * 0.7))
    ap.add_argument("--handler", default="py", choices=["py", "cython", "cdef"])
    ap.add_argument("--work", type=int, default=0, help="FNV passes over the payload (0 = echo)")
    ap.add_argument("--token", default="")
    args = ap.parse_args()

    # cython / cdef: the work runs INLINE inside the compiled, zero-PyObject
    # handler -- the whole request path is native (the Go-comparable line). py:
    # the interpreted Python def. Compiled handlers take the work via set_work().
    if args.handler == "cython":
        import handler_cy            # zero-PyObject Cython def, work inline
        handler_cy.set_work(args.work)
        srv_handler = handler_cy.handler
    elif args.handler == "cdef":
        import handler_cdef          # tstate-free c_entry, work inline
        handler_cdef.set_work(args.work)
        srv_handler = handler_cdef.handler
    else:
        srv_handler = make_handle(py_fnv, args.work)   # interpreted baseline

    def root():
        port, listeners = runloom_c.serve(args.host, args.port, srv_handler,
                                          acceptors=args.hubs, backlog=4096)
        print("LISTENING %d" % port, flush=True)
        runloom.sleep(float("inf"))

    runloom.run(args.hubs, main_fn=root)


if __name__ == "__main__":
    main()
