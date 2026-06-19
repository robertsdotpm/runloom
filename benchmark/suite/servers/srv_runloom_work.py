"""Work-curve server: ONE program that consolidates echo + the compute sweep.

`--work 0` is the echo (the handler skips the work call -> identical recv->send,
reproduces the echo numbers). `--work N>0` runs an FNV-1a byte hash over the
payload N times before echoing -- the SAME algorithm whether `--handler py`
(interpreted) or `--handler cython` (compiled work_cy.fnv_work). I/O is identical
in both (TCPConn recv_into/send_all); the ONLY variable is whether the handler's
work is interpreted or compiled. Swept across N, this is the curve that shows
"what compiling the handler buys" -- the thing echo structurally cannot show.

The work is PURE inline arithmetic (no stdlib, no I/O, no blockpool-offloaded
call), so it runs on the fiber's hub, never a worker thread -- required for a
valid per-hub measurement.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # find work_cy*.so

import runloom
import runloom_c

CHUNK = 65536
FNV_OFF = 2166136261        # 0x811c9dc5
FNV_PRIME = 16777619        # 0x01000193


def py_fnv(buf, n, passes):
    """Pure-Python FNV-1a -- the interpreted twin of work_cy.fnv_work."""
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
    ap.add_argument("--handler", default="py", choices=["py", "cython"])
    ap.add_argument("--work", type=int, default=0, help="FNV passes over the payload (0 = echo)")
    ap.add_argument("--token", default="")
    args = ap.parse_args()

    if args.handler == "cython":
        import work_cy
        work_fn = lambda buf, n, passes: work_cy.fnv_work(buf, n, passes)
    else:
        work_fn = py_fnv

    handle = make_handle(work_fn, args.work)

    def root():
        port, listeners = runloom_c.serve(args.host, args.port, handle,
                                          acceptors=args.hubs, backlog=4096)
        print("LISTENING %d" % port, flush=True)
        runloom.sleep(float("inf"))

    runloom.run(args.hubs, main_fn=root)


if __name__ == "__main__":
    main()
