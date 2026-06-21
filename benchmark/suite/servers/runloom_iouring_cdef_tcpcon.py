"""Std name: runloom_iouring_cdef_tcpcon  (this file ALSO backs
runloom_epoll_cdef_tcpcon -- same handler, no io_uring env, so it runs on the
epoll readiness backend; the orchestrator selects the backend per spec).

Server tier: runloom_c.serve + a tstate-free Cython CDEF handler (the c_entry
fast path); io_uring proactor when the orchestrator sets RUNLOOM_IOURING_LOOP=1,
else the epoll readiness backend.

The difference from runloom_iouring_cython_tcpcon.py: handler_cdef.handler is a
runloom_c.c_handler PyCapsule (a cdef C function), so serve() spawns it via
runloom_mn_fiber_c -> the g->c_entry path -> NO Python frame, NO per-park tstate
save/restore. Zero PyObjects in the hot loop AND zero tstate juggle per round
trip -- the all-C echo's advantage with a custom handler.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # find handler_cdef*.so

import runloom
import runloom_c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="10.99.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--hubs", type=int, default=int((os.cpu_count() or 1) * 0.7))
    ap.add_argument("--optimize", default="none", choices=["none", "throughput"])
    ap.add_argument("--token", default="")
    args = ap.parse_args()

    if args.optimize == "throughput":
        print("OPTIMIZE %s" % runloom.optimize("throughput"), flush=True)

    import handler_cdef  # builds the capsule at import

    def root():
        port, listeners = runloom_c.serve(
            args.host, args.port, handler_cdef.handler,
            acceptors=args.hubs, backlog=4096)
        print("LISTENING %d" % port, flush=True)
        runloom.sleep(float("inf"))

    runloom.run(args.hubs, main_fn=root)


if __name__ == "__main__":
    main()
