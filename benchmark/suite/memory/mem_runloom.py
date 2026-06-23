"""Memory probe for the runloom columns: hold N parked fibers in a given state
and report USED memory (RSS / PSS -- NOT virtual size).

Configs (via flags):
  --handler py            interpreted handler: the fiber's entry runs in the
                          CPython evaluator.  For 'socket' it holds a 64 KiB
                          bytearray (matches runloom_epoll_py_tcpcon) -> heap-faulted.
  --handler c             COMPILED handler: the fiber's entry is native (Cython)
                          code, so its frozen C stack carries NO
                          _PyEval_EvalFrameDefault interpreter activation.  For
                          'socket' it is the stack-buffer Cython handler; for
                          'empty' the bare parker itself is compiled (see below).
  --optimize memory       call runloom.optimize("memory") first

States (--state):
  empty   : N fibers parked on a shared Chan recv (bare fiber: g + stack).
            With --handler c the parker is COMPILED to native code, so a parked
            fiber's resident C stack is ~1 page (no eval-loop activation) vs ~2
            pages interpreted -- the real, ~2x-cheaper cost of a compiled fiber.
            (Before this, the 'c' column silently re-ran the Python parker and
            read identical to --handler py.)
  socket  : N fibers each holding a socketpair end, parked on recv (+ the
            handler's per-conn buffer)

The measurer fiber waits for everyone to settle, snapshots /proc/self RSS+PSS,
prints JSON, and hard-exits (the worker fibers are parked forever).
"""
import argparse
import json
import os
import socket as sk

import runloom
import runloom_c


def rss_bytes():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    return None


def pss_bytes():
    try:
        with open("/proc/self/smaps_rollup") as f:
            for line in f:
                if line.startswith("Pss:"):
                    return int(line.split()[1]) * 1024
    except Exception:
        return None


def compile_empty_parker(shared):
    """Compile the bare 'empty' parker to native (Cython) code.

    The point of the --handler c / empty column: a parked fiber whose frozen C
    stack carries the compiled cyfunction frame instead of a
    _PyEval_EvalFrameDefault interpreter activation.  That single eval frame
    (~448 B on 3.13t) is what tips the live park chain across a second 4 KiB
    page, so dropping it nearly halves a parked fiber's resident stack (~2 pages
    -> ~1 page).  Reuses benchmark/bench/cycompile (the on-the-fly handler
    compiler), so this measures the SAME source as the py parker, just compiled.

    Spawned with no args, so runloom.fiber uses the callable directly (no
    arg-binding lambda wrapper) -- the only frame between runloom_g_entry and
    Chan.recv is the native parker, the cleanest apples-to-apples vs --handler py.
    Raises (never silently falls back to the Python parker) if compilation is
    unavailable -- a silent fallback is the exact bug this column had.
    """
    import sys
    here = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, os.path.join(here, "..", "..", "bench"))   # cycompile
    import cycompile

    def parker():
        shared.recv()                       # park forever; compiled to native C

    # The preamble declares `shared` as a module global so Cython compiles the
    # bare name as a global load (not an undeclared-name error); bind the real
    # channel into the compiled module right after import.
    (cy,) = cycompile.compile_funcs([parker], preamble="shared = None")
    cy.__globals__["shared"] = shared
    return cy


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", required=True, choices=["empty", "socket"])
    ap.add_argument("--n", type=int, required=True)
    ap.add_argument("--handler", default="py", choices=["py", "c"])
    ap.add_argument("--optimize", default="none", choices=["none", "memory"])
    ap.add_argument("--hubs", type=int, default=int((os.cpu_count() or 1) * 0.7))
    ap.add_argument("--settle", type=float, default=0.0)
    args = ap.parse_args()

    if args.optimize == "memory":
        runloom.optimize("memory")
    handler_fn = None
    if args.handler == "c" and args.state == "socket":
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                        "..", "servers"))
        import handler_cy
        handler_fn = handler_cy.handler

    n = args.n
    settle = args.settle or max(3.0, n / 150000.0)
    shared = runloom_c.Chan()
    peers = []   # keep socketpair peer ends alive so recv never returns

    def worker_empty():
        shared.recv()                       # park forever (no sender)

    def worker_socket():
        a, b = sk.socketpair()
        peers.append(b)                     # keep peer open -> recv blocks
        afd = a.detach()
        conn = runloom_c.TCPConn(afd)
        if args.handler == "py":
            buf = bytearray(65536)          # the py-handler's per-conn heap buffer
            conn.recv_into(buf)             # parks forever
        else:
            handler_fn(conn)                # Cython handler: stack buffer, parks

    if args.state == "socket":
        worker = worker_socket
    elif args.handler == "c":
        worker = compile_empty_parker(shared)   # COMPILED bare fiber (~1 page)
    else:
        worker = worker_empty                   # interpreted bare fiber (~2 pages)

    def measurer():
        runloom.sleep(settle)
        out = {"runtime": "runloom", "handler": args.handler,
               "optimize": args.optimize, "state": args.state, "n": n,
               "hubs": args.hubs, "rss_bytes": rss_bytes(), "pss_bytes": pss_bytes()}
        print(json.dumps(out), flush=True)
        os._exit(0)

    def root():
        for _ in range(n):
            runloom.fiber(worker)
        runloom.fiber(measurer)

    runloom.run(args.hubs, root)


if __name__ == "__main__":
    main()
