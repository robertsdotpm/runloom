"""Memory probe for the runloom columns: hold N parked fibers in a given state
and report USED memory (RSS / PSS -- NOT virtual size).

Configs (via flags):
  --handler py            fiber parked in a python handler holding a 64 KiB
                          bytearray (matches runloom_epoll_py_tcpcon) -> heap-faulted
  --handler c             fiber parked in the Cython handler (stack buffer,
                          un-faulted until first recv) -> the cheap path
  --optimize memory       call runloom.optimize("memory") first

States (--state):
  empty   : N fibers parked on a shared Chan recv (bare fiber: g + stack)
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

    worker = worker_empty if args.state == "empty" else worker_socket

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
