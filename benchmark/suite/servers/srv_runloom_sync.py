"""Server tier 1 (and tier 3 with RUNLOOM_IOURING_LOOP=1): runloom default
backend, ZERO optimized -- the naive, object-heavy path.

Spec: wrapped python calls, no direct C calls, python objects.
    listener = runloom.sync.tcp_listen(...)
    while True:
        conn, _ = listener.accept()
        runloom.go(handle, conn)      # real name: runloom.fiber

The handler uses recv() (allocates a bytes per read) + sendall(bytes) on the
high-level runloom.sync.Socket facade -- deliberately the slow tier.
Tier 3 is byte-for-byte this file; the orchestrator just exports
RUNLOOM_IOURING_LOOP=1 (spec: "same code as 1 but io_uring loop").
"""
import argparse
import os

import runloom
import runloom.sync as rs


def handle(conn):
    try:
        while True:
            data = conn.recv(65536)          # python bytes alloc per read
            if not data:
                break
            conn.sendall(data)
    except OSError:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="10.99.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--hubs", type=int, default=int((os.cpu_count() or 1) * 0.7))
    ap.add_argument("--token", default="")   # for targeted pkill by the orchestrator
    args = ap.parse_args()

    def root():
        ln = rs.tcp_listen(args.host, args.port, backlog=4096)
        port = ln.getsockname()[1]
        print("LISTENING %d" % port, flush=True)
        while True:
            conn, _ = ln.accept()
            runloom.fiber(handle, conn)

    runloom.run(args.hubs, root)


if __name__ == "__main__":
    main()
