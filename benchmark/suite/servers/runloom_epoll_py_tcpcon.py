"""Server tier 2: runloom_c.serve C scaffold (listen/accept/spawn in C) with a
regular PYTHON handler that uses the C-level TCPConn methods recv_into/send_all.

Spec: uses C calls so faster than the sync wrappers, but the handler is still a
plain python function (no Cython). The C scaffold runs N SO_REUSEPORT acceptors.
"""
import argparse
import os

import runloom
import runloom_c

CHUNK = 65536


def handle(conn):
    # conn is a runloom_c.TCPConn. recv_into/send_all are single C calls (no
    # bytes alloc), but this is still a python frame dispatched per round trip.
    buf = bytearray(CHUNK)
    mv = memoryview(buf)
    try:
        while True:
            n = conn.recv_into(buf)
            if not n:
                break
            conn.send_all(mv[:n])
    except OSError:
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="10.99.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--hubs", type=int, default=int((os.cpu_count() or 1) * 0.7))
    ap.add_argument("--token", default="")
    args = ap.parse_args()

    def root():
        port, listeners = runloom_c.serve(
            args.host, args.port, handle,
            acceptors=args.hubs, backlog=4096)
        print("LISTENING %d" % port, flush=True)
        runloom.sleep(float("inf"))

    runloom.run(args.hubs, main_fn=root)


if __name__ == "__main__":
    main()
