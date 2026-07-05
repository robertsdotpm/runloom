"""R1: after a TCPConn send parks on WRITE once (backpressure), the fd's
EPOLLOUT arm is never removed.  Since the netpoll is LEVEL-triggered and a
drained socket is always writable, the idle pump should spin at 100% CPU
afterwards.  Measure process CPU during a 2s idle window, with and without
having triggered a WRITE park."""
import os
import socket
import sys
import time

import runloom
import runloom_c as rc

TRIGGER_WRITE_PARK = (len(sys.argv) > 1 and sys.argv[1] == "park")


def cpu_seconds():
    with open("/proc/self/stat") as f:
        parts = f.read().rsplit(")", 1)[1].split()
    utime, stime = int(parts[11]), int(parts[12])
    return (utime + stime) / os.sysconf("SC_CLK_TCK")


def _port(lst):
    s = socket.socket(fileno=socket.dup(lst.fileno()))
    try:
        return s.getsockname()[1]
    finally:
        s.detach(); s.close()


result = {}


def main():
    lst = rc.TCPConn.listen("127.0.0.1", 0)
    port = _port(lst)
    state = {}

    def server():
        conn = lst.accept()
        state["srv"] = conn
        if TRIGGER_WRITE_PARK:
            # Sleep so the client's send_all definitely fills the buffers and
            # parks on WRITE, then drain everything.
            runloom.sleep(0.5)
            total = 0
            while total < state["n"]:
                b = conn.recv(65536)
                if not b:
                    break
                total += len(b)
        # Idle phase: hold the conn open; wait for the final byte.
        b = conn.recv(1)
        conn.send_all(b"k")
        conn.close()

    def client():
        c = rc.TCPConn.connect("127.0.0.1", port)
        c.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, (16384).to_bytes(4, sys.byteorder))
        if TRIGGER_WRITE_PARK:
            payload = b"x" * (4 * 1024 * 1024)
            state["n"] = len(payload)
            c.send_all(payload)          # must park on WRITE at least once
        # Now everything is drained; both conns idle but OPEN.
        t0 = time.monotonic(); c0 = cpu_seconds()
        runloom.sleep(2.0)               # idle window
        t1 = time.monotonic(); c1 = cpu_seconds()
        result["wall"] = t1 - t0
        result["cpu"] = c1 - c0
        c.send_all(b"z")
        c.recv(1)
        c.close(); lst.close()

    rc.fiber(server)
    rc.fiber(client)


rc.fiber(main)
rc.run()
print("mode=%s idle window: wall=%.2fs cpu=%.2fs" %
      ("park" if TRIGGER_WRITE_PARK else "no-park", result["wall"], result["cpu"]))
