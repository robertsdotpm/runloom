"""R1b: same as r1 but under the M:N scheduler (runloom.run), where idle hubs
should block in epoll_wait.  Compare idle-window CPU with vs without a prior
WRITE park."""
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
    from runloom.sync import WaitGroup
    wg = WaitGroup(); wg.add(2)

    def server():
        try:
            conn = lst.accept()
            if TRIGGER_WRITE_PARK:
                runloom.sleep(0.5)
                total = 0
                while total < state["n"]:
                    b = conn.recv(65536)
                    if not b:
                        break
                    total += len(b)
            b = conn.recv(1)
            conn.send_all(b"k")
            conn.close()
        finally:
            wg.done()

    def client():
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF,
                         (16384).to_bytes(4, sys.byteorder))
            if TRIGGER_WRITE_PARK:
                payload = b"x" * (4 * 1024 * 1024)
                state["n"] = len(payload)
                c.send_all(payload)
            t0 = time.monotonic(); c0 = cpu_seconds()
            runloom.sleep(2.0)
            t1 = time.monotonic(); c1 = cpu_seconds()
            result["wall"] = t1 - t0
            result["cpu"] = c1 - c0
            c.send_all(b"z")
            c.recv(1)
            c.close(); lst.close()
        finally:
            wg.done()

    rc.mn_fiber(server)
    rc.mn_fiber(client)
    wg.wait()


runloom.run(2, main)
print("mode=%s idle window: wall=%.2fs cpu=%.2fs" %
      ("park" if TRIGGER_WRITE_PARK else "no-park", result["wall"], result["cpu"]))
