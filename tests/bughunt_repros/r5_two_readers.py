"""R5: two fibers blocked in recv() on the SAME TCPConn.  The io_uring
multishot handle keeps a SINGLE waiter_g; the second parked reader overwrites
the first, so the first is never woken again (lost wakeup -> permanent hang),
even as more data keeps arriving.  On the epoll path both readers eventually
get data.
"""
import socket
import sys
import time

import runloom
import runloom_c as rc


def _port(lst):
    s = socket.socket(fileno=socket.dup(lst.fileno()))
    try:
        return s.getsockname()[1]
    finally:
        s.detach(); s.close()


out = {"r1": None, "r2": None}


def main():
    lst = rc.TCPConn.listen("127.0.0.1", 0)
    port = _port(lst)
    holder = {}

    def server():
        conn = lst.accept()
        # Two messages, spaced out, so each parked reader should get one.
        runloom.sleep(0.5)
        conn.send_all(b"AAAA")
        runloom.sleep(0.5)
        conn.send_all(b"BBBB")
        runloom.sleep(4.0)
        conn.close()
        lst.close()

    def reader(key):
        c = holder["cli"]
        t0 = time.monotonic()
        try:
            out[key] = ("data", c.recv(4), round(time.monotonic() - t0, 2))
        except BaseException as e:
            out[key] = ("exc", type(e).__name__, str(e))

    def starter():
        holder["cli"] = rc.TCPConn.connect("127.0.0.1", port)
        rc.fiber(lambda: reader("r1"))
        rc.fiber(lambda: reader("r2"))

    rc.fiber(server)
    rc.fiber(starter)


rc.fiber(main)
rc.run()
print("r1=%r r2=%r" % (out["r1"], out["r2"]))
