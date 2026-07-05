"""R2: RUNLOOM_TCPCONN_IOURING=1: once a plain recv() has armed the multishot
recv on a conn, a later recv(n, MSG_PEEK) (flags != 0) goes down the
single-shot IORING_OP_RECV path while the multishot stays armed and consumes
incoming data into its private buffer queue -> the PEEK never sees data and
the fiber hangs (or data is reordered).

Run with: RUNLOOM_TCPCONN_IOURING=1 python r2_ms_peek.py
Expected (stdlib semantics): peek returns b"second" quickly.
"""
import os
import socket
import sys

import runloom
import runloom_c as rc


def _port(lst):
    s = socket.socket(fileno=socket.dup(lst.fileno()))
    try:
        return s.getsockname()[1]
    finally:
        s.detach(); s.close()


out = {}


def main():
    lst = rc.TCPConn.listen("127.0.0.1", 0)
    port = _port(lst)

    def server():
        conn = lst.accept()
        conn.send_all(b"first!")
        runloom.sleep(0.3)
        conn.send_all(b"second")
        runloom.sleep(3.0)          # keep conn open while client peeks
        conn.close()

    def client():
        c = rc.TCPConn.connect("127.0.0.1", port)
        out["first"] = c.recv(6)          # arms the multishot under IOURING=1
        runloom.sleep(0.8)                # let "second" arrive (and be eaten by ms)
        out["peek"] = c.recv(6, socket.MSG_PEEK)
        out["second"] = c.recv(6)
        c.close(); lst.close()

    rc.fiber(server)
    rc.fiber(client)


rc.fiber(main)
rc.run()
print("first=%r peek=%r second=%r" %
      (out.get("first"), out.get("peek"), out.get("second")))
