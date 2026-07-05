"""R3: does a Python signal handler exception (SIGALRM -> raise) interrupt a
fiber blocked in TCPConn.recv()?  Compare default epoll path vs
RUNLOOM_TCPCONN_IOURING=1.  On epoll the netpoll signal-wake path restores the
exception; suspect the iouring park has no such path -> recv never returns
until socket activity, so the alarm exception is delayed/lost.
"""
import signal
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


class Alarm(Exception):
    pass


def on_alarm(sig, frm):
    raise Alarm("alarm")


signal.signal(signal.SIGALRM, on_alarm)

out = {}


def main():
    lst = rc.TCPConn.listen("127.0.0.1", 0)
    port = _port(lst)

    def server():
        conn = lst.accept()
        # never send anything; close after 8s so the test always terminates
        runloom.sleep(8.0)
        conn.close()
        lst.close()

    def client():
        c = rc.TCPConn.connect("127.0.0.1", port)
        signal.alarm(1)
        t0 = time.monotonic()
        try:
            data = c.recv(64)
            out["result"] = ("recv-returned", data)
        except Alarm:
            out["result"] = ("alarm-exception",)
        except BaseException as e:
            out["result"] = ("exc", type(e).__name__, str(e))
        out["dt"] = time.monotonic() - t0
        signal.alarm(0)
        c.close()

    rc.fiber(server)
    rc.fiber(client)


rc.fiber(main)
rc.run()
print("result=%r dt=%.2fs" % (out.get("result"), out.get("dt", -1)))
