"""R4: cross-fiber conn.close() while another fiber is parked in recv().
Documented intent (runloom_tcp_conn_io.c.inc close/dealloc comments): wake the
parked fiber so it sees the close and exits, instead of hanging forever.

Scenarios:
  plain   : recv(64)            parked, then close
  flags   : recv(64, MSG_PEEK)  parked, then close  (forces single-shot under iouring)

Run under default (epoll) and RUNLOOM_TCPCONN_IOURING=1.
"""
import socket
import sys
import time

import runloom
import runloom_c as rc

SCEN = sys.argv[1] if len(sys.argv) > 1 else "plain"


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
    holder = {}

    def server():
        conn = lst.accept()
        holder["srv"] = conn
        runloom.sleep(60.0)      # keep peer open; never send
        conn.close()
        lst.close()

    def receiver():
        c = rc.TCPConn.connect("127.0.0.1", port)
        holder["cli"] = c
        t0 = time.monotonic()
        try:
            if SCEN == "flags":
                r = c.recv(64, socket.MSG_PEEK)
            else:
                r = c.recv(64)
            out["result"] = ("returned", r)
        except BaseException as e:
            out["result"] = ("exc", type(e).__name__, str(e))
        out["dt"] = time.monotonic() - t0

    def closer():
        runloom.sleep(0.5)          # let receiver park
        holder["cli"].close()

    rc.fiber(server)
    rc.fiber(receiver)
    rc.fiber(closer)


rc.fiber(main)
rc.run()
print("scen=%s result=%r dt=%.2fs" % (SCEN, out.get("result"), out.get("dt", -1)))
