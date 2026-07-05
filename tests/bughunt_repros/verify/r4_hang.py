# run: RUNLOOM_TCPCONN_IOURING=1 timeout 25 .venv/bin/python r4_close_during_recv.py flags
import socket, sys, time
import runloom, runloom_c as rc
SCEN = sys.argv[1] if len(sys.argv) > 1 else "plain"
def _port(lst):
    s = socket.socket(fileno=socket.dup(lst.fileno()))
    try: return s.getsockname()[1]
    finally: s.detach(); s.close()
out = {}
def main():
    lst = rc.TCPConn.listen("127.0.0.1", 0); port = _port(lst); holder = {}
    def server():
        conn = lst.accept(); holder["srv"] = conn
        runloom.sleep(60.0)   # raise to 60 to show the permanent hang
        conn.close(); lst.close()
    def receiver():
        c = rc.TCPConn.connect("127.0.0.1", port); holder["cli"] = c
        t0 = time.monotonic()
        try:
            r = c.recv(64, socket.MSG_PEEK) if SCEN == "flags" else c.recv(64)
            out["result"] = ("returned", r)
        except BaseException as e:
            out["result"] = ("exc", type(e).__name__, str(e))
        out["dt"] = time.monotonic() - t0
    def closer():
        runloom.sleep(0.5); holder["cli"].close()
    rc.fiber(server); rc.fiber(receiver); rc.fiber(closer)
rc.fiber(main); rc.run()
print("scen=%s result=%r dt=%.2fs" % (SCEN, out.get("result"), out.get("dt", -1)))
