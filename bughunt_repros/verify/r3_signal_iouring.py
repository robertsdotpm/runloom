# run: RUNLOOM_TCPCONN_IOURING=1 timeout 30 .venv/bin/python r3_signal_iouring.py
import signal, socket, time
import runloom, runloom_c as rc
def _port(lst):
    s = socket.socket(fileno=socket.dup(lst.fileno()))
    try: return s.getsockname()[1]
    finally: s.detach(); s.close()
class Alarm(Exception): pass
signal.signal(signal.SIGALRM, lambda s, f: (_ for _ in ()).throw(Alarm()))
out = {}
def main():
    lst = rc.TCPConn.listen("127.0.0.1", 0); port = _port(lst)
    def server():
        conn = lst.accept(); runloom.sleep(8.0); conn.close(); lst.close()
    def client():
        c = rc.TCPConn.connect("127.0.0.1", port)
        signal.alarm(1); t0 = time.monotonic()
        try:
            out["result"] = ("recv-returned", c.recv(64))
        except Alarm:
            out["result"] = ("alarm-exception",)
        except BaseException as e:
            out["result"] = ("exc", type(e).__name__, str(e))
        out["dt"] = time.monotonic() - t0; signal.alarm(0); c.close()
    rc.fiber(server); rc.fiber(client)
rc.fiber(main); rc.run()
print("result=%r dt=%.2fs" % (out.get("result"), out.get("dt", -1)))
