# run: RUNLOOM_TCPCONN_IOURING=1 timeout 20 .venv/bin/python r2_ms_peek.py
import socket
import runloom, runloom_c as rc
def _port(lst):
    s = socket.socket(fileno=socket.dup(lst.fileno()))
    try: return s.getsockname()[1]
    finally: s.detach(); s.close()
out = {}
def main():
    lst = rc.TCPConn.listen("127.0.0.1", 0); port = _port(lst)
    def server():
        conn = lst.accept()
        conn.send_all(b"first!")
        runloom.sleep(0.3); conn.send_all(b"second")
        runloom.sleep(3.0); conn.close()
    def client():
        c = rc.TCPConn.connect("127.0.0.1", port)
        out["first"] = c.recv(6)
        runloom.sleep(0.8)
        out["peek"] = c.recv(6, socket.MSG_PEEK)
        out["second"] = c.recv(6)
        c.close(); lst.close()
    rc.fiber(server); rc.fiber(client)
rc.fiber(main); rc.run()
print("first=%r peek=%r second=%r" % (out.get("first"), out.get("peek"), out.get("second")))
