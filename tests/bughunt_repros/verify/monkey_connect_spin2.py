import os, socket, sys, threading
import runloom, runloom.monkey
runloom.monkey.patch()
res = {}
def cpu():
    t = os.times(); return t.elapsed, t.user + t.system
# plain-OS-thread server so the fiber side is purely the client under test
lst = socket.socket(); lst.bind(("127.0.0.1", 0)); lst.listen(1)
addr = lst.getsockname()
def server():
    srv, _ = lst.accept()
    threading.Event().wait(4.0)   # stay silent while client idles in recv
    srv.send(b"x")
    srv.close(); lst.close()
th = threading.Thread(target=server, daemon=True); th.start()
def client():
    s = socket.socket()
    s.connect(addr)             # monkey: EINPROGRESS -> wait_fd(WRITE)
    res["data"] = s.recv(16)    # parks in READ; server silent for ~4s
    s.close()
def main():
    runloom.fiber(client)
    runloom.sleep(0.4)
    e0, c0 = cpu(); runloom.sleep(3.0); e1, c1 = cpu()
    res["wall"], res["cpu"] = e1 - e0, c1 - c0
runloom.run(1, main)
print("monkey idle wall=%.2fs cpu=%.2fs data=%r" % (res["wall"], res["cpu"], res.get("data")))
sys.exit(1 if res["cpu"] > 0.5 * res["wall"] else 0)
