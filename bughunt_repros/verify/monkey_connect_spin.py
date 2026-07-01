import os, socket, sys
import runloom
res = {}
def cpu():
    t = os.times(); return t.elapsed, t.user + t.system
def client(addr):
    s = socket.socket()
    s.connect(addr)           # monkey-patched: EINPROGRESS -> wait_fd(WRITE)
    res["data"] = s.recv(16)  # parks in READ while server is silent
    s.close()
def main():
    lst = socket.socket(); lst.bind(("127.0.0.1", 0)); lst.listen(1)
    runloom.fiber(client, lst.getsockname())
    srv, _ = lst.accept()
    runloom.sleep(0.2)
    e0, c0 = cpu(); runloom.sleep(3.0); e1, c1 = cpu()
    res["wall"], res["cpu"] = e1 - e0, c1 - c0
    srv.send(b"x"); runloom.sleep(0.1)
    srv.close(); lst.close()
runloom.run(1, main)
print("monkey idle wall=%.2fs cpu=%.2fs data=%r" % (res["wall"], res["cpu"], res.get("data")))
sys.exit(1 if res["cpu"] > 0.5 * res["wall"] else 0)
