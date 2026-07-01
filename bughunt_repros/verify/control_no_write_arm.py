import os, socket, sys
import runloom, runloom_c as rc
READ, WRITE = 1, 2
res = {}
def cpu():
    t = os.times(); return t.elapsed, t.user + t.system
def main():
    lst = socket.socket(); lst.bind(("127.0.0.1", 0)); lst.listen(1)
    cl = socket.socket()
    cl.connect(lst.getsockname())        # BLOCKING connect: no WRITE arm
    cl.setblocking(False)
    srv, _ = lst.accept()
    runloom.fiber(lambda: res.__setitem__("r", rc.wait_fd(cl.fileno(), READ, 6000)))
    runloom.sleep(0.2)
    e0, c0 = cpu(); runloom.sleep(3.0); e1, c1 = cpu()
    res["wall"], res["cpu"] = e1 - e0, c1 - c0
    srv.send(b"x"); res["socks"] = (lst, cl, srv)
runloom.run(1, main)
print("control wall=%.2fs cpu=%.2fs" % (res["wall"], res["cpu"]))
sys.exit(1 if res["cpu"] > 0.5 * res["wall"] else 0)
