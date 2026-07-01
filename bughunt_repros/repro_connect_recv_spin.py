# The common client pattern: cooperative connect() parks on WRITE (EINPROGRESS)
# which arms EPOLLOUT level-triggered and never disarms it.  The following
# recv() parks on READ of the SAME fd; the still-armed, always-ready OUT makes
# every epoll_wait return instantly -> 100% CPU while the client just waits
# for data.
import os, socket, sys
import runloom
import runloom_c as rc

READ, WRITE = 1, 2
res = {}

def cpu_seconds():
    t = os.times()
    return t.elapsed, t.user + t.system

def main():
    lst = socket.socket()
    lst.bind(("127.0.0.1", 0))
    lst.listen(1)
    cl = socket.socket()
    cl.setblocking(False)
    try:
        cl.connect(lst.getsockname())
    except BlockingIOError:
        pass
    res["conn_w"] = rc.wait_fd(cl.fileno(), WRITE, 2000)   # cooperative connect
    srv, _ = lst.accept()
    def reader():
        res["r"] = rc.wait_fd(cl.fileno(), READ, 6000)     # recv park
    runloom.fiber(reader)
    runloom.sleep(0.2)
    e0, c0 = cpu_seconds()
    runloom.sleep(3.0)                                     # client just waiting
    e1, c1 = cpu_seconds()
    res["idle_wall"], res["idle_cpu"] = e1 - e0, c1 - c0
    srv.send(b"x")
    res["socks"] = (lst, cl, srv)

runloom.run(1, main)
print("connect wait:", res["conn_w"], " reader:", res.get("r"))
print("idle wall=%.2fs cpu=%.2fs" % (res["idle_wall"], res["idle_cpu"]))
sys.exit(1 if res["idle_cpu"] > 0.5 * res["idle_wall"] else 0)
