# Variant: a second fiber is parked on READ of an unrelated quiet socket, so
# the scheduler's idle loop actually drives the netpoll pump.  The stale OUT
# level arm on the first socket then makes every epoll_wait return instantly.
import os, socket, sys, time
import runloom
import runloom_c as rc

READ, WRITE = 1, 2
res = {}

def cpu_seconds():
    t = os.times()
    return t.elapsed, t.user + t.system

def main():
    a, b = socket.socketpair()     # will carry the stale OUT arm
    c, d = socket.socketpair()     # quiet fd a reader parks on
    for s in (a, b, c, d):
        s.setblocking(False)
    def reader():
        res["r"] = rc.wait_fd(c.fileno(), READ, 6000)   # parks whole test
    runloom.fiber(reader)
    runloom.sleep(0.1)
    res["w"] = rc.wait_fd(a.fileno(), WRITE, 2000)      # arms OUT, wakes fast
    e0, c0 = cpu_seconds()
    runloom.sleep(3.0)
    e1, c1 = cpu_seconds()
    res["idle_wall"] = e1 - e0
    res["idle_cpu"] = c1 - c0
    d.send(b"x")                                        # release the reader
    res["socks"] = (a, b, c, d)

runloom.run(1, main)
print("write park result:", res["w"], " reader:", res.get("r"))
print("idle wall=%.2fs cpu=%.2fs" % (res["idle_wall"], res["idle_cpu"]))
if res["idle_cpu"] > 0.5 * res["idle_wall"]:
    print("BUG: idle pump busy-spins on the stale OUT level arm")
    sys.exit(1)
print("OK")
sys.exit(0)
