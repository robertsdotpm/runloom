# Repro: after a single WRITE park completes, the fd stays LEVEL-armed for
# EPOLLOUT with no waiter.  An always-writable socket then makes every
# epoll_wait return immediately -> the idle pump busy-spins at ~100% CPU
# until the socket is closed.
import os, socket, sys, time
import runloom
import runloom_c as rc

READ, WRITE = 1, 2
res = {}

def cpu_seconds():
    t = os.times()
    return t.elapsed, t.user + t.system

def main():
    a, b = socket.socketpair()
    a.setblocking(False); b.setblocking(False)
    # One WRITE park: socket is writable, wakes ~immediately, but the fd is
    # now armed EPOLLOUT level-triggered and never disarmed.
    r = rc.wait_fd(a.fileno(), WRITE, 2000)
    res["w"] = r
    e0, c0 = cpu_seconds()
    runloom.sleep(3.0)          # runtime should be idle: expect ~0 CPU
    e1, c1 = cpu_seconds()
    res["idle_wall"] = e1 - e0
    res["idle_cpu"] = c1 - c0
    res["socks"] = (a, b)

runloom.run(1, main)
print("write park result:", res["w"])
print("idle wall=%.2fs cpu=%.2fs" % (res["idle_wall"], res["idle_cpu"]))
if res["idle_cpu"] > 0.5 * res["idle_wall"]:
    print("BUG: idle pump busy-spins on the stale OUT level arm")
    sys.exit(1)
print("OK")
sys.exit(0)
