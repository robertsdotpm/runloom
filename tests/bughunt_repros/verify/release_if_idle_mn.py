"""Verify: netpoll_release_if_idle only checks the DEFAULT pool for parkers.

An M:N hub fiber parks on fd (parker lives in pool[hub], not runloom_pool).
Another OS thread then calls runloom_c.netpoll_release_if_idle(fd) -- exactly
what the aio bridge's _release_fd_after does after every loop.sock_* op on a
user-owned socket.  The guard `runloom_pool.by_fd[fd] == NULL` does not see
the hub parker, so it EPOLL_CTL_DELs the still-waited fd and zeroes the arm
cache.  Data sent afterwards produces no epoll event -> the fiber's wait_fd
never wakes and only its timeout fires.

Control run (RELEASE=0) proves the same setup wakes promptly without the
release call.
"""
import os
import socket
import sys
import threading
import time

import runloom
import runloom_c as rc

READ = 1
DO_RELEASE = os.environ.get("RELEASE", "1") == "1"

a, b = socket.socketpair()
a.setblocking(False)
fd = a.fileno()

result = {}


def interferer():
    time.sleep(0.5)          # let the hub fiber park on fd
    if DO_RELEASE:
        rc.netpoll_release_if_idle(fd)   # what the aio bridge does per sock_* op
    time.sleep(0.1)
    b.send(b"x")             # fd is now readable


def main():
    def waiter():
        t = threading.Thread(target=interferer, daemon=True)
        t.start()
        t0 = time.monotonic()
        rv = rc.wait_fd(fd, READ, 3000)   # 3s timeout
        result["rv"] = rv
        result["el"] = time.monotonic() - t0
    runloom.fiber(waiter)
    while "rv" not in result:
        runloom.sleep(0.01)


runloom.run(2, main)   # M:N: 2 hubs -> parker lands in a HUB pool, not default

rv = result["rv"]
el = result["el"]
print("release_called=%s wait_fd_rv=%d elapsed=%.3fs" % (DO_RELEASE, rv, el))
if rv & READ and el < 1.0:
    print("WOKE PROMPTLY (no bug in this configuration)")
    sys.exit(0)
elif rv == 0:
    print("LOST WAKEUP: wait_fd timed out despite data being readable -> BUG")
    sys.exit(2)
else:
    print("unexpected: rv=%d el=%.3f" % (rv, el))
    sys.exit(3)
