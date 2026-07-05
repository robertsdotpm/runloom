"""End-to-end variant: the REAL aio bridge path triggers the lost wakeup.

Thread B runs a runloom asyncio event loop and does loop.sock_sendall(a, ...)
on a socket -- an OUTBOUND write on the same full-duplex socket an M:N hub
fiber (main thread, runloom.run(2, ...)) is parked on waiting READ.  When the
sendall completes, the bridge's _release_fd_after wrapper calls
runloom_c.netpoll_release_if_idle(a.fileno()); the guard only checks the
DEFAULT pool, misses the hub parker, and EPOLL_CTL_DELs the fd + clears its
arm cache.  The peer's reply then produces no epoll event -> the hub fiber's
wait_fd times out instead of waking.
"""
import socket
import sys
import threading
import time

import runloom
import runloom_c as rc
import runloom.aio as raio

READ = 1

a, b = socket.socketpair()   # full-duplex; `a` shared: hub reads, bridge writes
a.setblocking(False)
fd = a.fileno()

result = {}


def bridge_thread():
    import asyncio
    loop = raio.RunloomEventLoopPolicy().new_event_loop()

    async def do_send():
        await asyncio.sleep(0)          # loop warm
        time.sleep(0.5)                 # let the hub fiber park on fd READ
        await loop.sock_sendall(a, b"out")   # write side of the SAME socket
        # _release_fd_after now ran: netpoll_release_if_idle(fd)

    loop.run_until_complete(do_send())
    loop.close()
    time.sleep(0.1)
    b.send(b"reply")                    # fd readable -> hub fiber should wake


def main():
    def waiter():
        t = threading.Thread(target=bridge_thread, daemon=True)
        t.start()
        t0 = time.monotonic()
        rv = rc.wait_fd(fd, READ, 3000)
        result["rv"] = rv
        result["el"] = time.monotonic() - t0
    runloom.fiber(waiter)
    while "rv" not in result:
        runloom.sleep(0.01)


runloom.run(2, main)

rv, el = result["rv"], result["el"]
print("wait_fd_rv=%d elapsed=%.3fs (data was readable at ~0.6s)" % (rv, el))
if rv & READ and el < 1.5:
    print("WOKE PROMPTLY (bridge path did not trigger the bug)")
    sys.exit(0)
elif rv == 0:
    print("LOST WAKEUP via the real aio-bridge sock_sendall path -> BUG")
    sys.exit(2)
else:
    sys.exit(3)
