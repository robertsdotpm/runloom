"""R6: default epoll path.  TCPConn.close() only wakes fibers ALREADY parked
(runloom_netpoll_cancel_fd).  A fiber that is between "recv() -> EAGAIN" and
its parker being linked misses the cancel: close() then unregisters + closes
the fd, and the fiber parks on a dead (or reused) fd number -> parks forever.

Strategy: fiber does recv() on an empty conn; a plain OS thread (free-threaded
build, truly parallel) closes the conn as soon as the fiber signals it is
about to call recv.  Iterate; most iterations end with OSError(EBADF/ECANCELED)
or b"" -- a HIT is an iteration whose recv never returns.
"""
import os
import socket
import sys
import threading
import time

import runloom
import runloom_c as rc

N = int(sys.argv[1]) if len(sys.argv) > 1 else 4000

progress = {"i": 0, "outcomes": {}}


def _port(lst):
    s = socket.socket(fileno=socket.dup(lst.fileno()))
    try:
        return s.getsockname()[1]
    finally:
        s.detach(); s.close()


def watchdog():
    last = -1
    stall = 0
    while True:
        time.sleep(1.0)
        cur = progress["i"]
        if cur == N:
            return
        if cur == last:
            stall += 1
            if stall >= 8:
                print("HIT: iteration %d wedged; outcomes so far: %s"
                      % (cur, progress["outcomes"]), flush=True)
                os._exit(2)
        else:
            stall = 0
            last = cur


def main():
    lst = rc.TCPConn.listen("127.0.0.1", 0)
    port = _port(lst)
    srv_conns = []

    def acceptor():
        while True:
            try:
                srv_conns.append(lst.accept())
            except BaseException:
                return

    rc.fiber(acceptor)

    for i in range(N):
        c = rc.TCPConn.connect("127.0.0.1", port)
        flag = threading.Event()

        def closer(c=c, flag=flag):
            flag.wait()
            # tiny random-ish spin to spray the close across the window
            for _ in range((i * 7) % 400):
                pass
            c.close()

        t = threading.Thread(target=closer)
        t.start()
        flag.set()
        try:
            r = c.recv(16)
            key = "ret:%r" % (r,)
        except OSError as e:
            key = "err:%d" % (e.errno if e.errno is not None else -1)
        progress["outcomes"][key] = progress["outcomes"].get(key, 0) + 1
        t.join()
        c.close()
        # occasionally drain server-side conns so fds recycle
        if len(srv_conns) > 64:
            for sc in srv_conns:
                sc.close()
            del srv_conns[:]
        progress["i"] = i + 1

    lst.close()
    print("done: %s" % (progress["outcomes"],), flush=True)


wd = threading.Thread(target=watchdog, daemon=True)
wd.start()
rc.fiber(main)
rc.run()
