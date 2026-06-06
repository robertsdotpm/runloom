"""big_100 / 62 -- cancelled socket connect.

A local listener with a tiny backlog is started but NEVER accepts.  Once its
accept queue fills, further connects to it hang in SYN_SENT.  Goroutines fire
those connects with a timeout and abandon (cancel) the ones that hang, closing
the fd.  No goroutine may stay stuck and no fd may leak.

Stresses: connect cancellation, fd cleanup on the abandoned path.  Fully local
(loopback) -- no packets leave the machine.
"""
import socket

import harness
import netutil
import runloom
import runloom_c


def connect_timeout(addr, timeout_ms):
    """Return 'connected' | 'cancelled' | 'refused'; always closes the fd."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setblocking(False)
    try:
        err = s.connect_ex(addr)        # EINPROGRESS on a nonblocking socket
        if err in (0,):
            return "connected"
        ready = runloom_c.wait_fd(s.fileno(), 2, timeout_ms)
        if not (ready & 2):
            return "cancelled"          # timed out / cancelled mid-connect
        soerr = s.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        return "connected" if soerr == 0 else "refused"
    finally:
        netutil.close_quiet(s)


def setup(H):
    # Backlog 1, never accept -> the queue fills and later connects hang.
    srv = netutil.listen_tcp(backlog=1)
    H.state = {"addr": (srv.getsockname()[0], srv.getsockname()[1]), "srv": srv,
               "cancelled": [0] * 1024, "connected": [0] * 1024}
    H.add_cleanup(lambda: netutil.close_quiet(srv))
    H.fd_ceiling = 0


def worker(H, wid, rng, state):
    addr = state["addr"]
    while H.running():
        outcome = connect_timeout(addr, int(rng.uniform(5, 40)))
        if outcome == "cancelled":
            state["cancelled"][wid & 1023] += 1
        elif outcome == "connected":
            state["connected"][wid & 1023] += 1
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)

    def auditor():
        base = harness.count_fds()
        while H.running():
            fds = harness.count_fds()
            H.fd_ceiling = max(H.fd_ceiling, fds)
            H.check(fds < base + H.funcs + 5000,
                    "fd leak on cancelled connects: {0} (base {1})".format(
                        fds, base))
            H.sleep(1.0)
        H.log("cancelled={0} connected={1} fd_ceiling={2}".format(
            sum(H.state["cancelled"]), sum(H.state["connected"]),
            H.fd_ceiling))

    H.go(auditor)


if __name__ == "__main__":
    harness.main("p62_cancelled_connect", body, setup=setup, default_funcs=3000,
                 describe="cancel hanging connects via timeout; no stuck fds")
