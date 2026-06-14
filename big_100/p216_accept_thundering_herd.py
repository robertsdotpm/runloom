"""big_100 / 216 -- accept thundering herd.

A small number of listener fds (SO_REUSEPORT), each with MANY accept goroutines
parked on it at once -- the one-shot-netpoll thundering herd.  Clients connect;
every connection must be accepted EXACTLY once: no connection lost, none double-
accepted.  Per-acceptor accepted-count slots and per-client connected-count
slots give an exact conservation check in post().

Stresses: many goroutines parked on ONE listen fd's EPOLLONESHOT arm (only one
wakes per readiness, must re-arm, the rest stay parked), accept fairness, no
lost/duplicate accept.  Fully local (loopback).
"""
import socket

import harness
import netutil
import runloom
import runloom_c

NLISTENERS = 4          # a few listener fds (SO_REUSEPORT)
ACCEPTORS_PER = 64      # many accept goroutines parked per listener fd


def make_listener(host):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except (AttributeError, OSError):
        pass
    return s


def acceptor(H, lsock, slot, accepted):
    """Park in accept on the SHARED listen fd; count each accepted connection in
    our own slot.  Drains the backlog past the deadline so every queued connect
    is accounted before we exit (conservation)."""
    n = 0
    empty_after_stop = 0
    while True:
        # while running, park up to 200ms; after the deadline keep draining as
        # long as the queue is non-empty, then exit only after the queue has been
        # empty for two consecutive polls (so a connect landing right at the
        # deadline is still drained -> exact conservation).
        try:
            fd = lsock.fileno()
        except (OSError, ValueError):
            break
        if fd < 0:
            break
        ready = runloom_c.wait_fd(fd, 1, 200)
        if not (ready & 1):
            if not H.running():
                empty_after_stop += 1
                if empty_after_stop >= 2:
                    break       # past deadline and queue drained -> exit
            continue
        empty_after_stop = 0
        try:
            cfd, _addr = lsock._accept()
        except (BlockingIOError, InterruptedError):
            continue
        except OSError:
            if not H.running():
                break
            continue
        try:
            conn = socket.socket(lsock.family, lsock.type, lsock.proto, fileno=cfd)
            conn.close()        # accepted: immediately drop it
        except OSError:
            pass
        n += 1
    accepted[slot] = n


def client(H, wid, rng, state):
    listeners_addrs = state["addrs"]
    connected = state["connected"]
    c = 0
    for _ in H.round_range():
        addr = listeners_addrs[rng.randrange(len(listeners_addrs))]
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect(addr)     # once this returns the conn is in the accept queue
            c += 1
            H.op(wid)
        except OSError:
            pass
        finally:
            netutil.close_quiet(s)
        H.task_done(wid)
    connected[wid] += c


def setup(H):
    host = H.net_ip(0)
    listeners = []
    addrs = []
    for _ in range(NLISTENERS):
        s = make_listener(host)
        s.bind((host, 0))
        s.listen(4096)
        s.setblocking(False)
        # NOTE: deliberately NOT register_close'd -- mark_done() closes
        # registered listeners at the deadline, which would close the listen fd
        # out from under acceptors still draining the backlog and break the
        # accept==connect conservation.  Acceptors self-terminate (two empty
        # polls after the deadline); we close the listeners in cleanup.
        H.add_cleanup(lambda sk=s: netutil.close_quiet(sk))
        listeners.append(s)
        addrs.append(s.getsockname())
    # per-acceptor accepted slots, per-client connected slots.
    naccept = NLISTENERS * ACCEPTORS_PER
    H.accepted = [0] * naccept
    H.connected = [0] * H.funcs
    H.state = {"listeners": listeners, "addrs": addrs,
               "accepted": H.accepted, "connected": H.connected}


def body(H):
    # spawn the acceptor herd FIRST so they are all parked before clients connect.
    listeners = H.state["listeners"]
    accepted = H.state["accepted"]
    slot = 0
    for lsock in listeners:
        for _ in range(ACCEPTORS_PER):
            H.go(acceptor, H, lsock, slot, accepted)
            slot += 1
    H.run_pool(H.funcs, client, H.state)


def post(H):
    total_accepted = sum(H.accepted)
    total_connected = sum(H.connected)
    H.check(total_connected > 0, "no client ever connected (test did no work)")
    H.check(total_accepted == total_connected,
            "accept conservation broken: accepted {0} != connected {1} "
            "(a connection was lost or double-accepted)".format(
                total_accepted, total_connected))
    H.log("listeners={0} acceptors={1} connected={2} accepted={3}".format(
        NLISTENERS, NLISTENERS * ACCEPTORS_PER, total_connected, total_accepted))


if __name__ == "__main__":
    harness.main("p216_accept_thundering_herd", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="many accept goroutines parked on a few shared listen "
                          "fds; every connection accepted exactly once")
