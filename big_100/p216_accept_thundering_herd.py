"""big_100 / 216 -- accept thundering herd.

A few listener fds (SO_REUSEPORT), each with MANY accept goroutines parked on it
at once -- the one-shot-netpoll thundering herd.  Clients connect and each sends
a UNIQUE token; the acceptor that wins the accept reads the token and echoes it
back; the client must get ITS OWN token back.

The strong, robust invariant is NO CROSS-TALK: under a thundering herd on one
listen fd, if the netpoll ever handed a connection's readiness/data to the wrong
owner, some client would get back a token it did not send -- caught inline.  (We
do not assert exact accepted==connected: connections still queued in the backlog
at the deadline are legitimately neither served nor confirmed, so an exact count
is unachievable; instead we require a high CONFIRMED fraction, which a systematic
lost-accept from a missed re-arm would fail.)  Every client recv is bounded so a
backlog straggler can never wedge teardown.

Stresses: many goroutines parked on ONE listen fd's EPOLLONESHOT arm (only one
wakes per readiness, must re-arm, the rest stay parked), accept fairness, no
lost/duplicate/misdelivered accept.  Fully local (loopback).
"""
import socket
import struct

import harness
import netutil
import runloom
import runloom_c

NLISTENERS = 4          # a few listener fds (SO_REUSEPORT)
ACCEPTORS_PER = 64      # many accept goroutines parked per listener fd
TOKLEN = 8
RECV_CEILING_MS = 1000  # bound the client's wait for its echo (straggler-safe)


def make_listener(host):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except (AttributeError, OSError):
        pass
    return s


def acceptor(H, lsock, slot, served):
    """Park in accept on the SHARED listen fd; for each accepted connection read
    its token and echo it straight back, then close.  Drains the backlog past the
    deadline so a connect landing at the deadline is still served."""
    n = 0
    empty_after_stop = 0
    while True:
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
                    break
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
        conn = None
        try:
            conn = socket.socket(lsock.family, lsock.type, lsock.proto, fileno=cfd)
            conn.setblocking(True)
            tok = netutil.recv_exact(conn, TOKLEN)   # the client's unique token
            conn.sendall(tok)                        # echo it straight back
            n += 1
        except OSError:
            pass
        finally:
            if conn is not None:
                netutil.close_quiet(conn)
    served[slot] = n


def client(H, wid, rng, state):
    addrs = state["addrs"]
    attempted = state["attempted"]
    confirmed = state["confirmed"]
    a = 0
    ok = 0
    seq = 0
    for _ in H.round_range():
        addr = addrs[rng.randrange(len(addrs))]
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect(addr)
            a += 1
            seq += 1
            tok = struct.pack("<II", wid, seq)       # unique per (wid, seq)
            s.sendall(tok)
            # bounded wait for the echo so a straggler never wedges teardown.
            if runloom_c.wait_fd(s.fileno(), 1, RECV_CEILING_MS) & 1:
                got = netutil.recv_exact(s, TOKLEN)
                if not H.check(got == tok,
                               "accept CROSS-TALK wid={0} seq={1}: sent {2!r} "
                               "got {3!r}".format(wid, seq, tok, got)):
                    return
                ok += 1
                H.op(wid)
        except OSError:
            pass
        finally:
            netutil.close_quiet(s)
        H.task_done(wid)
    attempted[wid] += a
    confirmed[wid] += ok


def setup(H):
    host = H.net_ip(0)
    listeners = []
    addrs = []
    for _ in range(NLISTENERS):
        s = make_listener(host)
        s.bind((host, 0))
        s.listen(4096)
        s.setblocking(False)
        # NOT register_close'd: mark_done() closes registered listeners at the
        # deadline, which would close the listen fd under acceptors still draining
        # the backlog.  Acceptors self-terminate; we close in cleanup.
        H.add_cleanup(lambda sk=s: netutil.close_quiet(sk))
        listeners.append(s)
        addrs.append(s.getsockname())
    naccept = NLISTENERS * ACCEPTORS_PER
    H.served = [0] * naccept
    H.attempted = [0] * H.funcs
    H.confirmed = [0] * H.funcs
    H.state = {"listeners": listeners, "addrs": addrs, "served": H.served,
               "attempted": H.attempted, "confirmed": H.confirmed}


def body(H):
    # spawn the acceptor herd FIRST so they are all parked before clients connect.
    listeners = H.state["listeners"]
    served = H.state["served"]
    slot = 0
    for lsock in listeners:
        for _ in range(ACCEPTORS_PER):
            H.go(acceptor, H, lsock, slot, served)
            slot += 1
    H.run_pool(H.funcs, client, H.state)


def post(H):
    total_served = sum(H.served)
    total_attempted = sum(H.attempted)
    total_confirmed = sum(H.confirmed)
    H.check(total_attempted > 0, "no client ever connected (test did no work)")
    H.check(total_confirmed > 0, "no connection round-tripped its token")
    # A systematic lost-accept (a missed one-shot re-arm) would starve the herd
    # and collapse the confirmed fraction; require most attempts to round-trip.
    H.check(total_confirmed * 4 >= total_attempted * 3,
            "confirmed round-trips too low: {0}/{1} (lost accepts under the "
            "herd?)".format(total_confirmed, total_attempted))
    H.log("listeners={0} acceptors={1} attempted={2} confirmed={3} served={4}".format(
        NLISTENERS, NLISTENERS * ACCEPTORS_PER, total_attempted,
        total_confirmed, total_served))


if __name__ == "__main__":
    harness.main("p216_accept_thundering_herd", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="many accept goroutines parked on a few shared listen "
                          "fds; every connection round-trips its own token, no "
                          "cross-talk")
