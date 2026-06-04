"""big_100 / 10 -- Happy Eyeballs simulator.

A dual-stack echo service listens on both 127.0.0.1 (IPv4) and ::1 (IPv6).
For each attempt a goroutine races an IPv4 connect against an IPv6 connect
(with deterministic jitter so the winner varies); the first to connect wins,
the loser path is cancelled/cleaned up.

Stresses: racing two connects, channel select, cancellation of the loser,
socket cleanup.
"""
import socket

import harness
import netutil
import runloom


def setup(H):
    state = {"v4": None, "v6": None}
    srv4 = netutil.listen_tcp("127.0.0.1", family=socket.AF_INET)
    state["v4"] = srv4.getsockname()[:2]
    H.register_close(srv4)
    H.go(accept_echo, H, srv4)
    try:
        srv6 = netutil.listen_tcp("::1", family=socket.AF_INET6)
        state["v6"] = srv6.getsockname()[:2]
        H.register_close(srv6)
        H.go(accept_echo, H, srv6)
    except OSError:
        state["v6"] = None      # no IPv6 loopback on this box -> v4 always wins
    H.state = state


def accept_echo(H, srv):
    while H.running():
        try:
            conn, _ = srv.accept()
        except OSError:
            break

        def handle(conn=conn):
            try:
                while True:
                    d = conn.recv(4096)
                    if not d:
                        break
                    conn.sendall(d)
            except OSError:
                pass
            finally:
                netutil.close_quiet(conn)

        H.go(handle)


def connector(family, addr, jitter, result):
    """Try to connect; push (family, socket-or-None) onto the result chan."""
    sock = None
    try:
        runloom.sleep(jitter)
        if addr is None:
            result.send((family, None))
            return
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.connect(addr)
        result.send((family, sock))
        sock = None             # ownership handed to the receiver
    except OSError:
        netutil.close_quiet(sock)
        result.send((family, None))


def client(H, wid, rng, state):
    H.sleep(rng.random() * 0.5)
    while H.running():
        result = runloom.Chan(2)
        j4 = rng.random() * 0.01
        j6 = rng.random() * 0.01
        H.go(connector, socket.AF_INET, state["v4"], j4, result)
        H.go(connector, socket.AF_INET6, state["v6"], j6, result)

        winner = None
        loser = None
        for _ in range(2):
            fam, sock = result.recv()[0]
            if sock is None:
                continue
            if winner is None:
                winner = sock
            else:
                loser = sock        # arrived second -> cancel/cleanup
        netutil.close_quiet(loser)
        if winner is None:
            # Under the connect storm both listeners can transiently refuse;
            # Happy Eyeballs just retries -- not an invariant violation.
            if not H.running():
                break
            H.sleep(0.01)
            continue
        try:
            payload = rng.randbytes(32)
            winner.sendall(payload)
            got = netutil.recv_exact(winner, len(payload))
            H.check(got == payload, "echo mismatch wid={0}".format(wid))
            H.op(wid)
            H.task_done(wid)
        except OSError:
            pass
        finally:
            netutil.close_quiet(winner)


def body(H):
    H.run_pool(H.funcs, client, H.state)


if __name__ == "__main__":
    harness.main("p10_happy_eyeballs", body, setup=setup, default_funcs=6000,
                 describe="race v4/v6 connects, cancel the loser")
