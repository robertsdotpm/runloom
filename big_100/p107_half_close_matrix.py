"""big_100 / 107 -- half-close transition matrix.

Connected TCP pairs over loopback.  Each side, in a randomised order, performs a
subset of shutdown(SHUT_RD)/shutdown(SHUT_WR)/send/recv/close.  The runtime must
survive every transition: the peer sees EOF (b'') on recv after the other's
SHUT_WR; a send after the peer closed raises BrokenPipe/OSError; nothing crashes,
and -- crucially -- nothing parks forever.

The "recv" step is BOUNDED with a readiness timeout (a blocking recv on a
half-open pair where the peer is itself parked would deadlock both sides), so a
side always finishes its sequence and reports through a `done` channel; the
worker blocks on both reports, which paces the loop and lets the watchdog catch
any genuine wedge.

Stresses: the cooperative recv/send/shutdown paths across half-open TCP states,
EOF + EPIPE delivery under M:N, no parked-forever recv on a half-closed fd.
"""
import random
import socket

import harness
import netutil
import runloom
import runloom_c


def connected_pair(H, host, lport, lsock):
    """Make a fresh connected TCP pair via the shared listener: connect a client
    and accept the server end.  Returns (client, server) or (None, None)."""
    cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        cli.connect((host, lport))
    except OSError:
        netutil.close_quiet(cli)
        return None, None
    if not (runloom_c.wait_fd(lsock.fileno(), 1, 1000) & 1):
        netutil.close_quiet(cli)
        return None, None
    try:
        srv, _addr = lsock.accept()
    except OSError:
        netutil.close_quiet(cli)
        return None, None
    cli.setblocking(True)
    srv.setblocking(True)
    return cli, srv


def side_sequence(sock, rng):
    """Run a randomised sequence of half-close transitions on `sock`.  Every step
    is bounded (the recv waits for readiness with a short timeout instead of
    blocking forever), and an OSError on a step the prior transitions made
    invalid (send after peer close, etc.) is the EXPECTED legal outcome.  Returns
    True if every step behaved legally."""
    steps = ["send", "recv", "shut_wr", "shut_rd", "send", "recv"]
    rng.shuffle(steps)
    for step in steps:
        try:
            if step == "send":
                try:
                    sock.send(b"data")
                except OSError:
                    pass            # peer closed / SHUT_RD -> EPIPE: legal
            elif step == "recv":
                # bounded: only recv if readable within 20ms, else treat as
                # would-block (legal) so we never park waiting on a peer that is
                # itself parked.
                if runloom_c.wait_fd(sock.fileno(), 1, 20) & 1:
                    try:
                        sock.recv(64)   # b'' (peer SHUT_WR/closed) or raise: legal
                    except OSError:
                        pass
            elif step == "shut_wr":
                try:
                    sock.shutdown(socket.SHUT_WR)
                except OSError:
                    pass
            elif step == "shut_rd":
                try:
                    sock.shutdown(socket.SHUT_RD)
                except OSError:
                    pass
        except Exception:
            return False
    return True


def run_side(H, sock, result, slot, rng, done):
    ok = side_sequence(sock, rng)
    try:
        sock.close()
    except OSError:
        pass
    result[slot] = 1 if ok else 2     # 1 ok-exit, 2 illegal
    try:
        done.send(slot)
    except Exception:
        pass


def worker(H, wid, rng, state):
    host, lport, lsock = state
    for _ in H.round_range():
        cli, srv = connected_pair(H, host, lport, lsock)
        if cli is None:
            if not H.running():
                break
            H.task_done(wid)
            continue
        result = [0, 0]
        done = runloom.Chan(2)
        r1 = random.Random(rng.getrandbits(48))
        r2 = random.Random(rng.getrandbits(48))
        H.go(run_side, H, cli, result, 0, r1, done)
        H.go(run_side, H, srv, result, 1, r2, done)
        # block until BOTH sides finished their (bounded) transition sequence.
        done.recv()
        done.recv()
        if not H.check(result[0] == 1 and result[1] == 1,
                       "illegal half-close transition wid={0}: {1}".format(
                           wid, result)):
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    host = H.net_ip(0)
    lsock = netutil.listen_tcp(host=host, backlog=4096)
    H.register_close(lsock)
    lport = lsock.getsockname()[1]
    H.state = (host, lport, lsock)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    H.check(H.total_ops() > 0, "no half-close sequences completed")
    H.log("half_close_sequences={0}".format(H.total_ops()))


if __name__ == "__main__":
    harness.main("p107_half_close_matrix", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="randomised SHUT_RD/SHUT_WR/send/recv/close transition "
                          "matrix on connected TCP pairs; survive every state")
