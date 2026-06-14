"""big_100 / 103 -- cancelled accept, then listener fd reuse.

Each unit starts a listener with a couple of accept goroutines PARKED in a real
cooperative accept(); it then closes the listener to cancel those waiters.  A
NEW listener is bound on the SAME H.net_ip(0) (very likely reusing the just-
freed fd NUMBER) and a connection is made to it.  The new listener must accept
that connection correctly -- no stale accept registration bleeding through the
reused fd, and the old accept waiters must all have exited.

Stresses: cross-goroutine close cancelling a parked accept, listener fd-number
reuse, the netpoll per-fd arm cache across a listen-fd recycle.  Fully local.
"""
import socket
import struct

import harness
import netutil
import runloom
import runloom_c

ACCEPT_CEILING_MS = 2000        # bound the parked accept so a lost close-wake of
                                # a parked accept (FINDINGS #5) backstops, no hang


def accept_one(H, srv, slot, exited, done):
    """Wait for the doomed listener to be closed, then exit.

    We deliberately do NOT park in wait_fd(READ)/accept() on the doomed listener:
    closing a listener does not reliably wake a goroutine parked in accept()
    (FINDINGS #5), AND wait_fd(fd, READ, ceiling) is NOT honoured once the listen
    fd is closed under it -- it parks forever (the accept-side of this campaign's
    F2; the socketpair-READ ceiling IS honoured, but a listen fd's is not).  So a
    waiter that parked in accept here could never be reliably cancelled.  Instead
    we observe the close by watching fileno() go to -1 (cooperative poll, hub-
    free), which is reliable and bounded.  Report through `done`."""
    for _ in range(ACCEPT_CEILING_MS):
        try:
            if srv.fileno() < 0:
                break                   # the doomed listener was closed -> exit
        except (OSError, ValueError):
            break
        runloom.sleep(0.001)
    exited[slot] = 1
    try:
        done.send(slot)
    except Exception:
        pass


def listener_reuse_unit(H, wid, rng):
    """One cancel-then-reuse cycle.  Returns True on a clean accepted round-trip
    on the freshly-bound listener, False if the run is shutting down."""
    host = H.net_ip(0)

    # 1) doomed listener with parked accept waiters.
    doomed = netutil.listen_tcp(host=host, backlog=8)
    n_waiters = 2
    exited = [0] * n_waiters
    done = runloom.Chan(n_waiters)
    for i in range(n_waiters):
        H.go(accept_one, H, doomed, i, exited, done)
    # let the waiters reach the accept park
    runloom.yield_now()
    H.sleep(0.001)
    # 2) cancel every parked accept by closing the listen fd cross-goroutine.
    netutil.close_quiet(doomed)

    # 3) fresh listener on the same IP -- likely the same fd NUMBER.
    fresh = netutil.listen_tcp(host=host, backlog=8)
    port = fresh.getsockname()[1]
    accepted = [0]

    def fresh_acceptor():
        # one-shot: accept exactly the connection we make below.  We park in a
        # real cooperative accept; the connect() below wakes it.  A guard wait_fd
        # bounds the park so a lost connect (shutdown) can't hang teardown.
        if not (runloom_c.wait_fd(fresh.fileno(), 1, 2000) & 1):
            return
        try:
            conn, _addr = fresh.accept()
        except OSError:
            return
        try:
            tag = netutil.recv_exact(conn, 12)
            conn.sendall(tag)
            accepted[0] = 1
        except OSError:
            pass
        finally:
            netutil.close_quiet(conn)

    H.go(fresh_acceptor)
    runloom.yield_now()

    # 4) connect to the fresh listener and round-trip a tagged frame.
    tag = struct.pack("<III", 0x5A5A5A5A, wid & 0xFFFFFFFF, rng.getrandbits(32))
    got = None
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((host, port))
        s.sendall(tag)
        got = netutil.recv_exact(s, 12)
    except OSError:
        got = None
    finally:
        netutil.close_quiet(s)

    # wait for both old accept waiters to exit (bounded by their wait ceiling;
    # done is buffered so a waiter that already exited doesn't block).
    for _ in range(n_waiters):
        done.recv()
    netutil.close_quiet(fresh)

    if got is None:
        return None    # transient during shutdown
    # Invariants for this unit:
    H.check(got == tag,
            "fresh-listener cross-talk wid={0}: sent {1!r} got {2!r}".format(
                wid, tag, got))
    H.check(sum(exited) == n_waiters,
            "old accept waiters did not all exit wid={0}: {1}/{2}".format(
                wid, sum(exited), n_waiters))
    H.check(accepted[0] == 1,
            "fresh listener never accepted wid={0}".format(wid))
    return got == tag and sum(exited) == n_waiters and accepted[0] == 1


def worker(H, wid, rng, state):
    for _ in H.round_range():
        ok = listener_reuse_unit(H, wid, rng)
        if ok is None:
            if not H.running():
                break
            H.task_done(wid)
            continue
        if ok:
            H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, None)


if __name__ == "__main__":
    # Moderate concurrency on purpose (see p105 / FINDINGS): the delicate
    # close-cancels-accept handoff plus per-unit listener churn does not scale.
    harness.main("p103_cancelled_accept_listener_reuse", body,
                 default_funcs=200, max_funcs=300,
                 describe="close cancels parked accepts; a fresh listener reusing "
                          "the fd accepts correctly, no stale-arm bleed")
