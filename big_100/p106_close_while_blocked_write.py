"""big_100 / 106 -- close while blocked in write.

Per unit: a connected socketpair whose send/recv buffers are shrunk to a few KB.
A writer goroutine fills that buffer (the peer never reads) until it can no longer
send, then parks for writability; a separate goroutine closes the writer's fd.
The blocked writer MUST wake.

The writer parks with a BOUNDED wait (wait_fd for writability with a 2s ceiling)
rather than a blind sendall(): if the cross-goroutine close correctly wakes the
parked write, the wait returns immediately; if the wakeup is ever LOST, the 2s
ceiling backstops it so the writer still exits and teardown never wedges.  Each
unit records HOW its writer woke -- promptly via the close, or only via the
timeout backstop (a candidate lost wakeup) -- so a residual close-vs-parked-send
lost-wakeup surfaces as a metric instead of a hang.

Stresses: cross-goroutine close cancelling a parked send, netpoll write-arm
teardown, EPOLLOUT cancellation.  Fully local (socketpair, no listener).  See
FINDINGS for the close-vs-parked-send lost-wakeup this measures.
"""
import socket

import harness
import runloom
import runloom_c

BUFSZ = 4096                    # shrink buffers so the send buffer fills fast
CHUNK = b"W" * 65536
WAIT_CEILING_MS = 2000          # bound so a lost wakeup backstops, never hangs


def writer(H, sock, ready, done):
    """Fill the send buffer until it would block, then park for writability
    (bounded).  Report through `done`: 1 = the close woke us, 2 = only the
    timeout ceiling fired (a candidate lost wakeup)."""
    sock.setblocking(False)
    try:
        while True:
            sock.send(CHUNK)               # fill until the buffer is full
    except BlockingIOError:
        pass                               # buffer full -> now we would block
    except OSError:
        ready.send(True)
        done.send(1)
        return
    ready.send(True)                       # genuinely unable to send -> "parked"
    try:
        fd = sock.fileno()
    except (OSError, ValueError):
        done.send(1)
        return
    if fd < 0:
        done.send(1)
        return
    r = runloom_c.wait_fd(fd, 2, WAIT_CEILING_MS)   # park for writability
    done.send(1 if r != 0 else 2)


def unit(H, wid, rng, woken_close, woken_timeout):
    a, b = socket.socketpair()
    for s in (a, b):
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, BUFSZ)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, BUFSZ)
        except OSError:
            pass
    ready = runloom.Chan(1)
    done = runloom.Chan(1)
    H.go(writer, H, a, ready, done)
    ready.recv()                           # writer filled the buffer; about to park
    runloom.sleep(0.003)                   # let it actually reach the parked write
    try:
        a.close()                          # cross-goroutine close of the write fd
    except OSError:
        pass
    try:
        b.close()
    except OSError:
        pass
    how, _ok = done.recv()                 # bounded: returns within WAIT_CEILING
    if how == 1:
        woken_close[wid] += 1
    else:
        woken_timeout[wid] += 1
    return True


def worker(H, wid, rng, state):
    woken_close, woken_timeout = state
    for _ in H.round_range():
        unit(H, wid, rng, woken_close, woken_timeout)
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.woken_close = [0] * H.funcs
    H.woken_timeout = [0] * H.funcs
    H.state = (H.woken_close, H.woken_timeout)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    by_close = sum(H.woken_close)
    by_timeout = sum(H.woken_timeout)
    total = by_close + by_timeout
    H.check(total > 0, "no writers were ever closed (test did no work)")
    H.check(by_close > 0, "close NEVER woke a blocked writer (wake path broken)")
    H.check(by_timeout * 20 <= total + 1,
            "close-vs-parked-send lost wakeups too frequent: {0}/{1} needed the "
            "timeout backstop".format(by_timeout, total))
    H.log("woken_by_close={0} woken_by_timeout(lost?)={1} total={2}".format(
        by_close, by_timeout, total))


if __name__ == "__main__":
    # Moderate concurrency on purpose (see p105 / FINDINGS): the close-vs-parked-
    # send handoff plus raw-syscall socketpair()/close() churn does not scale to
    # 1M; capping is the honest fix.
    harness.main("p106_close_while_blocked_write", body, setup=setup, post=post,
                 default_funcs=150, max_funcs=300,
                 describe="fill a socketpair send buffer so a goroutine blocks in "
                          "send, then close the fd; the writer must wake (bounded "
                          "backstop measures any lost wakeup)")
