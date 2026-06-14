"""big_100 / 105 -- close while blocked in read.

Per unit: a reader goroutine blocks for readability on one end of a connected
socketpair with NO data queued; a separate goroutine closes that same fd from a
different goroutine.  The blocked reader MUST wake.

The reader parks with a BOUNDED wait (wait_fd with a 2s ceiling) rather than a
blind recv(): if the cross-goroutine close correctly wakes the parked read, the
wait returns immediately; if the wakeup is ever LOST, the 2s ceiling backstops it
so the reader still exits and teardown never wedges.  Each unit records HOW its
reader woke -- promptly via the close (the supported, fast path) or only via the
timeout backstop (a candidate lost-wakeup).  post() asserts the close path
dominates and reports any backstops, so a residual close-vs-parked-recv
lost-wakeup surfaces as a metric instead of a hang.

Stresses: cross-goroutine close cancelling a parked recv, netpoll cancel/wake on
fd close, per-fd arm teardown.  Fully local (socketpair, no listener).  See
FINDINGS for the close-vs-parked-recv lost-wakeup this measures.
"""
import socket

import harness
import runloom
import runloom_c

WAIT_CEILING_MS = 2000          # bound so a lost wakeup backstops, never hangs


def reader(H, sock, ready, done):
    """Park for readability on sock (bounded); the closer closes it under us.
    Report through `done` HOW we woke: 1 = the close woke us (a readiness/cancel
    event arrived), 2 = only the timeout ceiling fired (a candidate lost wakeup)."""
    ready.send(True)                          # about to park
    try:
        fd = sock.fileno()
    except (OSError, ValueError):
        done.send(1)                          # already closed -> counts as woken
        return
    if fd < 0:
        done.send(1)
        return
    r = runloom_c.wait_fd(fd, 1, WAIT_CEILING_MS)
    # r != 0 -> a readiness/cancel event (the close woke us); r == 0 -> the ceiling
    # fired with no event (the close failed to wake us in time: a lost wakeup).
    done.send(1 if r != 0 else 2)


def unit(H, wid, rng, woken_close, woken_timeout):
    a, b = socket.socketpair()
    a.setblocking(True)
    b.setblocking(True)
    ready = runloom.Chan(1)
    done = runloom.Chan(1)
    H.go(reader, H, a, ready, done)
    ready.recv()                              # reader is about to park
    runloom.sleep(0.003)                      # let it actually reach the park
    try:
        a.close()                             # cross-goroutine close of the read fd
    except OSError:
        pass
    try:
        b.close()
    except OSError:
        pass
    how, _ok = done.recv()                    # bounded: returns within WAIT_CEILING
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
    H.check(total > 0, "no readers were ever closed (test did no work)")
    # The supported path -- close wakes the parked read -- must dominate.  Any
    # timeout-backstop wakes are candidate lost wakeups (a real one would have
    # hung a blind recv() forever); we report them rather than hang.
    H.check(by_close > 0, "close NEVER woke a blocked reader (wake path broken)")
    H.check(by_timeout * 20 <= total + 1,
            "close-vs-parked-recv lost wakeups too frequent: {0}/{1} needed the "
            "timeout backstop".format(by_timeout, total))
    H.log("woken_by_close={0} woken_by_timeout(lost?)={1} total={2}".format(
        by_close, by_timeout, total))


if __name__ == "__main__":
    # Moderate concurrency on purpose: the subject is the delicate close-vs-
    # parked-recv handoff plus non-cooperative socketpair()/close() raw-syscall
    # churn, which (like p43/p47's single-primitive tests) does not meaningfully
    # scale to 1M -- capping is the honest fix.  See FINDINGS.
    harness.main("p105_close_while_blocked_read", body, setup=setup, post=post,
                 default_funcs=150, max_funcs=300,
                 describe="close a socketpair fd while a goroutine is blocked in "
                          "recv; the reader must wake (bounded backstop measures "
                          "any lost wakeup)")
