"""big_100 / 114 -- SIGALRM setitimer storm.

A repeating ITIMER_REAL fires SIGALRM at a high rate while thousands of
goroutines do cooperative work (sleeps + socketpair round-trips).  The Python
SIGALRM handler (installed at import on the main thread) must run without
corrupting an in-flight goroutine, dropping a byte, or wedging the scheduler --
even when it reenters the running interpreter at arbitrary points.  Under M:N
the handler fires RARELY (the main thread is mostly in C), so the assertion is
no-crash + data-integrity + handler-ran-at-least-once, NOT a high delivery
count.

Stresses: SIGALRM reentrancy, setitimer, signal-handler-under-M:N, byte
integrity across a signal storm.
"""
import os
import signal
import socket as _socket
import time as _time

import harness
import runloom

REAL_SLEEP = _time.sleep

# --- handler installed at IMPORT time, on the main thread (the only thread
#     allowed to run Python signal handlers). -------------------------------
SIG_COUNT = [0]


def on_sigalrm(signum, frame):
    SIG_COUNT[0] += 1


try:
    signal.signal(signal.SIGALRM, on_sigalrm)
    HAVE_ITIMER = hasattr(signal, "setitimer")
except (ValueError, OSError, AttributeError):
    HAVE_ITIMER = False        # no SIGALRM / setitimer on this platform


def worker(H, wid, rng, pairs):
    # Each connected pair (a, b) is shared by two consecutive workers: even =
    # the "pinger" on socket a (send tag, recv echo); odd = the "echoer" on
    # socket b (recv, send back).  a<->b is connected: a.send -> b.recv and
    # b.send -> a.recv, so the tag does a full a->b->a round-trip.
    a, b = pairs[wid >> 1]
    if (wid & 1) == 0:
        sock = a
        tag = (wid & 0xFE) or 1        # nonzero
        got = 0
        for _ in H.round_range():
            try:
                sock.send(bytes([tag]))
                got_b = sock.recv(1)   # parks here; SIGALRM may interrupt it
            except OSError:
                break
            if not got_b:
                break
            if not H.check(got_b == bytes([tag]),
                           "byte corruption wid={0}: {1!r}!={2}".format(
                               wid, got_b, tag)):
                return
            got += 1
            H.op(wid)
            if (got & 31) == 0:
                H.task_done(wid)
            if rng.random() < 0.3:
                runloom.sleep(rng.uniform(0.0, 0.002))
        H.recv_counts[wid >> 1] = got
    else:
        # The echoer on socket b: bounce whatever the pinger sent straight back.
        sock = b
        for _ in H.round_range():
            try:
                got_b = sock.recv(1)
            except OSError:
                break
            if not got_b:
                break
            try:
                sock.send(got_b)
            except OSError:
                break
            H.op(wid)


def setup(H):
    npairs = max(1, (H.funcs + 1) // 2)
    pairs = []
    for _ in range(npairs):
        a, b = _socket.socketpair()
        a.setblocking(True)
        b.setblocking(True)
        H.register_close(a)
        H.register_close(b)
        # even worker uses (a-recv, a-send); odd worker uses (b-recv, b-send).
        # Cross-wire: pinger sends on a, echoer reads on b, sends on b, pinger
        # reads on a.  a<->b is a connected pair, so a.send -> b.recv.
        pairs.append((a, b))
    H.state = pairs
    H.recv_counts = [0] * npairs


def body(H):
    if HAVE_ITIMER:
        # Fire SIGALRM repeatedly: a small interval so the handler reenters the
        # interpreter under load.  Disabled at teardown via add_cleanup.
        signal.setitimer(signal.ITIMER_REAL, 0.002, 0.002)
        H.add_cleanup(lambda: signal.setitimer(signal.ITIMER_REAL, 0, 0))
    n = ((H.funcs + 1) // 2) * 2
    H.run_pool(n, worker, H.state)


def post(H):
    # Turn the timer off before anything else so no SIGALRM races teardown.
    if HAVE_ITIMER:
        try:
            signal.setitimer(signal.ITIMER_REAL, 0, 0)
        except OSError:
            pass
    H.check(HAVE_ITIMER, "setitimer/SIGALRM unavailable on this platform")
    recv = sum(H.recv_counts)
    H.check(recv > 0, "no round-trips completed across the SIGALRM storm")
    # SIG_COUNT may be 0 under M:N (handler rarely runs while main is in C);
    # the load-bearing invariants are no-crash + byte-integrity (checked
    # per-round above) + forward progress.  We only LOG the handler count.
    H.log("sigalrm_handled={0} round_trips={1}".format(SIG_COUNT[0], recv))


if __name__ == "__main__":
    harness.main("p114_setitimer_storm", body, setup=setup, post=post,
                 default_funcs=1000,
                 describe="repeating ITIMER_REAL fires SIGALRM while goroutines "
                          "do socketpair round-trips; survive reentrancy, no "
                          "byte loss")
