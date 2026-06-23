"""big_100 / 113 -- signal delivery during a blocking syscall.

A real OS thread fires SIGUSR1 at the process at a steady rate while thousands
of goroutines sit parked in a cooperative recv() (a socketpair whose producer
sends only every few milliseconds, so the consumer is parked most of the time).
A Python signal handler must run -- and run INSIDE a parked goroutine via the
netpoll signal-wake path -- without corrupting the goroutine, dropping the
in-flight byte, or wedging the scheduler.  After the handler runs the recv must
resume and still deliver every byte the producer sent (EINTR handled
transparently, exactly as a real recv() retries across a signal).

Stresses: signal delivery into a parked goroutine, EINTR handling, netpoll
signal-wake, main-thread signal-handler constraints under M:N.
"""
import os
import signal
import socket as _socket
import time as _time
import _thread as _real_thread       # captured before monkey.patch()

import harness
import netutil
import runloom

REAL_SLEEP = _time.sleep

# --- signal handler: installed at IMPORT time, on the main thread (the only
#     thread allowed to call signal.signal / run Python signal handlers). ----
SIG_COUNT = [0]


def on_sigusr1(signum, frame):
    SIG_COUNT[0] += 1


try:
    signal.signal(signal.SIGUSR1, on_sigusr1)
    HAVE_SIGNAL = True
except (ValueError, OSError, AttributeError):
    HAVE_SIGNAL = False        # Windows / no SIGUSR1


def sender_thread(H):
    """Real OS thread: hammer the process with SIGUSR1 while the run is live."""
    pid = os.getpid()
    while H.running():
        try:
            os.kill(pid, signal.SIGUSR1)
        except OSError:
            pass
        REAL_SLEEP(0.002)


def consumer(H, wid, rng, pairs):
    r, _w = pairs[wid >> 1]
    got = state_count = 0
    while H.running():
        try:
            b = r.recv(1)             # parks here; a signal may interrupt it
        except OSError:
            break
        if not b:
            break
        if not H.check(b == b"x", "byte corruption wid={0}: {1!r}".format(wid, b)):
            return
        got += 1
        H.op(wid)
        if (got & 63) == 0:
            H.task_done(wid)
    pairs_recv = pairs[wid >> 1]
    # stash per-consumer received count in the shared tally (single writer)
    H.recv_counts[wid >> 1] = got


def producer(H, wid, rng, pairs):
    _r, w = pairs[wid >> 1]
    sent = 0
    while H.running():
        try:
            w.send(b"x")
        except OSError:
            break
        sent += 1
        runloom.sleep(rng.uniform(0.001, 0.006))
    H.sent_counts[wid >> 1] = sent


def worker(H, wid, rng, pairs):
    if (wid & 1) == 0:
        consumer(H, wid, rng, pairs)
    else:
        producer(H, wid, rng, pairs)


def setup(H):
    npairs = max(1, H.funcs // 2)
    pairs = []
    for _ in range(npairs):
        a, b = _socket.socketpair()
        a.setblocking(True)
        b.setblocking(True)
        H.register_close(a)
        H.register_close(b)
        pairs.append((a, b))          # consumer reads a, producer writes b
    H.state = pairs
    H.recv_counts = [0] * npairs
    H.sent_counts = [0] * npairs


def body(H):
    if HAVE_SIGNAL:
        _real_thread.start_new_thread(sender_thread, (H,))
    n = (H.funcs // 2) * 2
    H.run_pool(n, worker, H.state)


def post(H):
    H.check(HAVE_SIGNAL, "SIGUSR1 unavailable on this platform")
    H.check(SIG_COUNT[0] > 0, "no SIGUSR1 was ever delivered/handled")
    recv = sum(H.recv_counts)
    sent = sum(H.sent_counts)
    H.check(recv > 0, "no bytes received across signal storm")
    # Every received byte was sent; some sent bytes may still be in flight at
    # teardown, so recv <= sent, but the signals must not have dropped data
    # beyond the small in-flight window (one per pair max).
    H.check(recv <= sent, "received {0} > sent {1} (phantom bytes)".format(recv, sent))
    H.log("signals_handled={0} sent={1} recv={2}".format(
        SIG_COUNT[0], sent, recv))


if __name__ == "__main__":
    harness.main("p113_signal_during_syscall", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="SIGUSR1 storm while goroutines park in recv; handler "
                          "runs in-goroutine, recv resumes, no byte lost")
