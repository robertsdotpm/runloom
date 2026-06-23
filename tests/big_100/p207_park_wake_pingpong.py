"""big_100 / 207 -- park/wake ping-pong (lost-wakeup hunt).

N goroutines are paired up; each pair shares two UNBUFFERED channels (capacity
0), so every send/recv is a full rendezvous -- a park on one side and a wake on
the other.  The pinger sends a tagged, monotonically increasing token; the
ponger relays it straight back.  The pinger verifies the value it gets back is
exactly the one it sent.

An unbuffered channel maximises park/wake traffic, which is the substrate a
single lost wakeup (the classic Dekker-shaped seq_cst-fence bug) would wedge:
one dropped wake and that pair's ring stops dead, so the watchdog fires.  A
cross-talk between pairs shows up as a token mismatch.

Stresses: park_safe/wake_safe under M:N, unbuffered-channel rendezvous,
seq_cst store/load ordering, cross-hub wake delivery.
"""
import struct

import harness
import runloom

PINGS_PER_ROUND = 256


def pinger(H, wid, rng, pairs):
    a, b = pairs[wid >> 1]          # send on a, receive the echo on b
    seq = 0
    for _ in H.round_range():
        for _ in range(PINGS_PER_ROUND):
            if not H.running():
                break
            seq += 1
            tok = struct.pack("<II", wid >> 1, seq)
            try:
                a.send(tok)
            except Exception:
                return                       # channel closed at teardown
            val, ok = b.recv()
            if not ok:
                return
            if not H.check(val == tok,
                           "ping-pong cross-talk pair={0} seq={1}: sent {2!r} "
                           "got {3!r}".format(wid >> 1, seq, tok, val)):
                return
            H.op(wid)
        H.task_done(wid)


def ponger(H, wid, rng, pairs):
    a, b = pairs[wid >> 1]          # receive on a, relay back on b
    while H.running():
        val, ok = a.recv()
        if not ok:
            return
        try:
            b.send(val)
        except Exception:
            return
        H.op(wid)


def worker(H, wid, rng, pairs):
    if (wid & 1) == 0:
        pinger(H, wid, rng, pairs)
    else:
        ponger(H, wid, rng, pairs)


def setup(H):
    npairs = max(1, H.funcs // 2)
    pairs = [(runloom.Chan(0), runloom.Chan(0)) for _ in range(npairs)]
    for a, b in pairs:
        H.register_close(a)
        H.register_close(b)
    H.state = pairs


def body(H):
    # funcs must be even so every pinger has a ponger; round down.
    n = (H.funcs // 2) * 2
    H.run_pool(n, worker, H.state)


if __name__ == "__main__":
    harness.main("p207_park_wake_pingpong", body, setup=setup,
                 default_funcs=4000,
                 describe="unbuffered-channel ping-pong; a lost wakeup wedges a "
                          "pair (watchdog), cross-talk shows as a mismatch")
