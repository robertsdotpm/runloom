"""big_100 / 213 -- select over data+timer with strict conservation.

A producer feeds a data channel; consumers `select` over
`[('recv', data), ('recv', After(timeout))]`.  Every produced item is either
consumed OR a timeout fires when the channel was transiently empty, and each
select resolves exactly once.

To make conservation checkable the topology is closed-world per worker group:
each worker owns ONE data channel, ONE producer goroutine, and ONE consumer
goroutine.  The producer sends a FIXED count of items then `close()`s the
channel.  The consumer loops selecting over (data, fresh After(timeout)):
  * a real item  -> consumed += 1
  * a closed-channel recv (ok=False) -> the producer is done; the consumer
    exits.  (select on a closed channel returns immediately with ok=False.)
  * a timeout    -> the channel was transiently empty; timed_out += 1, loop.

Because the consumer keeps looping until it sees the close, and every send is
either taken as an item or still buffered when the channel closes (a closed
channel still yields its buffered items before reporting ok=False), consumed
MUST equal produced.  A lost item (a select that ate a value but didn't deliver
it) would make consumed < produced; a double-consume would make consumed >
produced.

Invariant (post): consumed == produced exactly; timeouts occur only when the
channel was momentarily empty (we don't bound their count, just verify they
never substitute for a real item).

Stresses: select over data+timer, After() timeout churn, closed-channel drain
through select, no item lost or double-consumed.
"""
import random

import harness
import runloom
import runloom.time as rtime

ITEMS_PER_ROUND = 64


def try_recv(ch):
    """Chan.try_recv normalized: (value, True) if present else (None, False).
    runloom_c.Chan.try_recv returns None (not a tuple) when nothing is ready."""
    r = ch.try_recv()
    if r is None:
        return (None, False)
    return r


def producer(ch, n, base, jitter_seed):
    """Send n monotonic items then close.  A small jitter sleep makes the
    consumer's channel transiently empty so the timeout branch is exercised.
    Uses its OWN random.Random (sharing a Random across goroutines under M:N
    with the GIL off corrupts its Mersenne state)."""
    prng = random.Random(jitter_seed)
    try:
        for i in range(n):
            ch.send(base + i)
            if (i & 7) == 0:
                runloom.sleep(prng.uniform(0.0, 0.0008))
    finally:
        ch.close()


def consumer(ch, counts, slot, to_seed):
    """Select data vs a fresh per-iteration timeout; drain until closed.  Owns
    its own random.Random for the same reason as producer.

    IMPORTANT: ONLY the select's recv case consumes an item.  A blocking recv
    case correctly rendezvouses with a parked sender (and drains buffered
    items); a `try_recv` drain in the timeout branch does NOT reliably wake a
    parked sender, so on a cap-1 channel the sender of item k+1 (parked because
    the buffer was full) would never be woken after a try_recv took item k --
    deadlocking producer<->consumer.  We therefore treat a timeout purely as
    'channel transiently empty, loop' and let the next select recv take the
    item, rendezvousing with the sender."""
    prng = random.Random(to_seed)
    consumed = 0
    timed_out = 0
    while True:
        timer = rtime.After(prng.uniform(0.001, 0.004))
        idx, payload = runloom.select([("recv", ch), ("recv", timer)])
        if idx == 0:
            _v, ok = payload
            if not ok:
                break               # channel closed and drained -> done
            consumed += 1
        else:
            timed_out += 1          # transiently empty; loop, select again
    counts["consumed"][slot] += consumed
    counts["timed_out"][slot] += timed_out


def worker(H, wid, rng, state):
    produced = state["produced"]
    slot = wid & 1023
    rno = 0
    for _ in H.round_range():
        if not H.running():
            break
        rno += 1
        ch = runloom.Chan(rng.choice([1, 4, 16]))
        base = (wid << 20) | ((rno & 0xFFF) << 8)
        n = ITEMS_PER_ROUND
        # Distinct per-round seeds for the producer/consumer's OWN RNGs.
        pseed = rng.getrandbits(48)
        cseed = rng.getrandbits(48)
        wg = runloom.WaitGroup()
        wg.add(2)

        def run_producer(ch=ch, base=base, n=n, pseed=pseed):
            try:
                producer(ch, n, base, pseed)
            finally:
                wg.done()

        def run_consumer(ch=ch, slot=slot, cseed=cseed):
            try:
                consumer(ch, state, slot, cseed)
            finally:
                wg.done()

        H.go(run_producer)
        H.go(run_consumer)
        wg.wait()
        produced[slot] += n
        H.op(wid, n)
        H.task_done(wid)


def setup(H):
    H.state = {"produced": [0] * 1024, "consumed": [0] * 1024,
               "timed_out": [0] * 1024}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    p = sum(H.state["produced"])
    c = sum(H.state["consumed"])
    t = sum(H.state["timed_out"])
    H.log("produced={0} consumed={1} timed_out={2}".format(p, c, t))
    H.check(p > 0, "no items produced")
    H.check(c == p,
            "conservation broken: consumed={0} != produced={1} (item lost or "
            "double-consumed through select)".format(c, p))


if __name__ == "__main__":
    harness.main("p213_select_timer_conservation", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="producer/consumer with select(data, After timer); "
                          "consumed == produced, timeouts only when empty")
