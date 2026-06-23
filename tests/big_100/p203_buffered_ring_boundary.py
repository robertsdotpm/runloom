"""big_100 / 203 -- buffered ring boundary (cap 1 and cap N).

Producer/consumer pairs over channels whose capacity is a mix of 1 and N, driven
hard so each channel oscillates full<->empty.  Each producer sends a fixed run
of a strictly-increasing per-producer sequence (0,1,2,...) then `close()`s its
channel; its consumer drains until the channel is closed.

Conservation: every value produced is consumed exactly once (total produced ==
total consumed; producers send a known count, consumers count what they drain).
FIFO: a consumer must see its producer's sequence strictly increasing with no
gap and no reorder -- a buffered ring that drops, duplicates, or reorders a slot
at the full<->empty boundary breaks this.

Stresses: buffered channel ring buffer at cap 1 and cap N, full/empty boundary,
close-after-fill drain semantics, FIFO ordering, park/wake on a full/empty ring.

Invariant (post): produced == consumed; FIFO held on every channel (fifo
violations == 0).
"""
import harness
import runloom

SEND_COUNT = 64           # values each producer sends per round
BIG_CAP = 8


def setup(H):
    n = max(1, H.funcs // 2)          # one producer + one consumer per channel
    # Mix capacities: even channels cap 1, odd channels cap BIG_CAP.
    chans = [runloom.Chan(1 if (i & 1) == 0 else BIG_CAP) for i in range(n)]
    for ch in chans:
        H.register_close(ch)
    H.state = {
        "chans": chans,
        "nchan": n,
        "produced": [0] * n,           # per-producer slot
        "consumed": [0] * n,           # per-consumer slot
        "fifo_bad": [0] * n,           # per-consumer FIFO-violation slot
    }


def producer(H, wid, rng, state):
    ch = state["chans"][wid]           # producers are wids [0, nchan)
    total = 0
    for _ in H.round_range():
        if not H.running():
            break
        seq = 0
        ok_round = True
        for seq in range(SEND_COUNT):
            try:
                ch.send(seq)
            except Exception:
                ok_round = False
                break                  # closed at teardown
            total += 1
            H.op(wid)
        # Signal end-of-run to the consumer by closing, then RE-OPEN for the
        # next round is impossible (a Chan can't reopen) -- so we use ONE round
        # of sends per channel lifetime: close, and a fresh channel each round.
        if ok_round:
            ch.close()
        # next round gets a brand-new channel + consumer so close is one-shot.
        break
    state["produced"][wid] = total


def consumer(H, wid, rng, state):
    cidx = wid - state["nchan"]        # consumers are wids [nchan, 2*nchan)
    ch = state["chans"][cidx]
    got = 0
    last = -1
    bad = 0
    while True:
        try:
            val, ok = ch.recv()
        except Exception:
            break
        if not ok:
            break                       # channel closed and drained
        if val != last + 1:
            bad += 1                     # gap / reorder / duplicate
        last = val
        got += 1
        H.op(wid)
    state["consumed"][cidx] = got
    state["fifo_bad"][cidx] = bad


def body(H):
    n = H.state["nchan"]
    # Spawn all producers and all consumers; wid space: [0,n) producers,
    # [n, 2n) consumers, both indexing the same channel list.
    def both(H, wid, rng, state):
        if wid < state["nchan"]:
            producer(H, wid, rng, state)
        else:
            consumer(H, wid, rng, state)

    H.run_pool(2 * n, both, H.state)


def post(H):
    produced = sum(H.state["produced"])
    consumed = sum(H.state["consumed"])
    fifo_bad = sum(H.state["fifo_bad"])
    H.check(fifo_bad == 0,
            "FIFO violated: {0} out-of-order/gap/duplicate values across "
            "channels".format(fifo_bad))
    H.check(produced == consumed,
            "value LOSS/EXCESS: produced={0} consumed={1} (diff {2})".format(
                produced, consumed, produced - consumed))
    H.log("produced={0} consumed={1} fifo_violations={2} channels={3}".format(
        produced, consumed, fifo_bad, H.state["nchan"]))


if __name__ == "__main__":
    harness.main("p203_buffered_ring_boundary", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="cap-1 and cap-N channels: produced==consumed, FIFO "
                          "preserved across the full/empty boundary")
