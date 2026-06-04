"""big_100 / 42 -- many producers, single consumer.

Many producer goroutines each push numbered items (tagged with their id and a
per-producer sequence) onto one bounded channel; a single consumer drains it.
The consumer verifies that, per producer, sequence numbers arrive strictly
increasing with no gaps and no repeats -- nothing dropped, nothing duplicated.

Stresses: channel correctness and contention with one drain point.
"""
import harness
import runloom


def setup(H):
    H.state = {"ch": runloom.Chan(4096),
               "final_seq": {},             # producer wid -> how many it sent
               "last_seq": {},              # consumer's view, producer -> seq
               "received": [0]}


def producer(H, wid, rng, state):
    ch = state["ch"]
    seq = 0
    while H.running():
        ch.send((wid, seq))
        seq += 1
        H.op(wid)
    # Distinct-key write (free-threaded dict is safe) -- record items sent.
    state["final_seq"][wid] = seq


def consumer(H, state):
    ch = state["ch"]
    last = state["last_seq"]
    n = 0
    while True:
        got = ch.try_recv()
        if got is None:
            if not H.running():
                break
            runloom.sleep(0.0002)
            continue
        item, ok = got
        if not ok:
            break
        pid, seq = item
        prev = last.get(pid, -1)
        if not H.check(seq == prev + 1,
                       "gap/dup from producer {0}: seq {1} after {2}".format(
                           pid, seq, prev)):
            return
        last[pid] = seq
        n += 1
    state["received"][0] = n


def body(H):
    H.go(consumer, H, H.state)
    H.run_pool(H.funcs, producer, H.state)


def post(H):
    # Conservation: for every producer, the consumer must have seen exactly as
    # many items as that producer sent (channel fully drained, nothing lost).
    final = H.state["final_seq"]
    last = H.state["last_seq"]
    lost = 0
    for pid, sent in final.items():
        seen = last.get(pid, -1) + 1
        if seen != sent:
            lost += 1
    H.check(lost == 0,
            "{0} producers lost/extra items (sent != consumed)".format(lost))
    H.log("producers={0} received={1}".format(
        len(final), H.state["received"][0]))


if __name__ == "__main__":
    harness.main("p42_producers_consumer", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="many producers, one consumer; per-producer ordering")
