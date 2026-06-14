"""big_100 / 201 -- select linearizability (token conservation).

A small shared set of buffered channels.  Half the goroutines are SENDERS that
`select`-send a globally-unique token (sender id + a per-sender monotonic seq)
onto whichever channel is ready; half are RECEIVERS that `select`-recv and
record every token they get.  Run for the duration, then DRAIN the channels at
teardown.

Conservation (post): every token sent is received EXACTLY ONCE -- no loss
(total received == total sent) and no duplication (the per-receiver token sets
are pairwise disjoint, so their sizes sum to the size of their union).  A select
that loses a value, double-delivers it, or crosses tokens between channels
breaks one of these.

Stresses: select send/recv linearizability under M:N, buffered-channel
hand-off, cancellation of the losing select cases, no lost/duplicated value.
"""
import harness
import runloom

NCHAN = 16
CHAN_CAP = 4


def setup(H):
    chans = [runloom.Chan(CHAN_CAP) for _ in range(NCHAN)]
    for ch in chans:
        H.register_close(ch)
    half = H.funcs // 2
    H.state = {
        "chans": chans,
        "nsenders": half,
        # per-sender count of tokens successfully sent (single writer per slot)
        "sent": [0] * max(1, half),
        # per-receiver SET of tokens received (single writer per slot)
        "recv_sets": [set() for _ in range(max(1, H.funcs - half))],
    }


def sender(H, wid, rng, state):
    chans = state["chans"]
    sid = wid                       # senders are wids [0, nsenders)
    seq = 0
    base = sid << 32
    sent = 0
    while H.running():
        seq += 1
        tok = base | seq
        # send-select over every channel; take whichever is ready.
        cases = [("send", ch, tok) for ch in chans]
        try:
            r = runloom.select(cases, default=True)
        except Exception:
            break                   # channels closed at teardown
        if isinstance(r, int):      # -1: every channel full right now
            runloom.sleep(0.0002)
            seq -= 1                # token not sent; reuse this seq
            continue
        sent += 1
        H.op(wid)
    state["sent"][sid] = sent


def receiver(H, wid, rng, state):
    chans = state["chans"]
    ridx = wid - state["nsenders"]  # receivers are wids [nsenders, funcs)
    seen = state["recv_sets"][ridx]
    # While running, select-recv over the shared channels.  At teardown the
    # channels are closed (by register_close); a closed channel STILL delivers
    # its buffered values via try_recv (ok=True) until empty, then ok=False --
    # so we must drain every channel to empty before exiting, or we'd lose the
    # buffered tail and break conservation.  A closed channel's select-recv case
    # returns ok=False immediately, which would busy-spin, so once we're not
    # running we switch to an explicit try_recv drain sweep.
    while H.running():
        cases = [("recv", ch) for ch in chans]
        try:
            r = runloom.select(cases, default=True)
        except Exception:
            break
        if isinstance(r, int):      # nothing ready
            runloom.sleep(0.0002)
            continue
        _idx, (val, ok) = r
        if ok:
            seen.add(val)
            H.op(wid)
    # teardown drain: keep sweeping until every channel is empty (ok=False).
    # Multiple receivers race here -- each token goes to exactly one of them
    # (try_recv pops it), so the union is still complete and disjoint.
    while True:
        got_any = False
        for ch in chans:
            while True:
                # try_recv(): None if open+empty (would block), (v,True) if a
                # value is ready, (None,False) if closed+empty.
                res = ch.try_recv()
                if res is None:
                    break
                v, ok = res
                if not ok:
                    break
                seen.add(v)
                H.op(wid)
                got_any = True
        if not got_any:
            # one more check: if any channel is still OPEN (a slow sender hasn't
            # finished), yield and retry; once all closed+empty, exit.
            if all(ch.closed for ch in chans):   # `.closed` is a bool attr
                break
            runloom.yield_now()


def worker(H, wid, rng, state):
    if wid < state["nsenders"]:
        sender(H, wid, rng, state)
    else:
        receiver(H, wid, rng, state)


def body(H):
    # one combined pool so wid 0..nsenders-1 are senders, the rest receivers.
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    total_sent = sum(H.state["sent"])
    sets = H.state["recv_sets"]
    total_recv = sum(len(s) for s in sets)   # disjoint -> == |union| if no dup
    union = set()
    for s in sets:
        union |= s
    H.check(total_recv == len(union),
            "DUPLICATE delivery: receivers hold {0} tokens but only {1} are "
            "distinct (a token was delivered twice)".format(
                total_recv, len(union)))
    H.check(len(union) == total_sent,
            "token LOSS/EXCESS: sent={0} but received-distinct={1} "
            "(diff {2})".format(total_sent, len(union), total_sent - len(union)))
    H.log("sent={0} received_distinct={1} receiver_total={2}".format(
        total_sent, len(union), total_recv))


if __name__ == "__main__":
    harness.main("p201_select_linearizability", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="select send/recv conservation over shared buffered "
                          "channels: every token received exactly once")
