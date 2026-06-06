"""big_100 / 54 -- select-over-channels simulator.

Several channels.  Producers `select`-send each value onto whichever channel is
ready; consumers `select`-recv from whichever channel has data.  Random routing
across the channels exercises select's wakeups and the cancellation of the
losing waits.  Conservation: the sum sent equals the sum received.

Stresses: select wakeups, cancellation of non-chosen cases, fairness.

NOTE: runloom.select(default=True) returns a BARE int -1 when nothing is ready
(not a tuple) -- see FINDINGS BUG #3 -- so we branch on isinstance(r, int).
"""
import harness
import runloom

NCHAN = 8


def setup(H):
    # sent_sum/recv_sum: one slot per goroutine (indexed by wid) — no sharing,
    # no data race under GIL=0.
    H.state = {"chans": [runloom.Chan(16) for _ in range(NCHAN)],
               "sent_sum": [0] * H.funcs, "recv_sum": [0] * H.funcs}


def producer(H, wid, rng, state):
    chans = state["chans"]
    s = 0
    v = wid * 1000003 + 1
    while H.running():
        cases = [("send", ch, v) for ch in chans]
        r = runloom.select(cases, default=True)
        if isinstance(r, int):          # -1: every channel full right now
            runloom.sleep(0.0005)
            continue
        s += v
        v += 1
        H.op(wid)
    state["sent_sum"][wid] += s


def consumer(H, wid, rng, state):
    chans = state["chans"]
    s = 0
    while True:
        cases = [("recv", ch) for ch in chans]
        r = runloom.select(cases, default=True)
        if isinstance(r, int):          # -1: nothing ready
            if not H.running():
                break                   # producers stopped + all drained
            runloom.sleep(0.0005)
            continue
        _idx, payload = r
        val, ok = payload
        if ok:
            s += val
    state["recv_sum"][wid] += s


def body(H):
    half = H.funcs // 2
    H.run_pool(half, producer, H.state)
    H.run_pool(H.funcs - half, consumer, H.state)


def post(H):
    sent = sum(H.state["sent_sum"])
    recv = sum(H.state["recv_sum"])
    H.check(sent == recv,
            "select routing lost items: sent={0} received={1}".format(
                sent, recv))
    H.log("sent_sum={0} recv_sum={1}".format(sent, recv))


if __name__ == "__main__":
    harness.main("p54_select_channels", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="Go-like select over N channels; conservation")
