"""Property-based tests for channel + select semantics (single-thread sched).

These run on the deterministic single-thread scheduler (runloom_c.go +
run), so Hypothesis can explore buffer sizes, value sequences, and
producer/consumer fan-outs while we assert the algebraic invariants Go
channels must satisfy:

  * FIFO          a buffered channel delivers values in send order.
  * CONSERVATION  fan-in/fan-out delivers every value exactly once.
  * CLOSE         after close, a range drains exactly the still-buffered
                  values and then stops; a further recv is (None, False);
                  buffered values out-rank closed-ness.
  * SELECT        select(default=True) returns a ready case with the right
                  value, or -1 iff no case is ready.

The single-thread scheduler is process-global, so each example spawns its
fibers and drains to completion before the next.
"""
import sys

sys.path.insert(0, "src")

import runloom_c
from hypothesis import given, settings, strategies as st

SETTINGS = settings(max_examples=200, deadline=None)

ints = st.integers(min_value=-(10 ** 6), max_value=10 ** 6)


@given(cap=st.integers(min_value=1, max_value=16), values=st.lists(ints, max_size=40))
@SETTINGS
def test_buffered_fifo(cap, values):
    """A single producer/consumer over a buffered channel preserves order
    and loses nothing, for any cap and value sequence."""
    ch = runloom_c.Chan(cap)
    out = []

    def producer():
        for v in values:
            ch.send(v)
        ch.close()

    def consumer():
        for v in ch:
            out.append(v)

    for g in (producer, consumer):
        runloom_c.fiber(g)
    runloom_c.run()
    assert out == values


@given(
    nprod=st.integers(min_value=1, max_value=5),
    ncons=st.integers(min_value=1, max_value=5),
    per=st.integers(min_value=0, max_value=12),
    cap=st.integers(min_value=0, max_value=8),
)
@SETTINGS
def test_fanin_conservation(nprod, ncons, per, cap):
    """N producers, M consumers, one channel: every (pid, seq) token is
    received exactly once regardless of buffering."""
    ch = runloom_c.Chan(cap)
    done = runloom_c.Chan(nprod)
    got = []

    def producer(pid):
        def run():
            for s in range(per):
                ch.send((pid, s))
            done.send(1)
        return run

    def closer():
        for _ in range(nprod):
            done.recv()
        ch.close()

    def consumer():
        for v in ch:
            got.append(v)

    for c in range(ncons):
        runloom_c.fiber(consumer)
    for p in range(nprod):
        runloom_c.fiber(producer(p))
    runloom_c.fiber(closer)
    runloom_c.run()

    expected = sorted((p, s) for p in range(nprod) for s in range(per))
    assert sorted(got) == expected


@given(
    cap=st.integers(min_value=1, max_value=16),
    buffered=st.lists(ints, max_size=16),
    extra_recv=st.integers(min_value=1, max_value=4),
)
@SETTINGS
def test_close_drains_then_stops(cap, buffered, extra_recv):
    """Close on a channel holding `buffered` values: subsequent recvs
    drain exactly those (ok=True, in order), then every further recv is
    (None, False).  Buffered values out-rank closed-ness."""
    buffered = buffered[:cap]               # only what fits without blocking
    ch = runloom_c.Chan(cap)
    out = []

    def runner():
        for v in buffered:
            ch.send(v)
        ch.close()
        # drain the buffer
        for _ in range(len(buffered)):
            v, ok = ch.recv()
            out.append((v, ok))
        # then closed-and-empty
        for _ in range(extra_recv):
            out.append(ch.recv())

    runloom_c.fiber(runner)
    runloom_c.run()

    expect = [(v, True) for v in buffered] + [(None, False)] * extra_recv
    assert out == expect


@given(
    vals=st.lists(st.one_of(st.none(), ints), min_size=1, max_size=4),
)
@SETTINGS
def test_select_default_picks_ready(vals):
    """select(default=True) over N buffered channels: each channel is
    pre-loaded with a value (or left empty for None).  The select returns
    SOME ready case with that channel's value, or -1 iff all are empty."""
    n = len(vals)
    result = {}

    def runner():
        chans = [runloom_c.Chan(1) for _ in range(n)]
        ready = {}
        for i, v in enumerate(vals):
            if v is not None:
                chans[i].send(v)
                ready[i] = v
        r = runloom_c.select([("recv", chans[i]) for i in range(n)], default=True)
        result["r"] = r
        result["ready"] = ready

    runloom_c.fiber(runner)
    runloom_c.run()

    r = result["r"]
    ready = result["ready"]
    if not ready:
        assert r == -1, r
    else:
        idx, (v, ok) = r
        assert idx in ready, (idx, ready)
        assert ok is True and v == ready[idx], (idx, v, ready)


if __name__ == "__main__":
    test_buffered_fifo()
    test_fanin_conservation()
    test_close_drains_then_stops()
    test_select_default_picks_ready()
    print("property tests OK")
