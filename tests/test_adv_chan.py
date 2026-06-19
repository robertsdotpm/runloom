"""Adversarial QA: channels (runloom_c.Chan) + select().

Targets the chan.c state machine and the select tombstone/CAS arbitration --
the area covered by the chan_refflow / chan_refcount / chan PyObject-ref
conservation invariants (LIFECYCLE_INVARIANTS.md tiers 8/10).  We try to:

  * break the close path (send-on-closed, double-close, drain order, wake
    every parked sender AND receiver without a hang);
  * make select() deliver a value to MORE THAN ONE case, or to NO case
    (the CAS arbitration bug class);
  * lose or duplicate a value across an M:N fan-in/fan-out (the cross-hub
    wake path) -- asserted with set-equality, not just a count;
  * leak a PyObject a channel held (refcount conservation) -- asserted with
    weakrefs after a forced gc;
  * turn a rendezvous into a slow return (no cooperative overlap).
"""
import gc
import sys
import weakref

import pytest

import runloom
import runloom_c as rc
from adv_util import hang_guard, assert_faster_than, needs_free_threading


# --------------------------------------------------------------------------
# close / send / recv state machine (single-thread, deterministic)
# --------------------------------------------------------------------------
def _run_single(fn):
    box = {}
    def main():
        box["r"] = fn()
    rc.fiber(main)
    rc.run()
    return box.get("r")


def test_send_on_closed_raises():
    def f():
        ch = rc.Chan(1)
        ch.close()
        with pytest.raises(ValueError):
            ch.send(1)
        return "ok"
    assert _run_single(f) == "ok"


def test_double_close_raises():
    def f():
        ch = rc.Chan(0)
        ch.close()
        with pytest.raises(ValueError):
            ch.close()
        return "ok"
    assert _run_single(f) == "ok"


def test_recv_drains_buffer_in_order_then_sentinel():
    def f():
        ch = rc.Chan(3)
        for v in (10, 20, 30):
            assert ch.try_send(v) is True
        ch.close()
        # Closed channel still drains buffered values IN ORDER, then sentinel.
        return [ch.recv(), ch.recv(), ch.recv(), ch.recv(), ch.recv()]
    out = _run_single(f)
    assert out == [(10, True), (20, True), (30, True), (None, False), (None, False)]


def test_try_send_full_and_try_recv_empty():
    def f():
        ch = rc.Chan(1)
        assert ch.try_send("a") is True
        assert ch.try_send("b") is False          # full
        assert ch.try_recv() == ("a", True)
        assert ch.try_recv() is None              # empty -> None (not a tuple)
        return "ok"
    assert _run_single(f) == "ok"


def test_close_wakes_every_parked_receiver():
    # N receivers park on an empty channel; close() must wake ALL of them with
    # (None, False) -- a lost wake here is a permanent hang (caught by guard).
    N = 64
    results = []
    def main():
        ch = rc.Chan(0)
        for _ in range(N):
            rc.fiber(lambda: results.append(ch.recv()))
        rc.sched_yield()      # let receivers park
        ch.close()
    with hang_guard(20, "close wakes receivers"):
        rc.fiber(main)
        rc.run()
    assert len(results) == N
    assert all(r == (None, False) for r in results)


def test_close_makes_parked_senders_raise():
    # Fill the buffer, park extra senders, then close: every parked sender must
    # raise ValueError (channel closed) -- not hang, not silently succeed.
    N = 32
    outcomes = []
    def main():
        ch = rc.Chan(1)
        ch.try_send("filler")
        def sender(i):
            try:
                ch.send(i)
                outcomes.append(("sent", i))
            except ValueError:
                outcomes.append(("closed", i))
        for i in range(N):
            rc.fiber(lambda i=i: sender(i))
        rc.sched_yield()
        ch.close()
    with hang_guard(20, "close wakes senders"):
        rc.fiber(main)
        rc.run()
    assert len(outcomes) == N
    assert all(o[0] == "closed" for o in outcomes), outcomes


# --------------------------------------------------------------------------
# select() arbitration -- the CAS/tombstone correctness surface
# --------------------------------------------------------------------------
def test_select_argument_validation():
    def f():
        ch = rc.Chan(1)
        with pytest.raises(ValueError):
            rc.select([])                       # needs >= 1 case
        with pytest.raises(ValueError):
            rc.select([("frob", ch)])           # bad op
        with pytest.raises((ValueError, TypeError)):
            rc.select([("send", ch)])           # send needs a value
        assert rc.select([("recv", ch)], default=True) == -1   # nothing ready
        return "ok"
    assert _run_single(f) == "ok"


def test_select_recv_and_send_shapes():
    def f():
        ch = rc.Chan(1)
        ch.try_send(7)
        idx, res = rc.select([("recv", ch)])
        assert idx == 0 and res == (7, True)
        idx, res = rc.select([("send", ch, 99)])     # buffer now empty -> send ok
        assert idx == 0 and res is None
        assert ch.try_recv() == (99, True)
        return "ok"
    assert _run_single(f) == "ok"


def test_select_fires_exactly_one_ready_case():
    # Three channels each hold one value; a single select must consume EXACTLY
    # one of them (the tombstone/CAS arbitration must not double-consume or
    # drop).  After the select, exactly two values remain.
    def f():
        chans = [rc.Chan(1) for _ in range(3)]
        for i, ch in enumerate(chans):
            ch.try_send(i)
        idx, (val, ok) = rc.select([("recv", c) for c in chans])
        assert ok and val == idx
        remaining = [c.try_recv() for c in chans]
        consumed = [r is None for r in remaining]
        assert consumed.count(True) == 1, (idx, remaining)
        assert remaining[idx] is None
        return "ok"
    assert _run_single(f) == "ok"


def test_select_send_full_with_default_does_not_block():
    def f():
        ch = rc.Chan(1)
        ch.try_send("x")                         # full
        # send case not ready (full) + default -> -1, no block, no value lost
        assert rc.select([("send", ch, "y")], default=True) == -1
        assert len(ch) == 1
        return "ok"
    assert _run_single(f) == "ok"


def test_select_blocks_until_one_case_ready_no_busy_spin():
    # A select with no default must PARK (cooperatively), not busy-spin, and
    # wake promptly when a producer makes one case ready.
    order = []
    def main():
        a, b = rc.Chan(0), rc.Chan(0)
        def chooser():
            idx, (val, ok) = rc.select([("recv", a), ("recv", b)])
            order.append(("chose", idx, val))
        def burner():
            for i in range(5):
                order.append(("burn", i))
                rc.sched_yield()
            b.send("hello")          # rendezvous onto case 1
        rc.fiber(chooser)
        rc.fiber(burner)
    with hang_guard(20, "select blocks then wakes"):
        rc.fiber(main)
        rc.run()
    assert ("chose", 1, "hello") in order
    # the burner got to run while the chooser was parked (cooperative overlap)
    assert order.index(("burn", 0)) < order.index(("chose", 1, "hello"))


def test_channel_iteration_stops_on_close():
    def f():
        ch = rc.Chan(4)
        for v in range(4):
            ch.try_send(v)
        ch.close()
        return list(ch)          # iteration consumes until closed+empty
    assert _run_single(f) == [0, 1, 2, 3]


# --------------------------------------------------------------------------
# refcount conservation -- a channel must not leak / UAF the PyObjects it holds
# --------------------------------------------------------------------------
class _Box:
    __slots__ = ("v", "__weakref__")
    def __init__(self, v):
        self.v = v


def test_delivered_objects_are_released_after_drain():
    refs = []
    def f():
        ch = rc.Chan(8)
        objs = [_Box(i) for i in range(8)]
        refs.extend(weakref.ref(o) for o in objs)
        for o in objs:
            ch.try_send(o)
        objs2 = []
        while True:
            v, ok = ch.try_recv() or (None, False)
            if not ok:
                break
            objs2.append(v)
        # drop all strong refs
        objs.clear(); objs2.clear()
        return "ok"
    assert _run_single(f) == "ok"
    gc.collect()
    alive = [r for r in refs if r() is not None]
    assert not alive, "channel leaked %d delivered objects" % len(alive)


def test_close_with_buffered_objects_frees_them():
    # Objects left in the buffer when the channel is closed AND dropped must be
    # released when the channel is freed (chan_refflow: free drains buffer).
    refs = []
    def f():
        ch = rc.Chan(8)
        for i in range(8):
            o = _Box(i)
            refs.append(weakref.ref(o))
            ch.try_send(o)
        ch.close()
        return "ok"        # ch goes out of scope here -> freed
    assert _run_single(f) == "ok"
    gc.collect(); gc.collect()
    alive = [r for r in refs if r() is not None]
    assert not alive, "closed+dropped channel leaked %d buffered objects" % len(alive)


# --------------------------------------------------------------------------
# M:N cross-hub wake path -- the lost/duplicated value class
# --------------------------------------------------------------------------
@pytest.mark.skipif(not needs_free_threading(), reason="M:N needs GIL-disabled build")
def test_mn_fan_in_fan_out_no_dup_no_loss():
    # Each producer emits a UNIQUE tagged value; consumers collect into
    # per-consumer slots.  Set-equality proves NO value was lost and NONE
    # duplicated across the cross-hub handoff (a count alone would miss a
    # lost+duplicated pair that nets out).
    from runloom.sync import WaitGroup
    P, C, PER = 8, 8, 500
    ch = rc.Chan(64)
    collected = [list() for _ in range(C)]

    def main():
        wg = WaitGroup(); wg.add(P)
        def producer(pid):
            try:
                for j in range(PER):
                    ch.send(pid * PER + j)
            finally:
                wg.done()
        def consumer(cid):
            while True:
                v, ok = ch.recv()
                if not ok:
                    break
                collected[cid].append(v)
        for c in range(C):
            rc.mn_fiber(lambda cid=c: consumer(cid))
        for p in range(P):
            rc.mn_fiber(lambda pid=p: producer(pid))
        wg.wait()
        ch.close()

    with hang_guard(60, "mn fan-in/out"):
        runloom.run(4, main)

    got = [v for slot in collected for v in slot]
    expected = set(range(P * PER))
    assert len(got) == len(expected), "lost/dup: got %d want %d" % (len(got), len(expected))
    assert set(got) == expected, "value set mismatch (lost or duplicated)"


@pytest.mark.skipif(not needs_free_threading(), reason="M:N needs GIL-disabled build")
def test_mn_select_across_channels_integrity():
    from runloom.sync import WaitGroup
    K, PER = 6, 400
    chans = [rc.Chan(8) for _ in range(K)]
    sink = []
    sink_mu = rc.Mutex()

    def main():
        wg = WaitGroup(); wg.add(K)
        def producer(i):
            try:
                for j in range(PER):
                    chans[i].send(i * PER + j)
            finally:
                wg.done()
        total = K * PER
        def consumer():
            n = 0
            while n < total:
                idx, (val, ok) = rc.select([("recv", c) for c in chans])
                if ok:
                    sink_mu.lock()
                    try:
                        sink.append(val)
                    finally:
                        sink_mu.unlock()
                    n += 1
        rc.mn_fiber(consumer)
        for i in range(K):
            rc.mn_fiber(lambda i=i: producer(i))
        wg.wait()

    with hang_guard(60, "mn select integrity"):
        runloom.run(3, main)
    assert set(sink) == set(range(K * PER))
    assert len(sink) == K * PER


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
