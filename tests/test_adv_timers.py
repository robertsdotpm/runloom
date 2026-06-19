"""Adversarial QA: runloom.time (Timer/Ticker/After/Tick) + runloom.context.

Timer/Ticker prevent stale fires with a generation counter + a _stopped flag;
we attack exactly that: Stop() / Reset() must guarantee the OLD deadline never
fires.  For context we attack the cancel cascade (parent cancels children), the
manual-cancel-vs-deadline error code, multi-waiter broadcast, and double-cancel
idempotency (a naive cancel that close()s the done channel twice would hit
"close on closed channel").
"""
import sys
import time

import pytest

import runloom_c as rc
import runloom.time as rt
import runloom.context as rctx
from adv_util import hang_guard


def _run_single(fn):
    box = {}
    def main():
        box["r"] = fn()
    rc.fiber(main)
    rc.run()
    return box.get("r")


# --------------------------------------------------------------------------
# Timer
# --------------------------------------------------------------------------
def test_timer_fires_once_after_delay():
    def f():
        t0 = time.monotonic()
        t = rt.Timer(0.03)
        v, ok = t.c.recv()
        return ok, time.monotonic() - t0
    with hang_guard(10, "timer fire"):
        ok, el = _run_single(f)
    assert ok is True
    assert 0.02 < el < 0.5


def test_timer_stop_prevents_stale_fire():
    def f():
        t = rt.Timer(0.02)
        assert t.Stop() is True            # stopped before it fired
        assert t.Stop() is False           # already stopped
        rc.sched_sleep(0.06)               # wait well past the original deadline
        return t.c.try_recv()              # must NOT have fired
    with hang_guard(10, "timer stop"):
        assert _run_single(f) is None, "Stop() did not prevent the stale fire"


def test_timer_reset_old_deadline_does_not_fire_early():
    def f():
        t = rt.Timer(0.02)
        t.Reset(0.20)                      # supersede the 20ms deadline with 200ms
        rc.sched_sleep(0.08)               # past the OLD deadline, before the NEW
        early = t.c.try_recv()             # the gen-0 fire must have bailed
        return early
    with hang_guard(10, "timer reset"):
        assert _run_single(f) is None, "Reset() let the old deadline fire early (stale fire)"


# --------------------------------------------------------------------------
# Ticker
# --------------------------------------------------------------------------
def test_ticker_fires_repeatedly_then_stop_halts():
    def f():
        tk = rt.Ticker(0.01)
        got = 0
        for _ in range(3):
            v, ok = tk.c.recv()
            if ok:
                got += 1
        tk.Stop()
        # after Stop, drain: a fire racing the stop may leave <=1 buffered,
        # but no NEW ticks accumulate.
        rc.sched_sleep(0.05)
        leftover = 0
        while tk.c.try_recv() is not None:
            leftover += 1
        return got, leftover
    with hang_guard(10, "ticker"):
        got, leftover = _run_single(f)
    assert got == 3
    assert leftover <= 1, "ticker kept firing after Stop() (leftover=%d)" % leftover


def test_ticker_nonpositive_interval_raises():
    with pytest.raises(ValueError):
        rt.Ticker(0)
    with pytest.raises(ValueError):
        rt.Ticker(-1)


def test_ticker_slow_consumer_drops_backlog_no_block():
    # A buffer-1 ticker with a slow consumer must DROP backlog (Go semantics),
    # never block the ticker fiber or accumulate unboundedly.
    def f():
        tk = rt.Ticker(0.005)
        seen = 0
        t0 = time.monotonic()
        while time.monotonic() - t0 < 0.1:
            v, ok = tk.c.recv()
            if ok:
                seen += 1
            rc.sched_sleep(0.02)           # consumer slower than the ticker
        tk.Stop()
        return seen
    with hang_guard(10, "ticker slow consumer"):
        seen = _run_single(f)
    # ~5 slow reads over 100ms; far fewer than the ~20 ticks produced -> dropped.
    assert 1 <= seen <= 12, "unexpected tick count %d (drop/block broken)" % seen


# --------------------------------------------------------------------------
# After / Tick
# --------------------------------------------------------------------------
def test_after_fires_value_once():
    def f():
        ch = rt.After(0.02)
        return ch.recv()
    with hang_guard(10, "after"):
        v, ok = _run_single(f)
    assert ok is True


# --------------------------------------------------------------------------
# context
# --------------------------------------------------------------------------
def test_context_cancel_closes_done_and_sets_err():
    def f():
        ctx, cancel = rctx.WithCancel(rctx.Background())
        assert ctx.err() is None
        cancel()
        v, ok = ctx.done.recv()            # closed channel -> (None, False)
        return ok, ctx.err()
    with hang_guard(10, "context cancel"):
        ok, err = _run_single(f)
    assert ok is False
    assert err == "cancelled"


def test_context_double_cancel_is_idempotent():
    # A naive cancel that close()s the done channel twice would raise
    # "close on closed channel" on the second call.
    def f():
        ctx, cancel = rctx.WithCancel(rctx.Background())
        cancel()
        cancel()                           # must NOT raise
        cancel()
        return ctx.err()
    with hang_guard(10, "double cancel"):
        assert _run_single(f) == "cancelled"


def test_context_cancel_cascades_to_children():
    def f():
        parent, pcancel = rctx.WithCancel(rctx.Background())
        child, _ccancel = rctx.WithCancel(parent)
        gchild, _gcancel = rctx.WithCancel(child)
        assert child.err() is None and gchild.err() is None
        pcancel()                          # cancel the root
        # the cascade must close the descendants' done channels
        child.done.recv()
        gchild.done.recv()
        return child.err(), gchild.err()
    with hang_guard(10, "context cascade"):
        cerr, gerr = _run_single(f)
    assert cerr == "cancelled" and gerr == "cancelled"


def test_context_timeout_auto_fires_with_deadline_err():
    def f():
        ctx, cancel = rctx.WithTimeout(rctx.Background(), 0.03)
        t0 = time.monotonic()
        ctx.done.recv()
        el = time.monotonic() - t0
        err = ctx.err()
        cancel()
        return el, err
    with hang_guard(10, "context timeout"):
        el, err = _run_single(f)
    assert 0.02 < el < 0.5
    assert err == "deadline_exceeded"


def test_context_manual_cancel_before_deadline_is_cancelled_not_deadline():
    def f():
        ctx, cancel = rctx.WithTimeout(rctx.Background(), 5.0)
        cancel()                           # cancel well before the 5s deadline
        ctx.done.recv()
        return ctx.err()
    with hang_guard(10, "context manual cancel"):
        # cancelled wins over the (never-reached) deadline
        assert _run_single(f) == "cancelled"


def test_context_multiple_waiters_all_woken():
    woke = []
    def f():
        ctx, cancel = rctx.WithCancel(rctx.Background())
        def waiter(i):
            ctx.done.recv()
            woke.append(i)
        for i in range(16):
            rc.fiber(lambda i=i: waiter(i))
        rc.sched_yield()                   # all park on ctx.done
        cancel()
    with hang_guard(15, "context broadcast"):
        rc.fiber(f); rc.run()
    assert len(woke) == 16, "cancel did not wake all %d done-waiters (got %d)" % (16, len(woke))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
