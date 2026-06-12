"""runloom.sync fan-in primitives: WaitGroup / Future / gather.

These ride directly on the GenMC-verified park() / G.wake (wake_safe) handshake
with a runloom_c.Mutex guard held only for O(1) bookkeeping.  park() (NOT park_self,
which busy-spins on an M:N hub) means an awaiter genuinely blocks.  The tests pin
the contract, the goroutine-resolution contract (foreign-thread resolve raises a
clean error, never a SIGSEGV), that an await does not peg a hub, AND -- the failure
mode that matters for a park/wake primitive -- a lost wakeup under repeated high
fan-in across M:N hubs (a hang, caught by the timeout).
"""
import resource
import threading

import pytest

import runloom
import runloom_c
from runloom import sync


def _drive(fn, hubs=8):
    box = [None, None]

    def runner():
        try:
            box[0] = fn()
        except BaseException as e:  # noqa: BLE001
            box[1] = e

    runloom.run(hubs, runner)
    if box[1] is not None:
        raise box[1]
    return box[0]


# ---- WaitGroup ------------------------------------------------------------

def test_waitgroup_waits_for_all():
    def body():
        wg = sync.WaitGroup()
        done = bytearray(100)
        wg.add(100)
        for i in range(100):
            runloom.go(lambda i=i: (done.__setitem__(i, 1), wg.done()))
        wg.wait()
        return sum(done)
    assert _drive(body) == 100


def test_waitgroup_multiple_waiters():
    def body():
        wg = sync.WaitGroup()
        wg.add(3)
        woke = bytearray(10)
        for w in range(10):
            runloom.go(lambda w=w: (wg.wait(), woke.__setitem__(w, 1)))
        runloom.sleep(0.02)
        for _ in range(3):
            wg.done()
        runloom.sleep(0.05)
        return sum(woke)
    assert _drive(body) == 10


def test_waitgroup_reusable():
    def body():
        wg = sync.WaitGroup()
        total = 0
        for _ in range(3):
            wg.add(5)
            c = bytearray(5)
            for i in range(5):
                runloom.go(lambda i=i: (c.__setitem__(i, 1), wg.done()))
            wg.wait()
            total += sum(c)
        return total
    assert _drive(body) == 15


def test_waitgroup_negative_raises():
    def body():
        wg = sync.WaitGroup()
        wg.add(1)
        wg.done()
        with pytest.raises(ValueError):
            wg.done()
        return True
    assert _drive(body)


def test_waitgroup_wait_when_already_zero_returns():
    def body():
        wg = sync.WaitGroup()
        wg.wait()              # count 0 -> immediate
        wg.add(2); wg.done(); wg.done()
        wg.wait()              # back to 0 -> immediate
        return True
    assert _drive(body)


# ---- Future ---------------------------------------------------------------

def test_future_result_and_many_awaiters():
    def body():
        fut = sync.Future()
        got = bytearray(40)
        for i in range(40):
            runloom.go(lambda i=i: got.__setitem__(i, 1 if fut.result() == 7 else 0))
        runloom.sleep(0.02)
        fut.set_result(7)
        runloom.sleep(0.05)
        # a late awaiter returns immediately
        late = fut.result()
        return sum(got), late
    s, late = _drive(body)
    assert s == 40
    assert late == 7


def test_future_exception():
    def body():
        fut = sync.Future()
        fut.set_exception(ValueError("boom"))
        with pytest.raises(ValueError, match="boom"):
            fut.result()
        # a goroutine awaiter also sees it
        seen = []
        def aw():
            try:
                fut.result()
            except ValueError as e:
                seen.append(str(e))
        runloom.go(aw)
        runloom.sleep(0.02)
        return seen
    assert _drive(body) == ["boom"]


def test_future_double_resolve_raises():
    def body():
        fut = sync.Future()
        fut.set_result(1)
        with pytest.raises(RuntimeError):
            fut.set_result(2)
        with pytest.raises(RuntimeError):
            fut.set_exception(ValueError())
        return fut.result()
    assert _drive(body) == 1


# ---- gather ---------------------------------------------------------------

def test_gather_order_and_results():
    def body():
        return sync.gather(lambda: 10, lambda: 20, lambda: 30)
    assert _drive(body) == [10, 20, 30]


def test_gather_empty_and_single():
    def body():
        return sync.gather(), sync.gather(lambda: 99)
    assert _drive(body) == ([], [99])


def test_gather_propagates_first_exception():
    def body():
        def boom():
            raise KeyError("nope")
        with pytest.raises(KeyError):
            sync.gather(lambda: 1, boom, lambda: 3)
        return True
    assert _drive(body)


def test_gather_runs_concurrently():
    def body():
        order = []
        def slow():
            runloom.sleep(0.05); order.append("slow")
            return "s"
        def fast():
            order.append("fast")
            return "f"
        res = sync.gather(slow, fast)
        # fast finished before slow -> they ran concurrently, not serially
        return res, order
    res, order = _drive(body)
    assert res == ["s", "f"]
    assert order == ["fast", "slow"]


# ---- the one that matters: no lost wakeup under repeated M:N fan-in -------

def test_repeated_fanin_no_lost_wakeup():
    """Many rounds of high fan-in WaitGroup + gather across 8 hubs.  A lost wake
    in the park_self/wake_safe handshake or the Mutex hand-off would hang here
    (caught by the suite timeout); a miscount would fail the assert."""
    def body():
        total = 0
        for _ in range(15):
            wg = sync.WaitGroup()
            n = 120
            slots = bytearray(n)
            wg.add(n)
            for i in range(n):
                runloom.go(lambda i=i: (slots.__setitem__(i, 1), wg.done()))
            wg.wait()
            total += sum(slots)
            # interleave a gather round too
            total += sum(sync.gather(*[(lambda k=k: k) for k in range(1)]))
        return total
    assert _drive(body) == 15 * 120


# ---- Phase 2a: park() (not park_self) + goroutine-resolution contract -----

def test_sync_park_is_mn_park():
    # The fan-in primitives must use the M:N park (blocks on a hub), not park_self
    # (busy-spins on a hub).  Pin the export so a regression to park_self is loud.
    assert sync.park is runloom_c.park
    assert sync.park is not runloom_c.park_self


def test_future_resolution_from_foreign_thread_raises():
    """A foreign OS thread resolving a Future gets a clean RuntimeError, NOT a
    SIGSEGV.  The guard (runloom_c.Mutex) wakes a contending goroutine awaiter via
    mn_wake_g, which a foreign thread can't route safely; _resolve rejects the
    foreign caller BEFORE taking the guard (current_g() is a lock-free peek)."""
    def body():
        fut = sync.Future()
        err = []
        def foreign():
            try:
                fut.set_result(1)
            except RuntimeError as e:
                err.append(str(e))
        t = threading.Thread(target=foreign)
        t.start(); t.join()
        # the Future is still unresolved -- a goroutine can resolve it cleanly
        fut.set_result(2)
        return err, fut.result()
    err, val = _drive(body)
    assert len(err) == 1 and "foreign OS thread" in err[0]
    assert val == 2


def test_waitgroup_done_from_foreign_thread_raises():
    """WaitGroup.done()/add(negative) is the WAKE side; a foreign-thread done()
    that would wake a parked waiter raises cleanly instead of crashing.  A positive
    setup add() from a foreign thread is allowed (it never wakes)."""
    def body():
        wg = sync.WaitGroup()
        wg.add(1)
        err = []
        def foreign():
            try:
                wg.done()                       # add(-1): wake side -> must raise
            except RuntimeError as e:
                err.append(str(e))
        t = threading.Thread(target=foreign)
        t.start(); t.join()
        wg.done()                                # proper done from the goroutine
        wg.wait()                                # counter is 0 -> returns
        return err
    err = _drive(body)
    assert len(err) == 1 and "goroutine" in err[0]


def test_future_await_does_not_peg_a_hub():
    """The #4 bug: Future.result looped park_self(), which returns immediately on
    an M:N hub, so the awaiter busy-spun a hub at 100% CPU (sysmon flagged it
    WEDGED).  park() blocks for real.  Measured via user-CPU: a busy-looped hub
    adds ~a full core of utime over the run window; a parked awaiter adds ~none.
    Calibrated against an idle run so the bound tracks the box's idle-hub
    overhead, not a fixed guess (busy-loop diff ~+0.28s, parked ~0)."""
    def utime():
        return resource.getrusage(resource.RUSAGE_SELF).ru_utime

    a = utime()
    runloom.run(8, lambda: runloom.sleep(0.3))   # idle baseline, same window
    base = utime() - a

    def runner():
        fut = sync.Future()
        runloom.go(lambda: fut.result())         # awaiter parks for the window
        runloom.sleep(0.3)
        fut.set_result(1)
        runloom.sleep(0.02)
    a = utime()
    runloom.run(8, runner)
    got = utime() - a

    assert got - base < 0.15, (base, got)        # busy-loop would be ~+0.28
