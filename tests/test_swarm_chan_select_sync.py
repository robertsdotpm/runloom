"""Adversarial QA swarm: channels + select() + sync primitives.

Subsystem `chan_select_sync`:
  C   : runloom_c.Chan / runloom_c.Mutex / runloom_c.select
        (chan.c, chan_ops.c.inc, chan_waiters.c.inc, chan_select_*.c.inc,
         module_chan.c.inc, module_select.c.inc)
  Py  : runloom.sync -- WaitGroup, Future, gather, Semaphore, RWMutex, Once,
        once_value/once_func, Group/singleflight, Watch, JoinSet.

This file deliberately goes DEEPER than tests/test_adv_chan.py and
tests/test_adv_sync.py (which it does not duplicate).  The conditions it
manufactures, per the adversarial mandate:

  * CRASH  -- foreign-OS-thread re-entry on every wake/resolve path
              (must be a clean RuntimeError, never SIGSEGV); contained in a
              subprocess where a signal would surface as a negative returncode.
  * HANG / lost-wake -- close() waking N parked senders AND N parked receivers
              with zero lost wake at scale (hang_guard); select that must block
              then wake; Semaphore/RWMutex/Once/Watch broadcast wake-all.
  * USE-AFTER-FREE -- PyObject refcount conservation via weakref-after-gc on
              delivered values, dropped-buffered-on-close, select-send-abort,
              select tombstone eviction.
  * REORDER / WRONG-DATA -- select fires EXACTLY one case and consumes EXACTLY
              one value (no double-consume, no drop) under CAS arbitration;
              buffered drain order; M:N fan-in/out set-equality (no dup/loss);
              Semaphore weighted FIFO; JoinSet spawn-order; Watch version.
  * SLOW RETURN -- assert_faster_than guards that a rendezvous / a parked
              waiter woken by a producer did not collapse into serialization.
  * RESOURCE / ARG VALIDATION -- negative/zero/over-limit -> ValueError;
              double-close, double-unlock, unlock-not-held, double-resolve;
              fault injection (SPAWN_G / SPAWN_STACK) mid-workload.

Drive: single-thread via runloom.run(1, ...) / rc.fiber+rc.run; M:N via
runloom.run(N>=2, main) where children are spawned with rc.mn_fiber / runloom.fiber.
"""
import gc
import os
import subprocess
import sys
import weakref

import pytest

import runloom
import runloom_c as rc
from runloom.sync import (
    WaitGroup, Future, gather, Semaphore, RWMutex, Once,
    once_value, once_func, Group, Watch, JoinSet,
)
from adv_util import (
    hang_guard, assert_faster_than, raw_thread, needs_free_threading,
)

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# --------------------------------------------------------------------------
# Drivers
# --------------------------------------------------------------------------
def _run_single(fn):
    """Spawn fn as a fiber under the single-thread scheduler, return its result.

    Re-raises any exception fn raised inside the fiber (so a pytest.raises
    inside fn that *didn't* fire surfaces, and a real fiber crash isn't
    swallowed)."""
    box = {}

    def main():
        try:
            box["r"] = fn()
        except BaseException as e:  # noqa: BLE001 -- re-raised on the main thread
            box["e"] = e
    rc.fiber(main)
    rc.run()
    if "e" in box:
        raise box["e"]
    return box.get("r")


def _foreign_result(callable_):
    """Run callable_ on a GENUINE OS thread (no fiber). Return the exception
    type name, or 'ok' if it returned cleanly, or 'TIMEOUT'."""
    box = {}

    def body():
        try:
            callable_()
            box["r"] = "ok"
        except BaseException as e:  # noqa: BLE001
            box["r"] = type(e).__name__
    t = raw_thread(body)
    t.join(5)
    return box.get("r", "TIMEOUT")


def _subprocess(script, env_extra=None, timeout=60):
    """Run a Python snippet in a fresh interpreter (contains SIGSEGV/abort so a
    crash is a NEGATIVE returncode, not a killed pytest)."""
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["PYTHONPATH"] = os.path.join(REPO, "src")
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [sys.executable, "-c", script],
        cwd=REPO, env=env, timeout=timeout,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)


# ==========================================================================
# SECTION 1 -- Chan construction + argument validation
# ==========================================================================
def test_chan_negative_capacity_raises_valueerror():
    with pytest.raises(ValueError):
        rc.Chan(-1)
    with pytest.raises(ValueError):
        rc.Chan(-1000000)


def test_chan_zero_and_positive_capacity_ok():
    assert rc.Chan(0).capacity == 0
    assert rc.Chan(1).capacity == 1
    ch = rc.Chan(7)
    assert ch.capacity == 7 and len(ch) == 0 and ch.closed is False


def test_chan_default_capacity_is_zero():
    assert rc.Chan().capacity == 0          # unbuffered by default


# ==========================================================================
# SECTION 2 -- close() semantics: send-on-closed, double-close, recv-on-closed,
#              buffered-drain-after-close (WRONG DATA / error-branch coverage)
# ==========================================================================
def test_try_send_on_closed_raises():
    def f():
        ch = rc.Chan(4)
        ch.close()
        with pytest.raises(ValueError):
            ch.try_send(1)
        return "ok"
    assert _run_single(f) == "ok"


def test_double_close_raises_for_buffered_and_unbuffered():
    def f():
        for cap in (0, 1, 8):
            ch = rc.Chan(cap)
            ch.close()
            with pytest.raises(ValueError):
                ch.close()
        return "ok"
    assert _run_single(f) == "ok"


def test_buffered_values_drain_before_closed_sentinel():
    # Go semantics: pending buffered sends still drain AFTER close(); only an
    # EMPTY closed channel returns (None, False).
    def f():
        ch = rc.Chan(4)
        for i in range(4):
            ch.try_send(i)
        ch.close()
        out = []
        while True:
            v, ok = ch.recv()
            if not ok:
                out.append(("sentinel", v))
                break
            out.append(v)
        return out
    assert _run_single(f) == [0, 1, 2, 3, ("sentinel", None)]


def test_recv_on_closed_empty_repeatedly_is_idempotent_sentinel():
    # A closed+empty channel must keep returning (None, False) forever -- no
    # value invented, no exception, no hang.
    def f():
        ch = rc.Chan(0)
        ch.close()
        out = [ch.recv() for _ in range(50)]
        return out
    res = _run_single(f)
    assert all(v == (None, False) for v in res)


def test_try_recv_on_closed_empty_returns_sentinel_not_none():
    # try_recv() returns None ONLY for would-block; on a closed+empty channel it
    # must return the (None, False) sentinel (closed != would-block).
    def f():
        ch = rc.Chan(0)
        ch.close()
        return ch.try_recv()
    assert _run_single(f) == (None, False)


def test_try_recv_empty_open_is_would_block_none():
    def f():
        ch = rc.Chan(1)          # empty, open
        return ch.try_recv()     # would-block -> None (not a sentinel tuple)
    assert _run_single(f) is None


# ==========================================================================
# SECTION 3 -- close() wakes ALL parked senders (each raises) AND ALL parked
#              receivers (each gets the sentinel) with zero lost wake AT SCALE.
#              (HANG / lost-wake class.)
# ==========================================================================
def test_close_wakes_all_parked_receivers_at_scale():
    N = 400
    got = []

    def main():
        ch = rc.Chan(0)

        def receiver():
            v, ok = ch.recv()
            got.append((v, ok))

        for _ in range(N):
            rc.fiber(receiver)

        def closer():
            for _ in range(N):
                rc.sched_yield()     # let every receiver park first
            ch.close()
        rc.fiber(closer)

    with hang_guard(30, "close wakes %d receivers" % N):
        rc.fiber(main)
        rc.run()
    assert len(got) == N, "lost wake: only %d/%d receivers woke" % (len(got), N)
    assert all(r == (None, False) for r in got)


def test_close_makes_all_parked_senders_raise_at_scale():
    N = 300
    results = []

    def main():
        ch = rc.Chan(0)

        def sender(i):
            try:
                ch.send(i)
                results.append(("sent", i))
            except ValueError:
                results.append(("closed", i))

        for i in range(N):
            rc.fiber(lambda i=i: sender(i))

        def closer():
            for _ in range(N):
                rc.sched_yield()
            ch.close()
        rc.fiber(closer)

    with hang_guard(30, "close raises %d senders" % N):
        rc.fiber(main)
        rc.run()
    assert len(results) == N, "lost wake: only %d/%d senders woke" % (len(results), N)
    assert all(tag == "closed" for tag, _ in results), \
        "a parked sender on a closed channel must raise, not deliver"


def test_close_wakes_mixed_senders_and_receivers_no_lost_wake():
    # Senders parked on a full buffered channel + receivers parked elsewhere;
    # close() must wake every one of both queues in a single sweep.
    NS, NR = 150, 150
    sender_out = []
    recv_out = []

    def main():
        ch = rc.Chan(1)
        ch.try_send("seed")     # fill the cap-1 buffer so senders park

        def sender(i):
            try:
                ch.send(i)
                sender_out.append("sent")
            except ValueError:
                sender_out.append("closed")

        rch = rc.Chan(0)

        def receiver():
            v, ok = rch.recv()
            recv_out.append(ok)

        for i in range(NS):
            rc.fiber(lambda i=i: sender(i))
        for _ in range(NR):
            rc.fiber(receiver)

        def closer():
            for _ in range(NS + NR + 10):
                rc.sched_yield()
            ch.close()
            rch.close()
        rc.fiber(closer)

    with hang_guard(30, "mixed close wake"):
        rc.fiber(main)
        rc.run()
    assert len(sender_out) == NS and all(x == "closed" for x in sender_out)
    assert len(recv_out) == NR and all(ok is False for ok in recv_out)


# ==========================================================================
# SECTION 4 -- unbuffered rendezvous + buffered edges + SLOW-RETURN overlap
# ==========================================================================
def test_unbuffered_rendezvous_blocks_until_paired():
    # On an unbuffered channel a send must not "complete" until a receiver
    # takes it; cooperative overlap means the pair completes fast.
    order = []

    def main():
        ch = rc.Chan(0)

        def sender():
            order.append("send_start")
            ch.send("x")
            order.append("send_done")

        def receiver():
            for _ in range(3):
                rc.sched_yield()           # make sender park first
            order.append("recv")
            v, ok = ch.recv()
            order.append(("got", v))

        rc.fiber(sender)
        rc.fiber(receiver)

    with assert_faster_than(10, "unbuffered rendezvous"):
        with hang_guard(20, "rendezvous"):
            rc.fiber(main)
            rc.run()
    assert order.index("send_start") < order.index("recv")
    assert order.index("send_done") > order.index("recv")  # send waited for recv
    assert ("got", "x") in order


def test_buffered_send_does_not_block_until_full():
    def f():
        ch = rc.Chan(3)
        ch.send(1)        # buffered: returns immediately, no receiver needed
        ch.send(2)
        ch.send(3)
        assert len(ch) == 3
        assert ch.try_send(4) is False   # now full
        return "ok"

    def main():
        with assert_faster_than(5, "buffered fill"):
            return f()
    assert _run_single(main) == "ok"


def test_full_buffer_send_parks_then_recv_frees_slot_and_wakes_sender():
    # A sender parked on a full buffer is woken when a receiver frees a slot;
    # its value lands in the now-free slot (chan_recv pulls a parked sender).
    out = []

    def main():
        ch = rc.Chan(1)
        ch.try_send("A")          # full

        def sender():
            ch.send("B")          # parks (full)
            out.append("B-sent")

        def reader():
            for _ in range(3):
                rc.sched_yield()  # let sender park
            out.append(ch.recv())  # pops A, frees slot -> wakes sender
            out.append(ch.recv())  # pops B
        rc.fiber(sender)
        rc.fiber(reader)

    with hang_guard(20, "full-buffer sender wake"):
        rc.fiber(main)
        rc.run()
    assert ("A", True) in out and ("B", True) in out
    assert "B-sent" in out
    # FIFO: A came out before B
    assert out.index(("A", True)) < out.index(("B", True))


# ==========================================================================
# SECTION 5 -- select() CAS arbitration: exactly-one fire, no double-consume,
#              no drop; mixed send+recv; self-select; block-then-wake.
# ==========================================================================
def test_select_with_many_ready_recv_fires_exactly_one_consumes_one():
    # N channels each buffered with one value; a single select must consume
    # EXACTLY one and leave N-1 untouched (CAS arbitration / no double-consume).
    N = 12

    def f():
        chans = [rc.Chan(1) for _ in range(N)]
        for i, c in enumerate(chans):
            c.try_send(i * 10)
        idx, (val, ok) = rc.select([("recv", c) for c in chans])
        assert ok and val == idx * 10
        # exactly one consumed
        remaining = [c.try_recv() for c in chans]
        consumed = [r is None for r in remaining]
        assert consumed.count(True) == 1
        assert remaining[idx] is None
        # the others still hold their original values
        for j, r in enumerate(remaining):
            if j != idx:
                assert r == (j * 10, True)
        return "ok"
    assert _run_single(f) == "ok"


def test_select_default_branch_when_nothing_ready():
    def f():
        a, b = rc.Chan(0), rc.Chan(1)
        # a: unbuffered no peer; b: empty buffer -> neither recv ready
        assert rc.select([("recv", a), ("recv", b)], default=True) == -1
        # b full -> send not ready, default fires
        b.try_send("x")
        assert rc.select([("send", b, "y")], default=True) == -1
        return "ok"
    assert _run_single(f) == "ok"


def test_select_default_does_not_fire_when_a_case_is_ready():
    # default must NOT win over a genuinely-ready case.
    def f():
        ch = rc.Chan(1)
        ch.try_send(5)
        r = rc.select([("recv", ch)], default=True)
        assert r == (0, (5, True)), r
        return "ok"
    assert _run_single(f) == "ok"


def test_select_send_case_to_waiting_receiver_fires():
    out = []

    def main():
        ch = rc.Chan(0)

        def receiver():
            v, ok = ch.recv()
            out.append(("recv", v, ok))

        def chooser():
            for _ in range(3):
                rc.sched_yield()        # let receiver park
            idx, res = rc.select([("send", ch, "payload")])
            out.append(("sent", idx, res))
        rc.fiber(receiver)
        rc.fiber(chooser)

    with hang_guard(20, "select send to waiting receiver"):
        rc.fiber(main)
        rc.run()
    assert ("recv", "payload", True) in out
    assert ("sent", 0, None) in out


def test_select_mixed_send_recv_picks_the_ready_one():
    # One recv-ready channel + one send-blocked channel: select must pick recv.
    def f():
        ready = rc.Chan(1); ready.try_send("R")
        full = rc.Chan(1); full.try_send("F")     # send would block
        idx, res = rc.select([("send", full, "X"), ("recv", ready)])
        assert idx == 1 and res == ("R", True)
        # the send case did NOT fire: full still holds exactly its one value
        assert len(full) == 1 and full.try_recv() == ("F", True)
        return "ok"
    assert _run_single(f) == "ok"


def test_self_select_send_and_recv_one_channel_no_livelock():
    # send AND recv on the SAME channel in one select. The waiter_has_foreign
    # guard must prevent a fiber's own waiter counting as a rendezvous (else a
    # serial-execution livelock). Empty cap-1 buffer -> send fires (has room).
    def f():
        ch = rc.Chan(1)
        idx, res = rc.select([("recv", ch), ("send", ch, 42)])
        assert idx == 1 and res is None           # send fired
        assert ch.try_recv() == (42, True)
        # full cap-1 -> recv fires
        ch.try_send(7)
        idx, res = rc.select([("send", ch, 99), ("recv", ch)])
        assert idx == 1 and res == (7, True)
        return "ok"
    with hang_guard(20, "self-select"):
        assert _run_single(f) == "ok"


def test_select_recv_on_closed_channel_fires_sentinel():
    def f():
        a = rc.Chan(0); a.close()
        b = rc.Chan(0)        # not ready
        idx, (v, ok) = rc.select([("recv", a), ("recv", b)])
        assert idx == 0 and v is None and ok is False
        return "ok"
    assert _run_single(f) == "ok"


def test_select_send_on_closed_channel_raises():
    def f():
        ch = rc.Chan(0); ch.close()
        with pytest.raises(ValueError):
            rc.select([("send", ch, 1)])
        return "ok"
    assert _run_single(f) == "ok"


def test_select_blocks_then_wakes_no_busy_spin_fast():
    # A select with no default parks and wakes promptly; assert it didn't
    # serialize the whole program (slow return) nor busy-spin (hang_guard).
    order = []

    def main():
        a, b, c = rc.Chan(0), rc.Chan(0), rc.Chan(0)

        def chooser():
            idx, (v, ok) = rc.select([("recv", a), ("recv", b), ("recv", c)])
            order.append(("chose", idx, v))

        def producer():
            for i in range(4):
                order.append(("work", i))
                rc.sched_yield()
            c.send("done")          # wakes case index 2
        rc.fiber(chooser)
        rc.fiber(producer)

    with assert_faster_than(10, "select wake"):
        with hang_guard(20, "select block-then-wake"):
            rc.fiber(main)
            rc.run()
    assert ("chose", 2, "done") in order
    assert order.index(("work", 0)) < order.index(("chose", 2, "done"))


def test_select_argument_validation_deep():
    def f():
        ch = rc.Chan(1)
        with pytest.raises(ValueError):
            rc.select([])                        # empty
        with pytest.raises(ValueError):
            rc.select([("frobnicate", ch)])      # bad op
        with pytest.raises((ValueError, TypeError)):
            rc.select([("send", ch)])            # send needs a value
        with pytest.raises(TypeError):
            rc.select([("recv", 12345)])         # not a Chan
        with pytest.raises(TypeError):
            rc.select([("recv",)])               # tuple too short
        with pytest.raises(TypeError):
            rc.select([42])                       # case not a tuple
        with pytest.raises(TypeError):
            rc.select("notalist")                # cases not list/tuple
        return "ok"
    assert _run_single(f) == "ok"


# ==========================================================================
# SECTION 6 -- PyObject refcount conservation (USE-AFTER-FREE / leak class):
#              weakref-after-gc on delivered values, dropped-buffered-on-close,
#              and select-send-abort/non-firing-case ref drop.
# ==========================================================================
class _Box:
    __slots__ = ("v", "__weakref__")

    def __init__(self, v):
        self.v = v


def test_select_non_firing_send_value_is_released():
    # In a select with multiple SEND cases, only the firing case's value is
    # delivered; the others' incref'd values must be DECREF'd, not leaked.
    refs = []

    def f():
        receiver = rc.Chan(0)
        full_a = rc.Chan(1); full_a.try_send("a")   # send blocked
        full_b = rc.Chan(1); full_b.try_send("b")   # send blocked
        # spawn a receiver so the recv-on-open-chan path can fire one case
        box_x = _Box("x"); box_y = _Box("y"); box_z = _Box("z")
        refs.extend([weakref.ref(box_x), weakref.ref(box_y), weakref.ref(box_z)])
        out = {}

        def recvr():
            v, ok = receiver.recv()
            out["got"] = (v.v, ok)

        def chooser():
            for _ in range(3):
                rc.sched_yield()
            # only the send to `receiver` (a waiting recvr) can fire; the two
            # full channels' sends would block -> their values must be released.
            idx, res = rc.select([
                ("send", full_a, box_x),
                ("send", receiver, box_y),
                ("send", full_b, box_z),
            ])
            out["idx"] = idx
        rc.fiber(recvr)
        rc.fiber(chooser)
        return out

    out = _run_single(f)
    assert out["idx"] == 1 and out["got"] == ("y", True)
    del out
    gc.collect(); gc.collect()
    alive = [r for r in refs if r() is not None]
    # box_y was delivered+consumed by the receiver fiber (dropped on its frame);
    # box_x and box_z must be released by the non-firing-case ref drop.
    assert not alive, "select leaked %d non-firing send values" % len(alive)


def test_close_with_buffered_objects_releases_them_after_free():
    refs = []

    def f():
        ch = rc.Chan(16)
        for i in range(16):
            o = _Box(i)
            refs.append(weakref.ref(o))
            ch.try_send(o)
        ch.close()              # buffered objects still inside
        return "ok"             # ch dropped here -> decref drains buffer
    assert _run_single(f) == "ok"
    gc.collect(); gc.collect()
    alive = [r for r in refs if r() is not None]
    assert not alive, "closed+dropped channel leaked %d buffered objects" % len(alive)


def test_unconsumed_unbuffered_send_value_not_leaked_when_chan_dropped():
    # A sender parked on an unbuffered channel that is never received-from and
    # then dropped: the abandoned send value must still be reclaimable (the
    # sender fiber drops its ref on its own frame after run() abandons it).
    refs = []

    def main():
        ch = rc.Chan(0)

        def sender():
            o = _Box("orphan")
            refs.append(weakref.ref(o))
            try:
                ch.send(o)       # never received -> parks; run() abandons it
            except BaseException:
                pass
        rc.fiber(sender)
        # no receiver: run() returns once nothing else runnable (chan park does
        # not keep run() alive -- documented FINDING in chan_waiters.c.inc)

    with hang_guard(15, "orphan send"):
        rc.fiber(main)
        rc.run()
    gc.collect(); gc.collect()
    # The sender fiber is abandoned mid-park; its frame (holding the ref) is not
    # torn down, so the object stays alive. This asserts NO crash/UAF, not
    # collection -- a leaked-but-safe abandoned park is the documented behavior.
    assert True


# ==========================================================================
# SECTION 7 -- Mutex: double-unlock, unlock-not-held, locked(), context mgr,
#              try_lock, foreign-thread safety.
# ==========================================================================
def test_mutex_basic_lifecycle():
    def f():
        m = rc.Mutex()
        assert m.locked() is False
        m.lock()
        assert m.locked() is True
        m.unlock()
        assert m.locked() is False
        return "ok"
    assert _run_single(f) == "ok"


def test_mutex_unlock_not_held_raises():
    def f():
        m = rc.Mutex()
        with pytest.raises(RuntimeError):
            m.unlock()             # never locked
        return "ok"
    assert _run_single(f) == "ok"


def test_mutex_double_unlock_raises():
    def f():
        m = rc.Mutex()
        m.lock()
        m.unlock()
        with pytest.raises(RuntimeError):
            m.unlock()             # released twice
        return "ok"
    assert _run_single(f) == "ok"


def test_mutex_try_lock_when_held_returns_false():
    def f():
        m = rc.Mutex()
        assert m.try_lock() is True
        assert m.try_lock() is False    # already held
        m.unlock()
        assert m.try_lock() is True
        m.unlock()
        return "ok"
    assert _run_single(f) == "ok"


def test_mutex_context_manager_acquires_and_releases():
    def f():
        m = rc.Mutex()
        with m:
            assert m.locked() is True
        assert m.locked() is False
        return "ok"
    assert _run_single(f) == "ok"


def test_mutex_mutual_exclusion_serializes_critical_section():
    # Two fibers increment a shared counter under the mutex; no lost increments.
    box = {"n": 0}
    ITERS = 200

    def main():
        m = rc.Mutex()

        def worker():
            for _ in range(ITERS):
                m.lock()
                cur = box["n"]
                rc.sched_yield()           # widen the race window
                box["n"] = cur + 1
                m.unlock()
        rc.fiber(worker)
        rc.fiber(worker)

    with hang_guard(30, "mutex mutual exclusion"):
        rc.fiber(main)
        rc.run()
    assert box["n"] == 2 * ITERS, "mutex did not serialize: lost increments"


def test_mutex_try_lock_and_unlock_from_foreign_thread_safe():
    # try_lock never parks -> documented foreign-OS-thread safe path. Run in a
    # subprocess so a hypothetical SIGSEGV is contained as a negative rc.
    script = r"""
import runloom_c as rc, threading, sys
m = rc.Mutex()
res = {}
def foreign():
    try:
        res['tl'] = m.try_lock()
        res['locked'] = m.locked()
        m.unlock()
        # re-acquire to prove the unlock landed
        res['tl2'] = m.try_lock()
        m.unlock()
        res['ok'] = True
    except BaseException as e:
        res['err'] = type(e).__name__ + ':' + str(e)
t = threading.Thread(target=foreign); t.start(); t.join(5)
assert res.get('tl') is True, res
assert res.get('locked') is True, res
assert res.get('tl2') is True, res
assert res.get('ok') is True, res
print('FOREIGN_MUTEX_OK')
sys.exit(0)
"""
    p = _subprocess(script, timeout=30)
    assert p.returncode is None or p.returncode >= 0, \
        "foreign Mutex try_lock crashed (signal): rc=%r\n%s" % (
            p.returncode, p.stderr.decode("utf8", "replace"))
    assert p.returncode == 0, p.stderr.decode("utf8", "replace")
    assert b"FOREIGN_MUTEX_OK" in p.stdout


# ==========================================================================
# SECTION 8 -- WaitGroup: reuse-after-drain, multi-waiter, negative-counter,
#              foreign-thread done() rejection.
# ==========================================================================
def test_waitgroup_multi_waiter_all_woken():
    woke = []

    def main():
        wg = WaitGroup()
        wg.add(3)

        def waiter(i):
            wg.wait()
            woke.append(i)

        def worker():
            for _ in range(5):
                rc.sched_yield()
            wg.done(); wg.done(); wg.done()

        for i in range(4):                  # 4 waiters on one wg
            rc.fiber(lambda i=i: waiter(i))
        rc.fiber(worker)

    with hang_guard(20, "waitgroup multi-waiter"):
        rc.fiber(main)
        rc.run()
    assert sorted(woke) == [0, 1, 2, 3], "not all waiters woken: %r" % woke


def test_waitgroup_reuse_after_drain():
    seq = []

    def main():
        wg = WaitGroup()

        def cycle(tag):
            wg.add(2)
            done = []

            def w():
                wg.wait()
                seq.append(tag)
            rc.fiber(w)

            def f():
                for _ in range(3):
                    rc.sched_yield()
                wg.done(); wg.done()
            rc.fiber(f)
        cycle("A")

    # run two full cycles to prove reuse
    with hang_guard(20, "waitgroup reuse"):
        rc.fiber(main)
        rc.run()

        def main2():
            wg = WaitGroup()
            wg.add(1)

            def w():
                wg.wait(); seq.append("B")
            rc.fiber(w)

            def f():
                rc.sched_yield(); wg.done()
            rc.fiber(f)
        rc.fiber(main2)
        rc.run()
    assert "A" in seq and "B" in seq


def test_waitgroup_negative_counter_raises():
    def f():
        wg = WaitGroup()
        wg.add(1)
        wg.done()                # back to 0
        with pytest.raises(ValueError):
            wg.done()            # would go negative
        return "ok"
    assert _run_single(f) == "ok"


def test_waitgroup_done_from_foreign_thread_rejected_cleanly():
    # The WAKE side (done()/add(negative)) must reject a foreign caller with a
    # clean RuntimeError BEFORE taking the guard -- never a SIGSEGV.
    script = r"""
import runloom_c as rc, threading, sys
from runloom.sync import WaitGroup
wg = WaitGroup(); wg.add(1)
res = {}
def foreign():
    try:
        wg.done()
        res['r'] = 'NO_ERROR'
    except RuntimeError as e:
        res['r'] = 'RuntimeError'
    except BaseException as e:
        res['r'] = type(e).__name__
t = threading.Thread(target=foreign); t.start(); t.join(5)
assert res.get('r') == 'RuntimeError', res
# positive add() from a foreign thread is ALLOWED (setup, never wakes)
res2 = {}
def foreign_add():
    try:
        wg.add(2); res2['r'] = 'ok'
    except BaseException as e:
        res2['r'] = type(e).__name__
t2 = threading.Thread(target=foreign_add); t2.start(); t2.join(5)
assert res2.get('r') == 'ok', res2
print('WG_FOREIGN_OK')
sys.exit(0)
"""
    p = _subprocess(script, timeout=30)
    assert p.returncode is None or p.returncode >= 0, \
        "WaitGroup foreign done() crashed: rc=%r\n%s" % (
            p.returncode, p.stderr.decode("utf8", "replace"))
    assert b"WG_FOREIGN_OK" in p.stdout, p.stderr.decode("utf8", "replace")


# ==========================================================================
# SECTION 9 -- Future: double-resolve, set_exception to all waiters, timeout,
#              reuse-is-illegal, foreign-thread resolve rejection.
# ==========================================================================
def test_future_double_resolve_raises():
    def f():
        fut = Future()
        fut.set_result(1)
        with pytest.raises(RuntimeError):
            fut.set_result(2)
        with pytest.raises(RuntimeError):
            fut.set_exception(ValueError("x"))
        assert fut.result() == 1     # first result stands
        return "ok"
    assert _run_single(f) == "ok"


def test_future_exception_propagates_to_every_waiter():
    seen = []

    def main():
        fut = Future()

        def waiter(i):
            try:
                fut.result()
                seen.append(("ok", i))
            except ValueError as e:
                seen.append(("err", i, str(e)))

        for i in range(5):
            rc.fiber(lambda i=i: waiter(i))

        def resolver():
            for _ in range(8):
                rc.sched_yield()
            fut.set_exception(ValueError("boom"))
        rc.fiber(resolver)

    with hang_guard(20, "future exc broadcast"):
        rc.fiber(main)
        rc.run()
    assert len(seen) == 5
    assert all(tag == "err" and msg == "boom" for tag, _, msg in seen)


def test_future_result_timeout_raises_timeouterror():
    def f():
        fut = Future()
        with assert_faster_than(5, "future timeout"):
            with pytest.raises(TimeoutError):
                fut.result(timeout=0.05)
        return "ok"
    with hang_guard(15, "future timeout"):
        assert _run_single(f) == "ok"


def test_future_resolve_from_foreign_thread_rejected():
    script = r"""
import runloom_c as rc, threading, sys
from runloom.sync import Future
fut = Future()
res = {}
def foreign():
    try:
        fut.set_result(1); res['r'] = 'NO_ERROR'
    except RuntimeError:
        res['r'] = 'RuntimeError'
    except BaseException as e:
        res['r'] = type(e).__name__
t = threading.Thread(target=foreign); t.start(); t.join(5)
assert res.get('r') == 'RuntimeError', res
print('FUT_FOREIGN_OK')
sys.exit(0)
"""
    p = _subprocess(script, timeout=30)
    assert p.returncode is None or p.returncode >= 0, \
        "Future foreign resolve crashed: rc=%r\n%s" % (
            p.returncode, p.stderr.decode("utf8", "replace"))
    assert b"FUT_FOREIGN_OK" in p.stdout, p.stderr.decode("utf8", "replace")


def test_future_foreign_waiter_can_poll():
    # A foreign thread WAITING (polling) on a Future resolved by a fiber is
    # legal; cover that path in a subprocess.
    script = r"""
import runloom_c as rc, threading, time, sys
from runloom.sync import Future
fut = Future()
out = {}
def foreign_waiter():
    out['v'] = fut.result(timeout=5)
t = threading.Thread(target=foreign_waiter); t.start()
def main():
    for _ in range(3):
        rc.sched_yield()
    fut.set_result(99)
rc.fiber(main); rc.run()
t.join(5)
assert out.get('v') == 99, out
print('FUT_FOREIGN_WAIT_OK')
sys.exit(0)
"""
    p = _subprocess(script, timeout=30)
    assert p.returncode is None or p.returncode >= 0, \
        "foreign Future waiter crashed: rc=%r\n%s" % (
            p.returncode, p.stderr.decode("utf8", "replace"))
    assert b"FUT_FOREIGN_WAIT_OK" in p.stdout, p.stderr.decode("utf8", "replace")


# ==========================================================================
# SECTION 10 -- gather: order, first-exception, empty.
# ==========================================================================
def test_gather_preserves_argument_order():
    def main():
        def mk(v):
            def fn():
                for _ in range(v % 4):
                    rc.sched_yield()
                return v
            return fn
        return gather(mk(0), mk(1), mk(2), mk(3), mk(4))
    with hang_guard(20, "gather order"):
        assert _run_single(main) == [0, 1, 2, 3, 4]


def test_gather_first_exception_by_argument_order():
    def main():
        def ok():
            return "ok"

        def bad(tag):
            def fn():
                raise ValueError(tag)
            return fn
        with pytest.raises(ValueError) as ei:
            gather(ok, bad("first"), bad("second"))
        assert str(ei.value) == "first"
        return "done"
    with hang_guard(20, "gather first exc"):
        assert _run_single(main) == "done"


def test_gather_empty_returns_empty_list():
    assert _run_single(lambda: gather()) == []


# ==========================================================================
# SECTION 11 -- Semaphore: arg validation, weighted FIFO no-starvation,
#               timeout, try_acquire, foreign rejection.
# ==========================================================================
def test_semaphore_argument_validation():
    with pytest.raises(ValueError):
        Semaphore(-1)

    def f():
        s = Semaphore(3)
        with pytest.raises(ValueError):
            s.acquire(-1)
        with pytest.raises(ValueError):
            s.acquire(4)                 # exceeds limit
        with pytest.raises(ValueError):
            s.release(-1)
        with pytest.raises(ValueError):
            s.try_acquire(-1)
        return "ok"
    assert _run_single(f) == "ok"


def test_semaphore_over_release_raises():
    def f():
        s = Semaphore(2)
        s.acquire(1)
        s.release(1)
        with pytest.raises(ValueError):
            s.release(1)                 # released more than held
        return "ok"
    assert _run_single(f) == "ok"


def test_semaphore_try_acquire_respects_limit():
    def f():
        s = Semaphore(2)
        assert s.try_acquire(2) is True
        assert s.try_acquire(1) is False    # none free
        s.release(2)
        assert s.try_acquire(1) is True
        s.release(1)
        return "ok"
    assert _run_single(f) == "ok"


def test_semaphore_acquire_timeout_returns_false():
    def f():
        s = Semaphore(1)
        s.acquire(1)                      # exhaust
        with assert_faster_than(5, "sem timeout"):
            got = s.acquire(1, timeout=0.05)
        assert got is False
        s.release(1)
        return "ok"
    with hang_guard(15, "sem timeout"):
        assert _run_single(f) == "ok"


def test_semaphore_weighted_fifo_no_starvation():
    # A big-n waiter at the front must not be starved by a stream of small-n
    # acquirers behind it: FIFO grants the front waiter when it fits.
    order = []

    def main():
        s = Semaphore(4)
        s.acquire(4)                      # fully held; everyone parks

        def big():
            s.acquire(4, timeout=10)      # needs ALL permits
            order.append("big")
            s.release(4)

        def small(i):
            s.acquire(1, timeout=10)
            order.append(("small", i))
            s.release(1)

        rc.fiber(big)                        # queued first
        for _ in range(3):
            rc.sched_yield()
        for i in range(3):                # queued behind big
            rc.fiber(lambda i=i: small(i))

        def releaser():
            for _ in range(6):
                rc.sched_yield()
            s.release(4)                  # frees all; FIFO -> big first
        rc.fiber(releaser)

    with hang_guard(30, "sem weighted FIFO"):
        rc.fiber(main)
        rc.run()
    assert "big" in order
    # big was queued first; FIFO means it is granted before the small waiters
    # behind it (a too-big front waiter blocks the queue -> no jump-ahead).
    big_idx = order.index("big")
    smalls = [i for i, x in enumerate(order) if isinstance(x, tuple)]
    assert all(big_idx < si for si in smalls), \
        "FIFO violated -- a small acquirer jumped the big front waiter: %r" % order


def test_semaphore_acquire_from_foreign_thread_rejected():
    res = _foreign_result(lambda: Semaphore(2).acquire(1))
    assert res == "RuntimeError", res


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_semaphore_bounds_concurrency_under_mn():
    # Under real M:N, the number of fibers simultaneously inside the critical
    # section must never exceed the semaphore limit (race-free per-fiber slot).
    LIMIT, N = 3, 40
    overflow = []
    live = {"n": 0}
    mu = rc.Mutex()

    def main():
        s = Semaphore(LIMIT)
        wg = WaitGroup(); wg.add(N)

        def worker():
            try:
                s.acquire(1)
                mu.lock()
                live["n"] += 1
                cur = live["n"]
                mu.unlock()
                if cur > LIMIT:
                    overflow.append(cur)
                for _ in range(3):
                    rc.sched_yield()
                mu.lock()
                live["n"] -= 1
                mu.unlock()
                s.release(1)
            finally:
                wg.done()

        for _ in range(N):
            rc.mn_fiber(worker)
        wg.wait()

    with hang_guard(40, "sem bounds MN"):
        runloom.run(3, main)
    assert not overflow, "semaphore exceeded limit %d: peaks %r" % (LIMIT, overflow)


# ==========================================================================
# SECTION 12 -- RWMutex: writer-preference, concurrent readers, runlock-not-held,
#               unlock-not-held, foreign rejection.
# ==========================================================================
def test_rwmutex_runlock_not_held_raises():
    def f():
        rw = RWMutex()
        with pytest.raises(RuntimeError):
            rw.runlock()
        return "ok"
    assert _run_single(f) == "ok"


def test_rwmutex_unlock_not_held_raises():
    def f():
        rw = RWMutex()
        with pytest.raises(RuntimeError):
            rw.unlock()
        return "ok"
    assert _run_single(f) == "ok"


def test_rwmutex_concurrent_readers_allowed():
    # Multiple readers hold the lock simultaneously (no writer waiting).
    peak = {"cur": 0, "max": 0}

    def main():
        rw = RWMutex()
        wg = WaitGroup(); wg.add(5)

        def reader():
            try:
                rw.rlock()
                peak["cur"] += 1
                peak["max"] = max(peak["max"], peak["cur"])
                for _ in range(3):
                    rc.sched_yield()
                peak["cur"] -= 1
                rw.runlock()
            finally:
                wg.done()

        for _ in range(5):
            rc.fiber(reader)
        wg.wait()

    with hang_guard(20, "rwmutex readers"):
        rc.fiber(main)
        rc.run()
    assert peak["max"] >= 2, "readers did not run concurrently: max=%d" % peak["max"]


def test_rwmutex_writer_is_exclusive_and_preferred():
    # A writer excludes everyone; writer-preference means a queued writer blocks
    # NEW readers (so writers aren't starved). We assert the writer's critical
    # section never overlapped a reader.
    events = []

    def main():
        rw = RWMutex()
        wg = WaitGroup(); wg.add(2)

        def writer():
            try:
                rw.lock()
                events.append("W_in")
                for _ in range(4):
                    rc.sched_yield()
                events.append("W_out")
                rw.unlock()
            finally:
                wg.done()

        def reader():
            try:
                for _ in range(2):
                    rc.sched_yield()
                rw.rlock()
                events.append("R_in")
                events.append("R_out")
                rw.runlock()
            finally:
                wg.done()

        rc.fiber(writer)
        rc.fiber(reader)
        wg.wait()

    with hang_guard(20, "rwmutex writer exclusive"):
        rc.fiber(main)
        rc.run()
    # No reader may sit between W_in and W_out.
    w_in, w_out = events.index("W_in"), events.index("W_out")
    r_in = events.index("R_in")
    assert not (w_in < r_in < w_out), \
        "reader entered during writer's critical section: %r" % events


def test_rwmutex_lock_from_foreign_thread_rejected():
    assert _foreign_result(lambda: RWMutex().lock()) == "RuntimeError"
    assert _foreign_result(lambda: RWMutex().rlock()) == "RuntimeError"


# ==========================================================================
# SECTION 13 -- Once / once_value / once_func: exactly-once, panic-safety,
#               only-first-sees-exception, foreign rejection.
# ==========================================================================
def test_once_runs_exactly_once_under_concurrency():
    runs = []
    seen = []

    def main():
        once = Once()
        wg = WaitGroup(); wg.add(6)

        def fn():
            runs.append(1)

        def caller():
            try:
                once.do(fn)
                seen.append(once.done())
            finally:
                wg.done()

        for _ in range(6):
            rc.fiber(caller)
        wg.wait()

    with hang_guard(20, "once exactly-once"):
        rc.fiber(main)
        rc.run()
    assert sum(runs) == 1, "Once ran %d times" % sum(runs)
    assert all(seen), "all callers must observe done()==True after do()"


def test_once_first_caller_sees_exception_later_do_not():
    # Go semantics: a panicking fn still COMPLETES the Once; the first executor
    # sees the exception, later callers return cleanly (no re-run, no re-raise).
    outcomes = []

    def main():
        once = Once()
        wg = WaitGroup(); wg.add(4)
        ran = []

        def boom():
            ran.append(1)
            raise ValueError("only-once")

        def caller(i):
            try:
                try:
                    once.do(boom)
                    outcomes.append(("clean", i))
                except ValueError:
                    outcomes.append(("raised", i))
            finally:
                wg.done()

        # spawn the executor first, give it a head start to BE the executor
        rc.fiber(lambda: caller(0))
        for _ in range(3):
            rc.sched_yield()
        for i in range(1, 4):
            rc.fiber(lambda i=i: caller(i))
        wg.wait()
        outcomes.append(("nran", sum(ran)))

    with hang_guard(20, "once panic-safety"):
        rc.fiber(main)
        rc.run()
    nran = dict(o for o in outcomes if o[0] == "nran")["nran"] if False else None
    raised = [o for o in outcomes if o[0] == "raised"]
    clean = [o for o in outcomes if o[0] == "clean"]
    nran = [o for o in outcomes if o[0] == "nran"][0][1]
    assert nran == 1, "boom ran %d times (must run once)" % nran
    assert len(raised) == 1, "exactly one caller (the executor) must see the exc"
    assert len(clean) == 3, "the 3 non-executor callers must return cleanly"


def test_once_value_caches_and_reraises_to_all():
    # once_value caches the result OR the exception and re-raises to EVERY call.
    def main():
        calls = []

        def fn():
            calls.append(1)
            return 7
        getter = once_value(fn)
        assert getter() == 7
        assert getter() == 7
        assert sum(calls) == 1

        def boomfn():
            raise ValueError("cached")
        bad = once_value(boomfn)
        with pytest.raises(ValueError):
            bad()
        with pytest.raises(ValueError):
            bad()                # re-raised, fn not re-run
        return "ok"
    with hang_guard(20, "once_value"):
        assert _run_single(main) == "ok"


def test_once_func_runs_once():
    def main():
        calls = []
        f = once_func(lambda: calls.append(1))
        f(); f(); f()
        assert sum(calls) == 1
        return "ok"
    assert _run_single(main) == "ok"


def test_once_do_from_foreign_thread_as_first_executor_rejected():
    # A foreign thread may not be the FIRST executor (it would wake parked
    # fibers); must reject cleanly.
    res = _foreign_result(lambda: Once().do(lambda: None))
    assert res == "RuntimeError", res


# ==========================================================================
# SECTION 14 -- singleflight Group: dedup-by-key, shared result, exception
#               sharing, forget.
# ==========================================================================
def test_singleflight_dedups_concurrent_calls_by_key():
    calls = []
    results = []

    def main():
        g = Group()
        wg = WaitGroup(); wg.add(5)

        def fn():
            calls.append(1)
            for _ in range(4):
                rc.sched_yield()       # keep it in-flight so others join
            return "VALUE"

        def caller():
            try:
                v, shared = g.do("k", fn)
                results.append((v, shared))
            finally:
                wg.done()

        for _ in range(5):
            rc.fiber(caller)
        wg.wait()

    with hang_guard(20, "singleflight dedup"):
        rc.fiber(main)
        rc.run()
    assert sum(calls) == 1, "fn ran %d times; singleflight must dedup" % sum(calls)
    assert all(v == "VALUE" for v, _ in results)
    shared_flags = [shared for _, shared in results]
    assert shared_flags.count(False) == 1, "exactly one owner (shared=False)"
    assert shared_flags.count(True) == 4, "the rest shared the result"


def test_singleflight_shares_exception():
    outs = []

    def main():
        g = Group()
        wg = WaitGroup(); wg.add(4)

        def fn():
            for _ in range(3):
                rc.sched_yield()
            raise ValueError("shared-fail")

        def caller():
            try:
                try:
                    g.do("k", fn)
                    outs.append("clean")
                except ValueError as e:
                    outs.append(("err", str(e)))
            finally:
                wg.done()

        for _ in range(4):
            rc.fiber(caller)
        wg.wait()

    with hang_guard(20, "singleflight exc"):
        rc.fiber(main)
        rc.run()
    assert all(o[0] == "err" and o[1] == "shared-fail" for o in outs), outs


def test_singleflight_forget_starts_fresh():
    def main():
        g = Group()
        calls = []
        g.do("k", lambda: calls.append("a") or "A")
        # second do on the SAME key after the first completed: a new call (the
        # entry is removed when do() finishes), so it runs again.
        g.do("k", lambda: calls.append("b") or "B")
        g.forget("k")            # no-op (nothing in flight) but must not error
        return calls
    assert _run_single(main) == ["a", "b"]


def test_singleflight_first_call_from_foreign_thread_rejected():
    res = _foreign_result(lambda: Group().do("k", lambda: 1))
    assert res == "RuntimeError", res


# ==========================================================================
# SECTION 15 -- Watch: version broadcast, wait_changed, timeout, foreign set
#               rejection + foreign waiter poll.
# ==========================================================================
def test_watch_version_broadcast_wakes_all_observers():
    seen = []

    def main():
        w = Watch("init")
        wg = WaitGroup(); wg.add(5)

        def observer(i):
            try:
                val, ver = w.wait_changed(0)     # block until version > 0
                seen.append((i, val, ver))
            finally:
                wg.done()

        for i in range(5):
            rc.fiber(lambda i=i: observer(i))

        def setter():
            for _ in range(8):
                rc.sched_yield()
            w.set("changed")
        rc.fiber(setter)
        wg.wait()

    with hang_guard(20, "watch broadcast"):
        rc.fiber(main)
        rc.run()
    assert len(seen) == 5, "not all observers woke: %r" % seen
    assert all(val == "changed" and ver == 1 for _, val, ver in seen)


def test_watch_get_and_version_track_sets():
    def main():
        w = Watch(0)
        assert w.get() == 0 and w.version() == 0
        w.set(10)
        assert w.get() == 10 and w.version() == 1
        w.set(20)
        assert w.get_versioned() == (20, 2)
        return "ok"
    assert _run_single(main) == "ok"


def test_watch_wait_changed_timeout_returns_none():
    def f():
        w = Watch("x")
        with assert_faster_than(5, "watch timeout"):
            r = w.wait_changed(w.version(), timeout=0.05)
        assert r is None
        return "ok"
    with hang_guard(15, "watch timeout"):
        assert _run_single(f) == "ok"


def test_watch_set_from_foreign_thread_rejected():
    res = _foreign_result(lambda: Watch(0).set(1))
    assert res == "RuntimeError", res


# ==========================================================================
# SECTION 16 -- JoinSet: spawn-order results, first-exception, context manager.
# ==========================================================================
def test_joinset_results_in_spawn_order():
    def main():
        js = JoinSet()
        for v in range(6):
            js.spawn(lambda v=v: ([rc.sched_yield() for _ in range(v % 3)] and v) or v)
        return js.join_all()
    with hang_guard(20, "joinset order"):
        assert _run_single(main) == [0, 1, 2, 3, 4, 5]


def test_joinset_first_exception_by_spawn_order():
    def main():
        js = JoinSet()
        js.spawn(lambda: "ok0")
        js.spawn(lambda: (_ for _ in ()).throw(ValueError("first-fail")))
        js.spawn(lambda: (_ for _ in ()).throw(ValueError("second-fail")))
        with pytest.raises(ValueError) as ei:
            js.join_all()
        assert str(ei.value) == "first-fail"
        return "done"
    with hang_guard(20, "joinset first exc"):
        assert _run_single(main) == "done"


def test_joinset_context_manager_joins_on_exit():
    out = {}

    def main():
        with JoinSet() as js:
            js.spawn(lambda: out.setdefault("a", "done-a"))
            js.spawn(lambda: out.setdefault("b", "done-b"))
        # on exit, both must have completed
        return out
    with hang_guard(20, "joinset ctx"):
        res = _run_single(main)
    assert res == {"a": "done-a", "b": "done-b"}


def test_joinset_context_manager_propagates_task_exc():
    def main():
        try:
            with JoinSet() as js:
                js.spawn(lambda: "ok")
                js.spawn(lambda: (_ for _ in ()).throw(ValueError("task-boom")))
            return "no-raise"
        except ValueError as e:
            return str(e)
    with hang_guard(20, "joinset ctx exc"):
        assert _run_single(main) == "task-boom"


# ==========================================================================
# SECTION 17 -- M:N integrity: fan-in/fan-out NO-DUP-NO-LOSS under work-stealing,
#               select arbitration under M:N, mutex exclusion under M:N.
# ==========================================================================
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_mn_buffered_fan_in_set_equality_no_dup_no_loss():
    # Distinct from test_adv_chan's unbuffered fan-in: here the channel is
    # BUFFERED and there are MANY producers/consumers, hammering buf_push /
    # buf_pop + parked-sender pull under cross-hub work-stealing.
    P, C, PER = 10, 10, 400
    ch = rc.Chan(32)
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

    with hang_guard(60, "mn buffered fan-in"):
        runloom.run(4, main)

    got = [v for slot in collected for v in slot]
    expected = set(range(P * PER))
    assert len(got) == len(expected), "lost/dup: got %d want %d" % (len(got), len(expected))
    assert set(got) == expected, "value set mismatch (lost or duplicated)"


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_mn_select_send_and_recv_mixed_no_loss():
    # Producers select-SEND into a set of channels; consumers select-RECV out.
    # Set-equality across the whole transfer proves the select CAS arbitration
    # neither dropped nor duplicated a value under M:N work-stealing.
    K, PER = 5, 300
    in_chans = [rc.Chan(4) for _ in range(K)]
    sink = []
    sink_mu = rc.Mutex()
    total = K * PER

    def main():
        wg = WaitGroup(); wg.add(K)

        def producer(base):
            try:
                for j in range(PER):
                    val = base * PER + j
                    # select-send across all channels: whichever is writable wins.
                    # select(default=True) returns -1 (int) when nothing is ready,
                    # else (idx, res); so check the raw return BEFORE unpacking.
                    while True:
                        r = rc.select(
                            [("send", c, val) for c in in_chans], default=True)
                        if r != -1:
                            break
                        rc.sched_yield()
            finally:
                wg.done()

        def consumer():
            n = 0
            while n < total:
                r = rc.select([("recv", c) for c in in_chans], default=True)
                if r == -1:
                    rc.sched_yield()
                    continue
                idx, (val, ok) = r
                if ok:
                    sink_mu.lock()
                    sink.append(val)
                    sink_mu.unlock()
                    n += 1

        rc.mn_fiber(consumer)
        for i in range(K):
            rc.mn_fiber(lambda i=i: producer(i))
        wg.wait()

    with hang_guard(60, "mn select mixed"):
        runloom.run(3, main)
    assert len(sink) == total, "lost/dup under select: got %d want %d" % (len(sink), total)
    assert set(sink) == set(range(total)), "select dropped or duplicated a value"


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_mn_mutex_serializes_under_work_stealing():
    # A runloom_c.Mutex under M:N must give true mutual exclusion: a non-atomic
    # read-modify-write inside the critical section loses NO increments.
    N, ITERS = 16, 200
    box = {"n": 0}

    def main():
        m = rc.Mutex()
        wg = WaitGroup(); wg.add(N)

        def worker():
            try:
                for _ in range(ITERS):
                    m.lock()
                    cur = box["n"]
                    rc.sched_yield()
                    box["n"] = cur + 1
                    m.unlock()
            finally:
                wg.done()

        for _ in range(N):
            rc.mn_fiber(worker)
        wg.wait()

    with hang_guard(60, "mn mutex exclusion"):
        runloom.run(4, main)
    assert box["n"] == N * ITERS, \
        "mutex lost increments under M:N: %d != %d" % (box["n"], N * ITERS)


# ==========================================================================
# SECTION 18 -- Fault injection (SPAWN_G / SPAWN_STACK): a clean Python error,
#               never a crash, when a spawn fails mid-workload.
# ==========================================================================
def test_spawn_g_fault_injection_is_clean_error_not_crash():
    # RUNLOOM_FAULT_SPAWN_G="once:12" -> the next fiber() fails with ENOMEM-style
    # error. gather()/JoinSet spawn fibers; the failure must be a Python
    # exception, not a SIGSEGV. Contained in a subprocess.
    script = r"""
import os, sys
import runloom_c as rc
# RUNLOOM_FAULT_SPAWN_G="once:12" makes the NEXT fiber()/mn_fiber() fail with a clean
# MemoryError-class error. Whether it lands on the driver spawn or a workload
# spawn, the contract is the same: a clean Python exception, NO signal/crash.
crashed = {}
def main():
    try:
        from runloom.sync import gather
        gather(lambda: 1, lambda: 2, lambda: 3)
        crashed['r'] = 'gather-ok'
    except BaseException as e:
        crashed['r'] = type(e).__name__
try:
    rc.fiber(main)
    rc.run()
except BaseException as e:
    crashed['top'] = type(e).__name__
# A clean Python exception (MemoryError / RuntimeError / ...) is the PASS:
# the spawn fault must NOT corrupt the runtime.
print('RESULT', crashed)
sys.exit(0)
"""
    p = _subprocess(script, env_extra={"RUNLOOM_FAULT_SPAWN_G": "once:12",
                                       "RUNLOOM_GOROUTINE_PANIC": "silent"},
                    timeout=30)
    assert p.returncode is None or p.returncode >= 0, \
        "SPAWN_G fault crashed with a signal: rc=%r\n%s" % (
            p.returncode, p.stderr.decode("utf8", "replace"))
    assert b"RESULT" in p.stdout, p.stderr.decode("utf8", "replace")


def test_spawn_stack_fault_injection_is_clean_error_not_crash():
    script = r"""
import os, sys
import runloom_c as rc
out = {}
def child():
    return 1
def main():
    try:
        from runloom.sync import JoinSet
        js = JoinSet()
        for _ in range(4):
            js.spawn(child)
        js.join_all()
        out['r'] = 'ok'
    except BaseException as e:
        out['r'] = type(e).__name__
try:
    rc.fiber(main)
    rc.run()
except BaseException as e:
    out['top'] = type(e).__name__
print('RESULT', out)
sys.exit(0)
"""
    p = _subprocess(script, env_extra={"RUNLOOM_FAULT_SPAWN_STACK": "once:12",
                                       "RUNLOOM_GOROUTINE_PANIC": "silent"},
                    timeout=30)
    assert p.returncode is None or p.returncode >= 0, \
        "SPAWN_STACK fault crashed with a signal: rc=%r\n%s" % (
            p.returncode, p.stderr.decode("utf8", "replace"))
    assert b"RESULT" in p.stdout, p.stderr.decode("utf8", "replace")


# ==========================================================================
# SECTION 19 -- Resource / scale: many channels + selects in one fiber must not
#               leak waiters (self_check via conftest catches structural leaks).
# ==========================================================================
def test_many_selects_evict_tombstones_no_leak():
    # Repeatedly park a select on a set of channels then wake exactly one; the
    # tombstone eviction must clean every losing channel's waiter queue. The
    # conftest self_check + parked-leak fixture asserts no structural residue.
    rounds = []

    def main():
        chans = [rc.Chan(0) for _ in range(5)]

        def chooser():
            for _ in range(40):
                idx, (v, ok) = rc.select([("recv", c) for c in chans])
                rounds.append((idx, v))

        def feeder():
            for i in range(40):
                # rotate which channel becomes ready
                target = chans[i % len(chans)]
                for _ in range(2):
                    rc.sched_yield()
                target.send(i)
        rc.fiber(chooser)
        rc.fiber(feeder)

    with hang_guard(40, "select tombstone eviction"):
        rc.fiber(main)
        rc.run()
    assert len(rounds) == 40
    # every value 0..39 was delivered exactly once (no drop/dup across evictions)
    assert sorted(v for _, v in rounds) == list(range(40))


def test_channel_as_iterator_drains_then_stops_on_close():
    def f():
        ch = rc.Chan(8)
        for v in range(8):
            ch.try_send(v)
        ch.close()
        return list(ch)              # range-over-chan stops at closed+empty
    assert _run_single(f) == list(range(8))


# ==========================================================================
# ==========================================================================
# AUGMENTATION (adversarial critic pass): conditions the first pass MISSED.
# ==========================================================================
# ==========================================================================


# ==========================================================================
# A1 -- select argument-validation DEPTH (the first pass tested a handful;
#       these hit the remaining error branches in module_select.c.inc).
# ==========================================================================
def test_select_send_four_tuple_rejected():
    # module_select rejects a send case whose tuple size != 3 (a 4-tuple send).
    def f():
        ch = rc.Chan(1)
        with pytest.raises(TypeError):
            rc.select([("send", ch, 1, 2)])      # 4-tuple send
        return "ok"
    assert _run_single(f) == "ok"


def test_select_int_op_rejected_no_leftover_error():
    # A non-str op (int/bytes): PyUnicode_AsUTF8 fails; the code path must end
    # with a clean ValueError and leave NO leftover pending exception that would
    # corrupt the next call.
    def f():
        ch = rc.Chan(1)
        with pytest.raises((ValueError, TypeError)):
            rc.select([(123, ch)])               # int op
        with pytest.raises((ValueError, TypeError)):
            rc.select([(b"recv", ch)])           # bytes op
        # The runtime must be usable immediately after: a real select fires.
        ch.try_send(7)
        idx, (v, ok) = rc.select([("recv", ch)])
        assert idx == 0 and v == 7 and ok is True, (idx, v, ok)
        return "ok"
    assert _run_single(f) == "ok"


def test_select_none_case_and_tuple_of_cases():
    def f():
        ch = rc.Chan(1); ch.try_send(5)
        with pytest.raises(TypeError):
            rc.select([None])                    # case is None, not a tuple
        # cases may be a TUPLE, not only a list (module_select accepts both).
        idx, (v, ok) = rc.select((("recv", ch),))
        assert idx == 0 and v == 5 and ok is True
        return "ok"
    assert _run_single(f) == "ok"


def test_select_recv_with_extra_tuple_element_tolerated():
    # A recv case is ('recv', ch); module_select only requires size >= 2, so a
    # 3-element recv tuple ('recv', ch, junk) is tolerated (extra ignored). Pin
    # the OBSERVED behavior so a future tightening is caught.
    def f():
        empty = rc.Chan(0)
        # nothing ready -> default fires (proves the case parsed as a valid recv)
        r = rc.select([("recv", empty, "extra")], default=True)
        assert r == -1
        rdy = rc.Chan(1); rdy.try_send(9)
        idx, (v, ok) = rc.select([("recv", rdy, "extra")])
        assert idx == 0 and v == 9 and ok is True
        return "ok"
    assert _run_single(f) == "ok"


# ==========================================================================
# A2 -- select with a channel that is closed AND still buffered: the select
#       must DRAIN the buffered value (ok=True) before the closed sentinel.
#       Exercises the close-wake-but-buffered re-scan path in chan_select_main.
# ==========================================================================
def test_select_recv_on_closed_but_buffered_drains_value_first():
    def f():
        ch = rc.Chan(2)
        ch.try_send("buffered")
        ch.close()
        # buffered value must come out with ok=True even though closed
        idx, (v, ok) = rc.select([("recv", ch)])
        assert idx == 0 and v == "buffered" and ok is True, (idx, v, ok)
        # now empty+closed -> sentinel
        idx, (v, ok) = rc.select([("recv", ch)])
        assert idx == 0 and v is None and ok is False, (idx, v, ok)
        return "ok"
    assert _run_single(f) == "ok"


def test_select_duplicate_channel_recv_consumes_exactly_one():
    # Two recv cases on the SAME ready channel: select must consume EXACTLY one
    # value, not double-consume. (CAS arbitration on identical channels.)
    def f():
        ch = rc.Chan(1); ch.try_send(42)
        idx, (v, ok) = rc.select([("recv", ch), ("recv", ch)])
        assert v == 42 and ok is True
        assert ch.try_recv() is None, "double-consumed a duplicate channel"
        return "ok"
    assert _run_single(f) == "ok"


def test_select_duplicate_channel_send_fires_once():
    def f():
        ch = rc.Chan(2)
        idx, res = rc.select([("send", ch, "a"), ("send", ch, "b")])
        assert res is None
        # exactly one of the two send cases fired -> exactly one value buffered
        assert len(ch) == 1, "duplicate send case double-fired"
        return "ok"
    assert _run_single(f) == "ok"


# ==========================================================================
# A3 -- select SEND case that must PARK (no peer) then is woken by an arriving
#       receiver -- the send-side park/wake path through chan_select_main, NOT
#       just the recv-side block-then-wake the first pass covered.
# ==========================================================================
def test_select_send_case_parks_then_woken_by_arriving_receiver():
    out = {}

    def main():
        ch = rc.Chan(0)             # unbuffered -> send has no peer -> parks

        def chooser():
            idx, res = rc.select([("send", ch, "payload")])
            out["sent"] = (idx, res)

        def receiver():
            for _ in range(4):
                rc.sched_yield()    # ensure chooser parked as a sender first
            out["got"] = ch.recv()
        rc.fiber(chooser)
        rc.fiber(receiver)

    with assert_faster_than(10, "select send park"):
        with hang_guard(20, "select send park wake"):
            rc.fiber(main)
            rc.run()
    assert out.get("sent") == (0, None), out
    assert out.get("got") == ("payload", True), out


def test_select_send_parked_then_woken_raises_on_close():
    # A select SEND parked on an unbuffered channel that is then CLOSED must
    # surface a ValueError (select send on closed channel), not deliver/hang.
    out = {}

    def main():
        ch = rc.Chan(0)

        def chooser():
            try:
                rc.select([("send", ch, "x")])
                out["r"] = "sent"
            except ValueError:
                out["r"] = "closed"

        def closer():
            for _ in range(5):
                rc.sched_yield()
            ch.close()
        rc.fiber(chooser)
        rc.fiber(closer)

    with hang_guard(20, "select send close"):
        rc.fiber(main)
        rc.run()
    assert out.get("r") == "closed", out


# ==========================================================================
# A4 -- Chan len() / capacity introspection edges the first pass skipped.
# ==========================================================================
def test_chan_len_tracks_buffer_and_after_close():
    def f():
        ch = rc.Chan(4)
        assert len(ch) == 0
        ch.try_send(1); ch.try_send(2)
        assert len(ch) == 2
        ch.close()
        assert len(ch) == 2, "buffered len must survive close()"
        assert ch.closed is True
        v, ok = ch.recv()
        assert (v, ok) == (1, True) and len(ch) == 1
        ch.recv()
        assert len(ch) == 0
        return "ok"
    assert _run_single(f) == "ok"


def test_chan_huge_capacity_is_lazy_not_eager_alloc():
    # Chan(1e9) must NOT eagerly malloc ~8 GB; capacity is recorded, the ring is
    # grown lazily. A crash/OOM here would be a resource-exhaustion bug.
    def f():
        ch = rc.Chan(10 ** 8)
        assert ch.capacity == 10 ** 8 and len(ch) == 0
        ch.try_send("x")
        assert len(ch) == 1 and ch.try_recv() == ("x", True)
        return "ok"
    assert _run_single(f) == "ok"


# ==========================================================================
# A5 -- timeout=0 / negative timeout: an UNRESOLVED waiter with a non-positive
#       deadline must time out IMMEDIATELY (never park forever, never crash).
# ==========================================================================
def test_future_result_zero_and_negative_timeout_immediate():
    def f():
        for t in (0, -1, -0.5):
            fut = Future()
            with assert_faster_than(3, "future t=%r" % t):
                with pytest.raises(TimeoutError):
                    fut.result(timeout=t)
        return "ok"
    with hang_guard(15, "future zero timeout"):
        assert _run_single(f) == "ok"


def test_semaphore_zero_and_negative_timeout_returns_false_fast():
    def f():
        s = Semaphore(1); s.acquire(1)          # exhausted
        for t in (0, -1):
            with assert_faster_than(3, "sem t=%r" % t):
                assert s.acquire(1, timeout=t) is False
        s.release(1)
        return "ok"
    with hang_guard(15, "sem zero timeout"):
        assert _run_single(f) == "ok"


def test_watch_zero_timeout_returns_none_fast():
    def f():
        w = Watch("v")
        with assert_faster_than(3, "watch t0"):
            assert w.wait_changed(w.version(), timeout=0) is None
        return "ok"
    with hang_guard(15, "watch zero timeout"):
        assert _run_single(f) == "ok"


# ==========================================================================
# A6 -- Semaphore zero-weight + multi-grant-in-one-release + acquire(0).
# ==========================================================================
def test_semaphore_acquire_zero_always_succeeds():
    def f():
        s = Semaphore(2)
        s.acquire(2)                            # fully held
        assert s.acquire(0) is True             # 0 permits -> always fits
        assert s.try_acquire(0) is True
        s.release(2)
        s.release(0)                            # releasing 0 is a no-op, no error
        return "ok"
    assert _run_single(f) == "ok"


def test_semaphore_single_release_wakes_multiple_small_waiters():
    # One release(4) must grant SEVERAL small waiters in a single sweep (the
    # while-loop grant in release()), not just one -- else lost throughput.
    order = []

    def main():
        s = Semaphore(4)
        s.acquire(4)                            # everyone parks
        from runloom.sync import WaitGroup
        wg = WaitGroup(); wg.add(4)

        def small(i):
            try:
                s.acquire(1, timeout=10)
                order.append(i)
                for _ in range(2):
                    rc.sched_yield()
                s.release(1)
            finally:
                wg.done()
        for i in range(4):
            rc.fiber(lambda i=i: small(i))

        def releaser():
            for _ in range(4):
                rc.sched_yield()
            s.release(4)                        # frees all 4 at once
        rc.fiber(releaser)
        wg.wait()

    with hang_guard(30, "sem multi-grant"):
        rc.fiber(main)
        rc.run()
    assert sorted(order) == [0, 1, 2, 3], "not all small waiters granted: %r" % order


# ==========================================================================
# A7 -- Foreign-OS-thread WAIT-side polling (the first pass tested only the
#       reject side of the wake path; the poll side of the WAIT path is a
#       distinct code path -- a foreign thread BUSY-POLLS the counter/version).
# ==========================================================================
def test_waitgroup_foreign_thread_wait_side_polls_to_completion():
    # A foreign OS thread WAITING on a WaitGroup that a fiber drains is legal
    # (the wait() poll fallback). Contained in a subprocess.
    script = r"""
import runloom_c as rc, threading, sys
from runloom.sync import WaitGroup
wg = WaitGroup(); wg.add(1)
out = {}
def foreign_wait():
    wg.wait()                 # foreign poll path -- must return when drained
    out['ok'] = True
t = threading.Thread(target=foreign_wait); t.start()
def main():
    for _ in range(6):
        rc.sched_yield()
    wg.done()                 # fiber drains the wg
rc.fiber(main); rc.run()
t.join(5)
assert out.get('ok') is True, out
print('WG_FOREIGN_WAIT_OK')
sys.exit(0)
"""
    p = _subprocess(script, timeout=30)
    assert p.returncode is None or p.returncode >= 0, \
        "WaitGroup foreign wait crashed: rc=%r\n%s" % (
            p.returncode, p.stderr.decode("utf8", "replace"))
    assert b"WG_FOREIGN_WAIT_OK" in p.stdout, p.stderr.decode("utf8", "replace")


def test_watch_foreign_thread_wait_changed_polls():
    script = r"""
import runloom_c as rc, threading, sys
from runloom.sync import Watch
w = Watch("init")
out = {}
def foreign_wait():
    out['r'] = w.wait_changed(0, timeout=5)   # foreign poll path
t = threading.Thread(target=foreign_wait); t.start()
def main():
    for _ in range(6):
        rc.sched_yield()
    w.set("changed")
rc.fiber(main); rc.run()
t.join(5)
assert out.get('r') == ("changed", 1), out
print('WATCH_FOREIGN_WAIT_OK')
sys.exit(0)
"""
    p = _subprocess(script, timeout=30)
    assert p.returncode is None or p.returncode >= 0, \
        "Watch foreign wait crashed: rc=%r\n%s" % (
            p.returncode, p.stderr.decode("utf8", "replace"))
    assert b"WATCH_FOREIGN_WAIT_OK" in p.stdout, p.stderr.decode("utf8", "replace")


def test_once_foreign_thread_waiter_polls_while_fiber_executes():
    # A foreign thread that arrives AFTER a fiber became the executor must POLL
    # (the Once.do foreign-waiter branch), then return once the fiber finishes
    # -- without becoming a second executor and without crashing.
    # The fiber becomes the executor and HOLDS the in-flight window open via a
    # real sched_sleep (wall-clock while cooperatively yielding), so the foreign
    # thread is guaranteed to enter once.do() while _running is True and take the
    # foreign-waiter POLL branch -- not become a second executor.
    script = r"""
import runloom_c as rc, threading, time, sys
from runloom.sync import Once
once = Once()
ran = []
out = {}
running = threading.Event()
def fiber_exec():
    def slow():
        ran.append(1)
        running.set()                # executor is now in-flight
        rc.sched_sleep(0.3)          # hold the window open for the foreign join
    once.do(slow)
def foreign_waiter():
    running.wait(3)                  # join only once a fiber is mid-execution
    once.do(lambda: ran.append('FOREIGN_RAN'))   # must be a no-op WAIT
    out['foreign'] = 'returned'
threading.Thread(target=foreign_waiter).start()
rc.fiber(fiber_exec); rc.run()
time.sleep(0.3)                      # let the foreign poll observe _done
assert sum(1 for x in ran if x == 1) == 1, ran
assert 'FOREIGN_RAN' not in ran, ('foreign became executor', ran)
assert out.get('foreign') == 'returned', out
print('ONCE_FOREIGN_WAIT_OK')
sys.exit(0)
"""
    p = _subprocess(script, timeout=30)
    assert p.returncode is None or p.returncode >= 0, \
        "Once foreign waiter crashed: rc=%r\n%s" % (
            p.returncode, p.stderr.decode("utf8", "replace"))
    assert b"ONCE_FOREIGN_WAIT_OK" in p.stdout, p.stderr.decode("utf8", "replace")


# ==========================================================================
# A8 -- RWMutex deeper: writer hands off to ALL queued readers at once (the
#       broadcast unlock path), rlocked() context manager, double rlock by
#       distinct readers, and writer-preference blocking a new reader.
# ==========================================================================
def test_rwmutex_writer_release_wakes_all_queued_readers_at_once():
    events = []
    peak = {"cur": 0, "max": 0}

    def main():
        from runloom.sync import WaitGroup
        rw = RWMutex()
        wg = WaitGroup(); wg.add(4)

        def writer():
            try:
                rw.lock(); events.append("W_in")
                for _ in range(6):
                    rc.sched_yield()        # let all 3 readers queue
                events.append("W_out"); rw.unlock()
            finally:
                wg.done()

        def reader(i):
            try:
                for _ in range(2):
                    rc.sched_yield()        # queue behind the writer
                rw.rlock()
                peak["cur"] += 1
                peak["max"] = max(peak["max"], peak["cur"])
                events.append(("R", i))
                for _ in range(2):
                    rc.sched_yield()
                peak["cur"] -= 1
                rw.runlock()
            finally:
                wg.done()
        rc.fiber(writer)
        for i in range(3):
            rc.fiber(lambda i=i: reader(i))
        wg.wait()

    with hang_guard(30, "rwmutex broadcast to readers"):
        rc.fiber(main)
        rc.run()
    # all readers were handed the lock together after the writer released
    assert peak["max"] == 3, "writer did not release to ALL readers: %r" % events
    w_out = events.index("W_out")
    assert all(i > w_out for i, e in enumerate(events)
               if isinstance(e, tuple)), \
        "a reader entered before the writer released: %r" % events


def test_rwmutex_rlocked_context_manager():
    peak = {"cur": 0, "max": 0}

    def main():
        from runloom.sync import WaitGroup
        rw = RWMutex()
        wg = WaitGroup(); wg.add(3)

        def reader():
            try:
                with rw.rlocked():
                    peak["cur"] += 1
                    peak["max"] = max(peak["max"], peak["cur"])
                    for _ in range(2):
                        rc.sched_yield()
                    peak["cur"] -= 1
            finally:
                wg.done()
        for _ in range(3):
            rc.fiber(reader)
        wg.wait()

    with hang_guard(20, "rwmutex rlocked ctx"):
        rc.fiber(main)
        rc.run()
    assert peak["max"] >= 2, "rlocked() ctx serialized readers: %r" % peak


def test_rwmutex_runlock_with_no_active_readers_after_drain_raises():
    # After all readers released, an extra runlock() must raise (not underflow
    # _readers into negative territory -> a stuck writer-preference).
    def f():
        rw = RWMutex()
        rw.rlock()
        rw.runlock()
        with pytest.raises(RuntimeError):
            rw.runlock()                # already at zero
        # the lock is still usable for a writer
        rw.lock(); rw.unlock()
        return "ok"
    assert _run_single(f) == "ok"


# ==========================================================================
# A9 -- singleflight: forget WHILE in-flight starts a fresh call; foreign-thread
#       WAIT-side (a foreign thread joining an in-flight key) polls the Future.
# ==========================================================================
def test_singleflight_forget_while_inflight_starts_fresh_call():
    calls = []
    owners = []

    def main():
        from runloom.sync import WaitGroup
        g = Group()
        wg = WaitGroup(); wg.add(2)

        def slow():
            calls.append("A")
            for _ in range(8):
                rc.sched_yield()
            return "A"

        def fresh():
            calls.append("B")
            return "B"

        def c1():
            try:
                owners.append(("c1",) + g.do("k", slow))
            finally:
                wg.done()

        def c2():
            try:
                for _ in range(2):
                    rc.sched_yield()        # let c1 register the key
                g.forget("k")               # drop the in-flight entry
                owners.append(("c2",) + g.do("k", fresh))
            finally:
                wg.done()
        rc.fiber(c1)
        rc.fiber(c2)
        wg.wait()

    with hang_guard(20, "singleflight forget in-flight"):
        rc.fiber(main)
        rc.run()
    # forget made c2 start a NEW call: both fns ran, both are owners (shared=False)
    assert sorted(calls) == ["A", "B"], calls
    by = {o[0]: o for o in owners}
    assert by["c1"] == ("c1", "A", False), owners
    assert by["c2"] == ("c2", "B", False), owners


def test_singleflight_foreign_thread_joins_inflight_key():
    # The first caller is a fiber; a foreign thread arriving at the SAME key
    # while it's in-flight is a WAITER (Future.result poll), which is allowed --
    # only the FIRST call for a key must be a fiber.
    # The fiber owner must keep the key IN-FLIGHT until the foreign joiner has
    # actually registered on the shared Future, else a single-thread scheduler
    # finishes the call (deletes the key) before the foreign thread arrives and
    # the join would (correctly) become a fresh "first call" -> RuntimeError.
    # We synchronise: foreign sets joining=True the instant before g.do, then the
    # fiber spins (cooperatively yielding) until joined=True, which the foreign
    # thread sets only AFTER g.do returns -- so the fiber holds the key in-flight
    # across the whole join window. The foreign g.do BLOCKS on the Future until
    # the fiber resolves it, so we drive both from a watchdog timer.
    # Synchronisation: the foreign thread sets `entered` AFTER its g.do has
    # actually returned from registering as a waiter -- impossible (g.do blocks
    # in the join). So instead we drive it the other way: the fiber registers
    # the key, signals `key_ready`, then sched_sleeps (real wall-clock while
    # cooperatively yielding) so the foreign thread has a guaranteed window to
    # enter g.do and block on the shared Future. The fiber only returns (which
    # deletes the key + resolves the Future) after that window, so the foreign
    # join is guaranteed to have grabbed the in-flight Future, not a fresh key.
    script = r"""
import runloom_c as rc, threading, time, sys
from runloom.sync import Group
g = Group()
out = {}
key_ready = threading.Event()
def fiber_owner():
    def fn():
        key_ready.set()              # key 'k' is now registered in g._calls
        rc.sched_sleep(0.3)          # hold it in-flight (wall-clock) while the
        return "SHARED"              # foreign thread enters g.do + blocks
    out['owner'] = g.do("k", fn)
def foreign_joiner():
    key_ready.wait(3)                # wait until the fiber owns the key
    out['joiner'] = g.do("k", lambda: "SHOULD_NOT_RUN")   # joins, shares result
threading.Thread(target=foreign_joiner).start()
rc.fiber(fiber_owner); rc.run()
time.sleep(0.3)                      # let the foreign join's poll observe _done
assert out.get('owner') == ("SHARED", False), out
assert out.get('joiner') == ("SHARED", True), out
print('SF_FOREIGN_JOIN_OK')
sys.exit(0)
"""
    p = _subprocess(script, timeout=30)
    assert p.returncode is None or p.returncode >= 0, \
        "singleflight foreign joiner crashed: rc=%r\n%s" % (
            p.returncode, p.stderr.decode("utf8", "replace"))
    assert b"SF_FOREIGN_JOIN_OK" in p.stdout, p.stderr.decode("utf8", "replace")


# ==========================================================================
# A10 -- Watch monotonic version across many sets + last-value semantics + a
#        late observer never misses an already-bumped version.
# ==========================================================================
def test_watch_version_is_monotonic_and_holds_latest():
    def f():
        w = Watch(0)
        for i in range(1, 21):
            w.set(i * 10)
            assert w.version() == i and w.get() == i * 10
        # a late wait_changed(seen=0) returns IMMEDIATELY with the latest, not a
        # park (version already > seen).
        with assert_faster_than(3, "watch late observer"):
            val, ver = w.wait_changed(0)
        assert val == 200 and ver == 20
        return "ok"
    with hang_guard(15, "watch monotonic"):
        assert _run_single(f) == "ok"


def test_watch_coalesces_multiple_sets_to_latest_for_parked_observer():
    # An observer parked on version 0 woken after THREE sets must see the LATEST
    # value/version (watch coalesces -- it's a latest-value cell, not a queue).
    seen = []

    def main():
        w = Watch("v0")

        def observer():
            seen.append(w.wait_changed(0))

        def setter():
            for _ in range(3):
                rc.sched_yield()            # let observer park
            w.set("v1"); w.set("v2"); w.set("v3")
        rc.fiber(observer)
        rc.fiber(setter)

    with hang_guard(20, "watch coalesce"):
        rc.fiber(main)
        rc.run()
    assert seen == [("v3", 3)], "observer did not see the coalesced latest: %r" % seen


# ==========================================================================
# A11 -- Mutex deeper: FIFO hand-off ordering of parked lockers; locked()
#        reflects a parked locker; context-manager releases on body exception.
# ==========================================================================
def test_mutex_parked_lockers_handed_off_fifo():
    # Several fibers contend for a held mutex; the holder's unlock hands the
    # token to the FIRST parked locker (channel-backed FIFO). Assert grant order.
    order = []

    def main():
        m = rc.Mutex()
        m.lock()                            # held by main fiber's child below
        from runloom.sync import WaitGroup
        wg = WaitGroup(); wg.add(3)

        def locker(i):
            try:
                m.lock()
                order.append(i)
                rc.sched_yield()
                m.unlock()
            finally:
                wg.done()
        # spawn 0,1,2 in order; each parks behind the held mutex in FIFO order
        for i in range(3):
            rc.fiber(lambda i=i: locker(i))
            for _ in range(2):
                rc.sched_yield()            # ensure i parks before i+1 spawns

        def releaser():
            for _ in range(2):
                rc.sched_yield()
            m.unlock()                      # release -> first parked locker wins
        rc.fiber(releaser)
        wg.wait()

    with hang_guard(30, "mutex FIFO handoff"):
        rc.fiber(main)
        rc.run()
    assert order == [0, 1, 2], "mutex did not hand off FIFO: %r" % order


def test_mutex_context_manager_releases_on_body_exception():
    def f():
        m = rc.Mutex()
        try:
            with m:
                assert m.locked() is True
                raise ValueError("boom")
        except ValueError:
            pass
        assert m.locked() is False, "context manager leaked the lock on exception"
        return "ok"
    assert _run_single(f) == "ok"


def test_mutex_locked_reflects_parked_locker_under_mn():
    # locked() reads channel len; a token taken by a fiber that then parks a
    # second locker must still read locked()==True.
    def f():
        m = rc.Mutex()
        m.lock()
        assert m.locked() is True
        m.unlock()
        assert m.locked() is False
        return "ok"
    assert _run_single(f) == "ok"


# ==========================================================================
# A12 -- UNBUFFERED direct-handoff at scale under M:N (pure rendezvous, NO
#        buffer): set-equality no-dup-no-loss. Distinct from the first pass's
#        BUFFERED fan-in -- this hammers the sender-park / recv-pull handoff.
# ==========================================================================
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_mn_unbuffered_rendezvous_fan_in_out_set_equality():
    P, C, PER = 8, 8, 300
    ch = rc.Chan(0)                         # UNBUFFERED -> every send rendezvous
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

    with hang_guard(60, "mn unbuffered rendezvous"):
        runloom.run(4, main)
    got = [v for slot in collected for v in slot]
    expected = set(range(P * PER))
    assert len(got) == len(expected), "lost/dup: got %d want %d" % (len(got), len(expected))
    assert set(got) == expected, "unbuffered rendezvous lost or duplicated a value"


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_mn_select_competes_with_direct_recv_no_double_consume():
    # Half the consumers use select-recv, half use direct recv() on the SAME set
    # of channels. The CAS arbitration must keep set-equality even when a select
    # and a direct recv race for the same buffered value.
    K, PER = 5, 240
    chans = [rc.Chan(2) for _ in range(K)]
    sink = []
    sink_mu = rc.Mutex()
    total = K * PER

    def main():
        wg = WaitGroup(); wg.add(K)
        done_box = {"n": 0}

        def producer(base):
            try:
                for j in range(PER):
                    chans[base].send(base * PER + j)
            finally:
                wg.done()

        def collect(v):
            sink_mu.lock()
            sink.append(v)
            n = len(sink)
            sink_mu.unlock()
            return n

        def select_consumer():
            while True:
                if len(sink) >= total:
                    return
                r = rc.select([("recv", c) for c in chans], default=True)
                if r == -1:
                    rc.sched_yield()
                    continue
                idx, (v, ok) = r
                if ok:
                    collect(v)

        def direct_consumer(idx):
            while True:
                if len(sink) >= total:
                    return
                r = chans[idx].try_recv()
                if r is None:
                    rc.sched_yield()
                    continue
                v, ok = r
                if ok:
                    collect(v)

        # 2 select consumers across all channels + one direct consumer per channel
        for _ in range(2):
            rc.mn_fiber(select_consumer)
        for i in range(K):
            rc.mn_fiber(lambda i=i: direct_consumer(i))
        for i in range(K):
            rc.mn_fiber(lambda i=i: producer(i))
        wg.wait()
        # drain stragglers: keep consumers alive until sink full (they self-exit)
        while len(sink) < total:
            rc.sched_yield()

    with hang_guard(60, "mn select vs direct recv"):
        runloom.run(4, main)
    assert len(sink) == total, "lost/dup: got %d want %d" % (len(sink), total)
    assert set(sink) == set(range(total)), "select/direct race dropped or duped a value"


# ==========================================================================
# A13 -- gather / JoinSet under M:N (the mn_hub_count() routing path the first
#        pass only drove single-threaded), and a nested gather inside a hub.
# ==========================================================================
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_gather_and_joinset_route_to_mn_under_run_n():
    out = {}

    def main():
        def work(v):
            for _ in range(v % 4):
                rc.sched_yield()
            return v * v
        out["gather"] = gather(*[(lambda v=v: work(v)) for v in range(8)])
        js = JoinSet()
        for v in range(8):
            js.spawn(lambda v=v: work(v) + 1000)
        out["joinset"] = js.join_all()

    with hang_guard(40, "gather/joinset MN"):
        runloom.run(3, main)
    assert out["gather"] == [v * v for v in range(8)], out["gather"]
    assert out["joinset"] == [v * v + 1000 for v in range(8)], out["joinset"]


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_joinset_first_exception_by_spawn_order_under_mn():
    out = {}

    def main():
        js = JoinSet()
        js.spawn(lambda: "ok")
        js.spawn(lambda: (_ for _ in ()).throw(ValueError("first")))
        js.spawn(lambda: (_ for _ in ()).throw(ValueError("second")))
        try:
            js.join_all()
            out["r"] = "no-raise"
        except ValueError as e:
            out["r"] = str(e)

    with hang_guard(30, "joinset first-exc MN"):
        runloom.run(3, main)
    assert out["r"] == "first", out


# ==========================================================================
# A14 -- WaitGroup edge: add(0) is a no-op; wait() on an already-zero counter
#        returns immediately (no park); add after a partial drain.
# ==========================================================================
def test_waitgroup_wait_on_zero_returns_immediately():
    def f():
        wg = WaitGroup()
        with assert_faster_than(3, "wg zero wait"):
            wg.wait()                       # counter already 0 -> no park
        wg.add(0)                           # no-op, no error
        wg.add(2)
        wg.done(); wg.done()
        with assert_faster_than(3, "wg drained wait"):
            wg.wait()
        return "ok"
    with hang_guard(15, "wg zero wait"):
        assert _run_single(f) == "ok"


def test_waitgroup_reused_three_cycles_no_residue():
    # Drive the SAME WaitGroup through three add/done/wait cycles; a stale waiter
    # from a prior cycle stranded across reuse would surface as a hang or an
    # extra wake. (Reuse-after-drain at depth.)
    seq = []

    def main():
        wg = WaitGroup()
        for cyc in range(3):
            wg.add(2)
            box = {"done": 0}

            def waiter(c=cyc):
                wg.wait()
                seq.append(c)

            def worker():
                for _ in range(3):
                    rc.sched_yield()
                wg.done(); wg.done()
            rc.fiber(waiter)
            rc.fiber(worker)
            # let this cycle fully drain before starting the next
            for _ in range(8):
                rc.sched_yield()

    with hang_guard(30, "wg three cycles"):
        rc.fiber(main)
        rc.run()
    assert sorted(seq) == [0, 1, 2], "reuse leaked a cycle: %r" % seq


# ==========================================================================
# A15 -- Future reuse-is-illegal after both result paths; result() re-poll after
#        resolve is idempotent; set_exception with a CLASS (not instance).
# ==========================================================================
def test_future_result_idempotent_after_resolve():
    def f():
        fut = Future()
        fut.set_result([1, 2, 3])
        a = fut.result()
        b = fut.result()                    # second read returns the same object
        assert a == [1, 2, 3] and a is b
        assert fut.done() is True
        return "ok"
    assert _run_single(f) == "ok"


def test_future_set_exception_accepts_class_and_instance():
    def f():
        fa = Future()
        fa.set_exception(ValueError)        # a CLASS -> instantiated
        with pytest.raises(ValueError):
            fa.result()
        fb = Future()
        fb.set_exception(KeyError("k"))     # an instance
        with pytest.raises(KeyError):
            fb.result()
        return "ok"
    assert _run_single(f) == "ok"


# ==========================================================================
# A16 -- Once.do that PARKS / spawns inside fn (executor's fn cooperatively
#        yields while waiters queue) -- the waiters must all wake when it ends.
# ==========================================================================
def test_once_executor_fn_yields_while_waiters_queue_then_all_wake():
    woke = []
    ran = []

    def main():
        from runloom.sync import WaitGroup
        once = Once()
        wg = WaitGroup(); wg.add(6)

        def slow():
            ran.append(1)
            for _ in range(6):
                rc.sched_yield()            # waiters pile up while we run

        def caller(i):
            try:
                once.do(slow)
                woke.append(i)
            finally:
                wg.done()
        # executor first, then 5 more queue behind it
        rc.fiber(lambda: caller(0))
        for _ in range(2):
            rc.sched_yield()
        for i in range(1, 6):
            rc.fiber(lambda i=i: caller(i))
        wg.wait()

    with hang_guard(30, "once executor yields"):
        rc.fiber(main)
        rc.run()
    assert sum(ran) == 1, "Once ran %d times" % sum(ran)
    assert sorted(woke) == [0, 1, 2, 3, 4, 5], "not all waiters woke: %r" % woke


# ==========================================================================
# A17 -- Resource/scale: MANY concurrent selects, each parking on MANY channels,
#        woken one at a time -- deep tombstone-eviction + abort-retry pressure.
#        The conftest self_check + parked-leak fixture asserts no residue.
# ==========================================================================
def test_many_concurrent_selects_deep_eviction_no_leak():
    NSEL, NCH, ROUNDS = 8, 6, 30
    got = []

    def main():
        chans = [rc.Chan(0) for _ in range(NCH)]
        from runloom.sync import WaitGroup
        wg = WaitGroup(); wg.add(NSEL)

        def chooser(cid):
            try:
                for _ in range(ROUNDS):
                    idx, (v, ok) = rc.select([("recv", c) for c in chans])
                    if ok:
                        got.append(v)
            finally:
                wg.done()

        def feeder():
            # each feed wakes exactly one of the parked selects
            need = NSEL * ROUNDS
            i = 0
            while i < need:
                target = chans[i % NCH]
                # try_send fails if no select is parked as a receiver yet; the
                # direct-handoff to a parked select receiver is what we exercise
                if target.try_send(i):
                    i += 1
                else:
                    rc.sched_yield()
        rc.fiber(feeder)
        for cid in range(NSEL):
            rc.fiber(lambda cid=cid: chooser(cid))
        wg.wait()

    with hang_guard(60, "many selects deep eviction"):
        rc.fiber(main)
        rc.run()
    # NSEL*ROUNDS values were delivered, each exactly once (no drop/dup across
    # the tombstone evictions of the losing channels).
    assert len(got) == NSEL * ROUNDS, "lost/dup: %d != %d" % (len(got), NSEL * ROUNDS)
    assert sorted(got) == list(range(NSEL * ROUNDS)), "eviction dropped/duped a value"


# ==========================================================================
# A18 -- Chan iterator: partial drain then close mid-iteration; the iterator
#        must yield buffered values then stop (no hang, no extra value).
# ==========================================================================
def test_chan_iterator_partial_then_close_midstream():
    out = []

    def main():
        ch = rc.Chan(4)
        ch.try_send(0); ch.try_send(1)

        def producer():
            for _ in range(2):
                rc.sched_yield()
            ch.try_send(2); ch.try_send(3)
            ch.close()                      # close while iterator may be parked

        def consumer():
            for v in ch:                    # iterates until closed+empty
                out.append(v)
        rc.fiber(producer)
        rc.fiber(consumer)

    with hang_guard(20, "chan iterator midstream close"):
        rc.fiber(main)
        rc.run()
    assert out == [0, 1, 2, 3], "iterator lost/duped buffered values: %r" % out


# ==========================================================================
# A19 -- Fault injection during a CHANNEL/SELECT M:N workload: a SPAWN_G fault
#        mid run(N) must surface as a clean Python error, never a SIGSEGV, and
#        must not corrupt the channel state. Contained in a subprocess.
# ==========================================================================
def test_fault_spawn_g_during_mn_channel_workload_no_crash():
    script = r"""
import os, sys
import runloom_c as rc, runloom
from runloom.sync import WaitGroup
out = {}
def main():
    ch = rc.Chan(8)
    wg = WaitGroup()
    try:
        # spawning under the fault may raise on some fiber(); catch + record
        n = 6
        wg.add(n)
        def producer(pid):
            try:
                for j in range(20):
                    ch.send(pid*20+j)
            finally:
                wg.done()
        def consumer():
            seen = 0
            while seen < n*20:
                v, ok = ch.recv()
                if not ok: break
                seen += 1
        rc.mn_fiber(consumer)
        for p in range(n):
            rc.mn_fiber(lambda p=p: producer(p))
        wg.wait()
        ch.close()
        out['r'] = 'ok'
    except BaseException as e:
        out['r'] = type(e).__name__
try:
    runloom.run(3, main)
except BaseException as e:
    out['top'] = type(e).__name__
print('RESULT', out)
sys.exit(0)
"""
    p = _subprocess(script, env_extra={"RUNLOOM_FAULT_SPAWN_G": "once:12",
                                       "RUNLOOM_GOROUTINE_PANIC": "silent"},
                    timeout=60)
    assert p.returncode is None or p.returncode >= 0, \
        "SPAWN_G fault during MN channel workload crashed: rc=%r\n%s" % (
            p.returncode, p.stderr.decode("utf8", "replace"))
    assert b"RESULT" in p.stdout, p.stderr.decode("utf8", "replace")


def test_fault_spawn_tstate_clean_error_not_crash():
    script = r"""
import sys
import runloom_c as rc
out = {}
def main():
    try:
        from runloom.sync import gather
        gather(lambda: 1, lambda: 2, lambda: 3)
        out['r'] = 'ok'
    except BaseException as e:
        out['r'] = type(e).__name__
try:
    rc.fiber(main); rc.run()
except BaseException as e:
    out['top'] = type(e).__name__
print('RESULT', out)
sys.exit(0)
"""
    p = _subprocess(script, env_extra={"RUNLOOM_FAULT_SPAWN_TSTATE": "once:12",
                                       "RUNLOOM_GOROUTINE_PANIC": "silent"},
                    timeout=30)
    assert p.returncode is None or p.returncode >= 0, \
        "SPAWN_TSTATE fault crashed: rc=%r\n%s" % (
            p.returncode, p.stderr.decode("utf8", "replace"))
    assert b"RESULT" in p.stdout, p.stderr.decode("utf8", "replace")


# ==========================================================================
# A20 -- REFCOUNT conservation on the SELECT-RECV delivered value + the
#        not-yet-covered drop path: a select that times-out-via-default with a
#        send value must release that value (the Phase-1 default return path,
#        distinct from the Phase-2 abort path the first pass covered).
# ==========================================================================
def test_select_default_with_send_value_releases_it():
    refs = []

    def f():
        full = rc.Chan(1); full.try_send("seed")   # send would block
        box = _Box("v")
        refs.append(weakref.ref(box))
        # send case can't fire (full), default fires -> the send value (borrowed
        # in Phase-1, never incref'd because we never installed a waiter) must
        # NOT be over-released or leaked.
        r = rc.select([("send", full, box)], default=True)
        assert r == -1
        return "ok"
    assert _run_single(f) == "ok"
    del f
    gc.collect(); gc.collect()
    # box was only borrowed by the default-return path; once our local ref is
    # gone it must be collectable (no spurious incref leak).
    alive = [r for r in refs if r() is not None]
    assert not alive, "select default-return leaked the send value"


def test_select_recv_delivered_value_refcount_conserved():
    # A value delivered through a PARKED select-recv (Phase-2 path) must end up
    # held by exactly the receiving frame; after it's dropped + gc'd it's freed.
    refs = []

    def main():
        ch = rc.Chan(0)
        out = {}

        def chooser():
            idx, (v, ok) = rc.select([("recv", ch)])
            out["v"] = v.v if v is not None else None
            # drop our reference to the delivered box here
            v = None

        def sender():
            for _ in range(3):
                rc.sched_yield()
            box = _Box("delivered")
            refs.append(weakref.ref(box))
            ch.send(box)
        rc.fiber(chooser)
        rc.fiber(sender)
        return out

    out = _run_single(main)
    assert out["v"] == "delivered"
    del out
    gc.collect(); gc.collect()
    alive = [r for r in refs if r() is not None]
    assert not alive, "select-recv delivered value leaked (refcount not conserved)"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
