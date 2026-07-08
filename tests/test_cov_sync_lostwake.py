"""Coverage for two sync-lostwake gaps on runloom's fan-in primitives.

Gap 1 -- gather() with NO scheduler running.
    runloom.gather() spawns each callable as a fiber and then blocks in
    WaitGroup.wait().  Called from a bare OS thread with no run()/mn_run() live,
    those fibers queue on a single-thread ring that nothing drains, so wg.wait()
    would poll forever -> a silent forever-hang.  gather() is supposed to detect
    "no live scheduler AND not inside a fiber" and raise RuntimeError instead.
    The test wraps the call in hang_guard, so a REGRESSION (the raise removed)
    surfaces as a watchdog timeout/_exit rather than a wedged suite.

Gap 2 -- Future timeout-survivor (the stale-parker lost-wake class).
    Two (or more) fibers await one Future.  A timed awaiter parks via
    park(timeout=), times out, and MUST de-queue its handle from the waiter list
    before raising TimeoutError.  A survivor keeps waiting; a later set_result()
    must wake ONLY the survivor (the timed-out handle is gone) and the survivor
    must return the resolved value -- no lost wake, no double-wake of the
    de-queued handle, no crash.

Conventions copied from tests/test_sync_primitives.py (the `_drive` root-fiber
harness + runloom.run(hubs, ...)) and tests/test_adv_sched.py (hang_guard /
raw_thread from adv_util so a lost wake fails as a timeout, not a wedge).
"""
import re

import pytest

import runloom
import runloom_c
from runloom import sync

from adv_util import hang_guard, raw_thread


def _drive(fn, hubs=8):
    """Run `fn` as the root fiber under an M:N runtime; propagate its result /
    exception (same harness as tests/test_sync_primitives.py)."""
    box = [None, None]

    def runner():
        try:
            box[0] = fn()
        except BaseException as e:   # noqa: BLE001  (re-raised below)
            box[1] = e

    runloom.run(hubs, runner)
    if box[1] is not None:
        raise box[1]
    return box[0]


# ---- Gap 1: gather() with no running scheduler ---------------------------

def test_gather_no_scheduler_raises_on_main_thread():
    """Bare main OS thread, no run(): gather() must FAIL LOUD with a
    'running scheduler' RuntimeError, not spawn undrained fibers and hang in
    WaitGroup.wait().  hang_guard turns a missing-raise regression into a
    watchdog _exit (timeout) instead of a forever-hang."""
    with hang_guard(10, "gather no-scheduler main-thread"):
        with pytest.raises(RuntimeError, match="running scheduler"):
            runloom.gather(lambda: 1, lambda: 2, lambda: 3)


def test_gather_no_scheduler_raises_on_raw_thread():
    """Same contract from a genuine foreign OS thread (adv_util.raw_thread).
    The worker must return a caught RuntimeError promptly; if gather() instead
    hangs, the join() below times out and the not-alive assertion fails."""
    box = {}

    def worker():
        try:
            runloom.gather(lambda: 1, lambda: 2)
            box["r"] = ("no-raise",)
        except RuntimeError as e:
            box["r"] = ("raised", str(e))
        except BaseException as e:            # noqa: BLE001
            box["r"] = ("other", repr(e))

    with hang_guard(10, "gather no-scheduler raw-thread"):
        t = raw_thread(worker)
        t.join(8)

    assert not t.is_alive(), "gather() hung on a bare OS thread instead of raising"
    r = box.get("r")
    assert isinstance(r, tuple) and r[0] == "raised", r
    assert re.search("running scheduler", r[1]), r[1]


# ---- Gap 2: Future timeout-survivor (stale-parker lost-wake) -------------

def test_future_timeout_survivor_still_resolves():
    """One awaiter times out + de-queues; a survivor keeps waiting; a later
    set_result(7) must wake the survivor with 7 and never touch the de-queued
    handle."""
    def body():
        fut = runloom.Future()
        out = {}
        wg = sync.WaitGroup()
        wg.add(3)

        def timeout_waiter():
            try:
                fut.result(timeout=0.03)
                out["timeout"] = ("no-timeout",)
            except TimeoutError:
                out["timeout"] = ("timed-out",)
            except BaseException as e:        # noqa: BLE001
                out["timeout"] = ("other", repr(e))
            finally:
                wg.done()

        def survivor():
            try:
                out["survivor"] = ("ok", fut.result())
            except BaseException as e:        # noqa: BLE001
                out["survivor"] = ("err", repr(e))
            finally:
                wg.done()

        def resolver():
            # Sleep well past the 0.03 timeout so the timed-out waiter has
            # already expired AND de-queued its handle before we resolve.
            runloom.sleep(0.12)
            fut.set_result(7)
            wg.done()

        runloom.fiber(timeout_waiter)
        runloom.fiber(survivor)
        runloom.fiber(resolver)
        wg.wait()
        # Future stays intact after the survivor's wake -- a stale double-wake
        # would corrupt _done / _waiters; a re-read must still return 7.
        out["reread"] = fut.result()
        return out

    with hang_guard(15, "future timeout-survivor"):
        out = _drive(body)

    assert out["timeout"] == ("timed-out",), out
    assert out["survivor"] == ("ok", 7), out
    assert out["reread"] == 7, out


def test_future_many_timeouts_then_survivor():
    """Stronger stale-parker stress: several awaiters time out + de-queue, then
    a single survivor is resolved.  The resolve must wake ONLY the survivor;
    the de-queued handles must not receive a lost/double wake, and none may
    crash the runtime."""
    def body():
        fut = runloom.Future()
        n_timeouts = 4
        out = {"timed_out": 0, "not_timed_out": 0, "errors": []}
        wg = sync.WaitGroup()
        wg.add(n_timeouts + 2)   # n timeout-waiters + survivor + resolver

        def timeout_waiter():
            try:
                fut.result(timeout=0.03)
                out["not_timed_out"] += 1     # only ever the last-moment race
            except TimeoutError:
                out["timed_out"] += 1
            except BaseException as e:        # noqa: BLE001
                out["errors"].append(repr(e))
            finally:
                wg.done()

        def survivor():
            try:
                out["survivor"] = fut.result()
            except BaseException as e:        # noqa: BLE001
                out["survivor"] = ("err", repr(e))
            finally:
                wg.done()

        def resolver():
            runloom.sleep(0.12)               # all timeouts expired + de-queued
            fut.set_result(7)
            wg.done()

        for _ in range(n_timeouts):
            runloom.fiber(timeout_waiter)
        runloom.fiber(survivor)
        runloom.fiber(resolver)
        wg.wait()
        return out

    with hang_guard(15, "future many-timeouts survivor"):
        out = _drive(body)

    assert out["errors"] == [], out
    assert out["survivor"] == 7, out
    # Every timed waiter accounted for; none silently lost.  With the 0.12s
    # resolve gap they all time out, but tolerate a last-moment resolve that
    # legitimately hands one the value (the code's own "resolved at the last
    # moment" branch) -- what must never happen is a crash or a lost waiter.
    assert out["timed_out"] + out["not_timed_out"] == 4, out
    assert out["timed_out"] >= 1, out
