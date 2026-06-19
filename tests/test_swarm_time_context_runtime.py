"""Adversarial QA for the time / context / runtime subsystem.

OWNS: runloom.time (After/Tick/Timer/Ticker/NewTimer/NewTicker/Sleep -- Stop/
Reset + the generation counter that defeats stale fires), runloom.context
(Background/WithCancel/WithTimeout/WithDeadline + CANCELED/DEADLINE_EXCEEDED),
and runloom.runtime/__init__ (run(n,main), fiber() single-vs-M:N dispatch, sleep,
yield_now, blocking, current, the Goroutine handle, set_grow_down/
grow_down_enabled).

test_adv_timers.py / test_time.py / test_context.py already cover the happy
paths and the basic stale-fire / cascade / idempotency cases.  This file goes
DEEPER and hunts NEW conditions:

  * Timer.Stop()/Reset() stale-fire torture: the OLD deadline must NEVER fire
    across MANY Stop/Reset/fire interleavings -- reset-to-longer, reset-to-
    shorter, rapid Reset storms, Stop-after-fire, Reset-after-Stop, and the
    return-value contract on each.
  * Ticker buffer-1 backlog DROP with a slow consumer (never blocks the ticker
    fiber, never accumulates), Ticker.Reset re-arm, non-positive interval
    rejection on both __init__ AND Reset.
  * Context cancel CASCADE to grandchildren, double/triple-cancel IDEMPOTENCY,
    WithTimeout deadline-auto-fire vs manual-cancel-before-deadline error code,
    N-waiter broadcast on cancel, far-future deadline + immediate cancel, past
    deadline synchronous fire (no fiber), child of an already-cancelled parent.
  * Runtime: fiber() returns a Goroutine under run(1) and None under run(N);
    current() identity + None outside a fiber; blocking() under M:N; Goroutine
    .done/.result/.exception surfacing of a raised error; run(n) argument
    validation; nested go; sleep() outside a fiber.
  * Everything timing-asserted is wrapped in hang_guard (lost-wake => bounded
    failure) and slow-return-guarded with assert_faster_than where cooperative
    overlap is the property under test.  Crash-prone cases run in a SUBPROCESS
    so a SIGSEGV is contained + observed as a signal returncode.  Fault
    injection (SPAWN_G / FD_*) is woven into timer/context spawns.

FINDINGS (encoded as xfail / FINDING-commented subprocess assertions):
  * test_timer_stop_after_fire_should_return_false_FINDING -- Timer.Stop()
    returns True after the timer has already fired, but Go's time.Timer.Stop()
    (and this Timer's own docstring: "False if it had already fired") specify
    False.  The implementation tracks only _stopped, never "did it fire", so a
    caller using the Go-idiomatic `if !t.Stop() { <-t.C }` drain pattern would
    deadlock-drain a channel that has no pending value.
"""
import os
import subprocess
import sys
import time

import pytest

import runloom
import runloom_c as rc
import runloom.time as rt
import runloom.context as rctx

from adv_util import (
    hang_guard,
    assert_faster_than,
    raw_thread,
    needs_free_threading,
)

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYEXE = sys.executable


# ==========================================================================
# helpers
# ==========================================================================
def _run_single(fn, guard=10.0, label="single"):
    """Drive fn() to completion under the single-thread scheduler, return its
    value.  Wrapped in hang_guard so a lost wake is a bounded failure."""
    box = {}

    def main():
        box["r"] = fn()

    with hang_guard(guard, label):
        rc.fiber(main)
        rc.run()
    return box.get("r")


def _run_mn(main, n=2, guard=15.0, label="mn"):
    """Drive main() under run(n) (M:N).  main MUST spawn children via mn_go /
    runloom.go, never rc.go."""
    with hang_guard(guard, label):
        runloom.run(n, main)


def _subproc(script, timeout=40, extra_env=None):
    """Run a self-contained script in a child process so a SIGSEGV/abort is
    CONTAINED and observed as a negative returncode."""
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["PYTHONPATH"] = "src"
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        [PYEXE, "-c", script],
        cwd=REPO,
        env=env,
        timeout=timeout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _assert_no_signal(proc, what):
    """A child must never die on a signal (negative returncode)."""
    assert proc.returncode is None or proc.returncode >= 0, (
        "{0} crashed on signal {1} (negative returncode)\n--- stderr ---\n{2}"
        .format(what, -proc.returncode if proc.returncode else proc.returncode,
                proc.stderr.decode("utf-8", "replace")[-3000:]))


# Clean Python-level errors a fault-injection site is allowed to raise (the
# injected errno surfaces as one of these); the forbidden outcome is a signal
# crash / silent corruption, not a clean exception.
_CLEAN_FAULT_ERRORS = (b"MemoryError", b"OSError", b"RuntimeError",
                       b"BlockingIOError", b"OverflowError")


def _assert_clean_fault_outcome(proc, what):
    """A fault-injected run must either complete or degrade with a CLEAN Python
    error -- never a signal crash, never a hang (the subprocess timeout would
    have raised TimeoutExpired before we get here)."""
    _assert_no_signal(proc, what)
    out = proc.stdout
    err = proc.stderr
    clean_markers = (b"DONE", b"SURVIVED", b"CLEAN_ERR", b"RUN_ERR")
    if proc.returncode == 0 or any(m in out for m in clean_markers):
        return
    # A non-zero exit is acceptable IFF it was a clean Python exception (the
    # injected errno propagating out of a spawn), not a fatal signal.
    assert any(e in err for e in _CLEAN_FAULT_ERRORS), (
        "{0}: fault produced neither a clean completion nor a recognised clean "
        "Python error (rc={1})\n--- stdout ---\n{2}\n--- stderr ---\n{3}".format(
            what, proc.returncode, out.decode("utf-8", "replace")[-1500:],
            err.decode("utf-8", "replace")[-2000:]))


# ==========================================================================
# Timer -- stale-fire torture.  The generation counter (self._gen) + the
# _stopped flag are the only thing standing between a Stop()/Reset() and a
# stale OLD deadline firing into the channel.  We attack exactly that.
# ==========================================================================
def test_timer_reset_to_longer_old_deadline_never_fires():
    # Reset to a LONGER deadline; wait past the OLD one; the gen-0 fire must
    # have bailed (no stale value in the channel), and the NEW one fires later.
    def f():
        t = rt.Timer(0.02)
        t.Reset(0.30)
        rc.sched_sleep(0.10)            # past OLD (20ms), well before NEW (300ms)
        stale = t.c.try_recv()
        return stale
    assert _run_single(f, label="reset-longer") is None, \
        "old (gen-0) deadline fired after Reset to a longer interval (stale fire)"


def test_timer_reset_to_shorter_then_new_fires_only_once():
    # Reset to a SHORTER deadline: the NEW one fires, and only one value ever
    # lands (the old fiber must not also fire).
    def f():
        t = rt.Timer(0.30)
        t.Reset(0.02)
        v, ok = t.c.recv()             # NEW deadline fires
        rc.sched_sleep(0.05)           # give a hypothetical stale old fire time
        extra = t.c.try_recv()         # must be None: old gen-0 must have bailed
        return ok, v, extra
    ok, v, extra = _run_single(f, label="reset-shorter")
    assert ok is True
    assert extra is None, "a second (stale) value landed after Reset to shorter"


def test_timer_rapid_reset_storm_no_stale_pileup():
    # A storm of Reset() calls before any fire: only the FINAL generation may
    # fire, and it fires exactly once.  Earlier gens must all bail.
    def f():
        t = rt.Timer(0.50)
        for _ in range(50):
            t.Reset(0.02)              # 50 supersessions, all still sleeping
        v, ok = t.c.recv()            # exactly the last gen fires
        rc.sched_sleep(0.05)
        # Drain: every superseded gen must have bailed -> nothing left.
        leftover = 0
        while t.c.try_recv() is not None:
            leftover += 1
        return ok, leftover
    ok, leftover = _run_single(f, guard=12, label="reset-storm")
    assert ok is True
    assert leftover == 0, \
        "Reset storm let %d stale gen(s) fire into the channel" % leftover


def test_timer_stop_during_sleep_then_reset_no_double_fire():
    # Stop the timer mid-flight, then Reset it.  Only the post-Reset gen fires.
    def f():
        t = rt.Timer(0.10)
        assert t.Stop() is True        # cancel the armed gen-0
        t.Reset(0.02)                  # arm a fresh gen
        v, ok = t.c.recv()
        rc.sched_sleep(0.05)
        return ok, t.c.try_recv()
    ok, leftover = _run_single(f, label="stop-then-reset")
    assert ok is True
    assert leftover is None, "Stop()+Reset() produced a stale extra fire"


def test_timer_stop_returns_false_when_already_stopped():
    def f():
        t = rt.Timer(0.20)
        first = t.Stop()
        second = t.Stop()              # already stopped -> False
        third = t.Stop()
        return first, second, third
    first, second, third = _run_single(f, label="double-stop")
    assert first is True
    assert second is False
    assert third is False


def test_timer_reset_return_value_tracks_active_state():
    # Reset() returns whether the timer was active (not stopped) at call time.
    def f():
        t = rt.Timer(0.20)
        r_active = t.Reset(0.20)       # was active -> True
        t.Stop()
        r_stopped = t.Reset(0.02)      # was stopped -> False
        return r_active, r_stopped
    r_active, r_stopped = _run_single(f, label="reset-retval")
    assert r_active is True
    assert r_stopped is False


def test_timer_fires_exactly_once_not_repeatedly():
    # A Timer is one-shot: after it fires, no further values appear even after
    # several intervals elapse.
    def f():
        t = rt.Timer(0.02)
        v, ok = t.c.recv()
        rc.sched_sleep(0.10)           # 5x the interval
        extra = 0
        while t.c.try_recv() is not None:
            extra += 1
        return ok, extra
    ok, extra = _run_single(f, label="timer-once")
    assert ok is True
    assert extra == 0, "Timer fired %d extra times (not one-shot)" % extra


# REGRESSION (was finding #13): Timer.Stop() now returns False after the timer
# has already fired, matching Go's time.Timer.Stop() and the docstring -- the
# Timer tracks a _fired flag set when fire() sends, so the Go-idiomatic
# `if not t.Stop(): <-t.c` drain pattern is safe.
def test_timer_stop_after_fire_should_return_false_FINDING():
    def f():
        t = rt.Timer(0.02)
        v, ok = t.c.recv()             # let it FIRE
        assert ok is True
        return t.Stop()                # Go contract: already fired -> False
    assert _run_single(f, label="stop-after-fire") is False, \
        "Stop() after fire returned True; Go/docstring contract is False"


# ==========================================================================
# Ticker -- buffer-1 backlog DROP, Reset re-arm, interval validation.
# ==========================================================================
def test_ticker_slow_consumer_drops_backlog_never_blocks():
    # A fast ticker (5ms) with a slow consumer (25ms) must DROP backlog: the
    # ticker fiber never blocks (try_send), the channel never accumulates past
    # its buffer of 1.  Over ~120ms a slow 25ms consumer sees far fewer than
    # the ~24 ticks the ticker produced.
    def f():
        tk = rt.Ticker(0.005)
        seen = 0
        t0 = time.monotonic()
        while time.monotonic() - t0 < 0.12:
            v, ok = tk.c.recv()
            if ok:
                seen += 1
            rc.sched_sleep(0.025)
        tk.Stop()
        return seen
    seen = _run_single(f, guard=12, label="ticker-drop")
    # ~4-5 slow reads; nowhere near ~24 produced -> backlog dropped.
    assert 1 <= seen <= 12, \
        "ticker drop/block broken: saw %d ticks (slow consumer)" % seen


def test_ticker_reset_rearms_after_stop():
    # Stop() halts the ticker; Reset() must re-arm it onto a fresh generation.
    def f():
        tk = rt.Ticker(0.005)
        n = 0
        for _ in range(2):
            _v, ok = tk.c.recv()
            n += ok
        tk.Stop()
        tk.Reset(0.005)                # re-arm
        for _ in range(2):
            _v, ok = tk.c.recv()
            n += ok
        tk.Stop()
        return n
    n = _run_single(f, guard=12, label="ticker-reset")
    assert n == 4, "Ticker.Reset() did not re-arm (got %d/4 ticks)" % n


def test_ticker_stop_halts_no_unbounded_accumulation():
    # After Stop(), at most one buffered tick remains and NO new ticks land
    # even after several intervals.
    def f():
        tk = rt.Ticker(0.005)
        for _ in range(3):
            tk.c.recv()
        tk.Stop()
        rc.sched_sleep(0.06)           # >10 intervals
        leftover = 0
        while tk.c.try_recv() is not None:
            leftover += 1
        return leftover
    leftover = _run_single(f, guard=12, label="ticker-stop")
    assert leftover <= 1, \
        "Ticker kept firing after Stop() (%d leftover)" % leftover


def test_ticker_nonpositive_interval_rejected_on_init_and_reset():
    # __init__ AND Reset must reject <= 0 with ValueError.
    with pytest.raises(ValueError):
        rt.Ticker(0)
    with pytest.raises(ValueError):
        rt.Ticker(-0.5)
    with pytest.raises(ValueError):
        rt.NewTicker(0)

    def f():
        tk = rt.Ticker(0.01)
        raised = []
        for bad in (0, -1, -0.001):
            try:
                tk.Reset(bad)
            except ValueError:
                raised.append(bad)
        tk.Stop()
        return raised
    raised = _run_single(f, label="ticker-reset-validate")
    assert raised == [0, -1, -0.001], \
        "Ticker.Reset() failed to reject non-positive intervals: %r" % (raised,)


# ==========================================================================
# After / Tick
# ==========================================================================
def test_after_fires_value_then_closes():
    # After(d): recv #1 -> (d, True); recv #2 -> (None, False) (closed).
    def f():
        ch = rt.After(0.02)
        first = ch.recv()
        second = ch.recv()
        return first, second
    first, second = _run_single(f, label="after")
    assert first[1] is True and first[0] == 0.02
    assert second == (None, False), "After channel did not close after firing"


def test_after_fires_only_once_in_select():
    # Selecting work-vs-After: when After wins, it fires once.  Use a never-
    # ready work channel so After is the only path that can fire.
    def f():
        work = rc.Chan(1)
        timeout = rt.After(0.02)
        idx, _ = rc.select([("recv", work), ("recv", timeout)])
        return idx
    idx = _run_single(f, label="after-select")
    assert idx == 1, "After branch did not win the select (idx=%d)" % idx


def test_tick_channel_fires_repeatedly():
    # Tick(d) is sugar for NewTicker(d).c -- it ticks.  (No stop handle; we
    # just consume a few and let run() drain the loop fiber when main exits...
    # except the loop never exits.  So bound it: consume N then assert.)
    def f():
        c = rt.Tick(0.005)
        n = 0
        for _ in range(3):
            _v, ok = c.recv()
            n += ok
        return n
    # NB: the underlying Ticker has no Stop handle so its loop fiber would keep
    # the scheduler alive; run it in a subprocess so a never-draining loop is a
    # bounded TIMEOUT, not a wedged in-process test.
    script = r"""
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
import runloom.time as rt
out = {}
def main():
    c = rt.Tick(0.005)
    n = 0
    for _ in range(3):
        _v, ok = c.recv()
        n += ok
    out["n"] = n
    # Tick has no stop -> the loop fiber would keep run() alive forever.  Exit
    # the process from inside the fiber once we've proven it ticks.
    print("TICKS", n)
    sys.stdout.flush()
    import os; os._exit(0)
rc.fiber(main); rc.run()
"""
    proc = _subproc(script, timeout=20)
    out = proc.stdout.decode().strip()
    assert "TICKS 3" in out, "Tick() did not fire 3 times: %r / %r" % (
        out, proc.stderr.decode()[-500:])


# ==========================================================================
# Context -- cascade, idempotency, deadline-vs-cancel, broadcast.
# ==========================================================================
def test_context_cancel_cascades_to_grandchildren():
    def f():
        root, rc_cancel = rctx.WithCancel(rctx.Background())
        child, _ = rctx.WithCancel(root)
        gchild, _ = rctx.WithCancel(child)
        ggchild, _ = rctx.WithCancel(gchild)
        assert all(c.err() is None for c in (child, gchild, ggchild))
        rc_cancel()
        # Every descendant's done channel must close + err set.
        for c in (child, gchild, ggchild):
            c.done.recv()
        return tuple(c.err() for c in (child, gchild, ggchild))
    errs = _run_single(f, label="cascade")
    assert errs == (rctx.CANCELED,) * 3, \
        "cancel cascade did not reach all descendants: %r" % (errs,)


def test_context_triple_cancel_idempotent_no_close_on_closed():
    # A naive _cancel that close()s the done channel without an err-guard would
    # raise "close on closed channel" on the 2nd/3rd call.
    def f():
        ctx, cancel = rctx.WithCancel(rctx.Background())
        cancel(); cancel(); cancel()
        return ctx.err(), ctx.done.closed
    err, closed = _run_single(f, label="triple-cancel")
    assert err == rctx.CANCELED and closed is True


def test_context_cancel_child_does_not_cancel_parent():
    # Cancellation flows DOWN only, never up.  Cancelling a child must leave
    # the parent (and a sibling) active.
    def f():
        parent, _ = rctx.WithCancel(rctx.Background())
        a, a_cancel = rctx.WithCancel(parent)
        b, _ = rctx.WithCancel(parent)
        a_cancel()
        return parent.err(), a.err(), b.err()
    perr, aerr, berr = _run_single(f, label="no-upward-cancel")
    assert perr is None, "cancelling a child cancelled the parent"
    assert aerr == rctx.CANCELED
    assert berr is None, "cancelling one child cancelled a sibling"


def test_context_withtimeout_auto_fire_is_deadline_exceeded():
    def f():
        ctx, cancel = rctx.WithTimeout(rctx.Background(), 0.03)
        t0 = time.monotonic()
        ctx.done.recv()
        el = time.monotonic() - t0
        err = ctx.err()
        cancel()                       # idempotent post-fire
        return el, err
    el, err = _run_single(f, label="timeout-auto")
    assert err == rctx.DEADLINE_EXCEEDED
    assert 0.02 < el < 0.5, "deadline fired at the wrong time (%.3fs)" % el


def test_context_manual_cancel_before_deadline_is_cancelled():
    # Manual cancel BEFORE a far deadline -> err is CANCELED (cancelled wins),
    # and ctx.done.recv() returns PROMPTLY (the close happened synchronously in
    # cancel()).  We measure the recv latency *inside* the fiber: the context
    # itself is correct and fast.  NB: run() will still linger until the orphan
    # deadline fiber's sleep elapses -- that lingering is a separate FINDING
    # (test_context_cancel_leaves_deadline_fiber_lingering_FINDING); here we
    # prove the cancellation path is prompt, so we exit the process once we have
    # the measurement rather than waiting for run() to drain the orphan.
    script = r"""
import sys, os, time; sys.path.insert(0, "src")
import runloom, runloom_c as rc
import runloom.context as rctx
def main():
    ctx, cancel = rctx.WithTimeout(rctx.Background(), 5.0)
    cancel()
    t0 = time.monotonic()
    ctx.done.recv()
    el = time.monotonic() - t0
    print("RECV_EL %.6f ERR %s" % (el, ctx.err()))
    sys.stdout.flush()
    os._exit(0)
rc.fiber(main); rc.run()
"""
    proc = _subproc(script, timeout=15)
    _assert_no_signal(proc, "manual-before-deadline")
    out = proc.stdout.decode()
    assert "ERR cancelled" in out, "expected CANCELED: %r / %r" % (
        out, proc.stderr.decode()[-500:])
    el = float(out.split("RECV_EL")[1].split()[0])
    assert el < 2.0, "ctx.done.recv() after cancel was slow: %.3fs" % el


def test_context_far_future_deadline_immediate_cancel():
    # Far-future (1hr) deadline + immediate cancel: ctx.done.recv() must return
    # at once with CANCELED, never wait for the deadline -- a slow-return guard.
    # Same orphan-fiber caveat as above, so measure recv inside the fiber + exit.
    script = r"""
import sys, os, time; sys.path.insert(0, "src")
import runloom, runloom_c as rc
import runloom.context as rctx
def main():
    ctx, cancel = rctx.WithDeadline(rctx.Background(), time.monotonic() + 3600.0)
    cancel()
    t0 = time.monotonic()
    ctx.done.recv()
    el = time.monotonic() - t0
    print("RECV_EL %.6f ERR %s" % (el, ctx.err()))
    sys.stdout.flush()
    os._exit(0)
rc.fiber(main); rc.run()
"""
    proc = _subproc(script, timeout=15)
    _assert_no_signal(proc, "far-future-cancel")
    out = proc.stdout.decode()
    assert "ERR cancelled" in out, out + proc.stderr.decode()[-500:]
    el = float(out.split("RECV_EL")[1].split()[0])
    assert el < 2.0, "far-future recv after cancel was slow: %.3fs" % el


# REGRESSION (was finding #14): WithTimeout/WithDeadline cancel() now stops the
# deadline waker fiber immediately -- the waker parks in a cancellable wait_fd
# (not a bare sched_sleep), and _cancel() wakes it via cancel_wait_fd, so a
# cancelled context's run() returns at once instead of lingering to the
# original deadline.
def test_context_cancel_leaves_deadline_fiber_lingering_FINDING():
    # The CORRECT behavior: after cancel(), run() should return promptly because
    # the deadline fiber was stopped.  It currently lingers for the full timeout,
    # so this slow-return assertion fails (xfail).  A short 1.0s timeout keeps
    # the demonstration bounded even when it lingers.
    def f():
        ctx, cancel = rctx.WithTimeout(rctx.Background(), 1.0)
        cancel()                       # logically done immediately
        ctx.done.recv()
        return ctx.err()
    t0 = time.monotonic()
    err = _run_single(f, guard=10, label="leak-finding")
    el = time.monotonic() - t0
    assert err == rctx.CANCELED
    assert el < 0.5, (
        "run() lingered %.3fs after cancel -- the deadline fiber was not "
        "stopped and slept to its original 1.0s deadline" % el)


def test_context_past_deadline_fires_synchronously_no_fiber():
    # A deadline already in the past must cancel SYNCHRONOUSLY at construction
    # (no fiber spawned), with DEADLINE_EXCEEDED, before any scheduler runs.
    ctx, cancel = rctx.WithDeadline(rctx.Background(), time.monotonic() - 1.0)
    assert ctx.err() == rctx.DEADLINE_EXCEEDED
    assert ctx.done.closed is True
    cancel()                           # idempotent
    assert ctx.err() == rctx.DEADLINE_EXCEEDED


def test_context_child_of_cancelled_parent_is_born_cancelled():
    # Wiring a new child under an already-cancelled parent must propagate the
    # parent's err immediately (the _CancelCtx __init__ checks parent._err).
    parent, pcancel = rctx.WithCancel(rctx.Background())
    pcancel()
    child, _ = rctx.WithCancel(parent)
    assert child.err() == rctx.CANCELED
    assert child.done.closed is True
    # A deadline child of a cancelled parent: cancelled, not deadline.
    dchild, _ = rctx.WithTimeout(parent, 5.0)
    assert dchild.err() == rctx.CANCELED


def test_context_parent_tighter_deadline_wins():
    # Child asks for a far deadline; parent's tighter deadline must win and the
    # child fires at the parent's time.
    def f():
        parent, _ = rctx.WithTimeout(rctx.Background(), 0.02)
        child, _ = rctx.WithTimeout(parent, 5.0)
        t0 = time.monotonic()
        child.done.recv()
        return child.err(), time.monotonic() - t0
    err, el = _run_single(f, label="tighter-parent")
    assert err == rctx.DEADLINE_EXCEEDED
    assert el < 1.0, "child ignored the tighter parent deadline (%.3fs)" % el


def test_context_multi_waiter_broadcast_all_wake():
    # N fibers all selecting on ctx.done must ALL wake on a single cancel
    # (broadcast on channel close), and promptly.
    N = 32
    woke = []

    def main():
        ctx, cancel = rctx.WithCancel(rctx.Background())

        def waiter(i):
            rc.select([("recv", ctx.done)])
            woke.append(i)

        for i in range(N):
            rc.fiber(lambda i=i: waiter(i))
        rc.sched_yield()               # everyone parks on ctx.done
        cancel()

    with hang_guard(15, "broadcast"):
        with assert_faster_than(3.0, "%d-waiter broadcast wake" % N):
            rc.fiber(main)
            rc.run()
    assert len(woke) == N, \
        "cancel woke only %d/%d done-waiters" % (len(woke), N)


def test_context_background_never_cancels():
    # Background.err() is always None; its done never closes; deadline absent.
    bg = rctx.Background()
    assert bg.err() is None
    assert bg.deadline() == (None, False)
    assert rctx.Background() is bg, "Background() should return the singleton"


def test_context_deadline_accessor_reports_value():
    future = time.monotonic() + 10.0
    ctx, cancel = rctx.WithDeadline(rctx.Background(), future)
    dl, has = ctx.deadline()
    assert has is True and abs(dl - future) < 1e-6
    cancel()


# ==========================================================================
# Runtime -- fiber() dispatch, current(), blocking(), Goroutine handle.
# ==========================================================================
def test_run1_fiber_returns_fiber_handle_with_result():
    box = {}

    def main():
        g = runloom.fiber(lambda: 6 * 7)
        assert isinstance(g, runloom.Goroutine)
        # drain so the child completes
        for _ in range(4):
            rc.sched_yield()
        box["done"] = g.done
        box["result"] = g.result
        box["exc"] = g.exception

    with hang_guard(10, "run1-handle"):
        runloom.run(1, main)
    assert box["done"] is True
    assert box["result"] == 42
    assert box["exc"] is None


def test_run1_fiber_exception_surfaces_on_handle():
    # A raised error inside a fiber must surface on .exception (and be
    # silenced from the unraisable hook via RUNLOOM_GOROUTINE_PANIC=silent).
    script = r"""
import sys, os; sys.path.insert(0, "src")
os.environ["RUNLOOM_GOROUTINE_PANIC"] = "silent"
import runloom, runloom_c as rc
out = {}
def boom():
    raise ValueError("intentional")
def main():
    g = runloom.fiber(boom)
    for _ in range(8):
        rc.sched_yield()
    out["done"] = g.done
    out["exc"] = repr(g.exception)
runloom.run(1, main)
assert out["done"] is True, out
assert "ValueError" in out["exc"] and "intentional" in out["exc"], out
print("OK", out["exc"])
"""
    proc = _subproc(script, timeout=30)
    _assert_no_signal(proc, "goroutine-exception")
    assert proc.returncode == 0, proc.stderr.decode()[-2000:]
    assert b"OK" in proc.stdout, proc.stdout + proc.stderr


def test_current_identity_inside_and_none_outside():
    # current() outside any fiber is None; inside, it is a non-None handle for
    # the running fiber.  Each current_g() call returns a FRESH Python wrapper,
    # but the handles compare EQUAL (==) when they wrap the same C fiber, and
    # UNEQUAL across distinct fibers.  (Comparing id() would be wrong -- the
    # wrappers are distinct objects; the equality is at the C-g level.)
    assert runloom.current() is None, "current() outside a fiber must be None"

    seen = {}

    def main():
        seen["self1"] = runloom.current()
        assert seen["self1"] is not None
        # identity (==) is stable across a yield within the same fiber
        rc.sched_yield()
        seen["self2"] = runloom.current()

        handles = []

        def child(i):
            handles.append(runloom.current())

        runloom.fiber(lambda: child(0))
        runloom.fiber(lambda: child(1))
        for _ in range(4):
            rc.sched_yield()
        seen["handles"] = handles
        seen["main_self"] = runloom.current()

    with hang_guard(10, "current"):
        runloom.run(1, main)
    assert seen["self1"] is not None
    assert seen["self1"] == seen["self2"], \
        "current() identity changed within a fiber across a yield"
    # the two children ran as distinct fibers, each distinct from main
    assert len(seen["handles"]) == 2
    h0, h1 = seen["handles"]
    assert h0 != h1, "two distinct fibers reported the same current() identity"
    assert h0 != seen["main_self"] and h1 != seen["main_self"], \
        "a child fiber reported the parent's current() identity"


def test_sleep_outside_fiber_falls_back_to_time_sleep():
    # runloom.sleep() outside a fiber must NOT crash / hang; it falls back to
    # time.sleep.
    t0 = time.monotonic()
    runloom.sleep(0.02)
    el = time.monotonic() - t0
    assert el >= 0.015, "sleep() outside a fiber returned too early (%.3fs)" % el


def test_sleep_inside_fiber_yields_to_siblings():
    # sleep() inside a fiber must overlap with siblings: two fibers each
    # sleeping 50ms finish in ~50ms total, not ~100ms (cooperative overlap).
    order = []

    def main():
        def s(tag, d):
            runloom.sleep(d)
            order.append(tag)
        runloom.fiber(lambda: s("a", 0.05))
        runloom.fiber(lambda: s("b", 0.05))

    with hang_guard(10, "sleep-overlap"):
        with assert_faster_than(0.5, "two overlapping 50ms sleeps"):
            runloom.run(1, main)
    assert set(order) == {"a", "b"}


def test_yield_now_is_a_scheduling_point():
    # yield_now lets a sibling run between two appends; without a yield the
    # first fiber would run to completion first.
    seq = []

    def main():
        def a():
            seq.append("a1")
            runloom.yield_now()
            seq.append("a2")
        def b():
            seq.append("b1")
            runloom.yield_now()
            seq.append("b2")
        runloom.fiber(a)
        runloom.fiber(b)

    with hang_guard(10, "yield"):
        runloom.run(1, main)
    # a1 and b1 both happen before a2/b2 -> the yield interleaved them.
    assert seq.index("b1") < seq.index("a2"), \
        "yield_now did not yield to the sibling: %r" % seq


def test_nested_fiber_under_run1():
    # A fiber that spawns a fiber that spawns a fiber -- all must run.
    hits = []

    def main():
        def lvl3():
            hits.append(3)
        def lvl2():
            hits.append(2)
            runloom.fiber(lvl3)
        def lvl1():
            hits.append(1)
            runloom.fiber(lvl2)
        runloom.fiber(lvl1)

    with hang_guard(10, "nested-go"):
        runloom.run(1, main)
    assert sorted(hits) == [1, 2, 3], "nested fiber() did not run all levels: %r" % hits


# ==========================================================================
# Runtime -- run(n) argument validation (unvalidated-input crash hunt).
# ==========================================================================
@pytest.mark.parametrize("bad", [0, -1, -100, True, False, 1.5, "1", None, [1]])
def test_run_rejects_invalid_n(bad):
    # n must be a real int >= 1.  bool is explicitly excluded (run(True) would
    # otherwise mean run(1)).  None/str/float/list must all raise, not crash.
    with pytest.raises((ValueError, TypeError)):
        runloom.run(bad, lambda: None)


def test_run_rejects_noncallable_main():
    with pytest.raises(TypeError):
        runloom.run(1, 123)
    with pytest.raises(TypeError):
        runloom.run(1, "not callable")


def test_run_main_none_is_drain_only():
    # run(1) with no main_fn drains whatever you've already fiber()'d.
    hits = []
    rc.fiber(lambda: hits.append("drained"))
    with hang_guard(10, "drain-only"):
        runloom.run(1)
    assert hits == ["drained"]


def test_run_n_gt_1_on_gil_build_raises():
    # run(n>1) needs the GIL off.  On a GIL build it must RAISE (never silently
    # serialize).  Force the GIL on in a subprocess and assert the RuntimeError.
    script = r"""
import sys; sys.path.insert(0, "src")
import runloom
try:
    runloom.run(4, lambda: None)
    print("NO_RAISE")
except RuntimeError as e:
    print("RAISED" if "GIL" in str(e) or "free-threaded" in str(e) else "WRONG")
"""
    # PYTHON_GIL=1 forces the GIL on even on the 3.13t build.
    proc = _subproc(script, timeout=30, extra_env={"PYTHON_GIL": "1"})
    _assert_no_signal(proc, "run-gil-build")
    assert b"RAISED" in proc.stdout, \
        "run(n>1) on a GIL build did not raise: %r / %r" % (
            proc.stdout, proc.stderr[-500:])


# ==========================================================================
# grow_down toggle API
# ==========================================================================
def test_grow_down_toggle_roundtrips():
    orig = runloom.grow_down_enabled()
    try:
        runloom.set_grow_down(False)
        assert runloom.grow_down_enabled() is False
        runloom.set_grow_down(True)
        assert runloom.grow_down_enabled() is True
        runloom.set_grow_down(0)
        assert runloom.grow_down_enabled() is False   # truthiness coerced to bool
    finally:
        runloom.set_grow_down(orig)
    assert runloom.grow_down_enabled() == bool(orig)


# ==========================================================================
# M:N -- timers / contexts / runtime under run(n>=2) with slow-return guards.
# The time/context _spawn() routes through mn_go when mn_hub_count()>0, so
# these exercise the cross-hub timer-fiber path the single-thread tests can't.
# ==========================================================================
@pytest.mark.skipif(not needs_free_threading(),
                    reason="M:N needs GIL-disabled build")
def test_timer_fires_under_mn():
    box = {}

    def main():
        t = rt.Timer(0.03)
        v, ok = t.c.recv()
        box["ok"] = ok

    _run_mn(main, n=2, label="mn-timer")
    assert box.get("ok") is True, "Timer did not fire under M:N"


@pytest.mark.skipif(not needs_free_threading(),
                    reason="M:N needs GIL-disabled build")
def test_timer_reset_no_stale_fire_under_mn():
    # The stale-fire defense must hold under M:N too, where the old + new fire
    # fibers can run on DIFFERENT hubs concurrently.
    box = {}

    def main():
        t = rt.Timer(0.02)
        t.Reset(0.30)
        runloom.sleep(0.10)            # past OLD, before NEW
        box["stale"] = t.c.try_recv()

    _run_mn(main, n=4, guard=15, label="mn-reset-stale")
    assert box.get("stale") is None, \
        "old deadline fired after Reset under M:N (cross-hub stale fire)"


@pytest.mark.skipif(not needs_free_threading(),
                    reason="M:N needs GIL-disabled build")
def test_context_timeout_under_mn_deadline_exceeded():
    box = {}

    def main():
        ctx, cancel = rctx.WithTimeout(rctx.Background(), 0.03)
        ctx.done.recv()
        box["err"] = ctx.err()
        cancel()

    _run_mn(main, n=3, label="mn-timeout")
    assert box.get("err") == rctx.DEADLINE_EXCEEDED, \
        "WithTimeout did not auto-fire under M:N (err=%r)" % box.get("err")


@pytest.mark.skipif(not needs_free_threading(),
                    reason="M:N needs GIL-disabled build")
def test_context_cascade_broadcast_under_mn():
    # Cancel cascade + N-waiter broadcast across hubs.  All grandchild waiters
    # must wake on the root cancel, regardless of which hub they parked on.
    N = 24
    woke = []

    def main():
        root, root_cancel = rctx.WithCancel(rctx.Background())
        child, _ = rctx.WithCancel(root)

        def waiter(i):
            rc.select([("recv", child.done)])
            woke.append(i)

        for i in range(N):
            runloom.fiber(lambda i=i: waiter(i))   # mn_go under M:N
        runloom.sleep(0.02)            # let all park
        root_cancel()                  # cancel the ROOT -> cascades to child

    with hang_guard(20, "mn-cascade-broadcast"):
        runloom.run(4, main)
    assert len(woke) == N, \
        "M:N cascade woke only %d/%d grandchild waiters" % (len(woke), N)


@pytest.mark.skipif(not needs_free_threading(),
                    reason="M:N needs GIL-disabled build")
def test_blocking_under_mn_overlaps():
    # blocking() offloads to the pool so the fiber's hub keeps serving others.
    # Two fibers each doing a 50ms blocking() call finish in ~50ms, not ~100ms.
    results = []

    def main():
        def worker(tag):
            r = runloom.blocking(lambda: (time.sleep(0.05), tag)[1])
            results.append(r)
        runloom.fiber(lambda: worker("x"))
        runloom.fiber(lambda: worker("y"))

    with hang_guard(15, "mn-blocking"):
        with assert_faster_than(2.0, "two overlapping blocking() offloads"):
            runloom.run(2, main)
    assert sorted(results) == ["x", "y"]


@pytest.mark.skipif(not needs_free_threading(),
                    reason="M:N needs GIL-disabled build")
def test_fiber_returns_none_under_mn():
    box = {}

    def main():
        box["ret"] = runloom.fiber(lambda: None)
        box["cur"] = runloom.current() is not None

    _run_mn(main, n=2, label="mn-go-none")
    assert box.get("ret") is None, "fiber() under M:N must return None"
    assert box.get("cur") is True, "current() must be non-None inside an M:N fiber"


@pytest.mark.skipif(not needs_free_threading(),
                    reason="M:N needs GIL-disabled build")
def test_ticker_drop_under_mn():
    # Buffer-1 drop semantics hold under M:N with a slow consumer.
    box = {}

    def main():
        tk = rt.Ticker(0.005)
        seen = 0
        t0 = time.monotonic()
        while time.monotonic() - t0 < 0.10:
            _v, ok = tk.c.recv()
            seen += ok
            runloom.sleep(0.025)
        tk.Stop()
        box["seen"] = seen

    _run_mn(main, n=3, guard=15, label="mn-ticker-drop")
    assert 1 <= box.get("seen", 0) <= 12, \
        "M:N ticker drop broken: %d ticks" % box.get("seen")


# ==========================================================================
# Fault injection -- weaponize the runtime's built-in sites against the
# timer/context backing-fiber spawns.  A clean Python error is fine; a crash
# is not.  Run in subprocesses so a SIGSEGV is contained.
# ==========================================================================
def test_spawn_g_fault_during_timer_does_not_crash():
    # RUNLOOM_FAULT_SPAWN_G once: fail the next fiber spawn.  After/Timer/
    # context all spawn a backing fiber; the failure must surface as a clean
    # Python error, never a segfault.
    script = r"""
import sys, os; sys.path.insert(0, "src")
os.environ["RUNLOOM_GOROUTINE_PANIC"] = "silent"
import runloom, runloom_c as rc
import runloom.time as rt
def main():
    try:
        # the After() spawn may be the one that hits the injected fault
        ch = rt.After(0.01)
        # if the spawn survived, this fiber tries to consume it
        for _ in range(20):
            rc.sched_yield()
            if ch.try_recv() is not None:
                break
    except Exception as e:
        print("CLEAN_ERR", type(e).__name__)
        return
    print("NO_FAULT_OR_SURVIVED")
rc.fiber(main)
try:
    rc.run()
except Exception as e:
    print("RUN_ERR", type(e).__name__)
print("DONE")
"""
    proc = _subproc(script, timeout=30,
                    extra_env={"RUNLOOM_FAULT_SPAWN_G": "once:12"})
    _assert_clean_fault_outcome(proc, "spawn_g-fault-timer")


def test_spawn_stack_fault_during_context_does_not_crash():
    # RUNLOOM_FAULT_SPAWN_STACK: fail the stack reservation of the next spawn.
    # The WithTimeout deadline fiber spawn must degrade cleanly, not corrupt.
    script = r"""
import sys, os; sys.path.insert(0, "src")
os.environ["RUNLOOM_GOROUTINE_PANIC"] = "silent"
import runloom, runloom_c as rc
import runloom.context as rctx
def main():
    try:
        ctx, cancel = rctx.WithTimeout(rctx.Background(), 0.02)
        for _ in range(30):
            rc.sched_yield()
            if ctx.done.closed:
                break
        cancel()
    except Exception as e:
        print("CLEAN_ERR", type(e).__name__)
        return
    print("SURVIVED")
rc.fiber(main)
try:
    rc.run()
except Exception as e:
    print("RUN_ERR", type(e).__name__)
print("DONE")
"""
    proc = _subproc(script, timeout=30,
                    extra_env={"RUNLOOM_FAULT_SPAWN_STACK": "once:12"})
    _assert_clean_fault_outcome(proc, "spawn_stack-fault-context")


def test_spawn_tstate_fault_does_not_crash():
    script = r"""
import sys, os; sys.path.insert(0, "src")
os.environ["RUNLOOM_GOROUTINE_PANIC"] = "silent"
import runloom, runloom_c as rc
import runloom.time as rt
def main():
    try:
        t = rt.Timer(0.01)
        for _ in range(20):
            rc.sched_yield()
            if t.c.try_recv() is not None:
                break
    except Exception as e:
        print("CLEAN_ERR", type(e).__name__); return
    print("SURVIVED")
rc.fiber(main)
try:
    rc.run()
except Exception as e:
    print("RUN_ERR", type(e).__name__)
print("DONE")
"""
    proc = _subproc(script, timeout=30,
                    extra_env={"RUNLOOM_FAULT_SPAWN_TSTATE": "once:12"})
    _assert_clean_fault_outcome(proc, "spawn_tstate-fault")


# ==========================================================================
# Resource-exhaustion / scale -- many concurrent timers + contexts.  A leaked
# backing fiber or a lost wake at scale wedges run(); hang_guard bounds it.
# ==========================================================================
def test_many_timers_all_fire_no_lost_wake():
    N = 200
    fired = []

    def main():
        timers = [rt.Timer(0.01 + (i % 5) * 0.002) for i in range(N)]
        for t in timers:
            v, ok = t.c.recv()
            fired.append(ok)

    with hang_guard(20, "many-timers"):
        rc.fiber(main)
        rc.run()
    assert len(fired) == N and all(fired), \
        "only %d/%d timers fired (lost wake at scale)" % (sum(fired), N)


def test_many_contexts_cancel_all_wake_no_leak():
    N = 200
    woke = []

    def main():
        ctxs = []
        for _ in range(N):
            ctx, cancel = rctx.WithCancel(rctx.Background())
            ctxs.append((ctx, cancel))

        def waiter(ctx, i):
            rc.select([("recv", ctx.done)])
            woke.append(i)

        for i, (ctx, _c) in enumerate(ctxs):
            rc.fiber(lambda ctx=ctx, i=i: waiter(ctx, i))
        rc.sched_yield()
        for _ctx, cancel in ctxs:
            cancel()

    with hang_guard(20, "many-contexts"):
        with assert_faster_than(5.0, "%d context cancels" % N):
            rc.fiber(main)
            rc.run()
    assert len(woke) == N, \
        "only %d/%d context waiters woke" % (len(woke), N)


def test_timer_stop_then_no_fire_at_scale():
    # Stop a large batch of timers BEFORE they fire; none may leak a value.
    N = 200

    def main():
        timers = [rt.Timer(0.05) for _ in range(N)]
        for t in timers:
            assert t.Stop() is True
        rc.sched_sleep(0.10)           # past the original deadline
        leaked = sum(1 for t in timers if t.c.try_recv() is not None)
        return leaked

    with hang_guard(20, "stop-at-scale"):
        leaked = _run_single(main, guard=20, label="stop-scale")
    assert leaked == 0, "%d stopped timers fired anyway (stale fires)" % leaked


# ==========================================================================
# foreign-OS-thread: sleep()/current() from a genuine non-fiber thread must
# not crash or lazily allocate scheduler state.
# ==========================================================================
def test_current_and_sleep_from_foreign_thread():
    out = {}

    def foreign():
        out["cur"] = runloom.current()
        t0 = time.monotonic()
        runloom.sleep(0.02)            # outside a fiber -> time.sleep fallback
        out["el"] = time.monotonic() - t0

    th = raw_thread(foreign)
    th.join(5.0)
    assert not th.is_alive(), "foreign thread hung in sleep()/current()"
    assert out.get("cur") is None, "current() on a foreign thread must be None"
    assert out.get("el", 0) >= 0.015


# ==========================================================================
# AUGMENTATION (adversarial critic pass) -- conditions the first pass missed.
#
# Gaps found:
#   * Argument-validation ASYMMETRY: Ticker validates its interval eagerly in
#     __init__/Reset, but Timer/After/Sleep validate NOTHING -- a garbage
#     duration (None/str/list) is accepted by the constructor and the TypeError
#     is deferred to the DETACHED backing fiber, surfacing only via
#     unraisablehook while the timer silently never fires.  FINDING.
#   * RE-ENTRANCY: run(1,...) nested inside a single-thread fiber WORKS (drives
#     the same scheduler), but run(n>1,...) nested inside an M:N hub fiber HANGS
#     -- the public run() API does not reject re-entrant mn_init and deadlocks
#     instead of raising.  FINDING (bounded subprocess).
#   * INTEGRITY (not just counts): set-equality on WHICH timers fired / WHICH
#     contexts woke, and the actual fired VALUE, across mixed Stop/fire
#     interleavings -- the first pass mostly counted.
#   * Reset semantics: Reset does NOT drain a buffered stale value (Go parity);
#     a child requesting a SHORTER deadline than its parent keeps its own and
#     does not tighten the parent.
#   * Concurrent cancel RACE under M:N (many fibers cancel one ctx -> no
#     close-on-closed crash), signal interruption of a parked recv (no crash /
#     no hang), env-gated SYSMON/PREEMPT/HANDOFF + MN_BARRIER replay driving a
#     timer+context+CPU workload (the first pass exercised NONE of these),
#     fault injection on the SCALE + cascade paths, Goroutine handle read
#     PRE-completion, run() called twice sequentially, blocking()/yield_now()/
#     sleep() OUTSIDE a fiber, current() returning the bare C-G (not the wrapper)
#     and Ticker stale-fire torture (the first pass only tortured Timer).
# ==========================================================================


# ----- argument validation: Timer/After accept garbage, defer the error -----
# REGRESSION (was finding #12): Timer(d)/After(d)/Sleep(d) now validate the
# duration eagerly via _check_duration() -- a non-numeric duration raises
# TypeError at the call site instead of vanishing into the backing fiber and
# leaving the timer silently never firing.  (0/negative remain valid, as in Go;
# this is a type check, not Ticker's positivity check.)
def test_timer_nonnumeric_duration_should_raise_at_call_site_FINDING():
    # CORRECT behavior: rt.Timer(None) raises a TypeError at construction.
    # Currently it does not (the error escapes into the backing fiber), so this
    # xfails.  Run under a captured unraisablehook so the deferred fiber error
    # doesn't pollute output, and bound the never-firing timer.
    def f():
        captured = []
        prev = sys.unraisablehook
        sys.unraisablehook = lambda a: captured.append(a)
        try:
            raised = None
            try:
                t = rt.Timer(None)        # Go/Ticker contract: reject non-numeric
                for _ in range(8):
                    rc.sched_yield()
            except TypeError as e:
                raised = e
        finally:
            sys.unraisablehook = prev
        return raised
    raised = _run_single(f, label="timer-badtype")
    assert raised is not None, (
        "Timer(None) accepted a non-numeric duration without raising at the "
        "call site (error was deferred into the backing fiber)")


def test_timer_bad_duration_does_not_crash_only_silently_fails():
    # REGRESSION (was finding #12, the str-duration variant): a non-numeric
    # duration is rejected eagerly at the call site with TypeError -- across
    # Timer, After AND Sleep -- instead of being accepted and TypeError-ing
    # lazily inside the backing fiber (where it surfaced only via
    # unraisablehook and the timer silently never fired).
    # All three validate the duration BEFORE spawning/sleeping, so the
    # TypeError surfaces synchronously at the call site (no scheduler needed).
    for bad in ("not a number", None, [1, 2], object()):
        with pytest.raises(TypeError):
            rt.Timer(bad)
        with pytest.raises(TypeError):
            rt.After(bad)
        with pytest.raises(TypeError):
            rt.Sleep(bad)


# ----- re-entrant run(): single-thread nests fine; M:N nested HANGS ----------
def test_nested_run1_inside_fiber_drives_correctly():
    # run(1, inner) called from INSIDE a single-thread fiber re-enters and drives
    # the same scheduler -- inner runs to completion before the nested run()
    # returns, then the outer fiber continues.  (Not a finding -- a re-entrancy
    # that WORKS; we pin the ordering.)
    order = []

    def main():
        order.append("outer-start")
        r = runloom.run(1, lambda: order.append("inner"))
        order.append(("nested-returned", r))
        order.append("outer-end")

    with hang_guard(10, "nested-run1"):
        rc.fiber(main)
        rc.run()
    assert order[0] == "outer-start"
    assert "inner" in order
    assert order.index("inner") < order.index("outer-end"), \
        "nested run(1) did not drive inner before the outer fiber continued: %r" \
        % order


@pytest.mark.skipif(not needs_free_threading(),
                    reason="M:N needs GIL-disabled build")
# REGRESSION (was finding #3): runloom.run(n>1) called re-entrantly from inside
# an M:N hub fiber now raises RuntimeError promptly instead of deadlocking on a
# nested mn_init.  run() guards on mn_hub_count() > 0.  (run(1) re-entrancy stays
# supported.)
def test_nested_run_n_inside_mn_hub_hangs_FINDING():
    script = r"""
import sys, os; sys.path.insert(0, "src")
import runloom
def main():
    try:
        runloom.run(2, lambda: None)   # re-entrant mn_init inside a hub
        print("NESTED_RETURNED")
    except RuntimeError:
        print("NESTED_RAISED")
    sys.stdout.flush()
runloom.run(2, main)
print("OUTER_DONE")
"""
    timed_out = False
    try:
        proc = _subproc(script, timeout=8)
    except subprocess.TimeoutExpired:
        timed_out = True
        proc = None
    assert not timed_out and proc is not None, \
        "nested run(2) inside an M:N hub hung instead of raising RuntimeError"
    assert b"NESTED_RAISED" in proc.stdout, proc.stdout + proc.stderr
    assert b"OUTER_DONE" in proc.stdout, proc.stdout + proc.stderr


def test_run_twice_sequentially_resets_state():
    # Two back-to-back run(1) calls must each drive their own main cleanly --
    # the scheduler resets between runs (no leftover fibers, no double-drive).
    seen = []
    with hang_guard(10, "run-twice-a"):
        runloom.run(1, lambda: seen.append(1))
    with hang_guard(10, "run-twice-b"):
        runloom.run(1, lambda: seen.append(2))
    assert seen == [1, 2], "sequential run() did not reset cleanly: %r" % seen


# ----- INTEGRITY: which timers fired + the fired value (set-equality) --------
def test_many_timers_fire_with_correct_distinct_values_integrity():
    # Not just COUNTS: each Timer fires its OWN duration as the value, and the
    # SET of received values equals the set of configured durations (a stale
    # cross-fire would land the wrong duration; a lost wake would drop one).
    N = 60
    durs = [round(0.01 + (i % 6) * 0.001, 6) for i in range(N)]
    recv_vals = []

    def main():
        timers = [rt.Timer(d) for d in durs]
        for t in timers:
            v, ok = t.c.recv()
            assert ok is True
            recv_vals.append(v)

    with hang_guard(20, "timer-value-integrity"):
        rc.fiber(main)
        rc.run()
    assert len(recv_vals) == N, "lost a timer wake (%d/%d)" % (len(recv_vals), N)
    # Each timer delivered ITS OWN configured duration -- positional integrity.
    assert recv_vals == durs, (
        "a timer delivered the wrong duration value (stale cross-fire?): "
        "got %r want %r" % (recv_vals[:8], durs[:8]))


def test_mixed_stop_fire_interleave_only_unstopped_fire_set_equality():
    # Stop the EVEN-indexed timers; let the ODD ones fire.  The set of fired
    # indices must equal exactly the odd indices -- no stopped timer leaks a
    # value (false fire) and no live timer is dropped (lost wake).
    N = 40

    def main():
        timers = [rt.Timer(0.02) for _ in range(N)]
        for i in range(0, N, 2):
            assert timers[i].Stop() is True
        # Receive from the live (odd) ones.
        fired = set()
        for i in range(1, N, 2):
            v, ok = timers[i].c.recv()
            if ok:
                fired.add(i)
        rc.sched_sleep(0.05)              # past the deadline of the stopped ones
        leaked = {i for i in range(0, N, 2) if timers[i].c.try_recv() is not None}
        return fired, leaked

    fired, leaked = _run_single(main, guard=20, label="mixed-stop-fire")
    assert fired == set(range(1, N, 2)), \
        "live-timer fire set wrong: %r" % sorted(fired)
    assert leaked == set(), \
        "stopped timers leaked a value (stale fire): %r" % sorted(leaked)


# ----- Reset does NOT drain a buffered stale value (Go parity) ---------------
def test_timer_reset_does_not_drain_buffered_value():
    # If a timer fires into its buffer-1 channel UNCONSUMED and is then Reset to
    # a longer interval, Go's Reset does NOT drain the channel -- the stale value
    # remains until the consumer reads it.  Verify the impl matches (the old
    # value is still there right after Reset, before the new deadline).
    def f():
        t = rt.Timer(0.01)
        rc.sched_sleep(0.04)             # fire into the buffer; do NOT recv
        was_active = t.Reset(0.50)       # reset to a far deadline
        immediate = t.c.try_recv()       # the buffered old value must still be here
        return was_active, immediate

    was_active, immediate = _run_single(f, label="reset-no-drain")
    # The timer had already FIRED before Reset, so Reset (like Stop) reports
    # False -- "true if the timer had been active, false if it had expired"
    # (Go's contract; consistent with finding #13's Stop fix).
    assert was_active is False
    assert immediate == (0.01, True), (
        "Reset drained the buffered value (Go's Reset does not drain): %r"
        % (immediate,))


def test_context_child_shorter_deadline_keeps_own_parent_untightened():
    # A child requesting a SHORTER deadline than its parent keeps its own (it
    # fires first) and does NOT tighten the parent -- the parent stays active.
    def f():
        parent, _ = rctx.WithTimeout(rctx.Background(), 5.0)
        child, _ = rctx.WithTimeout(parent, 0.02)
        t0 = time.monotonic()
        child.done.recv()
        el = time.monotonic() - t0
        return child.err(), parent.err(), el

    cerr, perr, el = _run_single(f, label="child-shorter")
    assert cerr == rctx.DEADLINE_EXCEEDED
    assert perr is None, "child's shorter deadline tightened the parent"
    assert el < 1.0, "child did not honor its own shorter deadline (%.3fs)" % el


# ----- Goroutine handle read PRE-completion (no crash / sane defaults) -------
def test_fiber_handle_pre_completion_is_safe():
    box = {}

    def main():
        g = runloom.fiber(lambda: 99)
        # Read the handle BEFORE the child has had a chance to run.
        box["pre_done"] = g.done
        box["pre_result"] = g.result
        box["pre_exc"] = g.exception
        box["coro_is_self"] = g.coro is g
        box["repr"] = repr(g)
        for _ in range(4):
            rc.sched_yield()
        box["post_done"] = g.done
        box["post_result"] = g.result

    with hang_guard(10, "g-pre-completion"):
        runloom.run(1, main)
    assert box["pre_done"] is False, "handle reported done before the child ran"
    assert box["pre_result"] is None
    assert box["pre_exc"] is None
    assert box["coro_is_self"] is True, ".coro compat shim must forward to self"
    assert "Goroutine" in box["repr"]
    assert box["post_done"] is True
    assert box["post_result"] == 99


# ----- current() returns the bare C-G, not the Python Goroutine wrapper ------
def test_current_returns_bare_c_g_not_fiber_wrapper():
    # runtime.current() is documented to return the bare runloom_c.G, NOT the
    # Python Goroutine wrapper -- callers compare identity / None-ness.  Pin that
    # contract so a future change that wraps it doesn't silently break callers.
    box = {}

    def main():
        cur = runloom.current()
        box["type_is_G"] = isinstance(cur, rc.G)
        box["is_fiber_wrapper"] = isinstance(cur, runloom.Goroutine)

    with hang_guard(10, "current-type"):
        runloom.run(1, main)
    assert box["type_is_G"] is True, \
        "current() inside a fiber should be a bare runloom_c.G"
    assert box["is_fiber_wrapper"] is False, \
        "current() must NOT return the Python Goroutine wrapper (contract)"


# ----- blocking() / yield_now() / sleep() OUTSIDE a fiber are no-crash -------
def test_blocking_outside_fiber_runs_inline():
    # blocking() off any fiber must run fn INLINE (no offload, no scheduler
    # state), so the same call is safe in either context.  Run in a FRESH
    # subprocess: an in-process call here would (after this file's M:N tests have
    # already run + torn down a hub pool) abort the whole interpreter -- that
    # teardown-order abort is captured separately as
    # test_blocking_outside_fiber_after_mn_run_aborts_FINDING.  Here we verify
    # the documented inline-fallback contract on a clean process.
    script = r"""
import sys; sys.path.insert(0, "src")
import runloom
assert runloom.current() is None
r = runloom.blocking(lambda a, b: a * b, 6, 7)
print("RESULT", r)
"""
    proc = _subproc(script, timeout=15)
    _assert_no_signal(proc, "blocking-outside-fresh")
    assert b"RESULT 42" in proc.stdout, (
        "blocking() outside a fiber (fresh process) did not run inline: %r / %r"
        % (proc.stdout, proc.stderr.decode()[-500:]))


# REGRESSION (was finding #1): runloom.blocking(fn) called outside any fiber
# AFTER an M:N run(n>1) torn down used to ABORT ("_PyThreadState_Attach: non-NULL
# old thread state") -- the inline offload ran py_blocking_worker, which
# PyGILState_Ensure()d a tstate over the still-current main tstate (the gilstate
# TSS was desynced by the M:N teardown).  py_blocking_worker now calls directly
# when a tstate is already attached (the inline case), so it runs fn inline and
# returns 42 as on a fresh process.
def test_blocking_outside_fiber_after_mn_run_aborts_FINDING():
    if not needs_free_threading():
        pytest.skip("M:N needs GIL-disabled build")
    script = r"""
import sys; sys.path.insert(0, "src")
import runloom
runloom.run(2, lambda: None)        # exercise + tear down an M:N scheduler
assert runloom.mn_hub_count() == 0
r = runloom.blocking(lambda a, b: a * b, 6, 7)   # top-level inline blocking()
print("RESULT", r)
"""
    proc = _subproc(script, timeout=20)
    # The CORRECT contract: it must NOT crash and must return 42.
    _assert_no_signal(proc, "blocking-outside-after-mn")
    assert b"RESULT 42" in proc.stdout, (
        "blocking() outside a fiber after an M:N run did not return inline: "
        "%r / %r" % (proc.stdout, proc.stderr.decode()[-800:]))


def test_yield_now_outside_fiber_does_not_crash():
    # yield_now off a fiber has nothing to yield to; it must not crash or hang.
    assert runloom.current() is None
    with hang_guard(5, "yield-outside"):
        runloom.yield_now()
    # still alive + still off a fiber
    assert runloom.current() is None


# ----- Ticker stale-fire torture (the first pass only tortured Timer) --------
def test_ticker_reset_to_longer_old_gen_does_not_fire_early():
    # Reset a fast ticker to a SLOWER interval before its first tick; the old
    # (fast) generation must not fire at the old cadence -- only the new gen
    # ticks, and not before the new interval elapses.
    def f():
        tk = rt.Ticker(0.01)
        tk.Reset(0.40)                    # supersede gen-0 (fast) with gen-1 (slow)
        rc.sched_sleep(0.08)              # well past the OLD 10ms, before NEW 400ms
        early = tk.c.try_recv()           # the fast gen-0 must NOT have ticked
        tk.Stop()
        return early
    early = _run_single(f, guard=12, label="ticker-reset-longer")
    assert early is None, \
        "ticker old (fast) generation ticked after Reset to a slower interval"


def test_ticker_rapid_reset_storm_no_stale_pileup():
    # A storm of Reset() calls before any tick: only the FINAL gen may tick, and
    # the buffer-1 channel never piles up superseded gens.
    def f():
        tk = rt.Ticker(0.50)
        for _ in range(40):
            tk.Reset(0.01)               # 40 supersessions, all still sleeping
        v, ok = tk.c.recv()             # the last gen ticks
        rc.sched_sleep(0.03)
        # Drain: superseded gens must all have bailed.
        leftover = 0
        while tk.c.try_recv() is not None:
            leftover += 1
            if leftover > 5:
                break
        tk.Stop()
        return ok, leftover
    ok, leftover = _run_single(f, guard=12, label="ticker-reset-storm")
    assert ok is True
    assert leftover <= 1, \
        "Ticker Reset storm let %d superseded gen(s) tick" % leftover


# ----- concurrent cancel RACE under M:N: no close-on-closed crash ------------
@pytest.mark.skipif(not needs_free_threading(),
                    reason="M:N needs GIL-disabled build")
def test_concurrent_cancel_same_ctx_under_mn_no_crash():
    # _cancel() guards on self._err but close() on an already-closed channel
    # raises -- under M:N, many fibers across hubs calling the SAME cancel()
    # concurrently must not crash (the close() race is caught) and the ctx ends
    # CANCELED exactly once.
    box = {}
    callers = []

    def main():
        ctx, cancel = rctx.WithCancel(rctx.Background())

        def racer():
            cancel()
            callers.append(1)

        for _ in range(32):
            runloom.fiber(racer)            # mn_go: spread across hubs
        runloom.sleep(0.05)
        box["err"] = ctx.err()
        box["closed"] = ctx.done.closed

    with hang_guard(20, "mn-concurrent-cancel"):
        runloom.run(4, main)
    assert len(callers) == 32, \
        "only %d/32 concurrent cancellers completed" % len(callers)
    assert box.get("err") == rctx.CANCELED
    assert box.get("closed") is True


# ----- signal interruption of a parked recv: no crash / no hang --------------
def test_signal_during_parked_timer_recv_no_crash_no_hang():
    # A SIGALRM whose handler raises while a fiber is parked in a timer recv must
    # surface as a CLEAN Python exception (into the recv, or out of run() in the
    # idle/sleep-only case) -- never a segfault, never an unbounded hang.  Run in
    # a subprocess so a crash is a contained signal returncode and a hang is a
    # bounded TimeoutExpired.
    script = r"""
import sys, os, signal; sys.path.insert(0, "src")
import runloom, runloom_c as rc
import runloom.time as rt
def main():
    def handler(sig, frm):
        raise KeyboardInterrupt("alarm")
    signal.signal(signal.SIGALRM, handler)
    signal.setitimer(signal.ITIMER_REAL, 0.1)
    t = rt.Timer(5.0)            # long; the signal fires while we are parked
    try:
        t.c.recv()
        print("RECV_RETURNED")
    except BaseException as e:
        print("CLEAN_EXC", type(e).__name__)
try:
    rc.fiber(main); rc.run()
except KeyboardInterrupt:
    print("KI_OUT_OF_RUN")     # the idle/sleep-only delivery path is also fine
print("DONE")
"""
    try:
        proc = _subproc(script, timeout=15)
    except subprocess.TimeoutExpired:
        pytest.fail("signal during parked recv HUNG (no bounded delivery)")
    _assert_no_signal(proc, "signal-during-recv")
    out = proc.stdout.decode()
    # Any clean outcome is acceptable; a signal crash or a hang is not.
    assert ("CLEAN_EXC" in out or "KI_OUT_OF_RUN" in out or "DONE" in out), \
        "signal during recv produced no clean outcome: %r / %r" % (
            out, proc.stderr.decode()[-500:])


# ----- env-gated modes: SYSMON / PREEMPT / HANDOFF over timer+ctx+CPU --------
@pytest.mark.skipif(not needs_free_threading(),
                    reason="M:N needs GIL-disabled build")
def test_env_modes_sysmon_preempt_handoff_over_timer_ctx_workload():
    # Drive a workload that has BOTH a CPU-bound fiber (trips preempt/sysmon) and
    # cooperative timer/context fibers, under SYSMON + PREEMPT + HANDOFF all on.
    # The detectors must not crash the timer/context machinery; everything still
    # completes correctly.  Subprocess so a detector-induced crash is contained.
    script = r"""
import sys, os, time; sys.path.insert(0, "src")
import runloom, runloom_c as rc
import runloom.time as rt
import runloom.context as rctx
box = {}
def main():
    def hot():
        t0 = time.monotonic()
        x = 0
        while time.monotonic() - t0 < 0.12:
            x += 1
        box["hot"] = True
    def timed():
        t = rt.Timer(0.03); v, ok = t.c.recv(); box["t"] = ok
    def cancelled():
        ctx, cancel = rctx.WithTimeout(rctx.Background(), 0.03)
        ctx.done.recv(); box["c"] = ctx.err(); cancel()
    runloom.fiber(hot); runloom.fiber(timed); runloom.fiber(cancelled)
    runloom.sleep(0.25)
runloom.run(3, main)
ok = box.get("hot") and box.get("t") is True and box.get("c") == "deadline_exceeded"
print("MODES_OK" if ok else ("MODES_BAD %r" % box))
"""
    proc = _subproc(script, timeout=40, extra_env={
        "RUNLOOM_SYSMON": "1", "RUNLOOM_SYSMON_QUIET": "1", "RUNLOOM_SYSMON_MS": "8",
        "RUNLOOM_PREEMPT": "1", "RUNLOOM_PREEMPT_MS": "8",
        "RUNLOOM_HANDOFF": "1", "RUNLOOM_HANDOFF_POOL": "2",
    })
    _assert_no_signal(proc, "env-modes")
    assert b"MODES_OK" in proc.stdout, (
        "env-gated modes broke the timer/context workload: %r / %r" % (
            proc.stdout, proc.stderr.decode()[-800:]))


@pytest.mark.skipif(not needs_free_threading(),
                    reason="M:N needs GIL-disabled build")
def test_mn_barrier_deterministic_replay_timer_ctx():
    # The deterministic controlled-replay barrier (RUNLOOM_MN_BARRIER + seed)
    # must still deliver correct timer/context results -- the barrier reorders
    # scheduling decisions but must not break a timer fire or a deadline.  Two
    # runs with the SAME seed must both succeed (stability of the replay).
    script = r"""
import sys, os; sys.path.insert(0, "src")
import runloom, runloom_c as rc
import runloom.time as rt
import runloom.context as rctx
box = {}
def main():
    t = rt.Timer(0.02); v, ok = t.c.recv(); box["t"] = ok
    ctx, cancel = rctx.WithTimeout(rctx.Background(), 0.02)
    ctx.done.recv(); box["c"] = ctx.err(); cancel()
runloom.run(3, main)
print("BARRIER_OK" if (box.get("t") is True
      and box.get("c") == "deadline_exceeded") else ("BAD %r" % box))
"""
    env = {"RUNLOOM_MN_BARRIER": "1", "RUNLOOM_MN_SEED": "777",
           "RUNLOOM_MN_PCT": "8"}
    for attempt in range(2):
        proc = _subproc(script, timeout=40, extra_env=env)
        _assert_no_signal(proc, "mn-barrier-replay-%d" % attempt)
        assert b"BARRIER_OK" in proc.stdout, (
            "MN_BARRIER replay broke timer/context (attempt %d): %r / %r" % (
                attempt, proc.stdout, proc.stderr.decode()[-600:]))


# ----- fault injection on the SCALE + cascade backing-fiber spawns -----------
def test_spawn_g_fault_during_context_cascade_does_not_crash():
    # Inject a SPAWN_G fault while a WithTimeout chain is being built -- the
    # deadline fiber spawn for one of the contexts may hit the fault.  The
    # cascade must degrade to a clean Python error, never a segfault, and a
    # cancel() of whatever WAS built must not crash.
    script = r"""
import sys, os; sys.path.insert(0, "src")
os.environ["RUNLOOM_GOROUTINE_PANIC"] = "silent"
import runloom, runloom_c as rc
import runloom.context as rctx
def main():
    try:
        root, rcancel = rctx.WithCancel(rctx.Background())
        kids = []
        for _ in range(5):
            ctx, c = rctx.WithTimeout(root, 0.02)   # each spawns a deadline fiber
            kids.append((ctx, c))
        for _ in range(20):
            rc.sched_yield()
        rcancel()                                    # cascade-cancel the survivors
        for _, c in kids:
            c()
    except Exception as e:
        print("CLEAN_ERR", type(e).__name__); return
    print("SURVIVED")
rc.fiber(main)
try:
    rc.run()
except Exception as e:
    print("RUN_ERR", type(e).__name__)
print("DONE")
"""
    proc = _subproc(script, timeout=30,
                    extra_env={"RUNLOOM_FAULT_SPAWN_G": "once:12"})
    _assert_clean_fault_outcome(proc, "spawn_g-fault-cascade")


def test_spawn_stack_fault_at_timer_scale_does_not_crash():
    # Inject a SPAWN_STACK fault into a batch of timer spawns.  One spawn's stack
    # reservation fails; that must surface cleanly and the surviving timers must
    # still be drivable without a crash.
    script = r"""
import sys, os; sys.path.insert(0, "src")
os.environ["RUNLOOM_GOROUTINE_PANIC"] = "silent"
import runloom, runloom_c as rc
import runloom.time as rt
def main():
    try:
        timers = [rt.Timer(0.01) for _ in range(8)]
        got = 0
        for t in timers:
            for _ in range(15):
                rc.sched_yield()
                if t.c.try_recv() is not None:
                    got += 1
                    break
        print("GOT", got)
    except Exception as e:
        print("CLEAN_ERR", type(e).__name__); return
    print("SURVIVED")
rc.fiber(main)
try:
    rc.run()
except Exception as e:
    print("RUN_ERR", type(e).__name__)
print("DONE")
"""
    proc = _subproc(script, timeout=30,
                    extra_env={"RUNLOOM_FAULT_SPAWN_STACK": "once:12"})
    _assert_clean_fault_outcome(proc, "spawn_stack-fault-timer-scale")


# ----- After/Tick negative + zero duration edge values (no crash) -----------
def test_after_zero_and_negative_duration_fire_immediately():
    # After(0) and After(-1) must fire AT ONCE (a non-positive sched_sleep does
    # not block) and not crash -- an edge-value smoke for the After path.
    def f():
        z = rt.After(0.0)
        zv, zok = z.c.recv() if hasattr(z, "c") else z.recv()
        n = rt.After(-0.5)
        nv, nok = n.recv()
        return (zv, zok), (nv, nok)
    (zv, zok), (nv, nok) = _run_single(f, label="after-edge")
    assert zok is True and zv == 0.0
    assert nok is True and nv == -0.5, \
        "After(negative) did not fire immediately: %r" % ((nv, nok),)


def test_timer_zero_duration_fires_once_immediately():
    def f():
        t = rt.Timer(0.0)
        v, ok = t.c.recv()
        rc.sched_sleep(0.02)
        return ok, v, t.c.try_recv()
    ok, v, extra = _run_single(f, label="timer-zero")
    assert ok is True and v == 0.0
    assert extra is None, "Timer(0) fired more than once"


# ----- sleep() inside a fiber: NEGATIVE / zero does not hang ------------------
def test_sleep_zero_and_negative_inside_fiber_return_promptly():
    def f():
        t0 = time.monotonic()
        runloom.sleep(0.0)
        runloom.sleep(-1.0)
        rt.Sleep(0.0)
        rt.Sleep(-2.0)
        return time.monotonic() - t0
    with assert_faster_than(1.0, "non-positive sleeps return at once"):
        el = _run_single(f, label="sleep-edge")
    assert el < 0.5, "non-positive sleep blocked (%.3fs)" % el


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v", "-p", "no:cacheprovider", "-n0"]))
