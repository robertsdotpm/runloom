"""runloom.time -- Go-style time primitives.

Mirrors the subset of Go's `time` package most often used in production:

  runloom.time.After(d)       -> chan that fires once after d seconds
  runloom.time.Tick(d)        -> chan that fires every d seconds
  runloom.time.NewTimer(d)    -> Timer with Stop() / Reset()
  runloom.time.NewTicker(d)   -> Ticker with Stop() / Reset()
  runloom.time.Sleep(d)       -> alias for runloom.sleep

All channels here are unbuffered + Go-style: runloom_c.Chan.  Consumers
should select on them via runloom_c.select().

Cancellation: stopping (or resetting) a Timer or Ticker prevents further
sends AND wakes its backing fiber early -- via the fiber's cancel_wait_fd()
-- so a stopped timer never keeps run()/mn_run() alive until the original
deadline (Go removes a stopped timer from its heap immediately).
"""
import numbers
import os as _os
import socket as _socket
import sys as _sys
import threading as _threading
import time as _time

import runloom_c


# --- Cancellable-park support -------------------------------------------
#
# Stop()/Reset() must be able to wake a Timer/Ticker's backing fiber early
# rather than let it sleep to the ORIGINAL deadline: a fiber blocked in an
# uncancellable sched_sleep would otherwise keep run()/mn_run() alive for the
# full duration even after the timer was stopped.  So the backing fiber parks
# in a deadline-BOUNDED yet WAKEABLE wait_fd on a permanent, never-ready fd,
# and Stop()/Reset() call the fiber's cancel_wait_fd() to return it early --
# exactly the primitive runloom.context's deadline waker uses.
#
# The fd is created ONCE, eagerly at import (single-threaded, before any fiber
# runs and before monkey.patch(), so the REAL un-patched pipe/socket is used):
# a lazy per-timer fd would race under free-threading and, on Windows, storm
# heavy socketpair() fallbacks.  Nothing is ever written to either end, so READ
# never becomes ready -- a parked fiber only ever times out or is cancelled.
# On Windows the readiness backend can only poll Winsock sockets, so use a
# socketpair (an AF_INET loopback pair AFD can poll); on POSIX a pipe.
_READ = 1   # runloom_c.wait_fd READ direction

_wake_socks = None   # keep the Windows socketpair alive so the fds stay valid
if _sys.platform == "win32":
    _s1, _s2 = _socket.socketpair()
    _wake_socks = (_s1, _s2)
    _wake_rfd = _s1.fileno()
else:
    _wake_rfd, _wake_wfd = _os.pipe()


def _check_duration(seconds):
    """Reject a non-numeric duration eagerly, at the call site.

    Go's time API takes a Duration; passing a non-number should fail where the
    caller can see it, not lazily inside the detached backing fiber (where the
    TypeError surfaces only via sys.unraisablehook and the timer silently never
    fires).  0 and negative durations ARE valid for Timer/After/Sleep (they fire
    immediately, as in Go), so this is a type check only -- not a positivity
    check like Ticker's."""
    if isinstance(seconds, numbers.Real) and not isinstance(seconds, bool):
        return seconds
    raise TypeError("duration must be a real number, not %s"
                    % type(seconds).__name__)


def _spawn(fn):
    """Spawn a timer fiber on whichever scheduler is active.

    runloom.time primitives are driven by a backing fiber, which must run
    under the M:N scheduler (mn_run) as well as the single-thread one --
    otherwise After/Tick/Timer/Ticker hang under mn_run because nothing drains
    the single-thread queue.  mn_hub_count() > 0 means mn_init() is in effect,
    so route through mn_fiber; else use the single-thread go.  Reading
    runloom_c.fiber / mn_fiber at call time also picks up monkey's
    fiber-context wrapper when patch() is active."""
    if runloom_c.mn_hub_count() > 0:
        return runloom_c.mn_fiber(fn)
    return runloom_c.fiber(fn)


def Sleep(seconds):
    """Block the current fiber for `seconds` seconds.

    Equivalent to Go's `time.Sleep`.  Behaves like runloom.sleep so
    `from runloom.time import Sleep` reads naturally: inside a fiber it parks
    cooperatively (other fibers run); OUTSIDE any fiber it falls back to the
    OS sleep instead of returning at once -- sched_sleep queues a wake on a
    scheduler that is not running, so calling it off-fiber is a silent no-op."""
    seconds = _check_duration(seconds)
    if runloom_c.current_g() is None:
        # No fiber to park: mirror runloom.sleep's fallback to the OS sleep.
        # A non-positive duration returns immediately (as in Go; time.sleep
        # rejects negatives), matching the off-fiber no-op for those values.
        if seconds > 0:
            _time.sleep(seconds)
        return
    runloom_c.sched_sleep(seconds)


def After(seconds):
    """Return a 1-buffered channel that receives a single value after
    `seconds` seconds, then closes.

    Idiomatic usage:

        timeout = runloom.time.After(5.0)
        idx, _ = runloom_c.select([
            ('recv', work_ch),
            ('recv', timeout),
        ])
        if idx == 1:
            raise TimeoutError()
    """
    _check_duration(seconds)
    ch = runloom_c.Chan(1)

    def fire():
        runloom_c.sched_sleep(seconds)
        try:
            ch.try_send(seconds)
        except Exception:
            pass
        ch.close()

    _spawn(fire)
    return ch


class Timer(object):
    """Fires once after the configured duration.  Reset / Stop control
    it; the C channel `c` is what consumers select on."""

    __slots__ = ("c", "_d", "_stopped", "_gen", "_fired", "_lock", "_fire_g")

    def __init__(self, seconds):
        self._d       = _check_duration(seconds)
        self.c        = runloom_c.Chan(1)
        self._stopped = False
        self._fired   = False
        self._gen     = 0
        # Guards _stopped/_fired/_gen/_fire_g: under free-threaded M:N the
        # fire fiber's check-then-send and Stop()/Reset()'s check-then-set
        # can otherwise interleave across OS threads, so Stop() could return
        # True (Go: the timer will NOT fire) while fire() still delivers a
        # tick.  The lock makes those two decisions mutually exclusive.
        self._lock    = _threading.Lock()
        self._fire_g  = None
        self._spawn(self._gen)

    def _spawn(self, gen):
        d = self._d
        # wait_fd's timeout is in ms; +1 mirrors runloom.context's waker.  A
        # non-positive duration fires immediately (as in Go) -- skip the park.
        timeout_ms = int(d * 1000) + 1 if d > 0 else 0
        def fire():
            with self._lock:
                # The Timer may have been Stop()'d or Reset() before this
                # fiber first ran; the gen counter says whether *this* spawn
                # is still the live one.
                if self._stopped or self._gen != gen:
                    return
                # Publish our handle as the cancel target BEFORE parking, so a
                # Stop()/Reset() already holding the lock wakes us the instant
                # we park (lost-wakeup discipline: publish, then park).
                self._fire_g = runloom_c.current_g()
            if timeout_ms:
                # Deadline-bounded but cancellable: Stop()/Reset() call
                # cancel_wait_fd() to return this early instead of sleeping to
                # the original deadline.  We re-check the flags either way.
                runloom_c.wait_fd(_wake_rfd, _READ, timeout_ms)
            with self._lock:
                if self._stopped or self._gen != gen or self._fired:
                    # A newer Reset() spawn may have published its own handle
                    # into _fire_g; leave it be -- only the live generation
                    # clears the cancel target.
                    return
                self._fired = True
                self._fire_g = None
            self.c.try_send(self._d)
        _spawn(fire)

    def Stop(self):
        """Prevent the timer from firing.  Returns True if the call
        cancelled a still-armed timer, False if it had already fired or
        been stopped -- matching Go's time.Timer.Stop()."""
        with self._lock:
            if self._stopped or self._fired:
                return False
            self._stopped = True
            # gen bump makes a fire() fiber caught between its check and
            # try_send bail out; grabbing _fire_g lets us wake it now instead
            # of leaving it parked until the original deadline.
            self._gen += 1
            g = self._fire_g
            self._fire_g = None
        if g is not None:
            try:
                g.cancel_wait_fd()
            except Exception:
                pass
        return True

    def Reset(self, seconds):
        """Re-arm the timer for `seconds` from now.  Stops any in-flight
        fire fiber and spawns a fresh one.  Returns the same boolean
        that Stop() would have returned."""
        d = _check_duration(seconds)
        with self._lock:
            was_active = not self._stopped and not self._fired
            self._gen += 1
            gen = self._gen
            self._d = d
            self._stopped = False
            self._fired = False
            g = self._fire_g
            self._fire_g = None
        if g is not None:
            try:
                g.cancel_wait_fd()
            except Exception:
                pass
        self._spawn(gen)
        return was_active


class Ticker(object):
    """Fires every `seconds` seconds until Stop().  Behaves like Go's
    time.Ticker -- if the consumer is slow, ticks are dropped (the
    channel buffer is 1)."""

    __slots__ = ("c", "_d", "_stopped", "_gen", "_lock", "_tick_g")

    def __init__(self, seconds):
        if _check_duration(seconds) <= 0:
            raise ValueError("non-positive ticker interval")
        self._d       = seconds
        self.c        = runloom_c.Chan(1)
        self._stopped = False
        self._gen     = 0
        # See Timer: guards the unsynchronized flags against the fire/Stop
        # interleave, and pairs with cancel_wait_fd() for early wake.
        self._lock    = _threading.Lock()
        self._tick_g  = None
        self._spawn(self._gen)

    def _spawn(self, gen):
        d = self._d
        timeout_ms = int(d * 1000) + 1
        def loop():
            while True:
                with self._lock:
                    if self._stopped or self._gen != gen:
                        return
                    # Publish the cancel target before parking (lost-wakeup
                    # discipline); it is this fiber for every iteration.
                    self._tick_g = runloom_c.current_g()
                # Cancellable park: Stop()/Reset() call cancel_wait_fd() to
                # wake us early so a stopped ticker does not pin run().
                runloom_c.wait_fd(_wake_rfd, _READ, timeout_ms)
                with self._lock:
                    if self._stopped or self._gen != gen:
                        # A newer Reset() spawn may own _tick_g now; only the
                        # live generation clears the cancel target.
                        return
                    self._tick_g = None
                # try_send so we never block the ticker fiber; the
                # buffer-1 channel naturally drops backlog (matches Go).
                self.c.try_send(d)
        _spawn(loop)

    def Stop(self):
        with self._lock:
            if self._stopped:
                return
            self._stopped = True
            self._gen += 1
            g = self._tick_g
            self._tick_g = None
        if g is not None:
            try:
                g.cancel_wait_fd()
            except Exception:
                pass

    def Reset(self, seconds):
        if _check_duration(seconds) <= 0:
            raise ValueError("non-positive ticker interval")
        with self._lock:
            self._gen += 1
            gen = self._gen
            self._d = seconds
            self._stopped = False
            g = self._tick_g
            self._tick_g = None
        if g is not None:
            try:
                g.cancel_wait_fd()
            except Exception:
                pass
        self._spawn(gen)


def NewTimer(seconds):
    return Timer(seconds)


def NewTicker(seconds):
    return Ticker(seconds)


def Tick(seconds):
    """Convenience: returns the channel of a Ticker.  Note that there's
    no way to stop the underlying Ticker -- use NewTicker() if you need
    to clean up."""
    return Ticker(seconds).c
