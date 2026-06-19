"""runloom.time -- Go-style time primitives.

Mirrors the subset of Go's `time` package most often used in production:

  runloom.time.After(d)       -> chan that fires once after d seconds
  runloom.time.Tick(d)        -> chan that fires every d seconds
  runloom.time.NewTimer(d)    -> Timer with Stop() / Reset()
  runloom.time.NewTicker(d)   -> Ticker with Stop() / Reset()
  runloom.time.Sleep(d)       -> alias for runloom.sleep

All channels here are unbuffered + Go-style: runloom_c.Chan.  Consumers
should select on them via runloom_c.select().

Cancellation: stopping a Timer or Ticker drains its channel and prevents
further sends.  The backing fiber exits on next tick.
"""
import numbers

import runloom_c


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
    so route through mn_go; else use the single-thread go.  Reading
    runloom_c.fiber / mn_go at call time also picks up monkey's
    fiber-context wrapper when patch() is active."""
    if runloom_c.mn_hub_count() > 0:
        return runloom_c.mn_fiber(fn)
    return runloom_c.fiber(fn)


def Sleep(seconds):
    """Block the current fiber for `seconds` seconds.

    Equivalent to Go's `time.Sleep`.  Just an alias for runloom.sleep so
    `from runloom.time import Sleep` reads naturally."""
    runloom_c.sched_sleep(_check_duration(seconds))


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

    __slots__ = ("c", "_d", "_stopped", "_gen", "_fired")

    def __init__(self, seconds):
        self._d       = _check_duration(seconds)
        self.c        = runloom_c.Chan(1)
        self._stopped = False
        self._fired   = False
        self._gen     = 0
        self._spawn(self._gen)

    def _spawn(self, gen):
        d = self._d
        def fire():
            runloom_c.sched_sleep(d)
            # The Timer may have been Stop()'d or Reset() in the
            # meantime; the gen counter tells us whether *this* spawn is
            # still the live one.
            if self._stopped or self._gen != gen:
                return
            self._fired = True
            self.c.try_send(self._d)
        _spawn(fire)

    def Stop(self):
        """Prevent the timer from firing.  Returns True if the call
        cancelled a still-armed timer, False if it had already fired or
        been stopped -- matching Go's time.Timer.Stop()."""
        if self._stopped or self._fired:
            return False
        self._stopped = True
        # The fire() fiber may still be sleeping; gen bump makes it
        # bail out cleanly when it wakes.
        self._gen += 1
        return True

    def Reset(self, seconds):
        """Re-arm the timer for `seconds` from now.  Stops any in-flight
        fire fiber and spawns a fresh one.  Returns the same boolean
        that Stop() would have returned."""
        was_active = not self._stopped and not self._fired
        self._gen += 1
        self._d = _check_duration(seconds)
        self._stopped = False
        self._fired = False
        self._spawn(self._gen)
        return was_active


class Ticker(object):
    """Fires every `seconds` seconds until Stop().  Behaves like Go's
    time.Ticker -- if the consumer is slow, ticks are dropped (the
    channel buffer is 1)."""

    __slots__ = ("c", "_d", "_stopped", "_gen")

    def __init__(self, seconds):
        if _check_duration(seconds) <= 0:
            raise ValueError("non-positive ticker interval")
        self._d       = seconds
        self.c        = runloom_c.Chan(1)
        self._stopped = False
        self._gen     = 0
        self._spawn(self._gen)

    def _spawn(self, gen):
        def loop():
            while not self._stopped and self._gen == gen:
                runloom_c.sched_sleep(self._d)
                if self._stopped or self._gen != gen:
                    return
                # try_send so we never block the ticker fiber; the
                # buffer-1 channel naturally drops backlog (matches Go).
                self.c.try_send(self._d)
        _spawn(loop)

    def Stop(self):
        if self._stopped:
            return
        self._stopped = True
        self._gen += 1

    def Reset(self, seconds):
        if _check_duration(seconds) <= 0:
            raise ValueError("non-positive ticker interval")
        self._gen += 1
        self._d = seconds
        self._stopped = False
        self._spawn(self._gen)


def NewTimer(seconds):
    return Timer(seconds)


def NewTicker(seconds):
    return Ticker(seconds)


def Tick(seconds):
    """Convenience: returns the channel of a Ticker.  Note that there's
    no way to stop the underlying Ticker -- use NewTicker() if you need
    to clean up."""
    return Ticker(seconds).c
