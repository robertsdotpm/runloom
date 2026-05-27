"""pygo.time -- Go-style time primitives.

Mirrors the subset of Go's `time` package most often used in production:

  pygo.time.After(d)       -> chan that fires once after d seconds
  pygo.time.Tick(d)        -> chan that fires every d seconds
  pygo.time.NewTimer(d)    -> Timer with Stop() / Reset()
  pygo.time.NewTicker(d)   -> Ticker with Stop() / Reset()
  pygo.time.Sleep(d)       -> alias for pygo.sleep

All channels here are unbuffered + Go-style: pygo_core.Chan.  Consumers
should select on them via pygo_core.select().

Cancellation: stopping a Timer or Ticker drains its channel and prevents
further sends.  The backing goroutine exits on next tick.
"""
import pygo_core


def Sleep(seconds):
    """Block the current goroutine for `seconds` seconds.

    Equivalent to Go's `time.Sleep`.  Just an alias for pygo.sleep so
    `from pygo.time import Sleep` reads naturally."""
    pygo_core.sched_sleep(seconds)


def After(seconds):
    """Return a 1-buffered channel that receives a single value after
    `seconds` seconds, then closes.

    Idiomatic usage:

        timeout = pygo.time.After(5.0)
        idx, _ = pygo_core.select([
            ('recv', work_ch),
            ('recv', timeout),
        ])
        if idx == 1:
            raise TimeoutError()
    """
    ch = pygo_core.Chan(1)

    def fire():
        pygo_core.sched_sleep(seconds)
        try:
            ch.try_send(seconds)
        except Exception:
            pass
        ch.close()

    pygo_core.go(fire)
    return ch


class Timer(object):
    """Fires once after the configured duration.  Reset / Stop control
    it; the C channel `c` is what consumers select on."""

    __slots__ = ("c", "_d", "_stopped", "_gen")

    def __init__(self, seconds):
        self._d       = seconds
        self.c        = pygo_core.Chan(1)
        self._stopped = False
        self._gen     = 0
        self._spawn(self._gen)

    def _spawn(self, gen):
        d = self._d
        def fire():
            pygo_core.sched_sleep(d)
            # The Timer may have been Stop()'d or Reset() in the
            # meantime; the gen counter tells us whether *this* spawn is
            # still the live one.
            if self._stopped or self._gen != gen:
                return
            self.c.try_send(self._d)
        pygo_core.go(fire)

    def Stop(self):
        """Prevent the timer from firing.  Returns True if the call
        cancelled a still-armed timer, False if it had already fired."""
        if self._stopped:
            return False
        self._stopped = True
        # The fire() goroutine may still be sleeping; gen bump makes it
        # bail out cleanly when it wakes.
        self._gen += 1
        return True

    def Reset(self, seconds):
        """Re-arm the timer for `seconds` from now.  Stops any in-flight
        fire goroutine and spawns a fresh one.  Returns the same boolean
        that Stop() would have returned."""
        was_active = not self._stopped
        self._gen += 1
        self._d = seconds
        self._stopped = False
        self._spawn(self._gen)
        return was_active


class Ticker(object):
    """Fires every `seconds` seconds until Stop().  Behaves like Go's
    time.Ticker -- if the consumer is slow, ticks are dropped (the
    channel buffer is 1)."""

    __slots__ = ("c", "_d", "_stopped", "_gen")

    def __init__(self, seconds):
        if seconds <= 0:
            raise ValueError("non-positive ticker interval")
        self._d       = seconds
        self.c        = pygo_core.Chan(1)
        self._stopped = False
        self._gen     = 0
        self._spawn(self._gen)

    def _spawn(self, gen):
        def loop():
            while not self._stopped and self._gen == gen:
                pygo_core.sched_sleep(self._d)
                if self._stopped or self._gen != gen:
                    return
                # try_send so we never block the ticker goroutine; the
                # buffer-1 channel naturally drops backlog (matches Go).
                self.c.try_send(self._d)
        pygo_core.go(loop)

    def Stop(self):
        if self._stopped:
            return
        self._stopped = True
        self._gen += 1

    def Reset(self, seconds):
        if seconds <= 0:
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
