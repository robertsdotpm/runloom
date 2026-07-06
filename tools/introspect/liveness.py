"""liveness.py -- self-diagnosing liveness auditor + stall watchdog (item 5).

61 of the 125 historical bugs were permanent hangs, and the dominant COST was
multi-day live-debug arcs to assign blame -- worse, the runtime's own deadlock
census treats a netpoll-parked fiber as wakeable, so a genuine stranded park is
SILENT.  This module turns the existing introspection (runloom_c.fibers() +
mn_hub_states()) into a continuously-checkable liveness signal and, on a stall,
dumps blame the way Go's schedtrace/goroutine-dump does.

Two tools, both pure-Python over the introspection API (no runtime changes):

  deadlock_blame(snap) -- a PURE function over a snapshot.  Flags the
      unambiguous hard-deadlock shape: fibers are parked, NONE is runnable, NO
      parked fiber has a timer, NO hub has pending work, and NO fiber is waiting
      on external I/O (an fd) -- so every waiter is blocked on an INTERNAL
      primitive nobody will ever signal.  Returns a blame report or None.  (A
      fiber parked on an fd is reported separately as an I/O waiter -- it CAN be
      woken by the kernel, so it is a hang suspect, not a proven deadlock.)

  StallWatchdog -- a background OS thread sampling a caller-supplied progress
      signal.  If progress stops for `timeout` seconds while fibers remain
      parked, it snapshots and dumps blame.  Doubles as a soak/CI oracle: a soak
      fails on an invariant violation instead of on a wall-clock timeout with no
      explanation.

House style: %/.format, prints kept.
"""
import sys
import threading
import time

import runloom_c


RUNNABLE = ("runnable", "running")


def snapshot():
    """A coherent-enough snapshot of the scheduler for a liveness verdict."""
    try:
        hubs = runloom_c.mn_hub_states()
    except Exception:
        hubs = []
    return {"fibers": runloom_c.fibers(), "hubs": hubs}


def _is_io_waiter(f):
    return f.get("fd") is not None


def deadlock_blame(snap):
    """Return a blame dict for a hard cooperative deadlock, else None.

    Hard deadlock = at least one parked fiber, zero runnable, zero timers, zero
    hub pending work, zero external-I/O waiters -> every waiter is blocked on an
    internal primitive with no possible waker.  This is decidable from a single
    snapshot; a hang that DOES have an I/O waiter needs the time-based watchdog
    (the kernel might still deliver), so it is surfaced as a suspect, not a
    verdict."""
    fibers = snap["fibers"]
    hubs = snap["hubs"]
    parked = [f for f in fibers if f["state"] not in RUNNABLE]
    if not parked:
        return None
    runnable = [f for f in fibers if f["state"] in RUNNABLE]
    if runnable:
        return None
    if any(f.get("wake_in") is not None for f in parked):
        return None                                   # a timer will fire
    if any((h.get("pending") or 0) > 0 for h in hubs):
        return None                                   # queued work exists
    io_waiters = [f for f in parked if _is_io_waiter(f)]
    if io_waiters:
        # not a proven deadlock -- the kernel could still wake an I/O waiter;
        # leave it to the watchdog's time-based check.
        return None
    # every waiter is on an internal primitive with no waker: a hard deadlock.
    return {
        "verdict": "HARD-DEADLOCK",
        "reason": "all fibers parked on internal primitives; none runnable, no "
                  "timers, no pending hub work, no I/O waiters -- no possible waker",
        "parked": [_blame_row(f) for f in parked],
    }


def _blame_row(f):
    return {"id": f["id"], "state": f["state"], "blocked_on": f["blocked_on"],
            "fd": f.get("fd"), "wake_in": f.get("wake_in"),
            "age": f.get("age")}


def format_blame(snap, blame=None):
    """Human-readable blame dump (the goroutine-dump analog)."""
    out = []
    if blame:
        out.append("LIVENESS VIOLATION: %s" % blame["verdict"])
        out.append("  %s" % blame["reason"])
    out.append("  fibers:")
    for f in snap["fibers"]:
        out.append("    g%-4s %-12s blocked_on=%-8s fd=%s wake_in=%s age=%s"
                   % (f["id"], f["state"], f["blocked_on"], f.get("fd"),
                      f.get("wake_in"), f.get("age")))
    if snap["hubs"]:
        out.append("  hubs:")
        for h in snap["hubs"]:
            out.append("    hub%-3s %-10s pending=%s running_g=%s dwell_ms=%s"
                       % (h["id"], h.get("state"), h.get("pending"),
                          h.get("running_g"), h.get("dwell_ms")))
    return "\n".join(out)


class StallWatchdog(object):
    """Sample `progress()` (any monotonic int/float the workload advances) from a
    background thread; if it stops moving for `timeout` s while fibers are parked,
    call `on_stall(snapshot, blame_or_None)` (default: print blame + optionally
    exit).  Start()/stop() or use as a context manager."""

    def __init__(self, progress, timeout=5.0, poll=0.25, on_stall=None,
                 exit_on_stall=False):
        self.progress = progress
        self.timeout = timeout
        self.poll = poll
        self.on_stall = on_stall or self._default_on_stall
        self.exit_on_stall = exit_on_stall
        self._stop = threading.Event()
        self._thread = None
        self.stalled = False

    def _default_on_stall(self, snap, blame):
        sys.stderr.write("[stall-watchdog] no progress for %.1fs -- blame:\n%s\n"
                         % (self.timeout, format_blame(snap, blame)))
        sys.stderr.flush()

    def _run(self):
        last = self.progress()
        last_move = time.monotonic()
        while not self._stop.wait(self.poll):
            cur = self.progress()
            now = time.monotonic()
            if cur != last:
                last = cur
                last_move = now
                continue
            if now - last_move < self.timeout:
                continue
            snap = snapshot()
            parked = [f for f in snap["fibers"] if f["state"] not in RUNNABLE]
            if not parked:
                last_move = now                       # idle, not stalled
                continue
            self.stalled = True
            self.on_stall(snap, deadlock_blame(snap))
            if self.exit_on_stall:
                import os
                os._exit(3)
            return

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(2)

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()
        return False
