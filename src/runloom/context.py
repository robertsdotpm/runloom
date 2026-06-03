"""runloom.context -- Go-style context.Context for cancellation.

Patterns we support, lifted straight from Go:

    ctx, cancel = runloom.context.WithCancel(runloom.context.Background())
    defer cancel()    # (use try/finally; we don't have `defer`)

    ctx, cancel = runloom.context.WithTimeout(parent, 5.0)
    ctx, cancel = runloom.context.WithDeadline(parent, monotonic + 5.0)

A Context exposes:
  ctx.done        -> a channel that closes when the context is cancelled
  ctx.err()       -> None if active, "cancelled" or "deadline_exceeded"
                     once the context is done
  ctx.deadline()  -> (deadline_seconds, True) or (None, False)

Producers select on `ctx.done` to know when to stop:

    while True:
        idx, _ = runloom_c.select([
            ('recv', ctx.done),
            ('recv', work_ch),
        ])
        if idx == 0:
            return ctx.err()
        ...

Cancellation is transitive: when a parent context is cancelled, all
descendants are cancelled too.  This is the main reason for the
explicit tree, vs. just passing a channel around.
"""
import time as _time
import runloom_c


# Error sentinels (strings -- runloom deliberately avoids custom exception
# classes here so the public API stays close to Go's, where ctx.Err()
# just returns context.Canceled or context.DeadlineExceeded).
CANCELED          = "cancelled"
DEADLINE_EXCEEDED = "deadline_exceeded"


class _BackgroundCtx(object):
    """The root context.  Never cancelled, never has a deadline.  Used
    as the parent for the first WithCancel/WithTimeout call at the top
    of a goroutine tree."""
    __slots__ = ("done",)

    def __init__(self):
        # A never-closed channel.  Receives on it block forever -- that's
        # the right semantic: Background never fires "done".
        self.done = runloom_c.Chan(1)

    def err(self):
        return None

    def deadline(self):
        return (None, False)

    def value(self, key):
        return None


_BACKGROUND = _BackgroundCtx()


def Background():
    """The empty root context.  Identical instance every call."""
    return _BACKGROUND


class _CancelCtx(object):
    """Holds the cancellation state and the child-fanout list.  The
    public API surface (done / err / deadline) matches _BackgroundCtx
    so consumers can treat any context uniformly."""

    __slots__ = ("done", "_parent", "_err", "_children", "_deadline")

    def __init__(self, parent, deadline=None):
        self.done      = runloom_c.Chan(1)
        self._parent   = parent
        self._err      = None
        self._children = []
        self._deadline = deadline

        # Wire ourselves into the parent's cancel fanout.  Background is
        # the only "uncancellable" parent; everyone else exposes
        # _children for that purpose.
        if isinstance(parent, _CancelCtx):
            if parent._err is not None:
                # Parent already cancelled; propagate immediately.
                self._cancel(parent._err)
            else:
                parent._children.append(self)

    def err(self):
        return self._err

    def deadline(self):
        if self._deadline is None:
            return (None, False)
        return (self._deadline, True)

    def value(self, key):
        # Without WithValue today; punt to the parent.
        if hasattr(self._parent, "value"):
            return self._parent.value(key)
        return None

    def _cancel(self, reason):
        if self._err is not None:
            return       # already cancelled, idempotent
        self._err = reason
        try:
            self.done.close()
        except Exception:
            # close() on an already-closed channel raises; we treat the
            # window between "cancel called twice concurrently" as benign.
            pass
        # Fan cancellation out to all children.  Iterate a snapshot in
        # case a child registers more during their own cancel callbacks
        # (defensive -- our model says children can't add grandchildren
        # post-cancel, but cheap to be safe).
        for child in tuple(self._children):
            child._cancel(reason)
        self._children = []


def WithCancel(parent):
    """Return (ctx, cancel).  Calling cancel() closes ctx.done and
    propagates to every descendant context."""
    ctx = _CancelCtx(parent)
    def cancel():
        ctx._cancel(CANCELED)
    return ctx, cancel


def WithDeadline(parent, deadline_monotonic):
    """Return (ctx, cancel).  ctx fires automatically when
    monotonic() >= deadline_monotonic.

    If the parent already has a sooner deadline, that one wins -- ctx
    will inherit it and never extend beyond.
    """
    # Inherit the parent's deadline if it's tighter.
    if isinstance(parent, _CancelCtx) and parent._deadline is not None:
        if parent._deadline < deadline_monotonic:
            deadline_monotonic = parent._deadline

    ctx = _CancelCtx(parent, deadline=deadline_monotonic)

    def cancel():
        ctx._cancel(CANCELED)

    # Already past?  Cancel synchronously and return; no goroutine.
    now = _time.monotonic()
    if now >= deadline_monotonic:
        ctx._cancel(DEADLINE_EXCEEDED)
        return ctx, cancel

    def deadline_waker():
        remaining = deadline_monotonic - _time.monotonic()
        if remaining > 0:
            runloom_c.sched_sleep(remaining)
        if ctx._err is None:
            ctx._cancel(DEADLINE_EXCEEDED)

    runloom_c.go(deadline_waker)
    return ctx, cancel


def WithTimeout(parent, seconds):
    """Sugar around WithDeadline(parent, monotonic() + seconds)."""
    return WithDeadline(parent, _time.monotonic() + seconds)
