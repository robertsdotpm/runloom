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
import os
import socket as _socket
import sys as _sys
import time as _time
import runloom_c

_IS_WINDOWS = _sys.platform == "win32"

_READ = 1   # runloom_c.wait_fd READ direction


# A permanent, never-readable, never-closed pipe used purely as a parking
# target for deadline-waker fibers.  A waker parks in wait_fd(<read end>, READ,
# remaining_ms): the park is deadline-BOUNDED (the ms timeout) yet WAKEABLE --
# cancel() calls the waker fiber's cancel_wait_fd(), so a cancelled
# context's run() returns at once instead of lingering to the original
# deadline (Go stops its timer on cancel; we wake the equivalent fiber).  The
# fd number never changes and is never closed, so it cannot poison the netpoll
# arm cache (the fd-reuse hazard), and many wakers can park on it at once (each
# wakes/cancels independently).
# On Windows the readiness backend (iocp-afd) can ONLY poll Winsock sockets, not
# pipe fds, so wait_fd on an os.pipe() read end fails (OSError) -- use a socketpair
# there instead (an AF_INET loopback pair, which AFD can poll), the same thing
# monkey/_base.py + runloom.aio already do.  Nothing is ever written to either end,
# so READ never becomes ready: a waker only ever times out or is cancelled.
_wake_rfd = None
_wake_wfd = None
_wake_socks = None   # keep the Windows socketpair objects alive (so the fds stay valid)


def _wake_fd():
    global _wake_rfd, _wake_wfd, _wake_socks
    if _wake_rfd is None:
        if _IS_WINDOWS:
            _s1, _s2 = _socket.socketpair()
            _wake_socks = (_s1, _s2)   # never closed -- a permanent parking target
            _wake_rfd = _s1.fileno()
            _wake_wfd = _s2.fileno()
        else:
            _wake_rfd, _wake_wfd = os.pipe()
    return _wake_rfd


def _spawn(fn):
    """Spawn the deadline fiber on whichever scheduler is active.

    WithDeadline/WithTimeout arm a fiber that fires when the deadline
    passes; it must run under the M:N scheduler (mn_run) too, not only the
    single-thread one -- otherwise the deadline silently never fires under
    mn_run.  mn_hub_count() > 0 means mn_init() is in effect."""
    if runloom_c.mn_hub_count() > 0:
        return runloom_c.mn_fiber(fn)
    return runloom_c.fiber(fn)


# Error sentinels (strings -- runloom deliberately avoids custom exception
# classes here so the public API stays close to Go's, where ctx.Err()
# just returns context.Canceled or context.DeadlineExceeded).
CANCELED          = "cancelled"
DEADLINE_EXCEEDED = "deadline_exceeded"


class _BackgroundCtx(object):
    """The root context.  Never cancelled, never has a deadline.  Used
    as the parent for the first WithCancel/WithTimeout call at the top
    of a fiber tree."""
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

    __slots__ = ("done", "_parent", "_err", "_children", "_deadline",
                 "_deadline_g")

    def __init__(self, parent, deadline=None):
        self.done        = runloom_c.Chan(1)
        self._parent     = parent
        self._err        = None
        self._children   = []
        self._deadline   = deadline
        self._deadline_g = None   # the deadline-waker fiber, if armed

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
        # Wake the deadline-waker fiber (if armed) so it exits immediately
        # instead of sleeping to the original deadline -- otherwise a cancelled
        # WithTimeout/WithDeadline keeps a fiber alive (and run() blocked) for
        # the full timeout.  Covers both direct cancel() and a transitive
        # cancel from this ctx's parent (this runs in both).
        g = self._deadline_g
        if g is not None:
            self._deadline_g = None
            try:
                g.cancel_wait_fd()
            except Exception:
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

    # Already past?  Cancel synchronously and return; no fiber.
    now = _time.monotonic()
    if now >= deadline_monotonic:
        ctx._cancel(DEADLINE_EXCEEDED)
        return ctx, cancel

    def deadline_waker():
        # Park bounded by the deadline, but cancellable: if cancel() (or a
        # transitive parent cancel) fires first it wakes us via cancel_wait_fd
        # so run() doesn't linger.  The pre-park _err check handles the common
        # "cancel before this fiber is first scheduled" case with no park at
        # all.
        if ctx._err is None:
            remaining = deadline_monotonic - _time.monotonic()
            if remaining > 0:
                runloom_c.wait_fd(_wake_fd(), _READ, int(remaining * 1000) + 1)
        if ctx._err is None:
            ctx._cancel(DEADLINE_EXCEEDED)

    ctx._deadline_g = _spawn(deadline_waker)
    return ctx, cancel


def WithTimeout(parent, seconds):
    """Sugar around WithDeadline(parent, monotonic() + seconds)."""
    return WithDeadline(parent, _time.monotonic() + seconds)
