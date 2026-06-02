"""Cooperative select.select and select.poll/epoll/kqueue (selectors)."""
from ._base import *  # noqa: F401,F403  (shared foundation)
from .sockets import _netpoll_unregister  # noqa: F401

# ============================================================
# select.select
# ============================================================
_orig_select_select = None


def _fd_of(x):
    return x.fileno() if hasattr(x, "fileno") else int(x)


def _patched_select(rlist, wlist, xlist, timeout=None):
    if not _in_goroutine():
        return _orig_select_select(rlist, wlist, xlist, timeout)

    # On Windows, only SOCKET handles can be polled.  If any fd in the
    # request isn't a socket, fall back to the OS select (which will
    # itself reject non-sockets -- same behaviour as outside a
    # goroutine, so the caller sees a consistent error path).  This
    # avoids parking forever on wait_fd for a pipe/file fd that the
    # netpoll backend can't drive.
    if _IS_WINDOWS:
        for fd_obj in list(rlist) + list(wlist) + list(xlist):
            fd = _fd_of(fd_obj)
            try:
                os.fstat(fd)
                # fstat on a socket fd raises on Windows; if it
                # succeeds, the fd is NOT a socket.
                return _orig_select_select(rlist, wlist, xlist, timeout)
            except OSError:
                pass  # likely a socket -- continue with wait_fd path

    n = len(rlist) + len(wlist)
    # Fast path: one fd, no xlist -> map to wait_fd directly.
    if n == 1 and not xlist:
        if rlist:
            fd, events, src = _fd_of(rlist[0]), READ, "r"
            obj = rlist[0]
        else:
            fd, events, src = _fd_of(wlist[0]), WRITE, "w"
            obj = wlist[0]
        timeout_ms = -1 if timeout is None else max(0, int(timeout * 1000))
        try:
            ready = pygo_core.wait_fd(fd, events, timeout_ms)
        except OSError:
            return _orig_select_select(rlist, wlist, xlist, timeout)
        if ready == 0:
            return [], [], []
        return ([obj], [], []) if src == "r" else ([], [obj], [])

    # Multi-fd: short non-blocking selects + yields.  Not free, but
    # makes select.select() cooperative enough for stdlib internals.
    deadline = None if timeout is None else time.monotonic() + timeout
    step = 0.001
    while True:
        try:
            r, w, x = _orig_select_select(rlist, wlist, xlist, 0)
        except (OSError, ValueError):
            return _orig_select_select(rlist, wlist, xlist, timeout)
        if r or w or x:
            return r, w, x
        if deadline is not None:
            now = time.monotonic()
            if now >= deadline:
                return [], [], []
            _co_sleep(min(step, deadline - now))
        else:
            _co_sleep(step)


def _patch_select():
    global _orig_select_select
    _orig_select_select = _select_mod.select
    _select_mod.select = _patched_select


def _unpatch_select():
    _select_mod.select = _orig_select_select


# ============================================================
# selectors -- cooperative select.poll / select.epoll / select.kqueue
#
# The high-level `selectors` module is what modern stdlib actually blocks
# on: selectors.DefaultSelector is EpollSelector on Linux, KqueueSelector
# on *BSD/macOS, and subprocess.communicate() uses PollSelector.  None of
# those route through select.select (only SelectSelector does, and that is
# already covered by the `select` category).  Each of the others builds its
# backing object by calling select.poll() / select.epoll() / select.kqueue()
# at instantiation time -- looked up dynamically on the `select` module --
# so replacing those three factories with cooperative wrappers makes the
# whole `selectors` module cooperative for free, and also covers code that
# uses select.poll/epoll/kqueue directly (socketserver, asyncore-style
# loops, hand-rolled poll loops).
#
# Strategy per primitive:
#   epoll / kqueue: the object owns a real kernel fd (fileno()), and an
#     epoll/kqueue fd is itself pollable -- it signals readable exactly when
#     it has >=1 ready event.  So we park on wait_fd(self.fileno(), READ)
#     and then drain with a non-blocking poll(0).  Fully event-driven, no
#     busy-poll, no goroutine fan-out, no leaked parkers.
#   poll: select.poll has no backing fd, so we fall back to a non-blocking
#     poll(0) + cooperative yield loop (same shape as multi-fd select.select).
#
# Outside a goroutine every wrapper degrades to the real blocking call, so
# helper threads keep working after patch().
# ============================================================
_real_select_poll  = getattr(_select_mod, "poll", None)
_real_select_epoll = getattr(_select_mod, "epoll", None)
_real_select_kqueue = getattr(_select_mod, "kqueue", None)

# Cap on how long a single cooperative wait blocks before re-probing, so a
# wait that was registered before its fd became ready (or a level-triggered
# edge we already drained) can never wedge longer than this.  The epoll/
# kqueue fd readiness wakes us immediately in the common case; this is only
# the backstop.
_SELECTOR_REPROBE_S = 0.05


def _backing_fd_wait(real_obj, deadline):
    """Park on an epoll/kqueue object's own fd until it has ready events
    (or the per-iteration re-probe cap elapses, or the caller deadline is
    hit).  Returns False if the caller deadline has already passed."""
    if deadline is not None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        timeout_ms = max(1, int(min(_SELECTOR_REPROBE_S, remaining) * 1000))
    else:
        timeout_ms = int(_SELECTOR_REPROBE_S * 1000)
    try:
        pygo_core.wait_fd(real_obj.fileno(), READ, timeout_ms)
    except OSError:
        # fileno() gone (closed under us) or netpoll refused it -- let the
        # caller re-probe with poll(0), which will surface the real error.
        pass
    return True


class CoPoll(object):
    """Cooperative wrapper around select.poll().

    poll objects have no kernel fd of their own, so this is a non-blocking
    poll(0) + yield loop -- the same proven shape as the multi-fd
    select.select() path.  register/modify/unregister forward to the real
    object via __getattr__."""
    __slots__ = ("_r",)

    def __init__(self):
        self._r = _real_select_poll()

    def poll(self, timeout=None):
        # poll() timeout is in MILLISECONDS; None or negative == infinite.
        if not _in_goroutine():
            return self._r.poll(timeout)
        if timeout is None or timeout < 0:
            deadline = None
        else:
            deadline = time.monotonic() + timeout / 1000.0
        step = 0.0005
        while True:
            ev = self._r.poll(0)
            if ev:
                return ev
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return []
                _co_sleep(min(step, remaining))
            else:
                _co_sleep(step)
            if step < 0.02:
                step *= 2

    def __getattr__(self, name):
        return getattr(self._r, name)


class CoEpoll(object):
    """Cooperative wrapper around select.epoll(): parks on the epoll fd."""
    __slots__ = ("_r",)

    def __init__(self, sizehint=-1, flags=0):
        self._r = _real_select_epoll(sizehint, flags)

    @classmethod
    def fromfd(cls, fd):
        self = object.__new__(cls)
        self._r = _real_select_epoll.fromfd(fd)
        return self

    def poll(self, timeout=None, maxevents=-1):
        # epoll.poll() timeout is in SECONDS (float); None or negative ==
        # infinite.  maxevents -1 == unlimited.
        if not _in_goroutine():
            return self._r.poll(timeout, maxevents)
        if timeout is None or timeout < 0:
            deadline = None
        else:
            deadline = time.monotonic() + timeout
        while True:
            ev = self._r.poll(0, maxevents)
            if ev:
                return ev
            if not _backing_fd_wait(self._r, deadline):
                return []

    def fileno(self):
        return self._r.fileno()

    def close(self):
        # The epoll fd is about to disappear; drop its netpoll registration
        # so a later fd reuse re-registers cleanly (epoll.close() goes
        # straight to the C close, bypassing our patched os.close).
        if _netpoll_unregister is not None:
            try:
                fd = self._r.fileno()
                if fd >= 0:
                    _netpoll_unregister(fd)
            except (OSError, ValueError):
                pass
        return self._r.close()

    @property
    def closed(self):
        return self._r.closed

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()

    def __getattr__(self, name):
        return getattr(self._r, name)


class CoKqueue(object):
    """Cooperative wrapper around select.kqueue(): parks on the kqueue fd.

    kqueue.control(changelist, max_events, timeout) both applies changes
    and retrieves events.  We split it: apply the changelist with a
    register-only control (max_events=0), then park on the kqueue fd and
    drain with a non-blocking control."""
    __slots__ = ("_r",)

    def __init__(self):
        self._r = _real_select_kqueue()

    @classmethod
    def fromfd(cls, fd):
        self = object.__new__(cls)
        self._r = _real_select_kqueue.fromfd(fd)
        return self

    def control(self, changelist, max_events, timeout=None):
        # max_events == 0 means register-only (never waits) -- pass through.
        if not _in_goroutine() or not max_events:
            return self._r.control(changelist, max_events, timeout)
        if changelist:
            self._r.control(changelist, 0)          # apply, retrieve nothing
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            ev = self._r.control(None, max_events, 0)
            if ev:
                return ev
            if not _backing_fd_wait(self._r, deadline):
                return []

    def fileno(self):
        return self._r.fileno()

    def close(self):
        if _netpoll_unregister is not None:
            try:
                fd = self._r.fileno()
                if fd >= 0:
                    _netpoll_unregister(fd)
            except (OSError, ValueError):
                pass
        return self._r.close()

    @property
    def closed(self):
        return self._r.closed

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()

    def __getattr__(self, name):
        return getattr(self._r, name)


import selectors as _selectors_mod

# selectors.PollSelector / EpollSelector / KqueueSelector each capture their
# backing factory as a *class attribute* (_selector_cls) at import time, so
# replacing select.poll/epoll/kqueue is not enough on its own -- the already
# imported selector classes have to be flipped too.  We do both: the select.*
# factories for code that uses select.poll()/epoll()/kqueue() directly, and
# the selectors._selector_cls attributes for code that goes through the
# high-level `selectors` module (subprocess, socketserver, http.server, ...).
_orig_selector_cls = {}    # selectors class name -> original _selector_cls

_SELECTORS_BINDINGS = (
    ("PollSelector",   CoPoll,   _real_select_poll),
    ("EpollSelector",  CoEpoll,  _real_select_epoll),
    ("KqueueSelector", CoKqueue, _real_select_kqueue),
)


def _patch_selectors():
    # Only patch what the platform actually provides: epoll is Linux-only,
    # kqueue is *BSD/macOS-only, poll is most POSIX.
    if _real_select_poll is not None:
        _select_mod.poll = CoPoll
    if _real_select_epoll is not None:
        _select_mod.epoll = CoEpoll
    if _real_select_kqueue is not None:
        _select_mod.kqueue = CoKqueue
    for clsname, co_cls, real_factory in _SELECTORS_BINDINGS:
        if real_factory is None:
            continue
        sel_cls = getattr(_selectors_mod, clsname, None)
        if sel_cls is None or not hasattr(sel_cls, "_selector_cls"):
            continue
        _orig_selector_cls[clsname] = sel_cls._selector_cls
        sel_cls._selector_cls = co_cls


def _unpatch_selectors():
    if _real_select_poll is not None:
        _select_mod.poll = _real_select_poll
    if _real_select_epoll is not None:
        _select_mod.epoll = _real_select_epoll
    if _real_select_kqueue is not None:
        _select_mod.kqueue = _real_select_kqueue
    for clsname, orig in list(_orig_selector_cls.items()):
        sel_cls = getattr(_selectors_mod, clsname, None)
        if sel_cls is not None:
            sel_cls._selector_cls = orig
    _orig_selector_cls.clear()
