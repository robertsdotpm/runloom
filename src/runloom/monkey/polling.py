"""Cooperative select.select and select.poll/epoll/kqueue (selectors)."""
from ._base import *  # noqa: F401,F403  (shared foundation)
from .sockets import _netpoll_unregister  # noqa: F401

# Real select.* factories, captured once.  Used by BOTH the select.select
# reimplementation below and the selectors wrappers further down.
_real_select_poll   = getattr(_select_mod, "poll", None)
_real_select_epoll  = getattr(_select_mod, "epoll", None)
_real_select_kqueue = getattr(_select_mod, "kqueue", None)

# epoll event masks for the cooperative select.select reimplementation.
_EPOLLIN  = getattr(_select_mod, "EPOLLIN", None)
_EPOLLOUT = getattr(_select_mod, "EPOLLOUT", None)
_EPOLLPRI = getattr(_select_mod, "EPOLLPRI", None)
_EPOLLERR = getattr(_select_mod, "EPOLLERR", 0)
_EPOLLHUP = getattr(_select_mod, "EPOLLHUP", 0)
# The cooperative path needs a real epoll AND its event constants (Linux).
_CO_EPOLL_OK = _real_select_epoll is not None and _EPOLLIN is not None

# kqueue constants for the cooperative select.select reimplementation (macOS/BSD).
_KQ_FILTER_READ = getattr(_select_mod, "KQ_FILTER_READ", 0)
_KQ_FILTER_WRITE = getattr(_select_mod, "KQ_FILTER_WRITE", 2)
_KQ_EV_ADD = getattr(_select_mod, "KQ_EV_ADD", 1)
_KQ_EV_ERROR = getattr(_select_mod, "KQ_EV_ERROR", 0x4000)
_CO_KQUEUE_OK = _real_select_kqueue is not None

# ============================================================
# select.select -- cooperative, on netpoll
#
# select.select waits on a SET of fds; runloom's wait_fd parks on ONE fd.  An
# epoll fd is itself pollable (readable exactly when it has >=1 ready event),
# so we express select.select the same way the epoll/kqueue selector wrappers
# do: register the fds on a transient epoll, park on THAT epoll's fd via
# wait_fd, drain with a non-blocking poll(0), and map the events back to the
# three result lists.  This:
#   * never calls CPython's select_select_impl (whose three pylist[FD_SETSIZE+1]
#     arrays are a ~51 KB single C frame that overflows the fiber stack --
#     the only stdlib leaf that does; see docs/cooperative_stdlib_coverage.md),
#   * consumes NO pool thread -- the fiber parks on netpoll like every
#     other socket, so it scales to a million concurrent waiters, and
#   * keeps the hub free (cooperative) without the heavier offload fallback.
#
# Fallback to a pool-thread offload (the blocking OS select runs on a real
# 8 MB thread stack, the fiber merely parks on the offload) ONLY when there
# is no usable epoll -- a non-epollable fd (regular file/char device, which
# select treats as always-ready but epoll rejects) or a platform without epoll
# (Windows; *BSD/macOS could grow a kqueue path later).  The offload never runs
# select inline on the fiber stack, so the fat frame is never an issue.
# ============================================================
_orig_select_select = None


class _SelectFallback(Exception):
    """Internal: this select can't ride epoll (non-epollable fd / no epoll);
    fall back to the pool-thread offload."""


def _fd_of(x):
    return x.fileno() if hasattr(x, "fileno") else int(x)


def _select_map_back(ready, rfds, wfds, xfds):
    """Map epoll (fd, eventmask) results back to select's (r, w, x) lists,
    returning the ORIGINAL objects the caller passed.  An errored/hung-up fd
    is reported ready in whichever lists it was registered (matching how
    CPython emulates select over poll)."""
    rmask = _EPOLLIN | _EPOLLERR | _EPOLLHUP
    wmask = _EPOLLOUT | _EPOLLERR | _EPOLLHUP
    xmask = _EPOLLPRI | _EPOLLERR
    r = []; w = []; x = []
    for fd, ev in ready:
        if fd in rfds and (ev & rmask):
            r.append(rfds[fd])
        if fd in wfds and (ev & wmask):
            w.append(wfds[fd])
        if fd in xfds and (ev & xmask):
            x.append(xfds[fd])
    return r, w, x


def _co_select_via_epoll(rlist, wlist, xlist, timeout):
    """Cooperative select on a transient epoll; raises _SelectFallback if any
    fd can't be registered (caller then offloads the whole call)."""
    rfds = {}; wfds = {}; xfds = {}
    masks = {}
    for o in rlist:
        fd = _fd_of(o); rfds[fd] = o; masks[fd] = masks.get(fd, 0) | _EPOLLIN
    for o in wlist:
        fd = _fd_of(o); wfds[fd] = o; masks[fd] = masks.get(fd, 0) | _EPOLLOUT
    for o in xlist:
        fd = _fd_of(o); xfds[fd] = o; masks[fd] = masks.get(fd, 0) | _EPOLLPRI

    ep = _real_select_epoll()
    try:
        for fd, m in masks.items():
            try:
                ep.register(fd, m)
            except (OSError, ValueError):
                # Regular files / unpollable fds: epoll rejects them but select
                # treats them as ready.  Hand the whole call to the offload.
                raise _SelectFallback()
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            ready = ep.poll(0)              # non-blocking drain
            if ready:
                return _select_map_back(ready, rfds, wfds, xfds)
            if timeout is not None and timeout <= 0:
                return [], [], []           # non-blocking: nothing ready
            if deadline is None:
                wait_ms = -1
            else:
                rem = deadline - time.monotonic()
                if rem <= 0:
                    return [], [], []
                wait_ms = max(1, int(rem * 1000))
            try:
                runloom_c.wait_fd(ep.fileno(), READ, wait_ms)  # park on epoll fd
            except OSError:
                raise _SelectFallback()
    finally:
        # Drop the transient epoll fd's netpoll registration before closing it,
        # so a later fd reuse re-registers cleanly.
        try:
            if _netpoll_unregister is not None:
                fd = ep.fileno()
                if fd >= 0:
                    _netpoll_unregister(fd)
        except (OSError, ValueError):
            pass
        ep.close()


def _co_select_via_kqueue(rlist, wlist, xlist, timeout):
    """Cooperative select on a transient kqueue; raises _SelectFallback if any
    fd can't be registered (caller then offloads the whole call).  macOS/BSD."""
    rfds = {}; wfds = {}; xfds = {}
    for o in rlist:
        fd = _fd_of(o); rfds[fd] = o
    for o in wlist:
        fd = _fd_of(o); wfds[fd] = o
    for o in xlist:
        fd = _fd_of(o); xfds[fd] = o

    kq = _real_select_kqueue()
    try:
        changelist = []
        for fd in rfds:
            changelist.append(_select_mod.kevent(fd, _KQ_FILTER_READ, _KQ_EV_ADD))
        for fd in wfds:
            changelist.append(_select_mod.kevent(fd, _KQ_FILTER_WRITE, _KQ_EV_ADD))
        # xlist (exceptional conditions) don't have a direct kqueue mapping;
        # skip them (matches the epolling behavior for non-standard events).

        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            # Non-blocking drain: apply changelist (register only) and collect events.
            try:
                ready = kq.control(changelist, 1000, 0)  # max 1000 events per drain
                changelist = []  # Only apply changes on first iteration
            except (OSError, ValueError):
                raise _SelectFallback()
            if ready:
                # Map kqueue events back to select's (r, w, x) lists.
                r = []; w = []; x = []
                for ev in ready:
                    fd = ev.ident
                    if ev.flags & _KQ_EV_ERROR:
                        # Error on this fd: report in all requested lists (like epoll).
                        if fd in rfds:
                            r.append(rfds[fd])
                        if fd in wfds:
                            w.append(wfds[fd])
                        if fd in xfds:
                            x.append(xfds[fd])
                    else:
                        if ev.filter == _KQ_FILTER_READ and fd in rfds:
                            r.append(rfds[fd])
                        elif ev.filter == _KQ_FILTER_WRITE and fd in wfds:
                            w.append(wfds[fd])
                return r, w, x
            if timeout is not None and timeout <= 0:
                return [], [], []
            if deadline is None:
                wait_ms = 50  # reprobe frequently on macOS (kqueue fd wake is reliable)
            else:
                rem = deadline - time.monotonic()
                if rem <= 0:
                    return [], [], []
                wait_ms = max(1, int(min(0.05, rem) * 1000))
            try:
                runloom_c.wait_fd(kq.fileno(), READ, wait_ms)  # park on kqueue fd
            except OSError:
                raise _SelectFallback()
    finally:
        try:
            if _netpoll_unregister is not None:
                fd = kq.fileno()
                if fd >= 0:
                    _netpoll_unregister(fd)
        except (OSError, ValueError):
            pass
        kq.close()


def _patched_select(rlist, wlist, xlist, timeout=None):
    if not _in_fiber():
        return _orig_select_select(rlist, wlist, xlist, timeout)

    # select.select() accepts ANY iterable of fds, and selectors.SelectSelector
    # passes SETS (self._readers / self._writers).  Normalise to lists; real
    # select.select returns lists regardless of input type, so this matches.
    rlist = list(rlist)
    wlist = list(wlist)
    xlist = list(xlist)

    # No fds: select is just a (cooperative) sleep for `timeout`.
    if not rlist and not wlist and not xlist:
        if timeout is None:
            # Degenerate "block forever waiting for nothing": park in long
            # cooperative increments (cancellable) rather than busy-wait.
            while True:
                _co_sleep(3600.0)
        if timeout > 0:
            _co_sleep(timeout)
        return [], [], []

    # Cooperative path: register the fds on a transient epoll/kqueue and park
    # on ITS fd via netpoll -- no select_select_impl fat frame, no pool thread.
    if _CO_EPOLL_OK:
        try:
            return _co_select_via_epoll(rlist, wlist, xlist, timeout)
        except _SelectFallback:
            pass

    if _CO_KQUEUE_OK:
        try:
            return _co_select_via_kqueue(rlist, wlist, xlist, timeout)
        except _SelectFallback:
            pass

    # Fallback (no epoll/kqueue / non-epollable fd): run the blocking OS select
    # on a pool thread (8 MB stack -- the fat frame is fine there) and PARK this
    # fiber on the offload.  We never call _orig_select_select inline: its
    # ~51 KB frame overflows the fiber's C stack.  A normalised timeout<=0
    # stays non-blocking on the pool thread.
    pool_timeout = 0 if (timeout is not None and timeout <= 0) else timeout
    return _get_backend().submit(
        _orig_select_select, (rlist, wlist, xlist, pool_timeout), {})


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
#     busy-poll, no fiber fan-out, no leaked parkers.
#   poll: select.poll has no backing fd, so we fall back to a non-blocking
#     poll(0) + cooperative yield loop (same shape as multi-fd select.select).
#
# Outside a fiber every wrapper degrades to the real blocking call, so
# helper threads keep working after patch().
# (_real_select_poll / _real_select_epoll / _real_select_kqueue are captured
# once at the top of this module.)
# ============================================================
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
        runloom_c.wait_fd(real_obj.fileno(), READ, timeout_ms)
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
        if not _in_fiber():
            return self._r.poll(timeout)
        # A poll object has no kernel fd of its own to park on, so (unlike
        # epoll/kqueue, which park on their backing fd, and select.select,
        # which parks on a transient epoll fd) there's no clean netpoll target.
        # The old form busy-polled poll(0) + _co_sleep on the hub; instead we
        # offload the blocking poll to a pool thread and PARK on the offload --
        # the fiber yields, other fibers run, no hub spin.  (poll's C
        # frame is heap-backed, so this is about cooperation, not the
        # select_select_impl stack-frame issue.)  Backs selectors.PollSelector.
        return _get_backend().submit(self._r.poll, (timeout,), {})

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
        if not _in_fiber():
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
        if not _in_fiber() or not max_events:
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
