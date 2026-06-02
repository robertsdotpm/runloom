"""Cooperative threading.Lock / RLock (CoLock / CoRLock)."""
from ._base import *  # noqa: F401,F403  (shared foundation)

# ============================================================
# threading -- cooperative Lock / RLock / Event / Condition / Semaphore
# ============================================================
_real_Lock      = _th.Lock
_real_RLock     = _th.RLock
_real_Event     = _th.Event
_real_Condition = _th.Condition
_real_Semaphore = _th.Semaphore
_real_BoundedSemaphore = _th.BoundedSemaphore
_real_get_ident = _th.get_ident
# Low-level _thread primitives.  stdlib internals capture these DIRECTLY
# (e.g. tempfile: `from _thread import allocate_lock`), bypassing threading.*,
# so patching only threading.Lock leaves those internal locks real -- and a
# real lock held by one goroutine across a yielding (offloaded) call while
# another goroutine blocks on it FREEZES the single scheduler thread (deadlock).
_real_allocate_lock = _thread.allocate_lock
_real_thread_RLock  = getattr(_thread, "RLock", None)


class CoLock(object):
    """Cooperative mutex.  Non-reentrant.

    When called from a goroutine: park on a parker queue under contention.
    When called from outside any goroutine: degrade to immediate
    acquire/release (the single-thread cooperative model never has true
    cross-thread contention between goroutines).
    """
    # __weakref__: stdlib _thread.lock is weakref-able; without this slot a
    # weakref.ref(threading.Lock()) raises TypeError under monkey (found by the
    # verbatim CPython lock_tests).
    __slots__ = ("_locked", "_owner", "_waiters", "__weakref__")

    def __init__(self):
        self._locked  = False
        self._owner   = None
        self._waiters = collections.deque()

    def acquire(self, blocking=True, timeout=-1):
        cur = pygo.current() if _in_goroutine() else _real_get_ident()
        if not self._locked:
            self._locked = True
            self._owner  = cur
            return True
        if not blocking:
            return False
        if not _in_goroutine():
            # No goroutine to park; spin briefly + yield to the OS.
            t0 = time.monotonic()
            while self._locked:
                _raw_time_sleep(0.0001)
                if timeout is not None and timeout >= 0:
                    if time.monotonic() - t0 >= timeout:
                        return False
            self._locked = True
            self._owner  = cur
            return True
        p = _Parker()
        # _Parker() may yield during socketpair creation (Windows path).
        # If the lock got freed while we yielded, claim it now instead
        # of parking forever -- nobody will unpark us, because nobody
        # saw us in self._waiters when they released.
        if not self._locked:
            self._locked = True
            self._owner  = cur
            p.release()
            return True
        self._waiters.append(p)
        p.park()
        p.release()
        self._owner = cur
        return True

    def release(self):
        if not self._locked:
            # A forked child running stdlib teardown (threading._after_fork)
            # can reach a release whose matching acquire happened in the
            # parent / under an identity that no longer exists here.  That is
            # benign post-fork noise, not a real double-release, so swallow
            # it; a genuine in-process double-release still raises.
            if _is_forked_child():
                return
            raise RuntimeError("release unlocked lock")
        self._owner = None
        if self._waiters:
            # Ownership transfers to next waiter; keep _locked True.
            p = self._waiters.popleft()
            p.unpark()
        else:
            self._locked = False

    def locked(self):
        return self._locked

    def _at_fork_reinit(self):
        # Mirror _thread.lock._at_fork_reinit: the child inherits none of the
        # parent's goroutines, so reset to a fresh unlocked state.  Called via
        # os.register_at_fork by stdlib modules (concurrent.futures.thread,
        # logging, ...) that build a module-global Lock at import time.
        self._locked  = False
        self._owner   = None
        self._waiters.clear()

    def __enter__(self):
        self.acquire(); return self
    def __exit__(self, *a):
        self.release()


class CoRLock(object):
    """Cooperative re-entrant lock."""
    __slots__ = ("_lock", "_owner", "_count", "__weakref__")

    def __init__(self):
        self._lock  = CoLock()
        self._owner = None
        self._count = 0

    def acquire(self, blocking=True, timeout=-1):
        cur = pygo.current() if _in_goroutine() else _real_get_ident()
        # `self._owner is not None` guard: a fresh/released lock has owner
        # None, and a caller whose identity is also None (e.g. the forked
        # child running threading._after_fork, where no scheduler is live so
        # pygo.current() is None) must NOT be mistaken for the owner -- that
        # would skip the real inner acquire and then blow up on release with
        # "release unlocked lock".
        if self._owner is not None and self._owner == cur:
            self._count += 1
            return True
        ok = self._lock.acquire(blocking, timeout)
        if ok:
            self._owner = cur
            self._count = 1
        return ok

    def release(self):
        cur = pygo.current() if _in_goroutine() else _real_get_ident()
        if self._owner != cur:
            # In a forked child pygo.current() returns a different G each
            # call, so a legitimate acquire/release pair (e.g. the CoRLock
            # threading._after_fork builds for _active_limbo_lock) looks like
            # a foreign release.  Detect the fork and reset instead of raising.
            if _is_forked_child():
                self._count = 0
                self._owner = None
                if self._lock.locked():
                    self._lock.release()
                return
            raise RuntimeError("cannot release un-acquired lock")
        self._count -= 1
        if self._count == 0:
            self._owner = None
            self._lock.release()

    # ---- RLock-completeness API (CPython threading._RLock parity) ----
    # stdlib internals call these on anything that quacks like an RLock:
    # threading.Condition uses _release_save / _acquire_restore to drop a
    # recursively-held lock across a wait; multiprocessing.resource_tracker
    # asserts _recursion_count() > 0.  Without them those paths AttributeError.
    def _is_owned(self):
        cur = pygo.current() if _in_goroutine() else _real_get_ident()
        return self._owner is not None and self._owner == cur

    def _recursion_count(self):
        # Recursive-acquire depth held by the *current* owner, else 0 -- matches
        # _thread.RLock._recursion_count.
        return self._count if self._is_owned() else 0

    def _release_save(self):
        if self._count == 0:
            raise RuntimeError("cannot release un-acquired lock")
        count, owner = self._count, self._owner
        self._count = 0
        self._owner = None
        self._lock.release()
        return (count, owner)

    def _acquire_restore(self, state):
        count, owner = state
        self._lock.acquire()
        self._count = count
        self._owner = owner
        return True

    def _at_fork_reinit(self):
        self._lock._at_fork_reinit()
        self._owner = None
        self._count = 0

    def __enter__(self):
        self.acquire(); return self
    def __exit__(self, *a):
        self.release()
