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
# real lock held by one fiber across a yielding (offloaded) call while
# another fiber blocks on it FREEZES the single scheduler thread (deadlock).
_real_allocate_lock = _thread.allocate_lock
_real_thread_RLock  = getattr(_thread, "RLock", None)


class CoLock(object):
    """Cooperative mutex.  Non-reentrant.  M:N-safe.

    Backed by runloom_c.Mutex (a capacity-1 channel: one buffered token ==
    unlocked).  The channel's formally-verified cross-hub park/wake gives
    real mutual exclusion across M:N hub threads -- the previous non-atomic
    ``if not self._locked: self._locked = True`` lost updates / deadlocked
    once fibers ran on several hubs in parallel.

    Parking is only legal from a fiber, so:
      * fiber, blocking, no timeout -> Mutex.lock() (parks on contention);
      * non-blocking / timeout / a foreign OS thread (e.g. the
        multiprocessing.Queue feeder) -> try_lock(), spinning cooperatively
        (fiber) or with a raw sleep (foreign thread) -- never parks a
        non-fiber.
    """
    # __weakref__: stdlib _thread.lock is weakref-able; without this slot a
    # weakref.ref(threading.Lock()) raises TypeError under monkey (found by the
    # verbatim CPython lock_tests).
    __slots__ = ("_mu", "__weakref__")

    def __init__(self):
        self._mu = CoFMutex()

    def acquire(self, blocking=True, timeout=-1):
        # _mu is a CoFMutex (foreign-safe): acquire() handles the fiber-vs-foreign
        # and timed/untimed dispatch internally -- a fiber parks 0-fd
        # (foreign_wakeable, keeps run() alive); a FOREIGN OS thread parks on an
        # fd _Parker (real select, woken by os.write) rather than busy-spinning;
        # and a foreign holder no longer strands a parked fiber.
        if not blocking:
            return self._mu.try_lock()
        to = None if (timeout is None or timeout < 0) else timeout
        return self._mu.acquire(blocking=True, timeout=to)

    def release(self):
        try:
            self._mu.unlock()               # raises "release unlocked lock"
        except RuntimeError:
            # A forked child running stdlib teardown (threading._after_fork)
            # can reach a release whose matching acquire happened in the
            # parent.  Benign post-fork noise, not a real double-release.
            if _is_forked_child():
                return
            raise

    def locked(self):
        return self._mu.locked()

    def _at_fork_reinit(self):
        # Mirror _thread.lock._at_fork_reinit: the child inherits none of the
        # parent's fibers, so reset to a fresh unlocked mutex.  Called via
        # os.register_at_fork by stdlib modules (concurrent.futures.thread,
        # logging, ...) that build a module-global Lock at import time.
        self._mu = CoFMutex()

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
        cur = runloom.current() if _in_fiber() else _real_get_ident()
        # `self._owner is not None` guard: a fresh/released lock has owner
        # None, and a caller whose identity is also None (e.g. the forked
        # child running threading._after_fork, where no scheduler is live so
        # runloom.current() is None) must NOT be mistaken for the owner -- that
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
        cur = runloom.current() if _in_fiber() else _real_get_ident()
        if self._owner != cur:
            # In a forked child runloom.current() returns a different G each
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
        cur = runloom.current() if _in_fiber() else _real_get_ident()
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
