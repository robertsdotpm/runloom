"""Cooperative Event / Condition / Semaphore + Thread.join, and the
threading-module patch that installs them."""
from ._base import *  # noqa: F401,F403  (shared foundation)
from .locks import CoLock, CoRLock, _real_BoundedSemaphore, _real_Condition, _real_Event, _real_Lock, _real_RLock, _real_Semaphore, _real_allocate_lock, _real_thread_RLock  # noqa: F401


def _spawn(fn):
    """Spawn a helper goroutine (a wait-timeout waker) on whichever scheduler
    is active: mn_go under M:N (mn_hub_count() > 0), else the single-thread
    go.  A waker spawned via the single-thread go() never runs under mn_run,
    so the timeout would never fire -> a timed wait hangs until notified."""
    if runloom_c.mn_hub_count() > 0:
        return runloom_c.mn_go(fn)
    return runloom_c.go(fn)


class CoEvent(object):
    # Waiters park on a _Parker -- an OS pipe/socketpair fd -- NOT on a
    # runloom_c.Chan.  This looks like overhead (set() does one os.write per
    # waiter), and a channel close() would broadcast in one call, BUT a channel
    # recv-park is not foreign-thread-wakeable: threading.Thread.start() does
    # self._started.set() from the real WORKER thread, and a channel wake racing
    # a single-thread recv-park is lost (the chan park does not keep run() alive,
    # so run() exits and the foreign wake lands on a dead loop).  The fd park
    # keeps run() alive (netpoll_parked) and os.write() is a race-free foreign
    # wake.  See the FINDING in src/runloom_c/chan_waiters.c.inc park_waiter.
    # (Tried CoEvent-on-channel 2026-06, reverted -- it hung Thread.start().)
    #
    # _guard: a real lock making the flag + waiter-queue bookkeeping atomic
    # across M:N hub threads (held only for the O(1) bookkeeping, never across
    # a park).  Without it set()'s waiter snapshot races a concurrent wait()'s
    # append -> a lost wakeup (the appended waiter parks forever).
    __slots__ = ("_flag", "_waiters", "_guard", "__weakref__")

    def __init__(self):
        self._flag    = False
        self._waiters = collections.deque()
        self._guard   = _real_allocate_lock()

    def is_set(self):
        return self._flag
    isSet = is_set

    def set(self):
        self._guard.acquire()
        if self._flag:
            self._guard.release()
            return
        self._flag = True
        waiters, self._waiters = list(self._waiters), collections.deque()
        self._guard.release()
        for p in waiters:
            p.unpark()

    def clear(self):
        with self._guard:
            self._flag = False

    def _at_fork_reinit(self):
        # The flag survives a fork; the parent's parked waiters do not.
        self._guard = _real_allocate_lock()
        self._waiters.clear()

    def wait(self, timeout=None):
        if self._flag:
            return True
        if not _in_goroutine():
            # No cooperative scheduler to wake us; degrade to spin.
            t0 = time.monotonic()
            while not self._flag:
                _raw_time_sleep(0.001)
                if timeout is not None and time.monotonic() - t0 >= timeout:
                    return self._flag
            return True
        p = _Parker()
        self._guard.acquire()
        if self._flag:
            # set() fired while we were building the parker.
            self._guard.release()
            p.release()
            return True
        self._waiters.append(p)
        self._guard.release()
        if timeout is None:
            p.park()
        else:
            # Race park against a wakeup timer (spawned on the active scheduler).
            deadline = time.monotonic() + timeout
            done = [False]
            def waker(parker=p, dl=deadline):
                while not done[0]:
                    remaining = dl - time.monotonic()
                    if remaining <= 0:
                        parker.unpark()
                        return
                    _co_sleep(min(remaining, 0.05))
            _spawn(waker)
            p.park()
            done[0] = True
        p.release()
        return self._flag


class CoCondition(object):
    """Cooperative Condition variable, lock-pattern-compatible with
    threading.Condition."""

    def __init__(self, lock=None):
        self._lock    = lock if lock is not None else CoLock()
        self._waiters = collections.deque()

    def acquire(self, *a, **kw):
        return self._lock.acquire(*a, **kw)
    def release(self):
        self._lock.release()
    def __enter__(self):
        return self._lock.__enter__()
    def __exit__(self, *a):
        return self._lock.__exit__(*a)

    def _at_fork_reinit(self):
        if hasattr(self._lock, "_at_fork_reinit"):
            self._lock._at_fork_reinit()
        self._waiters.clear()

    def wait(self, timeout=None):
        p = _Parker()
        self._waiters.append(p)
        # Release inner lock while parked.
        owned_recursion = None
        if isinstance(self._lock, CoRLock):
            owned_recursion = self._lock._count
            self._lock._count = 1   # release() pops once; ok
        self._lock.release()
        if timeout is None:
            p.park()
            timed_out = False
        else:
            deadline = time.monotonic() + timeout
            if _in_goroutine():
                # Cooperative: a waker goroutine unparks us at the deadline.
                done = [False]
                def waker(parker=p, dl=deadline):
                    while not done[0]:
                        remaining = dl - time.monotonic()
                        if remaining <= 0:
                            parker.unpark()
                            return
                        _co_sleep(min(remaining, 0.05))
                _spawn(waker)
                p.park()
                done[0] = True
            else:
                # Foreign OS thread: no goroutine to run a waker -- let the
                # parker's real select() time out directly.
                p.park(max(0.0, deadline - time.monotonic()))
            timed_out = time.monotonic() >= deadline
        p.release()
        self._lock.acquire()
        if owned_recursion is not None:
            self._lock._count = owned_recursion
        return not timed_out

    def wait_for(self, predicate, timeout=None):
        endtime = None if timeout is None else time.monotonic() + timeout
        result = predicate()
        while not result:
            if endtime is not None:
                remaining = endtime - time.monotonic()
                if remaining <= 0:
                    break
                self.wait(remaining)
            else:
                self.wait()
            result = predicate()
        return result

    def notify(self, n=1):
        for _ in range(n):
            if not self._waiters:
                return
            p = self._waiters.popleft()
            p.unpark()

    def notify_all(self):
        waiters, self._waiters = list(self._waiters), collections.deque()
        for p in waiters:
            p.unpark()
    notifyAll = notify_all


class CoSemaphore(object):
    # _guard: CoLock (cooperative) making _value + _waiters bookkeeping atomic.
    # Must be a cooperative lock, NOT a real OS mutex:
    #   A goroutine can be preempted at any Python opcode (eval-breaker fires at
    #   every bytecode boundary in free-threaded 3.13t).  If it holds a real OS
    #   mutex when preempted, every other hub thread that tries to acquire it
    #   BLOCKS.  With all 8 hubs blocked and the holder in the submission deque,
    #   no hub is free to dispatch the holder -> deadlock.  With a CoLock, a
    #   contending goroutine parks cooperatively (yields its hub thread), so hubs
    #   remain free to dispatch the preempted holder; it releases the lock and
    #   wakes the waiters.  Same parker-before-guard order as CoEvent.wait.
    __slots__ = ("_value", "_waiters", "_guard", "_cancelled", "__weakref__")

    def __init__(self, value=1):
        if value < 0:
            raise ValueError("semaphore initial value must be >= 0")
        self._value     = value
        self._waiters   = collections.deque()
        self._guard     = CoLock()
        self._cancelled = False

    def acquire(self, blocking=True, timeout=None):
        self._guard.acquire()
        if self._value > 0:
            self._value -= 1
            self._guard.release()
            return True
        if not blocking:
            self._guard.release()
            return False
        if self._cancelled:
            self._guard.release()
            return False
        if not _in_goroutine():
            self._guard.release()
            t0 = time.monotonic()
            while True:
                self._guard.acquire()
                if self._value > 0:
                    self._value -= 1
                    self._guard.release()
                    return True
                self._guard.release()
                if timeout is not None and time.monotonic() - t0 >= timeout:
                    return False
                _raw_time_sleep(0.0001)
        # Must park.  Build the parker with the guard RELEASED first: _Parker()
        # can YIELD (on Windows the socketpair wake-fd handshake runs through
        # the cooperative socket path and parks the goroutine).  Re-acquire +
        # re-check afterward: a permit may have appeared while we built it, or
        # cancel_all() may have fired (TOCTOU: goroutine was between guard-release
        # and guard-re-acquire when cancel_all snapshotted the queue and missed it;
        # the _cancelled flag catches this and prevents a permanent park).
        self._guard.release()
        p = _Parker()
        # Waiter state: [parker, active, got_permit]
        # active     = True while live; set to False by the timeout waker
        # got_permit = set to True by release() when it hands this waiter a slot
        w = [p, True, False]
        self._guard.acquire()
        if self._value > 0:
            self._value -= 1
            self._guard.release()
            p.release()
            return True
        if self._cancelled:
            self._guard.release()
            p.release()
            return False
        self._waiters.append(w)
        self._guard.release()
        if timeout is not None:
            # Spawn a single goroutine that sleeps for the full timeout period.
            # Using the full duration (not 0.05s increments like CoCondition) keeps
            # the timer-event rate at 1/goroutine per timeout interval instead of
            # 20+/goroutine, avoiding a thundering herd at 100k goroutines.
            done = [False]
            def _waker(waiter=w, t=timeout, flag=done):
                _co_sleep(t)
                if not flag[0]:
                    waiter[1] = False   # mark timed out
                    waiter[0].unpark()
            _spawn(_waker)
            p.park()
            done[0] = True
        else:
            p.park()
        p.release()
        # w[2] is True if release() handed us a permit; False if cancelled/timed out.
        return w[2]

    def release(self, n=1):
        for _ in range(n):
            self._guard.acquire()
            found = False
            while self._waiters:
                w = self._waiters.popleft()
                if w[1]:    # active (not timed out)?
                    w[2] = True   # got_permit
                    self._guard.release()
                    w[0].unpark()
                    found = True
                    break
                # Stale timed-out waiter: discard (goroutine drains its own parker)
            if not found:
                self._value += 1
                self._guard.release()

    def cancel_all(self):
        """Unpark all waiting goroutines WITHOUT giving them permits.
        acquire() returns False for each woken goroutine.  Used by procutil to
        abort all goroutines queued behind _spawn_sem when the harness stops.
        _cancelled is set FIRST so goroutines that miss the waiter snapshot
        (in the window between guard-release and guard-re-acquire) bail out
        at the _cancelled check instead of parking forever.
        """
        self._guard.acquire()
        self._cancelled = True
        waiters = list(self._waiters)
        self._waiters = collections.deque()
        self._guard.release()
        for w in waiters:
            if w[1]:   # active (not already timed out)
                w[1] = False   # mark inactive — no permit
                w[0].unpark()

    def __enter__(self):
        self.acquire(); return self
    def __exit__(self, *a):
        self.release()


class CoBoundedSemaphore(CoSemaphore):
    __slots__ = ("_initial",)

    def __init__(self, value=1):
        CoSemaphore.__init__(self, value)
        self._initial = value

    def release(self, n=1):
        if self._value + len(self._waiters) + n > self._initial:
            raise ValueError("Semaphore released too many times")
        CoSemaphore.release(self, n)


_orig_thread_join = None


def _patched_thread_join(self, timeout=None):
    """Cooperative Thread.join: a goroutine joining a real OS thread polls
    is_alive() in a _co_sleep loop instead of parking the scheduler on the
    thread's C-level _tstate_lock (which only the dying thread can release,
    so a plain join would freeze every other goroutine until it does).

    Outside a goroutine, the real blocking join is used.  The not-started /
    join-self guards mirror threading.Thread.join so error semantics match."""
    if not self._initialized:
        raise RuntimeError("Thread.__init__() not called")
    if not _in_goroutine():
        return _orig_thread_join(self, timeout)
    if not self._started.is_set():
        raise RuntimeError("cannot join thread before it is started")
    if self is _th.current_thread():
        raise RuntimeError("cannot join current thread")
    deadline = None if timeout is None else time.monotonic() + timeout
    step = 0.0005
    while self.is_alive():
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            _co_sleep(min(step, remaining))
        else:
            _co_sleep(step)
        if step < 0.02:
            step *= 2


def _patch_threading():
    global _orig_thread_join
    _th.Lock      = CoLock
    _th.RLock     = CoRLock
    _th.Event     = CoEvent
    _th.Condition = CoCondition
    _th.Semaphore = CoSemaphore
    _th.BoundedSemaphore = CoBoundedSemaphore
    # Also patch the low-level _thread factories that stdlib internals grab
    # directly (tempfile._once_lock, etc.).  CoLock() is call-compatible with
    # allocate_lock() and degrades to an immediate acquire off-goroutine.
    _thread.allocate_lock = CoLock
    if _real_thread_RLock is not None:
        _thread.RLock = CoRLock
    _orig_thread_join = _th.Thread.join
    _th.Thread.join = _patched_thread_join


def _unpatch_threading():
    _th.Lock      = _real_Lock
    _th.RLock     = _real_RLock
    _th.Event     = _real_Event
    _th.Condition = _real_Condition
    _th.Semaphore = _real_Semaphore
    _th.BoundedSemaphore = _real_BoundedSemaphore
    _thread.allocate_lock = _real_allocate_lock
    if _real_thread_RLock is not None:
        _thread.RLock = _real_thread_RLock
    if _orig_thread_join is not None:
        _th.Thread.join = _orig_thread_join
