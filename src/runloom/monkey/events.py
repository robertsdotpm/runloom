"""Cooperative Event / Condition / Semaphore + Thread.join, and the
threading-module patch that installs them."""
from ._base import *  # noqa: F401,F403  (shared foundation)
from .locks import CoLock, CoRLock, _real_BoundedSemaphore, _real_Condition, _real_Event, _real_Lock, _real_RLock, _real_Semaphore, _real_allocate_lock, _real_thread_RLock  # noqa: F401


def _spawn(fn):
    """Spawn a helper fiber (a wait-timeout waker) on whichever scheduler
    is active: mn_go under M:N (mn_hub_count() > 0), else the single-thread
    go.  A waker spawned via the single-thread go() never runs under mn_run,
    so the timeout would never fire -> a timed wait hangs until notified."""
    if runloom_c.mn_hub_count() > 0:
        return runloom_c.mn_go(fn)
    return runloom_c.go(fn)


class CoEvent(object):
    # Waiters park on a _Parker, NOT a runloom_c.Chan: a channel recv-park is not
    # foreign-thread-wakeable (threading.Thread.start() does self._started.set()
    # from the real WORKER thread; a channel wake racing a single-thread recv-park
    # is lost -- the chan park does not keep run() alive, so run() exits and the
    # foreign wake lands on a dead loop).  See the FINDING in
    # src/runloom_c/chan_waiters.c.inc park_waiter.  (Tried CoEvent-on-channel
    # 2026-06, reverted -- it hung Thread.start().)
    #
    # _Parker picks the park mechanism (see _base.py): a GOROUTINE wait -- TIMED or
    # untimed -- parks IN MEMORY (runloom_c.park[(timeout=...)], ZERO per-waiter
    # fds; a million waiters cost ~1 shared anchor fd, not ~2 each), woken by
    # g.wake() (set) or, for a timed wait, the scheduler's timer heap at the
    # deadline (the same parked_safe CAS, exactly-once).  Only a FOREIGN-thread
    # wait keeps an OS pipe/socketpair fd (park() can't serve a non-fiber),
    # woken by os.write.  set() -> _unpark_all wakes both kinds; the run-alive
    # anchor + g.wake() make a foreign SETTER race-free for the in-memory waiters.
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
        _unpark_all(waiters)   # batched direct wake (no os.write per waiter)

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
        if not _in_fiber():
            # No cooperative scheduler to wake us; degrade to spin.
            t0 = time.monotonic()
            while not self._flag:
                _raw_time_sleep(0.001)
                if timeout is not None and time.monotonic() - t0 >= timeout:
                    return self._flag
            return True
        # Goroutine waits park IN MEMORY (0 fds; the run-alive anchor + g.wake()).
        # A million waiters cost ~1 shared fd instead of ~2 each.  TIMED waits ride
        # the scheduler's timer heap (still 0 fds, no waker fiber).  (We are
        # past the foreign-thread guard above, so this is always a fiber.)
        p = _Parker(inmem=True)
        self._guard.acquire()
        if self._flag:
            # set() fired while we were building the parker.
            self._guard.release()
            p.release()
            return True
        self._waiters.append(p)
        self._guard.release()
        # set() unparks us, OR the deadline fires inside the netpoll wait -- no
        # waker fiber + heap timer per timed wait (see _Parker.park).  self._flag
        # is authoritative -- so RE-PARK on a spurious/raced wake instead of
        # trusting a single park() return.  A spurious wake (a stale pooled-parker
        # byte; a kqueue re-arm where the poll() re-check does not apply) before
        # set() would otherwise make wait() return False prematurely -- wrong for a
        # no-timeout wait, which must only ever return True.  Mirrors the
        # while-not-done loop _ThreadPoolBackend.submit already uses.
        if timeout is None:
            while not self._flag:
                p.park(None)
        else:
            deadline = time.monotonic() + timeout
            while not self._flag:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                p.park(remaining)
        # Remove our parker so a later set() never unparks this (now pooled/reused)
        # parker -> spurious wake.  Under the guard so it is serialized with set()'s
        # snapshot; a no-op (ValueError) if set() already claimed us.
        with self._guard:
            try:
                self._waiters.remove(p)
            except ValueError:
                pass
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
        # Goroutine waits (timed or untimed) park in memory (0 fds); only a wait
        # from a FOREIGN thread keeps the fd-backed park.  notify wakes both.
        p = _Parker(inmem=_in_fiber())
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
            # notify()/notify_all() unparks us, OR the deadline fires inside the
            # park (wait_fd for a fiber, select for a foreign thread) -- no
            # per-wait waker fiber + heap timer (see _Parker.park).
            p.park(timeout)
            timed_out = time.monotonic() >= deadline
        p.release()
        self._lock.acquire()
        # Remove our parker if it is STILL queued -- i.e. we resumed by TIMEOUT (or
        # a spurious wake), not by a notify that popped us.  A lingering timed-out
        # parker would otherwise steal a later notify() (notify pops the leftmost
        # waiter), leaving the real waiter unwoken.  Serialized with notify's
        # popleft / notify_all's swap by the lock we just re-acquired; a no-op
        # (ValueError) if a notify already claimed us.  (The fd path accidentally
        # masked this: the timed-out parker's POOLED fd was reused by the next
        # waiter, so waking the stale parker woke the live one through the shared
        # fd.  In-memory parkers don't share -- so the removal is load-bearing.)
        try:
            self._waiters.remove(p)
        except ValueError:
            pass
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
        woke = None
        for _ in range(n):
            if not self._waiters:
                break
            if woke is None:
                woke = []
            woke.append(self._waiters.popleft())
        if woke:
            _unpark_all(woke)   # one batched wake for the n popped waiters

    def notify_all(self):
        waiters, self._waiters = list(self._waiters), collections.deque()
        _unpark_all(waiters)   # batched direct wake (no os.write per waiter)
    notifyAll = notify_all


class CoSemaphore(object):
    # _guard: CoLock (cooperative) making _value + _waiters bookkeeping atomic.
    # Must be a cooperative lock, NOT a real OS mutex:
    #   A fiber can be preempted at any Python opcode (eval-breaker fires at
    #   every bytecode boundary in free-threaded 3.13t).  If it holds a real OS
    #   mutex when preempted, every other hub thread that tries to acquire it
    #   BLOCKS.  With all 8 hubs blocked and the holder in the submission deque,
    #   no hub is free to dispatch the holder -> deadlock.  With a CoLock, a
    #   contending fiber parks cooperatively (yields its hub thread), so hubs
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
        if not _in_fiber():
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
        # the cooperative socket path and parks the fiber).  Re-acquire +
        # re-check afterward: a permit may have appeared while we built it, or
        # cancel_all() may have fired (TOCTOU: fiber was between guard-release
        # and guard-re-acquire when cancel_all snapshotted the queue and missed it;
        # the _cancelled flag catches this and prevents a permanent park).
        self._guard.release()
        # Goroutine waits (timed or untimed) park in memory (0 fds), woken by
        # release()/cancel_all() via _unpark_all, or the timer heap on timeout.
        # (Past the foreign-thread guard above -> always a fiber.)
        p = _Parker(inmem=True)
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
        # release() unparks us (handing a permit), OR the deadline fires inside
        # the park (wait_fd for a fiber, select for a foreign thread) -- no
        # per-acquire waker fiber + heap timer (see _Parker.park).
        if timeout is None:
            # Blocking, no deadline: park may return SPURIOUSLY -- a pooled
            # _Parker can carry a stale wake byte (a foreign set()/release()
            # os.write that raced the previous user's release()-drain and landed
            # after it pooled the fds; _base.py release() notes this), so
            # wait_fd can wake before release() actually hands us a permit.
            # w[2] (set under the guard by release()) is the authoritative
            # permit flag -- loop until it is set rather than returning a
            # spurious False from a BLOCKING acquire (a contract violation that
            # also strands the permit release() handed us).
            while not w[2]:
                p.park(None)
        else:
            # Same spurious-wake hazard as the blocking branch above, plus a
            # deadline: RE-PARK on a spurious wake with the REMAINING time (re-arming
            # the full timeout each spin would let acquire() wait up to N*timeout)
            # rather than returning a premature False.  w[2] (got_permit, set under
            # the guard by release()) is authoritative.
            deadline = time.monotonic() + timeout
            while not w[2]:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    # Timed out: mark the waiter inactive under the guard so a
                    # racing release() skips us (a later release() lazily discards
                    # inactive entries) rather than handing a permit to a parker
                    # we've abandoned.  If release() set w[2] at the last moment we
                    # keep it -- the guard serializes the two.
                    with self._guard:
                        if not w[2]:
                            w[1] = False
                    break
                p.park(remaining)
        p.release()
        # w[2] is True if release() handed us a permit; False if cancelled/timed out.
        return w[2]

    def release(self, n=1):
        woke = None
        for _ in range(n):
            self._guard.acquire()
            found = False
            while self._waiters:
                w = self._waiters.popleft()
                if w[1]:    # active (not timed out)?
                    w[2] = True   # got_permit (set under guard, before unpark)
                    self._guard.release()
                    if woke is None:
                        woke = []
                    woke.append(w[0])   # batch the wake, hand-off stays per-permit
                    found = True
                    break
                # Stale timed-out waiter: discard (fiber drains its own parker)
            if not found:
                self._value += 1
                self._guard.release()
        if woke:
            _unpark_all(woke)   # one batched wake for all handed permits

    def cancel_all(self):
        """Unpark all waiting fibers WITHOUT giving them permits.
        acquire() returns False for each woken fiber.  Used by procutil to
        abort all fibers queued behind _spawn_sem when the harness stops.
        _cancelled is set FIRST so fibers that miss the waiter snapshot
        (in the window between guard-release and guard-re-acquire) bail out
        at the _cancelled check instead of parking forever.
        """
        self._guard.acquire()
        self._cancelled = True
        waiters = list(self._waiters)
        self._waiters = collections.deque()
        self._guard.release()
        woke = None
        for w in waiters:
            if w[1]:   # active (not already timed out)
                w[1] = False   # mark inactive — no permit
                if woke is None:
                    woke = []
                woke.append(w[0])
        if woke:
            _unpark_all(woke)   # batched direct wake

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
    """Cooperative Thread.join: a fiber joining a real OS thread polls
    is_alive() in a _co_sleep loop instead of parking the scheduler on the
    thread's C-level _tstate_lock (which only the dying thread can release,
    so a plain join would freeze every other fiber until it does).

    Outside a fiber, the real blocking join is used.  The not-started /
    join-self guards mirror threading.Thread.join so error semantics match."""
    if not self._initialized:
        raise RuntimeError("Thread.__init__() not called")
    if not _in_fiber():
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
    # allocate_lock() and degrades to an immediate acquire off-fiber.
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
