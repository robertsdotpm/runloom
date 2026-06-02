"""Cooperative queue.SimpleQueue (queue.Queue rides on Condition)."""
from ._base import *  # noqa: F401,F403  (shared foundation)

# ============================================================
# queue  -- queue.Queue picks up CoLock/CoCondition at __init__ for free;
# queue.SimpleQueue is a C type (_queue.SimpleQueue) whose .get(block=True)
# parks on a C lock the scheduler can't wake, so it needs a cooperative
# replacement.  SimpleQueue is used by logging.handlers.QueueHandler /
# QueueListener and by ThreadPoolExecutor's work queue, so goroutine code
# bumps into it more than you'd expect.
# ============================================================
import queue as _queue_mod

_real_SimpleQueue = _queue_mod.SimpleQueue


class CoSimpleQueue(object):
    """Cooperative, unbounded FIFO matching queue.SimpleQueue's surface.

    put() never blocks (SimpleQueue is unbounded), so the block/timeout
    args are accepted and ignored exactly as the C type does.  get() parks
    the goroutine on a parker when empty; a producer hands the next item to
    the longest-waiting getter."""
    __slots__ = ("_items", "_waiters")

    def __init__(self):
        self._items   = collections.deque()
        self._waiters = collections.deque()

    def put(self, item, block=True, timeout=None):
        self._items.append(item)
        while self._waiters:
            # Wake one waiter; if it already timed out (released its parker)
            # the unpark is harmless, so loop until we hand off to a live one
            # or run out -- but a single live waiter is the normal case.
            self._waiters.popleft().unpark()
            break

    def put_nowait(self, item):
        self.put(item, False)

    def get(self, block=True, timeout=None):
        if self._items:
            return self._items.popleft()
        if not block:
            raise _queue_mod.Empty
        if not _in_goroutine():
            # No goroutine to park; spin + yield to the OS so a producer on
            # a real thread can fill us.
            t0 = time.monotonic()
            while not self._items:
                _raw_time_sleep(0.0001)
                if timeout is not None and time.monotonic() - t0 >= timeout:
                    raise _queue_mod.Empty
            return self._items.popleft()
        p = _Parker()
        # _Parker() may yield (Windows socketpair path); re-check first.
        if self._items:
            p.release()
            return self._items.popleft()
        self._waiters.append(p)
        if timeout is None:
            p.park()
        else:
            deadline = time.monotonic() + timeout
            done = [False]
            def waker(parker=p, dl=deadline):
                while not done[0]:
                    remaining = dl - time.monotonic()
                    if remaining <= 0:
                        parker.unpark()
                        return
                    _co_sleep(min(remaining, 0.05))
            pygo_core.go(waker)
            p.park()
            done[0] = True
        p.release()
        if self._items:
            return self._items.popleft()
        # Woken with nothing to take -> timed out.
        try:
            self._waiters.remove(p)
        except ValueError:
            pass
        raise _queue_mod.Empty

    def get_nowait(self):
        return self.get(False)

    def empty(self):
        return not self._items

    def qsize(self):
        return len(self._items)


def _patch_queue():
    # Queue uses threading.Lock()/Condition() at instantiation, so once
    # threading is patched every new Queue is already cooperative.  Only
    # SimpleQueue (C type) needs swapping out.
    _queue_mod.SimpleQueue = CoSimpleQueue

def _unpatch_queue():
    _queue_mod.SimpleQueue = _real_SimpleQueue
