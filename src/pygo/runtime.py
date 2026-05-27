"""pygo runtime: scheduler + goroutine helpers.

Design choices for v0:
  - Single-thread cooperative scheduler.  N goroutines on 1 OS thread.
  - FIFO ready queue.  Sleeps live in a sorted list keyed by wake-time.
  - yield_() goes back to the scheduler immediately.
  - sleep(t) parks the goroutine and re-queues it when wake-time passes.
  - run() drains the queue + waits on sleeps until empty.

Phase 2 will add netpoll integration so a blocked socket recv parks
the goroutine on an fd instead of busy-yielding.

Phase 3 adds the M:N multi-OS-thread version (once free-threaded Python
is installed).  The Go-style transparent-blocking-via-monkey-patch
layer plugs in at that level.
"""
import heapq
import time
import pygo_core

# v0 uses a single global scheduler.  threading.local() is intentionally
# avoided here -- accessing it from inside a yielded coroutine triggers
# CPython thread-state machinery (frame chain, exception state) that
# our ucontext-based stack switch does not preserve.  The proper fix is
# per-coro thread-state snapshots in the C layer (greenlet does this in
# version-specific C); deferred to the M:N phase where it becomes
# unavoidable anyway.
sched_instance = None


def _sched():
    global sched_instance
    if sched_instance is None:
        sched_instance = Scheduler()
    return sched_instance


class Goroutine(object):
    """User-facing handle for a spawned coroutine.  Mostly opaque."""
    __slots__ = ("coro", "name", "_wake_at", "_done")

    def __init__(self, callable_, name=None, stack_size=131072):
        self.coro = pygo_core.Coro(callable_, stack_size=stack_size)
        self.name = name or getattr(callable_, "__name__", "goroutine")
        self._wake_at = 0.0
        self._done = False

    @property
    def done(self):
        return self.coro.done

    @property
    def result(self):
        return self.coro.result

    def __repr__(self):
        return "<Goroutine {0} done={1}>".format(self.name, self.done)


class Scheduler(object):
    """Single-OS-thread cooperative scheduler.

    Ready queue is a plain list used FIFO.  Sleepers live in a min-heap
    keyed by (wake_at, monotonic_seq) so equal wake times keep insertion
    order.
    """

    def __init__(self):
        pygo_core.thread_init()
        self.ready = []
        self.sleepers = []   # heap of (wake_at, seq, goroutine)
        self.seq = 0
        self.current = None

    def spawn(self, callable_, name=None, stack_size=131072):
        g = Goroutine(callable_, name=name, stack_size=stack_size)
        self.ready.append(g)
        return g

    def yield_now(self):
        # Put current back on ready queue and switch to scheduler.
        if self.current is not None:
            self.ready.append(self.current)
        pygo_core.yield_()

    def sleep(self, seconds):
        if self.current is None:
            time.sleep(seconds)
            return
        wake_at = time.monotonic() + max(0.0, seconds)
        self.seq += 1
        heapq.heappush(self.sleepers, (wake_at, self.seq, self.current))
        # Note: do NOT push self.current onto ready; sleeper queue owns
        # it now.  Yield back to scheduler.
        pygo_core.yield_()

    def run(self):
        """Drive the scheduler until every goroutine is done.

        Returns the number of goroutines completed.
        """
        completed = 0
        while self.ready or self.sleepers:
            # 1) Move any woke-up sleepers to the ready queue.
            now = time.monotonic()
            while self.sleepers and self.sleepers[0][0] <= now:
                _, _, g = heapq.heappop(self.sleepers)
                self.ready.append(g)

            # 2) If nothing ready but sleepers exist, sleep until next wake.
            if not self.ready and self.sleepers:
                next_wake = self.sleepers[0][0]
                gap = max(0.0, next_wake - time.monotonic())
                if gap > 0:
                    # No netpoll yet, so we actually block the thread.
                    # Phase 2 replaces this with epoll_wait(timeout=gap).
                    time.sleep(min(gap, 0.05))
                continue

            # 3) Resume the next ready goroutine.
            g = self.ready.pop(0)
            prev = self.current
            self.current = g
            try:
                g.coro.resume()
            finally:
                self.current = prev
            if g.coro.done:
                completed += 1
                # Drop our reference promptly so user-side handles
                # release the underlying stack.
                continue
        return completed


def go(callable_, *args, **kwargs):
    """Spawn a goroutine.  Returns a Goroutine handle.

    Mirrors Go's `go fn(a, b)`: schedules fn(*args, **kwargs) to run
    cooperatively, returns immediately.
    """
    if args or kwargs:
        target = lambda: callable_(*args, **kwargs)
        target.__name__ = getattr(callable_, "__name__", "goroutine")
    else:
        target = callable_
    return _sched().spawn(target)


def yield_():
    """Cooperative yield -- equivalent to runtime.Gosched()."""
    _sched().yield_now()


def sleep(seconds):
    """Sleep without blocking the OS thread (other goroutines run)."""
    _sched().sleep(seconds)


def current():
    """Return the currently-running Goroutine handle, or None."""
    return _sched().current


def run(main_fn=None):
    """Drive the scheduler until idle.

    If main_fn is given it's spawned first, so:
        pygo.run(my_main)
    is the moral equivalent of Go's `func main()`.  If you've already
    called pygo.go(...) yourself, pass main_fn=None to just drain.
    """
    if main_fn is not None:
        go(main_fn)
    return _sched().run()
