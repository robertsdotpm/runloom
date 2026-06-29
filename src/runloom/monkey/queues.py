"""Cooperative queue.SimpleQueue (queue.Queue rides on Condition)."""
from ._base import *  # noqa: F401,F403  (shared foundation)

# ============================================================
# queue  -- queue.Queue picks up CoLock/CoCondition at __init__ for free;
# queue.SimpleQueue is a C type (_queue.SimpleQueue) whose .get(block=True)
# parks on a C lock the scheduler can't wake, so it needs a cooperative
# replacement.  SimpleQueue is used by logging.handlers.QueueHandler /
# QueueListener and by ThreadPoolExecutor's work queue, so fiber code
# bumps into it more than you'd expect.
# ============================================================
import queue as _queue_mod

_real_SimpleQueue = _queue_mod.SimpleQueue

# Distinct "deque was empty" sentinel: None is a legal payload (queue.SimpleQueue
# users put None as a poison sentinel to drain a consumer), so a popleft inside a
# loop must distinguish "got None" from "was empty" without a second len() read
# (which would race the pop under M:N).
_EMPTY = object()


def _spawn(fn):
    """Spawn a helper fiber on whichever scheduler is active: mn_fiber under
    M:N (mn_hub_count() > 0), else the single-thread go.  A waker spawned via
    the single-thread fiber() never runs under mn_run, so a timed get() would hang
    until something is put."""
    if runloom_c.mn_hub_count() > 0:
        return runloom_c.mn_fiber(fn)
    return runloom_c.fiber(fn)


class CoSimpleQueue(object):
    """Cooperative, unbounded FIFO matching queue.SimpleQueue's surface.

    put() never blocks (SimpleQueue is unbounded), so the block/timeout
    args are accepted and ignored exactly as the C type does.  get() parks
    the fiber on a parker when empty; a producer appending an item wakes the
    longest-waiting getter, which then re-pops the freshly enqueued item.

    THE BUG THIS FIXES.  The real C queue.SimpleQueue serialises its put/get
    internally; this cooperative replacement relied on GIL atomicity that
    free-threading (PEP 703) removed.  The old get() did an UNGUARDED
    check-then-pop ``if self._items: return self._items.popleft()`` and put() did
    ``self._waiters.popleft().unpark()`` -- so under M:N two consumers on
    different hubs both passed the empty check then both popleft'd a one-element
    _items (IndexError underflow / a duplicated delivery), and a producer's wake
    raced a timed waiter's self-``_waiters.remove`` (a LOST item, or a wake spent
    on nobody).  Verified plain-threads GIL-off 126/200, GIL-on 0/200.

    WHY THIS DESIGN IS LOCK-FREE (no guard).  ``collections.deque`` is internally
    atomic on the free-threaded build (PEP 703 per-object critical section):
    concurrent ``append``/``popleft``/``remove`` never corrupt and never
    double-deliver -- a losing ``popleft`` on an empty deque cleanly raises
    ``IndexError`` (verified: 8 threads, GIL off, 800k items, 0 dup / 0 lost / 0
    crash).  A cooperative LOCK was tried and rejected twice: a blocking CoLock
    guard convoys (every loser fiber parks per op -> ~3x slower, walls the p428
    sweep), and a SPIN guard (try_lock + sched_yield) LIVELOCKS at scale -- a
    fiber preempted (eval-breaker) mid-section while every other hub spins
    sched_yield'ing for it can never be rescheduled, so 2000 workers wedge with
    ops=0.  The atomic deque needs NO guard for the data; only the park/wake
    rendezvous needs care, and a re-check closes it lock-free:

      * get(): pop lock-free; on empty, APPEND a waiter record, then RE-POP
        _items.  If the re-pop wins, deactivate the record and return -- an item
        that arrived between the first pop and the append is taken here, not
        missed.  Otherwise park.  On wake (or a spurious return) re-pop again; the
        item lives durably in _items, so a wake-to-re-poll is robust to spurious /
        duplicate / early wakes (no item is ever "handed" to a parker, so None --
        the poison sentinel -- needs no special case).
      * put(): APPEND the item lock-free (published before any wake), then wake one
        live waiter so a parked getter re-pops it.  A producer that appends just
        before a getter registers is covered by the getter's post-append re-pop; a
        producer that appends just after is covered by waking the registered
        getter.  No lost wakeup either way.

    Each waiter record is [parker, active]: active is set False (a plain store,
    no lock -- a racy read only ever costs a harmless spurious wake) when the
    getter stops waiting (got an item, or timed out), so a racing put() skips it
    (wake_one pops and discards inactive records) and the getter self-removes its
    record so none linger at quiesce."""
    __slots__ = ("_items", "_waiters")

    def __init__(self):
        self._items   = collections.deque()
        self._waiters = collections.deque()

    def wake_one(self):
        """Pop and return the parker of one LIVE waiter (a getter parked because
        _items was empty); discard inactive (timed-out / already-served) records
        off the head.  Returns the parker to unpark, or None.  deque.popleft is
        atomic, so this races other wake_one / a getter's self-remove safely."""
        while True:
            try:
                rec = self._waiters.popleft()
            except IndexError:
                return None
            if rec[1]:                       # active (not timed out / served)
                return rec[0]
            # else inactive -> discard, keep scanning

    def put(self, item, block=True, timeout=None):
        # Append lock-free -- deque.append is atomic on the free-threaded build.
        # Publish the item BEFORE waking so a woken getter's re-pop always finds it.
        self._items.append(item)
        # Wake one parked getter if any.  The unlocked truthiness read is a cheap
        # fast-out; a stale-empty read (a getter mid-register) is harmless because
        # that getter re-pops _items right after registering and takes this item
        # itself -- it never parks with an item already waiting.
        if self._waiters:
            woke = self.wake_one()
            if woke is not None:
                # Wake with the getter's fd parker (os.write).  The byte is DURABLE:
                # a wake that beats the getter's park commit latches in the pipe and
                # its next wait_fd drains it -- never lost.  os.write is foreign-safe
                # too, so a producer on a foreign OS thread hands off correctly.
                woke.unpark()

    def put_nowait(self, item):
        self.put(item, False)

    def get(self, block=True, timeout=None):
        # Fast path: lock-free pop (deque.popleft is atomic; a losing pop on an
        # empty deque raises IndexError -- no torn read, no double-delivery).
        try:
            return self._items.popleft()
        except IndexError:
            pass
        if not block:
            raise _queue_mod.Empty
        if not _in_fiber():
            # No fiber to park; spin + yield to the OS so a producer on a real
            # thread can fill us.  Bare popleft each spin (atomic, no guard).
            t0 = time.monotonic()
            while True:
                try:
                    return self._items.popleft()
                except IndexError:
                    pass
                if timeout is not None and time.monotonic() - t0 >= timeout:
                    raise _queue_mod.Empty
                _raw_time_sleep(0.0001)
        # Must park.  Use a FD-backed _Parker (NOT the 0-fd inmem one): a producer
        # wakes it with a single direct unpark() (os.write), and only the fd
        # parker's pipe byte is DURABLE across the wake-before-park window for this
        # direct single-waiter wake (the inmem g.wake() path is wake-safe only when
        # driven by _unpark_all -- see _base.py _Parker).  _Parker() can YIELD (the
        # Windows socketpair handshake runs through the cooperative socket path),
        # which is fine here -- there is no lock held across it.
        p = _Parker()
        rec = [p, True]                      # [parker, active]
        # Register, THEN re-pop: an item that arrived between the fast-path pop and
        # this append is taken right here, closing the lost-wakeup gap lock-free
        # (a producer that appended that item either already saw an empty _waiters
        # -- so we take it via this re-pop -- or sees our record and wakes us).
        self._waiters.append(rec)
        try:
            item = self._items.popleft()
            rec[1] = False                   # we are done waiting -> deactivate
            try:
                self._waiters.remove(rec)
            except ValueError:
                pass                         # a put() already popped us (it will
                                             # spuriously unpark; harmless -- the
                                             # parker is released below)
            p.release()
            return item
        except IndexError:
            pass
        # park may return SPURIOUSLY (a pooled parker can carry a stale wake byte;
        # wait_fd can wake early), so the deque -- not a single park return -- is
        # authoritative: each turn re-pop; on success deactivate + self-remove and
        # return; else re-park with the REMAINING time so a spurious wake never
        # extends the deadline past `timeout`.
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            try:
                item = self._items.popleft()
            except IndexError:
                item = _EMPTY
            if item is not _EMPTY:
                rec[1] = False               # got it -> stop being a waiter
                try:
                    self._waiters.remove(rec)
                except ValueError:
                    pass                     # already popped by a put() wake_one()
                p.release()
                return item
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    # Timed out: deactivate (so a racing put() skips us) and
                    # self-remove (so we do not linger) -- both lock-free; a put()
                    # racing the remove either popped us already (ValueError, fine)
                    # or skips our now-inactive record.
                    rec[1] = False
                    try:
                        self._waiters.remove(rec)
                    except ValueError:
                        pass
                    p.release()
                    raise _queue_mod.Empty
            else:
                remaining = None
            p.park(remaining)

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
