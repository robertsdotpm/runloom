"""runloom.sync -- plain-Python (no async/await) facade over runloom.aio.

Same surface, no coroutines.  Lets you port async code to straight-line
synchronous code without giving up the runloom scheduler.  Two styles:

  Style A: blocking-style I/O inside a fiber.
    runloom.sync.start()                # opens an implicit scheduler
    sock = runloom.sync.tcp_connect(...)
    sock.sendall(b'...')              # cooperative
    data = sock.recv(...)
    runloom.sync.go(other_worker)        # other fibers run concurrently
    runloom.sync.stop()

  Style B: drive a "main" function as the entry point.
    def main():
        sock = runloom.sync.tcp_connect(...)
        ...
    runloom.sync.run(main)               # spawns main + drains

Behind the scenes everything is still cooperative fibers.  The
sockets we return are normal Python sockets in non-blocking mode that
park cooperatively via wait_fd.

This API exists so a library (e.g. aionetiface) can ship a version
that doesn't require async/await on the call site at all.  Users get
the same throughput characteristics as the async path -- the only
thing missing is the syntactic `await`.
"""
import socket as _socket
import threading as _threading
import time as _time

import runloom_c
from runloom.runtime import prewarm_stdlib as _prewarm_stdlib


# --------------------------------------------------------------------
# Scheduler control
# --------------------------------------------------------------------
def go(callable_, *args, **kwargs):
    """Spawn a fiber.  Returns a G handle (has .done / .result /
    .wake / .stack).  Equivalent of asyncio.create_task minus the
    coroutine layer."""
    # Warm the deep, non-yielding stdlib imports getaddrinfo triggers
    # (encodings.idna -> stringprep -> unicodedata) here, on the main
    # thread's large stack -- but only when NOT already inside a
    # fiber, since prewarm itself does a getaddrinfo and would
    # overflow the small coroutine stack it is meant to protect.
    if runloom_c.current_g() is None:
        _prewarm_stdlib()
    if args or kwargs:
        target = lambda: callable_(*args, **kwargs)
    else:
        target = callable_
    return runloom_c.go(target)


def run(main_fn=None):
    """Drive the scheduler until idle.  Optionally spawn main_fn first."""
    # Same prewarm as runloom.runtime.run(): resolve getaddrinfo's lazy codec
    # import on the big stack before any fiber runs on a small one.
    _prewarm_stdlib()
    if main_fn is not None:
        runloom_c.go(main_fn)
    return runloom_c.run()


def sleep(seconds):
    """Cooperative sleep -- other fibers run while this one waits.

    Outside any fiber, falls back to time.sleep."""
    if runloom_c.current_g() is None:
        _time.sleep(seconds)
        return
    runloom_c.sched_sleep(seconds)


def yield_now():
    """Cooperative yield."""
    runloom_c.sched_yield_classic()


def current():
    """Return a G handle to the currently-running fiber, or None."""
    return runloom_c.current_g()


# --------------------------------------------------------------------
# Channels (re-export so callers don't need runloom_c directly)
# --------------------------------------------------------------------
Chan = runloom_c.Chan
select = runloom_c.select


# --------------------------------------------------------------------
# Socket: a Socket wrapper that's cooperative under the hood.
# --------------------------------------------------------------------
class Socket(object):
    """Cooperative socket wrapper.

    Same API as socket.socket for the methods we care about (recv,
    send, sendall, recvfrom, sendto, connect, accept, bind, listen,
    close, getsockname, getpeername, setsockopt, setblocking, fileno,
    shutdown).  Blocking calls park the current fiber via wait_fd
    so other fibers keep running.

    NOT thread-safe -- one Socket per fiber.  For send/recv from
    multiple fibers onto the same fd, use a channel-based fan-in.
    """

    def __init__(self, family=_socket.AF_INET, type=_socket.SOCK_STREAM,
                 proto=0, fileno=None):
        if fileno is not None:
            self._s = _socket.socket(family, type, proto, fileno=fileno)
        else:
            self._s = _socket.socket(family, type, proto)
        self._s.setblocking(False)

    @classmethod
    def _wrap(cls, s):
        out = cls.__new__(cls)
        s.setblocking(False)
        out._s = s
        return out

    # ---- forwarded methods ----
    def fileno(self):       return self._s.fileno()
    def setsockopt(self, *a): return self._s.setsockopt(*a)
    def getsockopt(self, *a): return self._s.getsockopt(*a)
    def getsockname(self):  return self._s.getsockname()
    def getpeername(self):  return self._s.getpeername()
    def bind(self, addr):   return self._s.bind(addr)
    def listen(self, n=128): return self._s.listen(n)
    def close(self):
        fd = -1
        try: fd = self._s.fileno()
        except (OSError, ValueError): pass
        if fd >= 0:
            try: runloom_c.netpoll_unregister(fd)
            except (AttributeError, OSError): pass
        try: self._s.close()
        except OSError: pass
    def shutdown(self, how):
        try: self._s.shutdown(how)
        except OSError: pass
    def setblocking(self, flag):
        # No-op: we always run non-blocking and park on wait_fd.  We
        # accept the call so existing code that toggles this works.
        pass

    # ---- cooperative I/O ----
    def connect(self, addr):
        try:
            self._s.connect(addr)
        except BlockingIOError:
            runloom_c.wait_fd(self._s.fileno(), 2)
            err = self._s.getsockopt(_socket.SOL_SOCKET, _socket.SO_ERROR)
            if err != 0:
                raise OSError(err, "connect failed")

    def accept(self):
        while True:
            try:
                conn, addr = self._s.accept()
            except (BlockingIOError, InterruptedError):
                runloom_c.wait_fd(self._s.fileno(), 1)
                continue
            return Socket._wrap(conn), addr

    def recv(self, n):
        while True:
            try:
                return self._s.recv(n)
            except (BlockingIOError, InterruptedError):
                runloom_c.wait_fd(self._s.fileno(), 1)

    def recv_into(self, buf):
        while True:
            try:
                return self._s.recv_into(buf)
            except (BlockingIOError, InterruptedError):
                runloom_c.wait_fd(self._s.fileno(), 1)

    def recvfrom(self, n):
        while True:
            try:
                return self._s.recvfrom(n)
            except (BlockingIOError, InterruptedError):
                runloom_c.wait_fd(self._s.fileno(), 1)

    def send(self, data):
        while True:
            try:
                return self._s.send(data)
            except (BlockingIOError, InterruptedError):
                runloom_c.wait_fd(self._s.fileno(), 2)

    def sendto(self, data, addr):
        while True:
            try:
                return self._s.sendto(data, addr)
            except (BlockingIOError, InterruptedError):
                runloom_c.wait_fd(self._s.fileno(), 2)

    def sendall(self, data):
        view = memoryview(data)
        sent = 0
        while sent < len(view):
            try:
                n = self._s.send(view[sent:])
                sent += n
            except (BlockingIOError, InterruptedError):
                runloom_c.wait_fd(self._s.fileno(), 2)


# --------------------------------------------------------------------
# Convenience constructors (mirror the async versions in runloom.aio)
# --------------------------------------------------------------------
def tcp_connect(host, port, *, family=0, local_addr=None):
    """Connect a TCP socket cooperatively.  Returns a runloom.sync.Socket."""
    infos = _socket.getaddrinfo(host, port,
                                family or _socket.AF_UNSPEC,
                                _socket.SOCK_STREAM)
    last_err = None
    for fam, typ, proto, _canon, sa in infos:
        s = None
        try:
            s = Socket(fam, typ, proto)
            if local_addr is not None:
                s.bind(local_addr)
            s.connect(sa)
            return s
        except OSError as e:
            last_err = e
            if s is not None:
                s.close()
    raise last_err or OSError("could not connect")


def tcp_listen(host, port, *, backlog=128, reuse_address=True):
    """Bind+listen on host:port.  Returns the listening Socket."""
    infos = _socket.getaddrinfo(host, port, 0, _socket.SOCK_STREAM,
                                0, _socket.AI_PASSIVE)
    last_err = None
    for fam, typ, proto, _canon, sa in infos:
        s = None
        try:
            s = Socket(fam, typ, proto)
            if reuse_address:
                s.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
            s.bind(sa)
            s.listen(backlog)
            return s
        except OSError as e:
            last_err = e
            if s is not None:
                s.close()
    raise last_err or OSError("could not bind")


def udp_endpoint(local_addr=None, remote_addr=None, *, family=_socket.AF_INET,
                 reuse_address=False, allow_broadcast=False):
    """Create a UDP Socket bound to local_addr (and optionally connected
    to remote_addr).  Returns a runloom.sync.Socket."""
    s = Socket(family, _socket.SOCK_DGRAM)
    if reuse_address:
        s.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
    if allow_broadcast:
        s.setsockopt(_socket.SOL_SOCKET, _socket.SO_BROADCAST, 1)
    if local_addr is not None:
        s.bind(local_addr)
    if remote_addr is not None:
        s.connect(remote_addr)
    return s


# --------------------------------------------------------------------
# Lock / Event / Semaphore -- cooperative versions of threading
# primitives.  Same as monkey.py's CoLock / CoEvent / CoSemaphore,
# re-exported here so callers don't need to monkey-patch.
# --------------------------------------------------------------------
from runloom.monkey import CoLock as Lock
from runloom.monkey import CoEvent as Event
from runloom.monkey import CoSemaphore as Semaphore
from runloom.monkey import CoCondition as Condition


# --------------------------------------------------------------------
# Park / wake -- exposed for callers building their own primitives
# --------------------------------------------------------------------
# park() is the generic M:N in-memory park: it routes by hub (park_current+yield
# on a hub, park_safe on a single thread) so it BLOCKS on an M:N hub instead of
# busy-spinning -- park_self only parks the single-thread scheduler, so a loop
# around it spins on a hub (the bug the fan-in primitives hit).  Both share the
# GenMC-verified Dekker, so a wake racing the park is never lost.  park_self stays
# reachable for single-thread-only callers.
park = runloom_c.park
park_self = runloom_c.park_self


def wake(g):
    """Wake a fiber handle returned by go()."""
    if g is not None:
        g.wake()


# --------------------------------------------------------------------
# Fan-in primitives -- WaitGroup / Future / gather.
#
# Built directly on the GenMC-verified park() / G.wake handshake (G.wake is
# runloom_sched_wake_safe; "safe to call before the park" -- the Dekker absorbs a
# wake that races the park, on both the single-thread and M:N hub paths).  park()
# (NOT park_self -- which busy-spins on a hub) means a waiter BLOCKS under M:N,
# zero fds per waiter.  A runloom_c.Mutex (a cooperative fiber mutex) guards
# the O(1) counter/slot + waiter-list bookkeeping and is ALWAYS released before
# park(), so a waker never blocks on a parked waiter.  Each is the lean equivalent
# of a Chan(1)-per-result fan-in, without a channel buffer or a select per await.
#
# RESOLUTION CONTRACT -- resolve from a fiber, not a foreign OS thread.  The
# guard's wake path (runloom_c.Mutex unlock -> mn_wake_g) is NOT foreign-OS-thread
# safe: a foreign-thread done() / set_result that contends the guard with an
# awaiting fiber would re-queue it onto a garbage hub (SIGSEGV).  So the WAKE
# side (WaitGroup.done()/add(negative); Future.set_result/set_exception) raises a
# clean RuntimeError when current_g() is None, BEFORE taking the guard -- the same
# contract as asyncio (resolve on the loop thread; cross-thread goes through
# call_soon_threadsafe).  Setup (add(positive), await) is allowed from anywhere.
# (Full foreign-thread resolution awaits the park-based foreign-safe Mutex.)
#
# Scope: a park()-parked waiter is NOT counted as keeping a SINGLE-THREAD run()
# alive (same as a channel park -- see chan_waiters.c.inc park_waiter FINDING).
# Under M:N (run(N) / mn_run, the primary mode) the hubs stay alive while a waiter
# is parked.  Because resolution is fiber-only, the resolving fiber keeps
# run() alive until it fires the wake, so single-thread run() works too.  (A
# foreign-thread-wakeable Event must park on a netpoll fd instead -- that is why
# monkey's CoEvent uses _Parker, not park().)
# --------------------------------------------------------------------
class WaitGroup(object):
    """Go-style sync.WaitGroup: wait for a set of fibers to finish.

    add(n) before spawning n fibers, done() (or add(-1)) as each finishes,
    wait() blocks until the counter returns to zero.  Multiple fibers may
    wait(); all are woken when the count hits zero.  Reusable once the count is
    back at zero."""

    def __init__(self):
        self._n = 0
        self._waiters = []          # current_g() handles parked in wait()
        self._mu = runloom_c.Mutex()

    def add(self, delta=1):
        # done()/add(negative) is the WAKE side; it must come from a fiber.
        # The guard is a runloom_c.Mutex whose wake path is not foreign-OS-thread-
        # safe, so a foreign-thread done() that wakes a parked waiter would crash.
        # Reject it BEFORE taking the guard (current_g() is a lock-free peek), so
        # there is no SIGSEGV -- only a clean error.  A positive add() never wakes
        # (the count only reaches zero via a decrement), so setup add(n) is allowed
        # from anywhere.  (Foreign-thread resolution will be supported once the
        # fan-in primitives move to a park-based foreign-safe guard.)
        if delta < 0 and runloom_c.current_g() is None:
            raise RuntimeError(
                "WaitGroup.done() / add(negative) must be called from a "
                "fiber, not a foreign OS thread")
        self._mu.lock()
        self._n += delta
        if self._n < 0:
            self._mu.unlock()
            raise ValueError("WaitGroup counter went negative")
        if self._n == 0 and self._waiters:
            waiters, self._waiters = self._waiters, []
            self._mu.unlock()
            for g in waiters:
                g.wake()
        else:
            self._mu.unlock()

    def done(self):
        self.add(-1)

    def wait(self):
        g = runloom_c.current_g()
        if g is None:
            # Foreign OS thread: no fiber to park, poll the counter.
            while True:
                self._mu.lock()
                n = self._n
                self._mu.unlock()
                if n == 0:
                    return
                _time.sleep(0.0005)
        while True:
            self._mu.lock()
            if self._n == 0:
                self._mu.unlock()
                return
            self._waiters.append(g)
            self._mu.unlock()
            runloom_c.park()            # M:N-correct in-memory park (park_self
                                        # busy-loops on a hub).  Woken by the add()
                                        # that reaches zero; the wake-before-park
                                        # race in the [unlock .. park] window is
                                        # consumed by park()'s Dekker, not lost.
            # loop re-checks the count under the lock.


class Future(object):
    """A one-shot result/exception slot any number of fibers can await.

    set_result()/set_exception() resolve it once and wake every current awaiter;
    a later result() on a resolved Future returns (or raises) immediately.  The
    lean fan-in primitive a gather / as_completed builds on -- no Chan buffer."""

    def __init__(self):
        self._done = False
        self._result = None
        self._exc = None
        self._waiters = []
        self._mu = runloom_c.Mutex()

    def done(self):
        self._mu.lock()
        d = self._done
        self._mu.unlock()
        return d

    def _resolve(self, result, exc):
        # Resolving wakes parked awaiters; the guard is a runloom_c.Mutex whose
        # wake path is not foreign-OS-thread-safe, so a foreign-thread resolve that
        # contends the guard with an awaiter would crash.  Reject it BEFORE taking
        # the guard (current_g() is a lock-free peek) -- a clean error, no SIGSEGV.
        # This matches asyncio (resolve from the loop thread; cross-thread goes
        # through call_soon_threadsafe).  Full foreign-thread support arrives with
        # the park-based foreign-safe guard.
        if runloom_c.current_g() is None:
            raise RuntimeError(
                "Future must be resolved from a fiber, not a foreign OS thread")
        self._mu.lock()
        if self._done:
            self._mu.unlock()
            raise RuntimeError("Future already resolved")
        self._done = True
        self._result = result
        self._exc = exc
        waiters, self._waiters = self._waiters, []
        self._mu.unlock()
        for g in waiters:
            g.wake()

    def set_result(self, value):
        self._resolve(value, None)

    def set_exception(self, exc):
        if isinstance(exc, type):
            exc = exc()
        self._resolve(None, exc)

    def result(self, timeout=None):
        # timeout (seconds, optional): raise TimeoutError if unresolved by the
        # deadline.  A fiber parks via park(timeout=) (0 fds); a foreign thread
        # polls.  Re-checks _done on every (possibly spurious) wake -- the flag is
        # authoritative, never a single park() return.
        g = runloom_c.current_g()
        deadline = None if timeout is None else _time.monotonic() + timeout
        self._mu.lock()
        if self._done:
            exc, res = self._exc, self._result
            self._mu.unlock()
            if exc is not None:
                raise exc
            return res
        if g is None:
            # Foreign OS thread: poll until resolved (cannot park).
            self._mu.unlock()
            while True:
                self._mu.lock()
                if self._done:
                    exc, res = self._exc, self._result
                    self._mu.unlock()
                    if exc is not None:
                        raise exc
                    return res
                self._mu.unlock()
                if deadline is not None and _time.monotonic() >= deadline:
                    raise TimeoutError("Future.result timed out")
                _time.sleep(0.0005)
        # Append our handle ONCE (not per-iteration): a spurious park() return
        # leaves us in _waiters, so re-parking needs no re-append -- re-appending
        # would queue a DUPLICATE that _resolve()'s wake-all then wakes repeatedly.
        self._waiters.append(g)
        self._mu.unlock()
        while True:
            if deadline is None:
                runloom_c.park()        # woken by _resolve() (wake-all + clear)
            else:
                remaining = deadline - _time.monotonic()
                if remaining <= 0:
                    self._mu.lock()
                    try:
                        self._waiters.remove(g)   # de-queue on timeout
                    except ValueError:
                        pass
                    if self._done:                # resolved at the last moment
                        exc, res = self._exc, self._result
                        self._mu.unlock()
                        if exc is not None:
                            raise exc
                        return res
                    self._mu.unlock()
                    raise TimeoutError("Future.result timed out")
                runloom_c.park(timeout=remaining)
            self._mu.lock()
            if self._done:
                exc, res = self._exc, self._result
                self._mu.unlock()
                if exc is not None:
                    raise exc
                return res
            self._mu.unlock()
            # spurious wake -> still queued in _waiters -> re-park


def gather(*callables):
    """Run `callables` as fibers concurrently and block until all finish,
    returning their results in argument order.  If any raises, the first
    exception (by argument order) is re-raised after all have completed.  Each
    runner writes its own result slot (one writer per slot), so there is no
    shared-counter race even with the GIL off."""
    n = len(callables)
    if n == 0:
        return []
    results = [None] * n
    errs = [None] * n
    wg = WaitGroup()
    wg.add(n)

    def _runner(i, fn):
        try:
            results[i] = fn()
        except BaseException as e:   # noqa: BLE001  (propagated below)
            errs[i] = e
        finally:
            wg.done()

    # Spawn on whichever scheduler is live: mn_go under M:N (run/mn_run), else
    # the single-thread go.  A runner spawned via runloom_c.go never runs under
    # mn_run, so wg.wait() would hang -- same routing as monkey's _spawn helper.
    mn = runloom_c.mn_hub_count() > 0
    for i, fn in enumerate(callables):
        target = (lambda i=i, fn=fn: _runner(i, fn))
        if mn:
            runloom_c.mn_go(target)
        else:
            runloom_c.go(target)
    wg.wait()
    for e in errs:
        if e is not None:
            raise e
    return results


# ====================================================================
# Phase 3 primitives -- all on the GenMC-verified park() / g.wake() handshake +
# the runloom_c.Mutex guard, following the same rules as WaitGroup / Future:
#   * guarded state under self._mu, held ONLY for O(1) bookkeeping;
#   * a GOROUTINE waiter parks via park() (or park(timeout=) for a timed wait) and
#     RE-CHECKS its condition under the guard on each wake -- park() may return
#     spuriously, so the condition (never a single park() return) is authoritative;
#   * the guard is ALWAYS released BEFORE g.wake() (the Mutex wake path is not
#     foreign-OS-thread-safe -- wake while holding it can SIGSEGV);
#   * a timed waiter REMOVES itself from the waiter list on timeout, else a stale
#     parker steals a later wake.
# Foreign-OS-thread rule: locks (RWMutex, Semaphore) are GOROUTINE-ONLY (a foreign
# thread raises -- use threading.Lock/Semaphore under monkey for foreign-safe
# locking).  Resolution primitives (Once/Watch/Future/singleflight) raise on the
# WAKE/resolve side from a foreign thread (Future.set_result style) and let a
# foreign WAITER poll.
# ====================================================================

def _resolve_from_fiber(what):
    """Raise (before any guard) if a WAKE/resolve op runs on a foreign OS thread --
    the guard's wake path (mn_wake_g) is not foreign-thread-safe."""
    if runloom_c.current_g() is None:
        raise RuntimeError(
            "%s must be called from a fiber, not a foreign OS thread" % what)


class RWMutex(object):
    """Go-style sync.RWMutex: many readers OR one writer, WRITER-PREFERENCE (a
    pending writer blocks NEW readers so writers are not starved by a reader
    stream).  Goroutine-only; NOT reentrant.  Use as a context manager for the
    write lock (`with rw:`), or `with rw.rlocked():` for the read lock."""

    # HANDOFF design: a waiter appends a [g, [granted]] cell ONCE, parks, and loops
    # on `granted` (re-parking on a spurious wake -- it does NOT re-append, which
    # would create a duplicate that unlock's pop-one could waste, stranding a real
    # waiter).  unlock/runlock TRANSFER the lock directly: they set the next
    # waiter's granted flag (and the reader/writer counters) under the guard, then
    # wake -- so the lock is never "released and raced", and a fresh acquirer that
    # arrives mid-handoff sees the lock held and queues behind.
    __slots__ = ("_readers", "_writer", "_rwait", "_wwait", "_mu")

    def __init__(self):
        self._readers = 0        # active read-lock holders
        self._writer  = False    # a write lock is held
        self._rwait   = []       # parked reader cells [g, [granted]]
        self._wwait   = []       # parked writer cells [g, [granted]] (FIFO)
        self._mu = runloom_c.Mutex()

    def rlock(self):
        _resolve_from_fiber("RWMutex.rlock()")
        g = runloom_c.current_g()
        self._mu.lock()
        if not self._writer and not self._wwait:    # writer-preference: a waiting
            self._readers += 1                       # writer blocks new readers
            self._mu.unlock()
            return
        cell = [g, [False]]
        self._rwait.append(cell)
        self._mu.unlock()
        while True:
            runloom_c.park()
            self._mu.lock()
            granted = cell[1][0]
            self._mu.unlock()
            if granted:                              # _readers already bumped for us
                return

    def runlock(self):
        _resolve_from_fiber("RWMutex.runlock()")
        self._mu.lock()
        if self._readers <= 0:
            self._mu.unlock()
            raise RuntimeError("RWMutex.runlock(): read lock not held")
        self._readers -= 1
        if self._readers == 0 and self._wwait:      # last reader hands off to a writer
            cell = self._wwait.pop(0)
            self._writer = True
            cell[1][0] = True
            self._mu.unlock()
            cell[0].wake()
        else:
            self._mu.unlock()

    def lock(self):
        _resolve_from_fiber("RWMutex.lock()")
        g = runloom_c.current_g()
        self._mu.lock()
        if not self._writer and self._readers == 0:
            self._writer = True
            self._mu.unlock()
            return
        cell = [g, [False]]
        self._wwait.append(cell)
        self._mu.unlock()
        while True:
            runloom_c.park()
            self._mu.lock()
            granted = cell[1][0]
            self._mu.unlock()
            if granted:                              # _writer already True for us
                return

    def unlock(self):
        _resolve_from_fiber("RWMutex.unlock()")
        self._mu.lock()
        if not self._writer:
            self._mu.unlock()
            raise RuntimeError("RWMutex.unlock(): write lock not held")
        if self._wwait:                              # WRITER-PREFERENCE: a writer first
            cell = self._wwait.pop(0)                #   keep _writer True (handoff)
            cell[1][0] = True
            self._mu.unlock()
            cell[0].wake()
        elif self._rwait:                            # else hand off to ALL readers
            self._writer = False
            readers, self._rwait = self._rwait, []
            self._readers = len(readers)
            for cell in readers:
                cell[1][0] = True
            self._mu.unlock()
            for cell in readers:
                cell[0].wake()
        else:
            self._writer = False
            self._mu.unlock()

    def __enter__(self):
        self.lock(); return self
    def __exit__(self, *a):
        self.unlock()

    def rlocked(self):
        """Context manager for the READ lock: `with rw.rlocked(): ...`."""
        return _RWReadCtx(self)


class _RWReadCtx(object):
    __slots__ = ("_rw",)
    def __init__(self, rw): self._rw = rw
    def __enter__(self): self._rw.rlock(); return self._rw
    def __exit__(self, *a): self._rw.runlock()


class Semaphore(object):
    """Weighted semaphore (golang.org/x/sync/semaphore.Weighted): acquire(n) blocks
    until n permits are free, FIFO so a large-n waiter is never starved by a stream
    of small-n acquirers.  Goroutine-only.  Distinct from monkey's counting
    threading.Semaphore (one permit per waiter)."""

    __slots__ = ("_limit", "_held", "_waiters", "_mu")

    def __init__(self, value):
        if value < 0:
            raise ValueError("Semaphore value must be >= 0")
        self._limit   = value
        self._held    = 0        # permits currently held
        self._waiters = []       # FIFO: each [g, n, [granted_bool]]
        self._mu = runloom_c.Mutex()

    def acquire(self, n=1, timeout=None):
        if n < 0:
            raise ValueError("Semaphore.acquire(n): n must be >= 0")
        if n > self._limit:
            raise ValueError("Semaphore.acquire(%d) exceeds limit %d" % (n, self._limit))
        _resolve_from_fiber("Semaphore.acquire()")
        deadline = None if timeout is None else _time.monotonic() + timeout
        g = runloom_c.current_g()
        self._mu.lock()
        # Fast path: permits free AND nobody ahead (FIFO -- never jump the queue).
        if not self._waiters and self._held + n <= self._limit:
            self._held += n
            self._mu.unlock()
            return True
        w = [g, n, [False]]                          # granted flag set by release()
        self._waiters.append(w)
        self._mu.unlock()
        while True:
            if deadline is None:
                runloom_c.park()
            else:
                remaining = deadline - _time.monotonic()
                if remaining <= 0:
                    self._mu.lock()
                    if w[2][0]:                      # granted at the last moment
                        self._mu.unlock()
                        return True
                    try:
                        self._waiters.remove(w)
                    except ValueError:
                        pass
                    self._mu.unlock()
                    return False
                runloom_c.park(timeout=remaining)
            self._mu.lock()
            granted = w[2][0]
            self._mu.unlock()
            if granted:
                return True
            # spurious / not yet our turn -> loop and re-park

    def try_acquire(self, n=1):
        if n < 0:
            raise ValueError("n must be >= 0")
        _resolve_from_fiber("Semaphore.try_acquire()")   # fiber-only contract
        self._mu.lock()
        if not self._waiters and self._held + n <= self._limit:
            self._held += n
            self._mu.unlock()
            return True
        self._mu.unlock()
        return False

    def release(self, n=1):
        if n < 0:
            raise ValueError("Semaphore.release(n): n must be >= 0")
        _resolve_from_fiber("Semaphore.release()")
        self._mu.lock()
        self._held -= n
        if self._held < 0:
            self._held = 0
            self._mu.unlock()
            raise ValueError("Semaphore.release: released more than held")
        # Grant FIFO while the FRONT waiter fits (a too-big front waiter blocks the
        # rest -> no starvation; granted+held set under the guard BEFORE the wake).
        woke = None
        while self._waiters:
            w = self._waiters[0]
            if self._held + w[1] <= self._limit:
                self._held += w[1]
                w[2][0] = True
                self._waiters.pop(0)
                if woke is None:
                    woke = []
                woke.append(w[0])
            else:
                break
        self._mu.unlock()
        if woke:
            for g in woke:
                g.wake()

    def __enter__(self):
        self.acquire(); return self
    def __exit__(self, *a):
        self.release()


class Once(object):
    """Go sync.Once: do(fn) runs fn EXACTLY ONCE; concurrent callers block until it
    finishes, then return without re-running; after completion every call is a
    no-op.  The FIRST executor sees fn's exception; later callers do NOT (Go
    semantics -- use once_value to cache+re-raise it).  A panicking fn still
    completes the Once.  A foreign OS thread may WAIT (poll) but may not be the
    first executor (it would wake parked fibers)."""

    __slots__ = ("_done", "_running", "_waiters", "_mu")

    def __init__(self):
        self._done    = False
        self._running = False
        self._waiters = []
        self._mu = runloom_c.Mutex()

    def done(self):
        self._mu.lock()
        d = self._done
        self._mu.unlock()
        return d

    def do(self, fn):
        g = runloom_c.current_g()
        self._mu.lock()
        if self._done:
            self._mu.unlock()
            return
        if not self._running:
            # We are the executor.
            if g is None:
                self._mu.unlock()
                raise RuntimeError(
                    "Once.do() first call must be from a fiber, not a foreign "
                    "OS thread")
            self._running = True
            self._mu.unlock()
            try:
                fn()
            finally:
                # Mark done + wake waiters even if fn raised (Go: a panic still
                # completes the Once).  Snapshot+clear under the guard, wake AFTER.
                self._mu.lock()
                self._done = True
                self._running = False
                waiters, self._waiters = self._waiters, []
                self._mu.unlock()
                for w in waiters:
                    w.wake()
            return
        # A waiter: block until the executor finishes.  Guard is held here.
        if g is None:
            self._mu.unlock()
            while True:
                _time.sleep(0.0005)
                self._mu.lock()
                d = self._done
                self._mu.unlock()
                if d:
                    return
        while True:
            self._waiters.append(g)
            self._mu.unlock()
            runloom_c.park()
            self._mu.lock()
            if self._done:
                self._mu.unlock()
                return
            # spurious wake -> re-append + re-park


def once_value(fn):
    """Return a 0-arg callable that runs fn() once and returns its result on EVERY
    call (Go 1.21 sync.OnceValue).  Caches the result OR the exception and re-raises
    it to ALL callers (unlike Once.do, which shows the exception only to the
    executor)."""
    once = Once()
    box = {}

    def caller():
        def run():
            try:
                box["v"] = fn()
            except BaseException as e:   # noqa: BLE001  (cached + re-raised below)
                box["e"] = e
        once.do(run)
        if "e" in box:
            raise box["e"]
        return box.get("v")
    return caller


def once_func(fn):
    """Return a 0-arg callable that runs fn() exactly once (Go 1.21 sync.OnceFunc),
    caching + re-raising any exception to all callers."""
    inner = once_value(fn)

    def caller():
        inner()
    return caller


class Group(object):
    """singleflight.Group (golang.org/x/sync/singleflight): do(key, fn) dedupes
    concurrent calls with the same key -- the first caller runs fn; concurrent
    callers with the same key WAIT and SHARE its result/exception.  Returns
    (value, shared: bool).  The first caller for a key must be a fiber (it
    resolves the shared Future)."""

    __slots__ = ("_calls", "_mu")

    def __init__(self):
        self._calls = {}        # key -> Future (in-flight)
        self._mu = runloom_c.Mutex()

    def do(self, key, fn):
        g = runloom_c.current_g()
        self._mu.lock()
        fut = self._calls.get(key)
        if fut is not None:
            # Waiter: share the in-flight result (Future.result blocks + re-raises).
            self._mu.unlock()
            return (fut.result(), True)
        if g is None:
            self._mu.unlock()
            raise RuntimeError(
                "singleflight.Group.do() first call for a key must be from a "
                "fiber, not a foreign OS thread")
        fut = Future()
        self._calls[key] = fut
        self._mu.unlock()
        try:
            v = fn()
        except BaseException as e:   # noqa: BLE001  (shared to all waiters + re-raised)
            # Delete the entry BEFORE resolving, so a freshly-arriving do(key) after
            # this starts a NEW call instead of joining the finished one.
            self._mu.lock()
            if self._calls.get(key) is fut:
                del self._calls[key]
            self._mu.unlock()
            fut.set_exception(e)
            raise
        self._mu.lock()
        if self._calls.get(key) is fut:
            del self._calls[key]
        self._mu.unlock()
        fut.set_result(v)
        return (v, False)

    def forget(self, key):
        """Drop any in-flight call for key, so the next do(key) starts fresh."""
        self._mu.lock()
        self._calls.pop(key, None)
        self._mu.unlock()


class Watch(object):
    """tokio::sync::watch: a single latest-value cell many observers watch for
    CHANGES.  set(v) updates the value + a version counter + wakes ALL waiters
    (fiber-only).  get()/version() read; wait_changed(seen, timeout=None) blocks
    until version > seen and returns (value, version), or None on timeout."""

    __slots__ = ("_value", "_version", "_waiters", "_mu")

    def __init__(self, value=None):
        self._value   = value
        self._version = 0
        self._waiters = []
        self._mu = runloom_c.Mutex()

    def get(self):
        self._mu.lock()
        v = self._value
        self._mu.unlock()
        return v

    def version(self):
        self._mu.lock()
        ver = self._version
        self._mu.unlock()
        return ver

    def get_versioned(self):
        self._mu.lock()
        r = (self._value, self._version)
        self._mu.unlock()
        return r

    def set(self, value):
        _resolve_from_fiber("Watch.set()")
        self._mu.lock()
        self._value = value
        self._version += 1                  # increment UNDER the guard (no lost wake)
        waiters, self._waiters = self._waiters, []
        self._mu.unlock()
        for w in waiters:                   # broadcast: wake every current observer
            w.wake()

    def wait_changed(self, seen_version, timeout=None):
        g = runloom_c.current_g()
        deadline = None if timeout is None else _time.monotonic() + timeout
        if g is None:
            while True:
                self._mu.lock()
                if self._version > seen_version:
                    r = (self._value, self._version)
                    self._mu.unlock()
                    return r
                self._mu.unlock()
                if deadline is not None and _time.monotonic() >= deadline:
                    return None
                _time.sleep(0.0005)
        self._mu.lock()
        if self._version > seen_version:
            r = (self._value, self._version)
            self._mu.unlock()
            return r
        self._waiters.append(g)          # ONCE: a spurious wake leaves us queued,
        self._mu.unlock()                # so re-parking needs no re-append (a
        while True:                      # duplicate would be re-woken by set()).
            if deadline is None:
                runloom_c.park()
            else:
                remaining = deadline - _time.monotonic()
                if remaining <= 0:
                    self._mu.lock()
                    try:
                        self._waiters.remove(g)
                    except ValueError:
                        pass
                    if self._version > seen_version:   # changed at the last moment
                        r = (self._value, self._version)
                        self._mu.unlock()
                        return r
                    self._mu.unlock()
                    return None
                runloom_c.park(timeout=remaining)
            self._mu.lock()
            if self._version > seen_version:
                try:
                    self._waiters.remove(g)            # done -> de-queue ourselves
                except ValueError:
                    pass
                r = (self._value, self._version)
                self._mu.unlock()
                return r
            self._mu.unlock()
            # spurious wake -> still queued -> re-park


class JoinSet(object):
    """Structured concurrency (Tokio JoinSet / trio nursery): spawn(fn, *a, **kw)
    runs fn as a fiber tracked by the set; join_all() waits for ALL, returns
    results in SPAWN order, and (after all finish) raises the FIRST exception by
    spawn order -- like gather.  Single-use; spawn from ONE place before join_all().
    Also a context manager: `with JoinSet() as js: js.spawn(...)` joins on exit."""

    __slots__ = ("_wg", "_results", "_errs", "_n")

    def __init__(self):
        self._wg      = WaitGroup()
        self._results = []
        self._errs    = []
        self._n       = 0

    def spawn(self, fn, *args, **kwargs):
        """Spawn fn as a tracked fiber.  Returns its index (spawn order).
        Each runner writes only its OWN result/err slot -> no shared-counter race
        even with the GIL off."""
        i = self._n
        self._n += 1
        self._results.append(None)
        self._errs.append(None)
        self._wg.add(1)

        def runner():
            try:
                self._results[i] = fn(*args, **kwargs)
            except BaseException as e:   # noqa: BLE001  (propagated by join_all)
                self._errs[i] = e
            finally:
                self._wg.done()

        if runloom_c.mn_hub_count() > 0:
            runloom_c.mn_go(runner)
        else:
            runloom_c.go(runner)
        return i

    def join_all(self):
        """Wait for every spawned task, then return results in spawn order, or
        raise the first exception (by spawn order)."""
        self._wg.wait()
        for e in self._errs:
            if e is not None:
                raise e
        return list(self._results)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        # Structured concurrency: always wait for the spawned tasks on exit.  If the
        # body raised, let THAT propagate (don't mask it); else propagate the first
        # task error.
        self._wg.wait()
        if exc_type is None:
            for e in self._errs:
                if e is not None:
                    raise e
        return False                     # never suppress a body exception
