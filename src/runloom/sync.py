"""runloom.sync -- plain-Python (no async/await) facade over runloom.aio.

Same surface, no coroutines.  Lets you port async code to straight-line
synchronous code without giving up the runloom scheduler.  Two styles:

  Style A: blocking-style I/O inside a goroutine.
    runloom.sync.start()                # opens an implicit scheduler
    sock = runloom.sync.tcp_connect(...)
    sock.sendall(b'...')              # cooperative
    data = sock.recv(...)
    runloom.sync.go(other_worker)        # other goroutines run concurrently
    runloom.sync.stop()

  Style B: drive a "main" function as the entry point.
    def main():
        sock = runloom.sync.tcp_connect(...)
        ...
    runloom.sync.run(main)               # spawns main + drains

Behind the scenes everything is still cooperative goroutines.  The
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
    """Spawn a goroutine.  Returns a G handle (has .done / .result /
    .wake / .stack).  Equivalent of asyncio.create_task minus the
    coroutine layer."""
    # Warm the deep, non-yielding stdlib imports getaddrinfo triggers
    # (encodings.idna -> stringprep -> unicodedata) here, on the main
    # thread's large stack -- but only when NOT already inside a
    # goroutine, since prewarm itself does a getaddrinfo and would
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
    # import on the big stack before any goroutine runs on a small one.
    _prewarm_stdlib()
    if main_fn is not None:
        runloom_c.go(main_fn)
    return runloom_c.run()


def sleep(seconds):
    """Cooperative sleep -- other goroutines run while this one waits.

    Outside any goroutine, falls back to time.sleep."""
    if runloom_c.current_g() is None:
        _time.sleep(seconds)
        return
    runloom_c.sched_sleep(seconds)


def yield_now():
    """Cooperative yield."""
    runloom_c.sched_yield_classic()


def current():
    """Return a G handle to the currently-running goroutine, or None."""
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
    shutdown).  Blocking calls park the current goroutine via wait_fd
    so other goroutines keep running.

    NOT thread-safe -- one Socket per goroutine.  For send/recv from
    multiple goroutines onto the same fd, use a channel-based fan-in.
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
park = runloom_c.park_self


def wake(g):
    """Wake a goroutine handle returned by go()."""
    if g is not None:
        g.wake()


# --------------------------------------------------------------------
# Fan-in primitives -- WaitGroup / Future / gather.
#
# Built directly on the GenMC-verified park_self / G.wake handshake (G.wake is
# runloom_sched_wake_safe; "safe to call before the park" -- the wake_state CAS
# absorbs a wake that races the park).  A runloom_c.Mutex (an M:N-safe cooperative
# goroutine mutex) guards the O(1) counter/slot + waiter-list bookkeeping and is
# ALWAYS released before park_self, so a waker never blocks on a parked waiter.
# Each is the lean equivalent of a Chan(1)-per-result fan-in, without a channel
# buffer or a select loop per await.
#
# Scope: a park_self-parked waiter is NOT counted as keeping a SINGLE-THREAD
# run() alive (same as a channel park -- see chan_waiters.c.inc park_waiter
# FINDING).  Under M:N (run(N) / mn_run, the primary mode) the hubs stay alive
# while a waiter is parked, so a waker on ANY thread -- including a foreign OS
# thread -- works.  Under SINGLE-THREAD run(), keep the waker a goroutine: if the
# only remaining work is a park_self waiter whose waker is a foreign thread, run()
# can exit and abandon it.  (A foreign-thread-wakeable Event must park on a
# netpoll fd instead -- that is why monkey's CoEvent uses _Parker, not park_self.)
# --------------------------------------------------------------------
class WaitGroup(object):
    """Go-style sync.WaitGroup: wait for a set of goroutines to finish.

    add(n) before spawning n goroutines, done() (or add(-1)) as each finishes,
    wait() blocks until the counter returns to zero.  Multiple goroutines may
    wait(); all are woken when the count hits zero.  Reusable once the count is
    back at zero."""

    def __init__(self):
        self._n = 0
        self._waiters = []          # current_g() handles parked in wait()
        self._mu = runloom_c.Mutex()

    def add(self, delta=1):
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
            # Foreign OS thread: no goroutine to park, poll the counter.
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
            runloom_c.park_self()       # woken by the add() that reaches zero;
            # loop re-checks the count under the lock (absorbs a pre-park wake).


class Future(object):
    """A one-shot result/exception slot any number of goroutines can await.

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

    def result(self):
        g = runloom_c.current_g()
        while True:
            self._mu.lock()
            if self._done:
                exc, res = self._exc, self._result
                self._mu.unlock()
                if exc is not None:
                    raise exc
                return res
            if g is None:
                # Foreign OS thread: poll until resolved.
                self._mu.unlock()
                _time.sleep(0.0005)
                continue
            self._waiters.append(g)
            self._mu.unlock()
            runloom_c.park_self()       # woken by _resolve()


def gather(*callables):
    """Run `callables` as goroutines concurrently and block until all finish,
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
