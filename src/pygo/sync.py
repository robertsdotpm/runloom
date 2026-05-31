"""pygo.sync -- plain-Python (no async/await) facade over pygo.aio.

Same surface, no coroutines.  Lets you port async code to straight-line
synchronous code without giving up the pygo scheduler.  Two styles:

  Style A: blocking-style I/O inside a goroutine.
    pygo.sync.start()                # opens an implicit scheduler
    sock = pygo.sync.tcp_connect(...)
    sock.sendall(b'...')              # cooperative
    data = sock.recv(...)
    pygo.sync.go(other_worker)        # other goroutines run concurrently
    pygo.sync.stop()

  Style B: drive a "main" function as the entry point.
    def main():
        sock = pygo.sync.tcp_connect(...)
        ...
    pygo.sync.run(main)               # spawns main + drains

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

import pygo_core
from pygo.runtime import prewarm_stdlib as _prewarm_stdlib


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
    if pygo_core.current_g() is None:
        _prewarm_stdlib()
    if args or kwargs:
        target = lambda: callable_(*args, **kwargs)
    else:
        target = callable_
    return pygo_core.go(target)


def run(main_fn=None):
    """Drive the scheduler until idle.  Optionally spawn main_fn first."""
    # Same prewarm as pygo.runtime.run(): resolve getaddrinfo's lazy codec
    # import on the big stack before any goroutine runs on a small one.
    _prewarm_stdlib()
    if main_fn is not None:
        pygo_core.go(main_fn)
    return pygo_core.run()


def sleep(seconds):
    """Cooperative sleep -- other goroutines run while this one waits.

    Outside any goroutine, falls back to time.sleep."""
    if pygo_core.current_g() is None:
        _time.sleep(seconds)
        return
    pygo_core.sched_sleep(seconds)


def yield_now():
    """Cooperative yield."""
    pygo_core.sched_yield_classic()


def current():
    """Return a G handle to the currently-running goroutine, or None."""
    return pygo_core.current_g()


# --------------------------------------------------------------------
# Channels (re-export so callers don't need pygo_core directly)
# --------------------------------------------------------------------
Chan = pygo_core.Chan
select = pygo_core.select


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
            try: pygo_core.netpoll_unregister(fd)
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
            pygo_core.wait_fd(self._s.fileno(), 2)
            err = self._s.getsockopt(_socket.SOL_SOCKET, _socket.SO_ERROR)
            if err != 0:
                raise OSError(err, "connect failed")

    def accept(self):
        while True:
            try:
                conn, addr = self._s.accept()
            except (BlockingIOError, InterruptedError):
                pygo_core.wait_fd(self._s.fileno(), 1)
                continue
            return Socket._wrap(conn), addr

    def recv(self, n):
        while True:
            try:
                return self._s.recv(n)
            except (BlockingIOError, InterruptedError):
                pygo_core.wait_fd(self._s.fileno(), 1)

    def recv_into(self, buf):
        while True:
            try:
                return self._s.recv_into(buf)
            except (BlockingIOError, InterruptedError):
                pygo_core.wait_fd(self._s.fileno(), 1)

    def recvfrom(self, n):
        while True:
            try:
                return self._s.recvfrom(n)
            except (BlockingIOError, InterruptedError):
                pygo_core.wait_fd(self._s.fileno(), 1)

    def send(self, data):
        while True:
            try:
                return self._s.send(data)
            except (BlockingIOError, InterruptedError):
                pygo_core.wait_fd(self._s.fileno(), 2)

    def sendto(self, data, addr):
        while True:
            try:
                return self._s.sendto(data, addr)
            except (BlockingIOError, InterruptedError):
                pygo_core.wait_fd(self._s.fileno(), 2)

    def sendall(self, data):
        view = memoryview(data)
        sent = 0
        while sent < len(view):
            try:
                n = self._s.send(view[sent:])
                sent += n
            except (BlockingIOError, InterruptedError):
                pygo_core.wait_fd(self._s.fileno(), 2)


# --------------------------------------------------------------------
# Convenience constructors (mirror the async versions in pygo.aio)
# --------------------------------------------------------------------
def tcp_connect(host, port, *, family=0, local_addr=None):
    """Connect a TCP socket cooperatively.  Returns a pygo.sync.Socket."""
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
    to remote_addr).  Returns a pygo.sync.Socket."""
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
from pygo.monkey import CoLock as Lock
from pygo.monkey import CoEvent as Event
from pygo.monkey import CoSemaphore as Semaphore
from pygo.monkey import CoCondition as Condition


# --------------------------------------------------------------------
# Park / wake -- exposed for callers building their own primitives
# --------------------------------------------------------------------
park = pygo_core.park_self


def wake(g):
    """Wake a goroutine handle returned by go()."""
    if g is not None:
        g.wake()
