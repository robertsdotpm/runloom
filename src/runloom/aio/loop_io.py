"""RunloomEventLoop: add/remove reader+writer, the io-runner fiber, and
the low-level sock_* operations."""
import functools as _functools

from ._base import *  # noqa: F401,F403  (shared foundation)
from .handles import _Handle  # noqa: F401


def _release_fd_after(method):
    """Drop a low-level sock_* op's netpoll registration once the op is done.

    The low-level loop.sock_* take a USER-OWNED socket which the caller closes
    with a plain socket.close() -- that does NOT run the _close_sock /
    monkey.close unregister hook (only sockets the bridge itself owns do).
    Without dropping the per-fd arm cache, a reused fd NUMBER inherits the stale
    "armed" mask and netpoll's register-once skip never re-arms it -> the new
    socket's wait_fd parks forever (the long-standing test_recvfrom / fast-churn
    hang).  netpoll_release_if_idle is a no-op while another op is still parked on
    the fd (it only DELs an idle fd), so it is safe to call unconditionally on
    exit.  Monkey + the transports are untouched -- they keep the register-once
    fast path and clear the cache via their own close hooks, so the throughput
    hot path pays nothing.  asyncio's selector loop does the same thing
    (remove_reader/writer after each sock op)."""
    @_functools.wraps(method)
    async def wrapper(self, sock, *args, **kwargs):
        try:
            return await method(self, sock, *args, **kwargs)
        finally:
            try:
                fd = sock.fileno()
            except (OSError, ValueError, AttributeError):
                fd = -1
            if fd >= 0:
                runloom_c.netpoll_release_if_idle(fd)
    return wrapper


class _LoopIOMixin(object):
    def _pg_fileobj_to_fd(self, fileobj):
        # asyncio/selectors contract: accept an int fd or an object exposing
        # fileno(); anything else is a ValueError (test_add_reader_invalid_
        # argument).  Without this an arbitrary object would be stored as a live
        # io key and silently ignored instead of erroring.
        if isinstance(fileobj, int):
            fd = fileobj
        else:
            try:
                fd = int(fileobj.fileno())
            except (AttributeError, TypeError, ValueError):
                raise ValueError(
                    "Invalid file object: {0!r}".format(fileobj)) from None
        if fd < 0:
            raise ValueError("Invalid file descriptor: {0}".format(fd))
        return fd

    def add_reader(self, fd, callback, *args):
        return self._pg_set_io(self._pg_fileobj_to_fd(fd), 1,
                               _Handle(callback, args, self))

    def remove_reader(self, fd):
        return self._pg_clear_io(self._pg_fileobj_to_fd(fd), 1)

    def add_writer(self, fd, callback, *args):
        return self._pg_set_io(self._pg_fileobj_to_fd(fd), 2,
                               _Handle(callback, args, self))

    def remove_writer(self, fd):
        return self._pg_clear_io(self._pg_fileobj_to_fd(fd), 2)

    def _pg_set_io(self, fd, evt, handle):
        st = self._io.get(fd)
        if st is None:
            st = {"r": None, "w": None, "g": None}
            self._io[fd] = st
        key = "r" if evt == 1 else "w"
        old = st[key]
        if old is not None:
            old._cancelled = True
        st[key] = handle
        self._pg_kick_io(fd, st)
        return handle

    def _pg_clear_io(self, fd, evt):
        st = self._io.get(fd)
        if st is None:
            return False
        key = "r" if evt == 1 else "w"
        h = st[key]
        if h is None:
            return False
        h._cancelled = True
        st[key] = None
        if st["r"] is None and st["w"] is None:
            self._io.pop(fd, None)
        self._pg_kick_io(fd, st)
        return True

    def _pg_kick_io(self, fd, st):
        # Wake the fd's I/O fiber (if parked) so it re-reads the interest
        # mask after a reader/writer was added/removed; spawn it if none runs.
        g = st["g"]
        if g is not None:
            try:
                g.cancel_wait_fd()   # raises CancelledError in its _wait_fd;
            except Exception:        # the runner catches it and re-evaluates.
                pass
        if g is None and (st["r"] is not None or st["w"] is not None):
            st["g"] = _fiber_io(lambda: self._pg_io_runner(fd, st))

    def _pg_io_runner(self, fd, st):
        while True:
            r = st["r"]; w = st["w"]
            mask = (1 if (r is not None and not r._cancelled) else 0) \
                 | (2 if (w is not None and not w._cancelled) else 0)
            if mask == 0:
                st["g"] = None
                return
            try:
                ready = _wait_fd(fd, mask)
            except asyncio.CancelledError:
                # Interest changed (or fd dropped) via _pg_kick_io -- re-loop to
                # recompute the mask and re-park, or exit if nothing's left.
                continue
            except Exception:
                st["g"] = None
                return
            # Re-read each slot at dispatch time: a reader callback may add/remove
            # the writer (or close the fd) before we service the write side.
            if (ready & 1) and st["r"] is not None and not st["r"]._cancelled:
                st["r"]._run()
            if (ready & 2) and st["w"] is not None and not st["w"]._cancelled:
                st["w"]._run()
            # Yield before re-arming, mimicking a level-triggered selector pass.
            runloom_c.sched_yield_classic()

    # ---- Network: high-level loop APIs ----

    def _check_sock_nonblocking(self, sock):
        # asyncio's contract for the low-level sock_* ops: in debug mode a
        # blocking socket is a usage error (it would block the whole loop).
        # Matches CPython BaseSelectorEventLoop (selector_events.py).  Outside
        # debug runloom stays lenient and coerces the socket non-blocking below.
        if self._debug and sock.gettimeout() != 0:
            raise ValueError("the socket must be non-blocking")

    @_release_fd_after
    async def sock_connect(self, sock, address):
        self._check_sock_nonblocking(sock)
        sock.setblocking(False)
        try:
            sock.connect(address)
        except BlockingIOError:
            _wait_fd(sock.fileno(), 2)
            err = sock.getsockopt(_socket.SOL_SOCKET, _socket.SO_ERROR)
            if err != 0:
                raise OSError(err, "connect failed")

    @_release_fd_after
    async def sock_accept(self, sock):
        self._check_sock_nonblocking(sock)
        sock.setblocking(False)
        while True:
            try:
                conn, addr = sock.accept()
                conn.setblocking(False)   # asyncio returns a non-blocking conn
                return conn, addr
            except (BlockingIOError, InterruptedError):
                _wait_fd(sock.fileno(), 1)

    @_release_fd_after
    async def sock_recv(self, sock, nbytes):
        self._check_sock_nonblocking(sock)
        sock.setblocking(False)
        while True:
            try:
                return sock.recv(nbytes)
            except (BlockingIOError, InterruptedError):
                _wait_fd(sock.fileno(), 1)

    @_release_fd_after
    async def sock_recv_into(self, sock, buf):
        self._check_sock_nonblocking(sock)
        sock.setblocking(False)
        while True:
            try:
                return sock.recv_into(buf)
            except (BlockingIOError, InterruptedError):
                _wait_fd(sock.fileno(), 1)

    @_release_fd_after
    async def sock_recvfrom(self, sock, bufsize):
        self._check_sock_nonblocking(sock)
        sock.setblocking(False)
        while True:
            try:
                return sock.recvfrom(bufsize)
            except (BlockingIOError, InterruptedError):
                _wait_fd(sock.fileno(), 1)

    @_release_fd_after
    async def sock_recvfrom_into(self, sock, buf, nbytes=0):
        # asyncio 3.11+ API; base class raises NotImplementedError.
        self._check_sock_nonblocking(sock)
        sock.setblocking(False)
        while True:
            try:
                return sock.recvfrom_into(buf, nbytes)
            except (BlockingIOError, InterruptedError):
                _wait_fd(sock.fileno(), 1)

    async def sock_sendfile(self, sock, file, offset=0, count=None, *,
                            fallback=True):
        # No OS sendfile path on runloom; mirror asyncio's "native unavailable"
        # signal so callers fall back to read+send (loop.sendfile handles the
        # transport-level fallback).
        raise asyncio.SendfileNotAvailableError(
            "sock_sendfile syscall path is not available on runloom")

    @_release_fd_after
    async def sock_sendall(self, sock, data):
        self._check_sock_nonblocking(sock)
        sock.setblocking(False)
        view = memoryview(data)
        sent = 0
        while sent < len(view):
            try:
                n = sock.send(view[sent:])
                sent += n
            except (BlockingIOError, InterruptedError):
                _wait_fd(sock.fileno(), 2)

    @_release_fd_after
    async def sock_sendto(self, sock, data, address):
        self._check_sock_nonblocking(sock)
        sock.setblocking(False)
        while True:
            try:
                return sock.sendto(data, address)
            except (BlockingIOError, InterruptedError):
                _wait_fd(sock.fileno(), 2)

    # recvmsg / sendmsg (POSIX): ancillary-data + SCM_RIGHTS fd passing over the
    # loop.  Not part of AbstractEventLoop, but runloom.monkey makes the blocking
    # socket.recvmsg/sendmsg cooperative and the bridge (monkey OFF) needs an
    # equivalent -- same EAGAIN -> wait_fd loop as the other sock_* ops.
    if hasattr(_socket.socket, "recvmsg"):
        @_release_fd_after
        async def sock_recvmsg(self, sock, bufsize, ancbufsize=0, flags=0):
            self._check_sock_nonblocking(sock)
            sock.setblocking(False)
            while True:
                try:
                    return sock.recvmsg(bufsize, ancbufsize, flags)
                except (BlockingIOError, InterruptedError):
                    _wait_fd(sock.fileno(), 1)

        @_release_fd_after
        async def sock_recvmsg_into(self, sock, buffers, ancbufsize=0, flags=0):
            self._check_sock_nonblocking(sock)
            sock.setblocking(False)
            while True:
                try:
                    return sock.recvmsg_into(buffers, ancbufsize, flags)
                except (BlockingIOError, InterruptedError):
                    _wait_fd(sock.fileno(), 1)

        @_release_fd_after
        async def sock_sendmsg(self, sock, buffers, ancdata=(), flags=0,
                               address=None):
            self._check_sock_nonblocking(sock)
            sock.setblocking(False)
            while True:
                try:
                    if address is None:
                        return sock.sendmsg(buffers, ancdata, flags)
                    return sock.sendmsg(buffers, ancdata, flags, address)
                except (BlockingIOError, InterruptedError):
                    _wait_fd(sock.fileno(), 2)

    # ---- executor (thread pool) ----
