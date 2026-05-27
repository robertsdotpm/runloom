"""pygo socket monkey-patch.

Replaces the methods on socket.socket that would block the OS thread with
versions that park the current goroutine on the fd via pygo_core.wait_fd
and retry.  The Python API stays the same -- you call `s.recv(1024)`
exactly like blocking code, but other goroutines run while you wait.

Apply once at startup:
    import pygo, pygo.monkey
    pygo.monkey.patch()

Then write naive blocking-style code:
    s = socket.create_connection(("example.com", 80))
    s.sendall(b"GET / HTTP/1.0\\r\\n\\r\\n")
    print(s.recv(4096))

Internally each blocking call becomes nonblock + wait_fd(fd, READ|WRITE).
"""
import errno
import socket
import sys

import pygo_core

READ  = 1
WRITE = 2

_orig_socket = socket.socket


def _make_nonblocking(sock):
    """Set the underlying fd to non-blocking, idempotent."""
    if sock.gettimeout() != 0.0:
        sock.setblocking(False)


def _patched_recv(self, bufsize, flags=0):
    _make_nonblocking(self)
    while True:
        try:
            return _orig_recv(self, bufsize, flags)
        except (BlockingIOError, InterruptedError):
            r = pygo_core.wait_fd(self.fileno(), READ)
            if r == 0:
                raise socket.timeout("recv timed out")


def _patched_send(self, data, flags=0):
    _make_nonblocking(self)
    while True:
        try:
            return _orig_send(self, data, flags)
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(self.fileno(), WRITE)


def _patched_sendall(self, data, flags=0):
    _make_nonblocking(self)
    if isinstance(data, memoryview):
        view = data
    else:
        view = memoryview(data)
    sent = 0
    while sent < len(view):
        try:
            n = _orig_send(self, view[sent:], flags)
            if n:
                sent += n
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(self.fileno(), WRITE)


def _patched_accept(self):
    _make_nonblocking(self)
    while True:
        try:
            conn, addr = _orig_accept(self)
            return conn, addr
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(self.fileno(), READ)


def _patched_connect(self, address):
    _make_nonblocking(self)
    while True:
        try:
            return _orig_connect(self, address)
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(self.fileno(), WRITE)
        except OSError as e:
            if e.errno == errno.EISCONN:
                return
            if e.errno in (errno.EINPROGRESS, errno.EALREADY):
                pygo_core.wait_fd(self.fileno(), WRITE)
                continue
            raise


def _patched_recvfrom(self, bufsize, flags=0):
    _make_nonblocking(self)
    while True:
        try:
            return _orig_recvfrom(self, bufsize, flags)
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(self.fileno(), READ)


def _patched_sendto(self, data, *args):
    _make_nonblocking(self)
    while True:
        try:
            return _orig_sendto(self, data, *args)
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(self.fileno(), WRITE)


# Captured originals + flag so patch() is idempotent.
_orig_recv = None
_orig_send = None
_orig_sendall = None
_orig_accept = None
_orig_connect = None
_orig_recvfrom = None
_orig_sendto = None
_patched = False


def patch():
    """Apply socket monkey-patch.  Idempotent."""
    global _orig_recv, _orig_send, _orig_sendall, _orig_accept
    global _orig_connect, _orig_recvfrom, _orig_sendto, _patched
    if _patched:
        return
    sock = socket.socket
    _orig_recv      = sock.recv
    _orig_send      = sock.send
    _orig_sendall   = sock.sendall
    _orig_accept    = sock.accept
    _orig_connect   = sock.connect
    _orig_recvfrom  = sock.recvfrom
    _orig_sendto    = sock.sendto

    sock.recv      = _patched_recv
    sock.send      = _patched_send
    sock.sendall   = _patched_sendall
    sock.accept    = _patched_accept
    sock.connect   = _patched_connect
    sock.recvfrom  = _patched_recvfrom
    sock.sendto    = _patched_sendto
    _patched = True


def unpatch():
    """Restore original socket methods.  Useful for tests."""
    global _patched
    if not _patched:
        return
    sock = socket.socket
    sock.recv      = _orig_recv
    sock.send      = _orig_send
    sock.sendall   = _orig_sendall
    sock.accept    = _orig_accept
    sock.connect   = _orig_connect
    sock.recvfrom  = _orig_recvfrom
    sock.sendto    = _orig_sendto
    _patched = False
