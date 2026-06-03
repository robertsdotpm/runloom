"""Cooperative ssl.SSLSocket I/O patches."""
from ._base import *  # noqa: F401,F403  (shared foundation)

# ============================================================
# ssl
# ============================================================
_orig_ssl_recv = None
_orig_ssl_recv_into = None
_orig_ssl_send = None
_orig_ssl_sendall = None
_orig_ssl_do_handshake = None
_orig_ssl_unwrap = None


def _ssl_wait(self, want):
    if want is _ssl_mod.SSLWantReadError:
        runloom_c.wait_fd(self.fileno(), READ)
    else:
        runloom_c.wait_fd(self.fileno(), WRITE)


def _patched_ssl_recv(self, buflen=1024, flags=0):
    _make_nonblocking(self)
    while True:
        try:
            return _orig_ssl_recv(self, buflen, flags)
        except _ssl_mod.SSLWantReadError:
            runloom_c.wait_fd(self.fileno(), READ)
        except _ssl_mod.SSLWantWriteError:
            runloom_c.wait_fd(self.fileno(), WRITE)


def _patched_ssl_recv_into(self, buffer, nbytes=None, flags=0):
    _make_nonblocking(self)
    while True:
        try:
            if nbytes is None:
                return _orig_ssl_recv_into(self, buffer, flags=flags)
            return _orig_ssl_recv_into(self, buffer, nbytes, flags)
        except _ssl_mod.SSLWantReadError:
            runloom_c.wait_fd(self.fileno(), READ)
        except _ssl_mod.SSLWantWriteError:
            runloom_c.wait_fd(self.fileno(), WRITE)


def _patched_ssl_send(self, data, flags=0):
    _make_nonblocking(self)
    while True:
        try:
            return _orig_ssl_send(self, data, flags)
        except _ssl_mod.SSLWantReadError:
            runloom_c.wait_fd(self.fileno(), READ)
        except _ssl_mod.SSLWantWriteError:
            runloom_c.wait_fd(self.fileno(), WRITE)


def _patched_ssl_sendall(self, data, flags=0):
    _make_nonblocking(self)
    view = data if isinstance(data, memoryview) else memoryview(data)
    sent = 0
    while sent < len(view):
        try:
            n = _orig_ssl_send(self, view[sent:], flags)
            if n:
                sent += n
        except _ssl_mod.SSLWantReadError:
            runloom_c.wait_fd(self.fileno(), READ)
        except _ssl_mod.SSLWantWriteError:
            runloom_c.wait_fd(self.fileno(), WRITE)


def _patched_ssl_do_handshake(self, *a, **kw):
    _make_nonblocking(self)
    while True:
        try:
            return _orig_ssl_do_handshake(self, *a, **kw)
        except _ssl_mod.SSLWantReadError:
            runloom_c.wait_fd(self.fileno(), READ)
        except _ssl_mod.SSLWantWriteError:
            runloom_c.wait_fd(self.fileno(), WRITE)


def _patched_ssl_unwrap(self):
    while True:
        try:
            return _orig_ssl_unwrap(self)
        except _ssl_mod.SSLWantReadError:
            runloom_c.wait_fd(self.fileno(), READ)
        except _ssl_mod.SSLWantWriteError:
            runloom_c.wait_fd(self.fileno(), WRITE)


def _patch_ssl():
    global _orig_ssl_recv, _orig_ssl_recv_into, _orig_ssl_send
    global _orig_ssl_sendall, _orig_ssl_do_handshake, _orig_ssl_unwrap
    S = _ssl_mod.SSLSocket
    _orig_ssl_recv         = S.recv
    _orig_ssl_recv_into    = S.recv_into
    _orig_ssl_send         = S.send
    _orig_ssl_sendall      = S.sendall
    _orig_ssl_do_handshake = S.do_handshake
    _orig_ssl_unwrap       = S.unwrap
    S.recv         = _patched_ssl_recv
    S.recv_into    = _patched_ssl_recv_into
    S.send         = _patched_ssl_send
    S.sendall      = _patched_ssl_sendall
    S.do_handshake = _patched_ssl_do_handshake
    S.unwrap       = _patched_ssl_unwrap


def _unpatch_ssl():
    S = _ssl_mod.SSLSocket
    S.recv         = _orig_ssl_recv
    S.recv_into    = _orig_ssl_recv_into
    S.send         = _orig_ssl_send
    S.sendall      = _orig_ssl_sendall
    S.do_handshake = _orig_ssl_do_handshake
    S.unwrap       = _orig_ssl_unwrap
