"""Cooperative ssl.SSLSocket I/O patches."""
from ._base import *  # noqa: F401,F403  (shared foundation)
# The timeout- and WAIT_FD_CANCELLED-aware park used by the plain-socket loops.
# .sockets is imported before .tls in the package __init__, so this is safe.
from .sockets import _wait_io  # noqa: F401

# ============================================================
# ssl
# ============================================================
_orig_ssl_recv = None
_orig_ssl_recv_into = None
_orig_ssl_send = None
_orig_ssl_sendall = None
_orig_ssl_do_handshake = None
_orig_ssl_unwrap = None


def _patched_ssl_recv(self, buflen=1024, flags=0):
    _make_nonblocking(self)
    while True:
        try:
            return _orig_ssl_recv(self, buflen, flags)
        except _ssl_mod.SSLWantReadError:
            _wait_io(self, self.fileno(), READ)
        except _ssl_mod.SSLWantWriteError:
            _wait_io(self, self.fileno(), WRITE)


def _patched_ssl_recv_into(self, buffer, nbytes=None, flags=0):
    _make_nonblocking(self)
    while True:
        try:
            if nbytes is None:
                return _orig_ssl_recv_into(self, buffer, flags=flags)
            return _orig_ssl_recv_into(self, buffer, nbytes, flags)
        except _ssl_mod.SSLWantReadError:
            _wait_io(self, self.fileno(), READ)
        except _ssl_mod.SSLWantWriteError:
            _wait_io(self, self.fileno(), WRITE)


def _patched_ssl_send(self, data, flags=0):
    _make_nonblocking(self)
    while True:
        try:
            return _orig_ssl_send(self, data, flags)
        except _ssl_mod.SSLWantReadError:
            _wait_io(self, self.fileno(), READ)
        except _ssl_mod.SSLWantWriteError:
            _wait_io(self, self.fileno(), WRITE)


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
            _wait_io(self, self.fileno(), READ)
        except _ssl_mod.SSLWantWriteError:
            _wait_io(self, self.fileno(), WRITE)


def _patched_ssl_do_handshake(self, *a, **kw):
    _make_nonblocking(self)
    while True:
        try:
            return _orig_ssl_do_handshake(self, *a, **kw)
        except _ssl_mod.SSLWantReadError:
            _wait_io(self, self.fileno(), READ)
        except _ssl_mod.SSLWantWriteError:
            _wait_io(self, self.fileno(), WRITE)


def _patched_ssl_unwrap(self):
    while True:
        try:
            return _orig_ssl_unwrap(self)
        except _ssl_mod.SSLWantReadError:
            _wait_io(self, self.fileno(), READ)
        except _ssl_mod.SSLWantWriteError:
            _wait_io(self, self.fileno(), WRITE)


_orig_wrap_socket = None


def _patched_wrap_socket(self, sock, server_side=False,
                         do_handshake_on_connect=True,
                         suppress_ragged_eofs=True,
                         server_hostname=None, session=None):
    """Make the CLIENT implicit handshake cooperative.

    The high-level TLS footgun is client-side: after monkey's cooperative
    connect the fd is non-blocking, so wrap_socket(do_handshake_on_connect=
    True) raises "do_handshake_on_connect should not be specified for
    non-blocking sockets" (or, if blocking, runs a handshake that wedges the
    hub).  For a CONNECTED client socket we defer the handshake and run the
    (patched, cooperative) do_handshake ourselves -> urllib/http.client https,
    smtplib/imaplib/poplib SSL & STARTTLS, ftplib FTP_TLS, xmlrpc https all
    park instead of stall.

    Server listeners and server_side wraps pass through UNCHANGED: a listener
    is not connected (do_handshake_on_connect is a no-op there) and accepted
    connections auto-handshake through the already-patched cooperative
    do_handshake, so server-side TLS keeps working with no behavior change."""
    if do_handshake_on_connect and not server_side:
        connected = True
        try:
            sock.getpeername()
        except OSError:
            connected = False          # listener / unconnected -> pass through
        if connected:
            wrapped = _orig_wrap_socket(
                self, sock, server_side=False,
                do_handshake_on_connect=False,
                suppress_ragged_eofs=suppress_ragged_eofs,
                server_hostname=server_hostname, session=session)
            wrapped.do_handshake()     # patched -> cooperative (wait_fd on WANT_*)
            return wrapped
    return _orig_wrap_socket(
        self, sock, server_side=server_side,
        do_handshake_on_connect=do_handshake_on_connect,
        suppress_ragged_eofs=suppress_ragged_eofs,
        server_hostname=server_hostname, session=session)


def _patch_ssl():
    global _orig_ssl_recv, _orig_ssl_recv_into, _orig_ssl_send
    global _orig_ssl_sendall, _orig_ssl_do_handshake, _orig_ssl_unwrap
    global _orig_wrap_socket
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
    _orig_wrap_socket = _ssl_mod.SSLContext.wrap_socket
    _ssl_mod.SSLContext.wrap_socket = _patched_wrap_socket

    # Pre-pay OpenSSL's one-time init on the MAIN thread (8 MB stack).  The
    # first use of _ssl in a process drives a ~40 KB C-stack init path that
    # overflows the default 32 KB fiber stack -> a guard-page SEGV (a g
    # that is the first ever to touch ssl would crash; see
    # tests/test_stack_frames.py and docs/cooperative_stdlib_coverage.md).
    # Importing ssl (via _ssl_mod, on the main thread) already triggers this,
    # but force it explicitly so a future lazy-import refactor can't silently
    # move the fat init onto a fiber's stack.  Throwaway, no network.
    try:
        proto = getattr(_ssl_mod, "PROTOCOL_TLS_CLIENT", None)
        if proto is None:
            proto = _ssl_mod.PROTOCOL_TLS
        _ssl_mod.SSLContext(proto)
    except Exception:        # noqa: BLE001  warm-up is best-effort
        pass


def _unpatch_ssl():
    S = _ssl_mod.SSLSocket
    S.recv         = _orig_ssl_recv
    S.recv_into    = _orig_ssl_recv_into
    S.send         = _orig_ssl_send
    S.sendall      = _orig_ssl_sendall
    S.do_handshake = _orig_ssl_do_handshake
    S.unwrap       = _orig_ssl_unwrap
    if _orig_wrap_socket is not None:
        _ssl_mod.SSLContext.wrap_socket = _orig_wrap_socket
