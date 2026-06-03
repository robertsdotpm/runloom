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
