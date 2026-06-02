"""_TLSSock: synchronous SSLSocket-style TLS over a cooperative socket."""
from ._base import *  # noqa: F401,F403  (shared foundation)

class _TLSSock(object):
    """Cooperative TLS for the asyncio bridge, working on every netpoll
    backend (epoll/kqueue/IOCP/WSAPoll/select).

    Wraps the raw socket in a real ``ssl.SSLSocket`` (which owns the fd) and
    drives its non-blocking ``recv``/``send``/``do_handshake`` with pygo's
    ``wait_fd``, mirroring pygo.monkey's validated ssl patch.  It presents the
    same blocking-cooperative socket surface (recv/send/sendall/fileno/
    shutdown/close/getpeername/...) that StreamReader/StreamWriter/
    _StreamTransport already expect, so those classes use it unchanged --
    plaintext and TLS go through the exact same I/O loops.

    SSLSocket / OpenSSL are not safe for concurrent use, so a cooperative
    CoLock serialises every SSLObject call.  Crucially the lock is RELEASED
    across every wait_fd, so a read parked waiting for inbound bytes never
    blocks a concurrent write (full-duplex keeps working).  Holding a real
    OS lock here would be wrong -- pygo can switch goroutines at a bytecode
    boundary while one holds it, deadlocking the hub; CoLock is switch-safe.
    """

    def __init__(self, raw, context, *, server_side=False,
                 server_hostname=None):
        raw.setblocking(False)
        # Mirror asyncio's sslproto normalisation: a falsy server_hostname --
        # notably the empty string, which create_connection accepts to mean
        # "TLS without SNI / hostname verification" -- and every server-side
        # wrap must pass None to the ssl machinery.  ssl.wrap_socket raises
        # ValueError on an empty (or leading-dot) server_hostname, so without
        # this an explicit server_hostname='' blows up the whole handshake.
        if server_side or not server_hostname:
            server_hostname = None
        self._ssl = context.wrap_socket(
            raw, server_side=server_side,
            server_hostname=server_hostname,
            do_handshake_on_connect=False)
        self._ssl.setblocking(False)
        self._lock = _get_colock()()
        self._closed = False

    def __getattr__(self, name):
        # Delegate socket-introspection surface we don't wrap explicitly --
        # family / type / proto / setsockopt / getsockname / ... -- to the
        # underlying ssl.SSLSocket, which subclasses socket.socket and exposes
        # them.  asyncio code that pulls the socket via
        # transport.get_extra_info("socket") treats it as a real socket; e.g.
        # aiohttp's tcp_nodelay reads sock.family and calls sock.setsockopt(),
        # which raised AttributeError on the bare _TLSSock.  The cooperative
        # recv/send/sendall/fileno/etc. are defined on the class, so they take
        # precedence and __getattr__ never shadows them.  Guard _ssl and dunders
        # to avoid recursion before __init__ binds _ssl.
        if name == "_ssl" or name.startswith("__"):
            raise AttributeError(name)
        return getattr(self._ssl, name)

    def fileno(self):
        return self._ssl.fileno()

    def do_handshake(self, timeout=None):
        # timeout (seconds) bounds the WHOLE handshake (asyncio's
        # ssl_handshake_timeout); a peer that stalls mid-handshake must not
        # park this goroutine forever.  None = wait indefinitely.
        fd = self._ssl.fileno()
        deadline = None if timeout is None else (_time.monotonic() + timeout)
        while True:
            want = None
            with self._lock:
                try:
                    self._ssl.do_handshake()
                    return
                except _ssl.SSLWantReadError:
                    want = _WAIT_READ
                except _ssl.SSLWantWriteError:
                    want = _WAIT_WRITE
                except _ssl.SSLEOFError:
                    # Peer closed the connection mid-handshake (EOF in
                    # violation of protocol).  asyncio's sslproto translates a
                    # premature EOF while DO_HANDSHAKE into ConnectionResetError
                    # (eof_received -> _on_handshake_complete(ConnectionResetError));
                    # mirror that so callers see the connection-reset they expect
                    # rather than a raw ssl.SSLEOFError.
                    raise ConnectionResetError(
                        "Connection lost during TLS handshake") from None
            if deadline is None:
                _wait_fd(fd, want)
            else:
                remaining = deadline - _time.monotonic()
                if remaining <= 0:
                    # Match asyncio's ssl_handshake_timeout exception type and
                    # message (sslproto._check_handshake_timeout) so callers'
                    # assertRaisesRegex(ConnectionAbortedError, 'SSL handshake.*
                    # is taking longer') holds.
                    raise ConnectionAbortedError(
                        "SSL handshake is taking longer than {0} seconds: "
                        "aborting the connection".format(timeout))
                # wait_fd returns (without raising) when the timeout elapses;
                # the next loop re-checks the deadline and raises above.
                _wait_fd(fd, want, max(1, int(remaining * 1000)))

    def recv_nb(self, n):
        # SINGLE non-blocking recv attempt: returns decrypted bytes, b'' on EOF,
        # or raises BlockingIOError if no application data is ready yet.  Never
        # parks -- the merged _StreamTransport io goroutine must not block in
        # recv (it would stall the write drain on the SAME goroutine, a
        # full-duplex deadlock).  The cooperative parking recv() below is for
        # callers that own a dedicated read goroutine.
        if self._closed:
            return b""
        with self._lock:
            try:
                return self._ssl.recv(n)
            except (_ssl.SSLWantReadError, _ssl.SSLWantWriteError):
                raise BlockingIOError()
            except _ssl.SSLZeroReturnError:
                self._peer_close_notify = True
                return b""
            except _ssl.SSLEOFError:
                return b""

    def recv(self, n):
        if self._closed:
            return b""
        fd = self._ssl.fileno()
        while True:
            want = None
            with self._lock:
                try:
                    return self._ssl.recv(n)
                except _ssl.SSLWantReadError:
                    want = _WAIT_READ
                except _ssl.SSLWantWriteError:
                    want = _WAIT_WRITE
                except _ssl.SSLZeroReturnError:
                    return b""          # clean TLS close_notify -> EOF
                except _ssl.SSLEOFError:
                    return b""          # peer dropped without close_notify
                except OSError as e:
                    # SSLWant*/Zero/EOF are SSLError(=OSError) subclasses and
                    # are caught above; a bare EAGAIN means the kernel buffer
                    # is dry -- park for readability.  Anything else is real.
                    if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK):
                        want = _WAIT_READ
                    else:
                        raise
            _wait_fd(fd, want)

    def recv_into(self, buffer, nbytes=0):
        if self._closed:
            return 0
        fd = self._ssl.fileno()
        while True:
            want = None
            with self._lock:
                try:
                    return self._ssl.recv_into(buffer, nbytes)
                except _ssl.SSLWantReadError:
                    want = _WAIT_READ
                except _ssl.SSLWantWriteError:
                    want = _WAIT_WRITE
                except (_ssl.SSLZeroReturnError, _ssl.SSLEOFError):
                    return 0
                except OSError as e:
                    if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK):
                        want = _WAIT_READ
                    else:
                        raise
            _wait_fd(fd, want)

    def send(self, data):
        fd = self._ssl.fileno()
        while True:
            want = None
            with self._lock:
                try:
                    return self._ssl.send(data)
                except _ssl.SSLWantReadError:
                    want = _WAIT_READ
                except _ssl.SSLWantWriteError:
                    want = _WAIT_WRITE
                except OSError as e:
                    if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK):
                        want = _WAIT_WRITE
                    else:
                        raise
            _wait_fd(fd, want)

    def sendall(self, data):
        view = data if isinstance(data, memoryview) else memoryview(data)
        total = len(view)
        sent = 0
        while sent < total:
            sent += self.send(view[sent:])
        return None

    def setblocking(self, flag):
        # Always cooperative-nonblocking under the hood; ignore.
        pass

    def shutdown(self, how):
        try:
            self._ssl.shutdown(how)
        except OSError:
            pass

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self._ssl.close()
        except OSError:
            pass

    def getpeername(self):
        return self._ssl.getpeername()

    def getsockname(self):
        return self._ssl.getsockname()

    def getsockopt(self, *a):
        return self._ssl.getsockopt(*a)

    @property
    def ssl_object(self):
        return self._ssl

    def __del__(self):
        # Safety net: a _TLSSock dropped without close() -- e.g. a connection
        # that errored mid-setup and never routed through transport.close(), or
        # a session torn down on error -- would otherwise let its underlying
        # ssl.SSLSocket reach GC with an open fd, raising ResourceWarning(
        # "unclosed <ssl.SSLSocket ...>").  pytest's unraisable-exception hook
        # elevates that to a test error (test_error_in_performing_request,
        # test_aiohttp_request_ctx_manager_close_sess_on_error).  Close it here
        # before SSLSocket.__del__ can warn.
        if not getattr(self, "_closed", True):
            try:
                self._ssl.close()
            except Exception:
                pass
