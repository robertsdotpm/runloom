"""_MemoryBIOTLS + _SSLProtocolView: memory-BIO TLS engine for the
transport layer."""
from ._base import *  # noqa: F401,F403  (shared foundation)

class _MemoryBIOTLS(object):
    """Cooperative TLS over a MemoryBIO pair -- the asyncio-faithful design
    (mirrors ssl.SSLObject + sslproto.SSLProtocol).  pygo owns every handshake
    byte: it feeds the *incoming* BIO from the raw socket and drains the
    *outgoing* BIO to it.  That makes possible what the fd-based _TLSSock can't:

      * start_tls where the peer's ClientHello (or trailing plaintext) was
        ALREADY read off the socket into a Python buffer -- pass those bytes as
        ``incoming_data`` and they seed the handshake (gh-142352);
      * a real ``_ssl_protocol`` exposing ``_sslcontext`` (white-box code/tests
        read transport._ssl_protocol._sslcontext);
      * an explicit, cooperative close_notify on close.

    Presents the SAME socket surface (fileno/recv/recv_nb/recv_into/send/sendall/
    shutdown/close/getpeername/...) the transport already drives, so it is a
    drop-in for _TLSSock.  Runs inside the transport's single io goroutine, so
    blocking helpers may park; recv_nb never parks for inbound data.  One CoLock
    serialises SSLObject calls (released across every wait_fd)."""

    def __init__(self, raw, context, *, server_side=False,
                 server_hostname=None, incoming_data=b""):
        raw.setblocking(False)
        self._raw = raw
        self._fd = raw.fileno()
        if server_side or not server_hostname:
            server_hostname = None
        self._context = context
        self._server_side = server_side
        self._inc = _ssl.MemoryBIO()
        self._out = _ssl.MemoryBIO()
        self._obj = context.wrap_bio(self._inc, self._out,
                                     server_side=server_side,
                                     server_hostname=server_hostname)
        if incoming_data:
            self._inc.write(incoming_data)
        self._lock = _get_colock()()
        self._closed = False
        self._peer_close_notify = False
        self._eof_in = False

    # ---- BIO <-> raw-socket plumbing (caller holds NO lock for the socket
    # waits; SSLObject calls are serialised by self._lock) ----
    def _pump_out(self):
        # Drain the outgoing BIO to the raw socket, parking on WRITE until every
        # queued encrypted byte is on the wire.  Runs in the io goroutine, so
        # parking is safe.  Returns False if the socket died.
        data = self._out.read()
        if not data:
            return True
        view = memoryview(data)
        while view:
            try:
                n = self._raw.send(view)
                view = view[n:]
            except (BlockingIOError, InterruptedError):
                try:
                    _wait_fd(self._fd, _WAIT_WRITE)
                except Exception:
                    return False
            except OSError:
                return False
        return True

    def _feed_in_nb(self):
        # Read encrypted bytes from the raw socket NON-BLOCKING into the incoming
        # BIO.  True = bytes fed; raises BlockingIOError if the socket is dry;
        # False = peer EOF.
        #
        # DELIBERATELY does NOT self._inc.write_eof() on EOF: write_eof'ing the
        # incoming BIO poisons the SSLObject's WRITE side too -- a subsequent
        # self._obj.write() then raises SSLEOFError ("EOF in violation of
        # protocol").  That breaks a TLS half-close where the peer sent a bare
        # FIN (socket.shutdown(SHUT_WR), no close_notify) but is still READING
        # our queued trailing data (test_remote_shutdown_receives_trailing_data's
        # eof_server): we must keep draining 4MB through _obj.write() AFTER our
        # read side EOF'd.  asyncio's SSLProtocol never write_eof's its incoming
        # BIO either; it surfaces read-EOF via its state machine.  We do the
        # same with the _eof_in flag, which recv_nb turns into a b'' return.
        if self._eof_in:
            return False
        try:
            enc = self._raw.recv(65536)
        except (BlockingIOError, InterruptedError):
            raise BlockingIOError()
        except OSError:
            self._eof_in = True
            return False
        if not enc:
            self._eof_in = True
            return False
        self._inc.write(enc)
        return True

    def fileno(self):
        return self._fd

    def pending(self):
        # Bytes the SSL layer can yield without reading the socket: already
        # decrypted (SSLObject.pending) PLUS undecrypted bytes sitting in the
        # incoming BIO from a read-ahead (a recv that pulled the handshake's
        # final flight AND following app data, or several records at once).
        # The transport's io loop drains this before parking READ -- the socket
        # isn't readable for BIO-buffered data, so otherwise it strands.
        try:
            return self._obj.pending() + self._inc.pending
        except Exception:
            return 0

    def do_handshake(self, timeout=None):
        deadline = None if timeout is None else (_time.monotonic() + timeout)
        while True:
            want_read = False
            with self._lock:
                try:
                    self._obj.do_handshake()
                    done = True
                except _ssl.SSLWantReadError:
                    done = False
                    want_read = True
                except _ssl.SSLWantWriteError:
                    done = False
                except _ssl.SSLEOFError:
                    raise ConnectionResetError(
                        "Connection lost during TLS handshake") from None
                except _ssl.SSLError:
                    raise
            # Always flush whatever the handshake produced (our flight).
            if not self._pump_out():
                raise ConnectionResetError(
                    "Connection lost during TLS handshake") from None
            if done:
                return
            if not want_read:
                # WantWrite: we just pumped; loop to retry.
                continue
            # WantRead: pull the peer's next flight.
            if deadline is None:
                ms = -1
            else:
                remaining = deadline - _time.monotonic()
                if remaining <= 0:
                    raise ConnectionAbortedError(
                        "SSL handshake is taking longer than {0} seconds: "
                        "aborting the connection".format(timeout))
                ms = max(1, int(remaining * 1000))
            try:
                fed = self._feed_in_nb()
            except BlockingIOError:
                try:
                    _wait_fd(self._fd, _WAIT_READ, ms)
                except Exception:
                    raise ConnectionResetError(
                        "Connection lost during TLS handshake") from None
                continue
            if not fed:
                raise ConnectionResetError(
                    "Connection lost during TLS handshake") from None

    def recv_nb(self, n):
        # SINGLE non-blocking decrypt attempt.  Returns decrypted bytes, b'' on
        # EOF/close_notify, or raises BlockingIOError if no app data is ready.
        if self._closed:
            return b""
        while True:
            with self._lock:
                try:
                    return self._obj.read(n)
                except _ssl.SSLWantReadError:
                    want_read = True
                except _ssl.SSLWantWriteError:
                    want_read = False          # renegotiation wants to send
                except _ssl.SSLZeroReturnError:
                    self._peer_close_notify = True
                    return b""
                except _ssl.SSLEOFError:
                    return b""
            # Outside the lock: flush any output we produced, then (if the read
            # wanted inbound) pull more encrypted bytes -- NON-BLOCKING.
            if not self._pump_out():
                return b""
            if want_read:
                fed = self._feed_in_nb()       # raises BlockingIOError if dry
                if not fed:
                    # Peer EOF.  A clean close_notify arrives as DATA (an alert
                    # record), so read() already raised SSLZeroReturnError above
                    # -- reaching here means a BARE FIN with no close_notify and
                    # no buffered record.  _feed_in_nb does NOT write_eof the BIO
                    # (that would poison a concurrent _obj.write() draining our
                    # trailing data), so read() would just keep wanting-read;
                    # surface the EOF directly as b''.
                    return b""
            # want_write path: we pumped; loop to retry the read.

    def recv(self, n):
        # Cooperative parking recv (used by code that owns a dedicated read g).
        if self._closed:
            return b""
        while True:
            try:
                return self.recv_nb(n)
            except BlockingIOError:
                try:
                    _wait_fd(self._fd, _WAIT_READ)
                except Exception:
                    return b""

    def recv_into(self, buffer, nbytes=0):
        n = nbytes if nbytes else len(buffer)
        data = self.recv(n)
        if not data:
            return 0
        buffer[:len(data)] = data
        return len(data)

    def send(self, data):
        # Encrypt `data` into the outgoing BIO, then flush it to the raw socket
        # (parking on WRITE until drained).  Returns the app-byte count consumed.
        if self._closed:
            raise OSError(_errno.EBADF, "TLS socket closed")
        while True:
            with self._lock:
                try:
                    n = self._obj.write(data)
                    break
                except _ssl.SSLWantReadError:
                    pass
                except _ssl.SSLWantWriteError:
                    pass
            # Renegotiation mid-write: flush + feed, then retry the write.
            if not self._pump_out():
                raise ConnectionResetError("Connection lost")
            try:
                self._feed_in_nb()
            except BlockingIOError:
                try:
                    _wait_fd(self._fd, _WAIT_READ)
                except Exception:
                    raise ConnectionResetError("Connection lost")
        if not self._pump_out():
            raise ConnectionResetError("Connection lost")
        return n

    def sendall(self, data):
        view = data if isinstance(data, memoryview) else memoryview(data)
        total = len(view)
        sent = 0
        while sent < total:
            sent += self.send(view[sent:])
        return None

    def setblocking(self, flag):
        pass

    def send_close_notify(self):
        # Best-effort: emit a close_notify alert (unwrap()) and flush it -- so a
        # peer doing a clean ssl.SSLSocket.unwrap() (which waits for ours)
        # completes instead of seeing a bare FIN.  But NOT if the peer already
        # dropped with a bare FIN itself (TCP half-close, no close_notify): the
        # session is being torn down unexpectedly and a peer that did
        # socket.shutdown(SHUT_WR) is now in recv()==b'' -- our close_notify
        # bytes would be read as junk (test_shutdown_timeout_handler_not_set).
        # asyncio's SSLProtocol likewise shuts down cleanly only when the
        # session is healthy or the peer sent its own close_notify first.
        if self._eof_in and not self._peer_close_notify:
            return
        try:
            with self._lock:
                try:
                    self._obj.unwrap()
                except (_ssl.SSLError, OSError, ValueError):
                    pass
            self._pump_out()
        except Exception:
            pass

    def shutdown(self, how):
        try:
            self._raw.shutdown(how)
        except OSError:
            pass

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self._raw.close()
        except OSError:
            pass
        # Drop the SSLObject + SSLContext (and the BIO pair) so they are freed
        # promptly -- asyncio's SSLProtocol releases its sslcontext on
        # connection_lost, so the context dies even though the user's
        # transport<->protocol reference cycle lingers until the GC runs.
        # test_create_connection_memory_leak asserts the client SSLContext is
        # gone via weakref the instant the connection closes (no gc.collect()).
        # recv_nb()/send() already short-circuit on self._closed, so nothing
        # touches _obj after this; the ssl_object/context/_sslobj views just
        # return None on a closed transport, exactly like asyncio.  (Only _obj +
        # _context hold the SSLContext -- the SSLObject keeps it internally -- so
        # those are the ones to drop; the BIO pair holds no context ref.)
        self._obj = None
        self._context = None

    def getpeername(self):
        return self._raw.getpeername()

    def getsockname(self):
        return self._raw.getsockname()

    def getsockopt(self, *a):
        return self._raw.getsockopt(*a)

    def setsockopt(self, *a):
        return self._raw.setsockopt(*a)

    @property
    def family(self):
        return self._raw.family

    @property
    def type(self):
        return self._raw.type

    @property
    def proto(self):
        return self._raw.proto

    @property
    def ssl_object(self):
        return self._obj

    @property
    def context(self):
        return self._context

    @property
    def _ssl_protocol(self):
        # A real (minimal) stand-in for asyncio's SSLProtocol: white-box code
        # and tests read transport._ssl_protocol._sslcontext to recover the
        # SSLContext a connection was made with.
        return _SSLProtocolView(self)

    def __getattr__(self, name):
        if name.startswith("_") or name in ("_raw",):
            raise AttributeError(name)
        return getattr(self._raw, name)

    def __del__(self):
        if not getattr(self, "_closed", True):
            try:
                self._raw.close()
            except Exception:
                pass


class _SSLProtocolView(object):
    """Minimal asyncio-SSLProtocol-compatible view over a _MemoryBIOTLS, so
    transport._ssl_protocol._sslcontext / .ssl_object resolve like asyncio's."""
    __slots__ = ("_tls",)

    def __init__(self, tls):
        self._tls = tls

    @property
    def _sslcontext(self):
        return self._tls._context

    @property
    def _sslobj(self):
        return self._tls._obj

    def _get_extra_info(self, name, default=None):
        return default

    def pause_writing(self):
        # asyncio's SSLProtocol.pause_writing(): hold app writes off the wire.
        # We pause the driving transport's write drain so _write_buf accumulates.
        tr = getattr(self._tls, "_pg_transport", None)
        if tr is not None:
            tr._write_paused = True

    def resume_writing(self):
        tr = getattr(self._tls, "_pg_transport", None)
        if tr is not None and tr._write_paused:
            tr._write_paused = False
            tr._kick_io()   # respawn/wake the io goroutine to flush _write_buf
