"""_StreamTransport: construction, close/abort, flow-control and config."""
from ._base import *  # noqa: F401,F403  (shared foundation)
from .transport_stream_io import _StreamIOMixin  # noqa: F401

class _StreamTransport(_StreamIOMixin, asyncio.Transport):
    """Thin TCP transport over a socket.  Drives the protocol's
    data_received via a recv goroutine; transports its write() through
    cooperative sendall."""

    def __init__(self, sock, protocol, *, loop=None, call_connection_made=True,
                 context=None, server=None):
        # The _ProtocolServer that accepted this connection (None for client
        # transports), so connection_lost can _detach() it and let the server's
        # wait_closed() complete once every connection has dropped.
        self._pg_server = server
        # Per-connection contextvars Context.  Stock asyncio runs a transport's
        # protocol callbacks (connection_made / data_received / eof_received /
        # connection_lost) inside the context captured when its reader Handle
        # was registered -- i.e. the context active in create_server's accept
        # callback (or create_connection's caller).  runloom's recv goroutine
        # otherwise runs them in the bare scheduler context, so any contextvar
        # set before the server/connection was created (request-id middleware,
        # uvicorn's "context preserved by default") is invisible inside the
        # ASGI task that data_received spawns.  Capture a fresh copy here (each
        # connection independent, matching asyncio's per-transport copy_context)
        # and run every protocol callback through it.
        if context is not None:
            self._context = context.run(_contextvars.copy_context)
        else:
            self._context = _contextvars.copy_context()
        # Populate the asyncio.Transport _extra dict so the INHERITED
        # get_extra_info works -- libraries read these and tests
        # @patch("asyncio.Transport.get_extra_info"), which only intercepts
        # when we don't shadow it with our own method.
        extra = {"socket": sock}
        try: extra["sockname"] = sock.getsockname()
        except OSError: pass
        try: extra["peername"] = sock.getpeername()
        except OSError: pass
        ssl_obj = getattr(sock, "ssl_object", None)
        if ssl_obj is not None:
            extra["ssl_object"] = ssl_obj
            extra["sslcontext"] = ssl_obj.context
            try: extra["peercert"] = ssl_obj.getpeercert()
            except Exception: pass
            try: extra["cipher"] = ssl_obj.cipher()
            except Exception: pass
        super().__init__(extra=extra)
        self._sock = sock
        # asyncio enables TCP_NODELAY (Nagle off) on every TCP stream transport
        # -- _SelectorSocketTransport calls _set_nodelay in __init__.  Without it
        # a small write (e.g. a websocket ping frame) sits in the send buffer
        # until the idle peer's delayed ACK (up to ~40 ms), stalling
        # request/response and keepalive round-trips that stock asyncio completes
        # in microseconds.  Mirror asyncio's _set_nodelay exactly: TCP sockets
        # only (AF_INET/AF_INET6 + SOCK_STREAM + IPPROTO_TCP); never AF_UNIX.
        if (sock.family in (_socket.AF_INET, _socket.AF_INET6) and
                sock.type == _socket.SOCK_STREAM and
                sock.proto == _socket.IPPROTO_TCP):
            try:
                sock.setsockopt(_socket.IPPROTO_TCP, _socket.TCP_NODELAY, 1)
            except OSError:
                pass
        self._protocol = protocol
        self._loop = loop
        self._closed = False
        self._stopping = False
        self._paused = False        # pause_reading() flow control
        self._read_eof = False      # peer half-closed: stop reading, keep writing
        self._eof_written = False   # write_eof() done -> write() must raise
        self._eof_pending = False   # write_eof() requested, buffer not yet drained
        self._tls_shutdown_sent = False  # answered a peer close_notify with ours
        self._conn_lost_called = False  # connection_lost fires exactly once
        self._in_context = False    # re-entrancy guard for _run_cb (see below)
        # ---- write buffering (single ordered buffer, drained by the ONE io
        # goroutine) ----  runloom's netpoll arms one direction per fd one-shot, so
        # a separate write goroutine parking EPOLLOUT would clobber the recv
        # goroutine's EPOLLIN arm and strand the read under full-duplex
        # backpressure (verified).  So recv AND drain share a single goroutine
        # that parks on the UNION mask, exactly like add_reader/add_writer's
        # _pg_io_runner.  write() appends here and kicks that goroutine.
        self._write_buf = bytearray()
        self._protocol_paused = False   # did we call protocol.pause_writing()?
        # Explicit write-side flow control: transport._ssl_protocol.pause_writing()
        # (white-box TLS tests, e.g. test_ssl test_flush_before_shutdown) stops
        # the io goroutine from draining _write_buf so app writes accumulate;
        # resume_writing() flushes.  A close() always clears it (a graceful close
        # must flush the buffer regardless).  Distinct from _paused (read side).
        self._write_paused = False
        self._high_water = 64 * 1024
        self._low_water = 16 * 1024
        # Let the TLS layer's _SSLProtocolView reach back to us for the
        # pause_writing()/resume_writing() flow-control surface asyncio's
        # SSLProtocol exposes.  Only MemoryBIO TLS socks accept the attribute;
        # plaintext sockets don't (and have no _ssl_protocol anyway).
        try:
            sock._pg_transport = self
        except (AttributeError, TypeError):
            pass
        # Graceful close: close() with data still queued flushes the buffer
        # before tearing down (asyncio semantics -- a write()+close() must not
        # drop the write).  The io goroutine's _drain_step fires the teardown
        # once the buffer empties.
        self._close_when_drained = False
        self._close_exc = None
        self._close_deliver_cl = False
        # A plaintext non-blocking socket's send() never parks, so write() can
        # fast-path an immediate send from the caller's goroutine.  A _TLSSock
        # send() can park EPOLLOUT (and clobber the recv arm), so TLS writes
        # ALWAYS go through the buffer + single io goroutine.
        self._sock_is_plain = getattr(sock, "ssl_object", None) is None
        # _io_g must exist BEFORE connection_made: a protocol that WRITES inside
        # connection_made (e.g. aiocoap sends its initial CSM, SMTP its greeting)
        # reaches _kick_io, which reads self._io_g.  Over TLS every write goes
        # through the buffer + io goroutine (a _TLSSock send can park EPOLLOUT),
        # so the write can't fast-path -- it kicks, and an undefined _io_g raised
        # AttributeError.  Seed it None; _kick_io then SPAWNS the io goroutine,
        # and the post-connection_made spawn below becomes a no-op (must not
        # double-spawn -- two io goroutines on one fd corrupt the netpoll arm).
        self._io_g = None
        # start_tls reuses an already-connected protocol, so it suppresses the
        # re-fire (asyncio doesn't call connection_made again on TLS upgrade).
        if call_connection_made:
            try:
                self._run_cb(protocol.connection_made, self)
            except Exception as e:
                self._report(e, "connection_made")
        if self._io_g is None:
            self._io_g = _go_io(self._io_loop)


    def close(self, exc=None):
        if self._closed:
            return
        # _closed marks the transport closing: is_closing() True, further
        # write()s dropped, the io loop stops READing.  But DON'T tear down yet
        # if a graceful close still has queued output -- flush it first.
        self._closed = True
        # A graceful close must flush queued output even if writing was paused
        # via _ssl_protocol.pause_writing() -- asyncio drains the buffer before
        # connection_lost.  Lift the pause so the io goroutine can drain.
        self._write_paused = False
        deliver_cl = not self._conn_lost_called
        if deliver_cl:
            self._conn_lost_called = True
        if exc is None and self._write_buf:
            # Graceful close with data queued: let the io goroutine drain the
            # buffer; its _drain_step fires the teardown (and our close_notify)
            # once empty (asyncio flushes the write buffer before
            # connection_lost).
            self._close_exc = None
            self._close_deliver_cl = deliver_cl
            self._close_when_drained = True
            self._kick_io()
            return
        # Graceful close with nothing queued: send our TLS close_notify before
        # the FIN so a peer blocked in unwrap() completes (see helper).  Only on
        # a clean close -- an error/abort close (exc set) skips it.
        if exc is None:
            self._send_tls_close_notify()
        # Error/abort close, or nothing queued: tear down now.
        # asyncio closes the fd inside the DEFERRED _call_connection_lost, NOT
        # synchronously here.  Code routinely reads the socket right after
        # transport.close() -- e.g. aiohttp's fingerprint-mismatch path does
        # transport.close() then transport.get_extra_info("socket").getpeername()
        # to drop the bad peer -- so closing the fd synchronously gives them
        # EBADF (and the resulting OSError masks the ServerFingerprintMismatch
        # they expect).  Defer the shutdown+close to the loop turn that delivers
        # connection_lost.
        self._stopping = True
        # Wake the io goroutine if parked so it sees _stopping and exits now.
        g = self._io_g
        if g is not None:
            try:
                g.cancel_wait_fd()
            except Exception:
                pass
        self._deliver_connection_lost(exc, deliver_cl)

    def _deliver_connection_lost(self, exc, deliver_cl=True):
        # Schedule connection_lost (if not already delivered) AND the socket
        # shutdown+close on the loop in this connection's context, NEVER inline
        # -- exactly like asyncio's _call_connection_lost via call_soon, which
        # also closes self._sock only after connection_lost.  Deferring matters
        # on EOF: the recv loop may have just delivered the peer's final bytes
        # (e.g. a websocket Close frame) to data_received, waking the protocol's
        # reader task; that task must run and consume them BEFORE connection_lost
        # (or the protocol reports an abnormal close), and the fd must stay valid
        # until this turn so a post-close() getpeername() doesn't hit EBADF.
        def _close_sock_now():
            try:
                self._sock.shutdown(_socket.SHUT_RDWR)
            except OSError:
                pass
            _close_sock(self._sock)
        def _detach_server():
            # Let the accepting server's wait_closed() learn this connection is
            # gone (asyncio calls server._detach from connection_lost).
            srv = self._pg_server
            if srv is not None and deliver_cl:
                try:
                    srv._detach(self)
                except Exception:
                    pass
        def _deliver():
            if deliver_cl:
                try:
                    self._protocol.connection_lost(exc)
                except Exception as e:
                    self._report(e, "connection_lost")
            _close_sock_now()
            _detach_server()
            # Release the SSL references the extra dict holds (the SSLObject and
            # SSLContext stored at construction for get_extra_info).  asyncio's
            # SSLProtocol drops its sslcontext on connection_lost, so the context
            # dies even though the user's transport<->protocol reference cycle
            # lingers until the GC runs (test_create_connection_memory_leak
            # asserts the client SSLContext is gone via weakref the instant the
            # connection closes -- no gc.collect()).  _MemoryBIOTLS.close()
            # already cleared its own _obj/_context; these are the only other
            # strong refs.
            extra = getattr(self, "_extra", None)
            if extra:
                for key in ("ssl_object", "sslcontext", "peercert", "cipher"):
                    extra.pop(key, None)
        loop = self._loop if self._loop is not None else asyncio.get_event_loop()
        try:
            loop.call_soon(_deliver, context=self._context)
        except RuntimeError:
            # Loop already closed: best-effort inline so done-futures resolve.
            if deliver_cl:
                try:
                    self._run_cb(self._protocol.connection_lost, exc)
                except Exception as e:
                    self._report(e, "connection_lost")
            _close_sock_now()
            _detach_server()

    def is_closing(self):
        return self._closed

    # get_extra_info is inherited from asyncio.Transport (returns
    # self._extra.get(name, default), populated in __init__) so it stays
    # asyncio-compatible and patchable via asyncio.Transport.get_extra_info.

    def get_protocol(self):
        return self._protocol

    def set_protocol(self, protocol):
        self._protocol = protocol

    @property
    def _sslcontext(self):
        # White-box compat: code/tests read a transport's SSLContext via the
        # private _sslcontext (asyncio's _SSLProtocolTransport attribute).
        # aiohttp's test_tcp_connector_do_not_raise_connector_ssl_error asserts
        # `transport._sslcontext is client_ssl_ctx` to verify the connector
        # reuses the caller's context.  Surface the context the _TLSSock wrapped
        # the socket with (None for a plaintext transport, like stock asyncio).
        obj = getattr(self._sock, "ssl_object", None)
        return obj.context if obj is not None else None

    @property
    def _ssl_protocol(self):
        # White-box compat: asyncio's _SSLProtocolTransport exposes the
        # SSLProtocol as `_ssl_protocol`; code/tests read
        # transport._ssl_protocol._sslcontext (aiohttp's
        # test_tcp_connector_do_not_raise_connector_ssl_error).  Delegate to the
        # MemoryBIO TLS layer's view.  Plaintext transports have no
        # _ssl_protocol -- raise AttributeError, exactly like asyncio.
        sp = getattr(self._sock, "_ssl_protocol", None)
        if sp is None:
            raise AttributeError("_ssl_protocol")
        return sp

    # ---- flow control (read side) ----
    def pause_reading(self):
        self._paused = True

    def resume_reading(self):
        if not self._paused:
            return
        self._paused = False
        # The io goroutine may have exited (mask 0) or be parked WRITE-only;
        # kick it so READ re-enters its interest mask.
        self._kick_io()

    def is_reading(self):
        return not self._paused and not self._closed

    # ---- abort / half-close ----
    def abort(self):
        # Immediate teardown: discard any queued output so close() tears down
        # now instead of draining (asyncio's abort() drops the write buffer).
        self._write_buf = bytearray()
        self.close()

    def can_write_eof(self):
        return True

    def write_eof(self):
        if self._closed or self._eof_written or self._eof_pending:
            return
        if self._write_buf:
            # Defer the half-close until the buffer drains (asyncio flushes the
            # write buffer before shutting the write side); the io goroutine's
            # _drain_step does the SHUT_WR once the buffer empties.
            self._eof_pending = True
            return
        self._eof_written = True
        try:
            self._sock.shutdown(_socket.SHUT_WR)
        except OSError:
            pass

    # ---- write-buffer flow control ----  A single ordered buffer drained by
    # the io goroutine, with real high/low watermarks driving the protocol's
    # pause_writing/resume_writing (so a slow peer applies backpressure instead
    # of unbounded memory growth) and an accurate get_write_buffer_size.
    def set_write_buffer_limits(self, high=None, low=None):
        if high is None:
            high = 4 * low if low is not None else 64 * 1024
        if low is None:
            low = high // 4
        if not high >= low >= 0:
            raise ValueError(
                "high (%r) must be >= low (%r) must be >= 0" % (high, low))
        self._high_water = high
        self._low_water = low
        self._maybe_pause_writing()
        self._maybe_resume_writing()

    def get_write_buffer_limits(self):
        return (self._low_water, self._high_water)

    def get_write_buffer_size(self):
        return len(self._write_buf)

    def _test__append_write_backlog(self, data):
        # asyncio's _SSLProtocolTransport exposes this test-only hook (see
        # sslproto.py) to QUEUE data without an immediate flush -- simulating a
        # filled write backlog so tests can exercise trailing-data delivery
        # after a remote shutdown.  With our single ordered write buffer it maps
        # cleanly: append (preserving order) and let the io goroutine drain it.
        if not data:
            return
        self._write_buf += bytes(data)
        self._maybe_pause_writing()
        self._kick_io()

    def _report(self, exc, where):
        if self._loop is not None:
            self._loop.call_exception_handler({
                "message": "StreamTransport " + where + " raised",
                "exception": exc,
            })
