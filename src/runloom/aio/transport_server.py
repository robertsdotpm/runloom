"""_ProtocolServer: the accept loop behind loop.create_server."""
from ._base import *  # noqa: F401,F403  (shared foundation)
from .tls_bio import _MemoryBIOTLS  # noqa: F401
from .transport_stream import _StreamTransport  # noqa: F401

class _ProtocolServer(object):
    """Server compatible with asyncio.Server: per-accept builds a
    _StreamTransport and a protocol via factory."""

    def __init__(self, socks, protocol_factory, *, loop=None, ssl_context=None,
                 ssl_handshake_timeout=None, cleanup_unix=True,
                 start_serving=True):
        # create_server may bind several sockets (one per address family);
        # accept independently on each.  Named _sockets to match asyncio.Server
        # (libraries / tests read srv._sockets); nulled in close().
        self._sockets = list(socks)
        self._factory = protocol_factory
        self._loop = loop
        # asyncio.Server exposes _ssl_context (None when no TLS); libraries
        # (e.g. websockets' test helpers) read it off the server object.  It
        # holds the real SSLContext when create_server was given ssl=.
        self._ssl_context = ssl_context
        self._ssl_handshake_timeout = ssl_handshake_timeout
        self._closed = False
        # Context active when the server was created (inside the awaiting
        # create_server coroutine).  Each accepted connection's transport runs
        # its protocol callbacks in a fresh copy of this -- so a contextvar set
        # before create_server (uvicorn's "context preserved by default")
        # reaches the ASGI task spawned from data_received.  Mirrors asyncio's
        # accept-callback context flowing into each transport's reader Handle.
        self._context = _contextvars.copy_context()
        # Track live client transports so close()/abort_clients() can tear
        # them down.  Without this, stopping the server (e.g. aiosmtpd's
        # Controller.stop()) leaves accepted connections' sockets open with no
        # fiber servicing them -- a peer mid-request (a client between DATA
        # and the terminating dot) then blocks forever waiting for a reply that
        # never comes.  WeakSet so a finished connection's transport is pruned
        # once its recv fiber ends and drops the last reference.
        self._conns = _weakref.WeakSet()
        # asyncio.Server.wait_closed(): block until the server is closed AND
        # every accepted connection has finished.  _waiters is a list while
        # pending; _wakeup() (called when both conditions hold) sets it to None.
        self._waiters = []
        # Unix server sockets bound to a filesystem path: remember (path, inode)
        # so close() unlinks the socket file like asyncio's _unix_server_sockets
        # -- only when the inode still matches (never unlink a file that replaced
        # ours).  Abstract-namespace (\0-prefixed) and unbound sockets are skipped.
        self._unix_paths = []
        if cleanup_unix:
            for s in self._sockets:
                try:
                    if s.family == _socket.AF_UNIX:
                        path = s.getsockname()
                        if isinstance(path, str) and path and not path.startswith("\0"):
                            self._unix_paths.append((path, _os.stat(path).st_ino))
                except OSError:
                    pass
        # asyncio create_server(start_serving=False): bind+listen now, but don't
        # accept until start_serving()/serve_forever().  is_serving() reflects it.
        self._serving = False
        self._accept_gs = []
        if start_serving:
            self._start_accepting()

    def _start_accepting(self):
        if self._serving or self._closed or self._sockets is None:
            return
        self._serving = True
        self._accept_gs = [_fiber_io(lambda s=s: self._accept_loop(s))
                           for s in self._sockets]

    def _accept_loop(self, sock):
        while not self._closed:
            try:
                conn, _addr = sock.accept()
            except (BlockingIOError, InterruptedError):
                if self._closed: return
                try:
                    _wait_fd(sock.fileno(), 1)
                except asyncio.CancelledError:
                    return          # close() woke us to exit (see close())
                continue
            except OSError:
                # One listener erroring stops accepting on it but must not
                # tear down the whole (multi-socket) server.
                return
            conn.setblocking(False)
            if self._ssl_context is not None:
                # Finish the TLS handshake in its own fiber so a slow or
                # stalled client never blocks accepting new connections.
                _fiber_io(lambda c=conn: self._setup_tls_conn(c))
            else:
                try:
                    protocol = self._factory()
                    self._conns.add(_StreamTransport(conn, protocol, loop=self._loop,
                                                     context=self._context, server=self))
                except Exception as exc:
                    # A raising protocol factory (or transport construction) must
                    # NOT kill the accept loop -- otherwise one transient error
                    # turns into a permanent denial of service for this listener.
                    # Route it to the loop exception handler and drop just this
                    # connection, then keep accepting, like asyncio's
                    # selector_events._accept_connection2.
                    loop = (self._loop if self._loop is not None
                            else asyncio.get_event_loop())
                    loop.call_exception_handler({
                        "message": "Error on accepting connection from a client",
                        "exception": exc,
                    })
                    _close_sock(conn)

    def _setup_tls_conn(self, conn):
        try:
            tls = _MemoryBIOTLS(conn, self._ssl_context, server_side=True)
        except Exception:
            _close_sock(conn)
            return
        try:
            tls.do_handshake(self._ssl_handshake_timeout)
        except Exception:
            # Bad cert / SNI / protocol error, or a peer that stalled past
            # ssl_handshake_timeout: drop it quietly, like asyncio's SSL
            # transport does.
            _close_sock(tls)
            return
        protocol = self._factory()
        self._conns.add(_StreamTransport(tls, protocol, loop=self._loop,
                                         context=self._context, server=self))

    def get_loop(self):
        """asyncio.Server.get_loop().  Libraries (websockets) call this on
        the server returned by create_server to schedule cleanup tasks."""
        return self._loop if self._loop is not None else asyncio.get_event_loop()

    def is_serving(self):
        return self._serving and not self._closed

    async def start_serving(self):
        # asyncio.Server.start_serving(): begin accepting (idempotent).  For a
        # server created with start_serving=False this spawns the accept loops.
        self._start_accepting()

    async def serve_forever(self):
        # asyncio.Server.serve_forever(): start accepting if not already, then
        # run until close() (or cancellation of this coroutine) ends it.  On a
        # closed server it raises, like asyncio.
        if self._closed:
            raise RuntimeError("server {0!r} is closed".format(self))
        self._start_accepting()
        try:
            while not self._closed:
                await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            # asyncio.Server.serve_forever() closes the server when its task is
            # cancelled (graceful-shutdown / TaskGroup teardown) -- tear down the
            # listeners and wait for connections to drain before re-raising, so
            # the port stops accepting instead of staying bound with no server.
            try:
                self.close()
                await self.wait_closed()
            finally:
                raise

    def close(self):
        if self._closed: return
        self._closed = True
        self._serving = False
        # Wake the accept-loop fibers parked in _wait_fd so they observe
        # _closed and EXIT.  Without this they stay parked forever on the
        # listening fd we're about to close (no readability event ever comes) --
        # a fiber leak that accumulates one-per-server in a long-lived loop
        # that opens many servers (a test suite's per-test loop reset hid it).
        for g in self._accept_gs:
            try: g.cancel_wait_fd()
            except Exception: pass
        self._accept_gs = []
        # asyncio.Server.close() ONLY stops the listeners; established
        # connections keep running until they finish (or are closed explicitly
        # via close_clients()/abort_clients(), or cancelled when the loop ends).
        # Closing client transports here breaks callers that close() the server
        # and THEN message the live connections -- e.g. uvicorn's graceful
        # shutdown closes the server, then sends each websocket a 1012 close
        # frame; if we'd already torn the transport down that frame is dropped
        # and the peer sees an abnormal 1006 close.  (We used to close clients
        # here to dodge the cancel-can't-interrupt-wait_fd hang; that's fixed in
        # the C core now, so the recv fibers get cleaned up on loop teardown.)
        for sock in self._sockets:
            try: sock.shutdown(_socket.SHUT_RDWR)
            except OSError: pass
            _close_sock(sock)
        # asyncio.Server nulls _sockets on close (the public `sockets` property
        # then returns ()); tests assert `srv._sockets is None` afterward.
        self._sockets = None
        # Unlink unix server socket files (inode-checked), like asyncio's
        # _UnixSelectorEventLoop._stop_serving -- test_unix_server_addr_cleanup
        # asserts os.path.exists(addr) is False right after close().
        for path, ino in self._unix_paths:
            try:
                if _os.stat(path).st_ino == ino:
                    _os.unlink(path)
            except FileNotFoundError:
                pass
            except OSError:
                pass
        self._unix_paths = []
        # If no connections are live, the "closed AND drained" condition holds
        # now -- wake wait_closed() waiters.  Otherwise the last _detach() will.
        if not self._conns:
            self._wakeup()

    def _detach(self, transport):
        # Called by an accepted connection's transport when it finishes.  Once
        # the server is closed and the last connection drops, wake wait_closed().
        self._conns.discard(transport)
        if self._closed and not self._conns:
            self._wakeup()

    def _wakeup(self):
        waiters = self._waiters
        if waiters is None:
            return
        self._waiters = None
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    def close_clients(self):
        # asyncio 3.13+ API: gracefully close all client connections.
        for tr in list(self._conns):
            try:
                tr.close()
            except Exception:
                pass

    def abort_clients(self):
        # asyncio 3.13+ API: abort (our close() already does an immediate
        # shutdown + connection_lost, so it doubles as abort).
        for tr in list(self._conns):
            try:
                tr.abort()
            except Exception:
                pass

    async def wait_closed(self):
        # Block until the server is closed AND every connection has dropped, in
        # either order (asyncio.Server.wait_closed).  _waiters is None only once
        # _wakeup() has fired, i.e. both conditions already hold.
        if self._waiters is None:
            return
        loop = self._loop if self._loop is not None else asyncio.get_event_loop()
        waiter = loop.create_future()
        self._waiters.append(waiter)
        await waiter

    @property
    def sockets(self):
        if self._closed or self._sockets is None:
            return ()
        # Expose asyncio.trsock.TransportSocket wrappers (read-only introspection
        # views), like stock asyncio.Server.sockets -- not the raw listeners.
        return tuple(asyncio.trsock.TransportSocket(s) for s in self._sockets)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.close()
        await self.wait_closed()
