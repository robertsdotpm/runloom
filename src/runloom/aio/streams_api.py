"""open_connection / start_server + the _Server they return."""
from ._base import *  # noqa: F401,F403  (shared foundation)
from .streams import StreamReader, StreamWriter  # noqa: F401
from .tasks import RunloomTask  # noqa: F401
from .tls_bio import _MemoryBIOTLS  # noqa: F401
from .tls_wrap import _tls_wrap_client  # noqa: F401

async def open_connection(host=None, port=None, *, family=0, proto=0,
                          flags=0, sock=None, local_addr=None,
                          server_hostname=None, ssl=None,
                          ssl_handshake_timeout=None,
                          limit=2**16, **_ignored):
    """Establish a TCP connection and return (reader, writer).

    Mirrors asyncio.open_connection but bypasses Transport/Protocol --
    our Stream classes talk to the socket directly via cooperative
    wait_fd.  TLS is handled by the cooperative _TLSSock wrapper.
    """
    if sock is None:
        if host is None or port is None:
            raise ValueError("open_connection requires host+port or sock=")
        # getaddrinfo is a blocking C call; offload it so it doesn't wedge
        # the hub (aionetiface's monkey patch may also make it cooperative).
        infos = _resolve(host, port,
                         family or _socket.AF_UNSPEC,
                         _socket.SOCK_STREAM,
                         proto, flags)
        last_err = None
        for fam, typ, prt, _canon, sa in infos:
            try:
                s = _socket.socket(fam, typ, prt)
                s.setblocking(False)
                if local_addr is not None:
                    s.bind(local_addr)
                try:
                    s.connect(sa)
                except BlockingIOError:
                    _wait_fd(s.fileno(), 2)
                    err = s.getsockopt(_socket.SOL_SOCKET, _socket.SO_ERROR)
                    if err != 0:
                        raise OSError(err, "connect failed")
                sock = s
                break
            except OSError as e:
                last_err = e
                try: s.close()
                except OSError: pass
        if sock is None:
            raise last_err or OSError("could not connect")
    else:
        sock.setblocking(False)

    if ssl is not None:
        sock = _tls_wrap_client(sock, ssl, server_hostname, host,
                                ssl_handshake_timeout)
    reader = StreamReader(sock, limit=limit)
    writer = StreamWriter(sock, reader=reader)
    return reader, writer


class _Server(object):
    """asyncio.Server compatible: keeps the listening socket alive and
    the accept-loop goroutine running until close() is called."""

    def __init__(self, sock, client_connected_cb, *, limit=2**16,
                 ssl_context=None, ssl_handshake_timeout=None):
        self._sock = sock
        self._cb   = client_connected_cb
        self._limit = limit
        self._ssl_context = ssl_context
        self._ssl_handshake_timeout = ssl_handshake_timeout
        self._closed = False
        self._accept_g = _go_io(self._accept_loop)

    def _accept_loop(self):
        while not self._closed:
            try:
                conn, _addr = self._sock.accept()
            except (BlockingIOError, InterruptedError):
                if self._closed:
                    return
                _wait_fd(self._sock.fileno(), 1)
                continue
            except OSError as e:
                # close() will close the listening socket; the next
                # accept fails with EBADF / EINVAL.  Treat that as the
                # signal to exit cleanly.
                if self._closed:
                    return
                if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK):
                    _wait_fd(self._sock.fileno(), 1)
                    continue
                # Real error -- record and exit.
                self._closed = True
                return
            conn.setblocking(False)
            if self._ssl_context is not None:
                # Handshake off the accept loop so a slow client can't stall it.
                _go_io(lambda c=conn: self._setup_conn_tls(c))
            else:
                self._spawn_conn(conn)

    def _spawn_conn(self, sock):
        reader = StreamReader(sock, limit=self._limit)
        writer = StreamWriter(sock, reader=reader)
        # Build the connection coroutine and drive it directly as a RunloomTask.
        # We're already inside a non-task goroutine (the accept loop or a
        # per-conn TLS goroutine); creating RunloomTask directly here -- the
        # earlier "wrap in runloom_c.go then RunloomTask inside" added a second
        # goroutine spawn for no real benefit.
        coro = self._cb(reader, writer)
        if asyncio.iscoroutine(coro):
            RunloomTask(coro, loop=asyncio.get_event_loop())

    def _setup_conn_tls(self, conn):
        try:
            tls = _MemoryBIOTLS(conn, self._ssl_context, server_side=True)
        except Exception:
            _close_sock(conn)
            return
        try:
            tls.do_handshake(self._ssl_handshake_timeout)
        except Exception:
            _close_sock(tls)
            return
        self._spawn_conn(tls)

    def is_serving(self):
        return not self._closed

    def close(self):
        if self._closed:
            return
        self._closed = True
        # shutdown() before close() wakes any goroutine parked on this
        # fd via wait_fd -- epoll/kqueue/IOCP all signal POLLIN+POLLHUP
        # on the listen socket, which our netpoll routes back to the
        # accept_loop's wait_fd call.  close() alone doesn't reliably
        # wake parked pollers on Linux.
        try:
            self._sock.shutdown(_socket.SHUT_RDWR)
        except OSError:
            pass
        _close_sock(self._sock)

    async def wait_closed(self):
        # Best-effort; we don't currently track outstanding client tasks.
        await asyncio.sleep(0)

    @property
    def sockets(self):
        return (self._sock,) if not self._closed else ()


# ====================================================================
# UDP: DatagramTransport + create_datagram_endpoint.
#
# Datagram socket goroutine: one g per endpoint runs the recv loop,
# delivering each packet to the protocol's datagram_received().
# send_to bypasses the loop entirely -- just non-blocking sendto with
# wait_fd on EAGAIN.
# ====================================================================
# ====================================================================
# _StreamTransport / _ProtocolServer: lower-level Transport+Protocol
# pair used by loop.create_connection / loop.create_server.  Most user
# code uses the StreamReader/Writer high-level path above; these exist
# for libraries (like aionetiface) that consume the protocol API.
# ====================================================================

async def start_server(client_connected_cb, host=None, port=None, *,
                       family=_socket.AF_UNSPEC, flags=_socket.AI_PASSIVE,
                       sock=None, backlog=100, limit=2**16,
                       reuse_address=None, reuse_port=None,
                       ssl=None, ssl_handshake_timeout=None, **_ignored):
    """Listen on host:port and call client_connected_cb(reader, writer)
    per accepted connection.  Returns a _Server with .close() / .sockets.

    Compared to asyncio.start_server, we skip Transport/Protocol but still
    wrap accepted connections in cooperative TLS when ssl= is given."""
    if sock is None:
        infos = _socket.getaddrinfo(host, port, family,
                                    _socket.SOCK_STREAM, 0, flags)
        last_err = None
        for fam, typ, prt, _canon, sa in infos:
            try:
                sock = _socket.socket(fam, typ, prt)
                if reuse_address is not False:
                    sock.setsockopt(_socket.SOL_SOCKET,
                                    _socket.SO_REUSEADDR, 1)
                sock.setblocking(False)
                sock.bind(sa)
                sock.listen(backlog)
                break
            except OSError as e:
                last_err = e
                _close_sock(sock)
                sock = None
        if sock is None:
            raise last_err or OSError("could not bind")
    else:
        sock.setblocking(False)

    return _Server(sock, client_connected_cb, limit=limit, ssl_context=ssl,
                   ssl_handshake_timeout=ssl_handshake_timeout)
