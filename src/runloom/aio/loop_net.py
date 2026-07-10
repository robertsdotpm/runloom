"""RunloomEventLoop: create_connection/server, unix variants, datagram,
start_tls, sendfile, getaddrinfo/getnameinfo."""
from ._base import *  # noqa: F401,F403  (shared foundation)
from .tls_bio import _MemoryBIOTLS  # noqa: F401
from .tls_wrap import _tls_wrap_client  # noqa: F401
from .transport_datagram import _create_datagram_endpoint  # noqa: F401
from .transport_server import _ProtocolServer  # noqa: F401
from .transport_stream import _StreamTransport, _SSLStreamTransport  # noqa: F401

class _LoopNetMixin(object):
    async def create_datagram_endpoint(self, protocol_factory, **kw):
        return await _create_datagram_endpoint(self, protocol_factory, **kw)

    # ---- subprocesses (thread-backed) ----
    # AbstractEventLoop.subprocess_exec/shell -- asyncio.create_subprocess_exec/
    # _shell route through these.  runloom's netpoll can't portably select() on
    # child stdio pipes (esp. Windows anonymous pipes), so we drive each pipe on
    # its own OS thread and marshal data/exit back onto the loop thread via
    # call_soon_threadsafe -- exactly how run_in_executor already bridges blocking
    # work.  Returns (SubprocessTransport, protocol) like stock asyncio.

    async def create_connection(self, protocol_factory, host=None, port=None, *,
                                ssl=None, family=0, proto=0, flags=0, sock=None,
                                local_addr=None, server_hostname=None,
                                ssl_handshake_timeout=None, **_ignored):
        """Lower-level create_connection.  Returns (transport, protocol).
        Builds a TCP socket + thin Transport over our Stream classes;
        protocol's connection_made / data_received / connection_lost
        get fired."""
        if sock is None:
            infos = _resolve(host, port, family or _socket.AF_UNSPEC,
                             _socket.SOCK_STREAM, proto, flags)
            last_err = None
            for fam, typ, prt, _canon, sa in infos:
                # Init before the try so socket() failing on this entry (e.g.
                # EAFNOSUPPORT for an AAAA record on an IPv6-disabled host)
                # doesn't leave `s` unbound -- the except's s.close() would then
                # raise UnboundLocalError and abort the whole connect, skipping
                # the IPv4 fallback.  asyncio's _connect_sock guards this too.
                s = None
                try:
                    s = _socket.socket(fam, typ, prt)
                    s.setblocking(False)
                    if local_addr is not None:
                        try:
                            s.bind(local_addr)
                        except OSError as exc:
                            raise OSError(
                                exc.errno,
                                "error while attempting to bind on address %r: %s"
                                % (local_addr, (exc.strerror or "").lower())) from None
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
                    if s is not None:
                        try: s.close()
                        except OSError: pass
            if sock is None:
                # Clear last_err as we raise so the propagating exception's
                # traceback frame doesn't keep referencing it (exc -> tb ->
                # this frame -> last_err -> exc).  asyncio breaks the same cycle
                # explicitly; test_open_connection_happy_eyeball_refcycles
                # asserts gc.get_referrers(exc) == [].
                try:
                    raise last_err or OSError("could not connect")
                finally:
                    last_err = None
        else:
            sock.setblocking(False)
        if ssl is not None:
            sock = _tls_wrap_client(sock, ssl, server_hostname, host,
                                    ssl_handshake_timeout)
        protocol = protocol_factory()
        tr_cls = _SSLStreamTransport if ssl is not None else _StreamTransport
        transport = tr_cls(sock, protocol, loop=self)
        return transport, protocol

    async def create_server(self, protocol_factory, host=None, port=None, *,
                            family=_socket.AF_UNSPEC, flags=_socket.AI_PASSIVE,
                            sock=None, backlog=100, ssl=None,
                            reuse_address=None, reuse_port=None,
                            ssl_handshake_timeout=None, start_serving=True,
                            **_ignored):
        if sock is not None:
            sock.setblocking(False)
            socks = [sock]
        else:
            # asyncio binds EVERY address getaddrinfo returns (one socket each),
            # not just the first -- so "localhost" listens on both 127.0.0.1 and
            # ::1.  The old code break'd after the first bind, which left no IPv4
            # socket whenever getaddrinfo sorts IPv6 first (Windows), so callers
            # that look for an AF_INET socket (websockets' get_host_port) failed.
            if host == "" or host is None:
                hosts = [None]
            elif isinstance(host, str):
                hosts = [host]
            else:
                hosts = list(host)
            # asyncio default: SO_REUSEADDR on POSIX only -- on Windows it lets a
            # second bind hijack the port, so it stays off there by default.
            if reuse_address is None:
                reuse_address = (_os.name == "posix" and sys.platform != "cygwin")
            infos = []
            seen = set()
            for hst in hosts:
                for info in _resolve(hst, port, family,
                                     _socket.SOCK_STREAM, 0, flags):
                    fam, typ, prt, _canon, sa = info
                    key = (fam, sa)
                    if key in seen:
                        continue
                    seen.add(key)
                    infos.append(info)
            socks = []
            last_err = None
            completed = False
            try:
                for fam, typ, prt, _canon, sa in infos:
                    try:
                        s = _socket.socket(fam, typ, prt)
                    except OSError:
                        # getaddrinfo can return a family the host can't create
                        # (e.g. AF_INET6 with IPv6 disabled) -- skip it.
                        continue
                    socks.append(s)
                    if reuse_address:
                        s.setsockopt(_socket.SOL_SOCKET,
                                     _socket.SO_REUSEADDR, 1)
                    if reuse_port and hasattr(_socket, "SO_REUSEPORT"):
                        s.setsockopt(_socket.SOL_SOCKET,
                                     _socket.SO_REUSEPORT, 1)
                    # Keep the IPv6 wildcard socket from also grabbing the IPv4
                    # wildcard (dual-stack) and colliding with the AF_INET bind.
                    if (fam == _socket.AF_INET6
                            and hasattr(_socket, "IPPROTO_IPV6")
                            and hasattr(_socket, "IPV6_V6ONLY")):
                        try:
                            s.setsockopt(_socket.IPPROTO_IPV6,
                                         _socket.IPV6_V6ONLY, 1)
                        except OSError:
                            pass
                    s.setblocking(False)
                    try:
                        s.bind(sa)
                    except OSError as e:
                        last_err = OSError(
                            e.errno,
                            "error while attempting to bind on address %r: %s"
                            % (sa, e.strerror))
                        raise last_err
                completed = True
            finally:
                if not completed:
                    for s in socks:
                        _close_sock(s)
            if not socks:
                raise last_err or OSError("could not bind to any address")
        # listen() on EVERY socket -- including a caller-supplied sock= (asyncio's
        # create_server(sock=...) always listens on it).  aiohttp's TestServer
        # pre-binds a socket and hands it over un-listened via SockSite(sock=...);
        # without this it stayed bound-but-not-listening and every client got
        # ECONNREFUSED.  listen() on an already-listening socket is harmless.
        for s in socks:
            s.listen(backlog)
        # cb=None: caller wired up via protocol factory + Transport.
        # We still need an accept loop per socket that builds Transports per conn.
        return _ProtocolServer(socks, protocol_factory, loop=self, ssl_context=ssl,
                               ssl_handshake_timeout=ssl_handshake_timeout,
                               start_serving=start_serving)

    # ---- Unix domain sockets (loop.create_unix_server / _connection) ----
    # The base class raises NotImplementedError; UDS is common for local IPC
    # (uvicorn/gunicorn --uds, database sockets).  Mirror create_server /
    # create_connection with an AF_UNIX socket.
    async def create_unix_server(self, protocol_factory, path=None, *, sock=None,
                                 backlog=100, ssl=None, cleanup_socket=True,
                                 ssl_handshake_timeout=None, start_serving=True,
                                 **_ignored):
        if path is not None and sock is not None:
            raise ValueError(
                "path and sock can not be specified at the same time")
        if sock is None:
            if path is None:
                raise ValueError("path was not specified, and no sock specified")
            sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
            try:
                sock.bind(path)
            except OSError as e:
                sock.close()
                if e.errno == _errno.EADDRINUSE:
                    raise OSError(e.errno, "Address %r is already in use" % (path,))
                raise
            except Exception:
                sock.close()
                raise
        sock.setblocking(False)
        sock.listen(backlog)
        return _ProtocolServer([sock], protocol_factory, loop=self,
                               ssl_context=ssl, cleanup_unix=cleanup_socket,
                               ssl_handshake_timeout=ssl_handshake_timeout,
                               start_serving=start_serving)

    async def create_unix_connection(self, protocol_factory, path=None, *,
                                     ssl=None, sock=None, server_hostname=None,
                                     ssl_handshake_timeout=None, **_ignored):
        if path is not None and sock is not None:
            raise ValueError(
                "path and sock can not be specified at the same time")
        if sock is None:
            if path is None:
                raise ValueError("no path and sock were specified")
            sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
            sock.setblocking(False)
            try:
                try:
                    sock.connect(path)
                except BlockingIOError:
                    _wait_fd(sock.fileno(), 2)
                    err = sock.getsockopt(_socket.SOL_SOCKET, _socket.SO_ERROR)
                    if err != 0:
                        raise OSError(err, "connect failed")
            except BaseException:
                # connect() to a missing/forbidden path fails IMMEDIATELY with
                # FileNotFoundError / PermissionError -- never raising
                # BlockingIOError -- so close the socket on ANY failure (as
                # asyncio's create_unix_connection does), or it leaks and
                # surfaces as ResourceWarning("unclosed <socket ...>").
                sock.close()
                raise
        else:
            sock.setblocking(False)
        if ssl is not None:
            sock = _tls_wrap_client(sock, ssl, server_hostname, None,
                                    ssl_handshake_timeout)
        protocol = protocol_factory()
        tr_cls = _SSLStreamTransport if ssl is not None else _StreamTransport
        transport = tr_cls(sock, protocol, loop=self)
        return transport, protocol

    async def sendfile(self, transport, file, offset=0, count=None, *,
                       fallback=True):
        """asyncio.loop.sendfile.  We have no OS sendfile path, so do the
        portable read+write fallback (asyncio falls back to this too when the
        native path is unavailable).  Used by aiohttp's FileResponse etc.; the
        base class raises NotImplementedError.  Blocking file reads are offloaded
        so they don't wedge the loop."""
        if transport.is_closing():
            raise RuntimeError("Transport is closing")
        if not fallback:
            # Caller demanded the native path, which runloom transports lack.
            raise asyncio.SendfileNotAvailableError(
                "sendfile syscall path is not available on runloom transports")
        if offset:
            await self.run_in_executor(None, file.seek, offset)
        blocksize = 16384
        total = 0
        while True:
            want = blocksize
            if count is not None:
                want = min(blocksize, count - total)
                if want <= 0:
                    break
            data = await self.run_in_executor(None, file.read, want)
            if not data:
                break
            transport.write(data)
            total += len(data)
        return total

    async def start_tls(self, transport, protocol, sslcontext, *,
                        server_side=False, server_hostname=None,
                        ssl_handshake_timeout=None, ssl_shutdown_timeout=None,
                        **_ignored):
        """Upgrade an existing connection to TLS in place (STARTTLS, asyncpg
        SSL).  AbstractEventLoop raises NotImplementedError.  Quiesce the
        plaintext transport's recv loop (without closing the fd), wrap the same
        socket in cooperative TLS, handshake, and return a new transport over
        the TLS socket reusing the SAME protocol (connection_made is not
        re-fired, matching asyncio)."""
        sock = getattr(transport, "_sock", None)
        if sock is None:
            raise TypeError("transport does not expose a socket for start_tls")
        # Stop the plaintext recv loop consuming the fd; it exits WITHOUT
        # closing the socket (TLS takes fd ownership).  Suppress its
        # connection_lost so the protocol stays "connected" across the upgrade.
        transport._paused = True
        transport._stopping = True
        transport._conn_lost_called = True
        transport._closed = True
        # The old io fiber is parked in _wait_fd on this fd; a bare sleep(0)
        # won't wake it, so it would linger parked and STEAL the post-handshake
        # data wakeup meant for the new TLS transport (then exit), stranding the
        # read -> b''.  Cancel its park so it observes _stopping and exits NOW.
        old_g = getattr(transport, "_io_g", None)
        if old_g is not None:
            try:
                old_g.cancel_wait_fd()
            except Exception:
                pass
        await asyncio.sleep(0)   # give the old io loop a turn to observe + exit
        # gh-142352: on the server side the peer's TLS ClientHello may have
        # ALREADY been read off the plaintext socket into the StreamReader's
        # buffer (a server that waits for data before calling start_tls -- see
        # test_streams::test_start_tls_buffered_data).  Those bytes are gone from
        # the socket, so seed them into the handshake's incoming BIO or the
        # server's do_handshake() blocks forever waiting for a ClientHello that
        # already arrived.  Mirror asyncio's base_events.start_tls: pull the
        # StreamReaderProtocol's _stream_reader._buffer and clear it.
        incoming_data = b""
        if server_side:
            stream_reader = getattr(protocol, "_stream_reader", None)
            if stream_reader is not None:
                buffer = getattr(stream_reader, "_buffer", None)
                if buffer:
                    incoming_data = bytes(buffer)
                    buffer.clear()
        tls = _MemoryBIOTLS(sock, sslcontext, server_side=server_side,
                       server_hostname=server_hostname,
                       incoming_data=incoming_data)
        tls.do_handshake(ssl_handshake_timeout)
        # Transfer the accepting server's registration from the old (now
        # quiesced) transport to the new TLS one.  The accepted transport sits in
        # the server's _conns set and its connection_lost would _detach it -- but
        # we suppressed that connection_lost for the upgrade, so without moving
        # the registration the old transport lingers in _conns forever (it is
        # also pinned by its parked io fiber) and the new transport never
        # detaches, so server.wait_closed() blocks for good (the scheduler then
        # drains to empty -> "event loop stopped before Future completed").
        srv = getattr(transport, "_pg_server", None)
        if srv is not None:
            try:
                srv._conns.discard(transport)
            except Exception:
                pass
        new_tr = _StreamTransport(tls, protocol, loop=self,
                                  call_connection_made=False, server=srv)
        if srv is not None:
            try:
                srv._conns.add(new_tr)
            except Exception:
                pass
        return new_tr

    async def connect_accepted_socket(self, protocol_factory, sock, *, ssl=None,
                                      ssl_handshake_timeout=None, **_ignored):
        """Wrap an already-accepted socket into a transport (server side).
        AbstractEventLoop raises NotImplementedError; servers that accept()
        manually (some test harnesses, custom acceptors) hand the socket here."""
        if ssl_handshake_timeout is not None and not ssl:
            raise ValueError(
                "ssl_handshake_timeout is only meaningful with ssl")
        sock.setblocking(False)
        if ssl is not None:
            tls = _MemoryBIOTLS(sock, ssl, server_side=True)
            tls.do_handshake(ssl_handshake_timeout)
            sock = tls
        protocol = protocol_factory()
        transport = _StreamTransport(sock, protocol, loop=self)
        return transport, protocol

    async def getaddrinfo(self, host, port, *, family=0, type=0, proto=0, flags=0):
        # Offloaded to the blocking pool so DNS doesn't wedge the hub.
        # monkey.py may still patch this to a cooperative resolver.
        return _resolve(host, port, family, type, proto, flags)

    async def getnameinfo(self, sockaddr, flags=0):
        # Offloaded to the blocking pool so reverse-DNS doesn't wedge the hub
        # (like getaddrinfo above); a direct _socket.getnameinfo() is a
        # non-preemptible blocking C call that would freeze every task/timer on
        # the loop for the full resolver timeout.  Stock asyncio runs this in the
        # executor.  monkey.py may still patch this to a cooperative resolver.
        return _blocking(_socket.getnameinfo, sockaddr, flags)

    # ---- low-level socket ops (loop.sock_*) ----
