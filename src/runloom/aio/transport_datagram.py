"""DatagramTransport + _create_datagram_endpoint (UDP)."""
from ._base import *  # noqa: F401,F403  (shared foundation)

class DatagramTransport(asyncio.DatagramTransport):
    """asyncio.DatagramTransport-compatible transport.

    Wires a UDP socket to a user-supplied DatagramProtocol.  The
    protocol's datagram_received(data, addr) / error_received(exc) /
    connection_lost(exc) methods are called from our recv fiber.

    Subclasses asyncio.DatagramTransport (not plain object) so
    isinstance(tr, asyncio.DatagramTransport)/asyncio.BaseTransport
    type-dispatch in libraries succeeds, exactly as under stock asyncio.
    """

    def __init__(self, sock, protocol, *, loop=None):
        # Seat the asyncio.BaseTransport state (self._extra) so this is a
        # genuine asyncio.DatagramTransport and the inherited base helpers work.
        super().__init__()
        self._sock = sock
        self._protocol = protocol
        self._loop = loop
        self._closed = False
        # Tells the recv loop to bail on next iteration.
        self._stopping = False
        # Outbound datagrams queued when the socket send buffer is full, drained
        # by the flush fiber (self._send_g) on writability -- see sendto().
        self._send_buf = _collections.deque()
        self._send_g = None
        # connection_made fires before any recv work.
        try:
            protocol.connection_made(self)
        except Exception as e:
            self._report(e, "connection_made")
        # Spawn the recv loop.
        self._recv_g = _fiber_io(self._recv_loop)

    def _recv_loop(self):
        sock = self._sock
        while not self._stopping:
            try:
                data, addr = sock.recvfrom(65536)
            except (BlockingIOError, InterruptedError):
                if self._stopping: return
                # _wait_fd raises asyncio.CancelledError at shutdown -- a
                # BaseException, NOT Exception -- so catch BaseException to stop
                # this background recv fiber cleanly instead of letting it escape
                # as an "exception ignored in _recv_loop" unraisable warning.
                try:
                    _wait_fd(sock.fileno(), 1)
                except BaseException:
                    return
                continue
            except OSError as e:
                if self._stopping: return
                if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK):
                    try:
                        _wait_fd(sock.fileno(), 1)   # same shutdown-cancel guard
                    except BaseException:
                        return
                    continue
                # Error -- notify protocol and stop.
                try:
                    self._protocol.error_received(e)
                except Exception as e2:
                    self._report(e2, "error_received")
                return
            try:
                self._protocol.datagram_received(data, addr)
            except Exception as e:
                self._report(e, "datagram_received")

    def sendto(self, data, addr=None):
        if self._closed:
            return
        if self._send_buf:
            # A flush fiber is already draining a backlog; queue behind it so
            # datagrams leave the socket in submission order.  bytes() so a
            # caller that reuses its buffer isn't corrupted while queued.
            self._send_buf.append((bytes(data), addr))
            return
        try:
            if addr is None:
                self._sock.send(data)
            else:
                self._sock.sendto(data, addr)
            return
        except (BlockingIOError, InterruptedError):
            # Send buffer full: queue and flush on writability instead of
            # dropping the datagram (matches asyncio's _SelectorDatagramTransport,
            # which buffers and retries from _sendto_ready).
            pass
        except OSError as e:
            try:
                self._protocol.error_received(e)
            except Exception as e2:
                self._report(e2, "error_received")
            return
        self._send_buf.append((bytes(data), addr))
        if self._send_g is None:
            self._send_g = _fiber_io(self._send_loop)

    def _send_loop(self):
        # Drain queued datagrams as the socket becomes writable, mirroring the
        # recv loop's shutdown-cancel discipline: a close()/cancel_wait_fd during
        # the park raises a BaseException out of _wait_fd, which ends the fiber
        # cleanly instead of leaking it (and its fd registration) on a closed fd.
        sock = self._sock
        try:
            while self._send_buf and not self._closed:
                try:
                    _wait_fd(sock.fileno(), 2)   # 2 == wait-for-writable
                except BaseException:
                    return
                while self._send_buf and not self._closed:
                    data, addr = self._send_buf[0]
                    try:
                        if addr is None:
                            sock.send(data)
                        else:
                            sock.sendto(data, addr)
                    except (BlockingIOError, InterruptedError):
                        break   # still full -- re-park on writability
                    except OSError as e:
                        self._send_buf.popleft()
                        try:
                            self._protocol.error_received(e)
                        except Exception as e2:
                            self._report(e2, "error_received")
                        continue
                    self._send_buf.popleft()
        finally:
            self._send_g = None

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._stopping = True
        # Wake the recv fiber parked in _wait_fd so it observes _stopping and
        # exits, instead of staying parked forever on the fd we're about to close
        # (epoll/kqueue auto-remove on close emit NO event) -- a deterministic
        # per-endpoint fiber + fd-registration leak.  Mirrors _Server.close() and
        # the "Server close() must wake its accept-loop fibers" invariant
        # (audit finding B4).
        g = self._recv_g
        self._recv_g = None
        if g is not None:
            try:
                g.cancel_wait_fd()
            except Exception:
                pass
        # Same wake for the send-flush fiber if one is parked on writability, so
        # it observes _closed and exits instead of leaking on the closed fd.
        sg = self._send_g
        self._send_g = None
        if sg is not None:
            try:
                sg.cancel_wait_fd()
            except Exception:
                pass
        _close_sock(self._sock)
        try:
            self._protocol.connection_lost(None)
        except Exception as e:
            self._report(e, "connection_lost")

    def abort(self):
        # asyncio.DatagramTransport.abort(): immediate teardown, discarding any
        # datagrams still queued for send (no graceful drain).  close() already
        # tears down inline and cancels the flush fiber; drop the backlog first
        # so nothing is left to send.
        self._send_buf.clear()
        self.close()

    def is_closing(self):
        return self._closed

    def get_extra_info(self, name, default=None):
        if name == "socket":
            return self._sock
        if name == "sockname":
            try:
                return self._sock.getsockname()
            except OSError:
                return default
        if name == "peername":
            try:
                return self._sock.getpeername()
            except OSError:
                return default
        return default

    def get_protocol(self):
        return self._protocol

    def set_protocol(self, protocol):
        self._protocol = protocol

    def _report(self, exc, where):
        if self._loop is not None:
            self._loop.call_exception_handler({
                "message": "Datagram " + where + " raised",
                "exception": exc,
            })



async def _create_datagram_endpoint(loop, protocol_factory, local_addr=None,
                                    remote_addr=None, family=0, proto=0,
                                    flags=0, reuse_address=None,
                                    reuse_port=None, allow_broadcast=None,
                                    sock=None):
    """Implementation of loop.create_datagram_endpoint."""
    # SO_REUSEADDR on a UDP socket lets a co-resident process bind the same
    # port and hijack traffic, so asyncio removed reuse_address support for
    # datagram endpoints (bpo-37228).  Refuse it instead of silently enabling
    # the port-hijack hole; mirror stock CPython's message.
    if reuse_address:
        raise ValueError("Passing `reuse_address=True` is no longer supported, "
                         "as the usage of SO_REUSEPORT in UDP poses a "
                         "significant security concern.")
    if sock is None:
        if local_addr is None and remote_addr is None:
            family = family or _socket.AF_INET
        if family == 0:
            family = _socket.AF_INET
        sock = _socket.socket(family, _socket.SOCK_DGRAM, proto)
        sock.setblocking(False)
        if reuse_port and hasattr(_socket, "SO_REUSEPORT"):
            sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEPORT, 1)
        if allow_broadcast:
            sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_BROADCAST, 1)
        if local_addr is not None:
            sock.bind(local_addr)
        if remote_addr is not None:
            try:
                sock.connect(remote_addr)
            except BlockingIOError:
                _wait_fd(sock.fileno(), 2)
    else:
        sock.setblocking(False)

    protocol = protocol_factory()
    transport = DatagramTransport(sock, protocol, loop=loop)
    return transport, protocol
