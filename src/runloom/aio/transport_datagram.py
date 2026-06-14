"""DatagramTransport + _create_datagram_endpoint (UDP)."""
from ._base import *  # noqa: F401,F403  (shared foundation)

class DatagramTransport(object):
    """asyncio.DatagramTransport-compatible transport.

    Wires a UDP socket to a user-supplied DatagramProtocol.  The
    protocol's datagram_received(data, addr) / error_received(exc) /
    connection_lost(exc) methods are called from our recv fiber.
    """

    def __init__(self, sock, protocol, *, loop=None):
        self._sock = sock
        self._protocol = protocol
        self._loop = loop
        self._closed = False
        # Tells the recv loop to bail on next iteration.
        self._stopping = False
        # connection_made fires before any recv work.
        try:
            protocol.connection_made(self)
        except Exception as e:
            self._report(e, "connection_made")
        # Spawn the recv loop.
        self._recv_g = _go_io(self._recv_loop)

    def _recv_loop(self):
        sock = self._sock
        while not self._stopping:
            try:
                data, addr = sock.recvfrom(65536)
            except (BlockingIOError, InterruptedError):
                if self._stopping: return
                try:
                    _wait_fd(sock.fileno(), 1)
                except Exception:
                    return
                continue
            except OSError as e:
                if self._stopping: return
                if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK):
                    _wait_fd(sock.fileno(), 1)
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
        try:
            if addr is None:
                self._sock.send(data)
            else:
                self._sock.sendto(data, addr)
        except (BlockingIOError, InterruptedError):
            # UDP send rarely blocks, but if it does we just drop.
            # asyncio's selector loop does the same (best-effort).
            pass
        except OSError as e:
            try:
                self._protocol.error_received(e)
            except Exception as e2:
                self._report(e2, "error_received")

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._stopping = True
        # Wake the recv goroutine parked in _wait_fd so it observes _stopping and
        # exits, instead of staying parked forever on the fd we're about to close
        # (epoll/kqueue auto-remove on close emit NO event) -- a deterministic
        # per-endpoint fiber + fd-registration leak.  Mirrors _Server.close() and
        # the "Server close() must wake its accept-loop goroutines" invariant
        # (audit finding B4).
        g = self._recv_g
        self._recv_g = None
        if g is not None:
            try:
                g.cancel_wait_fd()
            except Exception:
                pass
        _close_sock(self._sock)
        try:
            self._protocol.connection_lost(None)
        except Exception as e:
            self._report(e, "connection_lost")

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
    if sock is None:
        if local_addr is None and remote_addr is None:
            family = family or _socket.AF_INET
        if family == 0:
            family = _socket.AF_INET
        sock = _socket.socket(family, _socket.SOCK_DGRAM, proto)
        sock.setblocking(False)
        if reuse_address:
            sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
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
