"""Small networking helpers shared by the socket-oriented big_100 projects.

Cooperative under monkey.patch() -- recv/send/accept park the goroutine, so
these are ordinary blocking-style helpers that scale to tens of thousands of
goroutines on the M:N scheduler.
"""
import socket


def recv_exact(sock, n):
    """Read exactly n bytes; raise OSError on premature EOF."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise OSError("eof after {0}/{1} bytes".format(len(buf), n))
        buf += chunk
    return bytes(buf)


def recv_until(sock, delim=b"\n", limit=65536):
    """Read until delim (inclusive) or limit; raise on EOF/oversize."""
    buf = bytearray()
    while delim not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            raise OSError("eof waiting for delimiter")
        buf += chunk
        if len(buf) > limit:
            raise OSError("line over limit")
    return bytes(buf)


def listen_tcp(host="127.0.0.1", port=0, backlog=4096, family=socket.AF_INET):
    s = socket.socket(family, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((host, port))
    s.listen(backlog)
    return s


def udp_socket(host="127.0.0.1", port=0, family=socket.AF_INET):
    s = socket.socket(family, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((host, port))
    return s


def start_echo_server(H, host="127.0.0.1"):
    """Bind a TCP echo server, register it for shutdown, spawn its accept
    loop, and return the listening port.  Handlers echo until EOF."""
    srv = listen_tcp(host)
    port = srv.getsockname()[1]
    H.register_close(srv)

    def handler(conn):
        try:
            while True:
                data = conn.recv(65536)
                if not data:
                    break
                conn.sendall(data)
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def accept_loop():
        while H.running():
            try:
                conn, _addr = srv.accept()
            except OSError:
                break
            H.go(handler, conn)

    H.go(accept_loop)
    return port


def udp_recvfrom_timeout(sock, n, timeout_ms):
    """recvfrom with a real timeout under the cooperative scheduler.

    The monkey-patched recvfrom loops on wait_fd forever, so we wait_fd
    ourselves with a timeout first; if it fires we return (None, None),
    otherwise the socket is readable and recvfrom returns immediately.  Each
    UDP socket is owned by one goroutine, so no one else drains it between the
    wait and the read."""
    import runloom_c
    fd = sock.fileno()
    ready = runloom_c.wait_fd(fd, 1, timeout_ms)
    if not (ready & 1):
        return (None, None)
    try:
        return sock.recvfrom(n)
    except (BlockingIOError, InterruptedError):
        return (None, None)


def close_quiet(sock):
    if sock is not None:
        try:
            sock.close()
        except OSError:
            pass
