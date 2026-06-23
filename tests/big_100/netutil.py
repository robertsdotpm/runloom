"""Small networking helpers shared by the socket-oriented big_100 projects.

Cooperative under monkey.patch() -- recv/send/accept park the goroutine, so
these are ordinary blocking-style helpers that scale to tens of thousands of
goroutines on the M:N scheduler.
"""
import os
import socket

_DEFAULT_HOST = os.environ.get("SOAK_HOST_IP", "127.0.0.1")

# Save the real (pre-monkey-patch) recvfrom.  udp_recvfrom_timeout calls
# wait_fd with a timeout first, then calls recvfrom; the monkey-patched version
# loops on wait_fd forever on EAGAIN (a spurious wake), so we must call the
# original directly here so a spurious readiness signal doesn't wedge the goroutine.
_real_socket_recvfrom = socket.socket.recvfrom


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


def listen_tcp(host=None, port=0, backlog=4096, family=socket.AF_INET):
    if host is None:
        host = _DEFAULT_HOST
    # A connect storm larger than the aggregate listen backlog (servers *
    # min(backlog, somaxconn)) overflows the kernel SYN queue: excess SYNs are
    # dropped, so those connects park in slow SYN-retransmit backoff (no error,
    # just stalled) and the run can't drain within its time budget -> watchdog
    # HANG.  For high-N runs raise BOTH this and kern.ipc.somaxconn (Linux:
    # net.core.somaxconn) above the peak concurrent connect count.  Default is
    # unchanged; BIG100_BACKLOG overrides it.
    bl = os.environ.get("BIG100_BACKLOG")
    if bl and bl.strip().isdigit():
        backlog = int(bl)
    s = socket.socket(family, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((host, port))
    s.listen(backlog)
    s.setblocking(False)
    return s


def accept_timeout(srv, timeout_ms=200):
    """Cooperative accept that returns None on timeout.

    runloom's close() does NOT wake a goroutine parked in the monkey-patched
    socket.accept() (FINDINGS BUG #5), so a server that relies on closing the
    listener to break its accept loop hangs at teardown.  We instead wait_fd
    with a timeout ourselves and do the RAW non-blocking _accept (unpatched),
    so the loop can re-check running() every timeout_ms and never parks
    without a deadline."""
    import runloom_c
    try:
        fd = srv.fileno()
    except (OSError, ValueError):
        return None
    if fd < 0 or not (runloom_c.wait_fd(fd, 1, timeout_ms) & 1):
        return None
    try:
        cfd, addr = srv._accept()
    except (BlockingIOError, InterruptedError):
        return None
    except OSError:
        return None
    conn = socket.socket(srv.family, srv.type, srv.proto, fileno=cfd)
    conn.setblocking(False)
    return conn, addr


def serve_forever(H, srv, on_conn):
    """Run an accept loop that self-terminates when H.running() goes false
    (no reliance on close() waking accept), spawning on_conn(conn, addr) per
    connection.  Closes srv on exit.

    Edge-triggered DRAIN accept: per readiness wakeup we accept the WHOLE
    pending backlog (loop until EAGAIN) before parking again.  Accepting just
    one connection per wait_fd park caps accept throughput at the netpoll pump
    cadence -- which under a heavy connect storm collapses: the kernel backlog
    fills faster than one-accept-per-wakeup drains it, so thousands of clients
    connect (SYN-ACK from the backlog) but are never accept()ed, pile up parked
    on recv waiting for an echo that never comes, and the run wedges at
    teardown.  Draining the backlog each wakeup lifts accept to the
    accept()-syscall rate (tens of thousands/s) and keeps the pipeline flowing.
    """
    import runloom_c
    try:
        fd = srv.fileno()
    except (OSError, ValueError):
        return
    try:
        while H.running():
            got_one = False
            while H.running():
                try:
                    cfd, addr = srv._accept()
                except (BlockingIOError, InterruptedError):
                    break          # backlog drained -> park below
                except OSError:
                    break
                conn = socket.socket(srv.family, srv.type, srv.proto,
                                     fileno=cfd)
                conn.setblocking(False)
                on_conn(conn, addr)
                got_one = True
            if not got_one:
                # Nothing pending: park until the listen fd is readable, with a
                # 200ms re-probe so running() is re-checked at teardown (close()
                # does not wake a parked accept -- FINDINGS BUG #5).
                if fd < 0:
                    break
                runloom_c.wait_fd(fd, 1, 200)
    finally:
        close_quiet(srv)


def listen_all(H, on_conn, backlog=4096, family=socket.AF_INET):
    """Bind ONE TCP server per H.net_ips IP, register each for shutdown, and
    spawn an accept loop for each.  Returns [(host, port), ...] for clients to
    pick from with pick_server().

    The multi-server pattern: a single accept loop can't drain a large connect
    storm (it serializes), so spreading servers across the loopback IP range
    scales ACCEPT load.  Use --ip-start-offset/--ip-end-offset to dial the
    number of server IPs.  Falls back to one default server if H has no net_ips."""
    ips = getattr(H, "net_ips", None) or [None]
    servers = []
    for ip in ips:
        srv = listen_tcp(host=ip, backlog=backlog, family=family)
        H.register_close(srv)
        name = srv.getsockname()
        H.fiber(serve_forever, H, srv, on_conn)
        servers.append((name[0], name[1]))
    return servers


def pick_server(servers, rng):
    """Pick one (host, port) from a listen_all() list deterministically."""
    return servers[rng.randrange(len(servers))]


def udp_socket(host=None, port=0, family=socket.AF_INET):
    if host is None:
        host = _DEFAULT_HOST
    s = socket.socket(family, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((host, port))
    return s


def start_echo_server(H, host=None):
    """Bind a TCP echo server, register it for shutdown, spawn its accept
    loop, and return the listening port.  Handlers echo until EOF."""
    if host is None:
        host = _DEFAULT_HOST
    srv = listen_tcp(host)
    port = srv.getsockname()[1]

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

    H.fiber(serve_forever, H, srv, lambda conn, addr: H.fiber(handler, conn))
    return port


TIMEOUT = object()      # sentinel returned by recv_line_timeout on no-data


def recv_line_timeout(sock, timeout_ms, buf):
    """Read one newline-terminated line with a timeout, keeping leftover bytes
    in the `buf` bytearray across calls.  Returns the line (without the \\n),
    netutil.TIMEOUT if none arrived in time, or raises OSError on EOF.  Lets a
    single-goroutine client drain a server without a second goroutine parked on
    the fd."""
    import runloom_c
    while b"\n" not in buf:
        if not (runloom_c.wait_fd(sock.fileno(), 1, timeout_ms) & 1):
            return TIMEOUT
        chunk = sock.recv(4096)
        if not chunk:
            raise OSError("eof")
        buf += chunk
    nl = buf.index(b"\n")
    line = bytes(buf[:nl])
    del buf[:nl + 1]
    return line


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
        # Use the real (pre-monkey-patch) recvfrom: the patched version loops
        # on wait_fd with no timeout, so a spurious readiness signal would
        # wedge the goroutine indefinitely.
        return _real_socket_recvfrom(sock, n)
    except (BlockingIOError, InterruptedError):
        return (None, None)


def close_quiet(sock):
    if sock is not None:
        try:
            sock.close()
        except OSError:
            pass
