"""mnweb -- a micro HTTP framework built purely on runloom's M:N sync API.

No async/await, no event loop ceremony, no monkey-patching.  You write
straight-line blocking-looking handlers; every connection is a goroutine
spawned with ``runloom_c.mn_fiber`` and scheduled across N hub threads (one
per core) with the GIL off (free-threaded 3.13t).

The only runloom primitives used:

    runloom_c.mn_init / mn_fiber / mn_run / mn_fini   -- the M:N scheduler
    runloom_c.wait_fd(fd, events, timeout_ms)      -- cooperative readiness
    runloom_c.Chan / select                        -- channels
    runloom.sync.Lock                              -- cooperative mutex

A handler is ``def handler(req) -> Response`` (or returns a str / bytes /
(status, body) tuple, which gets coerced to a Response).

Usage::

    app = App()

    @app.route("/")
    def index(req):
        return "hello"

    app.run("127.0.0.1", 8080, hubs=4)
"""
import socket
import sys
import time
import traceback

import runloom_c
import runloom.sync as sync

READ = 1   # wait_fd events bit: readable
WRITE = 2  # wait_fd events bit: writable

# Reason phrases for the few statuses we emit.
REASONS = {
    200: "OK", 201: "Created", 204: "No Content",
    400: "Bad Request", 404: "Not Found", 405: "Method Not Allowed",
    408: "Request Timeout", 413: "Payload Too Large",
    500: "Internal Server Error", 503: "Service Unavailable",
}


class HTTPError(Exception):
    """Raise from a handler to return a specific status."""
    def __init__(self, status, message=""):
        super().__init__(message or REASONS.get(status, "Error"))
        self.status = status
        self.message = message or REASONS.get(status, "Error")


class CoSock:
    """A cooperative non-blocking socket built directly on wait_fd.

    recv/sendall/accept park the goroutine on the netpoll instead of
    blocking the hub thread.  recv takes an optional timeout so idle
    keep-alive connections get reaped instead of pinning a goroutine
    forever.
    """

    def __init__(self, sock):
        self.sock = sock
        self.sock.setblocking(False)
        self.fd = sock.fileno()

    def accept(self):
        while True:
            try:
                conn, addr = self.sock.accept()
            except (BlockingIOError, InterruptedError):
                runloom_c.wait_fd(self.fd, READ)
                continue
            return CoSock(conn), addr

    def recv(self, n, timeout_ms=-1):
        while True:
            try:
                return self.sock.recv(n)
            except (BlockingIOError, InterruptedError):
                ready = runloom_c.wait_fd(self.fd, READ, timeout_ms)
                if ready == 0:
                    raise TimeoutError("recv timed out")

    def sendall(self, data):
        view = memoryview(data)
        sent = 0
        total = len(view)
        while sent < total:
            try:
                sent += self.sock.send(view[sent:])
            except (BlockingIOError, InterruptedError):
                runloom_c.wait_fd(self.fd, WRITE)

    def close(self):
        fd = -1
        try:
            fd = self.sock.fileno()
        except (OSError, ValueError):
            pass
        if fd >= 0:
            try:
                runloom_c.netpoll_unregister(fd)
            except (AttributeError, OSError):
                pass
        try:
            self.sock.close()
        except OSError:
            pass


def dial(host, port, timeout_ms=10_000):
    """Cooperative outbound TCP connect.  Returns a connected CoSock.

    getaddrinfo (DNS) is a blocking C call -- under M:N it briefly parks
    the hub thread, same as runloom.sync.tcp_connect.  The connect itself
    parks the goroutine on wait_fd(WRITE)."""
    infos = socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM)
    last_err = None
    for family, kind, proto, _canon, sa in infos:
        raw = None
        try:
            raw = socket.socket(family, kind, proto)
            raw.setblocking(False)
            try:
                raw.connect(sa)
            except BlockingIOError:
                ready = runloom_c.wait_fd(raw.fileno(), WRITE, timeout_ms)
                if ready == 0:
                    raise TimeoutError("connect timed out")
                err = raw.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                if err != 0:
                    raise OSError(err, "connect failed")
            return CoSock(raw)
        except OSError as exc:
            last_err = exc
            if raw is not None:
                raw.close()
    raise last_err or OSError("could not connect to {}:{}".format(host, port))


def fetch(host, path="/", port=80, timeout_ms=10_000, max_bytes=1 << 20):
    """Cooperative HTTP/1.0 GET.  Returns (status, body_bytes).

    A self-contained outbound request on the M:N sync API -- handy for
    background probes that just need to exercise the egress path."""
    conn = dial(host, port, timeout_ms)
    try:
        request = ("GET {} HTTP/1.0\r\nHost: {}\r\n"
                   "Connection: close\r\n\r\n").format(path, host)
        conn.sendall(request.encode("latin-1"))
        raw = bytearray()
        while len(raw) < max_bytes:
            try:
                chunk = conn.recv(65536, timeout_ms)
            except TimeoutError:
                break
            if not chunk:
                break
            raw.extend(chunk)
    finally:
        conn.close()
    status = 0
    if raw.startswith(b"HTTP/"):
        try:
            status = int(raw.split(b" ", 2)[1])
        except (IndexError, ValueError):
            status = 0
    head, _, body = raw.partition(b"\r\n\r\n")
    return status, bytes(body)


def every(interval_s, fn, *args):
    """Return a zero-arg goroutine body that runs fn(*args) every
    interval_s forever, swallowing (and printing) per-iteration errors so
    one failure never kills the loop."""
    def loop():
        while True:
            runloom_c.sched_sleep(interval_s)
            try:
                fn(*args)
            except Exception:
                traceback.print_exc()
    return loop


class Request:
    """A parsed HTTP request."""
    def __init__(self, method, path, query, version, headers, body, addr):
        self.method = method
        self.path = path
        self.query = query
        self.version = version
        self.headers = headers          # lower-cased keys
        self.body = body
        self.addr = addr

    def header(self, name, default=None):
        return self.headers.get(name.lower(), default)


class Response:
    """An HTTP response.  body may be str or bytes."""
    def __init__(self, body=b"", status=200, headers=None, content_type="text/plain; charset=utf-8"):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.body = body
        self.status = status
        self.headers = headers or {}
        if content_type and "content-type" not in {k.lower() for k in self.headers}:
            self.headers["Content-Type"] = content_type

    def encode(self, keep_alive):
        reason = REASONS.get(self.status, "OK")
        lines = ["HTTP/1.1 {} {}".format(self.status, reason)]
        lines.append("Content-Length: {}".format(len(self.body)))
        lines.append("Connection: {}".format("keep-alive" if keep_alive else "close"))
        for key, value in self.headers.items():
            lines.append("{}: {}".format(key, value))
        head = ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1")
        return head + self.body


def coerce_response(result):
    """Turn whatever a handler returned into a Response."""
    if isinstance(result, Response):
        return result
    if isinstance(result, tuple) and len(result) == 2:
        status, body = result
        return Response(body, status=status)
    if isinstance(result, (str, bytes, bytearray)):
        return Response(bytes(result) if isinstance(result, (bytes, bytearray)) else result)
    if result is None:
        return Response(b"", status=204)
    # Anything else: stringify.
    return Response(repr(result))


MAX_HEADER_BYTES = 64 * 1024
MAX_BODY_BYTES = 4 * 1024 * 1024
IDLE_TIMEOUT_MS = 30_000        # reap a keep-alive conn idle this long
MAX_REQUESTS_PER_CONN = 1000


class App:
    """A tiny routed HTTP application served over the M:N scheduler."""

    def __init__(self):
        self.routes = {}             # (method, path) -> handler
        self.before = []             # request hooks(req) -> None
        self.after = []              # response hooks(req, resp, elapsed_ms) -> None
        self.error_log = sys.stderr

    def route(self, path, methods=("GET",)):
        def register(handler):
            for method in methods:
                self.routes[(method.upper(), path)] = handler
            return handler
        return register

    def before_request(self, fn):
        self.before.append(fn)
        return fn

    def after_request(self, fn):
        self.after.append(fn)
        return fn

    # ---- request lifecycle ----------------------------------------

    def dispatch(self, req):
        handler = self.routes.get((req.method, req.path))
        if handler is None:
            # 405 if path exists under another method, else 404.
            if any(p == req.path for (m, p) in self.routes):
                raise HTTPError(405)
            raise HTTPError(404)
        for hook in self.before:
            hook(req)
        return coerce_response(handler(req))

    def handle_connection(self, conn, addr):
        try:
            for _ in range(MAX_REQUESTS_PER_CONN):
                req = self.read_request(conn, addr)
                if req is None:
                    return                      # client closed or idle timeout
                started = time.perf_counter()
                try:
                    resp = self.dispatch(req)
                except HTTPError as exc:
                    resp = Response(exc.message, status=exc.status)
                except Exception:
                    traceback.print_exc(file=self.error_log)
                    resp = Response("internal server error", status=500)
                elapsed_ms = (time.perf_counter() - started) * 1000.0
                for hook in self.after:
                    try:
                        hook(req, resp, elapsed_ms)
                    except Exception:
                        traceback.print_exc(file=self.error_log)
                keep_alive = self.wants_keep_alive(req, resp)
                try:
                    conn.sendall(resp.encode(keep_alive))
                except OSError:
                    return
                if not keep_alive:
                    return
        finally:
            conn.close()

    def wants_keep_alive(self, req, resp):
        if resp.status >= 500:
            return False
        connection = req.header("connection", "").lower()
        if req.version == "HTTP/1.1":
            return connection != "close"
        return connection == "keep-alive"

    def read_request(self, conn, addr):
        """Read and parse one request, or return None on close/idle."""
        buf = bytearray()
        # Read until end of headers.  First recv uses the idle timeout so
        # a keep-alive connection that goes quiet is dropped, not leaked.
        while b"\r\n\r\n" not in buf:
            try:
                chunk = conn.recv(8192, IDLE_TIMEOUT_MS if not buf else 10_000)
            except TimeoutError:
                return None
            if not chunk:
                return None
            buf.extend(chunk)
            if len(buf) > MAX_HEADER_BYTES:
                raise HTTPError(413, "headers too large")
        head, rest = buf.split(b"\r\n\r\n", 1)
        try:
            req = self.parse_head(head, addr)
        except HTTPError:
            raise
        except Exception:
            raise HTTPError(400, "malformed request")
        length = 0
        clen = req.headers.get("content-length")
        if clen is not None:
            try:
                length = int(clen)
            except ValueError:
                raise HTTPError(400, "bad content-length")
            if length > MAX_BODY_BYTES:
                raise HTTPError(413, "body too large")
        body = bytearray(rest)
        while len(body) < length:
            chunk = conn.recv(min(65536, length - len(body)), 10_000)
            if not chunk:
                raise HTTPError(400, "truncated body")
            body.extend(chunk)
        req.body = bytes(body[:length])
        return req

    def parse_head(self, head, addr):
        lines = head.split(b"\r\n")
        request_line = lines[0].decode("latin-1")
        method, target, version = request_line.split(" ", 2)
        if "?" in target:
            path, query = target.split("?", 1)
        else:
            path, query = target, ""
        headers = {}
        for line in lines[1:]:
            if not line:
                continue
            key, _, value = line.partition(b":")
            headers[key.decode("latin-1").strip().lower()] = value.decode("latin-1").strip()
        return Request(method.upper(), path, query, version, headers, b"", addr)

    # ---- server loop ----------------------------------------------

    def accept_loop(self, listener):
        while True:
            try:
                conn, addr = listener.accept()
            except OSError:
                return
            runloom_c.mn_fiber(lambda c=conn, a=addr: self.handle_connection(c, a))

    def run(self, host, port, hubs=0, background_goroutines=()):
        """Start the M:N scheduler and serve forever.

        background_goroutines: extra zero-arg callables spawned alongside
        the accept loop (e.g. a stats heartbeat or a db writer).
        """
        raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raw.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        raw.bind((host, port))
        raw.listen(512)
        listener = CoSock(raw)

        nhubs = runloom_c.mn_init(hubs) if hubs else runloom_c.mn_init()
        print("[mnweb] serving on {}:{} across {} hubs (backend={}, netpoll={})".format(
            host, port, nhubs, runloom_c.backend(), runloom_c.netpoll_backend()), flush=True)

        for fn in background_goroutines:
            runloom_c.mn_fiber(fn)
        runloom_c.mn_fiber(lambda: self.accept_loop(listener))
        runloom_c.mn_run()
        runloom_c.mn_fini()
