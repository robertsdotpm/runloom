"""Minimal hand-rolled HTTP/1.1 over cooperative sockets for the big_100
crawler / keep-alive / web-server projects.  No external libraries; just enough
of the protocol to exercise the socket + parsing paths under load.
"""
import netutil


def read_request(sock):
    """Read one request: return (method, path, headers dict, keep_alive).

    Raises OSError on EOF/short read (caller treats as connection close)."""
    raw = netutil.recv_until(sock, b"\r\n\r\n", limit=65536)
    head = raw.split(b"\r\n\r\n", 1)[0].decode("latin-1")
    lines = head.split("\r\n")
    method, path, version = (lines[0].split(" ") + ["", "", ""])[:3]
    headers = {}
    for ln in lines[1:]:
        if ":" in ln:
            k, v = ln.split(":", 1)
            headers[k.strip().lower()] = v.strip()
    conn = headers.get("connection", "").lower()
    keep_alive = (version == "HTTP/1.1" and conn != "close") or conn == "keep-alive"
    return method, path, headers, keep_alive


def send_response(sock, body, status="200 OK", keep_alive=True,
                  content_type="text/html"):
    if isinstance(body, str):
        body = body.encode("utf-8")
    hdrs = [
        "HTTP/1.1 {0}".format(status),
        "Content-Type: {0}".format(content_type),
        "Content-Length: {0}".format(len(body)),
        "Connection: {0}".format("keep-alive" if keep_alive else "close"),
        "", "",
    ]
    sock.sendall("\r\n".join(hdrs).encode("latin-1") + body)


def read_response(sock):
    """Read a Content-Length-framed response already sent.  Returns
    (status_code, body)."""
    raw = netutil.recv_until(sock, b"\r\n\r\n", limit=65536)
    head, rest = raw.split(b"\r\n\r\n", 1)
    lines = head.decode("latin-1").split("\r\n")
    status_code = int(lines[0].split(" ")[1])
    clen = 0
    for ln in lines[1:]:
        if ln.lower().startswith("content-length:"):
            clen = int(ln.split(":", 1)[1].strip())
    body = bytearray(rest)
    while len(body) < clen:
        chunk = sock.recv(clen - len(body))
        if not chunk:
            raise OSError("eof reading body")
        body += chunk
    return status_code, bytes(body)


def get(sock, path, host="127.0.0.1", keep_alive=True):
    """Send a GET and read the full response.  Returns (status_code, body).

    Content-Length framed only (that is all our server emits)."""
    req = (
        "GET {0} HTTP/1.1\r\n"
        "Host: {1}\r\n"
        "Connection: {2}\r\n\r\n"
    ).format(path, host, "keep-alive" if keep_alive else "close")
    sock.sendall(req.encode("latin-1"))
    raw = netutil.recv_until(sock, b"\r\n\r\n", limit=65536)
    head, rest = raw.split(b"\r\n\r\n", 1)
    lines = head.decode("latin-1").split("\r\n")
    status_code = int(lines[0].split(" ")[1])
    clen = 0
    for ln in lines[1:]:
        if ln.lower().startswith("content-length:"):
            clen = int(ln.split(":", 1)[1].strip())
    body = bytearray(rest)
    while len(body) < clen:
        chunk = sock.recv(clen - len(body))
        if not chunk:
            raise OSError("eof reading body")
        body += chunk
    return status_code, bytes(body)
