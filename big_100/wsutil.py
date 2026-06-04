"""Minimal RFC6455 WebSocket framing + handshake over cooperative sockets,
just enough for the big_100 chat-room project.  Text frames, no fragmentation,
no extensions, no TLS.
"""
import base64
import hashlib
import os
import struct

import netutil

GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def server_handshake(sock):
    """Read the client upgrade request, send the 101 response."""
    req = netutil.recv_until(sock, b"\r\n\r\n").decode("latin-1")
    key = ""
    for line in req.split("\r\n"):
        if line.lower().startswith("sec-websocket-key:"):
            key = line.split(":", 1)[1].strip()
    accept = base64.b64encode(
        hashlib.sha1((key + GUID).encode()).digest()).decode()
    resp = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\nConnection: Upgrade\r\n"
        "Sec-WebSocket-Accept: {0}\r\n\r\n").format(accept)
    sock.sendall(resp.encode("latin-1"))


def client_handshake(sock, host="127.0.0.1"):
    key = base64.b64encode(os.urandom(16)).decode()
    req = (
        "GET / HTTP/1.1\r\nHost: {0}\r\nUpgrade: websocket\r\n"
        "Connection: Upgrade\r\nSec-WebSocket-Key: {1}\r\n"
        "Sec-WebSocket-Version: 13\r\n\r\n").format(host, key)
    sock.sendall(req.encode("latin-1"))
    resp = netutil.recv_until(sock, b"\r\n\r\n").decode("latin-1")
    if "101" not in resp.split("\r\n")[0]:
        raise OSError("ws handshake failed: " + resp[:40])


def send_text(sock, text, mask=False):
    payload = text.encode("utf-8")
    header = bytearray([0x81])              # FIN + text opcode
    n = len(payload)
    mbit = 0x80 if mask else 0x00
    if n < 126:
        header.append(mbit | n)
    elif n < 65536:
        header.append(mbit | 126)
        header += struct.pack(">H", n)
    else:
        header.append(mbit | 127)
        header += struct.pack(">Q", n)
    if mask:
        mk = os.urandom(4)
        header += mk
        payload = bytes(b ^ mk[i & 3] for i, b in enumerate(payload))
    sock.sendall(bytes(header) + payload)


TIMEOUT = object()      # sentinel returned by recv_text_timeout on no-data


def recv_text_timeout(sock, timeout_ms):
    """recv_text but return TIMEOUT if no frame starts within timeout_ms.

    Once the socket is readable we read the whole (small) frame; on loopback a
    short frame's bytes arrive together.  Keeps the caller single-goroutine so
    there is never a second goroutine parked on this fd at teardown."""
    import runloom_c
    fd = sock.fileno()
    if not (runloom_c.wait_fd(fd, 1, timeout_ms) & 1):
        return TIMEOUT
    return recv_text(sock)


def recv_text(sock):
    """Read one text/close frame; return the text, or None on a close frame."""
    b0 = netutil.recv_exact(sock, 1)[0]
    opcode = b0 & 0x0F
    b1 = netutil.recv_exact(sock, 1)[0]
    masked = b1 & 0x80
    n = b1 & 0x7F
    if n == 126:
        n = struct.unpack(">H", netutil.recv_exact(sock, 2))[0]
    elif n == 127:
        n = struct.unpack(">Q", netutil.recv_exact(sock, 8))[0]
    mk = netutil.recv_exact(sock, 4) if masked else b""
    payload = netutil.recv_exact(sock, n) if n else b""
    if masked:
        payload = bytes(b ^ mk[i & 3] for i, b in enumerate(payload))
    if opcode == 0x8:
        return None                         # close
    return payload.decode("utf-8", "replace")
