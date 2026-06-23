"""big_100 / 89 -- SMTP sink.

A minimal SMTP server (220/HELO/MAIL/RCPT/DATA/QUIT).  Clients send full
transactions whose message body is a base64-encoded "attachment" prefixed by
its SHA-256.  The server reassembles the body, recomputes the checksum, and
counts mismatches -- so any line-protocol or partial-read bug corrupts the
message and is caught.

Stresses: line protocols with a DATA phase, large payloads, partial writes.
"""
import base64
import hashlib
import socket
import threading

import harness
import netutil


def setup(H):
    def handle(conn):
        buf = bytearray()

        def readline():
            while b"\r\n" not in buf:
                chunk = conn.recv(4096)
                if not chunk:
                    raise OSError("eof")
                buf += chunk
            nl = buf.index(b"\r\n")
            line = bytes(buf[:nl])
            del buf[:nl + 2]
            return line

        try:
            conn.sendall(b"220 big100 ESMTP\r\n")
            while True:
                line = readline()
                up = line.upper()
                if up.startswith(b"HELO") or up.startswith(b"EHLO"):
                    conn.sendall(b"250 ok\r\n")
                elif up.startswith(b"MAIL FROM") or up.startswith(b"RCPT TO"):
                    conn.sendall(b"250 ok\r\n")
                elif up == b"DATA":
                    conn.sendall(b"354 go\r\n")
                    body_lines = []
                    while True:
                        bl = readline()
                        if bl == b".":
                            break
                        body_lines.append(bl)
                    # First line is the checksum; the rest is base64 content.
                    chk = body_lines[0].split(b" ", 1)[1] if body_lines else b""
                    try:
                        content = base64.b64decode(b"".join(body_lines[1:]))
                        ok = hashlib.sha256(content).hexdigest().encode() == chk
                    except Exception:           # noqa: BLE001
                        ok = False
                    with H.state["lock"]:
                        H.state["accepted"][0] += 1
                        if not ok:
                            H.state["corrupt"][0] += 1
                    conn.sendall(b"250 queued\r\n")
                elif up == b"QUIT":
                    conn.sendall(b"221 bye\r\n")
                    break
                else:
                    conn.sendall(b"500 what\r\n")
        except (OSError, ValueError, IndexError):
            pass
        finally:
            netutil.close_quiet(conn)

    servers = netutil.listen_all(H, lambda conn, addr: H.fiber(handle, conn))
    H.state = {"servers": servers,
               "lock": threading.Lock(), "accepted": [0], "corrupt": [0]}


def expect(sock, code, buf):
    line = netutil.recv_until(sock, b"\r\n", limit=4096).rstrip(b"\r\n")
    return line.startswith(code)


def client(H, wid, rng, state):
    servers = state["servers"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        sock = None
        host, port = netutil.pick_server(servers, rng)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect((host, port))
            buf = bytearray()
            if not expect(sock, b"220", buf):
                return
            attach = rng.randbytes(rng.randint(256, 16384))
            chk = hashlib.sha256(attach).hexdigest().encode()
            b64 = base64.b64encode(attach)
            lines = [b64[i:i + 76] for i in range(0, len(b64), 76)]
            for cmd, code in ((b"HELO big100", b"250"),
                              (b"MAIL FROM:<a@x>", b"250"),
                              (b"RCPT TO:<b@y>", b"250"),
                              (b"DATA", b"354")):
                sock.sendall(cmd + b"\r\n")
                if not H.check(expect(sock, code, buf),
                               "smtp {0} not {1} wid={2}".format(cmd, code, wid)):
                    return
            sock.sendall(b"X-Checksum " + chk + b"\r\n")
            for ln in lines:
                sock.sendall(ln + b"\r\n")
            sock.sendall(b".\r\n")
            if not H.check(expect(sock, b"250", buf),
                           "smtp DATA not accepted wid={0}".format(wid)):
                return
            sock.sendall(b"QUIT\r\n")
            expect(sock, b"221", buf)
            H.op(wid)
            H.task_done(wid)
        except (OSError, ValueError):
            if not H.running():
                break
            H.sleep(0.005)
        finally:
            netutil.close_quiet(sock)


def body(H):
    H.run_pool(H.funcs, client, H.state)


def post(H):
    H.check(H.state["corrupt"][0] == 0,
            "{0} messages arrived corrupt".format(H.state["corrupt"][0]))
    H.log("accepted={0} corrupt={1}".format(
        H.state["accepted"][0], H.state["corrupt"][0]))


if __name__ == "__main__":
    harness.main("p89_smtp_sink", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="SMTP DATA transactions with checksummed attachments")
