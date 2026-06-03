#!/usr/bin/env python3
"""runloom server -- M:N stackful goroutines, multi-core via 3.13t free-threading.

Accept loop spawns one goroutine per connection; each goroutine loops:
recv fixed REQ, runloom_c.sched_sleep(io_delay) (simulated backend/DB I/O,
parks the goroutine and lets the hub run others), send fixed RESP.  Sync
straight-line code, no async/await coloring.  Driven by the external Go
loadgen so it competes head-to-head with the Go and asyncio servers on the
identical wire protocol.

Usage: PYTHONPATH=<runloom>/src python3 server_runloom.py [host] [port] [io_ms] [H]
"""
import os
import resource
import socket
import sys

sys.path.insert(0, os.environ.get("RUNLOOM_SRC", ""))
import runloom_c

REQ_LEN = 10
RESP = b"200 " + b"x" * 1024 + b"\n"   # 1029 bytes
READ = 1
WRITE = 2

IO_S = 0.0
listen_sock = None


def _recv_exactly(sock, fd, n):
    out = bytearray()
    while len(out) < n:
        try:
            chunk = sock.recv(n - len(out))
        except (BlockingIOError, InterruptedError):
            runloom_c.wait_fd(fd, READ)
            continue
        except OSError:
            return b""
        if not chunk:
            return b""
        out += chunk
    return bytes(out)


def _send_all(sock, fd, data):
    view = memoryview(data)
    sent = 0
    while sent < len(view):
        try:
            sent += sock.send(view[sent:])
        except (BlockingIOError, InterruptedError):
            runloom_c.wait_fd(fd, WRITE)
        except OSError:
            return False
    return True


def server_conn(conn):
    conn.setblocking(False)
    try:
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except OSError:
        pass
    fd = conn.fileno()
    try:
        while True:
            req = _recv_exactly(conn, fd, REQ_LEN)
            if not req:
                break
            if IO_S > 0:
                runloom_c.sched_sleep(IO_S)
            if not _send_all(conn, fd, RESP):
                break
    finally:
        try:
            f = conn.fileno()
            if f >= 0:
                runloom_c.netpoll_unregister(f)
        except (AttributeError, OSError, ValueError):
            pass
        try:
            conn.close()
        except OSError:
            pass


def accept_loop():
    lfd = listen_sock.fileno()
    while True:
        try:
            conn, _ = listen_sock.accept()
        except (BlockingIOError, InterruptedError):
            runloom_c.wait_fd(lfd, READ)
            continue
        except OSError:
            break
        try:
            runloom_c.netpoll_unregister(conn.fileno())
        except (AttributeError, OSError):
            pass
        runloom_c.mn_go(lambda c=conn: server_conn(c))


def main():
    global IO_S, listen_sock
    if os.environ.get("RUNLOOM_GC_DISABLE"):
        import gc
        gc.disable()
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9000
    IO_S = (float(sys.argv[3]) if len(sys.argv) > 3 else 0.0) / 1000.0
    H = int(sys.argv[4]) if len(sys.argv) > 4 else 8

    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (1 << 20, 1 << 20))
    except (ValueError, OSError):
        pass

    listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listen_sock.bind((host, port))
    listen_sock.listen(65535)
    listen_sock.setblocking(False)
    print("runloom-server listening on %s io=%sms H=%d" %
          (listen_sock.getsockname(), IO_S * 1000, H), flush=True)

    if runloom_c.mn_init(H) < 0:
        sys.stderr.write("mn_init failed\n")
        return 2
    runloom_c.mn_go(accept_loop)
    runloom_c.mn_run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
