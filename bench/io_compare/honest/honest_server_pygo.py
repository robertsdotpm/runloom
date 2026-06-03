#!/usr/bin/env python3
"""HONEST_BENCH pygo server -- one goroutine per conn, H hubs. The 100ms CPU
tier runs on a hub but preemption (sysmon) + the other hubs keep serving, so
the tail stays bounded. Usage: host port [io_unused] [H]"""
import os, random, resource, socket, sys
sys.path.insert(0, os.environ.get("PYGO_SRC", ""))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pygo_core
import workload as w
REQ_LEN = 10; RESP = b"200 " + b"x" * 1024 + b"\n"; READ = 1; WRITE = 2
listen_sock = None


def recv_exactly(sock, fd, n):
    out = bytearray()
    while len(out) < n:
        try: c = sock.recv(n - len(out))
        except (BlockingIOError, InterruptedError): pygo_core.wait_fd(fd, READ); continue
        except OSError: return b""
        if not c: return b""
        out += c
    return bytes(out)


def send_all(sock, fd, data):
    v = memoryview(data); sent = 0
    while sent < len(v):
        try: sent += sock.send(v[sent:])
        except (BlockingIOError, InterruptedError): pygo_core.wait_fd(fd, WRITE)
        except OSError: return False
    return True


def server_conn(conn):
    conn.setblocking(False)
    try: conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except OSError: pass
    fd = conn.fileno(); rng = random.Random()
    try:
        while True:
            if not recv_exactly(conn, fd, REQ_LEN): break
            kind, dur = w.tier(rng.random())
            if kind == "io":
                pygo_core.sched_sleep(dur)
            else:
                w.burn_cpu(100)               # pathological: hub keeps it, others serve
            if not send_all(conn, fd, RESP): break
    finally:
        try:
            f = conn.fileno()
            if f >= 0: pygo_core.netpoll_unregister(f)
        except (AttributeError, OSError, ValueError): pass
        try: conn.close()
        except OSError: pass


def accept_loop():
    lfd = listen_sock.fileno()
    while True:
        try: conn, _ = listen_sock.accept()
        except (BlockingIOError, InterruptedError): pygo_core.wait_fd(lfd, READ); continue
        except OSError: break
        try: pygo_core.netpoll_unregister(conn.fileno())
        except (AttributeError, OSError): pass
        pygo_core.mn_go(lambda c=conn: server_conn(c))


def main():
    global listen_sock
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9000
    H = int(sys.argv[4]) if len(sys.argv) > 4 else 8
    try: resource.setrlimit(resource.RLIMIT_NOFILE, (1 << 20, 1 << 20))
    except (ValueError, OSError): pass
    listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listen_sock.bind((host, port)); listen_sock.listen(65535); listen_sock.setblocking(False)
    print("honest-pygo listening on %s H=%d" % (listen_sock.getsockname(), H), flush=True)
    if pygo_core.mn_init(H) < 0: return 2
    pygo_core.mn_go(accept_loop); pygo_core.mn_run(); return 0


if __name__ == "__main__":
    sys.exit(main())
