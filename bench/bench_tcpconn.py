"""Bench runloom_c.TCPConn against the monkey-patched socket and the
free-floating tcp_recv/tcp_send fastpaths.  Single client, sequential
RT echoes -- the worst case for overhead because every byte pays one
goroutine yield + one netpoll wake."""
import socket
import sys
import time

sys.path.insert(0, "src")
import runloom, runloom.monkey, runloom_c


def _bound_port(listener):
    """Get the bound port of a TCPConn listener (no native getsockname yet)."""
    fd = listener.fileno()
    sk = socket.socket(fileno=socket.dup(fd))
    port = sk.getsockname()[1]
    sk.close()
    return port


def bench_monkey(N):
    runloom.monkey.patch()
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    port = srv.getsockname()[1]
    t = [0.0]

    def server():
        srv.setblocking(False)
        conn, _ = srv.accept()
        conn.setblocking(False)
        buf = bytearray(64)
        for _ in range(N):
            n = conn.recv_into(buf)
            if not n: break
            conn.sendall(memoryview(buf)[:n])
        conn.close()

    def client():
        c = socket.socket(); c.connect(("127.0.0.1", port)); c.setblocking(False)
        msg = b"hellopyg"
        buf = bytearray(8)
        t0 = time.perf_counter()
        for _ in range(N):
            c.sendall(msg)
            c.recv_into(buf)
        t[0] = time.perf_counter() - t0
        c.close()

    runloom_c.fiber(server); runloom_c.fiber(client); runloom_c.run()
    srv.close()
    return t[0]


def bench_native_fns(N):
    runloom.monkey.patch()
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    port = srv.getsockname()[1]
    t = [0.0]

    def server():
        srv.setblocking(False)
        conn, _ = srv.accept(); conn.setblocking(False)
        fd = conn.fileno()
        buf = bytearray(64)
        for _ in range(N):
            got = runloom_c.tcp_recv(fd, buf, 8)
            if got == 0: break
            runloom_c.tcp_send(fd, bytes(buf[:got]))
        conn.close()

    def client():
        c = socket.socket(); c.connect(("127.0.0.1", port)); c.setblocking(False)
        fd = c.fileno()
        msg = b"hellopyg"
        buf = bytearray(8)
        t0 = time.perf_counter()
        for _ in range(N):
            runloom_c.tcp_send(fd, msg)
            runloom_c.tcp_recv(fd, buf, 8)
        t[0] = time.perf_counter() - t0
        c.close()

    runloom_c.fiber(server); runloom_c.fiber(client); runloom_c.run()
    srv.close()
    return t[0]


def bench_tcpconn(N):
    t = [0.0]
    port_holder = [None]

    def server():
        listener = runloom_c.TCPConn.listen("127.0.0.1", 0, backlog=8)
        port_holder[0] = _bound_port(listener)
        conn = listener.accept()
        buf = bytearray(64)
        for _ in range(N):
            n = conn.recv_into(buf, 8)
            if not n: break
            conn.send_all(memoryview(buf)[:n])
        conn.close()
        listener.close()

    def client():
        while port_holder[0] is None:
            runloom_c.sched_yield()
        c = runloom_c.TCPConn.connect("127.0.0.1", port_holder[0])
        msg = b"hellopyg"
        buf = bytearray(8)
        t0 = time.perf_counter()
        for _ in range(N):
            c.send_all(msg)
            c.recv_into(buf, 8)
        t[0] = time.perf_counter() - t0
        c.close()

    runloom_c.fiber(server); runloom_c.fiber(client); runloom_c.run()
    return t[0]


if __name__ == "__main__":
    N = 5000
    for name, fn in (("monkey", bench_monkey),
                     ("native", bench_native_fns),
                     ("TCPConn", bench_tcpconn)):
        t = fn(N)
        print("{:>8}: {:.3f}s for {} RT -> {:6.1f} K/s, {:4.0f} us/RT".format(
            name, t, N, N / t / 1000, t * 1e6 / N))
