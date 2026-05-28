"""Concurrent-client bench for TCPConn.

bench_tcpconn.py runs 1 client sequentially -- the worst case for
io_uring multishot, which exists to absorb N concurrent flows with
one in-flight SQE per fd.  This bench runs N concurrent client
goroutines against one server, all sharing the scheduler, and
measures aggregate throughput.

For each N we run:
    server: listener accepts N conns, spawns one handler goroutine per
            conn, each handler does M RTs of 8-byte echo.
    clients: N client goroutines, each opens one conn, runs M RTs.

All N concurrent RTs are in flight simultaneously in the scheduler;
the kernel sees N parallel TCP streams.  Total wall time covers from
first connect to last client done.  Aggregate K/s = N*M/t.

Set PYGO_TCPCONN_IOURING=1 to route TCPConn.recv through the
io_uring multishot path (Linux only); leave unset for the default
epoll register-once path.
"""
import os
import socket
import sys
import time

sys.path.insert(0, "src")
import pygo, pygo.monkey, pygo_core


PAYLOAD = b"hellopyg"
# (N, M).  M shrinks at high N to keep total wall time bounded; the
# scaling profile (us/RT) is what we care about, not raw seconds.
WORK = ((1,    1000),
        (8,    1000),
        (64,    500),
        (256,   200),
        (512,   100),
        (1024,   50))


def _bound_port(listener):
    fd = listener.fileno()
    sk = socket.socket(fileno=socket.dup(fd))
    port = sk.getsockname()[1]
    sk.close()
    return port


def bench_tcpconn(N, M):
    port_holder = [None]
    done = [0]
    t_start = [None]
    t_end = [None]

    def make_handler(conn):
        def handler():
            buf = bytearray(len(PAYLOAD))
            for _ in range(M):
                n = conn.recv_into(buf, len(PAYLOAD))
                if not n: break
                conn.send_all(memoryview(buf)[:n])
            conn.close()
        return handler

    def server():
        listener = pygo_core.TCPConn.listen("127.0.0.1", 0, backlog=N + 8)
        port_holder[0] = _bound_port(listener)
        for _ in range(N):
            c = listener.accept()
            pygo_core.go(make_handler(c))
        listener.close()

    def make_client():
        def client():
            while port_holder[0] is None:
                pygo_core.sched_yield()
            c = pygo_core.TCPConn.connect("127.0.0.1", port_holder[0])
            buf = bytearray(len(PAYLOAD))
            for _ in range(M):
                c.send_all(PAYLOAD)
                c.recv_into(buf, len(PAYLOAD))
            c.close()
            done[0] += 1
            if done[0] == N:
                t_end[0] = time.perf_counter()
        return client

    def driver():
        pygo_core.go(server)
        while port_holder[0] is None:
            pygo_core.sched_yield()
        t_start[0] = time.perf_counter()
        for _ in range(N):
            pygo_core.go(make_client())

    pygo_core.go(driver)
    pygo_core.run()
    return t_end[0] - t_start[0]


def bench_monkey(N, M):
    """Apples-to-apples concurrent monkey-patched-socket bench."""
    pygo.monkey.patch()
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(N + 8)
    port = srv.getsockname()[1]
    srv.setblocking(False)

    done = [0]
    t_start = [None]
    t_end = [None]

    def make_handler(conn):
        def handler():
            conn.setblocking(False)
            buf = bytearray(len(PAYLOAD))
            for _ in range(M):
                n = conn.recv_into(buf)
                if not n: break
                conn.sendall(memoryview(buf)[:n])
            conn.close()
        return handler

    def server():
        for _ in range(N):
            c, _ = srv.accept()
            pygo_core.go(make_handler(c))

    def make_client():
        def client():
            c = socket.socket(); c.connect(("127.0.0.1", port))
            c.setblocking(False)
            buf = bytearray(len(PAYLOAD))
            for _ in range(M):
                c.sendall(PAYLOAD)
                c.recv_into(buf)
            c.close()
            done[0] += 1
            if done[0] == N:
                t_end[0] = time.perf_counter()
        return client

    def driver():
        pygo_core.go(server)
        t_start[0] = time.perf_counter()
        for _ in range(N):
            pygo_core.go(make_client())

    pygo_core.go(driver)
    pygo_core.run()
    srv.close()
    return t_end[0] - t_start[0]


def main():
    iouring_on = os.environ.get("PYGO_TCPCONN_IOURING") == "1"
    skip_monkey = os.environ.get("PYGO_BENCH_SKIP_MONKEY") == "1"

    print("Concurrent echo bench -- N clients, M RTs each, 8-byte payload")
    print("PYGO_TCPCONN_IOURING={}".format("1" if iouring_on else "(unset)"))
    print()
    cols = ["N", "M", "TCPConn K/s", "us/RT (t)"]
    if not skip_monkey:
        cols[2:2] = ["monkey K/s"]
        cols.append("us/RT (m)")
    print(("{:>6}  " * len(cols)).format(*cols).rstrip())
    print("-" * (8 * len(cols)))

    for N, M in WORK:
        total = N * M
        tt = bench_tcpconn(N, M)
        tt_kps = total / tt / 1000
        tt_us  = tt * 1e6 / total
        if skip_monkey:
            print("{:>6}  {:>6}  {:>11.1f}  {:>9.1f}".format(
                N, M, tt_kps, tt_us))
        else:
            tm = bench_monkey(N, M)
            tm_kps = total / tm / 1000
            tm_us  = tm * 1e6 / total
            print("{:>6}  {:>6}  {:>10.1f}  {:>11.1f}  {:>9.1f}  {:>9.1f}".format(
                N, M, tm_kps, tt_kps, tt_us, tm_us))


if __name__ == "__main__":
    main()
