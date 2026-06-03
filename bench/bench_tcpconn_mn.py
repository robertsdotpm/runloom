"""M:N (multi-hub) version of bench_tcpconn_concurrent.py.

Identical workload — N concurrent conns, M 8-byte echo round-trips each,
C `TCPConn` handlers — but on the M:N scheduler with H hubs, so the
multi-core throughput is the *same bench* as the single-thread (`run()`)
and Go comparisons. One H per process invocation (clean re-init).

Usage: bench_tcpconn_mn.py [H] [N] [M]   (defaults: H=4 N=256 M=200)
"""
import socket
import sys
import time

sys.path.insert(0, "src")
import pygo_core

PAYLOAD = b"hellopyg"   # 8 bytes, same as the single-thread + Go benches


def _bound_port(listener):
    fd = listener.fileno()
    sk = socket.socket(fileno=socket.dup(fd))
    port = sk.getsockname()[1]
    sk.close()
    return port


def run(H, N, M):
    listener = pygo_core.TCPConn.listen("127.0.0.1", 0, backlog=N + 16)
    port = _bound_port(listener)

    def make_handler(conn):
        def handler():
            buf = bytearray(len(PAYLOAD))
            for _ in range(M):
                n = conn.recv_into(buf, len(PAYLOAD))
                if not n:
                    break
                conn.send_all(memoryview(buf)[:n])
            conn.close()
        return handler

    def server():
        for _ in range(N):
            c = listener.accept()
            pygo_core.mn_go(make_handler(c))
        listener.close()

    def make_client():
        def client():
            c = pygo_core.TCPConn.connect("127.0.0.1", port)
            buf = bytearray(len(PAYLOAD))
            for _ in range(M):
                c.send_all(PAYLOAD)
                c.recv_into(buf, len(PAYLOAD))
            c.close()
        return client

    pygo_core.mn_init(H)
    pygo_core.mn_go(server)
    t0 = time.perf_counter()
    for _ in range(N):
        pygo_core.mn_go(make_client())
    pygo_core.mn_run()
    dt = time.perf_counter() - t0
    pygo_core.mn_fini()
    return dt


def main():
    H = int(sys.argv[1]) if len(sys.argv) > 1 else 4
    N = int(sys.argv[2]) if len(sys.argv) > 2 else 256
    M = int(sys.argv[3]) if len(sys.argv) > 3 else 200
    dt = run(H, N, M)
    total = N * M
    print("{:>4} {:>6} {:>6} {:>12.1f} {:>10.2f}".format(
        H, N, M, total / dt / 1000, dt * 1e6 / total))


if __name__ == "__main__":
    main()
