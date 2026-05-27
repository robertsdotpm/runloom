"""TCP echo loopback: native pygo_core.tcp_recv/send vs the
monkey-patched socket.recv/sendall.  Same workload, two paths."""
import socket
import sys
import time

sys.path.insert(0, "src")
import pygo, pygo.monkey, pygo_core
pygo.monkey.patch()


def bench(N=2000, mode="monkey"):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    port = srv.getsockname()[1]
    result = [0]
    t0 = [0.0]

    def server():
        srv.setblocking(False)
        conn, _ = srv.accept()
        conn.setblocking(False)
        if mode == "native":
            fd = conn.fileno()
            buf = bytearray(64)
            for _ in range(N):
                got = pygo_core.tcp_recv(fd, buf, 8)
                if got == 0:
                    break
                pygo_core.tcp_send(fd, bytes(buf[:got]))
        else:
            for _ in range(N):
                data = conn.recv(64)
                if not data:
                    break
                conn.sendall(data)
        conn.close()

    def client():
        c = socket.socket()
        c.connect(("127.0.0.1", port))
        c.setblocking(False)
        msg = b"hellopyg"   # 8 bytes
        t0[0] = time.perf_counter()
        if mode == "native":
            fd = c.fileno()
            buf = bytearray(8)
            for _ in range(N):
                pygo_core.tcp_send(fd, msg)
                pygo_core.tcp_recv(fd, buf, 8)
        else:
            for _ in range(N):
                c.sendall(msg)
                c.recv(8)
        result[0] = time.perf_counter() - t0[0]
        c.close()

    pygo_core.go(server)
    pygo_core.go(client)
    pygo_core.run()
    srv.close()
    return result[0]


if __name__ == "__main__":
    for mode in ("monkey", "native"):
        t = bench(N=2000, mode=mode)
        print("{:>6}: {:.3f}s for 2000 RT -> {:.1f} K/s, {:.0f} us/RT".format(
            mode, t, 2000 / t / 1000, t * 1e6 / 2000))
