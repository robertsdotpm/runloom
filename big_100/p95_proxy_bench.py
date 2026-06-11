"""big_100 / 95 -- proxy benchmark harness.

client -> proxy -> backend echo, the full path.  Clients push sized payloads
through and verify the echo while the harness measures round-trip latency and
throughput.  Correctness (every echo matches) plus a latency/throughput report.

Stresses: full-duplex network through three hops, backpressure, measurement.
"""
import socket
import time

import harness
import netutil
import runloom


def pipe(src, dst, done):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        try:
            dst.shutdown(socket.SHUT_WR)
        except OSError:
            pass
        done.send(1)


def proxy_conn(H, client_sock, backend_addr):
    backend = None
    try:
        backend = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        backend.connect(backend_addr)
    except OSError:
        netutil.close_quiet(client_sock)
        netutil.close_quiet(backend)
        return
    done = runloom.Chan(2)
    H.go(pipe, client_sock, backend, done)
    H.go(pipe, backend, client_sock, done)
    done.recv()
    done.recv()
    netutil.close_quiet(client_sock)
    netutil.close_quiet(backend)


def setup(H):
    backend_host = netutil._DEFAULT_HOST
    backend_port = netutil.start_echo_server(H, host=backend_host)
    backend_addr = (backend_host, backend_port)
    servers = netutil.listen_all(
        H, lambda conn, addr: H.go(proxy_conn, H, conn, backend_addr))
    H.state = {"servers": servers,
               "lat_sum": [0.0], "lat_max": [0.0], "lat_n": [0],
               "buckets": [0] * 1024}


def client(H, wid, rng, state):
    servers = state["servers"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        sock = None
        host, port = netutil.pick_server(servers, rng)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            for _ in range(rng.randint(2, 10)):
                if not H.running():
                    break
                payload = rng.randbytes(rng.randint(8, 1024))
                t0 = time.perf_counter()
                sock.sendall(payload)
                got = netutil.recv_exact(sock, len(payload))
                dt = time.perf_counter() - t0
                if not H.check(got == payload,
                               "proxy bench echo mismatch wid={0}".format(wid)):
                    return
                state["buckets"][wid & 1023] += 1
                state["lat_sum"][0] += dt        # racy but fine for a stat
                if dt > state["lat_max"][0]:
                    state["lat_max"][0] = dt
                H.op(wid)
            H.task_done(wid)
        except OSError:
            if not H.running():
                break
            H.sleep(0.005)
        finally:
            netutil.close_quiet(sock)


def body(H):
    H.run_pool(H.funcs, client, H.state)


def post(H):
    n = sum(H.state["buckets"])
    avg = (H.state["lat_sum"][0] / n * 1e6) if n else 0.0
    H.log("round_trips={0} avg_latency={1:.1f}us max_latency={2:.1f}us".format(
        n, avg, H.state["lat_max"][0] * 1e6))


if __name__ == "__main__":
    harness.main("p95_proxy_bench", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="client->proxy->backend throughput/latency + correctness")
