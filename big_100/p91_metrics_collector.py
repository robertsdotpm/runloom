"""big_100 / 91 -- metrics collector.

A collector ingests "name:value" metrics over BOTH a UDP and a TCP endpoint and
aggregates per-name sums; an aggregator goroutine periodically writes a summary
file.  TCP is reliable, so the aggregated TCP totals must exactly equal what the
TCP clients sent (conservation); UDP is best-effort and only counted.

Stresses: timers, UDP + TCP sockets at once, periodic file writes, locked
aggregation.
"""
import os
import socket
import threading

import harness
import netutil


def setup(H):
    tcp = netutil.listen_tcp()
    udp = netutil.udp_socket()
    udp.setblocking(False)
    summary = os.path.join(H.make_tmpdir("big100_metrics_"), "summary.txt")
    # tcp_sent: one slot per goroutine (indexed by wid) so no two TCP clients
    # ever share a slot.  Avoids the data race that loses updates when goroutines
    # share a slot under GIL=0 (free-threaded list[i] += x is not atomic).
    half = H.funcs // 2
    state = {
        "tcp_port": tcp.getsockname()[1],
        "tcp_host": tcp.getsockname()[0],
        "udp_addr": udp.getsockname(),
        "lock": threading.Lock(),
        "tcp_sum": [0], "tcp_sent": [0] * half,
        "udp_count": [0], "summary": summary,
    }
    H.state = state

    def ingest(line):
        try:
            name, val = line.split(b":")
            return name, int(val)
        except ValueError:
            return None, 0

    def tcp_handle(conn):
        buf = bytearray()
        try:
            while True:
                if b"\n" not in buf:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    buf += chunk
                    continue
                nl = buf.index(b"\n")
                line = bytes(buf[:nl])
                del buf[:nl + 1]
                name, val = ingest(line)
                if name is not None:
                    with state["lock"]:
                        state["tcp_sum"][0] += val
                # ACK after ingesting so the client only counts data the server
                # has actually accumulated (no loss from un-accepted backlog).
                conn.sendall(b"OK\n")
        except OSError:
            pass
        finally:
            netutil.close_quiet(conn)

    H.go(netutil.serve_forever, H, tcp,
         lambda conn, addr: H.go(tcp_handle, conn))

    def udp_server():
        try:
            while H.running():
                data, _addr = netutil.udp_recvfrom_timeout(udp, 2048, 300)
                if data is None:
                    continue
                name, val = ingest(data)
                if name is not None:
                    with state["lock"]:
                        state["udp_count"][0] += 1
        finally:
            netutil.close_quiet(udp)

    H.go(udp_server)

    def aggregator():
        while H.running():
            H.sleep(1.0)
            with state["lock"]:
                ts, uc = state["tcp_sum"][0], state["udp_count"][0]
            try:
                with open(summary, "w") as f:
                    f.write("tcp_sum={0}\nudp_count={1}\n".format(ts, uc))
            except OSError:
                pass

    H.go(aggregator)


def tcp_client(H, wid, rng, state):
    port = state["tcp_port"]
    host = state["tcp_host"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            buf = bytearray()
            for _ in range(rng.randint(5, 40)):
                if not H.running():
                    break
                v = rng.randint(1, 100)
                sock.sendall("m{0}:{1}\n".format(wid, v).encode())
                # Wait for the ACK -> the server has counted this metric.
                if netutil.recv_line_timeout(sock, 2000, buf) is netutil.TIMEOUT:
                    break
                state["tcp_sent"][wid] += v
                H.op(wid)
            H.task_done(wid)
        except OSError:
            if not H.running():
                break
            H.sleep(0.005)
        finally:
            netutil.close_quiet(sock)


def udp_client(H, wid, rng, state):
    addr = state["udp_addr"]
    sock = netutil.udp_socket()
    sock.setblocking(False)
    try:
        H.sleep(rng.random() * 0.5)
        while H.running():
            try:
                sock.sendto("u{0}:{1}\n".format(wid, rng.randint(1, 100))
                            .encode(), addr)
            except OSError:
                pass
            H.op(wid)
            H.task_done(wid)
            H.sleep(0.001)
    finally:
        netutil.close_quiet(sock)


def body(H):
    half = H.funcs // 2
    H.run_pool(half, tcp_client, H.state)
    H.run_pool(H.funcs - half, udp_client, H.state)


def post(H):
    # Server counts a metric, THEN acks; the client records only on ack.  So
    # every acked metric was counted (got >= sent: no loss), and the gap is the
    # in-flight metrics at teardown -- at most one per client (synchronous
    # protocol), each <= 100.
    sent = sum(H.state["tcp_sent"])
    got = H.state["tcp_sum"][0]
    gap = got - sent
    nclients = H.funcs // 2 + 1
    H.check(0 <= gap <= nclients * 100,
            "TCP conservation broken: got {0}, sent {1}, gap {2} (bound {3})"
            .format(got, sent, gap, nclients * 100))
    H.log("tcp_sum={0} tcp_sent={1} gap={2} udp_count={3}".format(
        got, sent, gap, H.state["udp_count"][0]))


if __name__ == "__main__":
    harness.main("p91_metrics_collector", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="UDP+TCP metric ingest, periodic summary, TCP conservation")
