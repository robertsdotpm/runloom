"""big_100 / 81 -- mini Redis clone.

A line-protocol TCP server with a single shared keyspace and GET/SET/DEL/INCR.
Each client exercises its own private keys (SET/GET/DEL round-trips) AND hammers
one shared counter with INCR; at the end the shared counter must equal the total
number of INCRs issued across all clients -- the server's command processing has
to be atomic under heavy concurrency.

Stresses: network, a lock-guarded shared keyspace, parsing, INCR atomicity.
"""
import threading

import harness
import netutil

SHARED = b"shared:counter"

def setup(H):
    host = H.net_ips[0]
    srv = netutil.listen_tcp(host=host)
    store = {}
    lock = threading.Lock()
    # One slot per goroutine (indexed by wid) to avoid the data race that
    # loses updates when multiple goroutines share a slot under GIL=0.
    H.state = {"port": srv.getsockname()[1], "host": host,
               "incrs": [0] * H.funcs, "store": store}

    def handle(conn):
        try:
            while True:
                line = netutil.recv_until(conn, b"\n").rstrip(b"\n")
                parts = line.split(b" ", 2)
                cmd = parts[0].upper()
                if cmd == b"SET" and len(parts) == 3:
                    with lock:
                        store[parts[1]] = parts[2]
                    conn.sendall(b"OK\n")
                elif cmd == b"GET" and len(parts) >= 2:
                    with lock:
                        v = store.get(parts[1])
                    conn.sendall(b"VALUE " + v + b"\n" if v is not None
                                 else b"NIL\n")
                elif cmd == b"DEL" and len(parts) >= 2:
                    with lock:
                        existed = store.pop(parts[1], None) is not None
                    conn.sendall(b"INT 1\n" if existed else b"INT 0\n")
                elif cmd == b"INCR" and len(parts) >= 2:
                    with lock:
                        n = int(store.get(parts[1], b"0")) + 1
                        store[parts[1]] = str(n).encode()
                    conn.sendall(b"INT " + str(n).encode() + b"\n")
                else:
                    conn.sendall(b"ERR\n")
        except (OSError, ValueError):
            pass
        finally:
            netutil.close_quiet(conn)

    H.go(netutil.serve_forever, H, srv,
         lambda conn, addr: H.go(handle, conn))


def client(H, wid, rng, state):
    import socket
    port = state["port"]
    host = state["host"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            # private-key round-trip
            key = "c{0}:{1}".format(wid, rng.randint(0, 99)).encode()
            val = str(rng.randint(0, 1 << 30)).encode()
            sock.sendall(b"SET " + key + b" " + val + b"\n")
            if not H.check(netutil.recv_until(sock, b"\n") == b"OK\n",
                           "SET failed wid={0}".format(wid)):
                return
            sock.sendall(b"GET " + key + b"\n")
            r = netutil.recv_until(sock, b"\n").rstrip(b"\n")
            if not H.check(r == b"VALUE " + val,
                           "GET mismatch wid={0}: {1!r}".format(wid, r)):
                return
            sock.sendall(b"DEL " + key + b"\n")
            netutil.recv_until(sock, b"\n")
            # shared INCR
            sock.sendall(b"INCR " + SHARED + b"\n")
            r = netutil.recv_until(sock, b"\n")
            if not H.check(r.startswith(b"INT "),
                           "INCR reply bad wid={0}: {1!r}".format(wid, r)):
                return
            state["incrs"][wid] += 1
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
    # The store dict outlives the (now torn-down) server, so read it directly.
    total = sum(H.state["incrs"])
    val = int(H.state["store"].get(SHARED, b"0"))
    H.check(val == total,
            "INCR not atomic: counter {0} != total INCRs {1}".format(
                val, total))
    H.log("shared_counter={0} total_incrs={1}".format(val, total))


if __name__ == "__main__":
    harness.main("p81_mini_redis", body, setup=setup, post=post,
                 default_funcs=4000,
                 describe="mini Redis GET/SET/DEL/INCR; INCR stays atomic")
