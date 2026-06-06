"""big_100 / 34 -- mixed subprocess and network proxy.

A local TCP server whose handlers shell out to a subprocess to answer each
request: the client sends `square N`, the handler runs a one-shot `python -c`
that prints N*N (built off-goroutine via procutil to dodge FINDINGS BUG #4),
and returns the result.  Sockets, pipes and process churn all interleave.

Stresses: sockets + pipes + subprocess + cancellation together.

NOTE: at much higher --funcs the per-request spawn rate can still trip BUG #4
(offload-result lost wakeup); the default keeps the concurrent spawn count in
the reliable range.
"""
import socket
import subprocess

import procutil

import harness
import netutil

CALC = "import sys; n=int(sys.argv[1]); print(n*n)"


def setup(H):
    import sys
    srv = netutil.listen_tcp()
    H.state = {"port": srv.getsockname()[1], "py": sys.executable}
    py = sys.executable

    def handler(conn):
        try:
            while True:
                line = netutil.recv_until(conn, b"\n")
                parts = line.split()
                if len(parts) != 2 or parts[0] != b"square":
                    conn.sendall(b"ERR\n")
                    continue
                n = int(parts[1])
                proc = procutil.popen([py, "-c", CALC, str(n)],
                                      stdout=subprocess.PIPE,
                                      running=H.running)
                out, _ = proc.communicate()
                conn.sendall(out.strip() + b"\n")
        except (OSError, ValueError):
            pass
        finally:
            netutil.close_quiet(conn)

    H.go(netutil.serve_forever, H, srv,
         lambda conn, addr: H.go(handler, conn))


def client(H, wid, rng, state):
    port = state["port"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("127.0.0.1", port))
            for _ in range(rng.randint(1, 4)):
                if not H.running():
                    break
                n = rng.randint(0, 100000)
                sock.sendall("square {0}\n".format(n).encode())
                reply = netutil.recv_until(sock, b"\n").strip()
                if not H.check(int(reply) == n * n,
                               "wrong square wid={0}: {1} != {2}".format(
                                   wid, reply, n * n)):
                    return
                H.op(wid)
            H.task_done(wid)
        except (OSError, ValueError):
            if not H.running():
                break
            H.sleep(0.01)
        finally:
            netutil.close_quiet(sock)


def body(H):
    H.run_pool(H.funcs, client, H.state)


if __name__ == "__main__":
    harness.main("p34_subproc_net", body, setup=setup, default_funcs=120,
                 describe="TCP requests answered by a one-shot subprocess each")
