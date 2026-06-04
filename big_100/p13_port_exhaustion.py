"""big_100 / 13 -- port exhaustion test.

Tens of thousands of goroutines open and immediately close outbound
connections to a local server as fast as they can, churning ephemeral ports
and fds.  We watch for two failure modes: leaked sockets (the live fd count
must stay bounded) and a collapse in connection success rate from TIME_WAIT /
ephemeral-port exhaustion (counted, not fatal -- that is the phenomenon under
study).

Stresses: resource cleanup on every path, error propagation.
"""
import socket

import harness
import netutil


def setup(H):
    port = netutil.start_echo_server(H)
    H.state = {"port": port}
    H.refused = [0]
    H.fd_ceiling = 0


def client(H, wid, rng, state):
    port = state["port"]
    H.sleep(rng.random() * 0.3)
    while H.running():
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("127.0.0.1", port))
            # Touch the connection so it actually establishes, then drop it.
            sock.sendall(b"x")
            sock.recv(16)
            H.op(wid)
            H.task_done(wid)
        except OSError:
            # ECONNREFUSED / EADDRNOTAVAIL under ephemeral exhaustion: counted,
            # not a fatal invariant -- it is exactly what this test provokes.
            H.refused[0] += 1
            if not H.running():
                break
            H.sleep(0.002)
        finally:
            netutil.close_quiet(sock)


def body(H):
    H.run_pool(H.funcs, client, H.state)

    def fd_auditor():
        base = harness.count_fds()
        while H.running():
            fds = harness.count_fds()
            if fds > H.fd_ceiling:
                H.fd_ceiling = fds
            # A genuine fd leak would climb without bound; the working set is
            # roughly the live connections plus the offload pool.  Allow a
            # generous headroom over the worker count, then call it a leak.
            H.check(fds < base + H.funcs * 3 + 5000,
                    "fd leak: {0} open fds (base {1}, funcs {2})".format(
                        fds, base, H.funcs))
            H.sleep(1.0)
        H.log("fd_ceiling={0} refused={1}".format(H.fd_ceiling, H.refused[0]))

    H.go(fd_auditor)


if __name__ == "__main__":
    harness.main("p13_port_exhaustion", body, setup=setup, default_funcs=10000,
                 describe="rapid outbound connect/close; watch fd leaks + TIME_WAIT")
