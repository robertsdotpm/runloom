"""big_100 / 215 -- netpoll re-arm storm.

An echo server on H.net_ip(0).  Each client connection performs K read/write
round-trips before closing -- re-arming the one-shot netpoll registration K
times on the SAME fd -- then the worker loops and opens a FRESH connection
(churning fd numbers).  Every round-trip must get the exact echoed bytes back; a
missed readiness (a one-shot arm that wasn't re-armed) would hang that recv and
the watchdog would fire.

Stresses: repeated EPOLLONESHOT re-arm on a live fd, the per-fd arm cache across
many round-trips and across fast fd recycling.  Fully local (loopback).
"""
import socket
import struct

import harness
import netutil
import runloom

K = 8       # round-trips per connection before it is closed


def connection_burst(H, addr, wid, seq0):
    """One connection: K tagged round-trips on the same fd, then close.  Returns
    the number of correct round-trips (K on full success), or -1 on a mismatch."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    done = 0
    try:
        s.connect(addr)
        for k in range(K):
            if not H.running():
                break
            tag = struct.pack("<III", 0xBEEF0000 ^ wid, seq0 + k, k)
            s.sendall(tag)
            got = netutil.recv_exact(s, len(tag))
            if got != tag:
                return -1
            done += 1
    except OSError:
        pass
    finally:
        netutil.close_quiet(s)
    return done


def worker(H, wid, rng, state):
    addr = (state["host"], state["port"])
    H.sleep(rng.random() * 0.3)
    seq = 0
    for _ in H.round_range():
        n = connection_burst(H, addr, wid, seq)
        seq += K
        if n == -1:
            H.check(False, "echo mismatch (netpoll re-arm lost a readiness) wid={0}".format(wid))
            return
        if n > 0:
            H.op(wid, n)
        H.task_done(wid)


def setup(H):
    host = H.net_ip(0)
    port = netutil.start_echo_server(H, host=host)
    H.state = {"host": host, "port": port}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    H.check(H.total_ops() > 0, "no round-trips completed")
    H.log("round_trips={0}".format(H.total_ops()))


if __name__ == "__main__":
    harness.main("p215_netpoll_rearm_storm", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="K round-trips per connection (re-arm the one-shot "
                          "netpoll K times) then churn a fresh fd; no lost readiness")
