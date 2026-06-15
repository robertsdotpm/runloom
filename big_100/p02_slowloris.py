"""big_100 / 02 -- TCP slowloris simulator.

A local request/reply server.  Most goroutines are "fast" clients doing quick
request/response round-trips; a large fraction are "slow" clients that hold a
connection open and dribble one byte every few seconds without ever completing
a request.  The server must keep serving the fast clients -- a fast round-trip
that ever exceeds the fairness bound means the slow clients starved everyone.

Stresses: blocking reads, scheduler fairness, idle sockets, timers.

SCALE NOTE (macOS arm64, 8-core, big_100 100k sweep -- see
docs/dev/BIG100_100K_MAC.md): this is the ONLY big_100 program with a hard
WALL-CLOCK latency bound (FAIRNESS_BOUND), so unlike the throughput/correctness
programs it is HARDWARE-bound, not just goroutine-count-bound.  It PASSES at its
8k design scale; it FAILS the bound at ~20k+ (10.0s) and 100k (11.8s) on an
8-core box -- but that is RESOURCE EXHAUSTION, not a scheduler fairness defect:
~50k FAST clients each needing CPU for a round-trip on 8 cores simply cannot all
finish a round-trip in <10s (the dribbling SLOW clients barely use CPU -- they're
parked between dribbles -- so they are NOT what starves the fast ones; the cores
are).  Widening servers 8->64 at 100k only moved 11.8s->10.2s, confirming it is
CPU scarcity, not accept-backlog or per-hub wake latency.  So drive this program
at a scale the box can SERVE (its latency-fairness ceiling is ~10-15k on this
hardware), distinct from the correctness/scale ceiling (100k+) the other programs
hit.  The runtime is correct here at every scale (all clients complete, every
echo matches); only the wall-clock SLO is hardware-relative.
"""
import socket
import time

import harness
import netutil

# Seconds; a fast round-trip slower than this counts as starvation.  HARDWARE-
# RELATIVE: achievable only while the box can actually service the fast-client
# population (see the SCALE NOTE above).  Beyond ~2x the 8k design scale on an
# 8-core box this bound is exceeded by CPU scarcity, not a scheduler defect.
FAIRNESS_BOUND = 10.0


def server_handler(conn):
    try:
        while True:
            line = netutil.recv_until(conn, b"\n")
            # One reply per newline-terminated request.
            conn.sendall(b"OK " + line)
    except OSError:
        pass
    finally:
        netutil.close_quiet(conn)


def setup(H):
    # One server per loopback IP so the connect storm spreads across many accept
    # loops (a single accept loop serializes and wedges under a large storm).
    servers = netutil.listen_all(H, lambda conn, addr: H.go(server_handler, conn))
    H.state = {"servers": servers}


def fast_client(H, wid, rng, state):
    servers = state["servers"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        sock = None
        host, port = netutil.pick_server(servers, rng)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            for _ in range(rng.randint(1, 20)):
                if not H.running():
                    break
                t0 = time.monotonic()
                sock.sendall(b"ping\n")
                reply = netutil.recv_until(sock, b"\n")
                dt = time.monotonic() - t0
                H.check(reply.startswith(b"OK "),
                        "bad reply wid={0}: {1!r}".format(wid, reply[:32]))
                H.check(dt < FAIRNESS_BOUND,
                        "starvation: fast round-trip took {0:.1f}s "
                        "(wid={1})".format(dt, wid))
                H.op(wid)
                H.sleep(rng.random() * 0.05)
            H.task_done(wid)
        except OSError:
            if not H.running():
                break
            H.sleep(0.01)
        finally:
            netutil.close_quiet(sock)


def slow_client(H, wid, rng, state):
    servers = state["servers"]
    H.sleep(rng.random() * 1.0)
    while H.running():
        sock = None
        host, port = netutil.pick_server(servers, rng)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            # Dribble a never-completed request: one byte every few seconds.
            for _ in range(rng.randint(20, 60)):
                if not H.running():
                    break
                sock.sendall(b"x")          # no newline -> server keeps waiting
                H.op(wid)                    # progress is "bytes dribbled"
                H.sleep(2.0 + rng.random() * 3.0)
        except OSError:
            pass
        finally:
            netutil.close_quiet(sock)
        H.sleep(0.05)


def body(H):
    slow = H.funcs // 2
    fast = H.funcs - slow
    H.run_pool(slow, slow_client, H.state)
    H.run_pool(fast, fast_client, H.state)


if __name__ == "__main__":
    harness.main("p02_slowloris", body, setup=setup, default_funcs=8000,
                 describe="slowloris: dribbling clients must not starve fast ones")
