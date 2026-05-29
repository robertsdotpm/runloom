#!/usr/bin/env python3
"""bench_keepalive_py.py -- realistic keepalive workload (T3.1 pygo target).

Unlike bench_server_py.py (fire-and-close echo), this models the N=1M
steady state honest-bench targets: many LONG-LIVED connections that each
interleave a request/response round-trip with a think-time idle gap, so
at any instant only a small fraction are active and the rest are parked
on netpoll holding a stack.  That interleaved active/idle pattern is the
one a per-coro predictor can't handle and the dwell-based sweep can, so
it's the right load to answer the open question: does the sweep keep RSS
low WITHOUT inflating tail latency (the wake-refault cost)?

Each connection runs `cycles` of: send a request, the server sleeps a
seeded "DB" latency then echoes a response, the client records the
round-trip latency, then sleeps `think_ms`.  Per-request latencies are
collected per-connection (distinct list per index -> lock-free under
3.13t) and reduced to p50/p99/p99.9 after the run.  A sampler goroutine
snapshots current RSS mid-run.

    PYTHONPATH=src ~/.pyenv/versions/3.13.13t/bin/python3 \
        tests_c/bench_keepalive_py.py 16384 8 10 100 5

Args: N [H] [cycles] [think_ms] [work_ms]
  N         long-lived connections
  H         hubs
  cycles    request/idle cycles per connection
  think_ms  idle gap between a connection's requests (its parked time)
  work_ms   median server-side "DB" latency per request

Compare PYGO_STACK_PARK_SWEEP=1 vs unset to read off the RSS reclaim and
its (expected ~0) tail-latency cost.
"""
import os
import resource
import socket
import struct
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import pygo_core

REQ = b"GET /work\n"
REQ_LEN = len(REQ)
RESP = b"200 " + b"x" * 1024 + b"\n"      # ~1 KB response
RESP_LEN = len(RESP)

READ = 1
WRITE = 2
NUM_SRC_IPS = 250
_LINGER_RST = struct.pack("ii", 1, 0)

HOST = "127.0.0.1"
PORT = 0
listen_sock = None
N = 1024
CYCLES = 10
THINK_S = 0.100
WORK_S = 0.005
RAMP_S = 2.0

latencies = []          # latencies[idx] = list of per-request seconds
succeeded = []          # succeeded[idx] = True if the connection ran all cycles
ready = []              # connections that have established (sampler barrier)
idle_rss_kib = -1


def _rst_close(sock):
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, _LINGER_RST)
    except OSError:
        pass
    fd = -1
    try:
        fd = sock.fileno()
    except (OSError, ValueError):
        pass
    if fd >= 0:
        try:
            pygo_core.netpoll_unregister(fd)
        except (AttributeError, OSError):
            pass
    try:
        sock.close()
    except OSError:
        pass


def _recv_exactly(sock, fd, n):
    out = bytearray()
    while len(out) < n:
        try:
            chunk = sock.recv(n - len(out))
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(fd, READ)
            continue
        except OSError:
            return b""
        if not chunk:
            return b""
        out += chunk
    return bytes(out)


def _send_all(sock, fd, data):
    view = memoryview(data)
    sent = 0
    while sent < len(view):
        try:
            sent += sock.send(view[sent:])
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(fd, WRITE)
        except OSError:
            return False
    return True


def _db_latency_s(seq):
    """Seeded DB-latency draw: median WORK_S, ~1% 10x, ~0.05% 100x."""
    r = (seq * 2654435761) & 0xFFFFFFFF      # cheap hash for a stable spread
    bucket = r % 10000
    if bucket < 5:          # 0.05% pathological
        return WORK_S * 100.0
    if bucket < 105:        # ~1% slow
        return WORK_S * 10.0
    return WORK_S


# ---- server-side per-connection handler ----
def server_conn(conn):
    conn.setblocking(False)
    fd = conn.fileno()
    seq = 0
    try:
        while True:
            req = _recv_exactly(conn, fd, REQ_LEN)
            if not req:
                break
            pygo_core.sched_sleep(_db_latency_s(fd * 131 + seq))   # "DB"
            seq += 1
            if not _send_all(conn, fd, RESP):
                break
    finally:
        f2 = -1
        try:
            f2 = conn.fileno()
        except (OSError, ValueError):
            pass
        if f2 >= 0:
            try:
                pygo_core.netpoll_unregister(f2)
            except (AttributeError, OSError):
                pass
        try:
            conn.close()
        except OSError:
            pass


# ---- server accept loop ----
def accept_loop():
    lfd = listen_sock.fileno()
    accepted = 0
    while accepted < N:
        try:
            conn, _ = listen_sock.accept()
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(lfd, READ)
            continue
        except OSError:
            break
        while True:
            conn.setblocking(False)
            try:
                pygo_core.netpoll_unregister(conn.fileno())
            except (AttributeError, OSError):
                pass
            accepted += 1
            pygo_core.mn_go(lambda c=conn: server_conn(c))
            if accepted >= N:
                break
            try:
                conn, _ = listen_sock.accept()
            except (BlockingIOError, InterruptedError):
                break
            except OSError:
                accepted = N
                break
    try:
        pygo_core.netpoll_unregister(lfd)
    except (AttributeError, OSError):
        pass


# ---- client connection (long-lived, keepalive) ----
def _client_body(idx):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setblocking(False)
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.%d" % (2 + idx % NUM_SRC_IPS), 0))
    except OSError:
        pass
    # Ramp: stagger connection establishment over RAMP_S so all N don't
    # herd at t=0 (a cold-start burst that otherwise dominates the tail
    # and hides the steady-state SLO).
    if RAMP_S > 0.0 and N > 1:
        pygo_core.sched_sleep((idx / float(N)) * RAMP_S)
    fd = s.fileno()
    try:
        s.connect((HOST, PORT))
    except BlockingIOError:
        pygo_core.wait_fd(fd, WRITE)
        if s.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR) != 0:
            _rst_close(s)
            return False
    except OSError:
        _rst_close(s)
        return False

    ready.append(1)
    mine = latencies[idx]
    for c in range(CYCLES):
        t0 = time.monotonic()
        if not _send_all(s, fd, REQ):
            _rst_close(s)
            return False
        if _recv_exactly(s, fd, RESP_LEN) != RESP:
            _rst_close(s)
            return False
        # Skip cycle 0 (warmup / ramp phase) from the SLO stats.
        if c > 0:
            mine.append(time.monotonic() - t0)
        pygo_core.sched_sleep(THINK_S)        # think-time: parked + idle
    _rst_close(s)
    return True


def client(idx):
    succeeded[idx] = _client_body(idx)


def sampler():
    """Snapshot RSS once most connections are mid-run (steady state)."""
    global idle_rss_kib
    while len(ready) < N:
        pygo_core.sched_sleep(0.01)
    # Sample partway through the cycle loop -- connections established,
    # most parked in think-time.
    pygo_core.sched_sleep(min(1.0, THINK_S * CYCLES * 0.3))
    idle_rss_kib = _cur_rss_kib()


def _cur_rss_kib():
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except (OSError, ValueError):
        pass
    return -1


def _percentile(sorted_vals, q):
    if not sorted_vals:
        return 0.0
    i = int(q * (len(sorted_vals) - 1) + 0.5)
    if i >= len(sorted_vals):
        i = len(sorted_vals) - 1
    return sorted_vals[i]


def main(argv):
    global N, CYCLES, THINK_S, WORK_S, RAMP_S, PORT, listen_sock
    global latencies, succeeded
    N = int(argv[1]) if len(argv) > 1 else 1024
    H = int(argv[2]) if len(argv) > 2 else 8
    CYCLES = int(argv[3]) if len(argv) > 3 else 10
    THINK_S = (float(argv[4]) if len(argv) > 4 else 100.0) / 1000.0
    WORK_S = (float(argv[5]) if len(argv) > 5 else 5.0) / 1000.0
    RAMP_S = (float(argv[6]) if len(argv) > 6 else 2.0)
    latencies = [[] for _ in range(N)]
    succeeded = [False] * N

    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (1 << 20, 1 << 20))
    except (ValueError, OSError) as e:
        sys.stderr.write("setrlimit NOFILE: %s (continuing)\n" % e)
    nofile = resource.getrlimit(resource.RLIMIT_NOFILE)[0]

    listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listen_sock.bind((HOST, 0))
    listen_sock.listen(min(N, 65535))
    listen_sock.setblocking(False)
    PORT = listen_sock.getsockname()[1]

    if pygo_core.mn_init(H) < 0:
        sys.stderr.write("mn_init failed\n")
        return 2

    t0 = time.monotonic()
    pygo_core.mn_go(accept_loop)
    pygo_core.mn_go(sampler)
    for i in range(N):
        pygo_core.mn_go(lambda i=i: client(i))
    pygo_core.mn_run()
    dt = time.monotonic() - t0

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    pygo_core.mn_fini()
    listen_sock.close()

    allat = []
    for lst in latencies:
        allat.extend(lst)
    allat.sort()
    nreq = len(allat)
    done = sum(1 for ok in succeeded if ok)
    p50 = _percentile(allat, 0.50) * 1000.0
    p99 = _percentile(allat, 0.99) * 1000.0
    p999 = _percentile(allat, 0.999) * 1000.0
    rps = nreq / dt if dt > 0 else 0.0
    print("N=%d H=%d cycles=%d think_ms=%.0f work_ms=%.1f "
          "done=%d/%d reqs=%d %.2fs %.0frps "
          "p50=%.1fms p99=%.1fms p99.9=%.1fms "
          "peak_rss_kib=%d idle_rss_kib=%d nofile=%d"
          % (N, H, CYCLES, THINK_S * 1000, WORK_S * 1000,
             done, N, nreq, dt, rps, p50, p99, p999,
             peak, idle_rss_kib, nofile))
    return 0 if done == N else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
