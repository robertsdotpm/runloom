#!/usr/bin/env python3
"""bench_keepalive_py.py -- steady-state keepalive workload (T3.1 pygo target).

Unlike bench_server_py.py (fire-and-close echo), this models the N=1M
steady state honest-bench targets: many LONG-LIVED connections that each
interleave a request/response round-trip with a think-time idle gap, so
at any instant only a small fraction are active and the rest are parked
on netpoll holding a stack.  That interleaved active/idle pattern is the
one a per-coro predictor can't handle and the dwell-based sweep can, so
it's the right load to answer the open question: does the sweep keep RSS
low WITHOUT inflating tail latency (the wake-refault cost)?

STEADY-STATE WINDOW (the fix over the old fixed-`cycles` design).  The
previous version ran a fixed number of cycles per connection and skipped
only cycle 0, so the multi-second connection ramp polluted the tail: a
late-establishing connection was still in its first cycles while an
early one was already draining, and the population was NEVER uniformly
in steady state during measurement.  This version drives three explicit
wall-clock phases off a single t0:

    ramp    [t0,            t0+ramp_s)              connections establish, staggered
    warmup  [t0+ramp_s,     t0+ramp_s+warmup_s)     all up + cycling, NOT recorded
    measure [measure_start, measure_start+measure_s) record round-trips here only

Each connection runs a continuous request/think loop until measure_end,
and a latency is recorded ONLY for a round-trip whose response lands
inside [measure_start, measure_end).  Establishment (ramp) finishes
before measure_start and connections are cut at measure_end, so neither
the ramp nor the drain can pollute the percentiles -- the samples are a
clean snapshot of the all-N-established steady state.  This is what lets
us attribute (or dismiss) the sweep's N>=65K p99 residual.

    PYTHONPATH=src ~/.pyenv/versions/3.13.13t/bin/python3 \
        tests_c/bench_keepalive_py.py 16384 8 1000 5 3 3 10

Args: N [H] [think_ms] [work_ms] [ramp_s] [warmup_s] [measure_s]
  N         long-lived connections
  H         hubs
  think_ms  idle gap between a connection's requests (its parked time)
  work_ms   median server-side "DB" latency per request
  ramp_s    stagger establishment over this window
  warmup_s  run-but-don't-record window after the ramp completes
  measure_s steady-state window over which latencies + RSS are sampled

Compare PYGO_STACK_PARK_SWEEP=1 vs unset to read off the RSS reclaim and
its (expected ~0) tail-latency cost on a clean steady-state window.
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
THINK_S = 1.000
WORK_S = 0.005
RAMP_S = 3.0
WARMUP_S = 3.0
MEASURE_S = 10.0

# Absolute monotonic deadlines, set in main() once t0 is fixed.
T0 = 0.0
MEASURE_START_T = 0.0
MEASURE_END_T = 0.0

latencies = []          # latencies[idx] = list of in-window per-request seconds
succeeded = []          # succeeded[idx] = ran to measure_end without error
established = []         # 1 per connection that completed connect()
steady_rss_kib = -1      # peak VmRSS sampled across the measure window


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
def server_conn(conn, idx):
    conn.setblocking(False)
    fd = conn.fileno()
    seq = 0
    try:
        while True:
            req = _recv_exactly(conn, fd, REQ_LEN)
            if not req:
                break
            pygo_core.sched_sleep(_db_latency_s(idx * 131 + seq))   # "DB"
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
    # Bounded by measure_end so a client that fails to establish can't
    # leave this loop waiting forever for the N-th accept (which would
    # hang mn_run).  Finite (50 ms) wait => self-healing re-drain of the
    # backlog regardless of any delayed listener-readiness edge.
    while accepted < N and time.monotonic() < MEASURE_END_T:
        try:
            conn, _ = listen_sock.accept()
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(lfd, READ, 50)
            continue
        except OSError:
            break
        while True:
            conn.setblocking(False)
            try:
                pygo_core.netpoll_unregister(conn.fileno())
            except (AttributeError, OSError):
                pass
            cidx = accepted
            accepted += 1
            pygo_core.mn_go(lambda c=conn, i=cidx: server_conn(c, i))
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
    # herd at t0 (a cold-start burst that otherwise dominates the tail).
    # Establishment lands before MEASURE_START_T, so it can't pollute the
    # measured window.
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

    established.append(1)
    mine = latencies[idx]
    # Continuous request/think loop until the steady-state window closes.
    # A round-trip is recorded ONLY if its response lands inside the
    # measure window -- ramp-phase and drain-phase round-trips are not.
    while time.monotonic() < MEASURE_END_T:
        t0 = time.monotonic()
        if not _send_all(s, fd, REQ):
            _rst_close(s)
            return False
        if _recv_exactly(s, fd, RESP_LEN) != RESP:
            _rst_close(s)
            return False
        t1 = time.monotonic()
        if MEASURE_START_T <= t1 < MEASURE_END_T:
            mine.append(t1 - t0)
        pygo_core.sched_sleep(THINK_S)        # think-time: parked + idle
    _rst_close(s)
    return True


def client(idx):
    succeeded[idx] = _client_body(idx)


def sampler():
    """Track peak VmRSS across the steady-state window."""
    global steady_rss_kib
    while time.monotonic() < MEASURE_START_T:
        pygo_core.sched_sleep(0.05)
    peak = -1
    while time.monotonic() < MEASURE_END_T:
        cur = _cur_rss_kib()
        if cur > peak:
            peak = cur
        pygo_core.sched_sleep(0.1)
    steady_rss_kib = peak


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
    global N, THINK_S, WORK_S, RAMP_S, WARMUP_S, MEASURE_S, PORT, listen_sock
    global latencies, succeeded, established
    global T0, MEASURE_START_T, MEASURE_END_T
    N = int(argv[1]) if len(argv) > 1 else 1024
    H = int(argv[2]) if len(argv) > 2 else 8
    THINK_S = (float(argv[3]) if len(argv) > 3 else 1000.0) / 1000.0
    WORK_S = (float(argv[4]) if len(argv) > 4 else 5.0) / 1000.0
    RAMP_S = (float(argv[5]) if len(argv) > 5 else 3.0)
    WARMUP_S = (float(argv[6]) if len(argv) > 6 else 3.0)
    MEASURE_S = (float(argv[7]) if len(argv) > 7 else 10.0)
    latencies = [[] for _ in range(N)]
    succeeded = [False] * N
    established = []

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

    # PYGO_BENCH_STACK=<bytes>: pin the goroutine stack size (freezes
    # calibration) so we can measure RSS vs stack allocation size.
    _bench_stack = os.environ.get("PYGO_BENCH_STACK")
    if _bench_stack:
        pygo_core.set_stack_size(int(_bench_stack))

    if pygo_core.mn_init(H) < 0:
        sys.stderr.write("mn_init failed\n")
        return 2

    T0 = time.monotonic()
    MEASURE_START_T = T0 + RAMP_S + WARMUP_S
    MEASURE_END_T = MEASURE_START_T + MEASURE_S
    pygo_core.mn_go(accept_loop)
    pygo_core.mn_go(sampler)
    for i in range(N):
        pygo_core.mn_go(lambda i=i: client(i))
    pygo_core.mn_run()

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    pygo_core.mn_fini()
    listen_sock.close()

    allat = []
    for lst in latencies:
        allat.extend(lst)
    allat.sort()
    nreq = len(allat)
    done = sum(1 for ok in succeeded if ok)
    est = len(established)
    p50 = _percentile(allat, 0.50) * 1000.0
    p99 = _percentile(allat, 0.99) * 1000.0
    p999 = _percentile(allat, 0.999) * 1000.0
    # In-window throughput: samples are confined to the measure window, so
    # this is the steady-state rps (not diluted by ramp/drain).
    win_rps = nreq / MEASURE_S if MEASURE_S > 0 else 0.0
    # Effective steady-state concurrency (Little's law): rps * mean_latency.
    mean_lat = (sum(allat) / nreq) if nreq else 0.0
    eff_conc = win_rps * mean_lat
    util = 100.0 * eff_conc / N if N else 0.0
    print("N=%d H=%d think_ms=%.0f work_ms=%.1f ramp=%.0f warmup=%.0f measure=%.0f "
          "established=%d done=%d/%d win_reqs=%d %.0frps util=%.0f%% "
          "p50=%.1fms p99=%.1fms p99.9=%.1fms "
          "peak_rss_kib=%d steady_rss_kib=%d nofile=%d"
          % (N, H, THINK_S * 1000, WORK_S * 1000, RAMP_S, WARMUP_S, MEASURE_S,
             est, done, N, nreq, win_rps, util, p50, p99, p999,
             peak, steady_rss_kib, nofile))
    return 0 if done == N else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
