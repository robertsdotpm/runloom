#!/usr/bin/env python3
"""bench_server_py.py -- Python-handler twin of tests_c/bench_server_runloom.

Same topology as the C bench (one accept-loop goroutine, N per-connection
echo handlers, N clients, all on the M:N scheduler), but every goroutine
runs a real Python `def` instead of a C function.  That matters for one
reason: a goroutine only allocates a CPython `_PyStackChunk` (the 4 KB
datastack chunk) once it executes Python *bytecode*.  The pure-C bench
(runloom_mn_go_c) never does, so it allocates zero chunks and can't measure
the datastack RSS that T2.3 targets.  This bench does, and -- because a
handler parks inside wait_fd while a live Python frame sits on its chunk
-- it models the N=1M steady state ("95% parked, each holding a chunk").

Run it next to the C bench at the same N to read off the per-goroutine
Python cost (frame + datastack chunk + socket/closure objects):

    PYTHONPATH=src ~/.pyenv/versions/3.13.13t/bin/python3 \
        tests_c/bench_server_py.py 16384 8 5

Args: N [H] [M]   (N connections, H hubs, M round-trips per connection)

Defences mirrored from bench_mn.c / bench_server_runloom.c so high N is
sustainable on one host:
  * RST close on the client (SO_LINGER{1,0}) -> no TIME_WAIT.
  * Round-robin source IP across 127.0.0.2..251 -> independent ~28K
    ephemeral pools per source IP (127/8 is locally bindable on Linux
    with no `ip addr add`).
"""
import os
import resource
import socket
import struct
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import runloom_c

PAYLOAD = b"hellopyg"
PAYLOAD_LEN = len(PAYLOAD)

# wait_fd direction masks (match RUNLOOM_NETPOLL_READ / WRITE).
READ = 1
WRITE = 2

NUM_SRC_IPS = 250
_LINGER_RST = struct.pack("ii", 1, 0)   # l_onoff=1, l_linger=0 -> RST on close

# ---- globals (plain ints; only the accept loop / main touch the counters
# that must be exact, and those run serially) ----
HOST = "127.0.0.1"
PORT = 0
M = 5
N = 1024
listen_sock = None

# Per-goroutine accounting.  mn_go is fire-and-forget (returns None), and
# a shared counter would race under 3.13t free-threading.  Instead each
# client owns a distinct slot in a preallocated list and writes its slot
# only -- distinct-index stores + Bool singletons need no lock.
results = []

# Idle-hold phase (T3.1 shape).  When IDLE_S > 0 each connection, after its
# round-trips, parks idle for IDLE_S before closing -- modelling the N=1M
# steady state (95% of connections parked on netpoll holding a stack).  A
# sampler goroutine reads current RSS mid-idle so we can see how much a
# parked stack actually costs (and how much madvise-on-park would reclaim).
IDLE_S = 0.0
ready = []                  # list.append is thread-safe under 3.13t; len() O(1)
idle_rss_kib = -1           # set by the sampler g; read by main after mn_run


def _cur_rss_kib():
    """Current resident set (VmRSS), not the peak."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except (OSError, ValueError):
        pass
    return -1


def _rst_close(sock):
    """Close with an immediate RST so the socket skips TIME_WAIT."""
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
            runloom_c.netpoll_unregister(fd)
        except (AttributeError, OSError):
            pass
    try:
        sock.close()
    except OSError:
        pass


def _recv_exactly(sock, fd, n):
    """Park-until-readable loop; returns bytes of length n, or b'' on EOF."""
    out = bytearray()
    while len(out) < n:
        try:
            chunk = sock.recv(n - len(out))
        except (BlockingIOError, InterruptedError):
            runloom_c.wait_fd(fd, READ)
            continue
        except OSError:
            return b""          # peer RST / error
        if not chunk:
            return b""          # peer closed
        out += chunk
    return bytes(out)


def _send_all(sock, fd, data):
    view = memoryview(data)
    sent = 0
    while sent < len(view):
        try:
            sent += sock.send(view[sent:])
        except (BlockingIOError, InterruptedError):
            runloom_c.wait_fd(fd, WRITE)
        except OSError:
            return False
    return True


# ---- server-side per-connection handler (a Python def -> datastack chunk) ----
def echo_handler(conn):
    fd = conn.fileno()
    try:
        while True:
            data = _recv_exactly(conn, fd, PAYLOAD_LEN)
            if not data:
                break
            if not _send_all(conn, fd, data):
                break
    finally:
        # Client RSTs first, so a plain close here never hits TIME_WAIT.
        fd2 = -1
        try:
            fd2 = conn.fileno()
        except (OSError, ValueError):
            pass
        if fd2 >= 0:
            try:
                runloom_c.netpoll_unregister(fd2)
            except (AttributeError, OSError):
                pass
        try:
            conn.close()
        except OSError:
            pass


# ---- server accept loop (one goroutine) ----
def accept_loop():
    lfd = listen_sock.fileno()
    accepted = 0
    while accepted < N:
        try:
            conn, _addr = listen_sock.accept()
        except (BlockingIOError, InterruptedError):
            runloom_c.wait_fd(lfd, READ)
            continue
        except OSError:
            break
        # Drain the accept backlog in a tight loop before re-parking, so
        # a burst of N SYNs doesn't overflow the listen queue.
        while True:
            conn.setblocking(False)
            cfd = conn.fileno()
            # Clear any stale registration on a reused fd number before the
            # handler arms it (the registration-cache gotcha).
            try:
                runloom_c.netpoll_unregister(cfd)
            except (AttributeError, OSError):
                pass
            accepted += 1
            runloom_c.mn_go(lambda c=conn: echo_handler(c))
            if accepted >= N:
                break
            try:
                conn, _addr = listen_sock.accept()
            except (BlockingIOError, InterruptedError):
                break
            except OSError:
                accepted = N
                break
    try:
        runloom_c.netpoll_unregister(lfd)
    except (AttributeError, OSError):
        pass


# ---- client goroutine (a Python def -> datastack chunk) ----
def _client_body(idx):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setblocking(False)
    # Source IP derived from the client index -> race-free round-robin
    # across independent ephemeral pools.
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.%d" % (2 + idx % NUM_SRC_IPS), 0))
    except OSError:
        pass
    fd = s.fileno()
    try:
        s.connect((HOST, PORT))
    except BlockingIOError:
        runloom_c.wait_fd(fd, WRITE)
        err = s.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if err != 0:
            _rst_close(s)
            return False
    except OSError:
        _rst_close(s)
        return False

    for _ in range(M):
        if not _send_all(s, fd, PAYLOAD):
            _rst_close(s)
            return False
        if _recv_exactly(s, fd, PAYLOAD_LEN) != PAYLOAD:
            _rst_close(s)
            return False
    if IDLE_S > 0.0:
        # Signal "reached idle", then park (sleep) so the connection sits
        # idle -- both this client and its server handler are now parked
        # holding a stack.  The server handler is parked on recv waiting
        # for the close that comes after the idle window.
        ready.append(1)
        runloom_c.sched_sleep(IDLE_S)
    _rst_close(s)
    return True


def client(idx):
    # Write only this client's own slot (no shared counter).
    results[idx] = _client_body(idx)


def sampler():
    """Spin-wait until every client is idle, settle, then snapshot RSS."""
    global idle_rss_kib
    # Wait for all connections to reach the idle phase.
    while len(ready) < N:
        runloom_c.sched_sleep(0.01)
    # Let the scheduler quiesce (any in-flight parks complete / madvise
    # runs at park time) before sampling.
    runloom_c.sched_sleep(min(0.5, IDLE_S * 0.4))
    idle_rss_kib = _cur_rss_kib()


def _peak_rss_kib():
    # ru_maxrss is KiB on Linux.
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def _maps_count():
    try:
        with open("/proc/self/maps") as f:
            return sum(1 for _ in f)
    except OSError:
        return -1


def main(argv):
    global N, M, PORT, listen_sock, results, IDLE_S
    N = int(argv[1]) if len(argv) > 1 else 1024
    H = int(argv[2]) if len(argv) > 2 else 8
    M = int(argv[3]) if len(argv) > 3 else 5
    IDLE_S = float(argv[4]) if len(argv) > 4 else 0.0   # idle-hold seconds
    results = [False] * N

    # Lift the fd limit to match the C bench.
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

    if runloom_c.mn_init(H) < 0:
        sys.stderr.write("mn_init failed\n")
        return 2

    t0 = time.monotonic()
    runloom_c.mn_go(accept_loop)
    if IDLE_S > 0.0:
        runloom_c.mn_go(sampler)
    for i in range(N):
        runloom_c.mn_go(lambda i=i: client(i))
    completed = runloom_c.mn_run()
    dt = time.monotonic() - t0

    peak = _peak_rss_kib()
    maps = _maps_count()
    runloom_c.mn_fini()
    listen_sock.close()

    done = sum(1 for r in results if r is True)
    # Cross-check: every goroutine (1 accept + N echo + N client [+ 1
    # sampler when idle]) drained.
    expect = 2 * N + 1 + (1 if IDLE_S > 0.0 else 0)
    if completed != expect:
        sys.stderr.write("note: completed=%d expected=%d\n"
                         % (completed, expect))
    thr = (N * M / dt / 1000.0) if dt > 0 else 0.0
    idle_str = (" idle_s=%.2f idle_rss_kib=%d" % (IDLE_S, idle_rss_kib)
                if IDLE_S > 0.0 else "")
    print("N=%d H=%d M=%d done=%d/%d %.3fs %.1fK/s "
          "peak_rss_kib=%d maps=%d nofile=%d hubs=%d%s"
          % (N, H, M, done, N, dt, thr, peak, maps, nofile, H, idle_str))
    if done != N:
        sys.stderr.write("FAIL: %d/%d completed\n" % (done, N))
        try:
            runloom_c._self_check(1)
        except Exception:
            pass
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
