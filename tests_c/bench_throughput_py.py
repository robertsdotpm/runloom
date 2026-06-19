#!/usr/bin/env python3
"""bench_throughput_py.py -- STEADY-STATE goroutine echo throughput (runloom).

The older bench_server_py.py measures connection *setup*: one accept loop,
a synchronized connect storm, and a single round-trip per connection -- so
its wall time is dominated by establishing N connections, not by the runtime.
That under-reports the scheduler's real throughput.

This bench removes those harness bottlenecks and measures the steady state:

  * PARALLEL acceptors (ACCEPTORS goroutines on one listener) so establishment
    is not serialized through a single accept loop.
  * A RAMP phase establishes all N connections; a barrier waits until every
    client is connected.  Setup is NOT in the measured window.
  * A WARMUP, then a fixed MEASURE window during which every connection
    hammers request/response round-trips as fast as it can.  Throughput is
    (round-trips completed inside the window) / window -- the runtime's real
    req/s with N concurrent goroutines, with no connect/accept in the count.

Plain blocking recv/send under monkey.patch() -- the actual "write blocking
code, run a million goroutines" path (go(fn), no async/await).

    tests_c/scale_bench_tp.sh N [HUBS] [MEASURE_S] [WARMUP_S]
or  PYTHONPATH=src python3.13t tests_c/bench_throughput_py.py N H measure warmup
"""
import os
import resource
import socket
import struct
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import runloom
import runloom.monkey
import runloom_c

REAL_MONO = time.monotonic
PAYLOAD = b"hellopyg"
PLEN = len(PAYLOAD)
NUM_SRC_IPS = 250
ACCEPTORS = 64                       # parallel accept goroutines (de-serialise ramp)
_LINGER_RST = struct.pack("ii", 1, 0)

# Sharded round-trip counters (one writer per goroutine slot -> race-free
# under free-threading; summed only at the window boundaries).
NSHARDS = 1 << 16
SHARD_MASK = NSHARDS - 1


def cur_rss_kib():
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except (OSError, ValueError):
        pass
    return -1


def main(argv):
    N = int(argv[1]) if len(argv) > 1 else 1024
    H = int(argv[2]) if len(argv) > 2 else 8
    MEASURE_S = float(argv[3]) if len(argv) > 3 else 3.0
    WARMUP_S = float(argv[4]) if len(argv) > 4 else 1.0

    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (1 << 23, 1 << 23))
    except (ValueError, OSError):
        pass
    nofile = resource.getrlimit(resource.RLIMIT_NOFILE)[0]

    rts = [0] * NSHARDS              # per-slot round-trip counters
    # Per-client connected flags: distinct index per goroutine, single writer
    # each -> race-free under free-threading (a shared `+= 1` loses increments
    # with the GIL off, which would stall the ramp barrier forever).
    connected_flags = bytearray(N)
    state = {
        "stop": False,              # set after the measure window
        "win_rts": 0,               # RTs counted inside the window
        "rss_kib": -1,
    }

    runloom.monkey.patch()

    # Canonical max-concurrency listener set: ACCEPTORS separate listener
    # sockets on ONE port via SO_REUSEPORT, each with its own accept goroutine,
    # so the kernel load-balances SYNs across independent fds.  (Putting many
    # accept goroutines on a SINGLE shared listener fd instead serializes them
    # on one EPOLLONESHOT netpoll registration -- a thundering-herd contention
    # that strangles establishment.  Match Go's reuseport idiom.)
    listeners = []
    port = 0
    for i in range(ACCEPTORS):
        ls = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        ls.bind(("127.0.0.1", port))
        ls.listen(min(N, 65535))
        if port == 0:
            port = ls.getsockname()[1]
        listeners.append(ls)

    def echo_handler(conn):
        try:
            while not state["stop"]:
                data = conn.recv(PLEN)
                if not data:
                    break
                conn.sendall(data)
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    accepted = [0]

    def acceptor(ls):
        # One acceptor per REUSEPORT listener -> its OWN fd, no shared-fd
        # netpoll contention.
        while accepted[0] < N and not state["stop"]:
            try:
                conn, _ = ls.accept()
            except OSError:
                break
            accepted[0] += 1
            runloom.fiber(echo_handler, conn)

    def client(idx):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("127.0.0.%d" % (2 + idx % NUM_SRC_IPS), 0))
        except OSError:
            pass
        try:
            s.connect(("127.0.0.1", port))
        except OSError:
            return
        connected_flags[idx] = 1           # distinct index, single writer -> race-free
        buf_slot = idx & SHARD_MASK
        local = 0
        try:
            while not state["stop"]:
                s.sendall(PAYLOAD)
                got = b""
                while len(got) < PLEN:
                    chunk = s.recv(PLEN - len(got))
                    if not chunk:
                        raise OSError("eof")
                    got += chunk
                local += 1
                rts[buf_slot] = rts[buf_slot] + 1   # only writer for this slot
        except OSError:
            pass
        finally:
            try:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, _LINGER_RST)
                s.close()
            except OSError:
                pass

    def controller():
        # Wait for the full population to establish (ramp), then run a clean
        # warmup + measure window, snapshotting the RT counters at the edges.
        t_ramp0 = REAL_MONO()
        while sum(connected_flags) < N:
            runloom.sleep(0.01)
            if REAL_MONO() - t_ramp0 > 120:    # ramp safety valve
                break
        ramp_s = REAL_MONO() - t_ramp0
        established = sum(connected_flags)
        runloom.sleep(WARMUP_S)                 # warmup: not counted
        start = sum(rts)
        state["rss_kib"] = cur_rss_kib()        # RSS with all N live
        t0 = REAL_MONO()
        runloom.sleep(MEASURE_S)
        win = REAL_MONO() - t0
        end = sum(rts)
        state["win_rts"] = end - start
        state["stop"] = True
        for ls in listeners:
            try:
                ls.close()
            except OSError:
                pass
        thr = state["win_rts"] / win / 1000.0   # K req/s
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        print("N=%d H=%d established=%d/%d ramp=%.2fs window=%.2fs "
              "rts=%d %.1fK req/s rss_live_kib=%d peak_rss_kib=%d nofile=%d"
              % (N, H, established, N, ramp_s, win, state["win_rts"], thr,
                 state["rss_kib"], peak, nofile))

    def root():
        for ls in listeners:
            runloom.fiber(acceptor, ls)
        for i in range(N):
            runloom.fiber(client, i)
        runloom.fiber(controller)
        # Root waits until the controller stops the run.
        while not state["stop"]:
            runloom.sleep(0.05)

    runloom.run(H, root)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
