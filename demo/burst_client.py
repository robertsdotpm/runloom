"""Burst load client on the SAME M:N sync library as the server.

Every CLIENT_INTERVAL seconds (default 60) it fires CLIENT_BURST (default
100) concurrent HTTP requests at the server -- one goroutine per request,
spawned with runloom_c.mn_go, all using the cooperative mnweb.fetch path.
It collects every result over a channel, logs a latency/status summary,
sleeps, and repeats forever.

It arms the same crash + traceback diagnostics as the server, so if the
*client* is the thing that wedges or faults, the watchdog can diagnose it
the same way.
"""
import os
import sys
import time
import traceback

import runloom_c
import mnweb

HOST = os.environ.get("CLIENT_HOST", "127.0.0.1")
PORT = int(os.environ.get("CLIENT_PORT", "8080"))
BURST = int(os.environ.get("CLIENT_BURST", "100"))
INTERVAL = float(os.environ.get("CLIENT_INTERVAL", "60"))
HUBS = int(os.environ.get("CLIENT_HUBS", "4"))
RUNDIR = os.environ.get("CLIENT_RUNDIR", os.path.join(os.path.dirname(__file__), "run"))
CRASH_REPORT = os.path.join(RUNDIR, "client_crash_report.txt")

# A spread of endpoints so each burst drives the counter, the DB writes,
# the timer path (/slow), and the cheap path (/health).
ENDPOINTS = ["/", "/ip", "/count", "/count", "/stats", "/slow", "/health"]


def one_request(index, results):
    """Issue a single request; report (ok, status, latency_ms, err)."""
    path = ENDPOINTS[index % len(ENDPOINTS)]
    started = time.perf_counter()
    try:
        status, body = mnweb.fetch(HOST, path, port=PORT, timeout_ms=15_000)
        latency = (time.perf_counter() - started) * 1000.0
        results.send((status == 200, status, latency, None))
    except Exception as exc:
        latency = (time.perf_counter() - started) * 1000.0
        results.send((False, 0, latency, repr(exc)))


def summarize(burst_no, latencies, ok, fails, errors):
    latencies.sort()
    n = len(latencies)
    def pct(p):
        if not latencies:
            return 0.0
        return latencies[min(n - 1, int(p * n))]
    line = ("[burst {:>5}] {}/{} ok  fail={}  "
            "lat ms: min={:.1f} p50={:.1f} p99={:.1f} max={:.1f}").format(
        burst_no, ok, ok + fails, fails,
        latencies[0] if latencies else 0.0, pct(0.50), pct(0.99),
        latencies[-1] if latencies else 0.0)
    print(line, flush=True)
    if errors:
        sample = sorted(set(errors))[:3]
        print("            errors: {}".format(sample), flush=True)


def driver():
    burst_no = 0
    print("[client] target {}:{}  burst={}  interval={}s  hubs={}".format(
        HOST, PORT, BURST, INTERVAL, runloom_c.mn_hub_count()), flush=True)
    while True:
        burst_no += 1
        results = runloom_c.Chan(BURST + 16)
        sent = time.perf_counter()
        for i in range(BURST):
            runloom_c.mn_go(lambda i=i, r=results: one_request(i, r))
        latencies, ok, fails, errors = [], 0, 0, []
        for _ in range(BURST):
            success, status, latency, err = results.recv()[0]
            latencies.append(latency)
            if success:
                ok += 1
            else:
                fails += 1
                if err:
                    errors.append(err)
        wall = (time.perf_counter() - sent) * 1000.0
        summarize(burst_no, latencies, ok, fails, errors)
        if fails:
            print("            burst wall={:.0f}ms".format(wall), flush=True)
        runloom_c.sched_sleep(INTERVAL)


def arm_diagnostics():
    # See site.py arm_diagnostics: no faulthandler, level without py/wait/gdb,
    # so a fault chains to SIG_DFL -> core + die (no wedge under M:N).
    runloom_c.set_introspect_timestamps(True)
    level = os.environ.get("RUNLOOM_CRASH", "goroutine,backtrace")
    runloom_c.install_crash_handler(level, CRASH_REPORT)
    runloom_c.install_traceback_signal()


def main():
    os.makedirs(RUNDIR, exist_ok=True)
    arm_diagnostics()
    runloom_c.mn_init(HUBS)
    runloom_c.mn_go(driver)
    runloom_c.mn_run()
    runloom_c.mn_fini()


if __name__ == "__main__":
    main()
