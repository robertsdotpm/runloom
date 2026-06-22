"""Measurement primitives shared by the network benchmarks: launch a server and
wait for its LISTENING line, sample per-core CPU to decide server- vs client-
bound, run the Go loadgen, and walk the connection ladder with a rigorous stop
rule + bootstrap CI (decision #8).
"""
import json
import os
import queue
import statistics
import subprocess
import threading
import time

import config


# --------------------------------------------------------------------------
# per-core CPU utilisation from /proc/stat  (server-bound check, decision #8)
# --------------------------------------------------------------------------
def _proc_stat():
    out = {}
    with open("/proc/stat") as f:
        for line in f:
            if line.startswith("cpu") and line[3].isdigit():
                parts = line.split()
                cpu = int(parts[0][3:])
                vals = list(map(int, parts[1:]))
                idle = vals[3] + (vals[4] if len(vals) > 4 else 0)  # idle + iowait
                total = sum(vals)
                out[cpu] = (total - idle, total)  # (busy, total)
    return out


def cpu_group_util(snap0, snap1, cpus):
    """Mean busy fraction across `cpus` between two /proc/stat snapshots."""
    fr = []
    for c in cpus:
        if c in snap0 and c in snap1:
            db = snap1[c][0] - snap0[c][0]
            dt = snap1[c][1] - snap0[c][1]
            if dt > 0:
                fr.append(db / dt)
    return (sum(fr) / len(fr)) if fr else 0.0


# --------------------------------------------------------------------------
# server process lifecycle
# --------------------------------------------------------------------------
class Server:
    def __init__(self, cmd, token, name):
        self.cmd = cmd
        self.token = token
        self.name = name
        self.proc = None
        self.port = None
        self.lines = []
        self._q = queue.Queue()

    def _drain(self, stream, tag):
        for line in iter(stream.readline, ""):
            self.lines.append("[%s] %s" % (tag, line.rstrip()))
            self._q.put(line.rstrip())
        stream.close()

    def start(self, timeout=30.0):
        self.proc = subprocess.Popen(
            self.cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, start_new_session=True)
        threading.Thread(target=self._drain, args=(self.proc.stdout, "out"), daemon=True).start()
        threading.Thread(target=self._drain, args=(self.proc.stderr, "err"), daemon=True).start()
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                line = self._q.get(timeout=0.5)
            except queue.Empty:
                if self.proc.poll() is not None:
                    raise RuntimeError("server %s exited before LISTENING:\n%s"
                                       % (self.name, "\n".join(self.lines)))
                continue
            if line.startswith("LISTENING"):
                self.port = int(line.split()[1])
                return self.port
        raise RuntimeError("server %s never printed LISTENING:\n%s"
                           % (self.name, "\n".join(self.lines)))

    def stop(self):
        # Servers run as root via `sudo ip netns exec ...`; kill by unique token.
        subprocess.run(["sudo", "-n", "pkill", "-9", "-f", self.token],
                       capture_output=True)
        if self.proc:
            try:
                self.proc.terminate()
                self.proc.wait(timeout=3)
            except Exception:
                try:
                    self.proc.kill()
                except Exception:
                    pass
        time.sleep(0.3)


# --------------------------------------------------------------------------
# loadgen invocation
# --------------------------------------------------------------------------
def run_loadgen(loadgen_bin, addr, conns, payload, ramp, measure, gomax,
                server_cpus=None, src_ips=None):
    """Run the Go loadgen in the client netns, pinned to client cpus.  Returns
    (result_dict, server_util, client_util).  server_cpus is the server's ACTUAL
    pinned core set (so a 1-core server's util isn't diluted across 44 cores).
    src_ips (churn only) is a list of client source IPs the loadgen rotates its
    connects across to dodge TIME_WAIT/ephemeral-port exhaustion."""
    import topo
    server_cpus = server_cpus if server_cpus is not None else config.SERVER_CPUS
    argv = [loadgen_bin, "-addr", addr, "-conns", str(conns),
            "-payload", str(payload), "-ramp", str(ramp),
            "-measure", str(measure), "-gomaxprocs", str(gomax)]
    if src_ips:
        argv += ["-srcips", ",".join(src_ips)]
    cmd = topo.ns_cmd(config.CLI_NS, argv, cpus=config.CLIENT_CPU_SPEC,
                      raise_fd=True, gil_off=True)
    s0 = _proc_stat()
    t0 = time.time()
    r = subprocess.run(cmd, capture_output=True, text=True,
                       timeout=ramp + measure + 45)
    dt = time.time() - t0
    s1 = _proc_stat()
    srv_u = cpu_group_util(s0, s1, server_cpus)
    cli_u = cpu_group_util(s0, s1, config.CLIENT_CPUS)
    out = r.stdout.strip().splitlines()
    res = None
    for line in reversed(out):
        line = line.strip()
        if line.startswith("{"):
            res = json.loads(line)
            break
    if res is None:
        raise RuntimeError("loadgen produced no JSON (conns=%d):\nSTDOUT:%s\nSTDERR:%s"
                           % (conns, r.stdout, r.stderr))
    return res, srv_u, cli_u


def bootstrap_ci(xs, iters=2000, q=0.95):
    """Nonparametric bootstrap CI of the median (decision #8)."""
    if len(xs) < 2:
        return (xs[0], xs[0]) if xs else (0.0, 0.0)
    import random
    rnd = random.Random(12345)
    meds = []
    n = len(xs)
    for _ in range(iters):
        sample = [xs[rnd.randrange(n)] for _ in range(n)]
        meds.append(statistics.median(sample))
    meds.sort()
    lo = meds[int((1 - q) / 2 * iters)]
    hi = meds[int((1 + q) / 2 * iters)]
    return (lo, hi)


def ladder(server_factory, loadgen_bin, addr, payload, ladder_conns,
           reps, ramp, measure, gomax, patience, server_cpus=None, src_ips=None):
    """Bring up ONE server, sweep the connection ladder, find peak rps with a
    rigorous stop rule.  Returns the full curve + peak summary.  server_cpus is
    the server's actual pinned core set for the server-bound check.  src_ips
    (churn only) is forwarded to the loadgen for source-IP fan-out.

    server_factory() -> a started Server (already LISTENING).  We own stopping it.
    """
    srv = server_factory()
    curve = []
    best = {"rps_median": -1.0}
    misses = 0
    try:
        for conns in ladder_conns:
            reps_rps, reps_lat, srv_us, cli_us, errs = [], [], [], [], 0
            for _ in range(reps):
                res, su, cu = run_loadgen(loadgen_bin, addr, conns, payload,
                                          ramp, measure, gomax, server_cpus=server_cpus,
                                          src_ips=src_ips)
                reps_rps.append(res["rps"])
                reps_lat.append(res)
                srv_us.append(su)
                cli_us.append(cu)
                errs += res.get("conn_errors", 0) + res.get("establish_errors", 0)
            med = statistics.median(reps_rps)
            lo, hi = bootstrap_ci(reps_rps)
            rung = {
                "conns": conns,
                "rps_median": med,
                "rps_ci": [lo, hi],
                "rps_reps": reps_rps,
                "server_cpu_util": statistics.median(srv_us),
                "client_cpu_util": statistics.median(cli_us),
                "p50_us": statistics.median([r["p50_us"] for r in reps_lat]),
                "p99_us": statistics.median([r["p99_us"] for r in reps_lat]),
                "p999_us": statistics.median([r["p999_us"] for r in reps_lat]),
                "live_conns": statistics.median([r["live_conns"] for r in reps_lat]),
                "errors": errs,
            }
            curve.append(rung)
            # plateau detection: a rung "improves" only if its median beats the
            # incumbent peak's CI *upper* bound -- a real gain, not noise.  A run
            # that fails to is a miss; `patience` consecutive misses stop the
            # sweep (and we keep climbing through marginal new maxima meanwhile).
            peak_ci_hi = best["rps_ci"][1] if best["rps_median"] >= 0 else -1.0
            if med > peak_ci_hi:
                best = rung
                misses = 0
            else:
                misses += 1
                if med > best["rps_median"]:
                    best = rung   # marginal new max -- keep for reporting
            print("  conns=%-6d rps=%-12.0f ci=[%.0f,%.0f] srvCPU=%.0f%% cliCPU=%.0f%% p99=%.0fus err=%d %s"
                  % (conns, med, lo, hi, rung["server_cpu_util"] * 100,
                     rung["client_cpu_util"] * 100, rung["p99_us"], errs,
                     "(miss %d)" % misses if misses else ""), flush=True)
            if misses >= patience:
                break
    finally:
        srv.stop()
    # bottleneck attribution at the peak
    su = best.get("server_cpu_util", 0.0) or 0.0
    cu = best.get("client_cpu_util", 0.0) or 0.0
    bottleneck = "server" if su >= 0.85 else (
        "client" if cu >= 0.85 else "neither_saturated")
    # When NOT server-bound, estimate the server's true ceiling by extrapolating
    # its measured CPU utilisation to 100% (decision: addresses the 16-core
    # client not saturating fast servers). Clearly an estimate, not a measurement.
    server_ceiling_est = (best["rps_median"] / su) if su > 0.05 else None
    return {"curve": curve, "peak": best, "bottleneck_at_peak": bottleneck,
            "server_ceiling_est": server_ceiling_est,
            "server_ceiling_note": (
                "extrapolated = peak_rps / server_cpu_util; the %d-core client was "
                "the bottleneck at peak" % config.CLIENT_CORES)
            if bottleneck != "server" else "server was the bottleneck (measured)"}
