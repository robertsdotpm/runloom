#!/usr/bin/env python3
"""Connection-CHURN benchmark -- conn/s, the metric the req/s benchmark avoids.

The persistent req/s benchmark establishes connections ONCE and loops requests on
them, so the server never spawns a handler under load.  This one does the
opposite: the client opens a NEW connection, sends one request, reads the echo,
and CLOSES -- repeated as hard as it can.  So the server pays
accept + spawn-a-handler + serve + teardown for EVERY counted connection, in the
hot loop.  This is where per-connection fiber/goroutine/coroutine spawn actually
lands -- the "spawn a handler per request" case every reader assumes.

Same runtimes + topology as the cross-runtime work sweep (reuses its server
launchers).  Each server spawns one handler per accepted connection (confirmed:
runloom module_io.c.inc, Go `go handle(conn)`, asyncio Task-per-conn, gevent
greenlet-per-conn).  Writes results/conn_churn.json.
"""
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "harness"))
import config
import topo
import measure
import work_xrt_sweep as wx        # reuse build_runtimes()

CHURN = os.path.join(config.CLIENTS_DIR, "churn_loadgen")
PAYLOAD = 64
WORKERS = int(os.environ.get("CHURN_WORKERS", "200"))  # concurrent in-flight dialers
RAMP = config.RAMP_S
MEAS = config.MEASURE_S
REPS = int(os.environ.get("CHURN_REPS", "2"))


def _churn_sysctls():
    """Connection churn floods TIME_WAIT and burns ephemeral ports -- widen the
    range and allow TW reuse in BOTH netns so dials don't start failing."""
    for ns in (config.CLI_NS, config.SRV_NS):
        subprocess.run(["sudo", "-n", "ip", "netns", "exec", ns, "sysctl", "-w",
                        "net.ipv4.tcp_tw_reuse=1",
                        "net.ipv4.ip_local_port_range=1024 65535",
                        "net.ipv4.tcp_fin_timeout=5"],
                       capture_output=True)


def launch(rt, port, token):
    argv = rt["make"](port, token, 0)          # work=0 == pure echo
    cmd = topo.ns_cmd(config.SRV_NS, argv, cpus=rt["cpus"], extra_env=rt["env"],
                      gil_off=rt["gil_off"], raise_fd=True)
    srv = measure.Server(cmd, token, "%s_churn" % rt["name"])
    srv.start(timeout=40)
    time.sleep(0.5)
    return srv


def main():
    runtimes = wx.build_runtimes()
    topo.setup()
    _churn_sysctls()
    results = {}
    port = 9700
    try:
        for rt in runtimes:
            port += 1
            token = "RLCHURN_%s_%d" % (rt["name"], port)
            best = None
            try:
                srv = launch(rt, port, token)
                try:
                    srv_cpus = [int(c) for c in rt["cpus"].split(",")]
                    for _ in range(REPS):
                        res, su, cu = measure.run_loadgen(
                            CHURN, "%s:%d" % (config.SRV_IP, srv.port),
                            WORKERS, PAYLOAD, RAMP, MEAS, config.CLIENT_CORES,
                            server_cpus=srv_cpus)
                        cps = (res or {}).get("conns_per_s", 0)
                        if best is None or cps > best.get("conns_per_s", 0):
                            best = dict(res or {})
                            best["server_util"] = su
                finally:
                    srv.stop()
            except Exception as e:
                best = {"error": repr(e)}
            results[rt["name"]] = {"label": rt["label"], "cores": rt["cores"],
                                   "kind": rt["kind"], **(best or {})}
            b = best or {}
            print("%-16s %10.0f conn/s   p50=%6.0fus p99=%8.0fus  srvCPU=%3.0f%%  "
                  "dial_err=%s io_err=%s  cores=%d"
                  % (rt["name"], b.get("conns_per_s", 0), b.get("p50_us", 0),
                     b.get("p99_us", 0), (b.get("server_util", 0) or 0) * 100,
                     b.get("dial_errors", "?"), b.get("io_errors", "?"),
                     rt["cores"]), flush=True)
    finally:
        topo.teardown()

    meta = {"payload": PAYLOAD, "workers": WORKERS, "reps": REPS,
            "ramp_s": RAMP, "measure_s": MEAS}
    out = os.path.join(config.RESULTS_DIR, "conn_churn.json")
    with open(out, "w") as f:
        json.dump({"meta": meta, "results": results}, f, indent=2)
    print("\nwrote", out)


if __name__ == "__main__":
    main()
