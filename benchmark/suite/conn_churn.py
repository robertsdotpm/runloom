#!/usr/bin/env python3
"""Connection-CHURN benchmark -- conn/s, the metric the req/s benchmark avoids,
measured against the SAME servers as the performance benchmark and driven to the
SAME saturation.

The persistent req/s benchmark (run_perf.py) establishes connections ONCE and
loops requests on them, so the server never spawns a handler under load.  This
one does the opposite: the client opens a NEW connection, sends one request,
reads the echo, and CLOSES -- repeated as hard as it can.  So the server pays
accept + spawn-a-handler + serve + teardown for EVERY counted connection, in the
hot loop.  This is where per-connection fiber/goroutine/coroutine spawn actually
lands -- the "spawn a handler per request" case every reader assumes.

Same servers, same load machinery as the performance benchmark, on purpose:

  * Servers: run_perf.build_specs() -- the identical 11 specs (7 runloom tiers +
    asyncio + uvloop + gevent + go), launched the identical way via
    run_perf.server_factory() (pinned cores, fd-raised, debug off, same HUBS).

  * Load: measure.ladder() -- the identical climb-until-it-plateaus saturation
    logic, with the per-core server- vs client-bound CPU check.  Here the ladder
    rung is the number of concurrent DIALERS (in-flight connection churn); the
    churn loadgen reports conn/s under the `rps` key so ladder drives it exactly
    as it drives the persistent loadgen.

Writes results/conn_churn.json in the same {meta, servers:{name:{peak,curve,...}}}
shape as perf.json.
"""
import argparse
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "harness"))
import config
import topo
import measure
import run_perf                       # reuse build_specs() + server_factory()

CHURN = os.path.join(config.CLIENTS_DIR, "churn_loadgen")
PAYLOAD = config.PAYLOAD_SMALL
# Ladder rung = number of concurrent dialers (in-flight connection churn).  Same
# ladder the req/s benchmark uses, so "same load"; the plateau rule stops the
# climb once conn/s stops growing, so heavier per-unit churn just plateaus sooner.
LADDER = config.CONN_LADDER


def _churn_sysctls():
    """Connection churn floods TIME_WAIT and burns ephemeral ports -- widen the
    range and allow TW reuse in BOTH netns so dials don't start failing."""
    for ns in (config.CLI_NS, config.SRV_NS):
        subprocess.run(["sudo", "-n", "ip", "netns", "exec", ns, "sysctl", "-w",
                        "net.ipv4.tcp_tw_reuse=1",
                        "net.ipv4.ip_local_port_range=1024 65535",
                        "net.ipv4.tcp_fin_timeout=5"],
                       capture_output=True)


def run(quick, only):
    os.makedirs(config.RESULTS_DIR, exist_ok=True)
    specs = run_perf.build_specs()
    if only:
        specs = [s for s in specs if s["name"] in only]
    reps = 1 if quick else config.REPS
    ramp = 1.0 if quick else config.RAMP_S
    measure_s = 2.0 if quick else config.MEASURE_S
    gomax = config.CLIENT_CORES
    lad = (LADDER[::3] or LADDER[:1]) if quick else LADDER

    print("== topology setup ==", flush=True)
    topo.setup()
    _churn_sysctls()
    results = {"meta": config.summary(), "quick": quick, "metric": "conn/s",
               "servers": {}}
    port = config.BASE_PORT + 700
    try:
        for spec in specs:
            port += 1
            token = "RLCHURN_%s_%d" % (spec["name"], port)
            entry = {"label": spec["label"], "interp": spec["interp"],
                     "cores": spec["cores"]}
            print("\n== churn %s (payload=%dB, cores=%d) =="
                  % (spec["name"], PAYLOAD, spec["cores"]), flush=True)
            try:
                srv_cpus = [int(c) for c in spec["cpus"].split(",")]
                out = measure.ladder(
                    run_perf.server_factory(spec, port, token), CHURN,
                    "%s:%d" % (config.SRV_IP, port), PAYLOAD, lad,
                    reps, ramp, measure_s, gomax, config.PLATEAU_PATIENCE,
                    server_cpus=srv_cpus)
                out["payload"] = PAYLOAD
                entry.update(out)
                pk = out["peak"]
                print("  -> PEAK %.0f conn/s @ dialers=%s  bottleneck=%s"
                      % (pk.get("rps_median", 0), pk.get("conns"),
                         out["bottleneck_at_peak"]), flush=True)
            except Exception as e:
                entry["error"] = repr(e)
                print("  !! FAILED: %r" % e, flush=True)
            results["servers"][spec["name"]] = entry
            subprocess.run(["sudo", "-n", "pkill", "-9", "-f", token],
                           capture_output=True)
            time.sleep(0.5)
    finally:
        topo.teardown()

    out_path = os.path.join(config.RESULTS_DIR, "conn_churn.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print("\nwrote", out_path, flush=True)
    return out_path


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--only", default="")
    args = ap.parse_args()
    only = set(filter(None, args.only.split(",")))
    run(args.quick, only)
