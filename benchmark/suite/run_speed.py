#!/usr/bin/env python3
"""Speed benchmark orchestrator: spawn / context-switch / HTTP req-s / TCP RTT
for [runloom, go, asyncio, greenlet, uvloop].

spawn + ctxswitch are pure-scheduler (no network); the orchestrator runs an n=0
startup baseline and subtracts it. rtt + http launch a Go target in the server
netns and run each runtime's client in the client netns.

Usage: python3 run_speed.py [--quick] [--only runtime,...] [--metric ...]
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

SP = os.path.join(config.SUITE_DIR, "speed")
SD = config.SERVERS_DIR
P = config.FT_PYTHON
G = config.GIL_PYTHON
SPEED_GO = os.path.join(SP, "speed_go")
SRV_GO = os.path.join(SD, "srv_go")           # echo target for rtt
MANY = config.SERVER_CPU_SPEC
ONE_S = str(config.SERVER_CPUS[0])
ONE_C = str(config.CLIENT_CPUS[0])
HUBS = config.HUBS
N_FULL = 1_000_000
N_SWITCH_FULL = 4_000_000
_port = [config.BASE_PORT + 400]


def nextport():
    _port[0] += 1
    return _port[0]


def parse_json(out):
    for line in reversed(out.strip().splitlines()):
        line = line.strip()
        if line.startswith("{"):
            return json.loads(line)
    raise RuntimeError("no JSON in: %s" % out[-500:])


def run_local(argv, cpus, gil_off, extra_env=None, raise_fd=True, timeout=300):
    cmd = topo.pinned_cmd(argv, cpus=cpus, extra_env=extra_env,
                          gil_off=gil_off, raise_fd=raise_fd)
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if r.returncode != 0 and not r.stdout.strip():
        raise RuntimeError("cmd failed rc=%d: %s\n%s" % (r.returncode, " ".join(argv), r.stderr[-800:]))
    return parse_json(r.stdout)


# ---- runtime client commands (argv builders) ----
def argv_for(rt, metric, n, host=None, port=None, conns=64, ramp=1.0, measure_s=3.0,
             gomax=None):
    if rt == "go":
        if metric in ("spawn", "ctxswitch"):
            return [SPEED_GO, "-metric", metric, "-n", str(n), "-gomaxprocs", str(gomax or HUBS)]
        if metric == "rtt":
            return [SPEED_GO, "-metric", "rtt", "-addr", "%s:%d" % (host, port), "-n", str(n)]
        return [SPEED_GO, "-metric", "httpclient", "-addr", "%s:%d" % (host, port),
                "-conns", str(conns), "-gomaxprocs", str(gomax or config.CLIENT_CORES),
                "-ramp", str(ramp), "-measure", str(measure_s)]
    if rt == "runloom":
        base = [P, os.path.join(SP, "speed_runloom.py"), "--metric", metric]
        if metric in ("spawn", "ctxswitch"):
            return base + ["--n", str(n), "--hubs", str(gomax or HUBS)]
        if metric == "rtt":
            return base + ["--host", host, "--port", str(port), "--n", str(n)]
        return base + ["--host", host, "--port", str(port), "--conns", str(conns),
                       "--hubs", str(gomax or config.CLIENT_CORES),
                       "--ramp", str(ramp), "--measure", str(measure_s)]
    if rt in ("asyncio", "uvloop"):
        base = [G, os.path.join(SP, "speed_asyncio.py"), "--metric", metric, "--loop", rt]
        if metric in ("spawn", "ctxswitch"):
            return base + ["--n", str(n)]
        if metric == "rtt":
            return base + ["--host", host, "--port", str(port), "--n", str(n)]
        return base + ["--host", host, "--port", str(port), "--conns", str(conns),
                       "--ramp", str(ramp), "--measure", str(measure_s)]
    if rt == "greenlet":
        base = [G, os.path.join(SP, "speed_greenlet.py"), "--metric", metric]
        if metric in ("spawn", "ctxswitch"):
            return base + ["--n", str(n)]
        if metric == "rtt":
            return base + ["--host", host, "--port", str(port), "--n", str(n)]
        return base + ["--host", host, "--port", str(port), "--conns", str(conns),
                       "--ramp", str(ramp), "--measure", str(measure_s)]
    raise ValueError(rt)


RUNTIMES = ["runloom", "go", "asyncio", "greenlet", "uvloop"]


def cpus_for(rt, metric):
    # single-threaded runtimes -> 1 core; multi-core -> server set (spawn/ctx) or
    # client set (network clients).
    if rt in ("asyncio", "uvloop", "greenlet"):
        return ONE_S if metric in ("spawn", "ctxswitch") else ONE_C
    return MANY if metric in ("spawn", "ctxswitch") else config.CLIENT_CPU_SPEC


def gil_off_for(rt):
    return rt in ("runloom", "go")  # go ignores it; GIL runtimes need GIL on


def cores_of(res, rt, metric):
    return res.get("cores", 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--only", default="")
    ap.add_argument("--metric", default="all")
    args = ap.parse_args()
    only = set(filter(None, args.only.split(","))) or set(RUNTIMES)
    metrics = (["spawn", "ctxswitch", "rtt", "http"] if args.metric == "all"
               else [args.metric])
    nspawn = 100_000 if args.quick else N_FULL
    nswitch = 1_000_000 if args.quick else N_SWITCH_FULL
    nrtt = 20_000 if args.quick else 100_000
    ramp, meas = (1.0, 2.0) if args.quick else (config.RAMP_S, config.MEASURE_S)
    conns = 256

    os.makedirs(config.RESULTS_DIR, exist_ok=True)
    results = {"meta": config.summary(), "quick": args.quick, "metrics": {}}

    # ---- spawn ----
    if "spawn" in metrics:
        results["metrics"]["spawn"] = {}
        for rt in [r for r in RUNTIMES if r in only]:
            try:
                base = run_local(argv_for(rt, "spawn", 0), cpus_for(rt, "spawn"),
                                 gil_off_for(rt), raise_fd=True)
                full = run_local(argv_for(rt, "spawn", nspawn), cpus_for(rt, "spawn"),
                                 gil_off_for(rt), raise_fd=True, timeout=600)
                sec = max(full["seconds"] - base["seconds"], 1e-9)
                rate = nspawn / sec
                results["metrics"]["spawn"][rt] = {
                    "n": nspawn, "seconds": sec, "rate_per_s": rate,
                    "cores": full.get("cores", 1)}
                print("spawn   %-9s %.3fs  %.0f/s  (%.2f us/task, %d cores)"
                      % (rt, sec, rate, sec * 1e6 / nspawn, full.get("cores", 1)), flush=True)
            except Exception as e:
                print("spawn   %-9s FAILED %r" % (rt, e), flush=True)
                results["metrics"]["spawn"][rt] = {"error": repr(e)}

    # ---- ctxswitch ----
    if "ctxswitch" in metrics:
        results["metrics"]["ctxswitch"] = {}
        for rt in [r for r in RUNTIMES if r in only]:
            # runloom: this is a pure-CPU yield loop, which spuriously trips the
            # ATTACHED/CPU-preempt watchdog (a feature for I/O workloads that park
            # back to hub_main, never this microbenchmark). Measure the
            # representative cooperative-yield cost with it OFF -- consistent with
            # the c_entry capstone, which is also preempt-off. Other runtimes
            # ignore these vars.
            cx_env = {"RUNLOOM_PREEMPT": "0", "RUNLOOM_SYSMON": "0"} if rt == "runloom" else None
            try:
                base = run_local(argv_for(rt, "ctxswitch", 0), cpus_for(rt, "ctxswitch"),
                                 gil_off_for(rt), extra_env=cx_env, raise_fd=True)
                full = run_local(argv_for(rt, "ctxswitch", nswitch), cpus_for(rt, "ctxswitch"),
                                 gil_off_for(rt), extra_env=cx_env, raise_fd=True, timeout=600)
                sw = full["switches"]
                sec = max(full["seconds"] - base["seconds"], 1e-9)
                ns = sec * 1e9 / sw
                results["metrics"]["ctxswitch"][rt] = {
                    "switches": sw, "seconds": sec, "ns_per_switch": ns,
                    "cores": full.get("cores", 1)}
                print("ctxsw   %-9s %.0f ns/switch  (%d switches, %d cores)"
                      % (rt, ns, sw, full.get("cores", 1)), flush=True)
            except Exception as e:
                print("ctxsw   %-9s FAILED %r" % (rt, e), flush=True)
                results["metrics"]["ctxswitch"][rt] = {"error": repr(e)}

    # ---- network metrics: need the topology + a Go target ----
    if "rtt" in metrics or "http" in metrics:
        topo.setup()
        try:
            if "rtt" in metrics:
                results["metrics"]["rtt"] = {}
                port = nextport()
                token = "RLSPEED_echo_%d" % port
                echo = measure.Server(topo.ns_cmd(config.SRV_NS,
                    [SRV_GO, "-host", config.SRV_IP, "-port", str(port),
                     "-gomaxprocs", str(HUBS), "-token", token],
                    cpus=MANY, raise_fd=True, gil_off=True), token, "go-echo")
                echo.start(timeout=20)
                time.sleep(0.5)
                try:
                    for rt in [r for r in RUNTIMES if r in only]:
                        try:
                            res = _netns_client(rt, "rtt", nrtt, config.SRV_IP, port, ONE_C)
                            results["metrics"]["rtt"][rt] = res
                            print("rtt     %-9s %.0f ns/rtt" % (rt, res["ns_per_rtt"]), flush=True)
                        except Exception as e:
                            print("rtt     %-9s FAILED %r" % (rt, e), flush=True)
                            results["metrics"]["rtt"][rt] = {"error": repr(e)}
                finally:
                    echo.stop()

            if "http" in metrics:
                results["metrics"]["http"] = {}
                port = nextport()
                token = "RLSPEED_httpd_%d" % port
                httpd = measure.Server(topo.ns_cmd(config.SRV_NS,
                    [SPEED_GO, "-metric", "httpd", "-host", config.SRV_IP,
                     "-port", str(port), "-gomaxprocs", str(HUBS), "-token", token],
                    cpus=MANY, raise_fd=True, gil_off=True), token, "go-httpd")
                httpd.start(timeout=20)
                time.sleep(0.5)
                try:
                    for rt in [r for r in RUNTIMES if r in only]:
                        try:
                            res = _netns_client(rt, "http", 0, config.SRV_IP, port,
                                                config.CLIENT_CPU_SPEC if rt in ("runloom", "go")
                                                else ONE_C, conns=conns, ramp=ramp, measure_s=meas)
                            results["metrics"]["http"][rt] = res
                            print("http    %-9s %.0f rps  (%d cores)" % (rt, res["rps"], res.get("cores", 1)), flush=True)
                        except Exception as e:
                            print("http    %-9s FAILED %r" % (rt, e), flush=True)
                            results["metrics"]["http"][rt] = {"error": repr(e)}
                finally:
                    httpd.stop()
        finally:
            topo.teardown()

    out = os.path.join(config.RESULTS_DIR, "speed_quick.json" if args.quick else "speed.json")
    # Merge into any existing file so a SUBSET run (e.g. --metric ctxswitch) does
    # NOT wipe the other metrics, and a runtime that errored this pass keeps its
    # prior good value rather than overwriting it with the error.
    if os.path.exists(out):
        try:
            with open(out) as f:
                prev = json.load(f)
            pm = dict(prev.get("metrics", {}))
            for metric, rtmap in results["metrics"].items():
                dest = dict(pm.get(metric, {}))
                for rt, val in rtmap.items():
                    if isinstance(val, dict) and "error" in val and rt in dest:
                        continue  # keep the prior good value
                    dest[rt] = val
                pm[metric] = dest
            prev["meta"] = results["meta"]
            prev["quick"] = results["quick"]
            prev["metrics"] = pm
            results = prev
        except Exception as e:
            print("merge into existing %s failed (%r); overwriting" % (out, e), flush=True)
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print("\nwrote", out, flush=True)


def _netns_client(rt, metric, n, host, port, cpus, conns=64, ramp=1.0, measure_s=3.0):
    """Run a runtime's network client inside the CLIENT netns."""
    argv = argv_for(rt, metric, n, host=host, port=port, conns=conns,
                    ramp=ramp, measure_s=measure_s)
    cmd = topo.ns_cmd(config.CLI_NS, argv, cpus=cpus, gil_off=gil_off_for(rt),
                      raise_fd=True)
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=ramp + measure_s + 120)
    return parse_json(r.stdout)


if __name__ == "__main__":
    main()
