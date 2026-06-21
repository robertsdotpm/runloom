#!/usr/bin/env python3
"""Performance benchmark orchestrator: req/s and bandwidth for the 5 runloom
tiers + asyncio + uvloop + gevent + go.

For each server config and each metric (small-payload req/s, 1.5 MB bandwidth) it
brings the server up in the server netns (pinned, fd-raised, debug-off), then has
the Go loadgen in the client netns walk the connection ladder until req/s
plateaus, recording the full curve, latency percentiles, and which side
(server/client) was CPU-bound at the peak.

Usage:
    python3 run_perf.py [--quick] [--only NAME[,NAME...]] [--metric reqps|bandwidth|both]
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "harness"))
import config
import topo
import measure

LOADGEN = os.path.join(config.CLIENTS_DIR, "loadgen")
SD = config.SERVERS_DIR
P = config.FT_PYTHON
G = config.GIL_PYTHON

_port_counter = [config.BASE_PORT]


def next_port():
    _port_counter[0] += 1
    return _port_counter[0]


def py(*rest):
    return [P, *rest]


# Each spec: name, label, interpreter tag, server "cores" (for per-core scaling),
# cpu pin spec, gil flag, env, and make_argv(port, token).
def build_specs():
    many = config.SERVER_CPU_SPEC
    one = str(config.SERVER_CPUS[0])
    HUBS = config.HUBS
    GO = config.GO_SERVER_CORES

    def rl(script, extra=()):
        def mk(port, token):
            return py(os.path.join(SD, script), "--host", config.SRV_IP,
                      "--port", str(port), "--hubs", str(HUBS),
                      "--token", token, *extra)
        return mk

    def base(script, interp, extra=()):
        def mk(port, token):
            return [G if interp == "gil" else P, os.path.join(SD, script),
                    "--host", config.SRV_IP, "--port", str(port),
                    "--token", token, *extra]
        return mk

    def gomk(port, token):
        return [os.path.join(SD, "go_netpoll_native_net"), "-host", config.SRV_IP,
                "-port", str(port), "-gomaxprocs", str(GO), "-token", token]

    return [
        dict(name="runloom_sync", label="Runloom sync wrappers (epoll, py handler)",
             interp="3.13t FT", cores=HUBS, cpus=many, gil_off=True, env={},
             make=rl("runloom_epoll_py_sync.py")),
        dict(name="runloom_c", label="Runloom C scaffold (py handler, C TCPConn)",
             interp="3.13t FT", cores=HUBS, cpus=many, gil_off=True, env={},
             make=rl("runloom_epoll_py_tcpcon.py")),
        dict(name="runloom_c_cython", label="Runloom C scaffold + Cython C handler (epoll)",
             interp="3.13t FT", cores=HUBS, cpus=many, gil_off=True, env={},
             make=rl("runloom_iouring_cython_tcpcon.py", ("--optimize", "none"))),
        dict(name="runloom_iouring", label="Runloom io_uring loop (py handler)",
             interp="3.13t FT", cores=HUBS, cpus=many, gil_off=True,
             env={"RUNLOOM_IOURING_LOOP": "1"}, make=rl("runloom_epoll_py_sync.py")),
        dict(name="runloom_cython", label="Runloom io_uring + Cython C handler",
             interp="3.13t FT", cores=HUBS, cpus=many, gil_off=True,
             env={"RUNLOOM_IOURING_LOOP": "1"},
             make=rl("runloom_iouring_cython_tcpcon.py", ("--optimize", "none"))),
        dict(name="runloom_cython_opt", label="Runloom io_uring + Cython + optimize(throughput)",
             interp="3.13t FT", cores=HUBS, cpus=many, gil_off=True,
             env={"RUNLOOM_IOURING_LOOP": "1"},
             make=rl("runloom_iouring_cython_tcpcon.py", ("--optimize", "throughput"))),
        dict(name="runloom_cdef", label="Runloom io_uring + Cython cdef handler (tstate-free c_entry)",
             interp="3.13t FT", cores=HUBS, cpus=many, gil_off=True,
             env={"RUNLOOM_IOURING_LOOP": "1"},
             make=rl("runloom_iouring_cdef_tcpcon.py")),
        dict(name="runloom_cdef_epoll", label="Runloom epoll + Cython cdef handler (tstate-free c_entry)",
             interp="3.13t FT", cores=HUBS, cpus=many, gil_off=True, env={},
             make=rl("runloom_iouring_cdef_tcpcon.py")),
        dict(name="asyncio", label="asyncio Protocol (GIL, 1 core)",
             interp="3.13 GIL", cores=1, cpus=one, gil_off=False, env={},
             make=base("asyncio_epoll_py_proto.py", "gil", ("--loop", "asyncio"))),
        dict(name="uvloop", label="uvloop (GIL, 1 core)",
             interp="3.13 GIL", cores=1, cpus=one, gil_off=False, env={},
             make=base("asyncio_epoll_py_proto.py", "gil", ("--loop", "uvloop"))),
        dict(name="gevent", label="gevent StreamServer (GIL, 1 core)",
             interp="3.13 GIL", cores=1, cpus=one, gil_off=False, env={},
             make=base("gevent_libev_py_stream.py", "gil")),
        dict(name="go", label="Go net (GOMAXPROCS=%d)" % GO,
             interp="go", cores=GO, cpus=many, gil_off=True, env={}, make=gomk),
    ]


def server_factory(spec, port, token):
    def factory():
        argv = spec["make"](port, token)
        cmd = topo.ns_cmd(config.SRV_NS, argv, cpus=spec["cpus"],
                          extra_env=spec["env"], gil_off=spec["gil_off"],
                          raise_fd=True)
        srv = measure.Server(cmd, token, spec["name"])
        srv.start(timeout=40)
        time.sleep(0.5)  # let acceptors arm
        return srv
    return factory


METRICS = {
    "reqps": dict(payload=config.PAYLOAD_SMALL, ladder=config.CONN_LADDER),
    "bandwidth": dict(payload=config.PAYLOAD_LARGE,
                      ladder=[1, 2, 4, 8, 16, 32, 64, 128]),
}


def run(quick, only, which_metrics):
    os.makedirs(config.RESULTS_DIR, exist_ok=True)
    specs = build_specs()
    if only:
        specs = [s for s in specs if s["name"] in only]
    reps = 1 if quick else config.REPS
    ramp = 1.0 if quick else config.RAMP_S
    measure_s = 2.0 if quick else config.MEASURE_S
    gomax = config.CLIENT_CORES

    print("== topology setup ==", flush=True)
    topo.setup()
    results = {"meta": config.summary(), "quick": quick, "servers": {}}
    try:
        for spec in specs:
            results["servers"][spec["name"]] = {
                "label": spec["label"], "interp": spec["interp"],
                "cores": spec["cores"], "metrics": {}}
            for metric in which_metrics:
                mc = METRICS[metric]
                lad = mc["ladder"]
                if quick:
                    lad = lad[::3] or lad[:1]
                port = next_port()
                token = "RLBENCH_%s_%s_%d" % (spec["name"], metric, port)
                print("\n== %s / %s (payload=%dB, cores=%d) =="
                      % (spec["name"], metric, mc["payload"], spec["cores"]), flush=True)
                try:
                    srv_cpus = [int(c) for c in spec["cpus"].split(",")]
                    out = measure.ladder(
                        server_factory(spec, port, token), LOADGEN,
                        "%s:%d" % (config.SRV_IP, port), mc["payload"], lad,
                        reps, ramp, measure_s, gomax, config.PLATEAU_PATIENCE,
                        server_cpus=srv_cpus)
                    out["payload"] = mc["payload"]
                    results["servers"][spec["name"]]["metrics"][metric] = out
                    pk = out["peak"]
                    print("  -> PEAK rps=%.0f @ conns=%s  bottleneck=%s"
                          % (pk["rps_median"], pk.get("conns"), out["bottleneck_at_peak"]),
                          flush=True)
                except Exception as e:
                    print("  !! FAILED: %r" % e, flush=True)
                    results["servers"][spec["name"]]["metrics"][metric] = {"error": repr(e)}
                # make sure nothing lingers
                import subprocess
                subprocess.run(["sudo", "-n", "pkill", "-9", "-f", token],
                               capture_output=True)
                time.sleep(0.5)
    finally:
        topo.teardown()

    out_path = os.path.join(config.RESULTS_DIR,
                            "perf_quick.json" if quick else "perf.json")
    # Merge into any existing results with the same quick flag, so a targeted
    # `--only <name>` re-run ADDS that server to the matrix instead of clobbering
    # the others (used to splice in extra configs without re-running everything).
    if os.path.exists(out_path):
        try:
            with open(out_path) as f:
                prev = json.load(f)
            if bool(prev.get("quick")) == bool(quick):
                merged = prev.get("servers", {})
                merged.update(results["servers"])
                results["servers"] = merged
        except Exception:
            pass
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print("\nwrote", out_path, flush=True)
    return out_path


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--only", default="")
    ap.add_argument("--metric", default="both", choices=["reqps", "bandwidth", "both"])
    args = ap.parse_args()
    only = set(filter(None, args.only.split(",")))
    metrics = ["reqps", "bandwidth"] if args.metric == "both" else [args.metric]
    run(args.quick, only, metrics)
