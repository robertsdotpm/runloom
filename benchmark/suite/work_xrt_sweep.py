#!/usr/bin/env python3
"""Cross-runtime handler work curve: the same FNV --work knob across EVERY
runtime, reported PER CORE so the comparison is honest (not "runloom magic").

The runloom-only curve (work_sweep.py -> work_curve.json) isolates "what does
compiling the handler buy" within one runtime. This puts that in context: the
identical FNV-1a byte hash runs in each runtime's natural handler language --

  interpreted Python : runloom_py (M:N), asyncio, uvloop, gevent  (1 core each
                       for the event loops, like the echo benchmark)
  compiled / native  : runloom_cython (M:N), go (GOMAXPROCS)

Read PER CORE, the prediction is two bands: the interpreted runtimes cluster
together and the compiled ones cluster together -- i.e. for CPU-bound handler
work the dominant variable is the handler LANGUAGE, not the runtime. runloom's
own advantage is that it gets the compiled band (Cython) while keeping M:N
parallelism across all cores; a single asyncio process serialises the same work
onto one core. Nothing cherry-picked: same algorithm, every runtime, per core.

Separate artifact (results/work_xrt.json) so the committed 8-point isolation
curve is untouched. Short ladder; work>0 is CPU-bound and saturates at low conns.
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

SD = config.SERVERS_DIR
FT = config.FT_PYTHON
GIL = config.GIL_PYTHON
LOADGEN = os.path.join(config.CLIENTS_DIR, "loadgen")
MANY = config.SERVER_CPU_SPEC
ONE = str(config.SERVER_CPUS[0])
HUBS = config.HUBS
GO = config.GO_SERVER_CORES

PAYLOAD = 1024
WORKS = [0, 1, 4, 16, 64]          # echo -> heavy; 5 points span the dynamic range
LADDER = [1024, 4096]              # work>0 saturates low; 4096 covers echo on 44 cores
REPS = 2


def s(script):
    return os.path.join(SD, script)


def build_runtimes():
    def rl(handler):
        def mk(port, token, w):
            return [FT, s("srv_runloom_work.py"), "--host", config.SRV_IP,
                    "--port", str(port), "--hubs", str(HUBS), "--token", token,
                    "--handler", handler, "--work", str(w)]
        return mk

    def aio(loop):
        def mk(port, token, w):
            return [GIL, s("srv_asyncio.py"), "--host", config.SRV_IP,
                    "--port", str(port), "--loop", loop, "--work", str(w),
                    "--token", token]
        return mk

    def gev(port, token, w):
        return [GIL, s("srv_gevent.py"), "--host", config.SRV_IP,
                "--port", str(port), "--work", str(w), "--token", token]

    def gomk(port, token, w):
        return [s("srv_go"), "-host", config.SRV_IP, "-port", str(port),
                "-gomaxprocs", str(GO), "-work", str(w), "-token", token]

    return [
        # The cdef c_entry (tstate-free) handler matched this Cython handler to
        # within noise, so the cross-runtime comparison shows just ONE compiled
        # runloom line -- the relatable "compile your hot handler in Cython" path.
        # (The cdef-vs-cython tstate-bypass detail lives in its own report
        # section, suite/servers/handler_cdef.pyx, not here.)
        dict(name="runloom_cython", label="Runloom (M:N) — Cython handler (compiled)",
             kind="compiled", cores=HUBS, cpus=MANY, gil_off=True, env={}, make=rl("cython")),
        dict(name="go", label="Go net (GOMAXPROCS=%d)" % GO,
             kind="compiled", cores=GO, cpus=MANY, gil_off=True, env={}, make=gomk),
        dict(name="runloom_py", label="Runloom (M:N) — Python handler",
             kind="interpreted", cores=HUBS, cpus=MANY, gil_off=True, env={}, make=rl("py")),
        dict(name="asyncio", label="asyncio Protocol (1 core)",
             kind="interpreted", cores=1, cpus=ONE, gil_off=False, env={}, make=aio("asyncio")),
        dict(name="uvloop", label="uvloop (1 core)",
             kind="interpreted", cores=1, cpus=ONE, gil_off=False, env={}, make=aio("uvloop")),
        dict(name="gevent", label="gevent StreamServer (1 core)",
             kind="interpreted", cores=1, cpus=ONE, gil_off=False, env={}, make=gev),
    ]


def factory(rt, port, token, w):
    def f():
        argv = rt["make"](port, token, w)
        cmd = topo.ns_cmd(config.SRV_NS, argv, cpus=rt["cpus"], extra_env=rt["env"],
                          gil_off=rt["gil_off"], raise_fd=True)
        srv = measure.Server(cmd, token, "%s_w%d" % (rt["name"], w))
        srv.start(timeout=40)
        time.sleep(0.5)
        return srv
    return f


def run_point(rt, w, port):
    token = "RLXRT_%s_%d_%d" % (rt["name"], w, port)
    srv_cpus = [int(c) for c in rt["cpus"].split(",")]
    print("\n== %s work=%d (cores=%d) ==" % (rt["name"], w, rt["cores"]), flush=True)
    out = measure.ladder(
        factory(rt, port, token, w), LOADGEN, "%s:%d" % (config.SRV_IP, port),
        PAYLOAD, LADDER, REPS, config.RAMP_S, config.MEASURE_S,
        config.CLIENT_CORES, len(LADDER), server_cpus=srv_cpus)
    pk = out["peak"]
    rps = pk["rps_median"]
    print("  -> %-16s peak=%.0f rps  per-core=%.0f  cpu=%.0f%%  bottleneck=%s"
          % (rt["name"], rps, rps / rt["cores"], (pk.get("server_cpu_util") or 0) * 100,
             out["bottleneck_at_peak"]), flush=True)
    out["_cores"] = rt["cores"]
    return out


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="", help="comma list of runtime names to (re)run; merges into work_xrt.json")
    a = ap.parse_args()
    only = set(filter(None, a.only.split(",")))
    topo.setup()
    runtimes = [rt for rt in build_runtimes() if not only or rt["name"] in only]
    results = {rt["name"]: {} for rt in runtimes}
    port = 9600
    try:
        for rt in runtimes:
            for w in WORKS:
                port += 1
                try:
                    results[rt["name"]][str(w)] = run_point(rt, w, port)
                except Exception as e:
                    print("  !! %s work=%d FAILED %r" % (rt["name"], w, e), flush=True)
                    results[rt["name"]][str(w)] = {"error": repr(e)}
                subprocess.run(["sudo", "-n", "pkill", "-9", "-f",
                                "RLXRT_%s_%d_%d" % (rt["name"], w, port)],
                               capture_output=True)
                time.sleep(0.4)
    finally:
        topo.teardown()

    rt_meta = {rt["name"]: {"cores": rt["cores"], "label": rt["label"], "kind": rt["kind"]}
               for rt in runtimes}
    out = os.path.join(config.RESULTS_DIR, "work_xrt.json")
    # merge-on-write: if --only re-ran a subset, keep the other runtimes' data
    merged_results, merged_rt = dict(results), dict(rt_meta)
    if os.path.exists(out):
        old = json.load(open(out))
        old_res = dict(old.get("results", {})); old_res.update(results); merged_results = old_res
        old_rt = dict(old.get("meta", {}).get("runtimes", {})); old_rt.update(rt_meta); merged_rt = old_rt
    meta = {"payload": PAYLOAD, "works": WORKS, "ladder": LADDER, "reps": REPS,
            "runtimes": merged_rt}
    with open(out, "w") as f:
        json.dump({"meta": meta, "results": merged_results}, f, indent=2)

    print("\n=== cross-runtime work curve (PER-CORE rps, 1 KiB payload) ===")
    head = "  %-8s" % "work" + "".join("%16s" % rt["name"] for rt in runtimes)
    print(head)
    for w in WORKS:
        row = "  %-8d" % w
        for rt in runtimes:
            r = results[rt["name"]].get(str(w), {})
            rps = r.get("peak", {}).get("rps_median")
            row += "%16s" % (("%.0f" % (rps / rt["cores"])) if rps else "-")
        print(row)
    print("\nwrote", out)


if __name__ == "__main__":
    main()
