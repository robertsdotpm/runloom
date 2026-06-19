#!/usr/bin/env python3
"""The handler work-curve: what does compiling the handler's WORK buy?

ONE server (srv_runloom_work.py), ONE knob (--work N = FNV-1a passes over the
payload), TWO builds of the identical algorithm: --handler py (interpreted
py_fnv) vs --handler cython (compiled work_cy.fnv_work). Same runtime (runloom,
same proactor/epoll I/O), same payload -- the ONLY variable is whether the
handler's per-byte work is interpreted or native.

`--work 0` IS the echo (the handler skips the work call), so the leftmost point
of the curve consolidates the echo load and should reproduce the echo numbers
(~600k saturated) -- a built-in cross-check. As N grows the interpreted curve
bends down while the compiled curve holds; that gap is the thing echo could
never show (every handler optimization ties on echo -- no handler CPU).

The work is PURE inline arithmetic (FNV xor/mul loop) -- nothing runloom routes
to the blockpool, so it runs on the fiber's hub and the per-core CPU accounting
stays valid. A hashlib/json/struct call would offload or converge to native and
erase the signal; stated in the report as the honest framing.

Short saturating ladder per point: as work rises the server gets more
CPU-bound and saturates at LOWER conn counts, so [1024,2048,4096] (2048 was the
proven echo saturation knee) captures every point. Writes results/work_curve.json.
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
P = config.FT_PYTHON
LOADGEN = os.path.join(config.CLIENTS_DIR, "loadgen")
MANY = config.SERVER_CPU_SPEC
HUBS = config.HUBS
SRV_CPUS = [int(c) for c in MANY.split(",")]

PAYLOAD = 1024                       # 1 KiB: enough bytes for the work to register
WORKS = [0, 1, 2, 4, 8, 16, 32, 64]  # FNV passes; 0 == echo (the lowest curve point)
HANDLERS = ["py", "cython"]
LADDER = [1024, 2048, 4096]          # short saturating ladder (work>0 saturates earlier)
REPS = 2


def factory(handler, work, port, token):
    def f():
        argv = [P, os.path.join(SD, "srv_runloom_work.py"), "--host", config.SRV_IP,
                "--port", str(port), "--hubs", str(HUBS), "--token", token,
                "--handler", handler, "--work", str(work)]
        cmd = topo.ns_cmd(config.SRV_NS, argv, cpus=MANY, extra_env={},
                          gil_off=True, raise_fd=True)
        srv = measure.Server(cmd, token, "work_%s_%d" % (handler, work))
        srv.start(timeout=40)
        time.sleep(0.5)
        return srv
    return f


def run_point(handler, work, port):
    token = "RLWORK_%s_%d_%d" % (handler, work, port)
    label = "%s work=%d" % (handler, work)
    print("\n== %s (payload=%dB) ==" % (label, PAYLOAD), flush=True)
    out = measure.ladder(
        factory(handler, work, port, token), LOADGEN,
        "%s:%d" % (config.SRV_IP, port), PAYLOAD, LADDER,
        REPS, config.RAMP_S, config.MEASURE_S, config.CLIENT_CORES,
        len(LADDER), server_cpus=SRV_CPUS)        # patience = full ladder: take the max
    pk = out["peak"]
    print("  -> %-16s peak=%.0f rps @ conns=%s  server_cpu=%.0f%%  bottleneck=%s"
          % (label, pk["rps_median"], pk.get("conns"),
             (pk.get("server_cpu_util") or 0) * 100, out["bottleneck_at_peak"]),
          flush=True)
    return out


def main():
    topo.setup()
    results = {h: {} for h in HANDLERS}
    port = 9400
    try:
        for handler in HANDLERS:
            for work in WORKS:
                port += 1
                try:
                    results[handler][str(work)] = run_point(handler, work, port)
                except Exception as e:
                    print("  !! %s work=%d FAILED %r" % (handler, work, e), flush=True)
                    results[handler][str(work)] = {"error": repr(e)}
                subprocess.run(["sudo", "-n", "pkill", "-9", "-f",
                                "RLWORK_%s_%d_%d" % (handler, work, port)],
                               capture_output=True)
                time.sleep(0.4)
    finally:
        topo.teardown()

    meta = {"payload": PAYLOAD, "works": WORKS, "ladder": LADDER, "reps": REPS,
            "hubs": HUBS, "server_cpus": MANY}
    out = os.path.join(config.RESULTS_DIR, "work_curve.json")
    with open(out, "w") as f:
        json.dump({"meta": meta, "results": results}, f, indent=2)

    # headline: the curve + the py-vs-cython speedup at each work level
    print("\n=== handler work curve (peak rps, 1 KiB payload) ===")
    print("  %-6s %12s %12s %9s   %s" % ("work", "py", "cython", "cy/py", "bottleneck(py|cy)"))

    def pk(h, w):
        r = results[h].get(str(w), {})
        p = r.get("peak", {})
        return p.get("rps_median", 0.0), r.get("bottleneck_at_peak", "?")

    for w in WORKS:
        py, bpy = pk("py", w)
        cy, bcy = pk("cython", w)
        spd = (cy / py) if py else 0.0
        tag = "  (echo)" if w == 0 else ""
        print("  %-6s %12.0f %12.0f %8.2fx   %s|%s%s"
              % (w, py, cy, spd, bpy[:6], bcy[:6], tag))
    print("\nwrote", out)


if __name__ == "__main__":
    main()
