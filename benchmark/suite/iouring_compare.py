#!/usr/bin/env python3
"""Focused io_uring-vs-epoll comparison, to settle whether the loop backend's
"+20% over epoll" reproduces and whether it transfers to a real handler.

Two tests, each epoll vs RUNLOOM_IOURING_LOOP=1, same server otherwise:

  Test 1  all-C 8-byte echo (serve handler=None -> runloom_io_c_echo, a
          tstate-free c_entry fiber). This is the ORIGINAL +20% condition:
          tiny payload (syscall-count-bound, where batching wins) + no Python
          state (cheap always-park). Payload = 8 bytes.

  Test 2  Cython C handler at 1 KiB (srv_runloom_cython.py). A REAL handler on
          a Python-tstate fiber. The capi now routes through the Stage-2 proactor
          (loop_recv) under the loop backend, so this asks: does the batching win
          survive the per-park tstate cost at a realistic payload? Payload = 1 KiB.

Writes results/iouring_test.json.

Results (2026-06-19, Xeon E5-2696 v3, free-threaded 3.13t; full record in
../IOURING_TSTATE_FINDINGS.md):
  Test 1  8-byte all-C echo : epoll 654k vs io_uring 659k (both client-bound);
          server ceiling 1.14M -> 1.21M = +6%. Modest: the all-C epoll path is
          already near-optimal for a tiny tstate-free echo.
  Test 2  1 KiB Cython      : epoll 455k (server-bound) vs io_uring 639k
          (client-bound); server ceiling 533k -> 1.16M = +2.17x, server CPU
          85% -> 55%. The proactor batching beats epoll decisively for a real
          handler -- and runloom_cython on io_uring becomes the FASTEST runloom
          config in the suite. "io_uring loses on loopback" was an artifact of
          driving it through the readiness path instead of loop_recv.
"""
import json
import os
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


def factory(script, extra, env, port, token):
    def f():
        argv = [P, os.path.join(SD, script), "--host", config.SRV_IP,
                "--port", str(port), "--hubs", str(HUBS), "--token", token] + extra
        cmd = topo.ns_cmd(config.SRV_NS, argv, cpus=MANY, extra_env=env,
                          gil_off=True, raise_fd=True)
        srv = measure.Server(cmd, token, script)
        srv.start(timeout=40)
        time.sleep(0.5)
        return srv
    return f


def run_one(name, script, extra, env, payload, port):
    token = "RLIOU_%s_%d" % (name, port)
    print("\n== %s (payload=%dB, env=%s) ==" % (name, payload, env or "epoll"), flush=True)
    out = measure.ladder(
        factory(script, extra, env, port, token), LOADGEN,
        "%s:%d" % (config.SRV_IP, port), payload, config.CONN_LADDER,
        3, config.RAMP_S, config.MEASURE_S, config.CLIENT_CORES,
        config.PLATEAU_PATIENCE, server_cpus=SRV_CPUS)
    pk = out["peak"]
    out["_payload"] = payload
    print("  -> %-24s peak=%.0f rps @ conns=%s  bottleneck=%s  server-ceiling=%s"
          % (name, pk["rps_median"], pk.get("conns"), out["bottleneck_at_peak"],
             ("%.0f" % out["server_ceiling_est"]) if out.get("server_ceiling_est") else "-"),
          flush=True)
    return out


def main():
    topo.setup()
    results = {}
    port = 9300
    try:
        IOU = {"RUNLOOM_IOURING_LOOP": "1"}
        cases = [
            # io_uring vs epoll (8-byte all-C echo + 1 KiB Cython handler)
            ("cecho_epoll", "srv_runloom_cecho.py", [], {}, 8),
            ("cecho_iouring", "srv_runloom_cecho.py", [], IOU, 8),
            ("cython_epoll", "srv_runloom_cython.py", ["--optimize", "none"], {}, 1024),
            ("cython_iouring_proactor", "srv_runloom_cython.py", ["--optimize", "none"], IOU, 1024),
            # tstate bypass: a Python-fiber Cython handler vs a tstate-free cdef
            # c_entry handler, at 8 bytes (op-bound -- where per-park tstate cost
            # should matter) and 1 KiB (I/O-bound -- where it should wash out).
            ("cython_iouring_8b", "srv_runloom_cython.py", ["--optimize", "none"], IOU, 8),
            ("cdef_iouring_8b", "srv_runloom_cdef.py", [], IOU, 8),
            ("cdef_iouring_1k", "srv_runloom_cdef.py", [], IOU, 1024),
        ]
        for name, script, extra, env, payload in cases:
            port += 1
            try:
                results[name] = run_one(name, script, extra, env, payload, port)
            except Exception as e:
                print("  !! %s FAILED %r" % (name, e), flush=True)
                results[name] = {"error": repr(e)}
            import subprocess
            subprocess.run(["sudo", "-n", "pkill", "-9", "-f", "RLIOU_%s_%d" % (name, port)],
                           capture_output=True)
            time.sleep(0.5)
    finally:
        topo.teardown()
    out = os.path.join(config.RESULTS_DIR, "iouring_test.json")
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    # headline
    print("\n=== io_uring vs epoll summary ===")
    def peak(n):
        r = results.get(n, {})
        return r.get("peak", {}).get("rps_median", 0), r.get("server_ceiling_est") or 0
    for a, b, label in [("cecho_epoll", "cecho_iouring", "all-C 8B echo"),
                        ("cython_epoll", "cython_iouring_proactor", "Cython 1KB handler")]:
        pa, ca = peak(a); pb, cb = peak(b)
        if pa and pb:
            print("  %-20s epoll peak=%.0f (ceil %.0f) | io_uring peak=%.0f (ceil %.0f) | "
                  "io_uring/epoll = %.2fx peak, %.2fx ceiling"
                  % (label, pa, ca, pb, cb, pb / pa, (cb / ca) if ca else 0))
    print("\nwrote", out)


if __name__ == "__main__":
    main()
