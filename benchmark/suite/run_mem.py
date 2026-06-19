#!/usr/bin/env python3
"""Memory benchmark orchestrator.

Matrix (spec): columns = [go, runloom py handler, runloom py+optimize(memory),
runloom c handler]; rows = [empty bytes/fiber, with-socket bytes/fiber,
1M fibers total RSS].

Per-fiber rows use a baseline (n=0) RSS subtracted from an n=K RSS, so the number
is the *incremental* used memory per fiber, not fixed interpreter/runtime
overhead. The 1M row is the absolute resident set for a million live fibers.
All numbers are USED memory (VmRSS / PSS), never virtual size.

Usage: python3 run_mem.py [--quick]
"""
import argparse
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "harness"))
import config
import topo

MEM = os.path.join(config.SUITE_DIR, "memory")
MEM_GO = os.path.join(MEM, "mem_go")
MEM_RL = os.path.join(MEM, "mem_runloom.py")
P = config.FT_PYTHON
MANY = config.SERVER_CPU_SPEC


def parse_json(out):
    for line in reversed(out.strip().splitlines()):
        line = line.strip()
        if line.startswith("{"):
            return json.loads(line)
    raise RuntimeError("no JSON in output: %s" % out[-400:])


def run(argv, gil_off, timeout=600):
    cmd = topo.pinned_cmd(argv, cpus=MANY, gil_off=gil_off, raise_fd=True)
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    try:
        return parse_json(r.stdout)
    except Exception as e:
        raise RuntimeError("%r (rc=%d) stderr=%s" % (e, r.returncode, r.stderr[-400:]))


# config -> (builder(state, n) -> argv, gil_off)
def go_argv(state, n):
    return [MEM_GO, "-state", state, "-n", str(n)]


def rl_argv(handler, optimize):
    def b(state, n):
        a = [P, MEM_RL, "--state", state, "--n", str(n), "--handler", handler,
             "--hubs", str(config.HUBS)]
        if optimize != "none":
            a += ["--optimize", optimize]
        return a
    return b


CONFIGS = [
    ("go", go_argv, False),
    ("runloom_py", rl_argv("py", "none"), True),
    ("runloom_py_optmem", rl_argv("py", "memory"), True),
    ("runloom_c", rl_argv("c", "none"), True),
]


def per_unit(builder, gil_off, state, k):
    base = run(builder(state, 0), gil_off)
    full = run(builder(state, k), gil_off)
    rss_b, rss_f = base.get("rss_bytes") or 0, full.get("rss_bytes") or 0
    pss_b, pss_f = base.get("pss_bytes"), full.get("pss_bytes")
    out = {"n": k, "rss_total": rss_f, "rss_baseline": rss_b,
           "bytes_per_fiber_rss": (rss_f - rss_b) / k if k else None}
    if pss_b is not None and pss_f is not None:
        out["bytes_per_fiber_pss"] = (pss_f - pss_b) / k
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    args = ap.parse_args()
    n_empty = 50_000 if args.quick else 200_000
    n_socket = 5_000 if args.quick else 20_000
    n_million = 200_000 if args.quick else 1_000_000

    topo.ensure_kernel_ceilings()   # vm.max_map_count / fs.nr_open for 1M fibers
    os.makedirs(config.RESULTS_DIR, exist_ok=True)
    results = {"meta": config.summary(), "quick": args.quick, "configs": {}}

    for name, builder, gil_off in CONFIGS:
        results["configs"][name] = {}
        # empty bytes/fiber
        try:
            e = per_unit(builder, gil_off, "empty", n_empty)
            results["configs"][name]["empty"] = e
            print("empty   %-20s %6.0f B/fiber RSS  (n=%d, total=%.1f MiB)"
                  % (name, e["bytes_per_fiber_rss"], e["n"], e["rss_total"] / 2**20), flush=True)
        except Exception as ex:
            print("empty   %-20s FAILED %r" % (name, ex), flush=True)
            results["configs"][name]["empty"] = {"error": repr(ex)}
        # with-socket bytes/fiber
        try:
            s = per_unit(builder, gil_off, "socket", n_socket)
            results["configs"][name]["socket"] = s
            print("socket  %-20s %6.0f B/fiber RSS  (n=%d, total=%.1f MiB)"
                  % (name, s["bytes_per_fiber_rss"], s["n"], s["rss_total"] / 2**20), flush=True)
        except Exception as ex:
            print("socket  %-20s FAILED %r" % (name, ex), flush=True)
            results["configs"][name]["socket"] = {"error": repr(ex)}
        # 1M total (empty)
        try:
            m = run(builder("empty", n_million), gil_off, timeout=900)
            results["configs"][name]["million"] = {
                "n": n_million, "rss_total": m.get("rss_bytes"),
                "pss_total": m.get("pss_bytes"),
                "rss_per_fiber": (m.get("rss_bytes") or 0) / n_million}
            print("1M      %-20s %.2f GiB total RSS  (%.0f B/fiber, n=%d)"
                  % (name, (m.get("rss_bytes") or 0) / 2**30,
                     (m.get("rss_bytes") or 0) / n_million, n_million), flush=True)
        except Exception as ex:
            print("1M      %-20s FAILED %r" % (name, ex), flush=True)
            results["configs"][name]["million"] = {"error": repr(ex)}

    out = os.path.join(config.RESULTS_DIR, "mem_quick.json" if args.quick else "mem.json")
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print("\nwrote", out, flush=True)


if __name__ == "__main__":
    main()
