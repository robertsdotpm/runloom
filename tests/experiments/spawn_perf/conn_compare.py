#!/usr/bin/env python3
"""Apples-to-apples conn/s CPU-efficiency comparison.

The --quick conn_churn run never saturated the servers (client-bound), so its
conn/s + CPU% were not real ceilings.  This forces the SERVER to be the bottleneck:
pin every server to the SAME small core budget (default 2 cores), hit it with a big
client (16 cores), and measure conn/s.  Same cores => conn/s IS the CPU efficiency.
We also read the server's /proc CPU over the window to CONFIRM it saturated (~200%
on 2 cores) -- if it didn't, the number is still client-bound and flagged.

Run on loopback (the firewall/softirq tax hits every server equally, so the RELATIVE
comparison holds; absolute numbers are depressed vs a real NIC)."""
import argparse
import json
import os
import socket
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.normpath(os.path.join(HERE, "..", ".."))
SRV = os.path.join(REPO, "benchmark", "suite", "servers")
CLI = os.path.join(REPO, "benchmark", "suite", "clients", "churn_loadgen")
PY = os.path.expanduser("~/.pyenv/versions/3.14.4t/bin/python3")
CLK = os.sysconf("SC_CLK_TCK")


def proc_cpu_jiffies(pid):
    try:
        f = open("/proc/%d/stat" % pid).read()
        parts = f[f.rfind(")") + 2:].split()
        return int(parts[11]) + int(parts[12])  # utime + stime (after comm)
    except Exception:
        return None


def wait_listen(host, port, timeout=20):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            s = socket.create_connection((host, port), 0.5)
            s.close()
            return True
        except OSError:
            time.sleep(0.2)
    return False


def run_server(spec, host, port, cores):
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH=os.path.join(REPO, "src"))
    env.update(spec.get("env", {}))
    argv = ["taskset", "-c", cores] + spec["cmd"](host, port)
    p = subprocess.Popen(argv, env=env, stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL, cwd=SRV)
    return p


def measure(spec, host, port, server_cores, client_cores, dialers, payload, measure_s):
    p = run_server(spec, host, port, server_cores)
    try:
        if not wait_listen(host, port):
            return {"server": spec["name"], "error": "no listen"}
        time.sleep(0.5)
        ncores = len(server_cores.split(","))  # rough; for "a-b" handled below
        if "-" in server_cores:
            a, b = server_cores.split("-"); ncores = int(b) - int(a) + 1
        c0 = proc_cpu_jiffies(p.pid)
        t0 = time.time()
        cli = subprocess.run(
            ["taskset", "-c", client_cores, CLI, "-addr", "%s:%d" % (host, port),
             "-conns", str(dialers), "-gomaxprocs", "16", "-payload", str(payload),
             "-measure", str(measure_s), "-ramp", "2"],
            capture_output=True, text=True, timeout=measure_s + 30)
        dt = time.time() - t0
        c1 = proc_cpu_jiffies(p.pid)
        res = {}
        for line in cli.stdout.splitlines():
            if line.strip().startswith("{"):
                res = json.loads(line)
        conns = res.get("conns_per_s") or res.get("rps") or 0.0
        srv_cpu = None
        if c0 is not None and c1 is not None:
            srv_cpu = 100.0 * (c1 - c0) / CLK / dt  # % of one core, over ACTUAL elapsed
        return {"server": spec["name"], "conns_per_s": conns,
                "server_cpu_pct": srv_cpu, "server_cores": ncores,
                "saturated": (srv_cpu is not None and srv_cpu > ncores * 90),
                "conns_per_core": conns / ncores if conns else 0.0}
    finally:
        p.terminate()
        try:
            p.wait(3)
        except Exception:
            p.kill()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server-cores", default="60-61")
    ap.add_argument("--client-cores", default="0-15")
    ap.add_argument("--dialers", type=int, default=400)
    ap.add_argument("--payload", type=int, default=1024)
    ap.add_argument("--measure", type=float, default=10.0)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9070)
    args = ap.parse_args()

    def ncores_of(s):
        return int(s.split('-')[1]) - int(s.split('-')[0]) + 1 if "-" in s else len(s.split(","))
    nc = str(ncores_of(args.server_cores))
    SPECS = [
        {"name": "runloom_cdef (compiled handler)", "cmd": lambda h, p: [
            PY, os.path.join(SRV, "srv_runloom_cdef.py"), "--host", h, "--port", str(p), "--hubs", nc]},
        {"name": "go", "cmd": lambda h, p: [
            os.path.join(SRV, "srv_go"), "-host", h, "-port", str(p), "-gomaxprocs", nc]},
        {"name": "runloom_c (py handler)", "cmd": lambda h, p: [
            PY, os.path.join(SRV, "srv_runloom_c.py"), "--host", h, "--port", str(p), "--hubs", nc]},
    ]
    print("server-bound conn/s on cores %s (%d core(s)), client=%s, dialers=%d, payload=%dB\n"
          % (args.server_cores, ncores_of(args.server_cores),
             args.client_cores, args.dialers, args.payload), file=sys.stderr)
    port = args.port
    rows = []
    for spec in SPECS:
        r = measure(spec, args.host, port, args.server_cores, args.client_cores,
                    args.dialers, args.payload, args.measure)
        rows.append(r)
        port += 1
        if "error" in r:
            print("%-32s ERROR: %s" % (r["server"], r["error"]), file=sys.stderr)
        else:
            print("%-32s %8.0f conn/s   srvCPU=%5.0f%%  %s  -> %7.0f conn/s/core"
                  % (r["server"], r["conns_per_s"], r["server_cpu_pct"] or -1,
                     "SATURATED" if r["saturated"] else "not-sat(client-bound!)",
                     r["conns_per_core"]), file=sys.stderr)
    print(json.dumps(rows))


if __name__ == "__main__":
    main()
