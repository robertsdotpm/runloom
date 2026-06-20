#!/usr/bin/env python3
"""Profile the Cython-on-epoll anomaly AT SATURATION in the netns (not loopback,
which was loadgen-limited and never reached the server wall).

Hypothesis under test (user's): the Cython handler touches a shared Python object
per request that gets atomic-refcounted / critical-section-locked across the 44
hub threads, so it contends only once the server is pushed hard -- which is why
the 2x appears at saturation (netns) but not at low load (loopback).

Pushes 2048 conns (saturates the cython server ~425k) and `perf record -g`s the
server cores during steady state, for both the Python and Cython handlers. Then
greps the call-graph report for the lock/atomic/refcount-contention signature.

Notes are appended to results/anomaly_notes.md.
"""
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "harness"))
import config
import topo
import measure

RES = config.RESULTS_DIR
PY = config.FT_PYTHON
SD = config.SERVERS_DIR
LOADGEN = os.path.join(config.CLIENTS_DIR, "loadgen")
MANY = config.SERVER_CPU_SPEC
SRVCPU = "%d-%d" % (config.SERVER_CPUS[0], config.SERVER_CPUS[-1])
SIG = "lock|futex|atomic|_Py_Dealloc|_Py_MergeZero|_Py_DecRefShared|brc_|CriticalSection|incref|decref|refcount|mimalloc|mi_|spin"


def sudo(*a):
    return subprocess.run(["sudo", "-n", *a], capture_output=True, text=True)


def main():
    sudo("sysctl", "-w", "kernel.perf_event_paranoid=-1", "kernel.kptr_restrict=0")
    topo.setup()
    notes = open(os.path.join(RES, "anomaly_notes.md"), "a")
    notes.write("\n## Saturation profile (netns, 2048 conns, server cores %s)\n\n" % SRVCPU)
    try:
        for tier, script, extra in [("cython", "runloom_iouring_cython_tcpcon.py", ["--optimize", "none"]),
                                    ("py", "runloom_epoll_py_tcpcon.py", [])]:
            port = config.BASE_PORT + 700 + (0 if tier == "cython" else 1)
            token = "SAT_%s_%d" % (tier, port)
            argv = [PY, os.path.join(SD, script), "--host", config.SRV_IP,
                    "--port", str(port), "--hubs", str(config.HUBS), "--token", token] + extra
            cmd = topo.ns_cmd(config.SRV_NS, argv, cpus=MANY, raise_fd=True)
            srv = measure.Server(cmd, token, tier)
            srv.start(timeout=40)
            time.sleep(0.5)
            loadcmd = topo.ns_cmd(config.CLI_NS, [LOADGEN, "-addr", "%s:%d" % (config.SRV_IP, port),
                                  "-conns", "2048", "-payload", "1024", "-ramp", "3",
                                  "-measure", "32", "-gomaxprocs", str(config.CLIENT_CORES)],
                                  cpus=config.CLIENT_CPU_SPEC, raise_fd=True)
            load = subprocess.Popen(loadcmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
            time.sleep(8)  # past ramp, saturated
            data = os.path.join(RES, "sat_%s.data" % tier)
            sudo("perf", "record", "-C", SRVCPU, "-g", "-F", "999", "-o", data, "--", "sleep", "14")
            try:
                lout, _ = load.communicate(timeout=20)
            except Exception:
                load.kill(); lout = ""
            srv.stop()
            time.sleep(1)
            top = os.path.join(RES, "sat_top_%s.txt" % tier)
            with open(top, "w") as f:
                subprocess.run(["sudo", "-n", "perf", "report", "-i", data, "--stdio",
                                "--percent-limit", "0.7", "-g", "graph,0.5,caller"],
                               stdout=f, stderr=subprocess.DEVNULL)
            # rps the loadgen reported
            rps = ""
            for line in (lout or "").splitlines():
                if line.strip().startswith("{"):
                    import json
                    rps = "%.0f rps" % json.loads(line)["rps"]
            # contention signature self% total
            sig = subprocess.run("grep -iE '%s' '%s' | head -40" % (SIG, top),
                                 shell=True, capture_output=True, text=True).stdout
            notes.write("### %s (%s)\n\n```\n%s```\n\n" % (tier, rps, sig[:2500] or "(no lock/atomic/refcount frames above threshold)\n"))
            print("=== %s %s -> contention-signature frames ===" % (tier, rps))
            print(sig[:1500] or "(none above threshold)")
    finally:
        notes.close()
        topo.teardown()
        sudo("sysctl", "-w", "kernel.perf_event_paranoid=2", "kernel.kptr_restrict=1")
    print("\nfull reports: results/sat_top_{cython,py}.txt ; notes: results/anomaly_notes.md")


if __name__ == "__main__":
    main()
