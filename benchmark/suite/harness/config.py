"""Shared configuration for the Runloom benchmark suite.

Everything that the spec pins to os.cpu_count() is derived here so every program
agrees on the same numbers, and the report can print them as the assumed
constraints.  Core *placement* implements scoping decision #3 (client and server
on disjoint cores so they don't steal each other's CPU) + #5 (per-core =
saturated multi-core / hub_count).
"""
import os

# ---------------------------------------------------------------------------
# Machine-derived sizing (the spec's int(cpu*k) knobs)
# ---------------------------------------------------------------------------
CPU_COUNT = os.cpu_count() or 1

HUBS = int(CPU_COUNT * 0.7)          # runloom M:N hubs  (spec: int(cpu*0.7))
GO_SERVER_CORES = int(CPU_COUNT * 0.7)  # go GOMAXPROCS    (spec: int(cpu*0.7))
CLIENT_CORES = int(CPU_COUNT * 0.25)    # go loadgen cores (spec: int(cpu*0.25))

# ---------------------------------------------------------------------------
# Disjoint CPU placement (decision #3).
#   client  -> the first CLIENT_CORES cpus (NUMA node 0 on the 2-node box)
#   server  -> the next HUBS cpus, starting after the client set
# They never overlap, so the loadgen cannot steal a server hub's core.  On the
# 64-vCPU / 2-NUMA test box this puts the client wholly in node0 (0-15) and the
# server in node0's tail + node1 (16-59); the gap is documented in the report.
# ---------------------------------------------------------------------------
_all = list(range(CPU_COUNT))
CLIENT_CPUS = _all[:CLIENT_CORES]
SERVER_CPUS = _all[CLIENT_CORES:CLIENT_CORES + max(HUBS, GO_SERVER_CORES)]
# taskset -c strings
CLIENT_CPU_SPEC = ",".join(map(str, CLIENT_CPUS))
SERVER_CPU_SPEC = ",".join(map(str, SERVER_CPUS))

# ---------------------------------------------------------------------------
# Network topology (decision #3: veth pair across two netns)
# ---------------------------------------------------------------------------
SRV_NS = "rl_srv"
CLI_NS = "rl_cli"
VETH_SRV = "rl_vsrv"
VETH_CLI = "rl_vcli"
SRV_IP = "10.99.0.1"
CLI_IP = "10.99.0.2"
PREFIX = 24
BASE_PORT = 9000

# Client source-IP fan-out for the CONNECTION-CHURN benchmark.  The churn client
# actively closes every connection, so it accumulates TIME_WAIT and burns
# ephemeral ports on its own 4-tuples; a single source IP caps out at ~64k ports,
# and at a high conn/s rate the TIME_WAIT backlog (rate x fin_timeout) exceeds
# that and dials begin to fail -- capping the MEASURED conn/s, not the server.
# This is the netns/veth analog of big_100's 127/8 fragment trick: spread the
# connects across a block of source IPs on the client veth so each gets its own
# independent ephemeral/TIME_WAIT pool.  All live in the client /24 (on-link, so
# the server's connected route returns replies with no extra routing).  The
# primary CLI_IP is first; BENCH_CLI_SRC_IPS overrides the count.
CLI_SRC_IP_COUNT = int(os.environ.get("BENCH_CLI_SRC_IPS", "32"))
CLI_SRC_IPS = ["10.99.0.%d" % (2 + i) for i in range(max(1, CLI_SRC_IP_COUNT))]

# Spec sysctls, applied INSIDE the server netns (they are namespaced).
NS_SYSCTLS = {
    "net.ipv4.tcp_wmem": "4096 16384 2097152",   # 2 MB max
    "net.ipv4.tcp_rmem": "4096 87380 2097152",   # 2 MB max
    "net.core.somaxconn": "65535",               # accept backlog ceiling
    "net.ipv4.tcp_tw_reuse": "1",
}

# fd ceiling for millions-of-connections runs (decision: raise per-block).
FD_LIMIT = 8_388_608

# ---------------------------------------------------------------------------
# Payloads (decision #1)
#   req/s headline  -> small payload, measures scheduling/syscall overhead
#   bandwidth (GB/s)-> the spec's 1.5 MB buffer, measures copy/IO throughput
# ---------------------------------------------------------------------------
PAYLOAD_SMALL = 1024                 # 1 KiB request for the req/s metric
PAYLOAD_LARGE = 1536 * 1024          # 1.5 MiB for the bandwidth metric
CLIENT_BUF = 1536 * 1024             # fixed 1.5 MB client buffer (spec)

# ---------------------------------------------------------------------------
# Measurement (decision #8: rigorous stop rule + ladder)
# ---------------------------------------------------------------------------
RAMP_S = 2.0                         # establish + warm connections before timing
MEASURE_S = 5.0                      # timed window
REPS = 3                             # independent reps per ladder rung
# Geometric connection ladder; stop when 2 consecutive rungs fail to beat the
# best rung's bootstrap CI.  Capped at 32768 so the PERSISTENT req/s benchmark --
# which establishes this many CONCURRENT connections from the single client IP --
# stays under the ~64k ephemeral-port ceiling.  (The CHURN benchmark fans its
# connects across CLI_SRC_IPS, so it is no longer bound by that ceiling at these
# rungs; the cap is the persistent benchmark's constraint.)
CONN_LADDER = [16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768]
PLATEAU_PATIENCE = 2

# ---------------------------------------------------------------------------
# Interpreters (decision #4 + #7)
# ---------------------------------------------------------------------------
PYENV = os.path.expanduser("~/.pyenv/versions")
FT_PYTHON = os.path.join(PYENV, "3.14.4t", "bin", "python3")   # runloom (GIL off)
GIL_PYTHON = os.path.join(PYENV, "3.13.13", "bin", "python3")   # asyncio/uvloop/gevent best-case

# Repo paths
HARNESS_DIR = os.path.dirname(os.path.abspath(__file__))
SUITE_DIR = os.path.dirname(HARNESS_DIR)
BENCH_DIR = os.path.dirname(SUITE_DIR)
REPO = os.path.dirname(BENCH_DIR)
SRC = os.path.join(REPO, "src")
RESULTS_DIR = os.path.join(BENCH_DIR, "results")
SERVERS_DIR = os.path.join(SUITE_DIR, "servers")
CLIENTS_DIR = os.path.join(SUITE_DIR, "clients")


def git_commit():
    """Short HEAD sha of the repo the suite is running from (provenance: which
    commit produced these numbers).  '?' if git is unavailable."""
    import subprocess
    try:
        return subprocess.run(["git", "-C", REPO, "rev-parse", "--short", "HEAD"],
                              capture_output=True, text=True, timeout=10).stdout.strip() or "?"
    except Exception:
        return "?"


def base_env(gil_off=True):
    """A clean child env: PYTHONPATH=src, GIL toggled, RUNLOOM_DEBUG cleared
    (decision #7: as-shipped release, debug OFF)."""
    e = dict(os.environ)
    e["PYTHONPATH"] = SRC + (os.pathsep + e["PYTHONPATH"] if e.get("PYTHONPATH") else "")
    e["PYTHON_GIL"] = "0" if gil_off else "1"
    e.pop("RUNLOOM_DEBUG", None)
    return e


def summary():
    return {
        "git_commit": git_commit(),
        "cpu_count": CPU_COUNT,
        "hubs": HUBS,
        "go_server_cores": GO_SERVER_CORES,
        "client_cores": CLIENT_CORES,
        "client_cpus": CLIENT_CPU_SPEC,
        "server_cpus": SERVER_CPU_SPEC,
        "payload_small_bytes": PAYLOAD_SMALL,
        "payload_large_bytes": PAYLOAD_LARGE,
        "ramp_s": RAMP_S,
        "measure_s": MEASURE_S,
        "reps": REPS,
        "conn_ladder": CONN_LADDER,
        "sysctls": NS_SYSCTLS,
        "fd_limit": FD_LIMIT,
        "ft_python": FT_PYTHON,
        "gil_python": GIL_PYTHON,
    }


if __name__ == "__main__":
    import json
    print(json.dumps(summary(), indent=2))
