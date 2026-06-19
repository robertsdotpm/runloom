"""Capture hardware / OS / toolchain details for the report header.

Decision: record exactly which machine produced the numbers, and flag the
things that affect reproducibility on this box -- it's a VMware guest (vCPUs,
possible steal), 2 NUMA nodes, and the editor-shell fd cap.
"""
import json
import os
import platform
import re
import subprocess


def _run(cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=15).stdout.strip()
    except Exception:
        return ""


def _read(path):
    try:
        with open(path) as f:
            return f.read()
    except Exception:
        return ""


def _cpu_model():
    for line in _read("/proc/cpuinfo").splitlines():
        if line.startswith("model name"):
            return line.split(":", 1)[1].strip()
    return platform.processor() or "unknown"


def _numa():
    nodes = {}
    base = "/sys/devices/system/node"
    if os.path.isdir(base):
        for n in sorted(os.listdir(base)):
            if re.fullmatch(r"node\d+", n):
                nodes[n] = _read(os.path.join(base, n, "cpulist")).strip()
    return nodes


def _mem_total_gib():
    for line in _read("/proc/meminfo").splitlines():
        if line.startswith("MemTotal"):
            kb = int(line.split()[1])
            return round(kb / 1024 / 1024, 1)
    return None


def _governor():
    g = _read("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor").strip()
    return g or "n/a"


def _steal_pct():
    # vmstat 'st' column: % CPU stolen by the hypervisor. >0 means noisy VM.
    out = _run(["vmstat", "1", "2"])
    lines = [l for l in out.splitlines() if l.strip() and l.split()[0].isdigit()]
    if lines:
        cols = lines[-1].split()
        if len(cols) >= 17:
            return cols[16]  # st
    return None


def _py_version(py):
    out = _run([py, "-c",
                "import sys;print(sys.version.split()[0], 'FT' if not sys._is_gil_enabled() else 'GIL')"])
    return out or "missing"


def _git_sha(repo):
    return _run(["git", "-C", repo, "rev-parse", "--short", "HEAD"]) or "?"


def _runloom_build_flags(repo):
    """The compile flags actually used (proves -O2 release, NDEBUG, no debug)."""
    # setup.py emits -O2 -D_FORTIFY_SOURCE=2 in release; reflect what we built.
    so = ""
    srcdir = os.path.join(repo, "src")
    if os.path.isdir(srcdir):
        for f in os.listdir(srcdir):
            if f.startswith("runloom_c") and f.endswith(".so"):
                so = os.path.join(srcdir, f)
                break
    return {
        "so": os.path.basename(so) if so else None,
        "RUNLOOM_DEBUG_env": os.environ.get("RUNLOOM_DEBUG", "<unset>"),
        "expected_cflags": "-O2 -DNDEBUG -D_FORTIFY_SOURCE=2 -fstack-protector-strong (as-shipped release)",
    }


def capture(repo=None):
    repo = repo or os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))))
    info = {
        "hostname": platform.node(),
        "os": _run(["bash", "-lc", ". /etc/os-release 2>/dev/null; echo $PRETTY_NAME"]) or platform.platform(),
        "kernel": platform.release(),
        "arch": platform.machine(),
        "cpu_model": _cpu_model(),
        "logical_cpus": os.cpu_count(),
        "numa_nodes": _numa(),
        "mem_total_gib": _mem_total_gib(),
        "cpu_governor": _governor(),
        "virtualization": _run(["systemd-detect-virt"]) or "unknown",
        "steal_pct_sample": _steal_pct(),
        "python_ft_3_13t": _py_version(os.path.expanduser("~/.pyenv/versions/3.13.13t/bin/python3")),
        "python_gil_3_13": _py_version(os.path.expanduser("~/.pyenv/versions/3.13.13/bin/python3")),
        "go_version": _run(["go", "version"]),
        "cython_version": _run([os.path.expanduser("~/.pyenv/versions/3.13.13t/bin/python3"),
                                "-c", "import Cython;print(Cython.__version__)"]),
        "uvloop": _run([os.path.expanduser("~/.pyenv/versions/3.13.13/bin/python3"),
                        "-c", "import uvloop;print(uvloop.__version__)"]) or "missing",
        "gevent": _run([os.path.expanduser("~/.pyenv/versions/3.13.13/bin/python3"),
                        "-c", "import gevent;print(gevent.__version__)"]) or "missing",
        "greenlet": _run([os.path.expanduser("~/.pyenv/versions/3.13.13t/bin/python3"),
                          "-c", "import greenlet;print(greenlet.__version__)"]) or "missing",
        "runloom_git_sha": _git_sha(repo),
        "runloom_build": _runloom_build_flags(repo),
        "shell_fd_soft_limit": _run(["bash", "-lc", "ulimit -n"]),
    }
    return info


def header_lines(info):
    n = info
    numa = ", ".join("%s=%s" % (k, v) for k, v in (n.get("numa_nodes") or {}).items())
    return [
        "Host:    %s (%s, %s)" % (n["hostname"], n["virtualization"], n["os"]),
        "Kernel:  %s %s" % (n["kernel"], n["arch"]),
        "CPU:     %s -- %s logical vCPUs; NUMA: %s; governor=%s; steal=%s%%" % (
            n["cpu_model"], n["logical_cpus"], numa, n["cpu_governor"], n["steal_pct_sample"]),
        "Memory:  %s GiB" % n["mem_total_gib"],
        "Runloom: %s @ %s, build=%s" % (
            n["python_ft_3_13t"], n["runloom_git_sha"], n["runloom_build"]["expected_cflags"]),
        "Baselines: %s (GIL), uvloop=%s, gevent=%s, greenlet=%s; %s" % (
            n["python_gil_3_13"], n["uvloop"], n["gevent"], n["greenlet"], n["go_version"]),
    ]


if __name__ == "__main__":
    info = capture()
    print("\n".join(header_lines(info)))
    print("\n--- full JSON ---")
    print(json.dumps(info, indent=2))
