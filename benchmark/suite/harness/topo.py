"""System setup for the network benchmarks: the veth-pair / two-netns topology,
disjoint CPU pinning, fd-limit raising, and namespaced sysctls (decisions #3,
#7, #8 and the spec's system-wide requirements).

Why a veth pair across two netns instead of loopback:
  * On this Docker host every loopback packet traverses the host nft ruleset
    (~14% throughput tax) -- a fresh netns has an empty ruleset.
  * Client and server in *separate* netns over a veth pair cross a real device
    queue, so io_uring is not hidden behind the loopback fast path, and the
    loadgen and server cannot contend on the same `lo`.

Everything here shells out through `sudo -n` (passwordless sudo confirmed on the
box).  Commands are returned as argv lists for subprocess; env vars are passed
explicitly via an `env` prefix because `sudo` strips the environment.
"""
import os
import shutil
import subprocess
import sys

from config import (SRV_NS, CLI_NS, VETH_SRV, VETH_CLI, SRV_IP, CLI_IP, PREFIX,
                    NS_SYSCTLS, FD_LIMIT, SRC, FT_PYTHON)


def _sudo(*argv, check=True, quiet=False):
    cmd = ["sudo", "-n", *argv]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0 and check and not quiet:
        sys.stderr.write("CMD FAILED: %s\n%s\n" % (" ".join(cmd), r.stderr))
    return r


def ensure_kernel_ceilings():
    """Raise the global kernel ceilings that bite at millions of fds/fibers.
    These persist until reboot; safe to re-apply."""
    for key, val in {
        "fs.nr_open": str(FD_LIMIT),
        "vm.max_map_count": "2000000",      # ~2 VMAs per goroutine stack
        "net.core.somaxconn": "65535",
    }.items():
        _sudo("sysctl", "-w", "%s=%s" % (key, val), quiet=True)


def teardown():
    """Delete both netns (removes the veth pair with them).  Idempotent."""
    for ns in (SRV_NS, CLI_NS):
        _sudo("ip", "netns", "del", ns, check=False, quiet=True)
    # A stray veth (e.g. a half-built setup) lingers in the root ns; clean it.
    _sudo("ip", "link", "del", VETH_SRV, check=False, quiet=True)


def setup():
    """(Re)create the two-netns veth topology + namespaced sysctls.  Idempotent:
    tears down any stale instance first."""
    teardown()
    ensure_kernel_ceilings()
    # netns
    _sudo("ip", "netns", "add", SRV_NS)
    _sudo("ip", "netns", "add", CLI_NS)
    # veth pair, one end into each ns
    _sudo("ip", "link", "add", VETH_SRV, "type", "veth", "peer", "name", VETH_CLI)
    _sudo("ip", "link", "set", VETH_SRV, "netns", SRV_NS)
    _sudo("ip", "link", "set", VETH_CLI, "netns", CLI_NS)
    # addresses + up
    _sudo("ip", "netns", "exec", SRV_NS, "ip", "addr", "add",
          "%s/%d" % (SRV_IP, PREFIX), "dev", VETH_SRV)
    _sudo("ip", "netns", "exec", CLI_NS, "ip", "addr", "add",
          "%s/%d" % (CLI_IP, PREFIX), "dev", VETH_CLI)
    for ns, dev in ((SRV_NS, VETH_SRV), (CLI_NS, VETH_CLI)):
        _sudo("ip", "netns", "exec", ns, "ip", "link", "set", dev, "up")
        _sudo("ip", "netns", "exec", ns, "ip", "link", "set", "lo", "up")
    # spec sysctls -- namespaced, so set them INSIDE the server ns
    for key, val in NS_SYSCTLS.items():
        _sudo("ip", "netns", "exec", SRV_NS, "sysctl", "-w", "%s=%s" % (key, val), quiet=True)
    # sanity: client can reach server
    r = _sudo("ip", "netns", "exec", CLI_NS, "ping", "-c", "1", "-W", "1", SRV_IP, check=False)
    if r.returncode != 0:
        raise RuntimeError("veth topology setup failed: client cannot ping server\n" + r.stderr)
    return {"srv_ns": SRV_NS, "cli_ns": CLI_NS, "srv_ip": SRV_IP, "cli_ip": CLI_IP}


def _env_prefix(extra_env=None, gil_off=True):
    """Explicit env passed through sudo's environment scrub."""
    env = {
        "PYTHONPATH": SRC,
        "PYTHON_GIL": "0" if gil_off else "1",
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": os.environ.get("HOME", "/root"),
    }
    if extra_env:
        env.update(extra_env)
    return ["env"] + ["%s=%s" % (k, v) for k, v in env.items()]


def ns_cmd(ns, argv, cpus=None, extra_env=None, gil_off=True, raise_fd=True):
    """Build an argv list that runs `argv` inside netns `ns`, pinned to `cpus`
    (a 'c0,c1,...' taskset spec), with RLIMIT_NOFILE raised and env injected."""
    cmd = ["sudo", "-n", "ip", "netns", "exec", ns]
    if raise_fd:
        cmd += ["prlimit", "--nofile=%d:%d" % (FD_LIMIT, FD_LIMIT)]
    if cpus:
        cmd += ["taskset", "-c", cpus]
    cmd += _env_prefix(extra_env, gil_off)
    cmd += list(argv)
    return cmd


def pinned_cmd(argv, cpus=None, extra_env=None, gil_off=True, raise_fd=False):
    """For benchmarks that don't need the network (spawn/ctxswitch/memory):
    just pin + env (+ optional fd raise via sudo prlimit, root)."""
    cmd = []
    if raise_fd:
        cmd += ["sudo", "-n", "prlimit", "--nofile=%d:%d" % (FD_LIMIT, FD_LIMIT)]
    if cpus:
        cmd += ["taskset", "-c", cpus]
    if raise_fd:
        # sudo scrubs env -> inject explicitly
        cmd += _env_prefix(extra_env, gil_off)
    else:
        # no sudo: caller passes env= to Popen; still set the basics inline
        cmd += _env_prefix(extra_env, gil_off)
    cmd += list(argv)
    return cmd


if __name__ == "__main__":
    import json
    action = sys.argv[1] if len(sys.argv) > 1 else "setup"
    if action == "setup":
        print(json.dumps(setup(), indent=2))
    elif action == "teardown":
        teardown()
        print("torn down")
    elif action == "demo-cmd":
        print(" ".join(ns_cmd(SRV_NS, [FT_PYTHON, "-c", "print(1)"], cpus="32,33")))
