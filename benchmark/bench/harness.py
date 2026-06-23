"""bench.harness -- reproducible micro / throughput benchmark harness for runloom.

Why a dedicated harness instead of the existing bench/bench_*.py scripts:
those print a single wall-clock number from one run.  That is fine as a
sanity check but unusable as a *measurement* -- no warmup, no repetition,
no dispersion, no environment record -- so two numbers taken on two days
cannot be compared, and a regression is invisible.  This module fixes that:

    * captures the full environment (CPU, NUMA, ASLR, governor, Python
      build, runloom backend, git sha, compile flags) into every result file,
    * pins the process to a fixed CPU set on one NUMA node to cut scheduler
      noise on this shared, virtualized box,
    * runs ``warmup`` discarded iterations then ``samples`` measured ones,
    * reports median + MAD + min(best) + %rsd + a bootstrap 95% CI of the
      median rather than a lone mean,
    * writes machine-readable JSON so runs are diffable and a regression
      gate can compare against a committed baseline.

Primary target runtime is free-threaded CPython 3.13t: runloom's M:N hub pool
only gets real core-level parallelism with the GIL off.  Everything here is
stdlib-only so it runs under 3.13t with no extra wheels to build.

Run a suite with, e.g.::

    PYTHONPATH=src ~/.pyenv/versions/3.13.13t/bin/python -m bench.micro
"""
import json
import os
import platform
import random
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone

from bench.gil import assert_nogil


# --------------------------------------------------------------------
# Repo / import bootstrap.  Make "import runloom_c" work regardless of
# the caller's PYTHONPATH by putting the worktree src/ first.
# --------------------------------------------------------------------
HARNESS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HARNESS_DIR)
SRC_DIR = os.path.join(REPO_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)
RESULTS_DIR = os.path.join(HARNESS_DIR, "results")


def cmd_output(cmd):
    """Best-effort capture of a shell command's stdout; '' on any failure."""
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL)
        return out.decode("utf-8", "replace").strip()
    except Exception:
        return ""


def read_text(path):
    try:
        with open(path) as f:
            return f.read().strip()
    except Exception:
        return ""


def git_sha():
    return cmd_output(["git", "-C", REPO_ROOT, "rev-parse", "--short", "HEAD"])


def git_dirty():
    return bool(cmd_output(["git", "-C", REPO_ROOT, "status", "--porcelain"]))


# --------------------------------------------------------------------
# CPU affinity.  This box is a 64-vCPU 2-NUMA-node VM shared with a
# desktop session, so unpinned runs wander across nodes and pick up
# cross-NUMA latency + desktop preemption.  Pin to a contiguous set on
# ONE node (default: node1, cpus 32-63, away from the OS/desktop which
# tends to land on the low cpus).
# --------------------------------------------------------------------
def numa_cpulist(node):
    txt = read_text("/sys/devices/system/node/node%d/cpulist" % node)
    cpus = []
    for part in txt.split(","):
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-")
            cpus.extend(range(int(a), int(b) + 1))
        else:
            cpus.append(int(part))
    return cpus


def default_pin_set(n=8, node=1):
    """First ``n`` cpus of NUMA ``node`` (fall back to node0 then to whatever
    affinity we already have)."""
    for nd in (node, 0):
        cpus = numa_cpulist(nd)
        if cpus:
            return cpus[:n]
    return sorted(os.sched_getaffinity(0))[:n]


def pin(cpus):
    """Pin this process (and children that inherit affinity) to ``cpus``.
    Returns the set actually applied, or None if unsupported."""
    if not hasattr(os, "sched_setaffinity"):
        return None
    try:
        os.sched_setaffinity(0, set(cpus))
        return sorted(os.sched_getaffinity(0))
    except OSError:
        return None


# --------------------------------------------------------------------
# Environment capture -- recorded into every result file so a number is
# never orphaned from the machine state that produced it.
# --------------------------------------------------------------------
def capture_env(pinned=None):
    import sysconfig

    try:
        import runloom_c
        backend = runloom_c.backend()
        netpoll = runloom_c.netpoll_backend()
        so = getattr(runloom_c, "__file__", "")
    except Exception as e:  # pragma: no cover - import is the whole point
        backend = netpoll = so = "import-failed: %r" % (e,)

    gil = None
    if hasattr(sys, "_is_gil_enabled"):
        gil = sys._is_gil_enabled()

    # cpufreq governor is absent on this VM; record whatever is (or isn't)
    # there so a future run on bare metal is comparable.
    gov = read_text("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor") or "n/a (virtualized)"

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "host": platform.node(),
        "kernel": platform.release(),
        "cpu_model": cpu_model_str(),
        "nproc": os.cpu_count(),
        "numa_nodes": numa_node_count(),
        "governor": gov,
        "aslr": read_text("/proc/sys/kernel/randomize_va_space") or "?",
        "perf_event_paranoid": read_text("/proc/sys/kernel/perf_event_paranoid") or "?",
        "loadavg": read_text("/proc/loadavg").split(" ")[:3],
        "python": platform.python_version(),
        "python_impl": platform.python_implementation(),
        "gil_enabled": gil,
        "python_build": " ".join(platform.python_build()),
        "py_cflags": (sysconfig.get_config_var("CFLAGS") or "")[:200],
        "runloom_backend": backend,
        "runloom_netpoll": netpoll,
        "runloom_so": so,
        "git_sha": git_sha(),
        "git_dirty": git_dirty(),
        "pinned_cpus": pinned,
    }


def cpu_model_str():
    for line in read_text("/proc/cpuinfo").splitlines():
        if line.startswith("model name"):
            return line.split(":", 1)[1].strip()
    return platform.processor() or "unknown"


def numa_node_count():
    try:
        return len([d for d in os.listdir("/sys/devices/system/node")
                    if d.startswith("node") and d[4:].isdigit()])
    except Exception:
        return 1


# --------------------------------------------------------------------
# Statistics.  Median + MAD + min are robust to the occasional desktop
# preemption spike that a mean would smear; the bootstrap CI gives an
# honest uncertainty band on the median without assuming normality.
# --------------------------------------------------------------------
def bootstrap_median_ci(samples, iters=2000, conf=0.95, seed=12345):
    if len(samples) < 3:
        return (min(samples), max(samples))
    rng = random.Random(seed)
    n = len(samples)
    meds = []
    for _ in range(iters):
        resample = [samples[rng.randrange(n)] for _ in range(n)]
        meds.append(statistics.median(resample))
    meds.sort()
    lo = meds[int((1 - conf) / 2 * iters)]
    hi = meds[int((1 + conf) / 2 * iters) - 1]
    return (lo, hi)


def summarize(times, inner):
    """times: list of per-sample wall seconds, each covering ``inner`` ops."""
    median = statistics.median(times)
    mean = statistics.fmean(times)
    mad = statistics.median([abs(t - median) for t in times])
    stdev = statistics.pstdev(times) if len(times) > 1 else 0.0
    lo, hi = bootstrap_median_ci(times)
    per_op = median / inner
    return {
        "samples": len(times),
        "inner": inner,
        "median_s": median,
        "min_s": min(times),
        "mean_s": mean,
        "mad_s": mad,
        "stdev_s": stdev,
        "rsd_pct": (stdev / mean * 100.0) if mean else 0.0,
        "ci95_lo_s": lo,
        "ci95_hi_s": hi,
        "per_op_s": per_op,
        "per_op_ns": per_op * 1e9,
        "ops_per_s": (inner / median) if median else float("inf"),
    }


# --------------------------------------------------------------------
# Runner
# --------------------------------------------------------------------
class Suite:
    """Collects benchmark results, captures env once, writes JSON + a table."""

    def __init__(self, name, *, pin_cpus=None, samples=15, warmup=3):
        self.name = name
        self.samples = samples
        self.warmup = warmup
        # Tripwire: never let a GIL-on run masquerade as free-threaded.
        assert_nogil("Suite(%r) construction" % name)
        cpus = pin_cpus if pin_cpus is not None else default_pin_set()
        self.pinned = pin(cpus)
        self.env = capture_env(self.pinned)
        self.results = []

    def bench(self, name, fn, *, inner=1, samples=None, warmup=None, note="",
              setup=None, teardown=None):
        """Time ``fn`` (which performs ``inner`` units of work per call).

        fn is called warmup+samples times; the first ``warmup`` are discarded.
        Use ``inner`` for cheap ops so each sample is long enough to dominate
        timer granularity (perf_counter_ns is ~10-40ns here).

        ``setup`` / ``teardown`` run once, untimed, around the whole sample
        loop -- use them to spin up persistent state (e.g. an M:N hub pool)
        whose construction cost must NOT be folded into the per-op number.
        """
        import gc
        # Re-check before every bench: a workload's own imports could have
        # flipped the GIL on after the Suite was built.
        assert_nogil("bench(%r)" % name)
        s = self.samples if samples is None else samples
        w = self.warmup if warmup is None else warmup
        if setup is not None:
            setup()
        try:
            for _ in range(w):
                fn()
            times = []
            for _ in range(s):
                # Collect cyclic garbage from the previous sample untimed,
                # then freeze the collector so a stray collection can't land
                # inside this timing window and inflate its dispersion.
                gc.collect()
                gc_was = gc.isenabled()
                gc.disable()
                try:
                    t0 = time.perf_counter_ns()
                    fn()
                    dt = time.perf_counter_ns() - t0
                finally:
                    if gc_was:
                        gc.enable()
                times.append(dt / 1e9)
        finally:
            if teardown is not None:
                teardown()
        stats = summarize(times, inner)
        stats["name"] = name
        stats["note"] = note
        stats["raw_s"] = times
        self.results.append(stats)
        self.print_row(stats)
        return stats

    def print_row(self, s):
        print("  %-34s %10.1f ops/s  %10.1f ns/op  med=%8.3fms  rsd=%4.1f%%"
              % (s["name"], s["ops_per_s"], s["per_op_ns"],
                 s["median_s"] * 1e3, s["rsd_pct"]))

    def write(self, path=None):
        os.makedirs(RESULTS_DIR, exist_ok=True)
        if path is None:
            path = os.path.join(RESULTS_DIR, "%s.json" % self.name)
        # Drop raw samples from the headline file to keep diffs small; keep
        # the summary stats which are what the regression gate compares.
        doc = {
            "suite": self.name,
            "env": self.env,
            "results": [{k: v for k, v in r.items() if k != "raw_s"}
                        for r in self.results],
        }
        with open(path, "w") as f:
            json.dump(doc, f, indent=2, sort_keys=True)
            f.write("\n")
        print("\nwrote %s" % path)
        return path

    def banner(self):
        e = self.env
        print("=" * 78)
        print("suite: %s" % self.name)
        print("python %s %s  gil=%s  | runloom %s/%s @ %s%s"
              % (e["python"], e["python_impl"], e["gil_enabled"],
                 e["runloom_backend"], e["runloom_netpoll"], e["git_sha"],
                 "-dirty" if e["git_dirty"] else ""))
        print("cpu: %s  | %d vCPU / %s NUMA  | pinned=%s  aslr=%s  load=%s"
              % (e["cpu_model"], e["nproc"], e["numa_nodes"],
                 fmt_cpus(e["pinned_cpus"]), e["aslr"],
                 "/".join(e["loadavg"])))
        print("=" * 78)


def fmt_cpus(cpus):
    if not cpus:
        return "none"
    return "%d cpus [%d..%d]" % (len(cpus), cpus[0], cpus[-1])


__all__ = ["Suite", "summarize", "capture_env", "pin", "default_pin_set",
           "numa_cpulist", "git_sha", "REPO_ROOT", "SRC_DIR", "RESULTS_DIR"]
