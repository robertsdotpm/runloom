"""One soak worker process (docs/dev/RELIABILITY_PROGRAM.md R1).

Runs ONE workload continuously for --seconds, self-sampling every --interval:
process metrics (RSS, VmSize, VMA count, open fds, threads) from /proc/self +
the full runloom.stats() gauge dict, appended to a CSV.  Each sample also
writes a heartbeat file "<elapsed> <progress> <alive>" so the orchestrator can
tell a live-but-slow worker from a wedged one (progress frozen) or a dead
sampler (heartbeat mtime stale).

The sampler is a plain OS thread, NOT a fiber: it must keep sampling even while
the scheduler is busy or wedged.  runloom.stats() is safe to call from it (the
C half is lock-free; the Python half is plain len()/qsize() on PEP703).  It
snapshots the workload's progress counter, so a scheduler wedge shows up as a
heartbeat whose progress stops advancing while its mtime keeps ticking.
"""
import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# Capture the RAW sleep before monkey.patch makes time.sleep cooperative: the
# sampler runs on a foreign thread and must block on the real OS clock.
import time as _time
_raw_sleep = _time.sleep

import runloom
import runloom.monkey
runloom.monkey.patch()
import runloom_c  # noqa: F401

from tools.soak.workloads import WORKLOADS


class Ctx(object):
    """Shared control surface between the workload fibers and the sampler."""
    def __init__(self, deadline, compress):
        self.deadline = deadline
        self.compress = compress
        self.progress = 0
        self.done = False

    def expired(self):
        return _time.monotonic() >= self.deadline

    def bump(self):
        # A plain += is fine here: one workload fiber at a time on the single-
        # thread scheduler; under M:N the sampler only reads it (a lost
        # increment merely under-reports progress, never a false wedge).
        self.progress += 1


def _proc_metrics():
    """RSS/VmSize/threads from /proc/self/status, VMA count from maps, open fd
    count from /proc/self/fd.  Zeroes on a platform without /proc."""
    m = {"rss_kb": 0, "vsz_kb": 0, "threads": 0, "vmas": 0, "fds": 0}
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    m["rss_kb"] = int(line.split()[1])
                elif line.startswith("VmSize:"):
                    m["vsz_kb"] = int(line.split()[1])
                elif line.startswith("Threads:"):
                    m["threads"] = int(line.split()[1])
    except OSError:
        pass
    try:
        with open("/proc/self/maps") as f:
            m["vmas"] = sum(1 for _ in f)
    except OSError:
        pass
    try:
        m["fds"] = len(os.listdir("/proc/self/fd"))
    except OSError:
        pass
    return m


def _sampler(ctx, csv_path, hb_path, interval, t0):
    """Sampler thread: append one CSV row + rewrite the heartbeat every
    interval, until ctx.done.  Header is written lazily on the first row so the
    stats() keys (which may vary by build) define the columns."""
    header_keys = None
    with open(csv_path, "w", buffering=1) as csv:
        while not ctx.done:
            elapsed = _time.monotonic() - t0
            prog = ctx.progress
            import gc
            gc.collect()   # separate a real leak from GC lag before sampling
            stats = {k: v for k, v in runloom.stats().items()
                     if isinstance(v, int)}
            proc = _proc_metrics()
            row = {"t": round(elapsed, 1), "progress": prog}
            row.update(proc)
            row.update(stats)
            if header_keys is None:
                header_keys = list(row.keys())
                csv.write(",".join(header_keys) + "\n")
            csv.write(",".join(str(row.get(k, "")) for k in header_keys) + "\n")
            try:
                with open(hb_path, "w") as hb:
                    hb.write("%.1f %d %d\n" % (elapsed, prog, 1))
            except OSError:
                pass
            # Sleep in short slices so ctx.done is observed promptly at shutdown.
            slept = 0.0
            while slept < interval and not ctx.done:
                _raw_sleep(min(0.5, interval - slept))
                slept += 0.5
    # final heartbeat: mark cleanly stopped
    try:
        with open(hb_path, "w") as hb:
            hb.write("%.1f %d %d\n" % (_time.monotonic() - t0, ctx.progress, 0))
    except OSError:
        pass


def main(argv):
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--workload", required=True, choices=sorted(WORKLOADS))
    ap.add_argument("--seconds", type=float, required=True)
    ap.add_argument("--interval", type=float, default=30.0)
    ap.add_argument("--csv", required=True)
    ap.add_argument("--heartbeat", required=True)
    ap.add_argument("--compress", action="store_true",
                    help="churn-compress: drop inter-unit yields for max turnover")
    args = ap.parse_args(argv)

    t0 = _time.monotonic()
    ctx = Ctx(deadline=t0 + args.seconds, compress=args.compress)
    sampler = threading.Thread(
        target=_sampler,
        args=(ctx, args.csv, args.heartbeat, args.interval, t0),
        daemon=True)
    sampler.start()
    try:
        WORKLOADS[args.workload](ctx)
    finally:
        ctx.done = True
        sampler.join(timeout=args.interval + 5)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
