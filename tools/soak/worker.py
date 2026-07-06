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
        self.chaos_events = 0
        self.done = False

    def expired(self):
        return _time.monotonic() >= self.deadline

    def bump(self):
        # A plain += is fine here: one workload fiber at a time on the single-
        # thread scheduler; under M:N the sampler only reads it (a lost
        # increment merely under-reports progress, never a false wedge).
        self.progress += 1


def _chaos_thread(ctx, seed):
    """R3 in-process chaos (docs/dev/RELIABILITY_PROGRAM.md R3): error paths age
    too.  A daemon thread that periodically inflicts a random disruption so the
    runtime's cleanup / degraded paths get the same cycle counts the happy path
    gets.  Runs on a foreign OS thread (not a fiber) so it disrupts the
    scheduler from outside, like a real signal / GC pause / fd-pressure event.

    Events (all self-inflicted, no root needed): GC storms (stop-the-world
    bursts under churn), fd pressure (open many pipes toward EMFILE then release
    -- exercises the ENOMEM/EMFILE degraded paths), and a brief allocator
    thrash.  External chaos (SIGSTOP/SIGCONT a worker, tc netem loss) is driven
    by the orchestrator / netns wrapper, not here.
    """
    import gc
    import random
    rng = random.Random(seed)
    while not ctx.done and not ctx.expired():
        _raw_sleep(rng.uniform(3.0, 12.0))
        ctx.chaos_events += 1
        pick = rng.random()
        if pick < 0.4:
            # GC storm: a burst of stop-the-world collections under live churn.
            for _ in range(rng.randint(10, 40)):
                gc.collect()
        elif pick < 0.75:
            # fd pressure: open pipes toward the soft limit, hold briefly, close.
            pipes = []
            try:
                for _ in range(rng.randint(64, 512)):
                    pipes.append(os.pipe())
            except OSError:
                pass  # EMFILE reached -> that IS the degraded path we want aged
            finally:
                for r, w in pipes:
                    try:
                        os.close(r); os.close(w)
                    except OSError:
                        pass
        else:
            # allocator thrash: churn large transient buffers.
            junk = [bytearray(rng.randint(4096, 65536)) for _ in range(200)]
            del junk


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


# Liveness auditor (item 5) -- wired in so soak actually CONSUMES it instead of
# it being dead code.  Best-effort: a soak must not die because introspection
# hiccuped.
try:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "introspect"))
    import liveness as _liveness
except Exception:
    _liveness = None


def _liveness_gauges(blame_path):
    """Return {parked_max_age, hard_deadlock} for the CSV, and dump blame on a
    hard-deadlock verdict.  parked_max_age is the max age (s) among parked
    fibers -- a monotone DWELL gauge: a stranded fiber's age climbs without
    bound, so oracle.py's slope detector catches a strand even when the
    population stays flat (the rare/transient lost-wake signature)."""
    if _liveness is None:
        return {}
    try:
        snap = _liveness.snapshot()
    except Exception:
        return {}
    parked = [f for f in snap["fibers"] if f["state"] not in _liveness.RUNNABLE]
    ages = [f.get("age") for f in parked if f.get("age") is not None]
    gauges = {"parked_max_age": round(max(ages), 2) if ages else 0.0,
              "hard_deadlock": 0}
    try:
        blame = _liveness.deadlock_blame(snap)
    except Exception:
        blame = None
    if blame is not None:
        gauges["hard_deadlock"] = 1
        try:
            with open(blame_path, "a") as bf:
                bf.write(_liveness.format_blame(snap, blame) + "\n---\n")
        except OSError:
            pass
    return gauges


try:
    runloom_c.set_introspect_timestamps(True)  # populate fiber ages for the dwell gauge
except Exception:
    pass


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
            row.update(_liveness_gauges(csv_path + ".blame"))
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
    ap.add_argument("--chaos", action="store_true",
                    help="R3: run the in-process chaos thread (GC storms, fd "
                         "pressure, allocator thrash) to age error/degraded paths")
    ap.add_argument("--chaos-seed", type=int, default=1234)
    args = ap.parse_args(argv)

    t0 = _time.monotonic()
    ctx = Ctx(deadline=t0 + args.seconds, compress=args.compress)
    sampler = threading.Thread(
        target=_sampler,
        args=(ctx, args.csv, args.heartbeat, args.interval, t0),
        daemon=True)
    sampler.start()
    if args.chaos:
        threading.Thread(target=_chaos_thread, args=(ctx, args.chaos_seed),
                         daemon=True).start()
    try:
        WORKLOADS[args.workload](ctx)
    finally:
        ctx.done = True
        sampler.join(timeout=args.interval + 5)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
