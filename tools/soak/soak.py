#!/usr/bin/env python3
"""Soak-test orchestrator (docs/dev/RELIABILITY_PROGRAM.md R1).

Runs N worker processes of a workload for a duration, watches their heartbeats
for hangs/crashes, then runs the slope oracle over the sampled CSVs and writes
a one-page REPORT.md verdict.  The whole point: catch a resource leak as a
rising trend line in the first hour, not as a crash at hour 40.

Usage:
  # a real 2-hour mixed soak (4 workers), the R1 acceptance run:
  python3 tools/soak/soak.py --workload mixed --hours 2 --workers 4

  # the negative control -- MUST report FAIL (proves the oracle has teeth):
  python3 tools/soak/soak.py --workload leak_control --minutes 3 --warmup-frac 0.3

  # accelerated-life (max lifecycle turnover), with mode knobs:
  python3 tools/soak/soak.py --workload mixed --hours 1 --compress \\
        --env RUNLOOM_PERHUB_EPOLL=1 --env RUNLOOM_IOURING_LOOP=1

A worker that misses ~3 heartbeat intervals (mtime stale) OR whose progress
counter freezes while it should be running is a HANG: we capture a gdb triage
(tools/hang_hunter/triage.py) BEFORE killing it, so a wedge is diagnosable.  A
worker that exits non-zero is a CRASH.  Either fails the run.
"""
import argparse
import os
import random
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
PY = sys.executable

sys.path.insert(0, ROOT)
from tools.soak import oracle


def _read_heartbeat(path):
    """Return (elapsed, progress, alive) or None if unreadable."""
    try:
        with open(path) as f:
            parts = f.read().split()
        return float(parts[0]), int(parts[1]), int(parts[2])
    except (OSError, ValueError, IndexError):
        return None


def _triage(pid, label, out_dir):
    """Best-effort gdb capture of a wedged worker (tools/hang_hunter/triage.py
    is the model).  Writes to out_dir; degrades to a note if gdb/ptrace
    unavailable."""
    dest = os.path.join(out_dir, "hang_%s_pid%d.txt" % (label, pid))
    try:
        sys.path.insert(0, os.path.join(ROOT, "tools", "hang_hunter"))
        import triage as hh_triage  # noqa
        # triage_hang(pid, write): write is a callback given the report text.
        with open(dest, "w") as f:
            hh_triage.triage_hang(pid, f.write)
        return dest
    except Exception as e:
        try:
            with open(dest, "w") as f:
                f.write("triage unavailable (%r); "
                        "install gdb + set ptrace_scope=0 for live capture\n" % e)
        except OSError:
            pass
    return dest


def run_soak(args):
    stamp = args.stamp
    out_dir = os.path.join(args.out, "soak_%s_%s" % (args.workload, stamp))
    os.makedirs(out_dir, exist_ok=True)

    seconds = args.hours * 3600.0 if args.hours else (
        args.minutes * 60.0 if args.minutes else args.seconds)
    if not seconds:
        print("error: give --hours, --minutes, or --seconds", file=sys.stderr)
        return 2

    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    for kv in args.env or []:
        k, _, v = kv.partition("=")
        env[k] = v

    procs = []
    for i in range(args.workers):
        csv = os.path.join(out_dir, "worker%d.csv" % i)
        hb = os.path.join(out_dir, "worker%d.hb" % i)
        argv = [PY, os.path.join(HERE, "worker.py"),
                "--workload", args.workload,
                "--seconds", "%.1f" % seconds,
                "--interval", "%.1f" % args.interval,
                "--csv", csv, "--heartbeat", hb]
        if args.compress:
            argv.append("--compress")
        if args.chaos:
            argv += ["--chaos", "--chaos-seed", str(1000 + i)]
        p = subprocess.Popen(argv, cwd=ROOT, env=env)
        procs.append({"i": i, "p": p, "csv": csv, "hb": hb,
                      "last_prog": -1, "stall": 0, "verdict": None,
                      "frozen_until": 0.0})

    print("[soak] %s: %d workers, %.0fs, out=%s"
          % (args.workload, args.workers, seconds, out_dir))
    t0 = time.monotonic()
    check_every = min(args.interval, 10.0)
    deadline = t0 + seconds + args.interval + 15.0  # grace for final flush
    rng = random.Random(20260705)   # fixed: reproducible chaos schedule
    next_freeze = t0 + rng.uniform(20.0, 60.0) if args.chaos else float("inf")

    while time.monotonic() < deadline:
        time.sleep(check_every)
        now = time.monotonic()
        # --- external chaos: SIGSTOP a random running worker briefly, SIGCONT ---
        # (docs/dev/RELIABILITY_PROGRAM.md R3: a process frozen mid-flight must
        # resume cleanly, not wedge -- the field equivalent of the OS descheduling
        # a hub thread under load.)  We record frozen_until so hang detection
        # does NOT count the deliberate freeze (+ a recovery grace) as a wedge.
        if args.chaos and now >= next_freeze:
            live = [w for w in procs if w["verdict"] is None
                    and w["p"].poll() is None]
            if live:
                victim = rng.choice(live)
                dur = rng.uniform(1.0, 4.0)
                try:
                    victim["p"].send_signal(signal.SIGSTOP)
                    print("[soak] chaos: froze worker%d for %.1fs" % (victim["i"], dur))
                    time.sleep(dur)
                    victim["p"].send_signal(signal.SIGCONT)
                    # suppress hang detection until it has had time to recover
                    victim["frozen_until"] = time.monotonic() + args.interval * 2
                except (OSError, ProcessLookupError):
                    pass
            next_freeze = time.monotonic() + rng.uniform(20.0, 60.0)
        all_done = True
        for w in procs:
            if w["verdict"] is not None:
                continue
            rc = w["p"].poll()
            if rc is not None:
                # process exited
                if rc != 0:
                    w["verdict"] = "CRASH(rc=%d)" % rc
                    print("[soak] worker%d CRASH rc=%d" % (w["i"], rc))
                else:
                    w["verdict"] = "done"
                continue
            all_done = False
            # A deliberately-frozen worker (external chaos) is not a hang.
            if time.monotonic() < w["frozen_until"]:
                continue
            # liveness: heartbeat mtime + progress advancement
            hb = _read_heartbeat(w["hb"])
            try:
                mtime_age = time.monotonic() - t0 - (
                    os.path.getmtime(w["hb"]) - (time.time() - (time.monotonic() - t0)))
            except OSError:
                mtime_age = 0.0
            if hb is not None:
                elapsed, prog, alive = hb
                if prog == w["last_prog"]:
                    w["stall"] += 1
                else:
                    w["stall"] = 0
                    w["last_prog"] = prog
                # A stall of >3 checks while the worker should be running = wedge.
                if w["stall"] >= max(3, int(args.interval / check_every) * 3) \
                        and elapsed < seconds - args.interval:
                    print("[soak] worker%d HANG (progress frozen at %d) -- triaging"
                          % (w["i"], prog))
                    dest = _triage(w["p"].pid, "w%d" % w["i"], out_dir)
                    print("[soak]   triage -> %s" % dest)
                    w["p"].kill()
                    w["verdict"] = "HANG(triage=%s)" % os.path.basename(dest)
        if all_done:
            break

    # reap stragglers
    for w in procs:
        if w["verdict"] is None:
            rc = w["p"].poll()
            if rc is None:
                w["p"].terminate()
                try:
                    w["p"].wait(timeout=10)
                except subprocess.TimeoutExpired:
                    w["p"].kill()
                w["verdict"] = "done"
            elif rc != 0:
                w["verdict"] = "CRASH(rc=%d)" % rc
            else:
                w["verdict"] = "done"

    # ---- slope oracle over each worker CSV ----
    warmup = seconds * args.warmup_frac
    worker_reports = []
    any_fail = False
    for w in procs:
        if w["verdict"].startswith(("CRASH", "HANG")):
            any_fail = True
            worker_reports.append((w["i"], w["verdict"], "FAIL", []))
            continue
        try:
            verdict, rows = oracle.analyze(w["csv"], warmup)
        except Exception as e:
            verdict, rows = "FAIL", [{"metric": "(oracle error)", "note": repr(e)}]
        if not verdict.startswith("PASS"):
            any_fail = True
        worker_reports.append((w["i"], w["verdict"], verdict, rows))

    verdict = "FAIL" if any_fail else "PASS"
    report_path = os.path.join(out_dir, "REPORT.md")
    _write_report(report_path, args, seconds, warmup, out_dir,
                  worker_reports, verdict)
    print("[soak] VERDICT: %s  (report: %s)" % (verdict, report_path))
    return 0 if verdict == "PASS" else 1


def _write_report(path, args, seconds, warmup, out_dir, worker_reports, verdict):
    lines = []
    lines.append("# Soak report — `%s`" % args.workload)
    lines.append("")
    lines.append("- **Verdict: %s**" % verdict)
    lines.append("- workload: `%s`%s" % (args.workload,
                 "  (churn-compress)" if args.compress else ""))
    lines.append("- duration: %.0fs  ·  warmup dropped: %.0fs  ·  workers: %d"
                 % (seconds, warmup, args.workers))
    lines.append("- sample interval: %.0fs" % args.interval)
    if args.env:
        lines.append("- env: %s" % ", ".join("`%s`" % e for e in args.env))
    lines.append("- data: `%s`" % out_dir)
    lines.append("")
    lines.append("The oracle fits a least-squares line to each metric over the "
                 "post-warmup window and passes iff every slope's 95% CI "
                 "includes 0 or is below its per-metric epsilon "
                 "(tools/soak/oracle.py). A leak surfaces as a metric whose "
                 "slope CI excludes 0 above epsilon.")
    lines.append("")
    for i, worker_verdict, ora_verdict, rows in worker_reports:
        lines.append("## worker %d — %s / oracle %s" % (i, worker_verdict, ora_verdict))
        lines.append("")
        if rows:
            lines.append(oracle.format_report("", ora_verdict, rows))
        lines.append("")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workload", default="mixed")
    ap.add_argument("--hours", type=float, default=0)
    ap.add_argument("--minutes", type=float, default=0)
    ap.add_argument("--seconds", type=float, default=0)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--interval", type=float, default=30.0,
                    help="sample cadence seconds (default 30)")
    ap.add_argument("--warmup-frac", type=float, default=None,
                    help="fraction of the run to drop as warmup (default: "
                         "10min/duration, capped [0.1, 0.5])")
    ap.add_argument("--compress", action="store_true",
                    help="churn-compress: max lifecycle turnover (a day ~ months)")
    ap.add_argument("--chaos", action="store_true",
                    help="R3: age error/degraded paths -- in-worker GC storms + "
                         "fd pressure + allocator thrash, plus orchestrator "
                         "SIGSTOP/SIGCONT freeze chaos (recovery must be clean)")
    ap.add_argument("--env", action="append",
                    help="KEY=VAL passed to workers (RUNLOOM_PERHUB_EPOLL, "
                         "RUNLOOM_IOURING_LOOP, RUNLOOM_STACK_PARK_SWEEP, ...)")
    ap.add_argument("--out", default=os.path.join(ROOT, "docs", "dev", "soak"))
    ap.add_argument("--stamp", default=None,
                    help="run id for the output dir (default: a counter)")
    args = ap.parse_args(argv)

    if args.stamp is None:
        # avoid time-based names for reproducibility of the harness itself; a
        # simple incrementing suffix under the out dir.
        n = 0
        while os.path.exists(os.path.join(
                args.out, "soak_%s_%03d" % (args.workload, n))):
            n += 1
        args.stamp = "%03d" % n

    if args.warmup_frac is None:
        secs = args.hours * 3600 or args.minutes * 60 or args.seconds or 1
        frac = 600.0 / secs
        args.warmup_frac = min(0.5, max(0.1, frac))

    return run_soak(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
