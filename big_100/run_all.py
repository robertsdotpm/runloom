"""big_100 parallel orchestrator.

Runs the 100 stress projects concurrently as subprocesses so the whole 64-core
box is busy.  Each project itself uses --hubs M:N hub threads; the default
packs the machine as jobs * hubs ~= cores (16 jobs * 4 hubs = 64).

    PYTHON_GIL=0 python3.13t big_100/run_all.py --jobs 16 --hubs 4 --duration 3600
    big_100/run_all.py --only 1,3,7 --duration 30 --hubs 4
    big_100/run_all.py --from 1 --to 20 --duration 600 --jobs 10 --hubs 6

Per-project stderr/stdout go to big_100/logs/pNN.log.  A summary table prints
at the end; the orchestrator exits nonzero if any project failed.
"""
import argparse
import glob
import os
import re
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
LOGDIR = os.path.join(HERE, "logs")

DEFAULT_PY = os.path.expanduser("~/.pyenv/versions/3.13.13t/bin/python3")

VERDICT_RE = re.compile(r"VERDICT\s*:\s*(\w+)\s*\(exit\s*(\d+)\)")


def discover():
    """Return [(num, path)] for every pNN_*.py, sorted by number."""
    out = []
    for path in glob.glob(os.path.join(HERE, "p[0-9]*.py")):
        m = re.match(r"p(\d+)_", os.path.basename(path))
        if m:
            out.append((int(m.group(1)), path))
    out.sort()
    return out


def select(projects, args):
    if args.only:
        want = set(int(x) for x in args.only.replace(" ", "").split(",") if x)
        return [(n, p) for (n, p) in projects if n in want]
    lo = args.from_ if args.from_ is not None else 0
    hi = args.to if args.to is not None else 10 ** 9
    return [(n, p) for (n, p) in projects if lo <= n <= hi]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--jobs", type=int, default=8,
                    help="projects to run at once (jobs*hubs ~= cores). Note: "
                         "high --jobs reliably triggers BUG #4 (offload hang) "
                         "in the filesystem/subprocess/TLS projects -- that is "
                         "a real finding, not a runner defect")
    ap.add_argument("--hubs", type=int, default=4, help="M:N hubs per project")
    ap.add_argument("--duration", type=float, default=3600.0)
    ap.add_argument("--seed", type=int, default=1234)
    ap.add_argument("--funcs", type=int, default=None,
                    help="override per-project goroutine count")
    ap.add_argument("--handoff", action="store_true", default=False,
                    help="enable the buggy handoff rescue (reproduce BUG #2)")
    ap.add_argument("--only", default=None, help="comma list, e.g. 1,3,7")
    ap.add_argument("--from", dest="from_", type=int, default=None)
    ap.add_argument("--to", type=int, default=None)
    ap.add_argument("--python", default=DEFAULT_PY)
    ap.add_argument("--hang-timeout", type=float, default=120.0)
    ap.add_argument("--drain-timeout", type=float, default=120.0,
                    help="post-deadline drain cap passed to each program")
    ap.add_argument("--ip-slot-base", type=int, default=0,
                    help="first IP slot to assign; concurrent jobs get "
                         "slot-base, slot-base+1, ... so each uses a unique "
                         "127.(slot+1).0.0/24 subnet")
    args = ap.parse_args()

    projects = select(discover(), args)
    if not projects:
        sys.stderr.write("no projects matched\n")
        return 2
    os.makedirs(LOGDIR, exist_ok=True)

    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env.setdefault("RUNLOOM_SYSMON_QUIET", "1")

    # Track which IP slots are free.  We cycle through JOBS slots; when a job
    # finishes its slot is returned to the pool for the next pending job.
    free_slots = list(range(args.ip_slot_base,
                            args.ip_slot_base + args.jobs))

    def build_cmd(path, ip_slot):
        cmd = [args.python, path,
               "--duration", str(args.duration),
               "--seed", str(args.seed),
               "--hubs", str(args.hubs),
               "--hang-timeout", str(args.hang_timeout),
               "--drain-timeout", str(args.drain_timeout),
               "--ip-slot", str(ip_slot)]
        if args.funcs is not None:
            cmd += ["--funcs", str(args.funcs)]
        if args.handoff:
            cmd += ["--handoff"]
        return cmd

    sys.stderr.write(
        "big_100: {0} projects, {1} at a time, {2} hubs each, "
        "{3:.0f}s duration, ip-slot-base={4}\n".format(
            len(projects), args.jobs, args.hubs, args.duration,
            args.ip_slot_base))
    sys.stderr.flush()

    pending = list(projects)
    running = {}        # popen -> (num, path, logf, t0, ip_slot)
    results = {}        # num -> (verdict, exit_code, seconds)
    t_start = time.monotonic()

    def launch(num, path):
        ip_slot = free_slots.pop(0)
        logpath = os.path.join(LOGDIR, "p{0:02d}.log".format(num))
        logf = open(logpath, "wb")
        proc = subprocess.Popen(build_cmd(path, ip_slot), stdout=logf,
                                stderr=logf, env=env, cwd=HERE)
        running[proc] = (num, path, logf, time.monotonic(), ip_slot)
        sys.stderr.write("  launch p{0:02d} {1}\n".format(
            num, os.path.basename(path)))
        sys.stderr.flush()

    while pending or running:
        while pending and len(running) < args.jobs and free_slots:
            num, path = pending.pop(0)
            launch(num, path)
        time.sleep(0.5)
        for proc in list(running):
            rc = proc.poll()
            if rc is None:
                continue
            num, path, logf, t0, ip_slot = running.pop(proc)
            free_slots.append(ip_slot)
            logf.close()
            secs = time.monotonic() - t0
            verdict = classify(os.path.join(LOGDIR, "p{0:02d}.log".format(num)),
                               rc)
            results[num] = (verdict, rc, secs)
            sys.stderr.write("  done   p{0:02d} {1:<28} {2:>5} exit={3} {4:.0f}s\n"
                             .format(num, os.path.basename(path), verdict, rc,
                                     secs))
            sys.stderr.flush()

    return summarize(results, time.monotonic() - t_start)


def classify(logpath, rc):
    """Map an exit code + log tail to a short verdict."""
    if rc == 0:
        return "PASS"
    if rc == 3:
        return "HANG"
    if rc < 0 or rc >= 128:
        return "CRASH"          # killed by signal (SIGSEGV/SIGBUS/...)
    if rc == 1:
        return "INVAR"          # invariant violation
    if rc == 2:
        return "ERROR"
    return "FAIL"


def summarize(results, total_secs):
    sys.stderr.write("\n==== big_100 SUMMARY ====\n")
    npass = 0
    bad = []
    for num in sorted(results):
        verdict, rc, secs = results[num]
        sys.stderr.write("  p{0:02d}  {1:<6} exit={2:<4} {3:6.0f}s\n".format(
            num, verdict, rc, secs))
        if verdict == "PASS":
            npass += 1
        else:
            bad.append(num)
    sys.stderr.write("  ----\n")
    sys.stderr.write("  {0}/{1} PASS in {2:.0f}s wall\n".format(
        npass, len(results), total_secs))
    if bad:
        sys.stderr.write("  FAILED: {0}\n".format(
            ", ".join("p{0:02d}".format(n) for n in bad)))
    sys.stderr.flush()
    return 0 if not bad else 1


if __name__ == "__main__":
    sys.exit(main())
