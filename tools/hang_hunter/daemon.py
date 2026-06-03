"""hang-hunter: an autonomous, always-on stress+fuzz daemon for the runloom M:N
scheduler, with automatic triage and deduplication.

It keeps a pool of randomized runloom workloads running in parallel (sized to the
box, load-gated so it never fights the CI runner or foreground work).  When a job
HANGS (exceeds its timeout while still alive) it attaches gdb to the LIVE process
and captures the all-thread backtrace + the interpreter stop-the-world state +
each hub's queue snapshot; when a job CRASHES it captures the core's backtrace.
Each finding is keyed by a backtrace signature so thousands of repeats of one bug
collapse to a single report-with-count, and distinct bugs stand out.

Why this exists: our targeted tools (verify/ formal models, lincheck, dst, the
sanitizers) check specific primitives; they did not -- and structurally could not
-- catch the stop-the-world MONOPOLY deadlock, which only appears as an emergent
scheduling-fairness failure under realistic churn.  This daemon hunts exactly that
class continuously.

Usage:
  python -m tools.hang_hunter.daemon --once 200          # run ~200 jobs then exit
  python -m tools.hang_hunter.daemon --duration 3600     # hunt for an hour
  python -m tools.hang_hunter.daemon --daemon            # run until signalled
  (flags: --engines stress,hypo  --jobs N  --load-frac 0.7  --report-dir DIR
          --python /path/to/python3.13t)

Live attach needs ptrace: /proc/sys/kernel/yama/ptrace_scope = 0 (or run as root).
For crash backtraces, point core_pattern at the report dir's cores/ (the daemon
tries via `sudo -n`; otherwise see the printed hint).
"""
import argparse
import os
import random
import signal
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import triage          # noqa: E402
import workloads       # noqa: E402

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def default_python():
    for c in (os.path.expanduser("~/.pyenv/versions/3.13.13t/bin/python3"),
              "python3.13t", "python3"):
        if os.path.sep in c:
            if os.path.exists(c):
                return c
        else:
            try:
                return subprocess.check_output(["bash", "-lc", "command -v " + c],
                                               text=True).strip()
            except Exception:
                pass
    return sys.executable


def loadavg1():
    try:
        return os.getloadavg()[0]
    except OSError:
        return 0.0


class Hunter(object):
    def __init__(self, args):
        self.args = args
        self.py = args.python or default_python()
        self.engines = [e for e in args.engines.split(",") if e in workloads.ENGINES]
        if not self.engines:
            self.engines = list(workloads.ENGINES)
        self.rng = random.Random(args.seed)
        self.report_dir = os.path.abspath(args.report_dir)
        self.core_dir = os.path.join(self.report_dir, "cores")
        os.makedirs(self.core_dir, exist_ok=True)
        self.status_path = os.path.join(self.report_dir, "status.txt")
        self.dups_path = os.path.join(self.report_dir, "dups.log")
        self.ncpu = os.cpu_count() or 4
        self.jobs = args.jobs or max(2, int(self.ncpu * args.load_frac * 0.5))
        self.running = []          # list of dicts: proc, job, start
        self.sigs = {}             # signature -> {count, kind, repro, report}
        self.totals = {"launched": 0, "ok": 0, "hang": 0, "crash": 0}
        self.start = time.time()
        self.setup_cores()

    def setup_cores(self):
        self.core_pattern_ok = False
        pat = os.path.join(self.core_dir, "core.%p")
        try:
            cur = open("/proc/sys/kernel/core_pattern").read().strip()
        except OSError:
            cur = ""
        if cur == pat:
            self.core_pattern_ok = True
            return
        try:
            r = subprocess.run(["sudo", "-n", "sh", "-c",
                                "echo '{0}' > /proc/sys/kernel/core_pattern".format(pat)],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.core_pattern_ok = (r.returncode == 0)
        except Exception:
            self.core_pattern_ok = False
        if not self.core_pattern_ok:
            sys.stderr.write("[hang-hunter] note: could not set core_pattern; crash "
                             "backtraces need:\n  sudo sh -c 'echo {0} > "
                             "/proc/sys/kernel/core_pattern'\n".format(pat))

    def child_env(self, job):
        env = dict(os.environ)
        env["PYTHONPATH"] = os.path.join(REPO, "src")
        env.update(job.env)
        return env

    def launch(self):
        engine = self.rng.choice(self.engines)
        job = workloads.ENGINES[engine](self.rng, self.py)
        try:
            import resource
            def preexec():
                resource.setrlimit(resource.RLIMIT_CORE,
                                   (resource.RLIM_INFINITY, resource.RLIM_INFINITY))
                os.nice(10)
        except Exception:
            preexec = None
        p = subprocess.Popen(job.argv, cwd=REPO, env=self.child_env(job),
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                             preexec_fn=preexec)
        self.running.append({"proc": p, "job": job, "start": time.time()})
        self.totals["launched"] += 1

    def record(self, kind, sig, key, repro, report_text):
        if sig in self.sigs:
            self.sigs[sig]["count"] += 1
            with open(self.dups_path, "a") as fh:
                fh.write("{0} {1} {2} (n={3})\n".format(
                    time.strftime("%H:%M:%S"), kind, sig, self.sigs[sig]["count"]))
            return
        fname = "{0}_{1}_{2}.txt".format(sig, kind, time.strftime("%Y%m%d-%H%M%S"))
        path = os.path.join(self.report_dir, fname)
        with open(path, "w") as fh:
            fh.write("REPRO: {0}\n\n{1}".format(repro, report_text))
        self.sigs[sig] = {"count": 1, "kind": kind, "repro": repro, "report": fname}
        sys.stderr.write("[hang-hunter] NEW {0} sig={1} -> {2}\n   repro: {3}\n".format(
            kind, sig, fname, repro))

    def on_hang(self, slot):
        pid = slot["proc"].pid
        buf = []
        triage.triage_hang(pid, buf.append)
        text = "".join(buf)
        sig, key = triage.signature(text)
        self.totals["hang"] += 1
        self.record("HANG", sig, key, slot["job"].repro, text)
        try:
            slot["proc"].kill()
        except OSError:
            pass

    def on_crash(self, slot, rc):
        pid = slot["proc"].pid
        core = os.path.join(self.core_dir, "core.{0}".format(pid))
        if not os.path.exists(core):
            core = None
        buf = []
        sig, key = triage.triage_crash(self.py, core, buf.append)
        self.totals["crash"] += 1
        self.record("CRASH", sig, key,
                    slot["job"].repro + "  (rc={0})".format(rc), "".join(buf))

    def reap(self):
        still = []
        now = time.time()
        for slot in self.running:
            rc = slot["proc"].poll()
            if rc is None:
                if now - slot["start"] > slot["job"].timeout:
                    self.on_hang(slot)            # alive past timeout -> HANG
                else:
                    still.append(slot)
            elif rc == 0:
                self.totals["ok"] += 1
            else:
                self.on_crash(slot, rc)           # nonzero exit / signal -> CRASH
        self.running = still

    def write_status(self):
        lines = ["hang-hunter status  {0}".format(time.strftime("%Y-%m-%d %H:%M:%S")),
                 "uptime: {0:.0f}s  python: {1}".format(time.time() - self.start, self.py),
                 "engines: {0}  jobs: {1}  load1: {2:.1f}/{3}".format(
                     ",".join(self.engines), self.jobs, loadavg1(), self.ncpu),
                 "totals: {0}".format(self.totals),
                 "distinct findings: {0}".format(len(self.sigs)), ""]
        for sig, d in sorted(self.sigs.items(), key=lambda kv: -kv[1]["count"]):
            lines.append("  {0}  {1:6}  n={2:<5} {3}".format(
                sig, d["kind"], d["count"], d["report"]))
            lines.append("       repro: {0}".format(d["repro"]))
        open(self.status_path, "w").write("\n".join(lines) + "\n")

    def done(self):
        if self.args.once:
            return self.totals["launched"] >= self.args.once and not self.running
        if self.args.duration:
            return time.time() - self.start > self.args.duration and not self.running
        return False                              # --daemon: until signalled

    def run(self):
        stop = {"v": False}
        signal.signal(signal.SIGINT, lambda *a: stop.__setitem__("v", True))
        signal.signal(signal.SIGTERM, lambda *a: stop.__setitem__("v", True))
        sys.stderr.write("[hang-hunter] py={0} engines={1} jobs={2} reports={3}\n".format(
            self.py, ",".join(self.engines), self.jobs, self.report_dir))
        last_status = 0
        accepting = True
        while True:
            self.reap()
            if stop["v"]:
                accepting = False
            if self.args.once and self.totals["launched"] >= self.args.once:
                accepting = False
            if self.args.duration and time.time() - self.start > self.args.duration:
                accepting = False
            if accepting and len(self.running) < self.jobs \
                    and loadavg1() < self.args.load_frac * self.ncpu:
                self.launch()
            if time.time() - last_status > 5:
                self.write_status()
                last_status = time.time()
            if not accepting and not self.running:
                break
            time.sleep(0.2)
        self.write_status()
        sys.stderr.write("[hang-hunter] done. totals={0} distinct={1}\n   status: {2}\n".format(
            self.totals, len(self.sigs), self.status_path))


def main():
    ap = argparse.ArgumentParser(description="autonomous runloom hang/crash hunter")
    ap.add_argument("--engines", default="stress,hypo")
    ap.add_argument("--jobs", type=int, default=0, help="parallel jobs (0=auto)")
    ap.add_argument("--once", type=int, default=0, help="run ~N jobs then exit")
    ap.add_argument("--duration", type=int, default=0, help="hunt for N seconds")
    ap.add_argument("--daemon", action="store_true", help="run until SIGINT/SIGTERM")
    ap.add_argument("--load-frac", type=float, default=0.7,
                    help="pause launching when load1 exceeds this fraction of cores")
    ap.add_argument("--report-dir", default=os.path.join(REPO, "hang_hunter_reports"))
    ap.add_argument("--python", default="")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()
    if not (args.once or args.duration or args.daemon):
        args.once = 50
    Hunter(args).run()


if __name__ == "__main__":
    main()
