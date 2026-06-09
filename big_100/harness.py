"""big_100 shared test harness for the runloom (pygo) extension.

Every one of the 100 stress projects is a thin workload on top of this
module.  The harness owns all the cross-cutting requirements so the
project files stay focused on the thing they actually stress:

  * --duration   run for N seconds (default 1 hour; 1-2h is the design point)
  * --seed       deterministic per-worker RNG derivation for replay
  * --hubs       number of M:N scheduler hubs (REQUIRED > 1; this whole
                 campaign runs runloom in M:N parallel mode, never the aio
                 bridge and never single-thread run(1))
  * --funcs      how many lightweight goroutines to field (tens of thousands)
  * progress     a log line every --log-interval seconds (default 5س -> 5s)
  * watchdog     a REAL OS thread that fails the process if forward progress
                 stalls for --hang-timeout seconds (catches scheduler hangs)
  * invariants   H.check(cond, msg) / H.fail(msg) -> fail fast, nonzero exit
  * metrics      ops/sec, completed funcs, failures, leaked fds at the end
  * exit code    0 ok, 1 invariant failure, 2 setup/exception, 3 watchdog hang

Design notes that matter under M:N (run(n>1)):
  * Goroutines run in PARALLEL across hubs with the GIL off, so naive
    `x += 1` on shared Python state races.  Hot counters here are SHARDED
    (one slot per worker, single-writer) so they are race-free without a
    lock on the hot path; the rare counters (failures, worker-exit) take a
    cooperative lock.
  * You may only spawn goroutines from INSIDE the root (once the hubs are
    live).  go() called before run() lands on the idle single-thread
    scheduler and never runs.  The harness always spawns inside the root.
  * Every goroutine must eventually RETURN -- mn_run() joins on the pending
    count, it does not return on quiescence.  Servers therefore loop on
    H.running() and the harness closes registered listeners at shutdown so a
    parked accept() unblocks and the loop exits.

Run one project directly:
    PYTHON_GIL=0 python3.13t big_100/p01_tcp_echo.py --duration 10 --hubs 4
Run many in parallel across the box:
    PYTHON_GIL=0 python3.13t big_100/run_all.py --jobs 16 --hubs 4 --duration 60
"""
import argparse
import faulthandler
import os
import sys
import time
import traceback

# ---- capture ORIGINAL stdlib entry points BEFORE monkey.patch() ----------
# The watchdog runs on a real OS thread and must not be turned cooperative;
# it needs a real time.sleep / time.monotonic and a real thread spawn.
REAL_MONO = time.monotonic
REAL_SLEEP = time.sleep
REAL_PERF = time.perf_counter
import _thread as _real_thread

# ---- make `runloom` importable from the repo checkout ---------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
_SRC = os.path.join(os.path.dirname(_HERE), "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

# Quiet the per-wedge SYSMON diagnostic spam by default (the detector +
# handoff + preemption stay fully ON -- this only suppresses the WEDGED/
# RECOVERED stderr lines that otherwise flood a multi-hour log).  Override
# with RUNLOOM_SYSMON_QUIET=0 to see them.  Must be set before mn_init().
os.environ.setdefault("RUNLOOM_SYSMON_QUIET", "1")

import runloom            # noqa: E402
import runloom.monkey     # noqa: E402
import runloom_c          # noqa: E402

# Exit codes
EXIT_OK = 0
EXIT_INVARIANT = 1
EXIT_ERROR = 2
EXIT_HANG = 3

# Hot-counter sharding.  Power of two so we can mask.  64k slots covers the
# common "tens of thousands of workers, one shard each" case exactly (one
# writer per slot -> race-free); past that, shards alias and ops/sec becomes a
# slight undercount, which is fine for a throughput metric.
NSHARDS = 1 << 16
SHARD_MASK = NSHARDS - 1


def count_fds():
    """Open file descriptors for this process (Linux).  -1 if unknown."""
    try:
        return len(os.listdir("/proc/self/fd"))
    except OSError:
        return -1


def raise_fd_limit(target):
    """Best-effort raise of RLIMIT_NOFILE so tens of thousands of sockets fit.

    A non-root process can raise its SOFT limit only up to the HARD limit; the
    hard limit on this box defaults to 4096, far below the system ceiling
    (fs.nr_open ~8M).  We raise the hard limit via `sudo -n prlimit` on our own
    pid (uid is unchanged), then pull the soft limit up to it.  If sudo isn't
    available we still raise soft->hard.  Returns the resulting (soft, hard)."""
    import resource
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    except Exception:
        return (-1, -1)
    if hard < target:
        try:
            import subprocess
            subprocess.run(
                ["sudo", "-n", "prlimit", "--pid", str(os.getpid()),
                 "--nofile={0}:{0}".format(target)],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=10)
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        except Exception:
            pass
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    except Exception:
        pass
    try:
        return resource.getrlimit(resource.RLIMIT_NOFILE)
    except Exception:
        return (soft, hard)


class StopWorkload(Exception):
    """Raised inside a worker to unwind cleanly when the run is over."""


class Harness(object):
    def __init__(self, name, default_funcs=10000, describe="", add_args=None):
        self.name = name
        self.describe = describe
        ap = argparse.ArgumentParser(
            prog=name,
            description=describe or name,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        ap.add_argument("--duration", type=float, default=3600.0,
                        help="seconds to run (design point 3600-7200)")
        ap.add_argument("--seed", type=int, default=1234,
                        help="master seed for deterministic per-worker RNG")
        ap.add_argument("--hubs", type=int, default=8,
                        help="M:N scheduler hubs (must be > 1)")
        ap.add_argument("--funcs", type=int, default=default_funcs,
                        help="number of lightweight goroutines")
        ap.add_argument("--hang-timeout", type=float, default=60.0,
                        help="watchdog: fail if no progress for this long")
        ap.add_argument("--drain-timeout", type=float, default=120.0,
                        help="seconds allowed for post-deadline drain/teardown"
                             " (hard_deadline = deadline + drain_timeout)")
        ap.add_argument("--log-interval", type=float, default=5.0,
                        help="seconds between progress log lines")
        ap.add_argument("--fd-limit", type=int, default=8388608,
                        help="raise RLIMIT_NOFILE to this (sudo prlimit). "
                             "Defaults to 8M; the kernel fs.nr_open ceiling "
                             "is typically 1M-8M depending on sysctl.")
        ap.add_argument("--stack-kb", type=int, default=512,
                        help="per-goroutine C stack in KB (default 512: the "
                             "128KB default overflows under Python socket load "
                             "at scale, just as the aio bridge bumps I/O "
                             "goroutines to 512KB)")
        ap.add_argument("--fail-fast", action="store_true", default=True,
                        help="stop on first invariant violation (default on)")
        ap.add_argument("--no-fail-fast", dest="fail_fast",
                        action="store_false")
        ap.add_argument("--handoff", action="store_true", default=False,
                        help="enable the RUNLOOM_HANDOFF rescue (default OFF: "
                             "the campaign found it corrupts memory under high "
                             "socket concurrency -- see FINDINGS.md BUG #2; "
                             "pass this to reproduce that crash)")
        ap.add_argument("--max-concurrent", type=int, default=None,
                        metavar="K",
                        help="limit goroutines per run_pool call to K.  "
                             "Useful when yield_now()/sleep() inside a lock "
                             "makes drain time scale with goroutine count: "
                             "with K goroutines the lock can't be held for "
                             ">K/hubs scheduler ticks.  Defaults to "
                             "RUNLOOM_MAX_CONCURRENT env var if set, else "
                             "unlimited.  Programs with hard resource limits "
                             "(PTY count, socket FDs) may apply a tighter cap "
                             "via run_pool(max_concurrent=N).")
        ap.add_argument("--ip-slot", type=int, default=0,
                        help="IP isolation slot; slot N uses 127.(N+1).0.x "
                             "so concurrent jobs never share loopback addresses "
                             "and cannot exhaust each other's ephemeral ports")
        if add_args is not None:
            add_args(ap)
        self.args = ap.parse_args()

        if self.args.hubs < 2:
            sys.stderr.write(
                "[{0}] --hubs must be > 1: this campaign exercises runloom in "
                "M:N parallel mode (run(n>1)).\n".format(name))
            raise SystemExit(EXIT_ERROR)

        self.seed = self.args.seed
        self.duration = self.args.duration
        self.hubs = self.args.hubs
        self.funcs = self.args.funcs
        self._max_funcs = None   # set by harness.main(max_funcs=) to cap H.funcs

        # Global concurrent-goroutine cap per run_pool call.
        # Programs with hard resource limits may override with a lower value
        # passed directly to run_pool(max_concurrent=N).
        _env_mc = os.environ.get("RUNLOOM_MAX_CONCURRENT", "")
        _arg_mc = getattr(self.args, "max_concurrent", None)
        if _arg_mc is not None:
            self.max_concurrent = _arg_mc
        elif _env_mc.strip().isdigit():
            self.max_concurrent = int(_env_mc.strip())
        else:
            self.max_concurrent = None   # unlimited

        self.hang_timeout = self.args.hang_timeout
        self.drain_timeout = self.args.drain_timeout
        self.log_interval = self.args.log_interval
        self.fail_fast = self.args.fail_fast

        # IP isolation: slot N -> subnet 127.(N+1).0.0/24
        # 8 IPs per slot; slot 0 = 127.1.0.1..8 (never 127.0.0.1 to avoid
        # colliding with the default loopback used outside the soak)
        _slot = max(0, self.args.ip_slot)
        self.net_ips = [
            "127.{0}.0.{1}".format(_slot + 1, i + 1) for i in range(8)
        ]
        # Expose primary IP as env var so netutil defaults pick it up without
        # requiring every test to read H.net_ips explicitly.
        os.environ["SOAK_HOST_IP"] = self.net_ips[0]

        # timing
        self.t0 = REAL_MONO()
        self.deadline = self.t0 + self.duration

        # sharded hot counters (race-free single-writer-per-slot)
        self.ops = [0] * NSHARDS         # granular operations -> ops/sec
        self.tasks = [0] * NSHARDS       # completed lightweight funcs

        # rare counters under a cooperative lock
        self.failures = 0
        self.exited = 0                  # worker goroutines that returned
        self.expected = 0                # worker goroutines spawned
        self.first_fail = None           # (msg) of the first invariant break
        self.errors = []                 # sample of (wid, repr) error strings

        # Real OS lock for exited: at 100k goroutines all finishing simultaneously
        # a CoLock would serialize 100k cooperative handoffs (~1s each at scale),
        # making drain take minutes.  A real OS lock takes <1µs per acquire for
        # this tiny critical section (`self.exited += 1`) and never parks a
        # goroutine or blocks a hub thread for more than a microsecond.
        # _real_thread is imported before monkey.patch(), so it is never replaced.
        self._exit_lock = _real_thread.allocate_lock()

        # control flags
        self.failed = False              # invariant violated -> nonzero exit
        self.done_flag = False           # workload finished -> shut down
        self.finished = False            # process is wrapping up (watchdog off)

        # resources to close at shutdown so parked accept()/recv() unblock
        self.closeables = []
        # callables run once at the very end (e.g. shutil.rmtree a temp dir)
        self.cleanups = []

        # fd accounting
        self.fd_base = -1
        self.fd_end = -1

        # cooperative lock (monkey) -- created lazily inside the scheduler
        self.lock = None

        self.exit_code = EXIT_OK
        self._watch_started = False

        # Raise the fd ceiling up front so socket-heavy projects can field
        # tens of thousands of concurrent connections.
        self.fd_limit = raise_fd_limit(self.args.fd_limit)

        # Roomier per-goroutine stack.  The 128KB scheduler default overflows
        # the guard page on the deep Python socket path under M:N at scale
        # (10k+ goroutines) -> SIGSEGV/SIGBUS; 512KB is virtual+pooled (cheap
        # RSS) and matches the aio bridge's own I/O-goroutine choice.  A
        # project that needs more (deep recursion) raises --stack-kb.
        self.stack_kb = self.args.stack_kb
        if self.stack_kb > 0:
            try:
                runloom_c.set_stack_size(self.stack_kb * 1024)
            except Exception:
                pass

        # BUG #2 workaround (see FINDINGS.md): the handoff rescue corrupts
        # memory under high socket concurrency.  Default it OFF so the whole
        # campaign can soak; --handoff turns it back on to reproduce.  Must be
        # set before mn_init() reads it (runloom.run, below).
        self.handoff = self.args.handoff
        os.environ["RUNLOOM_HANDOFF"] = "1" if self.handoff else "0"

    # ---------------- determinism ----------------
    def derive(self, *parts):
        """A fresh random.Random seeded deterministically from the master
        seed plus the given parts.  Same seed + same parts -> same stream,
        which is what makes a failing run replayable."""
        import random
        h = self.seed & 0xFFFFFFFFFFFF
        for p in parts:
            h = (h * 1000003 + (hash(p) & 0xFFFFFFFF)) & 0xFFFFFFFFFFFF
        return random.Random(h)

    # ---------------- timing / control ----------------
    def now(self):
        return REAL_MONO() - self.t0

    def time_left(self):
        return self.deadline - REAL_MONO()

    def running(self):
        """True while workers should keep doing work."""
        return (not self.failed and not self.done_flag
                and REAL_MONO() < self.deadline)

    def sleep(self, seconds):
        runloom.sleep(seconds)

    # ---------------- counters ----------------
    def op(self, shard, k=1):
        i = shard & SHARD_MASK
        self.ops[i] = self.ops[i] + k

    def task_done(self, shard, k=1):
        i = shard & SHARD_MASK
        self.tasks[i] = self.tasks[i] + k

    def total_ops(self):
        return sum(self.ops)

    def total_tasks(self):
        return sum(self.tasks)

    def progress_signal(self):
        """Monotonic-ish scalar the watchdog samples for forward progress."""
        return self.total_ops() + self.total_tasks() + self.exited

    # ---------------- invariants / failures ----------------
    def fail(self, msg):
        """Record an invariant violation and (if fail-fast) stop the run."""
        if self.lock is not None:
            with self.lock:
                self.failures += 1
                if self.first_fail is None:
                    self.first_fail = msg
        else:
            self.failures += 1
            if self.first_fail is None:
                self.first_fail = msg
        self.failed = True
        sys.stderr.write("[{0}] INVARIANT FAIL: {1}\n".format(self.name, msg))
        sys.stderr.flush()

    def check(self, cond, msg):
        if not cond:
            self.fail(msg)
        return cond

    def error(self, wid, exc):
        """Record an unexpected worker exception (counts as a failure)."""
        rep = "{0}: {1}".format(type(exc).__name__, exc)
        if self.lock is not None:
            with self.lock:
                self.failures += 1
                if len(self.errors) < 20:
                    self.errors.append((wid, rep))
                if self.first_fail is None:
                    self.first_fail = "worker {0}: {1}".format(wid, rep)
        self.failed = True
        sys.stderr.write("[{0}] worker {1} error: {2}\n".format(
            self.name, wid, rep))
        sys.stderr.write(traceback.format_exc())
        sys.stderr.flush()

    # ---------------- spawning ----------------
    def go(self, fn, *args, **kwargs):
        """Spawn a goroutine.  Must be called from inside the root (M:N)."""
        return runloom.go(fn, *args, **kwargs)

    def register_close(self, obj):
        """Register a socket/file to be closed at shutdown so a parked
        accept()/recv() unblocks and its server loop can exit."""
        self.closeables.append(obj)
        return obj

    def add_cleanup(self, fn):
        """Register a callable to run once at the very end (after metrics),
        e.g. to remove a temp directory."""
        self.cleanups.append(fn)

    def net_ip(self, n=0):
        """Return the nth IP in this job's isolated loopback subnet."""
        return self.net_ips[n]

    def make_tmpdir(self, prefix="big100_"):
        """Create a temp dir that is shutil.rmtree'd at the end."""
        import tempfile
        import shutil
        d = tempfile.mkdtemp(prefix=prefix)
        self.add_cleanup(lambda: shutil.rmtree(d, ignore_errors=True))
        return d

    def run_pool(self, n, worker_fn, *extra, **kw):
        """Spawn worker goroutines, each running worker_fn(H, wid, rng, *extra).

        max_concurrent=K   spawn only min(n, K) goroutines instead of n.
                           Overrides H.max_concurrent for this pool.
                           Use for hard resource limits (PTY count, socket FDs).
                           Most programs should omit this and let H.max_concurrent
                           (set via --max-concurrent / RUNLOOM_MAX_CONCURRENT) do
                           the job.
        """
        max_concurrent = kw.pop("max_concurrent", self.max_concurrent)
        if kw:
            raise TypeError("unexpected keyword arguments: " + ", ".join(kw))
        actual = n if (max_concurrent is None or max_concurrent >= n) else max_concurrent
        self.expected += actual
        for wid in range(actual):
            rng = self.derive("pool", worker_fn.__name__, wid)
            self.go(self._worker_wrap, worker_fn, wid, rng, extra)

    def _worker_wrap(self, fn, wid, rng, extra):
        try:
            fn(self, wid, rng, *extra)
        except StopWorkload:
            pass
        except Exception as exc:           # noqa: BLE001 - report everything
            if self.running() or not isinstance(exc, OSError):
                self.error(wid, exc)
        finally:
            with self._exit_lock:
                self.exited += 1

    # ---------------- progress logging ----------------
    def progress_loop(self):
        last_ops = 0
        last_t = self.now()
        self.log("start  hubs={0} funcs={1} seed={2} duration={3:.0f}s "
                 "netpoll={4} backend={5} gil={6} nofile={7} stack={8}KB "
                 "handoff={9}".format(
                     self.hubs, self.funcs, self.seed, self.duration,
                     runloom_c.netpoll_backend(), runloom_c.backend(),
                     sys._is_gil_enabled(), self.fd_limit, self.stack_kb,
                     "on" if self.handoff else "off"))
        while self.running():
            target = self.now() + self.log_interval
            while self.running() and self.now() < target:
                runloom.sleep(0.25)
            t = self.now()
            ops = self.total_ops()
            dt = max(1e-6, t - last_t)
            rate = (ops - last_ops) / dt
            self.log(
                "t={0:6.1f}s ops={1:>10d} {2:>9.0f}/s tasks={3:>9d} "
                "exited={4}/{5} fail={6} fds={7} left={8:.0f}s".format(
                    t, ops, rate, self.total_tasks(), self.exited,
                    self.expected, self.failures, count_fds(),
                    max(0.0, self.time_left())))
            last_ops, last_t = ops, t

    def log(self, msg):
        sys.stderr.write("[{0} {1:7.1f}] {2}\n".format(
            self.name, self.now(), msg))
        sys.stderr.flush()

    # ---------------- watchdog (real OS thread) ----------------
    def start_watchdog(self):
        if self._watch_started:
            return
        self._watch_started = True
        faulthandler.enable()
        _real_thread.start_new_thread(self._watchdog, ())

    def _watchdog(self):
        last = self.progress_signal()
        last_change = REAL_MONO()
        hard_deadline = self.deadline + max(self.drain_timeout, 0.15 * self.duration)
        while not self.finished:
            REAL_SLEEP(2.0)
            if self.finished:
                return
            now = REAL_MONO()
            sig = self.progress_signal()
            if sig != last:
                last = sig
                last_change = now
            # Only treat a stall as a hang while there is work to do: before
            # the deadline (workers should be progressing) or during the
            # post-deadline drain (which must finish, not wedge).
            stalled = (now - last_change) > self.hang_timeout
            if stalled and not self.done_flag and now < self.deadline:
                self._hang("no forward progress for {0:.0f}s".format(
                    now - last_change))
                return
            if now > hard_deadline:
                self._hang("hard deadline exceeded (drain/teardown wedged)")
                return

    def _hang(self, why):
        sys.stderr.write(
            "\n[{0}] WATCHDOG HANG: {1}\n".format(self.name, why))
        sys.stderr.write(
            "[{0}] ops={1} tasks={2} exited={3}/{4} -- dumping all threads:\n"
            .format(self.name, self.total_ops(), self.total_tasks(),
                    self.exited, self.expected))
        sys.stderr.flush()
        try:
            faulthandler.dump_traceback(all_threads=True)
        except Exception:
            pass
        try:
            runloom.dump()
        except Exception:
            pass
        sys.stderr.flush()
        os._exit(EXIT_HANG)

    # ---------------- fd accounting ----------------
    def snapshot_fds(self):
        self.fd_base = count_fds()

    # ---------------- lifecycle ----------------
    def mark_done(self):
        """Signal workers to stop and unblock parked servers by closing
        the registered listeners/sockets."""
        self.done_flag = True
        for obj in self.closeables:
            try:
                obj.close()
            except Exception:
                pass

    def wait_for_deadline(self, poll=0.2):
        """Block the root goroutine until the duration elapses (or an
        invariant fails).  Workers run on the hubs meanwhile."""
        while self.running():
            runloom.sleep(poll)

    def drain_workers(self, grace=30.0):
        """After mark_done(), give worker goroutines a bounded window to
        return so we can report an accurate completed count."""
        until = REAL_MONO() + grace
        while self.exited < self.expected and REAL_MONO() < until:
            runloom.sleep(0.05)

    def run(self, body, setup=None, post=None):
        """The single entry point a project calls.

        setup(H): optional, runs inside the root before the workload (bind
                  servers, create temp dirs).  Servers it starts should be
                  registered with H.register_close().
        body(H):  spawns the worker pool(s) and returns; the harness then
                  waits out the duration, marks done, and drains.
        post(H):  optional, runs in the MAIN process after the scheduler has
                  fully drained (all goroutines done) and before the verdict is
                  computed -- the place for end-of-run conservation checks that
                  need the final aggregated state.
        """
        self.start_watchdog()

        def root():
            self.lock = runloom.sync.Lock()
            self.go(self.progress_loop)
            if setup is not None:
                try:
                    setup(self)
                except BaseException as exc:
                    self.failed = True
                    if self.first_fail is None:
                        self.first_fail = "setup() raised: {0}: {1}".format(
                            type(exc).__name__, exc)
                    sys.stderr.write("[{0}] SETUP FAILED:\n".format(self.name))
                    sys.stderr.write(traceback.format_exc())
                    sys.stderr.flush()
                    self.exit_code = EXIT_ERROR
                    return  # let deadline/drain finish naturally
            self.snapshot_fds()
            try:
                body(self)
            except BaseException as exc:
                self.failed = True
                if self.first_fail is None:
                    self.first_fail = "body() raised: {0}: {1}".format(
                        type(exc).__name__, exc)
                sys.stderr.write("[{0}] BODY FAILED:\n".format(self.name))
                sys.stderr.write(traceback.format_exc())
                sys.stderr.flush()
                self.exit_code = EXIT_ERROR
                return  # let deadline/drain finish naturally
            self.wait_for_deadline()
            self.mark_done()
            self.drain_workers()

        runloom.monkey.patch()
        try:
            runloom.run(self.hubs, root)
        except SystemExit:
            raise
        except BaseException as exc:        # noqa: BLE001
            self.failed = True
            if self.first_fail is None:
                self.first_fail = "run() raised: {0}: {1}".format(
                    type(exc).__name__, exc)
            sys.stderr.write("[{0}] run() raised:\n".format(self.name))
            sys.stderr.write(traceback.format_exc())
            sys.stderr.flush()
            self.exit_code = EXIT_ERROR
        finally:
            self.finished = True
        self.fd_end = count_fds()
        if post is not None and self.exit_code != EXIT_ERROR:
            try:
                post(self)
            except Exception as exc:        # noqa: BLE001
                self.fail("post-check raised: {0}: {1}".format(
                    type(exc).__name__, exc))
        return self.finish()

    def finish(self):
        elapsed = self.now()
        ops = self.total_ops()
        tasks = self.total_tasks()
        rate = ops / max(1e-6, elapsed)
        leaked = (self.fd_end - self.fd_base
                  if self.fd_base >= 0 and self.fd_end >= 0 else -1)

        if self.exit_code == EXIT_OK:
            if self.failed:
                self.exit_code = EXIT_INVARIANT
            else:
                self.exit_code = EXIT_OK

        sys.stderr.write("\n")
        sys.stderr.write("==== {0} RESULTS ====\n".format(self.name))
        sys.stderr.write("  elapsed_s     : {0:.1f}\n".format(elapsed))
        sys.stderr.write("  hubs          : {0}\n".format(self.hubs))
        sys.stderr.write("  funcs         : {0}\n".format(self.funcs))
        sys.stderr.write("  seed          : {0}\n".format(self.seed))
        sys.stderr.write("  ops           : {0}\n".format(ops))
        sys.stderr.write("  ops_per_sec   : {0:.0f}\n".format(rate))
        sys.stderr.write("  completed_funcs: {0}\n".format(tasks))
        sys.stderr.write("  worker_exits  : {0}/{1}\n".format(
            self.exited, self.expected))
        sys.stderr.write("  failures      : {0}\n".format(self.failures))
        sys.stderr.write("  fd_base       : {0}\n".format(self.fd_base))
        sys.stderr.write("  fd_end        : {0}\n".format(self.fd_end))
        sys.stderr.write("  leaked_fds    : {0}  (a fixed ~100-150 floor is "
                         "scheduler/offload-pool fds, not a per-op leak; the "
                         "auditor projects check bounded growth)\n".format(
                             leaked))
        if self.first_fail:
            sys.stderr.write("  first_failure : {0}\n".format(self.first_fail))
        verdict = "PASS" if self.exit_code == EXIT_OK else "FAIL"
        sys.stderr.write("  VERDICT       : {0} (exit {1})\n".format(
            verdict, self.exit_code))
        sys.stderr.flush()
        for fn in self.cleanups:
            try:
                fn()
            except Exception:
                pass
        return self.exit_code


def main(name, body, setup=None, post=None, default_funcs=10000, describe="",
         add_args=None, max_funcs=None):
    """Convenience entry point for a project module.

    max_funcs   hard ceiling on H.funcs regardless of --funcs.  Use for
                programs that are resource-constrained (subprocesses, PTYs,
                file handles) and must not be driven at 1M goroutines even
                when the soak script passes --funcs 1000000.
    """
    H = Harness(name, default_funcs=default_funcs, describe=describe,
                add_args=add_args)
    if max_funcs is not None and H.funcs > max_funcs:
        H.funcs = max_funcs
    code = H.run(body, setup=setup, post=post)
    sys.exit(code)
