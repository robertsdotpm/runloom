"""Job generators ("engines") for the hang-hunter.

Each engine, given an RNG, returns a Job: a self-contained subprocess invocation
(a runloom workload run under the free-threaded interpreter) plus the metadata
needed to reproduce and time it.  The orchestrator launches Jobs in parallel and
triages any that hang or crash.

Shipped engines that run on this box today:
  stress    -- randomized real M:N workloads (gc churn, channel storm) with random
               hub counts / sizes and random scheduler env knobs (sysmon / preempt /
               handoff on-off, world-yield ns).
  hypo      -- Hypothesis-generated always-terminating programs (so any hang is a
               real bug); shrinks to a minimal repro on assertion failures.
  lifefuzz  -- one generative life-cycle program from tools/lifefuzz (varied stacks,
               channel ref churn, nested spawn/migration, timed parks, select+close,
               undrained buffers).  Always-terminating, so a HANG is a real lost
               wakeup; a nonzero exit (crash / life-cycle-oracle violation) is a bug.

TSan-rotation engine (auto-selected when the ext is TSan-built):
  lifefuzz-tsan -- the same generative programs under the gold-standard TSan ext
               (setarch -R + LD_PRELOAD=libtsan), so a non-suppressed data race
               surfaces.  This is the engine that found the deadlock-census race
               cluster (tools/README Finding D).  When the built ext links libtsan
               this engine REPLACES the normal set (the non-TSan engines can't load
               a TSan ext); build it via tools/run_sanitizers_ext.sh, then run the
               daemon (it auto-detects and rotates lifefuzz-tsan).

Future engines (hooks, need installs): atheris (coverage-guided Python fuzzing),
afl/libfuzzer (C-harness for the deque/chan/select primitives).
"""
import os
import subprocess

HERE = os.path.dirname(os.path.abspath(__file__))
WL = os.path.join(HERE, "workloads")
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
LIFEFUZZ = os.path.join(ROOT, "tools", "lifefuzz", "lifefuzz.py")
NETPOLL_BACKENDS = ["epoll", "select", "io_uring"]


class Job(object):
    def __init__(self, name, argv, env, timeout, repro):
        self.name = name
        self.argv = argv
        self.env = env            # overlay on os.environ
        self.timeout = timeout
        self.repro = repro


def _knobs(rng):
    """Random scheduler env knobs -- exercise the recovery machinery on and off."""
    # Flight recorder (#1): record the scheduler event ring and install the
    # crash handler so any crash carries its recent per-thread timeline.
    env = {"PYTHON_GIL": "0", "RUNLOOM_GIL": "0",
           "RUNLOOM_DEBUG": "ring", "RUNLOOM_CRASH": "on"}
    for k in ("RUNLOOM_SYSMON", "RUNLOOM_PREEMPT", "RUNLOOM_HANDOFF"):
        if rng.random() < 0.3:
            env[k] = "0"
    if rng.random() < 0.3:
        # vary the world-yield pause but never 0 -- 0 disables the stop-the-world
        # monopoly fix (a known deadlock), which would be a self-inflicted false
        # finding rather than a new bug.
        env["RUNLOOM_WORLD_YIELD_NS"] = str(rng.choice([1000, 50000, 100000, 500000]))
    return env


def stress_job(rng, py):
    which = rng.choice(["gc_churn", "chan_storm"])
    env = _knobs(rng)
    nhub = rng.choice([1, 2, 2, 4, 4, 6, 8])
    env["HH_NHUB"] = str(nhub)
    if which == "gc_churn":
        # Each collector loops gc.collect() = near-continuous stop-the-world, so
        # workers progress slowly; NCOLL*NWORK*ROUNDS is a "how long" budget.
        # Cap it so a HEALTHY run finishes in seconds-to-~30s (even under load):
        # that keeps the generous hang timeout a clean deadlock/slow separator.
        # We still want variety (incl. multi-collector), just not the
        # pathologically-slow corner (NCOLL=3 x NWORK=96 x ROUNDS=500 ~ 1-2 min).
        ncoll = rng.choice([1, 1, 1, 2])
        if ncoll == 1:
            env["HH_NWORK"] = str(rng.choice([1, 4, 16, 48, 96]))
            env["HH_ROUNDS"] = str(rng.choice([50, 200, 500]))
        else:                                      # 2 collectors -> keep workers light
            env["HH_NWORK"] = str(rng.choice([4, 16, 48]))
            env["HH_ROUNDS"] = str(rng.choice([50, 200]))
        env["HH_NCOLL"] = str(ncoll)
    else:
        env["HH_PAIRS"] = str(rng.choice([2, 8, 16, 48]))
        env["HH_MSGS"] = str(rng.choice([50, 200, 500]))
        env["HH_CHAN_CAP"] = str(rng.choice([1, 1, 4, 16]))
        env["HH_GC"] = rng.choice(["1", "1", "0"])
    argv = [py, os.path.join(WL, which + ".py")]
    repro = " ".join("{0}={1}".format(k, v) for k, v in sorted(env.items())
                     if k.startswith(("HH_", "RUNLOOM_"))) + \
        "  {0} {1}".format(py, os.path.join(WL, which + ".py"))
    # 120s >> a healthy capped run (seconds-to-~30s) but << "forever": alive at
    # 120s == a real deadlock.
    return Job("stress:" + which, argv, env, 120, repro)


def hypo_job(rng, py):
    sd = rng.randrange(1, 2 ** 31)
    env = {"PYTHON_GIL": "0", "RUNLOOM_GIL": "0",
           "RUNLOOM_DEBUG": "ring", "RUNLOOM_CRASH": "on",
           "HH_MAX_EXAMPLES": str(rng.choice([50, 100, 150]))}
    argv = [py, os.path.join(WL, "hypo_model.py"), str(sd)]
    repro = "HH_MAX_EXAMPLES={0}  {1} {2} {3}".format(
        env["HH_MAX_EXAMPLES"], py, os.path.join(WL, "hypo_model.py"), sd)
    return Job("hypo", argv, env, 180, repro)


def lifefuzz_job(rng, py):
    """One generative life-cycle program (tools/lifefuzz) under the hang/crash net.
    Reuses the scheduler-knob randomizer; adds the freed-state oracle + a pinned
    RUNLOOM_MN_SEED so a finding replays.  The worker's INTERNAL watchdog is set
    high (600s) so a true wedge stays ALIVE past this Job's timeout -> the daemon's
    gdb-on-live-process hang triage fires (not the worker's own self-kill)."""
    seed = rng.randrange(1, 2 ** 31)
    mn_seed = rng.randrange(1, 2 ** 31)
    env = _knobs(rng)
    env["RUNLOOM_DBG_GSTATE"] = "1"                 # freed-state timer-entry oracle
    env["RUNLOOM_MN_SEED"] = str(mn_seed)           # deterministic baton -> replay
    if rng.random() < 0.5:
        env["RUNLOOM_NETPOLL"] = rng.choice(NETPOLL_BACKENDS)
    argv = [py, LIFEFUZZ, "worker", str(seed), str(mn_seed), "600"]
    repro = " ".join("{0}={1}".format(k, v) for k, v in sorted(env.items())
                     if k.startswith("RUNLOOM_")) + \
        "  {0} {1} repro {2} --mn-seed {3}".format(py, LIFEFUZZ, seed, mn_seed)
    # life-cycle programs finish in ~1-2s; alive at 90s == a real wedge.
    return Job("lifefuzz", argv, env, 90, repro)


def _libtsan():
    """Absolute path to libtsan.so, or None."""
    try:
        out = subprocess.check_output(["gcc", "-print-file-name=libtsan.so"]).decode().strip()
        return out if os.path.isabs(out) and os.path.exists(out) else None
    except Exception:                               # noqa: BLE001
        return None


def _ext_links_tsan():
    """True iff the built runloom_c ext links libtsan (a TSan-rotation build)."""
    try:
        import glob
        sos = glob.glob(os.path.join(ROOT, "src", "runloom_c*.so"))
        if not sos:
            return False
        out = subprocess.check_output(["ldd", sos[0]], stderr=subprocess.DEVNULL).decode()
        return "tsan" in out.lower()
    except Exception:                               # noqa: BLE001
        return False


def lifefuzz_tsan_job(rng, py):
    """lifefuzz under the gold-standard TSan ext: setarch -R (ASLR off, or TSan
    aborts on 6.x high-entropy mmaps) + LD_PRELOAD=libtsan + the runloom CPython-
    only suppressions; a non-suppressed race exits 86 -> CRASH triage.  TSan is
    ~10-20x slower, so a bigger timeout."""
    seed = rng.randrange(1, 2 ** 31)
    mn_seed = rng.randrange(1, 2 ** 31)
    env = _knobs(rng)
    env["RUNLOOM_DBG_GSTATE"] = "1"
    env["RUNLOOM_MN_SEED"] = str(mn_seed)
    libtsan = _libtsan() or "libtsan.so"
    env["LD_PRELOAD"] = libtsan
    env["TSAN_OPTIONS"] = ("halt_on_error=0:exitcode=86:history_size=7:suppressions="
                           + os.path.join(ROOT, "tools", "tsan_suppressions.txt"))
    arch = os.uname().machine
    argv = ["setarch", arch, "-R", py, LIFEFUZZ, "worker", str(seed), str(mn_seed), "600"]
    repro = ("LD_PRELOAD={0} TSAN_OPTIONS={1} {2}  setarch {3} -R {4} {5} repro {6} --mn-seed {7}"
             .format(libtsan, env["TSAN_OPTIONS"],
                     " ".join("{0}={1}".format(k, v) for k, v in sorted(env.items())
                              if k.startswith("RUNLOOM_")),
                     arch, py, LIFEFUZZ, seed, mn_seed))
    return Job("lifefuzz-tsan", argv, env, 240, repro)


# Auto-adapt to the build: a TSan-linked ext can only be loaded with libtsan
# preloaded, so the normal (non-preloaded) engines would fail to import -- in that
# state run the TSan rotation EXCLUSIVELY; otherwise the standard set.
if _ext_links_tsan() and _libtsan():
    ENGINES = {"lifefuzz-tsan": lifefuzz_tsan_job}
else:
    ENGINES = {"stress": stress_job, "hypo": hypo_job, "lifefuzz": lifefuzz_job}
