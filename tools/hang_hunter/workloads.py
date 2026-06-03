"""Job generators ("engines") for the hang-hunter.

Each engine, given an RNG, returns a Job: a self-contained subprocess invocation
(a runloom workload run under the free-threaded interpreter) plus the metadata
needed to reproduce and time it.  The orchestrator launches Jobs in parallel and
triages any that hang or crash.

Shipped engines that run on this box today:
  stress  -- randomized real M:N workloads (gc churn, channel storm) with random
             hub counts / sizes and random scheduler env knobs (sysmon / preempt /
             handoff on-off, world-yield ns).
  hypo    -- Hypothesis-generated always-terminating programs (so any hang is a
             real bug); shrinks to a minimal repro on assertion failures.

Future engines (hooks, need installs): atheris (coverage-guided Python fuzzing),
afl/libfuzzer (C-harness for the deque/chan/select primitives), tsan-rotation
(periodic run under the gold-standard TSan interpreter).
"""
import os

HERE = os.path.dirname(os.path.abspath(__file__))
WL = os.path.join(HERE, "workloads")


class Job(object):
    def __init__(self, name, argv, env, timeout, repro):
        self.name = name
        self.argv = argv
        self.env = env            # overlay on os.environ
        self.timeout = timeout
        self.repro = repro


def _knobs(rng):
    """Random scheduler env knobs -- exercise the recovery machinery on and off."""
    env = {"PYTHON_GIL": "0", "RUNLOOM_GIL": "0"}
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
           "HH_MAX_EXAMPLES": str(rng.choice([50, 100, 150]))}
    argv = [py, os.path.join(WL, "hypo_model.py"), str(sd)]
    repro = "HH_MAX_EXAMPLES={0}  {1} {2} {3}".format(
        env["HH_MAX_EXAMPLES"], py, os.path.join(WL, "hypo_model.py"), sd)
    return Job("hypo", argv, env, 180, repro)


ENGINES = {"stress": stress_job, "hypo": hypo_job}
