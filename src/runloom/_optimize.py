"""runloom.optimize() -- pick the trade-off(s) you care about and the runtime
sets the underlying knobs for you.

The point: you should never have to learn the raw RUNLOOM_* tuning env vars.
Call nothing and you get smart automatic defaults; call optimize() with one or
more *named trades* and the runtime leans that way.  Each name says exactly what
you are spending and buying:

    runloom.optimize()                          # auto -- smart defaults (the default)
    runloom.optimize("throughput")              # max req/s   (spends RAM)
    runloom.optimize("memory")                  # tight RSS   (spends throughput)
    runloom.optimize("latency")                 # sharp tail  (spends a little CPU)
    runloom.optimize("secure")                  # hardened    (spends a little speed)
    runloom.optimize("throughput", "latency")   # compose -- pass the trades you want
    runloom.optimize("memory", max_fibers=200_000)

Conflicts resolve by precedence ``secure > memory > latency > throughput`` (so
``optimize("throughput", "memory")`` keeps RSS lean where they disagree).

CALL IT ONCE, BEFORE ``runloom.run()`` -- the settings are read as the runtime
starts, and the first call wins for any given knob.  An explicit shell env var
(e.g. you exported RUNLOOM_STACK_MADV) still wins over optimize(), so power users
keep full control; the returned dict reflects the EFFECTIVE values after that.

These trades are deliberately SAFE: none of them flips an experimental lever or a
setting that can OOM-kill a RAM-tight host. The sharpest expert tricks (e.g.
RUNLOOM_STACK_MADV=off for zero-syscall-but-no-pressure-reclaim) stay raw env
vars with their own warnings -- a friendly name should never hide a footgun.
"""
import os

# Each goal -> the env-var bundle it pulls.  Values are in the exact format the C
# runtime parses (verified against the getenv sites).  Kept conservative: every
# value here is safe to request without a hidden OOM / experimental-mode footgun.
_GOAL_ENV = {
    "throughput": {
        "RUNLOOM_TCPCONN_IOURING":           "auto",   # flip epoll->io_uring as conns climb
        "RUNLOOM_TCPCONN_IOURING_THRESHOLD": "512",
        "RUNLOOM_BLOCKPOOL_WORKERS":         "16",      # more blocking-offload workers
        "RUNLOOM_GON_BULK":                  "1",       # bulk-arena spawn for big go_n
        # depot pool size is now AUTO -- it sizes itself to the live-fiber
        # high-water (vm.max_map_count- and RAM-clamped), so no static cap here.
    },
    "latency": {
        # Tighter stall detection -> faster recovery from a wedged hub. Only the
        # watchdog (default-on on free-threaded builds) acts on it; a no-op, never
        # a hazard, elsewhere. Costs a few hundred extra wakeups/sec -> CPU, not RAM.
        "RUNLOOM_SYSMON_MS":                 "25",
    },
    "memory": {
        "RUNLOOM_STACK_MADV":                "dontneed",  # eager reclaim, tightest RSS
        "RUNLOOM_STACK_PARK_DONTNEED":       "1",          # return idle parked-fiber pages now
    },
    "secure": {
        "RUNLOOM_STACK_SCRUB":               "1",       # wipe recycled stacks (TLS keys/bodies)
    },
}

# Apply order = ascending precedence; the later one wins on a conflicting key.
_PRECEDENCE = ("throughput", "latency", "memory", "secure")

#: the valid trade names, in precedence order.
GOALS = tuple(_PRECEDENCE)


def optimize(*goals, max_fibers=None):
    """Tune runloom for the trade-off(s) you care about.  Call ONCE, before run().

    goals: zero or more of "throughput", "latency", "memory", "secure" -- they
        compose, and a higher-precedence goal (secure > memory > latency >
        throughput) wins on any conflicting knob.  No goals = leave the smart
        automatic defaults in place.
    max_fibers: optional hard ceiling on concurrent fibers (backpressure); there
        is no sane automatic value for this, so it stays explicit.

    Returns the dict of EFFECTIVE settings for the knobs it touched (an explicit
    shell env var shows through here, since it wins).
    """
    for g in goals:
        if g not in _GOAL_ENV:
            raise ValueError(
                "unknown optimize goal {0!r}; choose from {1}".format(
                    g, ", ".join(GOALS)))

    merged = {}
    for g in _PRECEDENCE:
        if g in goals:
            merged.update(_GOAL_ENV[g])
    if max_fibers is not None:
        merged["RUNLOOM_MAX_GOROUTINES"] = str(int(max_fibers))

    # setdefault: an explicit shell env var (or an earlier optimize() call) wins.
    for k, v in merged.items():
        os.environ.setdefault(k, v)

    # Report what is ACTUALLY in effect for those keys (shell overrides show here).
    return {k: os.environ.get(k) for k in merged}
