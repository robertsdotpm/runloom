"""mn_digest -- the M:N determinism-digest harness (MN_SIM_DST_PLAN.md I0).

The determinism oracle for the native mn-sim plane: run a seeded M:N workload in
a FRESH subprocess, record the fiber completion order, and print its md5.  Same
(workload, hubs, seed) must be bit-identical across runs; a different seed must
differ.  This is the exact method the 2026-07-09 empirical probes validated
(P1 CPU+yield 7/7 identical at H=2, P2 timers 5/5, both seed-sensitive).

Harness rules (all load-bearing -- see the plan's wake-source contract + risks):
  * Subprocess per run: mn statics + the lazily-cached env flags reset only at
    process birth, so cross-run comparison is only honest in fresh processes.
  * PYTHONHASHSEED pinned (contract #26): str-keyed dict iteration otherwise
    varies per process and would show up as a false digest mismatch.
  * Assertions on PRINTED output, never exit codes: mn fiber exceptions are
    swallowed ("Exception ignored in") and mn_run exits 0 regardless (probe P4).
    The payload prints MN_DIGEST only after its own self-checks pass, else
    MN_DIGEST_ERROR -- the harness raises on anything that is not a digest line.
  * The digest must never include object ids / memory addresses (contract #9:
    GC/brc/QSBR state is not seed-stable) -- completion order is recorded as
    plain ints/tuples of ints.

Usable both ways:
  * imported:   from mn_digest import run_digest
                d = run_digest("cpu_yield", hubs=2, seed=12345)
  * subprocess: python mn_digest.py --workload cpu_yield --hubs 2
                (RUNLOOM_MN_SEED comes from the env, as in a real repro)
"""
import hashlib
import os
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DIGEST_MARK = "MN_DIGEST "
ERROR_MARK = "MN_DIGEST_ERROR "


# ---------------------------------------------------------------------------
# Workload payloads.  Each returns the completion-order list (plain ints or
# tuples of ints -- never object ids) after running its own self-checks, or
# raises with a message for the MN_DIGEST_ERROR path.
# ---------------------------------------------------------------------------

def workload_cpu_yield(rc, hubs):
    """64 fibers x 5 cooperative yields; order = completion sequence.
    The probe-validated P1 shape: pure scheduling, no timers, no I/O."""
    nfibers, rounds = 64, 5
    order = []

    def mk(k):
        def w():
            x = 0
            for r in range(rounds):
                x += k + r
                rc.sched_yield()
            order.append(k)
        return w

    rc.mn_init(hubs)
    for k in range(nfibers):
        rc.mn_fiber(mk(k))
    rc.mn_run()
    rc.mn_fini()
    if len(order) != nfibers:
        raise RuntimeError("cpu_yield: {0}/{1} fibers completed".format(
            len(order), nfibers))
    if sorted(order) != list(range(nfibers)):
        raise RuntimeError("cpu_yield: completion set mismatch")
    return order


def workload_timers(rc, hubs):
    """64 fibers each sched_sleep a varying duration; order = wake sequence.
    The probe-validated P2 shape: the census logical-clock advance decides
    ordering (equal durations tie-broken by the seeded schedule)."""
    nfibers = 64
    order = []

    def mk(k):
        def w():
            rc.sched_sleep(0.001 * ((k * 7) % 13 + 1))
            order.append(k)
        return w

    rc.mn_init(hubs)
    for k in range(nfibers):
        rc.mn_fiber(mk(k))
    rc.mn_run()
    rc.mn_fini()
    if len(order) != nfibers:
        raise RuntimeError("timers: {0}/{1} fibers completed".format(
            len(order), nfibers))
    if sorted(order) != list(range(nfibers)):
        raise RuntimeError("timers: completion set mismatch")
    return order


def workload_chan(rc, hubs):
    """8 producers x 16 items -> one Chan(4) -> 8 consumers.  Order = the
    consumed (consumer, item) sequence: exercises the staged cross-hub chan
    wake path (wake contract #3), which P1/P2 never touched -- this workload
    is that entry's first empirical support."""
    nprod, ncons, per = 8, 8, 16
    total = nprod * per
    order = []
    ch = rc.Chan(4)

    def producer(p):
        def w():
            for j in range(per):
                ch.send(p * per + j)
            order.append((1, p))            # producer p done
        return w

    def consumer(c):
        def w():
            for j in range(per):
                v, ok = ch.recv()           # Go-style (value, ok)
                if not ok:
                    raise RuntimeError("chan closed early")
                order.append((2, c, v))     # consumer c got item v
        return w

    rc.mn_init(hubs)
    for p in range(nprod):
        rc.mn_fiber(producer(p))
    for c in range(ncons):
        rc.mn_fiber(consumer(c))
    rc.mn_run()
    rc.mn_fini()

    got = sorted(rec[2] for rec in order if rec[0] == 2)
    if got != list(range(total)):
        raise RuntimeError("chan: consumed multiset mismatch ({0}/{1})".format(
            len(got), total))
    if sum(1 for rec in order if rec[0] == 1) != nprod:
        raise RuntimeError("chan: not all producers completed")
    return order


WORKLOADS = {
    "cpu_yield": workload_cpu_yield,
    "timers": workload_timers,
    "chan": workload_chan,
}


def digest_of(order):
    """md5 of the canonical repr of the completion order (plain ints/tuples)."""
    return hashlib.md5(repr(order).encode("utf-8")).hexdigest()


def payload_main(argv):
    """Subprocess entry: run one workload, print MN_DIGEST <md5> COUNT <n>."""
    workload, hubs = None, 2
    i = 1
    while i < len(argv):
        if argv[i] == "--workload":
            workload = argv[i + 1]; i += 2
        elif argv[i] == "--hubs":
            hubs = int(argv[i + 1]); i += 2
        else:
            raise SystemExit("unknown arg: {0}".format(argv[i]))
    sys.path.insert(0, os.path.join(REPO, "src"))
    import runloom_c as rc
    try:
        order = WORKLOADS[workload](rc, hubs)
    except BaseException as e:
        sys.stdout.write("{0}{1}: {2}\n".format(ERROR_MARK, type(e).__name__, e))
        sys.stdout.flush()
        return
    sys.stdout.write("{0}{1} COUNT {2}\n".format(
        DIGEST_MARK, digest_of(order), len(order)))
    sys.stdout.flush()


# ---------------------------------------------------------------------------
# Harness side (importable).
# ---------------------------------------------------------------------------

def hermetic_env(extra_env=None):
    """The digest subprocess env: the caller's environment with EVERY RUNLOOM_*
    knob stripped, then exactly the pinned keys (+ any explicit extra_env).

    Stripping is load-bearing twice over (adversarial review findings): an
    inherited RUNLOOM_SIM=1 -- which several sim test modules set via
    os.environ at IMPORT time, contaminating an in-process `pytest tests/`
    run at collection -- would trip the I0 mn_init fence inside every digest
    child; and inherited schedule knobs (RUNLOOM_MN_PREEMPT_FRAMES,
    RUNLOOM_LDFI_DROP, RUNLOOM_MN_TRACE, ...) would silently change WHICH
    schedule the suite freezes.  Deliberate knob-testing (e.g. I3's
    "digests stay stable WITH RUNLOOM_IOURING_LOOP=1" re-run) passes the knob
    explicitly via extra_env."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("RUNLOOM_")}
    env["PYTHON_GIL"] = "0"
    env["PYTHONHASHSEED"] = "0"                 # contract #26
    env["PYTHONPATH"] = os.path.join(REPO, "src")
    if extra_env:
        env.update(extra_env)
    return env


def run_digest(workload, hubs, seed, timeout=60, python=None, extra_env=None):
    """Run one (workload, hubs, seed) in a fresh hermetic subprocess; return
    the digest.

    Raises RuntimeError (with full output) on anything that is not a clean
    MN_DIGEST line from a cleanly-exited child: a swallowed fiber exception, a
    workload self-check failure, AND a nonzero/deadly-signal exit even when the
    digest line already printed (mn_fini teardown + interpreter shutdown run
    AFTER the digest prints -- a SEGV there must not score green; this does not
    contradict the printed-output rule, which forbids trusting exit 0 as
    success, not surfacing a crash).  A hang raises subprocess.TimeoutExpired."""
    env = hermetic_env(extra_env)
    env["RUNLOOM_MN_SEED"] = str(seed)
    cmd = [python or sys.executable, os.path.abspath(__file__),
           "--workload", workload, "--hubs", str(hubs)]
    p = subprocess.run(cmd, cwd=REPO, env=env, timeout=timeout,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    digest = None
    for line in p.stdout.splitlines():
        if line.startswith(DIGEST_MARK):
            digest = line[len(DIGEST_MARK):].split()[0]
            break
        if line.startswith(ERROR_MARK):
            raise RuntimeError("workload error: {0}\n--- stderr ---\n{1}".format(
                line, p.stderr[-1500:]))
    if p.returncode != 0:
        what = ("crash AFTER the digest printed (mn_fini/interpreter-shutdown "
                "window)" if digest is not None else "child died with no digest")
        raise RuntimeError(
            "{0}: rc={1}\n--- stdout ---\n{2}\n--- stderr ---\n{3}".format(
                what, p.returncode, p.stdout[-800:], p.stderr[-1500:]))
    if digest is None:
        raise RuntimeError(
            "no digest line (rc=0)\n--- stdout ---\n{0}\n--- stderr ---\n{1}".format(
                p.stdout[-800:], p.stderr[-1500:]))
    return digest


if __name__ == "__main__":
    payload_main(sys.argv)
