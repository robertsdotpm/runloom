"""Multithreaded M:N scheduler tests (the path the rest of tests/ never
exercises).

pygo's channels, select, sleep, and work-stealing all behave differently
once goroutines are spread across N OS-thread hubs on free-threaded
CPython -- that is where cross-hub channel handoff, work-stealing, and
the per-g wake machinery actually run in parallel.  None of the other
test modules call mn_init/mn_go/mn_run, so this fills that gap.

Each test runs its workload in a FRESH SUBPROCESS, for two reasons:
  1. mn_init/mn_fini install process-global hub threads; a clean process
     per test avoids cross-test contamination of that global state.
  2. the M:N scheduler can SIGSEGV under some contended Python workloads
     (see test_contended_select_xfail); a subprocess turns that into a
     clean test failure instead of taking down the whole pytest run.

Subprocesses run with PYTHON_GIL=0 so hubs genuinely run in parallel
(true free-threading) -- the condition under which the scheduler's
concurrency is actually tested.
"""
import os
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Only meaningful on a free-threaded ("t") build; mn still runs under the
# GIL but serially, so the parallel behaviour we care about needs 3.13t+.
FREE_THREADED = bool(getattr(sys, "_is_gil_enabled", None)) or "t" in getattr(
    sys, "abiflags", "")


def run_mn(code, timeout=60):
    """Run an M:N snippet in a fresh free-threaded subprocess.
    Returns (returncode, stdout, stderr).  The snippet should print
    'PASS' on success."""
    preamble = (
        "import sys; sys.path.insert(0, %r)\n"
        "import pygo_core\n" % os.path.join(REPO, "src")
    )
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"          # force GIL off: real parallel hubs
    env["PYGO_GIL"] = "0"
    try:
        p = subprocess.run(
            [sys.executable, "-c", preamble + code],
            cwd=REPO, env=env, timeout=timeout,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except subprocess.TimeoutExpired as e:
        # A wedge/lost-wake hang: surface as rc=124 (like coreutils timeout)
        # rather than letting TimeoutExpired escape as a test error.
        out = e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or "")
        err = e.stderr.decode() if isinstance(e.stderr, bytes) else (e.stderr or "")
        return 124, out, err + "\n[run_mn: timed out after {0}s]".format(timeout)
    return p.returncode, p.stdout, p.stderr


def assert_pass(code, timeout=60):
    rc, out, err = run_mn(code, timeout=timeout)
    assert rc == 0 and "PASS" in out, (
        "rc={0}\n--- stdout ---\n{1}\n--- stderr ---\n{2}".format(rc, out, err))
    return out


# ---------------------------------------------------------------------------
# Stable workloads -- these must always pass.
# ---------------------------------------------------------------------------
def test_spawn_drain_compute():
    """Many pure-compute goroutines across hubs; every one runs to
    completion and the scheduler drains cleanly."""
    assert_pass(r"""
N = 2000
results = []
def mk(k):
    def w():
        s = 0
        for i in range(50):
            s += i
            pygo_core.sched_yield_classic()
        results.append((k, s))
    return w
pygo_core.mn_init(4)
for k in range(N):
    pygo_core.mn_go(mk(k))
pygo_core.mn_run()
pygo_core.mn_fini()
assert len(results) == N, len(results)
assert all(s == 1225 for _, s in results)
assert pygo_core._self_check(0) == 0
print("PASS", len(results))
""")


def test_channel_fanin_with_close():
    """N producers -> shared buffered channel -> M range-recv consumers,
    coordinator closes after producers finish.  Conservation of every
    token across all consumers, run repeatedly to shake out races."""
    assert_pass(r"""
def fanin(nprod, ncons, per):
    work = pygo_core.Chan(16)
    done = pygo_core.Chan(nprod)
    res  = pygo_core.Chan(ncons)
    def prod(pid):
        def r():
            for s in range(per):
                work.send(pid * 1000 + s)
            done.send(1)
        return r
    def closer():
        for _ in range(nprod):
            done.recv()
        work.close()
    def cons():
        c = t = 0
        for v in work:
            c += 1; t += v
        res.send((c, t))
    pygo_core.mn_init(4)
    for _ in range(ncons): pygo_core.mn_go(cons)
    for p in range(nprod): pygo_core.mn_go(prod(p))
    pygo_core.mn_go(closer)
    pygo_core.mn_run()
    tc = tt = 0
    for _ in range(ncons):
        g = res.try_recv()
        if g is None: break
        (c, t), ok = g
        tc += c; tt += t
    pygo_core.mn_fini()
    exp_c = nprod * per
    exp_t = sum(pid*1000 + s for pid in range(nprod) for s in range(per))
    assert (tc, tt) == (exp_c, exp_t), (tc, tt, exp_c, exp_t)
    assert pygo_core._self_check(0) == 0

for _ in range(20):
    fanin(6, 6, 30)
print("PASS")
""")


def test_pingpong_unbuffered_pairs():
    """Many pairs of goroutines bouncing values through unbuffered
    channels (direct cross-hub handoff, no buffer)."""
    assert_pass(r"""
NPAIRS, ROUNDS = 16, 100
totals = pygo_core.Chan(NPAIRS)
def pair(pid):
    a = pygo_core.Chan(); b = pygo_core.Chan()
    def pinger():
        acc = 0
        for i in range(ROUNDS):
            a.send(i)
            v, _ = b.recv()
            acc += v
        totals.send(acc)
    def ponger():
        for _ in range(ROUNDS):
            v, _ = a.recv()
            b.send(v * 2)
    return pinger, ponger
pygo_core.mn_init(4)
for pid in range(NPAIRS):
    pi, po = pair(pid)
    pygo_core.mn_go(pi); pygo_core.mn_go(po)
pygo_core.mn_run()
got = 0
for _ in range(NPAIRS):
    g = totals.try_recv()
    if g is None: break
    v, ok = g; got += v
pygo_core.mn_fini()
exp = NPAIRS * sum(i * 2 for i in range(ROUNDS))
assert got == exp, (got, exp)
assert pygo_core._self_check(0) == 0
print("PASS", got)
""")


def test_self_check_clean_after_mn():
    """The runtime self-check (parker lists / fd buckets / counters) must
    report zero violations after an M:N run."""
    assert_pass(r"""
def w():
    ch = pygo_core.Chan(1)
    ch.send(1); ch.recv()
pygo_core.mn_init(3)
for _ in range(500): pygo_core.mn_go(w)
pygo_core.mn_run()
pygo_core.mn_fini()
v = pygo_core._self_check(1)
assert v == 0, v
print("PASS")
""")


# ---------------------------------------------------------------------------
# Known-broken workload, isolated + documented.
# ---------------------------------------------------------------------------
@pytest.mark.xfail(reason="OPEN BUG, minimally isolated: a busy-poll loop on "
                          "select(..., default=True) under M:N loses a wake when "
                          "the channel's sender parks (buffer full) -> the sender "
                          "is stranded (hang) or its value is corrupted (SIGSEGV). "
                          "Scheduling-sequence-dependent; reproduces single-hub.",
                   strict=False)
def test_select_default_busy_poll_xfail():
    """Minimal deterministic repro of the open M:N select bug.

    A single consumer busy-polls `select([(\"recv\", b)], default=True)` on a
    buffered channel while a feeder fills it.  When the buffer fills the
    feeder parks as a sender; the select path's wake of that parked sender
    is lost, so the feeder is stranded (mn_run never returns -> hang) or,
    timing-dependent, the received value is corrupted (-> SIGSEGV in the
    result-tuple unpack).

    Tightly isolated (proven by elimination):
      * blocking `select([(\"recv\", b)])`           -> CLEAN
      * busy-poll `b.try_recv()` (no select)          -> CLEAN
      * `select(default=True)` + buffer big enough     -> CLEAN
        that the sender never parks
    Only `select(default=True)` + a sender that parks fails.  So the defect
    is in select's interaction with the parked-sender wake under the M:N
    hub scheduler -- NOT in select_try_each's value handling (textually
    identical to the working chan_recv_locked), NOT preemption/handoff
    (crashes with both off), NOT cross-hub (single hub repros).

    Run in a subprocess so the hang/crash can't take down the suite.
    When the M:N select-default wake path is fixed this flips to xpass.
    """
    rc, out, err = run_mn(r"""
N = 400
b = pygo_core.Chan(8)          # small buffer -> feeder parks as sender when full
res = pygo_core.Chan(1)
def feeder():
    for i in range(N):
        b.send(1000 + i)       # ints > 256 are real refcounted objects
def cons():
    c = 0
    while c < N:
        idx, (v, ok) = pygo_core.select([("recv", b)], default=True)
        if idx == -1:
            pygo_core.sched_yield_classic(); continue
        if ok: c += 1
    res.send(c)
pygo_core.mn_init(2)
pygo_core.mn_go(cons); pygo_core.mn_go(feeder)
pygo_core.mn_run()
g = res.try_recv()
pygo_core.mn_fini()
assert g is not None and g[0] == N, g
print("PASS")
""", timeout=10)
    assert rc == 0 and "PASS" in out, (
        "select(default=True) busy-poll under M:N failed: rc={0} "
        "(124=hang/stranded sender; 139=SIGSEGV/value corruption)\n"
        "stderr tail:\n{1}".format(rc, err[-800:]))
