"""Multithreaded M:N scheduler tests (the path the rest of tests/ never
exercises).

runloom's channels, select, sleep, and work-stealing all behave differently
once fibers are spread across N OS-thread hubs on free-threaded
CPython -- that is where cross-hub channel handoff, work-stealing, and
the per-g wake machinery actually run in parallel.  None of the other
test modules call mn_init/mn_go/mn_run, so this fills that gap.

Each test runs its workload in a FRESH SUBPROCESS, for two reasons:
  1. mn_init/mn_fini install process-global hub threads; a clean process
     per test avoids cross-test contamination of that global state.
  2. a regression in the scheduler/channels can SIGSEGV or hang under
     contended parallel workloads (see test_select_close_conservation,
     which guards a fixed select()+close() crash/loss arc); a subprocess
     turns that into a clean test failure, not a dead pytest run.

Subprocesses run with PYTHON_GIL=0 so hubs genuinely run in parallel
(true free-threading) -- the condition under which the scheduler's
concurrency is actually tested.
"""
import os
import subprocess
import sys

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
        "import runloom_c\n" % os.path.join(REPO, "src")
    )
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"          # force GIL off: real parallel hubs
    env["RUNLOOM_GIL"] = "0"
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
    """Many pure-compute fibers across hubs; every one runs to
    completion and the scheduler drains cleanly."""
    assert_pass(r"""
N = 2000
results = []
def mk(k):
    def w():
        s = 0
        for i in range(50):
            s += i
            runloom_c.sched_yield_classic()
        results.append((k, s))
    return w
runloom_c.mn_init(4)
for k in range(N):
    runloom_c.mn_fiber(mk(k))
runloom_c.mn_run()
runloom_c.mn_fini()
assert len(results) == N, len(results)
assert all(s == 1225 for _, s in results)
assert runloom_c._self_check(0) == 0
print("PASS", len(results))
""")


def test_sched_yield_no_starvation_multi_hub():
    """sched_yield must not let a busy-spinning fiber starve a never-run
    one.  Before the fairness fix, hub_main served the ready ring (yielded gs)
    strictly before the deque (fresh gs), AND the yield fast path spun without
    returning to hub_main to drain sub_head -- so a fiber looping
    `while not flag: sched_yield()` waiting for a fiber spawned afterwards
    (or onto an already-busy hub) would spin forever.  Fixed by bounding both
    (hub_main forces a deque turn after N ready services; the fast path forces
    a real yield after N trivial spins).  A hang here = subprocess timeout."""
    assert_pass(r"""
flag = [False]
def waiter():
    while not flag[0]:
        runloom_c.sched_yield()
def setter():
    flag[0] = True
runloom_c.mn_init(4)
for _ in range(8):          # 8 spinners monopolizing the hubs first ...
    runloom_c.mn_fiber(waiter)
runloom_c.mn_fiber(setter)     # ... then the never-run fiber they wait on
runloom_c.mn_run()
runloom_c.mn_fini()
assert flag[0]
assert runloom_c._self_check(0) == 0
print("PASS")
""", timeout=20)


def test_sched_yield_no_starvation_single_hub():
    """H=1 deterministic case: a spinner popped before a fresh setter sitting
    on the deque must still let the setter run.  The ready-ring-before-deque
    order used to re-serve the spinner forever; the hub_main deque-turn bound
    breaks it."""
    assert_pass(r"""
flag = [False]
def waiter():
    while not flag[0]:
        runloom_c.sched_yield()
def setter():
    flag[0] = True
runloom_c.mn_init(1)
runloom_c.mn_fiber(setter)     # fresh -> goes to the deque
runloom_c.mn_fiber(waiter)     # popped first (deque LIFO); must yield to setter
runloom_c.mn_run()
runloom_c.mn_fini()
assert flag[0]
print("PASS")
""", timeout=20)


def test_channel_fanin_with_close():
    """N producers -> shared buffered channel -> M range-recv consumers,
    coordinator closes after producers finish.  Conservation of every
    token across all consumers, run repeatedly to shake out races."""
    assert_pass(r"""
def fanin(nprod, ncons, per):
    work = runloom_c.Chan(16)
    done = runloom_c.Chan(nprod)
    res  = runloom_c.Chan(ncons)
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
    runloom_c.mn_init(4)
    for _ in range(ncons): runloom_c.mn_fiber(cons)
    for p in range(nprod): runloom_c.mn_fiber(prod(p))
    runloom_c.mn_fiber(closer)
    runloom_c.mn_run()
    tc = tt = 0
    for _ in range(ncons):
        g = res.try_recv()
        if g is None: break
        (c, t), ok = g
        tc += c; tt += t
    runloom_c.mn_fini()
    exp_c = nprod * per
    exp_t = sum(pid*1000 + s for pid in range(nprod) for s in range(per))
    assert (tc, tt) == (exp_c, exp_t), (tc, tt, exp_c, exp_t)
    assert runloom_c._self_check(0) == 0

for _ in range(20):
    fanin(6, 6, 30)
print("PASS")
""")


def test_pingpong_unbuffered_pairs():
    """Many pairs of fibers bouncing values through unbuffered
    channels (direct cross-hub handoff, no buffer)."""
    assert_pass(r"""
NPAIRS, ROUNDS = 16, 100
totals = runloom_c.Chan(NPAIRS)
def pair(pid):
    a = runloom_c.Chan(); b = runloom_c.Chan()
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
runloom_c.mn_init(4)
for pid in range(NPAIRS):
    pi, po = pair(pid)
    runloom_c.mn_fiber(pi); runloom_c.mn_fiber(po)
runloom_c.mn_run()
got = 0
for _ in range(NPAIRS):
    g = totals.try_recv()
    if g is None: break
    v, ok = g; got += v
runloom_c.mn_fini()
exp = NPAIRS * sum(i * 2 for i in range(ROUNDS))
assert got == exp, (got, exp)
assert runloom_c._self_check(0) == 0
print("PASS", got)
""")


def test_self_check_clean_after_mn():
    """The runtime self-check (parker lists / fd buckets / counters) must
    report zero violations after an M:N run."""
    assert_pass(r"""
def w():
    ch = runloom_c.Chan(1)
    ch.send(1); ch.recv()
runloom_c.mn_init(3)
for _ in range(500): runloom_c.mn_fiber(w)
runloom_c.mn_run()
runloom_c.mn_fini()
v = runloom_c._self_check(1)
assert v == 0, v
print("PASS")
""")


# ---------------------------------------------------------------------------
# Known-broken workload, isolated + documented.
# ---------------------------------------------------------------------------
def test_select_close_conservation():
    """Regression for the M:N blocking-select + close() bug arc.

    Many consumers block in select() across a shared channel pool while
    producers push a known multiset of tokens and a coordinator closes
    the channels.  Every token must be received exactly once.

    Before the fix this manifested three ways under M:N parallelism:
      * SIGSEGV -- close() woke a parked select RECV with value==NULL,
        which m_select put into the (value, ok) result tuple, crashing the
        caller's `v, ok = ...` unpack (now: close-wake returns Py_None).
      * Lost tokens / consumer death -- the Phase-2 install ABORT path
        returned select_try_each()'s result directly, which can be -1 when
        the ready channel raced away; a bare -1 from a *blocking* select
        became PyLong(-1) and the caller's unpack raised TypeError, killing
        the consumer (now: the abort retries instead of returning -1).
      * Lost tokens -- if a delivery fired the select on an earlier case
        while a later case looked ready, the abort evicted the waiter
        holding the just-delivered value (now: a lost claim-CAS breaks to
        the park and returns the delivered value).

    Several iterations to shake out the timing-dependent races.  Runs in a
    subprocess so any residual crash fails one test, not the suite.
    """
    assert_pass(r"""
def experiment(nhubs, nprod, ncons, nchan, per):
    chans = [runloom_c.Chan([0, 1, 8][i % 3]) for i in range(nchan)]
    done = runloom_c.Chan(nprod)
    res  = runloom_c.Chan(ncons)
    def producer(pid):
        def run():
            for s in range(per):
                chans[(pid + s) % nchan].send(pid * 1000 + s)
                if (s & 7) == 0:
                    runloom_c.sched_yield_classic()
            done.send(pid)
        return run
    def closer():
        for _ in range(nprod):
            done.recv()
        for ch in chans:
            ch.close()
    def consumer():
        got = 0
        total = 0
        closed = [False] * nchan
        while not all(closed):
            cases = [("recv", chans[i]) for i in range(nchan) if not closed[i]]
            if not cases:
                break
            idx, (v, ok) = runloom_c.select(cases)   # BLOCKING select
            live = [i for i in range(nchan) if not closed[i]]
            if ok:
                got += 1; total += v
            else:
                closed[live[idx]] = True
        res.send((got, total))
    runloom_c.mn_init(nhubs)
    for _ in range(ncons): runloom_c.mn_fiber(consumer)
    for p in range(nprod):  runloom_c.mn_fiber(producer(p))
    runloom_c.mn_fiber(closer)
    runloom_c.mn_run()
    rc = rt = 0
    for _ in range(ncons):
        g = res.try_recv()
        if g is None: break
        (c, t), ok = g
        rc += c; rt += t
    runloom_c.mn_fini()
    exp_c = nprod * per
    exp_t = sum(pid * 1000 + s for pid in range(nprod) for s in range(per))
    assert (rc, rt) == (exp_c, exp_t), (nhubs, nprod, ncons, nchan, per, rc, rt, exp_c, exp_t)
    assert runloom_c._self_check(0) == 0

for it in range(60):
    experiment(nhubs=2 + it % 3, nprod=5, ncons=8, nchan=1 + it % 3, per=24)
print("PASS")
""", timeout=60)


def test_select_send_and_recv_mixed():
    """select with BOTH send and recv cases under M:N: a relay fiber
    selects to either forward into `out` (send) or pull from `in_` (recv);
    conservation across a chain of relays.  Exercises the select SEND
    install/abort/park path that the RECV tests don't."""
    assert_pass(r"""
def run_once(nrelay, n):
    src  = runloom_c.Chan(0)
    dst  = runloom_c.Chan(0)
    res  = runloom_c.Chan(1)
    def producer():
        for i in range(n):
            src.send(1000 + i)
        src.close()
    def relay(a, b):
        # forward every value a->b via blocking select on a recv + b send
        def run():
            pending = None
            while True:
                if pending is None:
                    cases = [("recv", a)]
                else:
                    cases = [("recv", a), ("send", b, pending)]
                idx, payload = runloom_c.select(cases)
                if idx == 0:
                    v, ok = payload
                    if not ok:
                        if pending is not None:
                            b.send(pending)
                        b.close(); return
                    if pending is None:
                        pending = v
                    else:
                        # got a new value while still holding one: drain old first
                        b.send(pending); pending = v
                else:
                    pending = None   # the send fired
        return run
    def consumer():
        c = 0; t = 0
        for v in dst:
            c += 1; t += v
        res.send((c, t))
    runloom_c.mn_init(3)
    runloom_c.mn_fiber(consumer)
    runloom_c.mn_fiber(relay(src, dst))
    runloom_c.mn_fiber(producer)
    runloom_c.mn_run()
    g = res.try_recv()
    runloom_c.mn_fini()
    (c, t), ok = g
    assert (c, t) == (n, sum(1000 + i for i in range(n))), (c, t, n)
    assert runloom_c._self_check(0) == 0

for _ in range(20):
    run_once(2, 25)
print("PASS")
""")


def test_select_concurrent_send_close():
    """The edge case verify/spin/select_close.pml flagged: producers send
    WITHOUT a done-barrier while a closer closes concurrently, so values
    can be buffered in the Phase-1->install window and close can race the
    select's abort/park.  We don't assert exact conservation here (a send
    racing close legitimately raises), only that it never crashes, hangs,
    or trips the self-check -- the model proves the no-loss/no-NULL parts.
    """
    assert_pass(r"""
def experiment(it):
    nchan = 1 + it % 3
    chans = [runloom_c.Chan([0, 1, 4][i % 3]) for i in range(nchan)]
    res = runloom_c.Chan(6)
    ncons = 4 + it % 3
    def prod(pid):
        def r():
            for s in range(20):
                try:
                    chans[(pid + s) % nchan].send(pid * 100 + s)
                except ValueError:
                    pass   # send on closed channel: expected under the race
        return r
    def closer():
        runloom_c.sched_yield_classic()
        for ch in chans:
            ch.close()
    def cons():
        c = 0
        closed = [False] * nchan
        while not all(closed):
            cs = [("recv", chans[i]) for i in range(nchan) if not closed[i]]
            if not cs:
                break
            idx, (v, ok) = runloom_c.select(cs)
            live = [i for i in range(nchan) if not closed[i]]
            if ok:
                c += 1
            else:
                closed[live[idx]] = True
        res.send(c)
    runloom_c.mn_init(3)
    for _ in range(ncons): runloom_c.mn_fiber(cons)
    for p in range(3): runloom_c.mn_fiber(prod(p))
    runloom_c.mn_fiber(closer)
    runloom_c.mn_run()
    runloom_c.mn_fini()
    assert runloom_c._self_check(0) == 0

for it in range(80):
    experiment(it)
print("PASS")
""", timeout=60)
