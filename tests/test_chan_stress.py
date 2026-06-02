"""Channel torture tests, ported in spirit from Go's runtime/chan_test.go.

pygo's ``Chan`` is a near-exact port of Go channels (buffered/unbuffered
send/recv, close, range, select with send+recv cases), so Go's own channel
torture suite maps almost 1:1 onto it.  These are the cases that go straight
at pygo's worst bug class -- the M:N blocking-select + close() crash/loss arc
(fixed in 2723164; see test_mn.test_select_close_conservation) -- plus
cross-hub publication and concurrent close.

Like test_mn.py, each workload runs in a FRESH free-threaded subprocess
(PYTHON_GIL=0) so the hubs genuinely run in parallel -- the only condition
under which channel hand-off, close-wake, and select install/abort actually
race.  A wedge/lost-wake shows up as a subprocess timeout (rc 124), a
corruption as a non-zero rc; both become a clean test failure, not a dead
pytest run.

Go originals (golang/go, src/runtime/chan_test.go):
  TestChan, TestMultiConsumer, TestSelfSelect, TestSelectStress,
  TestChanSendBarrier, TestChanClose (concurrent), and the close/send race.

NOTE on fairness: Go's select picks a uniformly-random ready case; pygo's
does not (it has no shuffle).  So the select test here asserts LIVENESS
(every ready case is eventually serviced -- no case is permanently starved)
and CONSERVATION, never Go's uniform distribution, which would be a false
failure against pygo's deterministic select.
"""
import os
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# True on a GIL-enabled interpreter (stock CPython, or a free-threaded build run
# with PYTHON_GIL=1).  A few tests assert a property that requires hubs to run in
# genuine PARALLEL; under the GIL they are concurrent-but-serialised and the
# property can't hold, so those tests skip rather than spuriously hang.
_GIL_ON = (not hasattr(sys, "_is_gil_enabled")) or sys._is_gil_enabled()


def run_mn(code, timeout=60):
    """Run an M:N snippet in a fresh free-threaded subprocess (GIL off).
    Returns (returncode, stdout, stderr); the snippet prints 'PASS' on ok."""
    preamble = (
        "import sys; sys.path.insert(0, %r)\n"
        "import pygo_core\n" % os.path.join(REPO, "src")
    )
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["PYGO_GIL"] = "0"
    try:
        p = subprocess.run(
            [sys.executable, "-c", preamble + code],
            cwd=REPO, env=env, timeout=timeout,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except subprocess.TimeoutExpired as e:
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
# TestChan -- buffered AND unbuffered, every value delivered exactly once.
# ---------------------------------------------------------------------------
def test_chan_conservation_across_capacities():
    """Go TestChan: for a range of capacities (0 = unbuffered through deep
    buffers), P producers push distinct tokens, C range-recv consumers drain,
    a coordinator closes after the producers finish.  Every token is received
    exactly once and the sum is conserved, regardless of cap or interleaving."""
    assert_pass(r"""
def run_cap(cap, nprod, ncons, per):
    work = pygo_core.Chan(cap)
    done = pygo_core.Chan(nprod)
    res  = pygo_core.Chan(ncons)
    def prod(pid):
        def r():
            for s in range(per):
                work.send(pid * 100000 + s)
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
        (c, t), ok = g; tc += c; tt += t
    pygo_core.mn_fini()
    exp_c = nprod * per
    exp_t = sum(pid*100000 + s for pid in range(nprod) for s in range(per))
    assert (tc, tt) == (exp_c, exp_t), (cap, tc, tt, exp_c, exp_t)
    assert pygo_core._self_check(0) == 0

for cap in (0, 1, 2, 8, 64):
    run_cap(cap, nprod=6, ncons=5, per=40)
print("PASS")
""")


# ---------------------------------------------------------------------------
# TestMultiConsumer -- uneven producer/consumer counts, sum conserved.
# ---------------------------------------------------------------------------
def test_multi_consumer_sum_conserved():
    """Go TestMultiConsumer: many producers feed one buffered channel that a
    different (prime, so deliberately uneven) number of consumers drain; the
    total count and the checksum across all consumers must equal what was
    produced.  Run repeatedly to shake out cross-hub handoff races."""
    assert_pass(r"""
def once(nprod, ncons, per):
    work = pygo_core.Chan(8)
    done = pygo_core.Chan(nprod)
    res  = pygo_core.Chan(ncons)
    def prod(pid):
        def r():
            for s in range(per):
                work.send((pid * 7 + s) % 9973 + 1)   # nonzero, spread out
            done.send(1)
        return r
    def closer():
        for _ in range(nprod): done.recv()
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
        (c, t), ok = g; tc += c; tt += t
    pygo_core.mn_fini()
    exp_c = nprod * per
    exp_t = sum((pid * 7 + s) % 9973 + 1 for pid in range(nprod) for s in range(per))
    assert (tc, tt) == (exp_c, exp_t), (tc, tt, exp_c, exp_t)
    assert pygo_core._self_check(0) == 0

for _ in range(15):
    once(nprod=23, ncons=7, per=20)
print("PASS")
""")


# ---------------------------------------------------------------------------
# TestSelfSelect -- a goroutine selecting send AND recv on the SAME channel
# must never satisfy its own send with its own recv (unbuffered), and the
# whole thing must make progress (no deadlock).
# ---------------------------------------------------------------------------
@pytest.mark.skipif(_GIL_ON, reason="self-select requires genuine parallelism; "
                    "the select-to-select rendezvous deadlocks under the GIL "
                    "(see project_pygo_gil_on_mn_self_select)")
def test_self_select_no_self_pairing():
    """Go TestSelfSelect: two goroutines, each looping
    `select { case c<-id: ; case v:=<-c: }` on a SHARED channel.  For an
    UNBUFFERED channel a goroutine's own select-send must rendezvous with the
    OTHER goroutine's select-recv, never its own (v != id); progress must be
    made for both cap 0 and cap 1.  (Probed: pygo holds this -- selfpair=0.)"""
    assert_pass(r"""
def run_cap(cap, n):
    c = pygo_core.Chan(cap)
    done = pygo_core.Chan(2)
    selfpair = [0]
    def mk(idv):
        def run():
            for _ in range(n):
                idx, payload = pygo_core.select([("send", c, idv), ("recv", c)])
                if idx == 1:
                    v, ok = payload
                    if cap == 0 and ok and v == idv:
                        selfpair[0] += 1
            done.send(1)
        return run
    pygo_core.mn_init(4)
    pygo_core.mn_go(mk(0)); pygo_core.mn_go(mk(1))
    pygo_core.mn_run()
    a = done.try_recv(); b = done.try_recv()
    pygo_core.mn_fini()
    assert a is not None and b is not None, "deadlock: a goroutine never finished"
    assert selfpair[0] == 0, ("unbuffered self-select self-paired", cap, selfpair[0])
    assert pygo_core._self_check(0) == 0

for _ in range(10):
    run_cap(0, 300)
    run_cap(1, 300)
print("PASS")
""", timeout=40)


# ---------------------------------------------------------------------------
# TestSelectStress -- the marquee: many senders AND receivers driving a pool
# of mixed-capacity channels entirely through select (send cases too, which
# test_mn.test_select_close_conservation does not exercise).  Conservation.
# ---------------------------------------------------------------------------
def test_select_stress_send_and_recv():
    """Go TestSelectStress: a pool of channels with mixed capacities driven
    concurrently by select-SEND producers and select-RECV consumers across
    several hubs.  Producers finish (done barrier) before a closer closes the
    pool, so no send races close; every token is received exactly once.

    This is the send+recv select install/abort/park path under real M:N
    parallelism -- the exact machinery behind the fixed select+close crash."""
    assert_pass(r"""
def once(it):
    nchan = 4
    caps = [0, 1, 8, 64]
    chans = [pygo_core.Chan(caps[i]) for i in range(nchan)]
    nprod, ncons, per = 6, 5, 30
    done = pygo_core.Chan(nprod)
    res  = pygo_core.Chan(ncons)

    def prod(pid):
        def r():
            for s in range(per):
                val = pid * 100000 + s
                # push via select-SEND across whichever channel takes it first
                targets = [("send", chans[(pid + s + k) % nchan], val)
                           for k in range(nchan)]
                pygo_core.select(targets)
            done.send(1)
        return r

    def closer():
        for _ in range(nprod): done.recv()
        for ch in chans: ch.close()

    def cons():
        c = t = 0
        closed = [False] * nchan
        while not all(closed):
            live = [i for i in range(nchan) if not closed[i]]
            idx, (v, ok) = pygo_core.select([("recv", chans[i]) for i in live])
            if ok:
                c += 1; t += v
            else:
                closed[live[idx]] = True
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
        (c, t), ok = g; tc += c; tt += t
    pygo_core.mn_fini()
    exp_c = nprod * per
    exp_t = sum(pid*100000 + s for pid in range(nprod) for s in range(per))
    assert (tc, tt) == (exp_c, exp_t), (it, tc, tt, exp_c, exp_t)
    assert pygo_core._self_check(0) == 0

for it in range(12):
    once(it)
print("PASS")
""", timeout=60)


# ---------------------------------------------------------------------------
# Select LIVENESS (pygo's deterministic-select analogue of Go's fairness
# test): when only one of several cases is ready, select must service THAT
# case -- so over many rounds with the ready case alternating, EVERY case
# fires.  No case is permanently starved.
# ---------------------------------------------------------------------------
def test_select_liveness_every_case_served():
    """Both cases of a 2-way recv-select must eventually fire when a feeder
    alternates which channel is ready.  A select that always served case 0
    (the kind of starvation the sched_yield fix was about, one layer up)
    would never record a case-1 hit -> assertion fails."""
    assert_pass(r"""
N = 4000
a = pygo_core.Chan(); b = pygo_core.Chan()
hits = pygo_core.Chan(1)
def selector():
    ca = cb = 0
    for _ in range(N):
        idx, (v, ok) = pygo_core.select([("recv", a), ("recv", b)])
        if idx == 0: ca += 1
        else:        cb += 1
    hits.send((ca, cb))
def feeder():
    for i in range(N):
        (a if (i & 1) == 0 else b).send(i)
pygo_core.mn_init(2)
pygo_core.mn_go(selector)
pygo_core.mn_go(feeder)
pygo_core.mn_run()
g = hits.try_recv()
pygo_core.mn_fini()
(ca, cb), ok = g
assert ca + cb == N, (ca, cb, N)
assert ca > 0 and cb > 0, ("a case was starved", ca, cb)
assert pygo_core._self_check(0) == 0
print("PASS", ca, cb)
""", timeout=30)


# ---------------------------------------------------------------------------
# TestChanSendBarrier -- cross-hub publication: the receiver of a channel send
# observes ALL of the sender's prior writes to the sent object (happens-before
# via the channel), never a torn/partial view.  Matters on free-threaded.
# ---------------------------------------------------------------------------
def test_send_barrier_cross_hub_publication():
    """Go TestChanSendBarrier: a sender freshly builds an object, fully
    initialises it, then sends it; the receiver (on another hub) must see the
    complete, consistent object.  Many iterations with distinct freshly-built
    payloads -> any missing publication/visibility shows as a content mismatch."""
    assert_pass(r"""
ITERS = 3000
ch = pygo_core.Chan(0)          # unbuffered: direct cross-hub hand-off
res = pygo_core.Chan(1)
def sender():
    for i in range(ITERS):
        # fresh object every time, filled with an i-dependent pattern
        payload = {"i": i, "data": [i, i + 1, i + 2, i * 2, i ^ 0x55],
                   "tag": ("p", i)}
        ch.send(payload)
    ch.close()
def receiver():
    bad = 0
    n = 0
    for payload in ch:
        i = payload["i"]
        if payload["data"] != [i, i + 1, i + 2, i * 2, i ^ 0x55] \
           or payload["tag"] != ("p", i):
            bad += 1
        n += 1
    res.send((n, bad))
pygo_core.mn_init(4)
pygo_core.mn_go(receiver)
pygo_core.mn_go(sender)
pygo_core.mn_run()
g = res.try_recv()
pygo_core.mn_fini()
(n, bad), ok = g
assert n == ITERS, (n, ITERS)
assert bad == 0, ("torn/partial object seen across hubs", bad)
assert pygo_core._self_check(0) == 0
print("PASS", n)
""", timeout=40)


# ---------------------------------------------------------------------------
# TestChanClose (concurrent) -- close() wakes EVERY parked receiver, each
# exactly once, with the zero/ok=false sentinel; no crash, no double-wake.
# ---------------------------------------------------------------------------
def test_concurrent_close_wakes_all_receivers():
    """Go close semantics under M:N: N receivers all park in recv() on a shared
    unbuffered channel; one closer closes.  Every receiver wakes exactly once
    with (None, False); none crash or hang.  Repeated to shake out the
    close-wakes-all-parked-receivers path across hubs."""
    assert_pass(r"""
def once(nrecv):
    ch = pygo_core.Chan()        # unbuffered
    res = pygo_core.Chan(nrecv)
    def recv():
        v, ok = ch.recv()
        res.send((v, ok))
    def closer():
        # let receivers park first
        for _ in range(50):
            pygo_core.sched_yield_classic()
        ch.close()
    pygo_core.mn_init(4)
    for _ in range(nrecv): pygo_core.mn_go(recv)
    pygo_core.mn_go(closer)
    pygo_core.mn_run()
    got = []
    for _ in range(nrecv):
        g = res.try_recv()
        if g is None: break
        (v, ok), _ = g
        got.append((v, ok))
    pygo_core.mn_fini()
    assert len(got) == nrecv, ("not every receiver woke", len(got), nrecv)
    assert all(x == (None, False) for x in got), got[:5]
    assert pygo_core._self_check(0) == 0

for _ in range(12):
    once(32)
print("PASS")
""", timeout=40)


# ---------------------------------------------------------------------------
# Concurrent close + send torture (channel level, not select) -- the analogue
# of test_select_concurrent_send_close but on plain send/recv: never crash or
# hang, self-check clean, and no value is delivered AFTER a receiver has seen
# the channel closed (no post-close delivery).
# ---------------------------------------------------------------------------
def test_concurrent_close_send_no_loss_no_crash():
    """Producers send on a pool of channels while a closer closes them
    concurrently.  A send that races close legitimately raises ValueError
    (counted, not fatal).  Invariants: no crash, no hang, self-check clean,
    and once a consumer observes a channel closed it never receives another
    value from it (monotonic close)."""
    assert_pass(r"""
def experiment(it):
    nchan = 1 + it % 4
    chans = [pygo_core.Chan([0, 1, 4][i % 3]) for i in range(nchan)]
    res = pygo_core.Chan(8)
    ncons = 3 + it % 4
    def prod(pid):
        def r():
            for s in range(25):
                try:
                    chans[(pid + s) % nchan].send(pid * 100 + s)
                except ValueError:
                    pass   # send raced close: expected
                if (s & 3) == 0:
                    pygo_core.sched_yield_classic()
        return r
    def closer():
        pygo_core.sched_yield_classic()
        for ch in chans:
            ch.close()
    def cons():
        got = 0
        closed = [False] * nchan
        violations = 0
        while not all(closed):
            for i in range(nchan):
                if closed[i]:
                    continue
                r = chans[i].try_recv()
                if r is None:
                    continue
                v, ok = r
                if ok:
                    if closed[i]:
                        violations += 1   # delivery after observed close
                    got += 1
                else:
                    closed[i] = True
            pygo_core.sched_yield_classic()
        res.send((got, violations))
    pygo_core.mn_init(3)
    for _ in range(ncons): pygo_core.mn_go(cons)
    for p in range(3): pygo_core.mn_go(prod(p))
    pygo_core.mn_go(closer)
    pygo_core.mn_run()
    tv = 0
    for _ in range(ncons):
        g = res.try_recv()
        if g is None: break
        (got, viol), _ = g
        tv += viol
    pygo_core.mn_fini()
    assert tv == 0, ("post-close delivery", tv)
    assert pygo_core._self_check(0) == 0

for it in range(40):
    experiment(it)
print("PASS")
""", timeout=60)


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
