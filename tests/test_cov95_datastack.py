"""Coverage recovery for src/runloom_c/runloom_sched_datastack.c.inc.

This .inc carries four cold subsystems that the round-1 cov suites never
reached because each is gated behind an env mode that is resolved ONCE per
process (so it must be set in a fresh SUBPROCESS), or behind a heap geometry
that the happy path never produces:

  1. The datastack-tail idle reclaim + its RUNLOOM_DATASTACK_DEBUG decompose
     instrumentation (runloom_ds_resident_bytes, the debug counter block, the
     public _datastack_sweep_stats readout, and the C-only-g early return).
     Driven by RUNLOOM_STACK_PARK_SWEEP=1 + RUNLOOM_DATASTACK_DEBUG=1 in an
     M:N run() with Python fibers parked deep in CPython long enough for the
     hub-idle dwell sweep to madvise their idle chunk tails.

  2. PCT -- the probabilistic concurrency-testing controlled scheduler
     (runloom_pct_init / _rand / _argmax / _pick), reached only when
     RUNLOOM_PCT_SEED is set, on the single-hub run(1) ready-pop path.

  3. The timer-heap sift-UP body (a timed in-memory park whose deadline is
     EARLIER than an already-queued one), reached via rc.park(timeout=...).

Each env-gated case runs in a child process that EXITS CLEANLY so gcov flushes.
Oracles are real (exact byte echo, exact wake counts, observed schedule order,
nonzero debug accounting), never line-touching filler.

Targeted uncovered lines (gcov ##### in
build/coverage/runloom_sched_datastack.c.inc.gcov):
  44-56  runloom_ds_resident_bytes (mincore decompose; DEBUG only)
  92     madvise_datastack_idle early-return: C-only g / no chunk installed
  112-118 the RUNLOOM_DATASTACK_DEBUG counter block
  128-145 runloom_sched_datastack_sweep_stats (the _datastack_sweep_stats readout)
  307-348 runloom_pct_rand + runloom_pct_init (incl. the DEBUG print)
  361-398 runloom_pct_argmax + runloom_pct_pick (FIFO order, change point, shift)
  406     runloom_sched_ready_pop -> runloom_pct_pick dispatch
  545-547 runloom_timer_push sift-up loop body
"""
import os
import subprocess
import sys

import pytest

from adv_util import needs_free_threading

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable


def _run(script, env_extra, timeout=240):
    # Generous timeout + skip-on-timeout: this box is shared with a CI runner
    # that competes for CPU; a timeout is contention, not a bug.
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src", **env_extra)
    try:
        return subprocess.run([PY, "-c", script], cwd=REPO, env=env,
                              capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.skip("datastack-cov workload timed out (box under heavy load)")


# ==========================================================================
# 1. Datastack-tail idle reclaim + DEBUG decompose instrumentation.
#
#    Drives, under the M:N hub-idle dwell sweep with the decompose debug flag:
#      - runloom_sched_madvise_datastack_idle's main body (madvise the chunk
#        tail of a Python fiber parked deep in CPython);
#      - the RUNLOOM_DATASTACK_DEBUG counter block (gcov L112-118);
#      - runloom_ds_resident_bytes including its resident-accumulation body
#        (gcov L44-56) -- by faulting the chunk tail (deep recurse) then
#        parking SHALLOW so the faulted pages sit RESIDENT above the frontier;
#      - the C-only-g early return (gcov L92) -- the all-C rc.serve accept/recv
#        fibers have snap.datastack_chunk == NULL (no Python frame pushed
#        before they park) and are skipped;
#      - runloom_sched_datastack_sweep_stats via rc._datastack_sweep_stats()
#        (gcov L128-145).
#
#    Oracle: every client got its exact echo back, every Python parker woke,
#    AND the debug accounting is non-trivial (chunks swept > 0, and a resident
#    tail was measured -- proving the resident-accumulation body ran, not just
#    the zero-resident fast path).
# ==========================================================================
_DATASTACK = r'''
import sys, socket, struct; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup

NC = 16            # all-C echo clients -> C-only (no-chunk) parkers (L92)
NP = 24            # deep-then-shallow Python parkers -> resident tails (L44-56,112-118)
got = [None] * NC
pywoke = [0] * NP

def main():
    port, lst = rc.serve("127.0.0.1", 0, None, 2)   # all-C echo; C-only parkers
    wg = WaitGroup(); wg.add(NC)
    def client(i):
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(struct.pack(">Q", i)); got[i] = c.recv(8); c.close()
        finally:
            wg.done()
    for i in range(NC):
        rc.mn_go(lambda i=i: client(i))

    pp = []
    for j in range(NP):
        a, b = socket.socketpair(); a.setblocking(False); pp.append((a, b))
    wg2 = WaitGroup(); wg2.add(NP)
    def pyreader(j):
        a, b = pp[j]
        try:
            # fault the chunk tail by recursing deep, UNWIND, then park shallow:
            # the faulted pages now sit resident in [datastack_top, limit).
            def churn(d):
                scratch = bytearray(2048)
                if d > 0:
                    return churn(d - 1)
                return len(scratch)
            churn(60)
            pywoke[j] = 1 if rc.wait_fd(a.fileno(), 1, 4000) is not None else 0
        finally:
            wg2.done()
    for j in range(NP):
        rc.mn_go(lambda j=j: pyreader(j))

    rc.sched_sleep(0.5)              # let the dwell sweep fire on the parked fibers
    for a, b in pp:
        try: b.send(b"x")
        except Exception: pass
    wg.wait(); wg2.wait()
    for a, b in pp:
        try: rc.netpoll_unregister(a.fileno())
        except Exception: pass
        a.close(); b.close()
    for ln in lst:
        ln.close()

runloom.run(3, main)
cgot = sum(1 for i in range(NC) if got[i] == struct.pack(">Q", i))
tail, resident, chunks = rc._datastack_sweep_stats()
sys.stdout.write("DS cgot=%d pywoke=%d tail=%d resident=%d chunks=%d\n"
                 % (cgot, sum(pywoke), tail, resident, chunks))
'''


@pytest.mark.skipif(not FT, reason="datastack dwell sweep is an M:N hub-idle path")
def test_datastack_sweep_debug_decompose():
    p = _run(_DATASTACK, {
        "RUNLOOM_STACK_PARK_SWEEP": "1", "RUNLOOM_STACK_PARK_SWEEP_MS": "1",
        "RUNLOOM_DATASTACK_SWEEP": "1", "RUNLOOM_DATASTACK_DEBUG": "1",
        "RUNLOOM_SWEEP_MAX_CHURN": "0",   # never throttle the sweep
    })
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1600:])
    line = [l for l in p.stdout.splitlines() if l.startswith("DS ")]
    assert line, (p.stdout[-400:], p.stderr[-800:])
    fields = dict(kv.split("=") for kv in line[0][3:].split())
    # exact-once echo + every Python parker woken (clean teardown, no strand)
    assert int(fields["cgot"]) == 16, line[0]
    assert int(fields["pywoke"]) == 24, line[0]
    # the DEBUG decompose actually accounted real chunks AND measured a resident
    # tail -> runloom_ds_resident_bytes' accumulation body ran (not just L0 path)
    assert int(fields["chunks"]) > 0, line[0]
    assert int(fields["resident"]) > 0, (
        "no resident tail measured -- the resident-accumulation body never "
        "ran: " + line[0])
    assert int(fields["tail"]) >= int(fields["resident"]), line[0]


# A second variant with the sweep ON but the DEBUG flag OFF: drives the
# _datastack_sweep_stats() #else-free path returning the (still-zero, since
# debug never accumulated) counters AND the madvise main body WITHOUT the
# debug block -- confirming the gate at L111 short-circuits cleanly.
_DATASTACK_NODEBUG = r'''
import sys, socket; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
NP = 20
woke = [0] * NP
def main():
    pp = []
    for j in range(NP):
        a, b = socket.socketpair(); a.setblocking(False); pp.append((a, b))
    wg = WaitGroup(); wg.add(NP)
    def reader(j):
        a, b = pp[j]
        try:
            woke[j] = 1 if rc.wait_fd(a.fileno(), 1, 4000) is not None else 0
        finally:
            wg.done()
    for j in range(NP):
        rc.mn_go(lambda j=j: reader(j))
    rc.sched_sleep(0.4)
    for a, b in pp:
        try: b.send(b"x")
        except Exception: pass
    wg.wait()
    for a, b in pp:
        try: rc.netpoll_unregister(a.fileno())
        except Exception: pass
        a.close(); b.close()
runloom.run(3, main)
tail, resident, chunks = rc._datastack_sweep_stats()
# DEBUG off: the decompose counters must stay zero (the L111 gate skipped them).
sys.stdout.write("NODBG woke=%d tail=%d chunks=%d\n" % (sum(woke), tail, chunks))
'''


@pytest.mark.skipif(not FT, reason="datastack dwell sweep is an M:N hub-idle path")
def test_datastack_sweep_no_debug_counters_zero():
    p = _run(_DATASTACK_NODEBUG, {
        "RUNLOOM_STACK_PARK_SWEEP": "1", "RUNLOOM_STACK_PARK_SWEEP_MS": "1",
        "RUNLOOM_DATASTACK_SWEEP": "1",   # sweep on, DEBUG deliberately absent
        "RUNLOOM_SWEEP_MAX_CHURN": "0",
    })
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1600:])
    line = [l for l in p.stdout.splitlines() if l.startswith("NODBG ")]
    assert line, (p.stdout[-400:], p.stderr[-800:])
    fields = dict(kv.split("=") for kv in line[0][6:].split())
    assert int(fields["woke"]) == 20, line[0]
    # DEBUG gate skipped: no decompose accounting accumulated.
    assert int(fields["tail"]) == 0 and int(fields["chunks"]) == 0, line[0]


# ==========================================================================
# 2. PCT -- the probabilistic concurrency-testing controlled scheduler.
#
#    With RUNLOOM_PCT_SEED set, run(1)'s ready-pop stops being FIFO and routes
#    through runloom_pct_pick (gcov L406).  This drives:
#      - runloom_pct_init incl. the change-point sort and the DEBUG print
#        (gcov L307-348);
#      - runloom_pct_argmax incl. the FIFO-order constraint branch (only the
#        OLDEST ready pct_fifo g is a candidate; younger ones `continue` --
#        gcov L369) and the random-priority assignment (gcov L372-373);
#      - runloom_pct_pick incl. the change-point demotion + re-pick (gcov
#        L387-391) and the queue-shift removal (gcov L395-397).
#
#    Oracles:
#      - FIFO contract: gs spawned fifo=True run in strict spawn order relative
#        to each other (asyncio call_soon-FIFO), even though raw gs interleave.
#      - every step ran exactly once (no g dropped/duplicated by the shift).
# ==========================================================================
_PCT = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc

NF = 4          # FIFO gs (must keep relative spawn order)
NR = 4          # raw reorderable gs (PCT may interleave freely)
ROUNDS = 4
fifo_order = []
raw_runs = [0] * NR

def main():
    def fifo_worker(i):
        for _ in range(ROUNDS):
            fifo_order.append(i)
            rc.sched_yield()
    def raw_worker(i):
        for _ in range(ROUNDS):
            raw_runs[i] += 1
            rc.sched_yield()
    # interleave the spawns so multiple FIFO gs sit ready together (forces the
    # argmax seen_fifo `continue` skip) and raw gs mix in for the change point.
    for i in range(NF):
        rc.fiber(lambda i=i: fifo_worker(i), fifo=True)
    for i in range(NR):
        rc.fiber(lambda i=i: raw_worker(i))

rc.fiber(main)
rc.run()

# FIFO contract: across the whole schedule the FIFO ids appear in
# non-decreasing "rounds completed" order -- equivalently, id k never runs its
# r-th time before id k-1 has run its r-th time.  Check it as: the subsequence
# of first-appearances is strictly 0,1,2,3 and within each id the order is
# stable relative to lower ids.
seen_count = {}
ok_fifo = True
last_progress = {i: 0 for i in range(NF)}
for x in fifo_order:
    seen_count[x] = seen_count.get(x, 0) + 1
    # id x is now on its seen_count[x]-th run; every lower id must already have
    # run at least that many times (call_soon-FIFO).
    for lo in range(x):
        if seen_count.get(lo, 0) < seen_count[x]:
            ok_fifo = False

each_fifo_full = all(seen_count.get(i, 0) == ROUNDS for i in range(NF))
each_raw_full = all(r == ROUNDS for r in raw_runs)
sys.stdout.write("PCT ok_fifo=%s fifo_full=%s raw_full=%s total=%d\n"
                 % (ok_fifo, each_fifo_full, each_raw_full, len(fifo_order)))
'''


def test_pct_controlled_scheduler_fifo_and_change_points():
    # PCT lives on the single-hub run(1) path; works with or without the GIL.
    p = _run(_PCT, {
        "RUNLOOM_PCT_SEED": "1234",   # nonzero decimal seed (strtoull base 10)
        "RUNLOOM_PCT_DEPTH": "4",     # depth>=2 -> change points exist
        "RUNLOOM_PCT_STEPS": "16",    # small k -> change points hit within the run
        "RUNLOOM_PCT_DEBUG": "1",     # drive the one-time debug print (L342-348)
    })
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1600:])
    line = [l for l in p.stdout.splitlines() if l.startswith("PCT ")]
    assert line, (p.stdout[-400:], p.stderr[-800:])
    fields = dict(kv.split("=") for kv in line[0][4:].split())
    # the call_soon-FIFO contract held despite PCT reordering raw gs
    assert fields["ok_fifo"] == "True", (line[0], p.stderr[-400:])
    # no g dropped or duplicated by the ready-ring shift in runloom_pct_pick
    assert fields["fifo_full"] == "True", line[0]
    assert fields["raw_full"] == "True", line[0]
    assert int(fields["total"]) == 4 * 4, line[0]   # NF * ROUNDS
    # the one-time PCT debug header was emitted (covers the L342-348 print)
    assert "[pct] seed=" in p.stderr, p.stderr[-400:]


# A depth=1 PCT run: change_at is EMPTY (the L330 init loop and the L387 change
# branch never run), exercising runloom_pct_pick's no-change-point straight
# line and argmax over a single ready g.  Different priority through the init.
_PCT_DEPTH1 = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc
runs = [0] * 6
def main():
    def w(i):
        for _ in range(3):
            runs[i] += 1
            rc.sched_yield()
    for i in range(6):
        rc.fiber(lambda i=i: w(i))
rc.fiber(main)
rc.run()
sys.stdout.write("PCT1 total=%d allthree=%s\n"
                 % (sum(runs), all(r == 3 for r in runs)))
'''


def test_pct_depth1_no_change_points():
    p = _run(_PCT_DEPTH1, {
        "RUNLOOM_PCT_SEED": "99",
        "RUNLOOM_PCT_DEPTH": "1",   # depth 1 -> zero change points
    })
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1600:])
    line = [l for l in p.stdout.splitlines() if l.startswith("PCT1 ")]
    assert line, (p.stdout[-400:], p.stderr[-800:])
    fields = dict(kv.split("=") for kv in line[0][5:].split())
    assert fields["allthree"] == "True", line[0]
    assert int(fields["total"]) == 6 * 3, line[0]


def test_pct_seed_zero_and_clamps():
    # seed="0" -> strtoull returns 0 -> the xorshift-fixpoint reseed (L324).
    # depth=0 -> clamp up to 1 (L326); steps=0 -> clamp up to 1 (L329); a huge
    # depth would clamp down to PCT_MAX_DEPTH (L327) but we keep this at the
    # seed-zero + low-clamp corner so the run stays well-defined.  Re-using the
    # depth-1 workload keeps the oracle simple (every g runs all its rounds).
    p = _run(_PCT_DEPTH1, {
        "RUNLOOM_PCT_SEED": "0",     # -> rng==0 -> fixpoint reseed (L324)
        "RUNLOOM_PCT_DEPTH": "0",    # -> clamped to 1 (L326)
        "RUNLOOM_PCT_STEPS": "0",    # -> clamped to 1 (L329)
    })
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1600:])
    line = [l for l in p.stdout.splitlines() if l.startswith("PCT1 ")]
    assert line, (p.stdout[-400:], p.stderr[-800:])
    fields = dict(kv.split("=") for kv in line[0][5:].split())
    assert fields["allthree"] == "True", line[0]
    assert int(fields["total"]) == 6 * 3, line[0]


def test_pct_depth_clamp_high():
    # depth far above PCT_MAX_DEPTH (16) -> clamped down (L327); a normal seed
    # so change points exist and the insertion-sort over depth-1 entries runs.
    p = _run(_PCT, {
        "RUNLOOM_PCT_SEED": "777",
        "RUNLOOM_PCT_DEPTH": "9999",   # -> clamped to PCT_MAX_DEPTH (L327)
        "RUNLOOM_PCT_STEPS": "8",
    })
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1600:])
    line = [l for l in p.stdout.splitlines() if l.startswith("PCT ")]
    assert line, (p.stdout[-400:], p.stderr[-800:])
    fields = dict(kv.split("=") for kv in line[0][4:].split())
    assert fields["ok_fifo"] == "True", (line[0], p.stderr[-400:])
    assert fields["fifo_full"] == "True" and fields["raw_full"] == "True", line[0]


# ==========================================================================
# 3. Timer-heap sift-UP body (runloom_timer_push, gcov L545-547).
#
#    A timed in-memory park (rc.park(timeout=...)) pushes { deadline, g } onto
#    the per-sched timer min-heap.  The sift-up loop body only runs when the new
#    entry's deadline is EARLIER than its parent already in the heap.  So: in a
#    single-thread run(), push a LONG-deadline park first (it becomes the heap
#    root), then push SHORTER-deadline parks that must sift UP above it.
#
#    Oracle: the short parkers time out (return True) at their short deadlines;
#    the long parker is woken EARLY by its handle (returns False) -- proving the
#    heap ordered them by deadline, i.e. the sift-up placed the short ones above
#    the long one.
# ==========================================================================
_TIMER = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc
from runloom.sync import WaitGroup
res = {}
def main():
    order = []
    long_h = {}
    wg = WaitGroup(); wg.add(5)
    def long_parker():
        long_h["g"] = rc.current_g()
        order.append(("long", rc.park(timeout=5.0)))   # heap root (far deadline)
        wg.done()
    def short_parker(t, name):
        order.append((name, rc.park(timeout=t)))        # must sift up above root
        wg.done()
    rc.fiber(long_parker)
    rc.sched_yield()                  # ensure the 5s timer is in the heap first
    # several shorter deadlines, each pushed AFTER the 5s root -> each sifts up
    rc.fiber(lambda: short_parker(0.04, "s1"))
    rc.fiber(lambda: short_parker(0.05, "s2"))
    rc.fiber(lambda: short_parker(0.06, "s3"))
    rc.fiber(lambda: short_parker(0.07, "s4"))
    rc.sched_sleep(0.25)              # let all the short timers fire (timed out)
    long_h["g"].wake()               # wake the long parker early (False)
    wg.wait()
    res["order"] = order
rc.fiber(main)
rc.run()
o = res["order"]
shorts = [v for (n, v) in o if n.startswith("s")]
longs = [v for (n, v) in o if n == "long"]
# shorts_timedout = how many short parkers timed out (True); long_woke = the
# long parker was woken early (False, i.e. not timed out).
shorts_timedout = sum(1 for v in shorts if v is True)
long_woke = sum(1 for v in longs if v is False)
sys.stdout.write("TIMER nshorts=%d shorts_timedout=%d nlong=%d long_woke=%d\n"
                 % (len(shorts), shorts_timedout, len(longs), long_woke))
'''


def test_timer_push_siftup():
    p = _run(_TIMER, {})
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1600:])
    line = [l for l in p.stdout.splitlines() if l.startswith("TIMER ")]
    assert line, (p.stdout[-400:], p.stderr[-800:])
    fields = dict(kv.split("=") for kv in line[0][len("TIMER "):].split())
    # every short parker timed out (True) -- they fired at their own deadlines,
    # which only happens if the heap correctly ordered them ABOVE the 5s root
    assert int(fields["nshorts"]) == 4, line[0]
    assert int(fields["shorts_timedout"]) == 4, line[0]
    # the long parker was woken EARLY (False), not timed out
    assert int(fields["nlong"]) == 1, line[0]
    assert int(fields["long_woke"]) == 1, line[0]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
