"""Coverage-driven adversarial tests for netpoll_wake_iouring.c.inc.

This fragment holds the *direct* (in-process, syscall-free) wake/cancel paths
for fibers parked in runloom_netpoll_wait_fd, plus the io_uring eventfd/ring
registration glue that bridges a hub's io_uring CQ into the shared epoll pump.
The normal corpus exercises only the single-thread, single-pool variants of the
cancel/unpark paths and the *first-64* ring registrations; this file drives the
M:N (multi-pool) variants and the over-capacity ring path.

Each test names the uncovered line(s) it targets and HOW it makes the gate true.

Targets that are GENUINELY unreachable through the public API (a lost-wake
structural backstop, defensive epoll-failure folds on a modern kernel, and a
dead exported symbol with no in-tree caller) are documented in the module
docstring of the central report, not contorted into tests here.

  * runloom_netpoll_lock_g_pool (L60-64)        -- the re-validating pool lock
    used by cancel_g + unpark_many; reached on every cancel/unpark of an
    actually-parked g.  Single-thread tests stay in ONE pool (p->hub == NULL);
    only an M:N hub g lands in a per-hub pool, so these tests run under
    runloom.run(N>=2).
  * runloom_netpoll_cancel_g (L100)             -- the commit-CAS claim loop.
  * runloom_netpoll_unpark_many (L146, L156)    -- the cheap not-parked miss
    AND the per-g claim loop, batched, under M:N.
  * runloom_netpoll_cancel_fd (L218)            -- the per-fd-bucket walk's
    claim loop, under M:N (one parker per pool).
  * runloom_netpoll_add_iouring_ring ENOSPC (L419-422) -- >64 hubs each
    register a per-hub io_uring ring; the 65th+ overflow RUNLOOM_IOURING_RINGS_MAX
    and must fall back to the epoll pump without losing I/O correctness.
"""
import os
import socket
import sys
import tempfile
import time

import pytest

import runloom
import runloom_c as rc
from runloom.sync import WaitGroup
from adv_util import hang_guard, needs_free_threading

READ = 1
UNPARKED = 0x10000000        # RUNLOOM_NETPOLL_UNPARKED sentinel
CANCELLED = rc.WAIT_FD_CANCELLED
FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

pytestmark = pytest.mark.skipif(
    not FT or rc.netpoll_backend() != "epoll",
    reason="M:N multi-pool + epoll-iouring coverage needs GIL-disabled epoll build")


def _drop(fd):
    try:
        rc.netpoll_unregister(fd)
    except OSError:
        pass


def _wait_until_parked(n, timeout=6.0):
    """Block (cooperatively) until at least ``n`` fibers have COMMITTED to a
    netpoll park, then return True; return False on timeout.

    This replaces a fixed ``sched_sleep`` guess.  These tests spawn N waiter
    fibers that ``wait_fd``-park, then wake them and assert all N were
    parked.  Under load a fiber may not have been scheduled to reach its
    park yet when a fixed sleep elapses, and the runtime then CORRECTLY reports
    that not-yet-parked g as "missed" / leaves it un-stranded (the documented
    edge-before-park contract of unpark_many/cancel_fd) -- so the fixed-sleep
    timing assumption, NOT the runtime, was the ~1% flake (proven: 8/640 under
    16x parallel load, all the same `missed`-barrier assertion).  The
    process-global ``netpoll_parked`` count is exact here (only this test's
    waiters park), so polling it is a deterministic barrier.

    The waiters park with a LONG (20s) wait_fd timeout -- comfortably above this
    barrier -- so an early parker can never self-time-out-and-vanish while the
    barrier is still waiting for the last straggler to commit (that inversion,
    5s waiter vs 10s barrier, briefly produced an 'all-missed' failure)."""
    deadline = time.monotonic() + timeout
    while rc.stats().get("netpoll_parked", 0) < n and time.monotonic() < deadline:
        rc.sched_sleep(0.002)
    return rc.stats().get("netpoll_parked", 0) >= n


def _wait_until(pred, timeout=6.0):
    """Cooperatively poll ``pred`` until true (resume/record completed) or
    timeout -- the post-wake analogue of _wait_until_parked, so a slow resume
    under load can't spuriously fail a completion-count assertion either."""
    deadline = time.monotonic() + timeout
    while not pred() and time.monotonic() < deadline:
        rc.sched_sleep(0.002)
    return pred()


# --------------------------------------------------------------------------
# cancel_g under M:N -> runloom_netpoll_lock_g_pool (L60-64) + the claim CAS
# loop (L100).  The g is parked on a HUB pool (p->hub != NULL), which the
# single-thread cancel tests never reach: there the parker lives in the
# default pool and lock_g_pool's pool lookup is the degenerate single-pool
# case.  We assert the parked fiber is woken with the CANCELLED sentinel
# (exactly-once: the cancel claimed the commit, set ready_out=CANCELLED, and
# re-queued the committed g), proving cancel_g ran its claim+unlink+wake body.
# --------------------------------------------------------------------------
def test_mn_cancel_g_wakes_parked_fiber_cancelled():
    res = {}
    def main():
        a, b = socket.socketpair()
        a.setblocking(False); b.setblocking(False)
        hold = {}
        def parker():
            hold["g"] = rc.current_g()
            res["rv"] = rc.wait_fd(a.fileno(), READ, 20000)
        rc.mn_fiber(parker)
        _wait_until_parked(1)                 # parker has COMMITTED its park
        res["woke"] = hold["g"].cancel_wait_fd()
        _wait_until(lambda: "rv" in res)      # woken g resumed + recorded
        _drop(a.fileno()); a.close(); b.close()
    with hang_guard(20, "mn cancel_g"):
        runloom.run(4, main)
    assert res.get("woke") is True, "cancel_g did not claim the parked g"
    assert res.get("rv") == CANCELLED, (
        "wait_fd returned %r, expected CANCELLED -- cancel_g must set "
        "ready_out=CANCELLED on the claimed parker" % res.get("rv"))


# --------------------------------------------------------------------------
# cancel_g on a fiber that is NOT parked (running) -> the cheap NULL-parker
# bail (cancel_g L96) returns 0 without touching lock_g_pool.  We assert it
# returns False (no wake) AND the fiber keeps running to completion -- a false
# "woke" would mean cancel_g spuriously claimed a non-parked g.
# --------------------------------------------------------------------------
def test_mn_cancel_g_on_running_fiber_is_noop():
    res = {}
    def main():
        me = rc.current_g()
        res["woke"] = me.cancel_wait_fd()     # main is RUNNING, parker == NULL
        res["after"] = True                   # proves we kept running
    with hang_guard(15, "mn cancel_g noop"):
        runloom.run(2, main)
    assert res.get("woke") is False
    assert res.get("after") is True


# --------------------------------------------------------------------------
# unpark_many under M:N -> the per-g claim loop (L156) for each parked g AND
# the cheap not-parked miss (L146) for the running main g.  Batched: one call
# wakes 24 parked fibers (each gets the UNPARKED sentinel) and reports the one
# running handle as missed.  Drives lock_g_pool (L60-64) per parked g on its
# hub pool.  Asserts the missed index, the UNPARKED sentinel on every woken
# fiber, and that nothing was lost (all 24 resumed).
# --------------------------------------------------------------------------
def test_mn_unpark_many_batch_wakes_all_and_reports_running_missed():
    N = 24
    res = {}
    def main():
        a, b = socket.socketpair()
        a.setblocking(False); b.setblocking(False)
        handles = [None] * N
        rvs = [None] * N
        def waiter(i):
            handles[i] = rc.current_g()
            rvs[i] = rc.wait_fd(a.fileno(), READ, 20000)
        for i in range(N):
            rc.mn_fiber(lambda i=i: waiter(i))
        _wait_until_parked(N)                  # all N COMMITTED to park
        me = rc.current_g()                    # running -> must be reported missed
        missed = rc.unpark_many(handles + [me])
        _wait_until(lambda: all(v is not None for v in rvs))  # woken fibers resumed
        res["missed"] = missed
        res["rvs"] = sorted(set(rvs))
        res["woke_n"] = sum(1 for v in rvs if v is not None)
        _drop(a.fileno()); a.close(); b.close()
    with hang_guard(25, "mn unpark_many batch"):
        runloom.run(4, main)
    assert res.get("missed") == [N], (
        "expected only the running main handle (index %d) missed, got %r"
        % (N, res.get("missed")))
    assert res.get("rvs") == [UNPARKED], (
        "every directly-woken fiber must see UNPARKED, got %r" % res.get("rvs"))
    assert res.get("woke_n") == N, "lost a fiber: only %r/%d resumed" % (
        res.get("woke_n"), N)


# --------------------------------------------------------------------------
# cancel_fd under M:N -> the per-fd-bucket walk's claim loop (L218) and the
# unlink+wake of each matching parker.  Multiple fibers parked on the SAME fd
# (so the by_fd bucket has a chain) are all woken CANCELLED by ONE cancel_fd
# (the socket-close hook's analogue).  Under M:N each parker may live in a
# different hub pool, so cancel_fd's outer loop walks every pool's bucket.
# Asserts all parkers were cancelled (none stranded), which only holds if the
# claim CAS loop ran for each.
# --------------------------------------------------------------------------
def test_mn_cancel_fd_wakes_all_parkers_on_one_fd():
    N = 16
    res = {}
    def main():
        a, b = socket.socketpair()
        a.setblocking(False); b.setblocking(False)
        rvs = [None] * N
        def waiter(i):
            rvs[i] = rc.wait_fd(a.fileno(), READ, 20000)
        for i in range(N):
            rc.mn_fiber(lambda i=i: waiter(i))
        _wait_until_parked(N)                  # all N parked on a.fileno()
        rc.netpoll_cancel_fd(a.fileno())       # one call wakes the whole bucket
        _wait_until(lambda: all(v is not None for v in rvs))
        res["rvs"] = sorted(set(rvs))
        res["n"] = sum(1 for v in rvs if v == CANCELLED)
        _drop(a.fileno()); a.close(); b.close()
    with hang_guard(25, "mn cancel_fd bucket"):
        runloom.run(4, main)
    assert res.get("n") == N, (
        "cancel_fd left %r/%d parkers stranded on the closed fd"
        % (res.get("n"), N))
    assert res.get("rvs") == [CANCELLED], (
        "cancel_fd must wake every bucket parker CANCELLED, got %r"
        % res.get("rvs"))


# --------------------------------------------------------------------------
# cancel_fd with fd < 0 -> the early return guard (cancel_fd L194).  A real
# no-op that must not crash / must not touch any pool.  Asserts the run still
# completes and structures stay consistent.
# --------------------------------------------------------------------------
def test_mn_cancel_fd_negative_fd_is_noop():
    res = {}
    def main():
        rc.netpoll_cancel_fd(-1)               # guarded early return
        res["ok"] = True
    with hang_guard(15, "mn cancel_fd neg"):
        runloom.run(2, main)
    assert res.get("ok") is True


# --------------------------------------------------------------------------
# add_iouring_ring ENOSPC overflow (L419-422).  Each M:N hub creates its own
# per-hub io_uring ring and registers its eventfd via add_iouring_ring; the
# table is capped at RUNLOOM_IOURING_RINGS_MAX (64).  With 100 hubs, hubs
# 65-100 overflow the table -> add_iouring_ring sets errno=ENOSPC and returns
# -1, and hub_main DISCARDS that hub's ring and falls back to the epoll pump
# for its file I/O.  We run real io_uring file I/O on every hub: correctness
# (all reads return the written bytes) proves the ENOSPC-discarded hubs
# degraded gracefully instead of losing CQE delivery.  This is the ONLY
# realistic trigger for the over-capacity branch (you cannot register the same
# eventfd twice -- distinct hubs hold distinct fds -- so the idempotent-update
# branch is not reachable, but the table-full branch is, by simply exceeding 64
# concurrent hub rings).
# --------------------------------------------------------------------------
@pytest.mark.skipif(not rc.iouring_available(), reason="io_uring not available")
def test_iouring_ring_table_overflow_falls_back_gracefully():
    HUBS = 100                                 # > RUNLOOM_IOURING_RINGS_MAX (64)
    N = 200
    res = {}
    payload = b"runloom-cov-" + b"q" * 500
    def main():
        wg = WaitGroup(); wg.add(N)
        ok = bytearray(N)
        def w(i):
            try:
                fd, path = tempfile.mkstemp()
                rc.file_write(fd, payload, 0)
                buf = bytearray(len(payload))
                if (rc.file_read(fd, buf, len(payload), 0) == len(payload)
                        and bytes(buf) == payload):
                    ok[i] = 1
                os.close(fd); os.unlink(path)
            finally:
                wg.done()
        for i in range(N):
            rc.mn_fiber(lambda i=i: w(i))
        wg.wait()
        res["ok"] = sum(ok)
    with hang_guard(90, "iouring ring overflow"):
        runloom.run(HUBS, main)
    assert res.get("ok") == N, (
        "%d/%d file ops lost -- a ring-table-overflow hub failed to fall back "
        "to the epoll pump" % (N - (res.get("ok") or 0), N))


# --------------------------------------------------------------------------
# Combined stress: many parked fibers across many hubs, woken by a MIX of
# direct unpark_many (in-process claim) + cancel_fd (close-hook) + cancel_g
# (per-handle cancel), all racing the netpoll pump.  Every parker must drain
# (no leaked netpoll parker -- the conftest invariant) and _self_check must
# stay 0.  This re-runs the lock_g_pool / claim-CAS loops (L60-64, L100, L156,
# L218) under genuine M:N contention where a parker's pool lookup matters, and
# exercises the exactly-once arbitration between the three wakers and the pump.
# --------------------------------------------------------------------------
def test_mn_mixed_wakers_drain_cleanly_under_contention():
    res = {}
    def main():
        groups = []
        # group A: cancelled via per-handle cancel_g
        a1, b1 = socket.socketpair(); a1.setblocking(False); b1.setblocking(False)
        # group B: woken via cancel_fd (whole bucket)
        a2, b2 = socket.socketpair(); a2.setblocking(False); b2.setblocking(False)
        # group C: woken via unpark_many (direct batch)
        a3, b3 = socket.socketpair(); a3.setblocking(False); b3.setblocking(False)
        groups = [(a1, b1), (a2, b2), (a3, b3)]
        handlesA = []
        handlesC = []
        rvA = [None] * 8
        rvB = [None] * 8
        rvC = [None] * 8
        def wA(i):
            handlesA.append(rc.current_g()); rvA[i] = rc.wait_fd(a1.fileno(), READ, 20000)
        def wB(i):
            rvB[i] = rc.wait_fd(a2.fileno(), READ, 20000)
        def wC(i):
            handlesC.append(rc.current_g()); rvC[i] = rc.wait_fd(a3.fileno(), READ, 20000)
        for i in range(8):
            rc.mn_fiber(lambda i=i: wA(i))
            rc.mn_fiber(lambda i=i: wB(i))
            rc.mn_fiber(lambda i=i: wC(i))
        _wait_until_parked(24)                 # all 24 COMMITTED to park
        for h in list(handlesA):
            h.cancel_wait_fd()
        rc.netpoll_cancel_fd(a2.fileno())
        rc.unpark_many(list(handlesC))
        _wait_until(lambda: all(v is not None for v in rvA + rvB + rvC))
        res["A"] = sum(1 for v in rvA if v == CANCELLED)
        res["B"] = sum(1 for v in rvB if v == CANCELLED)
        res["C"] = sum(1 for v in rvC if v == UNPARKED)
        for a, b in groups:
            _drop(a.fileno()); a.close(); b.close()
    with hang_guard(30, "mn mixed wakers"):
        runloom.run(6, main)
    # Every fiber in every group woke via its intended path -- no lost wakes,
    # no cross-path mis-claim.
    assert res.get("A") == 8, "cancel_g group: %r/8 cancelled" % res.get("A")
    assert res.get("B") == 8, "cancel_fd group: %r/8 cancelled" % res.get("B")
    assert res.get("C") == 8, "unpark_many group: %r/8 unparked" % res.get("C")


# --------------------------------------------------------------------------
# REGRESSION: many fibers parking on the SAME fd across MULTIPLE hubs must all
# commit their park -- none lost.  This is the lost-park bug fixed in
# netpoll_register.c.inc: per-hub epoll registered the shared fd into ONE hub's
# epoll and ping-ponged it (DEL+re-ADD) on every cross-hub re-park; under
# concurrent parks on one fd that churn raced the park/unlink bookkeeping into a
# LOST PARK -- a committed-PARKED g dropped from its by_fd bucket, never woken,
# never timed out, then freed (docs/dev/repro/LOST_PARK_FINDING.md).  Repro'd
# ~1-2% single-copy pre-fix; PERHUB_EPOLL=0 or one-fd-per-waiter was clean.  The
# fix moves a multi-pool fd ONCE to the shared epoll (nested in every hub's
# pump), so all hubs deliver its events with no migration churn.  Oracle: with
# the deterministic netpoll_parked barrier, ALL N must reach the park (the
# barrier returns True); a lost park leaves it < N and the assert fires.
# --------------------------------------------------------------------------
def test_mn_many_parkers_one_fd_across_hubs_none_lost():
    N = 32
    res = {}
    def main():
        a, b = socket.socketpair()
        a.setblocking(False); b.setblocking(False)
        rvs = [None] * N
        def w(i):
            rvs[i] = rc.wait_fd(a.fileno(), READ, 20000)
        for i in range(N):
            rc.mn_fiber(lambda i=i: w(i))
        res["all_parked"] = _wait_until_parked(N)        # the lost-park oracle
        res["parked_count"] = rc.stats().get("netpoll_parked", 0)
        rc.netpoll_cancel_fd(a.fileno())                 # wake the whole bucket
        _wait_until(lambda: all(v is not None for v in rvs))
        res["woke"] = sum(1 for v in rvs if v == CANCELLED)
        _drop(a.fileno()); a.close(); b.close()
    with hang_guard(30, "mn many parkers one fd"):
        runloom.run(4, main)
    assert res.get("all_parked") is True, (
        "lost park: only %r/%d fibers committed to a netpoll park (a parker was "
        "dropped from its bucket -- the per-hub-epoll same-fd-across-pools "
        "regression)" % (res.get("parked_count"), N))
    assert res.get("woke") == N, (
        "cancel_fd woke %r/%d -- a parker on the shared fd was unreachable"
        % (res.get("woke"), N))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
