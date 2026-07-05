"""R0 acceptance: the runloom.stats() gauge surface must BALANCE.

The reliability program (docs/dev/RELIABILITY_PROGRAM.md R0) turns every
internal population into a stats() counter so a soak sees a leak as a rising
number.  This test is the in-suite proof that the gauges actually DO that:
drive each major workload shape many times and assert every live-population
gauge returns to its post-warmup baseline.  It is the tools/leak_check.py
oracle -- but read through the PUBLIC gauge surface instead of gc.get_objects,
so a regression that leaks a g struct / a coro stack / a netpoll parker / an
aio task / a monkey socket-timeout entry shows up as a non-balancing gauge.

Gauge semantics (see stats.py / the C accessors):
  * LIVE gauges return to ~baseline at quiescence (mn_pending_total,
    netpoll_parked, coro_stack_live, py_aio_tasks_live, py_sock_timeouts, ...).
  * HIGH-WATER / POOL gauges (g_structs_total, coro_depot_pooled) do NOT fall
    to zero -- warmup establishes their peak; subsequent identical iterations
    must not push them HIGHER.  Both are covered by "end - post_warmup_baseline
    <= tol".
  * ODOMETERS (mn_completed_total, stale_arm_heals) only rise -- excluded.
"""
import gc
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest

import runloom
import runloom.monkey
runloom.monkey.patch()
import runloom_c


# Cumulative odometers + context values that legitimately move; excluded from
# the balance assertion (a soak tracks their SLOPE, not their balance).
_ODOMETERS = {
    "mn_completed_total", "stale_arm_heals", "completed", "stack_completed",
    "running", "stack_calibrated", "stack_painting", "ready_capacity",
    "stack_size_default", "hubs_live", "py_offload_workers",
}
# Per-gauge slack above 0.  Pools/high-waters and the diagnostics call itself
# allocate a little; a genuine per-iteration leak dwarfs these.
_TOL = {
    "g_structs_total": 4,      # slab high-water; may tick up a couple under churn
    "coro_depot_pooled": 8,    # cross-hub stack depot retains freed stacks
    "coro_stack_live": 2,      # the driver + a transient
    "stack_hwm": 1 << 62,      # a max, not a population -- ignore movement
    "py_dns_cache_entries": 1 << 62,  # no eviction by design -- not asserted here
}
_DEFAULT_TOL = 2


def _drive(fn):
    box = []
    runloom_c.fiber(lambda: box.append(fn()), stack_size=8 << 20)
    runloom_c.run()
    return box[0] if box else None


def _numeric_stats():
    gc.collect()
    return {k: v for k, v in runloom.stats().items() if isinstance(v, int)}


def _check_balance(workload, name, iters=25, warmup=5):
    for _ in range(warmup):
        _drive(workload)
    base = _numeric_stats()
    for _ in range(iters):
        _drive(workload)
    end = _numeric_stats()

    leaks = []
    for key, bval in base.items():
        if key in _ODOMETERS:
            continue
        eval_ = end.get(key, bval)
        growth = eval_ - bval
        tol = _TOL.get(key, _DEFAULT_TOL)
        if growth > tol:
            leaks.append("%s: %d -> %d (+%d > tol %d)"
                         % (key, bval, eval_, growth, tol))
    assert not leaks, (
        "[%s] gauge(s) did not balance over %d iters:\n  %s"
        % (name, iters, "\n  ".join(leaks)))


# ----- workload shapes ------------------------------------------------------
def _wl_spawn_join():
    # A burst of child fibers that each return -- exercises the g slab +
    # coro-stack acquire/release balance.
    import runloom_c as rc
    done = [0]
    def child():
        done[0] += 1
    for _ in range(32):
        rc.fiber(child)
    # yield until the children have run (mn_run joins; single-thread run drains)
    for _ in range(4):
        rc.sched_yield()


def _wl_chan_pipeline():
    # A producer -> consumer chan pipeline that PROVABLY fully drains: the
    # driver sends an exact count on an unbuffered channel (each send hands off
    # to the consumer), closes, then JOINS the consumer through a done channel
    # before returning.  No stranded fiber (which would leak a stack by design
    # and is a workload bug, not a runtime leak).  Exercises chan waiter
    # park/unpark + close-wakes-receiver.
    import runloom_c as rc
    ch = rc.Chan()
    done = rc.Chan()
    def consumer():
        n = 0
        while True:
            v, ok = ch.recv()
            if not ok:
                break
            n += 1
        done.send(n)
    rc.fiber(consumer)
    for i in range(40):
        ch.send(i)
    ch.close()
    total, _ = done.recv()   # join: blocks until the consumer has drained + closed
    assert total == 40


def _wl_timer_storm():
    # Many short sleeps -- exercises the sleep heap + timer parkers.
    import runloom_c as rc
    def sleeper():
        rc.sched_sleep(0.001)
    for _ in range(16):
        rc.fiber(sleeper)
    rc.sched_sleep(0.02)


def _wl_socketpair_echo():
    # A local echo round -- exercises netpoll parkers + fd arm cache + the
    # monkey socket-timeout side table.  Must fully close both ends so the
    # arm cache + _SOCK_TIMEOUTS return to baseline.
    import socket
    a, b = socket.socketpair()
    try:
        a.sendall(b"ping")
        assert b.recv(4) == b"ping"
        b.sendall(b"pong")
        assert a.recv(4) == b"pong"
    finally:
        a.close()
        b.close()


def _wl_offload():
    # A blocking-pool offload round -- exercises the offload backend + parker.
    def blocking():
        return sum(range(100))
    runloom.blocking(blocking)


@pytest.mark.parametrize("wl,name", [
    (_wl_spawn_join, "spawn_join"),
    (_wl_chan_pipeline, "chan_pipeline"),
    (_wl_timer_storm, "timer_storm"),
    (_wl_socketpair_echo, "socketpair_echo"),
    (_wl_offload, "offload"),
])
def test_gauge_balance(wl, name):
    _check_balance(wl, name)


def test_stats_has_r0_gauges():
    # The R0 keys must all be present and integer-typed.
    s = _drive(lambda: runloom.stats())
    for key in ("g_structs_total", "coro_stack_live", "mn_pending_total",
                "netpoll_deadline_heap", "netpoll_fd_armed", "stale_arm_heals",
                "blockpool_inflight", "iouring_inflight",
                "py_aio_tasks_live", "py_dns_cache_entries", "py_sock_timeouts"):
        assert key in s, "missing R0 gauge %r" % key
        assert isinstance(s[key], int), "%r not int: %r" % (key, s[key])
