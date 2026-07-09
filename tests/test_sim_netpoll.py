"""Slice 2 -- the deterministic simulated-I/O netpoll backend (RUNLOOM_SIM).

Exercises the REAL wait_fd path (park/commit FSM, deadline heap, drain_expired,
M:N-agnostic single-thread wake routing) with its deadline clock routed through
the single-thread logical clock, and the sim pump advancing that clock instead of
blocking a real poller.  So a wait_fd timeout becomes a function of logical time:
a one-hour logical wait completes in ~zero wall time (logical compression), many
timeouts fire in deadline order, and a mixed sched_sleep + wait_fd workload
interleaves on ONE clock.

RUNLOOM_SIM + RUNLOOM_LOGICAL_CLOCK are read once and cached in the extension, so
they are set before import and this whole file runs under sim (run_isolated gives
it its own subprocess).  See docs/dev/soak/SIM_IO_DST.md.
"""
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
os.environ["PYTHON_GIL"] = "0"
os.environ["RUNLOOM_SIM"] = "1"                 # sim on (implies the logical clock)
os.environ.setdefault("RUNLOOM_LOGICAL_CLOCK", "1")
import runloom_c  # noqa: E402

READ = 0x1


class _NeverReady(object):
    """A real pipe whose read end never becomes readable (write end held open,
    never written).  wait_fd parks on a real, epoll-able fd -- the sim pump just
    never polls it, so only the timeout ever fires."""

    def __init__(self):
        self.r, self.w = os.pipe()

    def close(self):
        for fd in (self.r, self.w):
            try:
                os.close(fd)
            except OSError:
                pass


class TestSimNetpollTimeout(unittest.TestCase):
    def test_single_timeout_is_instant(self):
        """A one-hour logical wait_fd timeout completes in ~zero WALL time."""
        p = _NeverReady()
        out = {}

        def waiter():
            out["r"] = runloom_c.wait_fd(p.r, READ, 3600 * 1000)   # 1 logical hour

        t0 = time.monotonic()
        runloom_c.fiber(waiter)
        runloom_c.run()
        elapsed = time.monotonic() - t0
        p.close()

        self.assertEqual(out.get("r"), 0, "wait_fd should return 0 (timeout)")
        self.assertLess(elapsed, 2.0,
                        "one logical HOUR took %.2fs wall -- logical clock did "
                        "not compress it (sim pump not advancing?)" % elapsed)

    def test_many_timeouts_fire_in_deadline_order(self):
        """K parkers on one fd with distinct timeouts wake in ascending order,
        and the whole thing is instant (max deadline, not the sum)."""
        p = _NeverReady()
        order = []
        # deliberately spawn OUT of deadline order to prove it's the heap, not
        # the spawn order, that decides wake order.
        timeouts_ms = [800, 100, 500, 200, 400, 50, 700]

        def waiter(ms):
            r = runloom_c.wait_fd(p.r, READ, ms)
            order.append((ms, r))

        t0 = time.monotonic()
        for ms in timeouts_ms:
            runloom_c.fiber(lambda ms=ms: waiter(ms))
        runloom_c.run()
        elapsed = time.monotonic() - t0
        p.close()

        self.assertEqual([r for _, r in order], [0] * len(timeouts_ms),
                         "every wait_fd should time out (return 0)")
        woke_ms = [ms for ms, _ in order]
        self.assertEqual(woke_ms, sorted(timeouts_ms),
                         "timeouts did not fire in deadline order: %r" % woke_ms)
        self.assertLess(elapsed, 2.0,
                        "sum of logical timeouts (%dms) took %.2fs wall -- not "
                        "compressed" % (sum(timeouts_ms), elapsed))

    def test_reproducible(self):
        """Same scenario -> identical wake order across runs (determinism)."""
        def run_once():
            p = _NeverReady()
            order = []
            tos = [300, 100, 400, 100, 200]      # includes a tie (two 100s)

            def waiter(i, ms):
                r = runloom_c.wait_fd(p.r, READ, ms)
                order.append((i, ms, r))

            for i, ms in enumerate(tos):
                runloom_c.fiber(lambda i=i, ms=ms: waiter(i, ms))
            runloom_c.run()
            p.close()
            return order

        a = run_once()
        b = run_once()
        self.assertEqual(a, b, "sim wait_fd wake order not reproducible")
        self.assertEqual([ms for _, ms, _ in a], sorted([300, 100, 400, 100, 200]))

    def test_mixed_sched_sleep_and_wait_fd_share_one_clock(self):
        """The load-bearing case: sched_sleep and wait_fd interleave on the SAME
        logical clock, so the sim pump must advance to min(sleeper, netpoll)."""
        p = _NeverReady()
        order = []

        def wf(label, ms):
            runloom_c.wait_fd(p.r, READ, ms)
            order.append(label)

        def sl(label, secs):
            runloom_c.sched_sleep(secs)
            order.append(label)

        t0 = time.monotonic()
        runloom_c.fiber(lambda: wf("wf_1000ms", 1000))
        runloom_c.fiber(lambda: sl("sleep_500ms", 0.5))
        runloom_c.fiber(lambda: wf("wf_200ms", 200))
        runloom_c.fiber(lambda: sl("sleep_50ms", 0.05))
        runloom_c.run()
        elapsed = time.monotonic() - t0
        p.close()

        self.assertEqual(order,
                         ["sleep_50ms", "wf_200ms", "sleep_500ms", "wf_1000ms"],
                         "mixed sched_sleep/wait_fd did not interleave by "
                         "logical deadline: %r" % order)
        self.assertLess(elapsed, 2.0, "mixed workload not compressed")


class TestSimSettledDeadlock(unittest.TestCase):
    def test_forever_park_terminates_not_hangs(self):
        """An UNTIMED wait_fd with no possible readiness is a settled deadlock:
        the sim pump must terminate the run (surface it) rather than hot-spin.
        A hang here would wedge the whole test process -- the assertion is really
        'run() returned at all', with the elapsed bound as the witness."""
        p = _NeverReady()
        out = {}

        def waiter():
            try:
                out["r"] = runloom_c.wait_fd(p.r, READ, -1)   # forever
            except OSError as e:
                out["err"] = repr(e)

        t0 = time.monotonic()
        runloom_c.fiber(waiter)
        runloom_c.run()                                       # must RETURN
        elapsed = time.monotonic() - t0
        p.close()

        self.assertLess(elapsed, 2.0,
                        "forever wait_fd hot-spun/hung (%.2fs) instead of "
                        "terminating on the settled deadlock" % elapsed)
        self.assertTrue("err" in out or out.get("r") == -1,
                        "unsatisfiable forever wait_fd should surface as an "
                        "error / -1, got %r" % out)

    def test_finite_timeouts_still_win_before_forever_is_reaped(self):
        """A finite parker fires on time even when a forever parker coexists;
        the forever one is reaped only once nothing timed remains."""
        p = _NeverReady()
        order = []

        def finite(ms):
            order.append(("finite", ms, runloom_c.wait_fd(p.r, READ, ms)))

        def forever():
            try:
                runloom_c.wait_fd(p.r, READ, -1)
            except OSError:
                order.append(("forever", "reaped"))

        runloom_c.fiber(forever)
        runloom_c.fiber(lambda: finite(100))
        runloom_c.fiber(lambda: finite(50))
        runloom_c.run()
        p.close()

        # the two finite timeouts fire in deadline order, THEN the forever is reaped
        self.assertEqual(order,
                         [("finite", 50, 0), ("finite", 100, 0),
                          ("forever", "reaped")],
                         "settled reap ran before finite timeouts, or misordered: "
                         "%r" % order)


class TestSimPrereqFixes(unittest.TestCase):
    def test_stale_arm_probe_gated_under_sim(self):
        """The stale-arm probe stamps a WALL deadline (monotonic_ns + probe_ns)
        into the deadline heap; under sim that heap feeds the logical-clock
        advance, so an ungated probe jumps the logical clock to wall time and
        shatters determinism.  Scenario: a timed park ARMS the fd and times out
        (logical clock -> 0.1s); a following UNTIMED park on the now-armed fd is
        the probe's target (arm_predict_skip true, deadline_ns < 0 -> it would
        overwrite the -1 with a wall value).  With the gate the logical clock
        stays ~0.1s; without it, it leaps to ~monotonic_ns() (~1e14 on this box).
        This is the regression guard for the load-bearing sim clock edit."""
        p = _NeverReady()

        def scenario():
            r1 = runloom_c.wait_fd(p.r, READ, 100)      # arms p.r; times out @0.1s logical
            assert r1 == 0
            try:
                runloom_c.wait_fd(p.r, READ, -1)        # forever on ARMED fd (probe target)
            except OSError:
                pass                                     # settled-reap terminates it

        # DELTA across just this scenario (the logical clock is a process-global
        # static, so prior tests' logical time -- e.g. the 1-hour timeout test --
        # accumulates; only the delta this scenario adds is meaningful).  The
        # timed park advances it 0.1s; the forever park must add ~0 (gated) rather
        # than leap by a wall value (~1e14 ns, monotonic_ns) if the probe fired.
        before = runloom_c._logical_ns()
        runloom_c.fiber(scenario)
        runloom_c.run()
        p.close()
        delta = runloom_c._logical_ns() - before
        self.assertLess(delta, 10 ** 9,
                        "logical clock advanced %d ns (~%.1fs) for a 0.1s timed "
                        "park + a forever park -- the stale-arm probe stamped a "
                        "WALL deadline (not gated off under sim)" % (delta, delta / 1e9))
        self.assertGreater(delta, 0, "logical clock did not advance at all")

    def test_io_uring_forced_off_under_sim(self):
        """io_uring must be OFF under sim so the TCP fast paths take the
        socketpair/netpoll route the sim pump drives, not the CQ backend."""
        self.assertFalse(runloom_c.iouring_available(),
                         "io_uring must be forced off under RUNLOOM_SIM")


class TestSimGate(unittest.TestCase):
    def test_backend_and_sim_on(self):
        # Backend is still epoll (sim replaces the pump, not the platform pick).
        self.assertIn(runloom_c.netpoll_backend(), ("epoll", "kqueue", "select"))


if __name__ == "__main__":
    unittest.main()
