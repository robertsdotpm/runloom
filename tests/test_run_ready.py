"""runloom_c.run_ready() -- quiescence-barrier yield.

run_ready() parks the calling fiber until no other fiber is
immediately runnable (every ready g, including ones just woken, has run to its
next park or to completion), then resumes -- before the scheduler would block
on netpoll/timers.  This is asyncio's "run the ready callbacks for this loop
iteration" boundary, iterated to quiescence.

Motivating case (uvicorn websocket teardown): a client-close fiber (A)
wakes the server-connection fiber (B = run_asgi, which removes the
connection from server_state), then crosses a *synchronous* boundary
(server.shutdown()).  Under stock asyncio every ready callback runs each
iteration, so B finishes before A's shutdown; under a plain run-to-next-park
scheduler A races ahead and shuts down a still-registered connection.  A
run_ready() at the teardown checkpoint restores asyncio's ordering.
"""
import unittest

import runloom_c


class TestRunReady(unittest.TestCase):
    def test_flips_wake_then_sync_boundary(self):
        """A wakes B then crosses a sync boundary; run_ready lets B's effect
        land first (the uvicorn A/B race, distilled)."""
        def trial(use_run_ready):
            order = []
            state = {"removed": False, "seen": None}
            def B():
                order.append("B")
                state["removed"] = True
            def A():
                runloom_c.go(B)            # "close frame woke run_asgi"
                if use_run_ready:
                    runloom_c.run_ready()
                order.append("A")
                state["seen"] = state["removed"]   # what shutdown() observes
            runloom_c.go(A)
            runloom_c.run()
            return order, state["seen"]

        order_no, seen_no = trial(False)
        order_yes, seen_yes = trial(True)
        self.assertEqual(order_no, ["A", "B"])
        self.assertIs(seen_no, False)              # A raced ahead of B
        self.assertEqual(order_yes, ["B", "A"])
        self.assertIs(seen_yes, True)              # A saw B's completed effect
        self.assertEqual(runloom_c._self_check(0), 0)

    def test_drains_whole_cascade_not_one_pass(self):
        """The defining property vs sched_yield_classic: run_ready drains the
        entire wake cascade (A->B->C->D), not a single round-robin pass."""
        order = []
        def D(): order.append("D")
        def C(): order.append("C"); runloom_c.go(D)
        def B(): order.append("B"); runloom_c.go(C)
        def A():
            runloom_c.go(B)
            runloom_c.run_ready()
            order.append("A")
        runloom_c.go(A)
        runloom_c.run()
        self.assertEqual(order, ["B", "C", "D", "A"])
        self.assertEqual(runloom_c._self_check(0), 0)

    def test_classic_yield_is_only_one_pass(self):
        """Contrast: a single classic yield resumes A after just one level."""
        order = []
        def C(): order.append("C")
        def B(): order.append("B"); runloom_c.go(C)
        def A():
            runloom_c.go(B)
            runloom_c.sched_yield_classic()
            order.append("A")
        runloom_c.go(A)
        runloom_c.run()
        self.assertEqual(order, ["B", "A", "C"])

    def test_multiple_callers_resume_fifo_no_hang(self):
        """Two fibers both park on the barrier; both resume (FIFO) after
        the rest drains -- no lost wake, no hang."""
        order = []
        def W(): order.append("W")
        def A():
            runloom_c.go(W); runloom_c.run_ready(); order.append("A")
        def B():
            runloom_c.run_ready(); order.append("B")
        runloom_c.go(A)
        runloom_c.go(B)
        runloom_c.run()
        self.assertEqual(order, ["W", "A", "B"])
        self.assertEqual(runloom_c._self_check(0), 0)

    def test_no_other_work_resumes_immediately(self):
        """Nothing else runnable: run_ready resumes at once, doesn't block."""
        order = []
        def A():
            runloom_c.run_ready()
            order.append("A")
        runloom_c.go(A)
        runloom_c.run()
        self.assertEqual(order, ["A"])

    def test_outside_fiber_is_noop(self):
        """Called from the main thread (no current g): safe no-op."""
        runloom_c.run_ready()
        self.assertEqual(runloom_c._self_check(0), 0)

    def test_does_not_starve_a_sleeper(self):
        """A run_ready waiter resumes at quiescence even with a pending timer;
        the sleeper still fires afterwards (drain stays live)."""
        order = []
        def sleeper():
            runloom_c.sched_sleep(0.01)
            order.append("slept")
        def A():
            runloom_c.run_ready()
            order.append("A")
        runloom_c.go(sleeper)
        runloom_c.go(A)
        runloom_c.run()
        # A resumes at the quiescence point (before the timer); both complete.
        self.assertIn("A", order)
        self.assertIn("slept", order)
        self.assertEqual(runloom_c._self_check(0), 0)


if __name__ == "__main__":
    unittest.main()
