"""Smoke test for runloom_c.stats() -- production introspection."""
import unittest

import runloom
import runloom_c


class TestStats(unittest.TestCase):
    def test_keys_and_types(self):
        s = runloom_c.stats()
        self.assertIsInstance(s, dict)
        for k in ("ready", "sleeping", "netpoll_parked", "completed",
                  "running", "stack_size_default", "ready_capacity",
                  "backend", "netpoll"):
            self.assertIn(k, s, "missing key %r" % k)

        # Numeric counters are non-negative ints; backends are non-empty strings.
        for k in ("ready", "sleeping", "netpoll_parked", "completed",
                  "running", "stack_size_default", "ready_capacity"):
            self.assertIsInstance(s[k], int, "non-int %s: %r" % (k, s[k]))
            self.assertGreaterEqual(s[k], 0)
        self.assertIsInstance(s["backend"], str)
        self.assertIsInstance(s["netpoll"], str)
        self.assertTrue(s["backend"])
        self.assertTrue(s["netpoll"])

    def test_completed_increments(self):
        before = runloom_c.stats()["completed"]
        runloom_c.fiber(lambda: None)
        runloom_c.fiber(lambda: None)
        runloom_c.run()
        after = runloom_c.stats()["completed"]
        self.assertGreaterEqual(after - before, 2)


if __name__ == "__main__":
    unittest.main()
