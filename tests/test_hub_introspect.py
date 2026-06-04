"""runloom.inspect.hubs() / runloom_c.mn_hub_states() -- the per-hub diagnostic
snapshot (the hub-level companion to goroutines()).

Covers the reliable contract:
  * [] outside an M:N run; one dict per hub inside, with the full key set and a
    py-spy stack_cmd carrying THIS process's PID;
  * a hub genuinely stuck in a single blocking C call is reported as
    `detached` with a growing dwell, and `blocked_at` names the call site;
  * a quiescent / cooperative run reports no `blocked_at` (no false wedges);
  * the default config (handoff rescue ON) takes the snapshot's frame-read
    lockout path without crashing.

`blocked_at` is read from another hub's tstate, so the deterministic cases run
with the handoff rescue OFF (no thread can be mutating that tstate's frames);
the last test flips it back on to exercise the CAS-lockout path.
"""
import os
import sys
import unittest

# Deterministic blocked_at: no rescue thread adopting the wedged hub's tstate.
# Read at mn_init, so set before the first runloom.run().  The handoff-on test
# re-enables it explicitly.
os.environ["RUNLOOM_HANDOFF"] = "0"

sys.path.insert(0, "src")

import runloom
import runloom_c
from runloom import inspect as gi

HUB_KEYS = {"id", "state", "running_g", "dwell_ms", "pending",
            "preempt_requested", "instrumented", "blocked_at"}

# A blocking (non-cooperative) C call, long enough to outlast the ~50 ms sysmon
# wedge budget while a sampler goroutine inspects the hubs.
def blocking_sleep_worker():
    import time
    time.sleep(0.30)            # raw time.sleep -> DETACHED hub wedge


class HubIntrospectTest(unittest.TestCase):

    def test_empty_outside_run(self):
        # No M:N scheduler running -> [] (not an error).
        self.assertEqual(runloom_c.mn_hub_states(), [])
        self.assertEqual(gi.hubs(), [])

    def test_structure_and_stack_cmd(self):
        out = {}

        def main():
            out["hubs"] = gi.hubs()

        runloom.run(4, main)
        hubs = out["hubs"]
        self.assertEqual(len(hubs), 4)
        self.assertEqual(sorted(h["id"] for h in hubs), [0, 1, 2, 3])
        pid = str(os.getpid())
        for h in hubs:
            self.assertTrue(HUB_KEYS <= set(h), h)
            self.assertIn(h["state"],
                          ("detached", "attached", "suspended", "unknown"))
            self.assertEqual(h["stack_cmd"], "py-spy dump --pid " + pid)

    def test_wedge_and_blocked_at(self):
        out = {}

        def main():
            runloom.go(blocking_sleep_worker)
            runloom.sleep(0.10)          # let the wedge pass the budget
            out["hubs"] = gi.hubs()

        runloom.run(4, main)
        hubs = out["hubs"]
        wedged = [h for h in hubs
                  if h["state"] == "detached" and (h["dwell_ms"] or 0) >= 50]
        self.assertTrue(wedged, "expected a detached-wedged hub: %r" % (hubs,))
        # The top Python frame of the wedged hub is the worker that called the
        # blocking time.sleep, so blocked_at names it.
        named = [h["blocked_at"] for h in wedged if h["blocked_at"]]
        self.assertTrue(named, "expected blocked_at on the wedged hub: %r" % (wedged,))
        self.assertTrue(any("blocking_sleep_worker" in b for b in named), named)

    def test_quiescent_has_no_blocked_at(self):
        out = {}

        def main():
            # Purely cooperative work -- nothing should look wedged.
            runloom.sleep(0.10)
            out["hubs"] = gi.hubs()

        runloom.run(4, main)
        for h in out["hubs"]:
            self.assertIsNone(h["blocked_at"], h)

    def test_print_hubs_smoke(self):
        import io
        out = {}

        def main():
            runloom.go(blocking_sleep_worker)
            runloom.sleep(0.10)
            buf = io.StringIO()
            gi.print_hubs(file=buf)
            out["text"] = buf.getvalue()

        runloom.run(4, main)
        text = out["text"]
        self.assertIn("runloom hubs", text)
        # A wedge was present, so the py-spy hint is emitted.
        self.assertIn("py-spy dump --pid", text)

    def test_handoff_on_lockout_path(self):
        # Re-enable the rescue: the snapshot must take its CAS-lockout path
        # (and skip blocked_at when it loses the race) without crashing.
        os.environ["RUNLOOM_HANDOFF"] = "1"
        try:
            out = {}

            def main():
                for _ in range(6):
                    runloom.go(blocking_sleep_worker)
                runloom.sleep(0.10)
                out["hubs"] = gi.hubs()

            runloom.run(4, main)
            hubs = out["hubs"]
            self.assertEqual(len(hubs), 4)
            for h in hubs:
                self.assertTrue(HUB_KEYS <= set(h), h)
                # blocked_at is best-effort here (may be None under the rescue);
                # whatever it is, it's None or a string naming a frame.
                self.assertTrue(h["blocked_at"] is None
                                or isinstance(h["blocked_at"], str))
        finally:
            os.environ["RUNLOOM_HANDOFF"] = "0"


if __name__ == "__main__":
    unittest.main()
