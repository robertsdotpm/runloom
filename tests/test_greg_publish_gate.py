"""Cross-hub g-registry publish gate: a fibers() reader never sees a
pre-PUBLISH goroutine.

A goroutine struct is linked into the global registry at slab-alloc
(runloom_greg_link), but its display fields (id / owner / refcount / noyield)
are written and the struct is PUBLISHED only later, via state_set(RUNNABLE)
(runloom_sched_core.c.inc, an __ATOMIC_RELEASE).  A concurrent registry walker
(runloom_c.fibers() / fiber_count(), used from any OS thread) must therefore
SKIP every g still in a pre-RUNNABLE state -- the ACQUIRE gate in
runloom_introspect.c (`st < RUNLOOM_GST_RUNNABLE || st == FREED`).  Reading a g
mid-spawn would be a torn read of uninitialised/stale fields (a real data race a
TSan-gold run caught on the snapshot reader).

Until now this was guarded ONLY by a crash oracle (the foreign-thread registry
stress under SIGSEGV/TSan): a torn-but-non-crashing read -- a row with a stale
id or a pre-publish state -- would pass silently.  This is a VALUE oracle: it
asserts every returned row is a fully-published, self-consistent fiber.

The pre-publish window is normally hit ~1/56k; we WIDEN it deterministically
with the seeded delay injector (RUNLOOM_DELAY) armed at the new
RUNLOOM_DLY_SPAWN_PUBLISH site, which sleeps between the registry link and the
RELEASE publish.  A foreign OS thread hammers fibers() throughout, so it reliably
walks gs while they sit pre-RUNNABLE and must skip them.  run_isolated gives this
file its own subprocess; the env is set before import.
"""
import os
import sys
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "src"))
os.environ["PYTHON_GIL"] = "0"
os.environ.setdefault("RUNLOOM_DELAY", "0x5EED")      # arm seeded delay injection
os.environ.setdefault("RUNLOOM_DELAY_MAX_NS", "40000")  # up to 40us window widening
import runloom_c            # noqa: E402

from adv_util import raw_thread, needs_free_threading   # noqa: E402

# Only states >= RUNNABLE are published; the gate must never leak these three.
PREPUBLISH = {"init", "spawning", "freed"}
PUBLISHED = {"runnable", "submitted", "running", "io-wait", "chan-wait",
             "sleep", "park", "waking", "done"}


@unittest.skipUnless(needs_free_threading(),
                     "registry publish race is only meaningful GIL-disabled")
class TestGregPublishGate(unittest.TestCase):
    def test_reader_never_sees_prepublish_fiber(self):
        """While a driver fiber spawns thousands of children (each traversing the
        widened SPAWNING->RUNNABLE window), a foreign OS thread walks the registry
        continuously.  Every row it returns must be a published, self-consistent
        fiber -- never a pre-publish state, never a torn id."""
        stop = [False]
        violations = []
        rows_seen = [0]

        def reader():
            while not stop[0]:
                try:
                    rows = runloom_c.fibers()
                except BaseException as e:                  # a crash-in-walk surfaces here
                    violations.append(("fibers-raised", repr(e)))
                    return
                for r in rows:
                    rows_seen[0] += 1
                    st = r.get("state")
                    if st in PREPUBLISH or st not in PUBLISHED:
                        violations.append(("unpublished-state", st, dict(r)))
                    if not isinstance(r.get("id"), int) or r.get("id") <= 0:
                        violations.append(("torn-id", dict(r)))

        t = raw_thread(reader)

        def driver():
            for i in range(4000):
                runloom_c.fiber(lambda: None)     # each spawn crosses the widened window
                if i % 48 == 0:
                    runloom_c.sched_yield()        # let children run + reap; keep churn
            # drain remaining children
            for _ in range(64):
                runloom_c.sched_yield()

        runloom_c.fiber(driver)
        runloom_c.run()
        stop[0] = True
        t.join(timeout=10)

        self.assertEqual(violations[:8], [],
                         "reader observed a pre-publish / torn fiber row")
        self.assertGreater(rows_seen[0], 0,
                           "reader saw no rows -- window not exercised")
        self.assertEqual(runloom_c._self_check(0), 0)


if __name__ == "__main__":
    unittest.main()
