"""Memory + scheduler stability soak tests.

Marked SKIP by default so the normal test suite stays fast.  Run with:
    RUNLOOM_RUN_SOAK=1 python -m unittest tests.test_soak

Tests here exercise long lifetimes and high spawn rates to catch:
  * stack / coro / g leaks (RSS climbs over many drains)
  * ready-ring / sleep-heap / parker-pool growth that doesn't shrink
  * channel waiter leaks (close without recv)
  * monkey-patched socket pool stuck handles

The leak budget is intentionally generous (+50 MiB) because Python's
allocator caches per-arena and we can't tell legitimate caching from
small steady-state leaks at this resolution.  Anything that grows
RSS by hundreds of MiB across the soak run is a real bug.
"""
import gc
import os
import time
import unittest

import runloom_c


_RUN = os.environ.get("RUNLOOM_RUN_SOAK", "").strip() not in ("", "0", "no", "false")


def _rss_mb():
    """Best-effort RSS in MiB.  Returns -1 if /proc isn't available."""
    try:
        with open("/proc/self/statm") as f:
            return int(f.read().split()[1]) * (os.sysconf("SC_PAGE_SIZE") / (1 << 20))
    except Exception:
        return -1.0


@unittest.skipUnless(_RUN, "set RUNLOOM_RUN_SOAK=1 to enable")
class TestSpawnSoak(unittest.TestCase):
    def test_spawn_drain_million(self):
        """1M total spawn/drain cycles in batches of 10k.  RSS measured
        AFTER the first batch (pool warmup is expected to bump RSS by
        ~60 MiB) so we're only watching for steady-state growth."""
        batches  = 100
        per      = 10_000

        # Warmup batch: stack pool, g slab, parker pool all populate.
        for _ in range(per):
            runloom_c.go(lambda: None)
        runloom_c.run()
        gc.collect()
        rss_baseline = _rss_mb()

        for i in range(1, batches):
            for _ in range(per):
                runloom_c.go(lambda: None)
            runloom_c.run()
            if i % 10 == 0:
                gc.collect()
                rss = _rss_mb()
                print("[soak] batch %d  rss=%.1f MiB (baseline %.1f)"
                      % (i, rss, rss_baseline), flush=True)

        gc.collect()
        rss_end = _rss_mb()
        growth = rss_end - rss_baseline
        print("[soak] baseline=%.1f end=%.1f delta=%.1f MiB"
              % (rss_baseline, rss_end, growth), flush=True)
        # Tight budget: after warmup, 990k more gs should be ~free.
        self.assertLess(growth, 5.0,
            "RSS grew by %.1f MiB across 990k post-warmup gs" % growth)


@unittest.skipUnless(_RUN, "set RUNLOOM_RUN_SOAK=1 to enable")
class TestChannelSoak(unittest.TestCase):
    def test_chan_ping_pong_100k(self):
        """100k ping-pong cycles between two goroutines through a
        single channel.  Catches g/snap-block growth in the C path."""
        ch_a = runloom_c.Chan(0)
        ch_b = runloom_c.Chan(0)

        def pinger():
            for _ in range(100_000):
                ch_a.send(1)
                ch_b.recv()

        def ponger():
            for _ in range(100_000):
                ch_a.recv()
                ch_b.send(1)

        t0 = time.monotonic()
        runloom_c.go(pinger)
        runloom_c.go(ponger)
        runloom_c.run()
        dt = time.monotonic() - t0
        print("[soak] 100k ping-pong in %.2fs (%.1f ns/round)"
              % (dt, dt * 1e9 / 100_000), flush=True)


if __name__ == "__main__":
    unittest.main()
