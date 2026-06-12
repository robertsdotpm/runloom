"""sysmon-as-oracle: use the runtime's own stall detector to prove the
cooperative property end-to-end.

The M:N scheduler ships a sysmon watchdog (default-on on free-threaded 3.13t)
that logs `[RUNLOOM_SYSMON] hub N WEDGED ...` when a fiber pins a hub past the
budget without yielding.  We use that as a test oracle:

  * a workload that does NOT cooperate (unwrapped CPU-heavy hashing inline)
    WEDGES -- which also proves the detector itself works, so a "no WEDGE" is
    meaningful;
  * the SAME workload with `heavy` auto-offload on does NOT wedge -- the hashes
    relocate to the pool and the fibers park, so no hub is ever pinned;
  * a purely cooperative workload (cooperative sleeps) never wedges (no false
    positives).

Each case runs in its own subprocess (needs mn_init + a low RUNLOOM_SYSMON_MS, and
the WEDGED line is a C fprintf to stderr).
"""
import os
import subprocess
import sys
import textwrap
import unittest

_IS_POSIX = os.name == "posix"


def _run(snippet, sysmon_ms=20, timeout=90):
    env = dict(os.environ)
    env["PYTHONPATH"] = "src"
    env["RUNLOOM_SYSMON_MS"] = str(sysmon_ms)
    env.setdefault("PYTHON_GIL", "0")
    p = subprocess.run([sys.executable, "-c", snippet],
                       capture_output=True, text=True, timeout=timeout, env=env)
    return p.stdout + p.stderr


_HASH_WORKLOAD = textwrap.dedent("""
    import sys, hashlib, runloom, runloom.monkey, runloom_c
    runloom.monkey.patch(heavy=({heavy}))
    BUF = b"x" * (8 * 1024 * 1024)
    def g():
        for _ in range(25):
            hashlib.sha256(BUF).digest()
    runloom_c.mn_init(4)
    for _ in range(4):
        runloom_c.mn_go(g)
    runloom_c.mn_run()
""")


@unittest.skipUnless(_IS_POSIX, "sysmon WEDGED log is a POSIX-path diagnostic")
class TestSysmonOracle(unittest.TestCase):
    def test_unwrapped_heavy_wedges(self):
        """Negative control: inline CPU-heavy hashing pins hubs -> WEDGED.
        Proves the detector fires (so the positive test's silence is real).
        (hashlib/zlib release the GIL, so these classify DETACHED and handoff
        can even rescue the hub's siblings -- but the wedge is still logged.)"""
        out = _run(_HASH_WORKLOAD.format(heavy="False"))
        self.assertIn("WEDGED", out,
                      "expected the sysmon detector to flag the inline stall")

    def test_attached_cpu_loop_classified(self):
        """A frameless pure-Python loop is the true ATTACHED class -- it holds
        the tstate (no GIL release) so neither handoff nor preempt can rescue
        it.  The detector must flag it AND classify it ATTACHED; this is the
        case offload() exists for."""
        snippet = textwrap.dedent("""
            import runloom_c
            def hog():
                i = 0
                while i < 80_000_000:
                    i += 1
            runloom_c.mn_init(4)
            for _ in range(4):
                runloom_c.mn_go(hog)
            runloom_c.mn_run()
        """)
        out = _run(snippet)
        self.assertIn("WEDGED", out)
        self.assertIn("ATTACHED", out)

    def test_heavy_autooffload_prevents_wedge(self):
        """The money test: with `heavy` on (default), the same hashing
        auto-offloads to the pool, the fibers park, and NO hub wedges."""
        out = _run(_HASH_WORKLOAD.format(heavy="True"))
        self.assertNotIn("WEDGED", out,
                         "auto-offloaded hashing should never pin a hub:\n" + out)

    def test_cooperative_workload_never_wedges(self):
        """No false positives: cooperative sleeps park every few ms."""
        snippet = textwrap.dedent("""
            import runloom, runloom.monkey, runloom_c
            runloom.monkey.patch()
            def g():
                for _ in range(60):
                    runloom.sleep(0.005)
            runloom_c.mn_init(4)
            for _ in range(8):
                runloom_c.mn_go(g)
            runloom_c.mn_run()
        """)
        out = _run(snippet)
        self.assertNotIn("WEDGED", out, out)


if __name__ == "__main__":
    unittest.main()
