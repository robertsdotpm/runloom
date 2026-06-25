"""Goroutine-spawned subprocess.Popen with pipes -- the offload-fstat path.

Constructing a Popen with stdout/stderr=PIPE from inside a fiber drives
Popen.__init__ -> io.open(pipe_fd) -> _fd_pollable -> os.fstat, which the monkey
layer offloads to the pool (one park per pipe).  monkey now runs the whole
constructor off-fiber (subproc._patched_popen_init) so those fstats execute
INLINE on the worker -- one park instead of one-per-pipe, no per-caller dodge.

Nothing else in the suite exercised this path WITHOUT the by-hand off-fiber
dodge in tests/big_100/procutil.py, so these tests guard both:
  * correctness -- a usable Popen (working pipe file objects) comes back from a
    fiber, and an __init__ that raises propagates the exception, and
  * liveness   -- a concurrent spawn storm completes and never wedges in
    Popen.__init__ (a regression of the offload wake path would hang here).
"""
import sys
import time
import unittest

import runloom
import runloom.monkey

runloom.monkey.patch()

# A fast child that writes a known marker (explicit flush: free-threaded builds
# don't deterministically flush on implicit close).
_HI = [sys.executable, "-c", "import sys; sys.stdout.write('hi'); sys.stdout.flush()"]
# Cheapest possible child for the storm (no output needed -- only completion).
_NOOP = ["true"] if sys.platform != "win32" else [sys.executable, "-c", ""]


class TestGoroutinePopen(unittest.TestCase):
    def test_popen_pipes_from_fiber_correct(self):
        """A Popen(stdout=PIPE) built inside a fiber yields the child's bytes."""
        import subprocess
        out = []

        def w():
            p = subprocess.Popen(_HI, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
            o, _ = p.communicate()
            out.append((p.returncode, o))

        runloom.run(2, w)
        self.assertEqual(out, [(0, b"hi")])

    def test_init_exception_propagates_to_fiber(self):
        """An OSError from the off-fiber constructor reaches the fiber."""
        import subprocess
        seen = []

        def w():
            try:
                subprocess.Popen(["/no/such/binary/xyzzy42"],
                                 stdout=subprocess.PIPE)
            except FileNotFoundError:
                seen.append("ok")

        runloom.run(2, w)
        self.assertEqual(seen, ["ok"])

    def test_spawn_storm_completes(self):
        """Many fibers each spawn a piped Popen concurrently; all complete.
        A regression of the offload wake path would wedge in Popen.__init__."""
        import subprocess
        N = 120
        done = []

        def w(i):
            p = subprocess.Popen(_NOOP, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            p.communicate()
            if p.returncode == 0:
                done.append(i)

        def main():
            for i in range(N):
                runloom.fiber(w, i)

        t0 = time.monotonic()
        runloom.run(8, main)
        self.assertEqual(len(done), N)
        # Generous bound: a lost-wake hang would blow well past this.
        self.assertLess(time.monotonic() - t0, 60.0)


if __name__ == "__main__":
    unittest.main()
