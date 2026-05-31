"""Tests for pygo.blocking / pygo_core.blocking -- the blocking-offload pool.

A goroutine that makes a non-preemptible blocking call (DNS, blocking
sockets, GIL-releasing C extensions) must not wedge the OS thread it
shares with other goroutines.  blocking() offloads the call to a thread
pool and parks the goroutine, so the others keep running.
"""
import time
import unittest

import pygo
import pygo_core


# Single-thread blocking offloads run CONCURRENTLY only on netpoll backends
# that expose a pump-wake primitive: epoll (eventfd), kqueue (EVFILT_USER)
# and Windows IOCP+AFD (PostQueuedCompletionStatus).  The Windows WSAPoll /
# select fallback pumps re-poll the parked-fd set on a timeout and have no
# wakeable object, so a worker thread can't interrupt an idle pump; on those
# backends blocking() runs the call inline (serial) rather than offloading
# (see pygo_netpoll_wake_pump_arm in netpoll.c).  The offloads still complete
# correctly there -- only the wall-clock concurrency bound does not hold.
_PUMP_WAKE = pygo_core.netpoll_backend() in ("epoll", "kqueue", "iocp-afd")


class TestBlocking(unittest.TestCase):
    def test_result_and_args(self):
        """blocking() returns fn's value and forwards *args / **kwargs."""
        out = []

        def add(a, b, c=0):
            time.sleep(0.01)
            return a + b + c

        def w():
            out.append(pygo.blocking(add, 2, 3, c=10))

        pygo_core.go(w)
        pygo_core.run()
        self.assertEqual(out, [15])

    def test_exception_propagates(self):
        """An exception in the offloaded call is re-raised in the goroutine."""
        seen = []

        def boom():
            time.sleep(0.01)
            raise ValueError("kaboom")

        def w():
            try:
                pygo.blocking(boom)
            except ValueError as e:
                seen.append(str(e))

        pygo_core.go(w)
        pygo_core.run()
        self.assertEqual(seen, ["kaboom"])

    def test_does_not_wedge_the_hub(self):
        """N goroutines each offloading a blocking sleep run CONCURRENTLY
        on one OS thread -- wall time ~= one sleep, not N sleeps."""
        N, NAP = 8, 0.2
        done = []

        def w(i):
            pygo.blocking(time.sleep, NAP)
            done.append(i)

        for i in range(N):
            pygo_core.go(lambda i=i: w(i))
        t0 = time.monotonic()
        pygo_core.run()
        wall = time.monotonic() - t0

        # Correctness holds on every backend: all offloads complete.
        self.assertEqual(sorted(done), list(range(N)))
        if _PUMP_WAKE:
            # Serial-on-one-thread would be N*NAP; offloaded is ~NAP.  Half
            # the serial time is a generous bar that still proves concurrency.
            self.assertLess(wall, N * NAP * 0.5)
        else:
            # WSAPoll / select fallback pumps run the offload inline; only
            # completion (checked above) is guaranteed, not concurrency.
            self.skipTest(
                "netpoll backend %r has no pump-wake; blocking() runs inline"
                % pygo_core.netpoll_backend())

    def test_inline_outside_goroutine(self):
        """Called outside any goroutine, blocking() just runs fn inline."""
        self.assertEqual(pygo_core.blocking(lambda x: x * 2, 21), 42)


if __name__ == "__main__":
    unittest.main()
