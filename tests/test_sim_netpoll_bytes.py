"""Slice 3 -- the socketpair-backed byte/readiness plane under RUNLOOM_SIM.

A REAL socket workload (real send/recv on a real socketpair, real wait_fd park on
EAGAIN) runs deterministically: readiness is delivered by the per-scheduler ready
ledger (runloom_c.sim_deliver_ready dispatched by the sim pump), never the kernel
epoll -- so the full real park/commit/deadline/wake path is exercised as a
function of the seed.  Under sim the pump never epoll_waits, so a socketpair
reader parked on EAGAIN is woken ONLY by the ledger; these tests prove the wake
lands, the bytes are exact, it is instant (logical compression) and deterministic.

RUNLOOM_SIM is read once + cached, so it is set before import; run_isolated gives
this file its own subprocess.  See docs/dev/soak/SIM_IO_DST.md.
"""
import os
import sys
import time
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "src"))
sys.path.insert(0, os.path.join(REPO, "tools", "dst"))
os.environ["PYTHON_GIL"] = "0"
os.environ["RUNLOOM_SIM"] = "1"
os.environ.setdefault("RUNLOOM_LOGICAL_CLOCK", "1")
os.environ.setdefault("RUNLOOM_HUBS", "1")           # H=1: sim is deterministic single-hub
import runloom_c            # noqa: E402
import simnet_fd            # noqa: E402


class TestSimBytes(unittest.TestCase):
    def test_single_conn_roundtrip_via_ledger(self):
        """Reader parks on a real socketpair recv (EAGAIN); the writer's send +
        ledger delivery is the SOLE wake (the sim pump never epoll_waits).  The
        exact bytes arrive, instantly, with no deadlock."""
        conn = simnet_fd.SimFdConn()
        got = {}

        def reader():
            got["data"] = conn.b.recv_exact(5)      # parks here until the ledger wakes it

        def writer():
            conn.a.sendall(b"hello")

        d0 = runloom_c.count_deadlocked()
        runloom_c.fiber(reader)                       # spawned first -> runs + parks first
        runloom_c.fiber(writer)
        t0 = time.monotonic()
        runloom_c.run()
        elapsed = time.monotonic() - t0
        conn.close()

        self.assertEqual(got.get("data"), b"hello",
                         "socketpair reader did not receive the bytes via the ledger")
        self.assertEqual(runloom_c.count_deadlocked() - d0, 0, "unexpected deadlock")
        self.assertLess(elapsed, 2.0, "not instant -- logical clock did not compress")

    def test_bidirectional_echo(self):
        """Full-duplex over one socketpair: client sends, server echoes back."""
        conn = simnet_fd.SimFdConn()
        out = {}

        def server():
            data = conn.b.recv_exact(4)
            conn.b.sendall(data[::-1])               # echo reversed

        def client():
            conn.a.sendall(b"ping")
            out["reply"] = conn.a.recv_exact(4)

        runloom_c.fiber(server)
        runloom_c.fiber(client)
        runloom_c.run()
        conn.close()
        self.assertEqual(out.get("reply"), b"gnip")

    def test_multi_conn_dispatch_is_deterministic(self):
        """N connections all ready at the same logical instant wake their readers
        in the stable conn_id order -- identical across runs (the ledger sort key
        is the ordering authority, not the kernel / fd number)."""
        def run_scenario():
            conns = [simnet_fd.SimFdConn() for _ in range(5)]
            order = []

            def reader(i):
                buf = conns[i].b.recv_exact(3)
                order.append((i, buf))

            def writer(i):
                conns[i].a.sendall(bytes([65 + i]) * 3)

            for i in range(5):
                runloom_c.fiber(lambda i=i: reader(i))    # all park first
            for i in range(5):
                runloom_c.fiber(lambda i=i: writer(i))
            runloom_c.run()
            for c in conns:
                c.close()
            return order

        a = run_scenario()
        b = run_scenario()
        self.assertEqual(a, b, "multi-conn dispatch order not reproducible")
        # readers wake in ascending conn_id == loop index order
        self.assertEqual([i for i, _ in a], [0, 1, 2, 3, 4],
                         "readers did not wake in stable conn_id order: %r" % a)
        self.assertEqual([bytes(buf) for _, buf in a],
                         [bytes([65 + i]) * 3 for i in range(5)])

    def test_reader_with_no_sender_terminates(self):
        """A socketpair reader that never gets a delivery parks forever; the sim
        pump reaps the settled deadlock so run() terminates rather than hangs."""
        conn = simnet_fd.SimFdConn()
        out = {}

        def lonely_reader():
            try:
                out["r"] = conn.b.recv_exact(1)
            except OSError as e:
                out["err"] = repr(e)

        t0 = time.monotonic()
        runloom_c.fiber(lonely_reader)
        runloom_c.run()                              # must return, not hang
        elapsed = time.monotonic() - t0
        conn.close()
        self.assertLess(elapsed, 2.0, "lonely reader hung instead of being reaped")
        # recv_exact swallows a clean EOF (b"") ; a reaped park raises OSError.
        self.assertTrue("err" in out or out.get("r") == b"",
                        "expected reap (OSError) or empty, got %r" % out)


if __name__ == "__main__":
    unittest.main()
