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


class TestSimBytesMITM(unittest.TestCase):
    """Increment 2: the MITM model goroutine holds bytes for a seed-drawn delay
    on the logical clock before delivery."""

    def test_delayed_delivery_lands_at_logical_T(self):
        """A message delayed by D logical seconds wakes the reader exactly D
        later on the LOGICAL clock -- the delivery-at-T property, and instant in
        WALL time (the model's sched_sleep is logical-compressed)."""
        D = 0.5
        conn = simnet_fd.SimFdConn(delay_fn=lambda: D)
        out = {}

        def reader():
            out["t0"] = runloom_c._logical_ns()
            out["data"] = conn.b.recv_exact(4)       # parks; woken only after D
            out["t1"] = runloom_c._logical_ns()

        def writer():
            conn.a.sendall(b"pong")

        d0 = runloom_c.count_deadlocked()
        runloom_c.fiber(reader)                       # parks first
        runloom_c.fiber(writer)
        t_wall = time.monotonic()
        runloom_c.run()
        wall = time.monotonic() - t_wall
        conn.close()

        self.assertEqual(out.get("data"), b"pong")
        self.assertEqual(runloom_c.count_deadlocked() - d0, 0,
                         "shuttlers inflated the deadlock census")
        delta = out["t1"] - out["t0"]
        self.assertTrue(0.49e9 <= delta <= 0.51e9,
                        "delivery logical latency was %d ns, expected ~0.5e9 "
                        "(the model delay was not honoured on the logical clock)" % delta)
        self.assertLess(wall, 2.0, "logical delay was not compressed in wall time")

    def test_mitm_stream_order_preserved(self):
        """Several messages in one direction, each independently delayed, arrive
        in SEND order (one shuttler per direction serializes the stream)."""
        conn = simnet_fd.SimFdConn(delay_fn=lambda: 0.01)
        got = {}

        def reader():
            got["data"] = conn.b.recv_exact(6)

        def writer():
            for c in b"abcdef":
                conn.a.sendall(bytes([c]))

        runloom_c.fiber(reader)
        runloom_c.fiber(writer)
        runloom_c.run()
        conn.close()
        self.assertEqual(got.get("data"), b"abcdef",
                         "MITM did not preserve stream order")

    def test_mitm_deterministic(self):
        """Same seeded delay draws -> identical outcome across runs."""
        import random

        def run_scenario():
            runloom_c.sim_reset()                    # fresh logical clock -> bit-exact across runs
            rng = random.Random(1234)
            conn = simnet_fd.SimFdConn(delay_fn=lambda: rng.random() * 0.05)
            out = {}

            def reader():
                out["data"] = conn.b.recv_exact(8)
                out["t"] = runloom_c._logical_ns()

            def writer():
                for c in b"deadbeef":
                    conn.a.sendall(bytes([c]))

            runloom_c.fiber(reader)
            runloom_c.fiber(writer)
            runloom_c.run()
            conn.close()
            return out["data"], out["t"]              # absolute logical time (clock reset each run)

        a = run_scenario()
        b = run_scenario()
        self.assertEqual(a[0], b"deadbeef")
        self.assertEqual(a, b, "MITM delayed delivery not bit-exact reproducible: "
                               "%r vs %r" % (a, b))


class TestSimBytesLoss(unittest.TestCase):
    """Increment 3 (faults): the model can DROP chunks -- a modelled loss (bytes
    never arrive, no retransmit).  Disruptive to conservation by design, so it is
    an opt-in model behaviour, tested in isolation."""

    def test_full_loss_reader_terminates(self):
        """100% loss: the dropped bytes never arrive, the reader is reaped, run()
        terminates rather than hanging."""
        conn = simnet_fd.SimFdConn(delay_fn=lambda: 0.0, loss_fn=lambda: True)
        out = {}

        def reader():
            try:
                out["data"] = conn.b.recv_exact(3)
            except OSError:
                out["err"] = True

        def writer():
            conn.a.sendall(b"xyz")

        t0 = time.monotonic()
        runloom_c.fiber(reader)
        runloom_c.fiber(writer)
        runloom_c.run()
        wall = time.monotonic() - t0
        conn.close()
        self.assertLess(wall, 2.0, "lossy connection hung instead of terminating")
        self.assertTrue(out.get("err") or out.get("data", b"") == b"",
                        "dropped bytes should not arrive, got %r" % out)

    def test_seeded_loss_is_deterministic(self):
        import random

        def scenario():
            runloom_c.sim_reset()
            rng = random.Random(555)
            conn = simnet_fd.SimFdConn(delay_fn=lambda: 0.0,
                                       loss_fn=lambda: rng.random() < 0.5)
            got = []

            def reader():
                try:
                    while True:
                        c = conn.b.recv(16)
                        if not c:
                            break
                        got.append(bytes(c))
                except OSError:
                    pass

            def writer():
                for i in range(6):
                    conn.a.sendall(bytes([65 + i]))

            runloom_c.fiber(reader)
            runloom_c.fiber(writer)
            runloom_c.run()
            conn.close()
            return b"".join(got)

        a = scenario()
        b = scenario()
        self.assertEqual(a, b, "lossy delivery not deterministic: %r vs %r" % (a, b))


class TestSimFdProgram(unittest.TestCase):
    """The self-contained byte-plane workload (simnet_fd.simfd_program) -- a
    pure-function-of-seed unit for the fleet soak, exercising the real netpoll
    park/commit/wake path deterministically."""

    def test_clean_and_deterministic_over_seeds(self):
        for seed in (1, 2, 3, 7, 11, 42, 99, 123, 777):
            ok1, r1 = simnet_fd.simfd_program(seed)
            ok2, r2 = simnet_fd.simfd_program(seed)
            self.assertTrue(ok1, "simfd seed %d not clean: %s" % (seed, r1))
            self.assertEqual((ok1, r1), (ok2, r2),
                             "simfd seed %d not deterministic" % seed)


if __name__ == "__main__":
    unittest.main()
