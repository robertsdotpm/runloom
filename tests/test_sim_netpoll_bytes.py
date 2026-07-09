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


class TestSimBytesLargeSend(unittest.TestCase):
    """Increment W: a send larger than the socketpair buffer must not strand.
    Before W, tcp_send loops-to-completion and parks WRITE *inside* the C call
    before the wrapper posts the ledger entry, so the consumer never wakes and
    the settled-reap cancels everyone (verified: 200 KB -> 0 bytes, both modes)."""

    N = 200 * 1024        # > ~2x the pinned 64 KB SO_SNDBUF (kernel doubling)

    def _roundtrip(self, conn, reader_ep, writer_ep, reader_sip=None):
        out = {}

        def reader():
            buf = b""
            try:
                while len(buf) < self.N:
                    c = reader_ep.recv(reader_sip or (self.N - len(buf)))
                    if not c:
                        break
                    buf += c
            except OSError as e:
                out["reader_err"] = repr(e)
            out["got"] = len(buf)

        def writer():
            try:
                writer_ep.sendall(b"z" * self.N)
                out["sent_all"] = True
            except OSError as e:
                out["writer_err"] = repr(e)

        d0 = runloom_c.count_deadlocked()
        runloom_c.fiber(reader)
        runloom_c.fiber(writer)
        t0 = time.monotonic()
        runloom_c.run()
        out["wall"] = time.monotonic() - t0
        out["dl"] = runloom_c.count_deadlocked() - d0
        conn.close()
        return out

    def test_large_send_over_sndbuf_mitm(self):
        conn = simnet_fd.SimFdConn(delay_fn=lambda: 0.0001)
        out = self._roundtrip(conn, conn.b, conn.a)
        self.assertEqual(out.get("got"), self.N,
                         "MITM 200KB stranded: %r" % out)
        self.assertLess(out["wall"], 5.0)

    def test_large_send_over_sndbuf_direct(self):
        conn = simnet_fd.SimFdConn()          # DIRECT
        out = self._roundtrip(conn, conn.b, conn.a)
        self.assertEqual(out.get("got"), self.N,
                         "DIRECT 200KB stranded: %r" % out)
        self.assertLess(out["wall"], 5.0)

    def test_large_send_zero_delay(self):
        # THE teeth for piece 3: a rules-1+2-only fix still strands hop 2 here.
        conn = simnet_fd.SimFdConn(delay_fn=lambda: 0.0)
        out = self._roundtrip(conn, conn.b, conn.a)
        self.assertEqual(out.get("got"), self.N,
                         "zero-delay MITM 200KB stranded (hop-2): %r" % out)

    def test_large_send_slow_reader(self):
        # reader sips 4 KB at a time -> exercises the shuttler WRITE-park backpressure.
        conn = simnet_fd.SimFdConn(delay_fn=lambda: 0.0)
        out = self._roundtrip(conn, conn.b, conn.a, reader_sip=4096)
        self.assertEqual(out.get("got"), self.N,
                         "slow-reader 200KB stranded: %r" % out)

    def test_large_send_deterministic(self):
        import random

        def scenario():
            runloom_c.sim_reset()
            rng = random.Random(4321)
            conn = simnet_fd.SimFdConn(delay_fn=lambda: rng.random() * 0.001)
            out = self._roundtrip(conn, conn.b, conn.a)
            return out.get("got"), runloom_c._logical_ns()

        a = scenario()
        b = scenario()
        self.assertEqual(a[0], self.N)
        self.assertEqual(a, b, "large-send delivery not bit-exact: %r vs %r" % (a, b))


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


class TestSimBytesReset(unittest.TestCase):
    """Increment R: a modelled connection reset (RST-discard).  reset() cancels
    every fd-parker (CANCELLED -> no retry -> fd-reuse-safe) and sets a flag the
    wrappers check first, so both ends observe SimError(ECONNRESET)."""

    def test_reset_wakes_parked_reader(self):
        conn = simnet_fd.SimFdConn(delay_fn=lambda: 0.0)
        out = {}

        def reader():
            try:
                conn.b.recv_exact(4)
                out["r"] = "no-error"
            except simnet_fd.SimError:
                out["r"] = "ECONNRESET"

        def resetter():
            conn.reset()                             # reader has parked by now (spawned first)

        t0 = time.monotonic()
        runloom_c.fiber(reader)
        runloom_c.fiber(resetter)
        runloom_c.run()
        wall = time.monotonic() - t0
        conn.close()
        self.assertEqual(out.get("r"), "ECONNRESET",
                         "parked reader did not observe the reset: %r" % out)
        self.assertLess(wall, 2.0, "reset hung instead of waking the reader")

    def test_reset_discards_buffered_data(self):
        """RST-discard: a recv after reset raises even though bytes were buffered."""
        conn = simnet_fd.SimFdConn(delay_fn=lambda: 0.0)
        out = {}

        def flow():
            conn.a.sendall(b"ping")                  # delivered to b_app buffer
            out["first"] = conn.b.recv(2)            # forces delivery; gets "pi"
            conn.reset()                             # "ng" still buffered in b_app
            try:
                conn.b.recv(2)
                out["second"] = "no-error"
            except simnet_fd.SimError:
                out["second"] = "ECONNRESET"

        runloom_c.fiber(flow)
        runloom_c.run()
        conn.close()
        self.assertTrue(out.get("first"), "no data delivered before reset: %r" % out)
        self.assertEqual(out.get("second"), "ECONNRESET",
                         "buffered data not discarded on reset: %r" % out)

    def test_reset_send_raises(self):
        conn = simnet_fd.SimFdConn(delay_fn=lambda: 0.0)
        out = {}

        def flow():
            conn.reset()
            try:
                conn.a.sendall(b"x")
                out["s"] = "no-error"
            except simnet_fd.SimError:
                out["s"] = "ECONNRESET"

        runloom_c.fiber(flow)
        runloom_c.run()
        conn.close()
        self.assertEqual(out.get("s"), "ECONNRESET")

    def test_reset_mid_flight_leaves_new_conn_unaffected(self):
        """ADVERSARIAL: reset a conn with a delayed chunk in flight, run a fresh
        conn concurrently -- the reset's sleeping shuttler must exit without
        touching (or cross-delivering into) anything, and the new conn is clean."""
        conn1 = simnet_fd.SimFdConn(delay_fn=lambda: 0.05)   # positive delay -> in-flight
        conn2 = simnet_fd.SimFdConn(delay_fn=lambda: 0.0)
        out = {}

        def c1flow():
            try:
                conn1.a.sendall(b"aaaa")             # shuttler recv's it, sched_sleeps 0.05
            except simnet_fd.SimError:
                pass
            conn1.reset()                            # shuttler is mid-sleep -> checks flag on wake

        def c2reader():
            out["c2"] = conn2.b.recv_exact(4)

        def c2writer():
            conn2.a.sendall(b"bbbb")

        runloom_c.fiber(c1flow)
        runloom_c.fiber(c2reader)
        runloom_c.fiber(c2writer)
        runloom_c.run()
        conn1.close()
        conn2.close()
        self.assertEqual(out.get("c2"), b"bbbb",
                         "new conn corrupted by a mid-flight reset: %r" % out)

    def test_reset_deterministic(self):
        def scenario():
            runloom_c.sim_reset()
            conn = simnet_fd.SimFdConn(delay_fn=lambda: 0.0)
            out = {}

            def flow():
                conn.a.sendall(b"hello")
                out["first"] = conn.b.recv(3)
                conn.reset()
                try:
                    conn.b.recv(3)
                    out["second"] = "no-error"
                except simnet_fd.SimError:
                    out["second"] = "reset"

            runloom_c.fiber(flow)
            runloom_c.run()
            conn.close()
            return out.get("first"), out.get("second"), runloom_c._logical_ns()

        self.assertEqual(scenario(), scenario())

    def test_close_after_reset_idempotent(self):
        conn = simnet_fd.SimFdConn(delay_fn=lambda: 0.0)

        def flow():
            conn.reset()

        runloom_c.fiber(flow)
        runloom_c.run()
        conn.close()
        conn.close()          # idempotent, no raise


class TestSimBytesPartition(unittest.TestCase):
    """Increment P: a partition holds deliveries until a logical heal time; the
    hold is a logical sleeper (time compresses to it, no false reap)."""

    def test_partition_holds_until_heal(self):
        conn = simnet_fd.SimFdConn(delay_fn=lambda: 0.0)
        out = {}
        HEAL = 0.5

        def writer():
            out["t0"] = conn.logical_now()
            conn.partition_until_t(out["t0"] + HEAL)     # partition BEFORE sending
            conn.a.sendall(b"held")

        def reader():
            out["data"] = conn.b.recv_exact(4)
            out["arrived"] = conn.logical_now()

        d0 = runloom_c.count_deadlocked()
        t0 = time.monotonic()
        runloom_c.fiber(writer)
        runloom_c.fiber(reader)
        runloom_c.run()
        wall = time.monotonic() - t0
        conn.close()
        self.assertEqual(out.get("data"), b"held")
        self.assertEqual(runloom_c.count_deadlocked() - d0, 0,
                         "reader parked through the partition was falsely reaped")
        latency = out["arrived"] - out["t0"]
        self.assertTrue(HEAL - 0.01 <= latency <= HEAL + 0.01,
                        "delivered at logical +%.4fs, expected ~%.2fs (heal)" % (latency, HEAL))
        self.assertLess(wall, 2.0, "logical partition not compressed in wall time")

    def test_partition_preserves_order(self):
        conn = simnet_fd.SimFdConn(delay_fn=lambda: 0.0)
        out = {}

        def writer():
            conn.partition_until_t(conn.logical_now() + 0.2)
            conn.a.sendall(b"abcdef")

        def reader():
            out["data"] = conn.b.recv_exact(6)

        runloom_c.fiber(writer)
        runloom_c.fiber(reader)
        runloom_c.run()
        conn.close()
        self.assertEqual(out.get("data"), b"abcdef", "partition broke stream order")

    def test_partition_deterministic(self):
        def scenario():
            runloom_c.sim_reset()
            conn = simnet_fd.SimFdConn(delay_fn=lambda: 0.0)
            out = {}

            def writer():
                conn.partition_until_t(conn.logical_now() + 0.3)
                conn.a.sendall(b"data")

            def reader():
                out["data"] = conn.b.recv_exact(4)
                out["at"] = runloom_c._logical_ns()

            runloom_c.fiber(writer)
            runloom_c.fiber(reader)
            runloom_c.run()
            conn.close()
            return out.get("data"), out.get("at")

        self.assertEqual(scenario(), scenario())


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
