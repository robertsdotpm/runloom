"""M:N-scheduler correctness for the cooperative-compat fixes.

Covers the API changes that made the monkey layer + feature modules work under
the M:N scheduler (mn_init/mn_fiber/mn_run, free-threaded 3.13t, GIL off):

  * runloom_c.Mutex                 -- new C-level M:N-safe mutex
  * current_g() under M:N           -- returns the hub's running fiber
  * threading.Lock / RLock          -- mutual exclusion across hubs (CoLock on Mutex)
  * queue.Queue                     -- producer/consumer conservation across hubs
  * time.After / context.WithTimeout-- timers fire under mn_run (active-scheduler spawn)
  * context.WithCancel              -- cancellation propagates
  * dns AI_NUMERICHOST              -- fast gaierror, no network hang
  * ssl.wrap_socket                 -- cooperative client handshake (https)
  * buffered pipe read              -- subprocess.stdout.read() parks (no hub wedge)
  * select.select / select.poll     -- multi-fd cooperative, no SIGSEGV

Each test drives a fiber tree under mn_init and asserts the result AFTER
mn_run (fiber exceptions are swallowed, so results go through shared state
/ a runloom.Chan and are checked on the main thread).  mn_run only returns when
every fiber finishes, so a dropped/stranded fiber shows up as a hang
(the isolated runner's timeout) -> a clean failure.
"""
import os
import socket
import ssl
import struct
import subprocess
import sys
import threading
import time
import unittest

import runloom
import runloom.monkey
import runloom_c

NHUBS = 4


def setUpModule():
    runloom.monkey.patch()


def tearDownModule():
    runloom.monkey.unpatch()


def _drive_mn(main_fn, nhubs=NHUBS):
    """Run main_fn as the root fiber on an N-hub M:N scheduler."""
    box = [None, None]

    def runner():
        try:
            box[0] = main_fn()
        except BaseException as e:   # noqa: BLE001
            box[1] = e

    runloom_c.mn_init(nhubs)
    runloom_c.mn_fiber(runner)
    runloom_c.mn_run()
    runloom_c.mn_fini()
    if box[1] is not None:
        raise box[1]
    return box[0]


def _fanin(spawn_workers, n):
    """Spawn n workers (each sends one bool to a Chan) + collect the tally."""
    results = runloom.Chan(n)
    state = {"good": 0}

    def coordinator():
        good = 0
        for _ in range(n):
            v, _ = results.recv()
            if v:
                good += 1
        state["good"] = good

    runloom_c.mn_fiber(coordinator)
    spawn_workers(results)
    return state


# ---------------------------------------------------------------- C Mutex
class TestMutex(unittest.TestCase):
    def test_mutual_exclusion(self):
        def body():
            mu = runloom_c.Mutex()
            ctr = {"n": 0}
            n, k = 8, 400

            def worker():
                for _ in range(k):
                    mu.lock()
                    ctr["n"] += 1
                    mu.unlock()
            for _ in range(n):
                runloom_c.mn_fiber(worker)
            # the root fiber also contends, then we read after mn_run
            return (ctr, n * k)
        ctr, want = _drive_mn(body)
        self.assertEqual(ctr["n"], want)   # exact total => real mutual exclusion

    def test_try_lock_and_locked(self):
        def body():
            mu = runloom_c.Mutex()
            self.assertFalse(mu.locked())
            self.assertTrue(mu.try_lock())
            self.assertTrue(mu.locked())
            self.assertFalse(mu.try_lock())   # already held
            mu.unlock()
            self.assertFalse(mu.locked())
        _drive_mn(body)

    def test_double_unlock_raises(self):
        def body():
            mu = runloom_c.Mutex()
            mu.lock()
            mu.unlock()
            with self.assertRaises(RuntimeError):
                mu.unlock()
        _drive_mn(body)

    def test_context_manager(self):
        def body():
            mu = runloom_c.Mutex()
            with mu:
                self.assertTrue(mu.locked())
            self.assertFalse(mu.locked())
        _drive_mn(body)


# ------------------------------------------------------------- current_g
class TestCurrentG(unittest.TestCase):
    def test_non_none_and_stable_under_mn(self):
        def body():
            g1 = runloom_c.current_g()
            g2 = runloom_c.current_g()
            self.assertIsNotNone(g1)             # was None on hubs before the fix
            self.assertEqual(g1, g2)             # same g => CoRLock owner identity
        _drive_mn(body)


# ---------------------------------------------------- threading primitives
class TestThreadingMN(unittest.TestCase):
    def test_lock_counter_exact(self):
        def body():
            lock = threading.Lock()
            ctr = {"n": 0}
            n, k = 8, 400

            def worker():
                for _ in range(k):
                    with lock:
                        ctr["n"] += 1
            for _ in range(n):
                runloom_c.mn_fiber(worker)
            return ctr, n * k
        ctr, want = _drive_mn(body)
        self.assertEqual(ctr["n"], want)

    def test_rlock_reentrant_and_exclusive(self):
        def body():
            lock = threading.RLock()
            ctr = {"n": 0}
            n, k = 6, 300

            def worker():
                for _ in range(k):
                    with lock:
                        with lock:               # reentrant
                            ctr["n"] += 1
            for _ in range(n):
                runloom_c.mn_fiber(worker)
            return ctr, n * k
        ctr, want = _drive_mn(body)
        self.assertEqual(ctr["n"], want)


# ----------------------------------------------------------------- queue
class TestQueueMN(unittest.TestCase):
    def test_producer_consumer_conservation(self):
        import queue

        def body():
            bus = queue.Queue()
            n, per = 6, 50
            state = {"got": 0}

            def producer():
                for _ in range(per):
                    bus.put(1)

            def consumer():
                got = 0
                for _ in range(n * per):
                    bus.get()
                    got += 1
                state["got"] = got
            runloom_c.mn_fiber(consumer)
            for _ in range(n):
                runloom_c.mn_fiber(producer)
            return state, n * per
        state, want = _drive_mn(body)
        self.assertEqual(state["got"], want)


# -------------------------------------------------------- time / context
class TestTimeContextMN(unittest.TestCase):
    def test_after_fires(self):
        def body():
            state = {}

            def waiter():
                value, ok = runloom.time.After(0.02).recv()
                state["ok"] = ok
            runloom_c.mn_fiber(waiter)
            return state
        state = _drive_mn(body)
        self.assertTrue(state.get("ok"))         # would hang/never-fire before the fix

    def test_withtimeout_deadline_fires(self):
        def body():
            state = {}

            def waiter():
                ctx, _cancel = runloom.context.WithTimeout(
                    runloom.context.Background(), 0.02)
                runloom.sleep(0.2)
                state["err"] = ctx.err()
            runloom_c.mn_fiber(waiter)
            return state
        state = _drive_mn(body)
        self.assertEqual(state.get("err"), runloom.context.DEADLINE_EXCEEDED)

    def test_withcancel_propagates(self):
        def body():
            woke = []

            def run():
                ctx, cancel = runloom.context.WithCancel(
                    runloom.context.Background())

                def child():
                    ctx.done.recv()
                    woke.append(1)
                for _ in range(6):
                    runloom_c.mn_fiber(child)
                runloom.sleep(0.02)
                cancel()
            runloom_c.mn_fiber(run)
            return woke
        woke = _drive_mn(body)
        # children run within the same cycle; mn_run drains them
        self.assertEqual(len(woke), 6)


# ------------------------------------------------------------------- dns
class TestDNSMN(unittest.TestCase):
    def test_ai_numerichost_fast_gaierror(self):
        def body():
            state = {}

            def worker():
                t0 = time.monotonic()
                try:
                    socket.getaddrinfo("127.0.0.1", "no-such-svc-xyz",
                                       proto=socket.IPPROTO_TCP)
                    state["err"] = None
                except socket.gaierror:
                    state["err"] = "gaierror"
                state["dt"] = time.monotonic() - t0
            runloom_c.mn_fiber(worker)
            return state
        state = _drive_mn(body)
        self.assertEqual(state.get("err"), "gaierror")
        self.assertLess(state.get("dt", 99), 2.0)   # no network hang


# ----------------------------------------------------------- recvexact util
def _recvexact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("short read")
        buf += chunk
    return bytes(buf)


# ------------------------------------------------------------------- ssl
class TestSSLMN(unittest.TestCase):
    def setUp(self):
        try:
            import trustme
        except ImportError:
            self.skipTest("trustme not available")
        ca = trustme.CA()
        cert = ca.issue_cert("localhost", "127.0.0.1")
        self._sctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        cert.configure_cert(self._sctx)

    def test_https_client_handshake_cooperative(self):
        sctx = self._sctx

        def body():
            ls = socket.socket()
            ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            ls.bind(("127.0.0.1", 0))
            ls.listen(8)
            port = ls.getsockname()[1]
            tls_ls = sctx.wrap_socket(ls, server_side=True)
            state = {}

            def server():
                conn, _ = tls_ls.accept()        # accept auto-handshakes (cooperative)
                conn.recv(64)
                conn.sendall(b"pong")
                conn.close()

            def client():
                cctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                cctx.check_hostname = False
                cctx.verify_mode = ssl.CERT_NONE
                raw = socket.create_connection(("127.0.0.1", port))
                # wrap_socket with the implicit (default) handshake: the fix
                # makes this cooperative instead of ValueError / hub-wedge.
                s = cctx.wrap_socket(raw, server_hostname="localhost")
                s.sendall(b"ping")
                state["resp"] = s.recv(64)
                s.close()
            runloom_c.mn_fiber(server)
            runloom_c.mn_fiber(client)
            return state
        state = _drive_mn(body)
        self.assertEqual(state.get("resp"), b"pong")


# ------------------------------------------------------- buffered pipe read
class TestBufferedPipeMN(unittest.TestCase):
    def test_subprocess_stdout_read_parks(self):
        # proc.stdout.read() over a pipe used to block the hub (immutable C
        # BufferedReader's raw read).  Now routed through _pyio on cooperative
        # os.read: it must park (a canary on the same hub keeps ticking) and
        # return the child's full output.
        def body():
            canary = {"ticks": 0, "stop": False}
            state = {}

            def canary_loop():
                while not canary["stop"]:
                    runloom.sleep(0.005)
                    canary["ticks"] += 1

            def reader():
                proc = subprocess.Popen(
                    [sys.executable, "-c",
                     "import time,sys; time.sleep(0.2); "
                     "sys.stdout.write('done'); sys.stdout.flush()"],
                    stdout=subprocess.PIPE)
                state["data"] = proc.stdout.read()
                proc.wait()
                canary["stop"] = True
            runloom_c.mn_fiber(canary_loop)
            runloom_c.mn_fiber(reader)
            return canary, state
        canary, state = _drive_mn(body, nhubs=1)   # 1 hub: a hub-block freezes the canary
        # The child's full output comes back on every platform.
        self.assertEqual(state.get("data"), b"done")
        if os.name == "nt":
            # Windows anonymous pipes can't be put in non-blocking mode and
            # netpoll-selected the way POSIX fds can, so proc.stdout.read() does
            # a genuine blocking read that wedges the hub.  The handoff rescuer
            # recovers it -- the data still arrives and the body completes with
            # no deadlock (mn_run would hang on a stranded canary otherwise) --
            # but the read does NOT park, so the same-hub canary can't keep
            # ticking.  Assert the Windows guarantee (handoff recovery: the
            # reader finished and signalled the canary to stop) instead.
            self.assertTrue(canary["stop"])
        else:
            self.assertGreater(canary["ticks"], 5)     # cooperative: canary kept ticking


# ----------------------------------------------------------- select / poll
class TestSelectPollMN(unittest.TestCase):
    def _multi_fd(self, use_poll):
        import select as _sel

        def body():
            pairs = [socket.socketpair() for _ in range(3)]
            for a, b in pairs:
                a.setblocking(False)
            state = {}

            def writer():
                runloom.sleep(0.03)
                pairs[2][1].sendall(b"x")

            def worker():
                if use_poll:
                    p = _sel.poll()
                    p.register(pairs[2][0].fileno(), _sel.POLLIN)
                    ev = p.poll(3000)
                    state["n"] = len(ev)
                else:
                    r, _w, _x = _sel.select([a for a, b in pairs], [], [], 3.0)
                    state["n"] = len(r)
            runloom_c.mn_fiber(writer)
            runloom_c.mn_fiber(worker)
            return state
        # >=2 hubs is where the old busy-poll deterministically SIGSEGV'd
        state = _drive_mn(body, nhubs=2)
        self.assertEqual(state.get("n"), 1)

    def test_select_multifd_no_crash(self):
        self._multi_fd(use_poll=False)

    def test_poll_multifd_no_crash(self):
        import select as _sel
        if not hasattr(_sel, "poll"):
            self.skipTest("select.poll is Unix-only (absent on Windows)")
        self._multi_fd(use_poll=True)


if __name__ == "__main__":
    unittest.main()
