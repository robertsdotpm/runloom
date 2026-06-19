"""A Python signal handler that raises during a cooperative blocking call must
propagate out of THAT call, into the fiber's own try/except -- exactly as a
signal interrupting a real recv()/select() does -- not get swallowed by the
scheduler or carried out of runloom_c.run().

Regression test for the scheduler-grab bypass bug: commit 34efeb5 ("make
run_forever interruptible by signals") taught the idle scheduler to run a
pending Python signal handler and carry a raised exception out of run().  That
is right for an idle run_forever (Ctrl-C), but it also fired for a fiber
actively parked in a cooperative select()/recv()/accept(), stealing the
exception before the fiber's own eval loop -- and its try/except -- ever
saw it (CPython's verbatim test_select_interrupt_exc went red on epoll).

The fix delivers the raised handler exception INTO the parked fiber (it
restores it on resume from wait_fd and returns out of the blocking call), and
only carries it out of run() when no parked fiber can carry it (the idle /
sleep-only Ctrl-C case).  The delivery path is backend-independent, so these
run on epoll / kqueue / select alike.
"""
import signal
import socket
import time
import unittest

import runloom.monkey as monkey

monkey.patch()

import runloom_c  # noqa: E402  (after patch, like the rest of the suite)

HAVE_ALARM = hasattr(signal, "alarm")


class _Raise(Exception):
    pass


def _run_fiber(body):
    """Run `body` as a single fiber to completion; return whatever it put
    in the one-element list it is handed.  Surfaces an exception that escaped
    the fiber (and thus out of run()) as `escaped`."""
    box = {"result": None, "escaped": None}

    def wrapper():
        try:
            body(box)
        except BaseException as e:   # noqa: BLE001 -- record, don't swallow silently
            box["result"] = "ESCAPED_GOROUTINE"
            raise
    try:
        runloom_c.fiber(wrapper)
        runloom_c.run()
    except BaseException as e:       # noqa: BLE001
        box["escaped"] = e
    return box


@unittest.skipUnless(HAVE_ALARM, "signal.alarm() required")
class TestSignalInterruptsCooperativeCall(unittest.TestCase):
    def setUp(self):
        self._orig = signal.signal(signal.SIGALRM, self._handler)

    def tearDown(self):
        signal.alarm(0)
        signal.signal(signal.SIGALRM, self._orig)

    @staticmethod
    def _handler(signum, frame):
        raise _Raise

    def test_select_interrupted_raises_into_fiber(self):
        """A SIGALRM during a cooperative selectors.EpollSelector-style poll on
        a never-ready fd raises INTO the fiber (bounded re-probe wait)."""
        import selectors

        def body(box):
            sel = selectors.DefaultSelector()
            rd, wr = socket.socketpair()
            self.addCleanup(rd.close)
            self.addCleanup(wr.close)
            self.addCleanup(sel.close)
            sel.register(rd, selectors.EVENT_READ)
            signal.alarm(1)
            t = time.time()
            try:
                sel.select(30)
                box["result"] = "no-exception"
            except _Raise:
                box["result"] = "caught"
                box["dt"] = time.time() - t

        box = _run_fiber(body)
        self.assertEqual(box["result"], "caught",
                         "signal did not raise into the fiber's try/except "
                         "(escaped=%r)" % (box["escaped"],))
        self.assertIsNone(box["escaped"], "exception escaped out of run()")
        self.assertLess(box.get("dt", 99), 5.0)

    def test_recv_interrupted_raises_into_fiber(self):
        """A SIGALRM during a cooperative socket.recv() on a never-ready socket
        (infinite wait, no re-probe timer) raises INTO the fiber."""
        def body(box):
            rd, wr = socket.socketpair()
            self.addCleanup(rd.close)
            self.addCleanup(wr.close)
            signal.alarm(1)
            try:
                rd.recv(64)               # blocks forever; only the signal frees it
                box["result"] = "no-exception"
            except _Raise:
                box["result"] = "caught"

        box = _run_fiber(body)
        self.assertEqual(box["result"], "caught",
                         "signal did not raise into recv() (escaped=%r)"
                         % (box["escaped"],))
        self.assertIsNone(box["escaped"], "exception escaped out of run()")

    def test_accept_interrupted_raises_into_fiber(self):
        """A SIGALRM during a cooperative socket.accept() (infinite wait) raises
        INTO the fiber rather than out of run()."""
        def body(box):
            srv = socket.socket()
            srv.bind(("127.0.0.1", 0))
            srv.listen(1)
            self.addCleanup(srv.close)
            signal.alarm(1)
            try:
                srv.accept()
                box["result"] = "no-exception"
            except _Raise:
                box["result"] = "caught"

        box = _run_fiber(body)
        self.assertEqual(box["result"], "caught",
                         "signal did not raise into accept() (escaped=%r)"
                         % (box["escaped"],))
        self.assertIsNone(box["escaped"], "exception escaped out of run()")

    def test_idle_signal_still_carried_out_of_run(self):
        """The complementary contract (the 34efeb5 feature): when NOTHING is
        parked in a cooperative wait to carry it -- a fiber busy in a
        cooperative sleep -- a raised signal handler still surfaces (here, out
        of the fiber).  Guards against over-correcting the bypass fix into
        swallowing signals on the genuinely-idle path."""
        def body(box):
            signal.alarm(1)
            try:
                # cooperative sleep via the monkey-patched time.sleep
                time.sleep(30)
                box["result"] = "no-exception"
            except _Raise:
                box["result"] = "caught"

        box = _run_fiber(body)
        # Either delivered into the fiber's try/except, or carried out of
        # run() -- both are "the signal was not lost".  It must not hang or be
        # swallowed.
        self.assertTrue(box["result"] == "caught" or isinstance(box["escaped"], _Raise),
                        "signal was lost on the idle/sleep path "
                        "(result=%r escaped=%r)" % (box["result"], box["escaped"]))


if __name__ == "__main__":
    unittest.main()
