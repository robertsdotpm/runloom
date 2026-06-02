"""Cooperative signal waits: signal.sigwait / sigtimedwait / pause.

Adapted from CPython Lib/test/test_signal.py (WaitSignalTests.test_sigwait,
test_sigtimedwait, test_sigtimedwait_timeout, and the pause()-based wakeup
tests).  The contract a cooperative version must keep:

  * sigwait/sigtimedwait require the signals to be *blocked* (pthread_sigmask)
    so they queue as pending; the wait reaps the pending signal and returns
    its number / siginfo;
  * sigtimedwait returns None when the timeout elapses with nothing pending;
  * a blocked wait yields the scheduler -- a sibling goroutine keeps running;
  * pause() returns once a handled signal is caught.

These run under the single-threaded scheduler (pygo_core.go/run on the main
thread), which is where signal masks are stable and set_wakeup_fd is usable.
"""
import os
import platform
import time
import unittest

import pygo
import pygo.monkey
import pygo_core

try:
    import signal
except ImportError:
    signal = None

_IS_WINDOWS = platform.system() == "Windows"
_HAVE_SIGWAIT = signal is not None and hasattr(signal, "sigwait")
_HAVE_SIGTIMEDWAIT = signal is not None and hasattr(signal, "sigtimedwait")
_HAVE_SIGMASK = signal is not None and hasattr(signal, "pthread_sigmask")
# SIGUSR1 is a safe, app-defined signal (no default side effects we care
# about beyond terminating an unhandled process).
SIG = getattr(signal, "SIGUSR1", None)


def _drive(fn):
    box = [None, None]

    def runner():
        try:
            box[0] = fn()
        except BaseException as e:   # noqa: BLE001
            box[1] = e

    pygo_core.go(runner)
    pygo_core.run()
    if box[1] is not None:
        raise box[1]
    return box[0]


def setUpModule():
    pygo.monkey.patch()


def tearDownModule():
    pygo.monkey.unpatch()


@unittest.skipIf(SIG is None, "no SIGUSR1")
@unittest.skipUnless(_HAVE_SIGMASK, "no pthread_sigmask")
@unittest.skipUnless(_HAVE_SIGWAIT, "no sigwait")
# sigwait/sigwaitinfo are only made *cooperative* via a zero-timeout
# sigtimedwait poll; without sigtimedwait (e.g. macOS) the shim falls back to
# the blocking sigwait, which can't yield to the goroutine that sends the
# signal -- so the cooperative tests only apply where sigtimedwait exists.
@unittest.skipUnless(_HAVE_SIGTIMEDWAIT, "no sigtimedwait (sigwait not cooperative)")
class TestSigwait(unittest.TestCase):
    def test_sigwait_returns_signal_and_yields(self):
        def body():
            ticks = []
            done = {"v": False}
            signal.pthread_sigmask(signal.SIG_BLOCK, {SIG})
            try:
                def sender():
                    pygo.sleep(0.02)
                    os.kill(os.getpid(), SIG)

                def ticker():
                    while not done["v"]:
                        ticks.append(1)
                        pygo.sleep(0.002)

                pygo_core.go(sender)
                pygo_core.go(ticker)
                sig = signal.sigwait({SIG})
                done["v"] = True
            finally:
                signal.pthread_sigmask(signal.SIG_UNBLOCK, {SIG})
            return sig, len(ticks)
        sig, ticks = _drive(body)
        self.assertEqual(sig, SIG)
        self.assertGreaterEqual(ticks, 1)   # the runtime kept scheduling

    def test_sigwait_retries_eintr_fault(self):
        """Fault injection: sigtimedwait raising EINTR mid-poll must be retried,
        not propagated -- the sigwait still reaps the pending signal."""
        import pygo.monkey as _m
        real = _m.signals._orig_sigtimedwait
        st = {"n": 0}

        def flaky(sigset, timeout):
            if st["n"] < 2:
                st["n"] += 1
                raise InterruptedError()
            return real(sigset, timeout)

        def body():
            signal.pthread_sigmask(signal.SIG_BLOCK, {SIG})
            _m.signals._orig_sigtimedwait = flaky
            try:
                def sender():
                    pygo.sleep(0.02)
                    os.kill(os.getpid(), SIG)
                pygo_core.go(sender)
                return signal.sigwait({SIG})
            finally:
                _m.signals._orig_sigtimedwait = real
                signal.pthread_sigmask(signal.SIG_UNBLOCK, {SIG})
        self.assertEqual(_drive(body), SIG)
        self.assertGreaterEqual(st["n"], 1)

    @unittest.skipUnless(hasattr(signal, "sigwaitinfo"), "no sigwaitinfo")
    def test_sigwaitinfo_returns_siginfo_and_yields(self):
        """sigwaitinfo returns the full struct_siginfo (vs sigwait's signo)."""
        def body():
            ticks = []
            done = {"v": False}
            signal.pthread_sigmask(signal.SIG_BLOCK, {SIG})
            try:
                def sender():
                    pygo.sleep(0.02)
                    os.kill(os.getpid(), SIG)

                def ticker():
                    while not done["v"]:
                        ticks.append(1)
                        pygo.sleep(0.002)

                pygo_core.go(sender)
                pygo_core.go(ticker)
                info = signal.sigwaitinfo({SIG})
                done["v"] = True
            finally:
                signal.pthread_sigmask(signal.SIG_UNBLOCK, {SIG})
            return info.si_signo, len(ticks)
        signo, ticks = _drive(body)
        self.assertEqual(signo, SIG)
        self.assertGreaterEqual(ticks, 1)


@unittest.skipIf(SIG is None, "no SIGUSR1")
@unittest.skipUnless(_HAVE_SIGMASK, "no pthread_sigmask")
@unittest.skipUnless(_HAVE_SIGTIMEDWAIT, "no sigtimedwait")
class TestSigtimedwait(unittest.TestCase):
    def test_receives_pending_signal(self):
        def body():
            signal.pthread_sigmask(signal.SIG_BLOCK, {SIG})
            try:
                def sender():
                    pygo.sleep(0.02)
                    os.kill(os.getpid(), SIG)

                pygo_core.go(sender)
                info = signal.sigtimedwait({SIG}, 5.0)
            finally:
                signal.pthread_sigmask(signal.SIG_UNBLOCK, {SIG})
            return None if info is None else info.si_signo
        self.assertEqual(_drive(body), SIG)

    def test_timeout_returns_none_and_yields(self):
        def body():
            ticks = []
            done = {"v": False}
            signal.pthread_sigmask(signal.SIG_BLOCK, {SIG})
            try:
                def ticker():
                    while not done["v"]:
                        ticks.append(1)
                        pygo.sleep(0.003)

                pygo_core.go(ticker)
                info = signal.sigtimedwait({SIG}, 0.08)   # nothing pending
                done["v"] = True
            finally:
                signal.pthread_sigmask(signal.SIG_UNBLOCK, {SIG})
            return info, len(ticks)
        info, ticks = _drive(body)
        self.assertIsNone(info)
        self.assertGreaterEqual(ticks, 1)

    def test_immediate_pending_signal(self):
        """A signal already pending before the wait is reaped at once."""
        def body():
            signal.pthread_sigmask(signal.SIG_BLOCK, {SIG})
            try:
                os.kill(os.getpid(), SIG)        # queue it before waiting
                info = signal.sigtimedwait({SIG}, 5.0)
            finally:
                signal.pthread_sigmask(signal.SIG_UNBLOCK, {SIG})
            return None if info is None else info.si_signo
        self.assertEqual(_drive(body), SIG)


@unittest.skipIf(SIG is None, "no SIGUSR1")
@unittest.skipUnless(hasattr(signal, "pause"), "no signal.pause")
@unittest.skipUnless(hasattr(signal, "set_wakeup_fd"), "no set_wakeup_fd")
class TestPause(unittest.TestCase):
    def test_pause_returns_on_handled_signal_and_yields(self):
        """pause() must return once a handled signal is caught, while a
        sibling goroutine keeps running in the meantime."""
        caught = {"n": 0}

        def handler(signum, frame):
            caught["n"] += 1

        old = signal.signal(SIG, handler)
        try:
            def body():
                ticks = []
                done = {"v": False}

                def ticker():
                    while not done["v"]:
                        ticks.append(1)
                        pygo.sleep(0.003)

                def sender():
                    pygo.sleep(0.03)
                    os.kill(os.getpid(), SIG)

                pygo_core.go(ticker)
                pygo_core.go(sender)
                signal.pause()           # parks on the wakeup-fd pipe
                done["v"] = True
                return len(ticks)
            ticks = _drive(body)
        finally:
            signal.signal(SIG, old)
        self.assertGreaterEqual(caught["n"], 1)
        self.assertGreaterEqual(ticks, 1)


if __name__ == "__main__":
    unittest.main()
