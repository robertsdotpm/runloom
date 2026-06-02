"""Cooperative process waits: subprocess.run / Popen.communicate / Popen.wait
and os.waitpid / os.wait / os.waitid / os.system.

Adapted from CPython's Lib/test/test_subprocess.py and the os.wait* coverage
in Lib/test/test_os.py.  This is the end-to-end pay-off of the `selectors`,
`process` and `subprocess` categories together: subprocess.run() blocks in
communicate(), which uses a selectors.PollSelector plus pipe reads -- none of
which the original monkey layer made cooperative.

Coverage:
  * return codes: returncode for clean exit, CalledProcessError on nonzero
    with check=True, captured stdout/stderr, WIFEXITED/WEXITSTATUS and
    WIFSIGNALED/WTERMSIG from os.waitpid;
  * fault injection: nonzero exit, death by signal (SIGKILL), TimeoutExpired,
    ECHILD with no children, WNOHANG when the child is still running;
  * cooperation: a sibling goroutine keeps running while run()/wait()/system()
    are in flight (they do not freeze the scheduler).

POSIX-only where the API is POSIX-only.
"""
import errno
import os
import platform
import signal
import subprocess
import sys
import time
import unittest

import pygo
import pygo.monkey
import pygo_core

_IS_WINDOWS = platform.system() == "Windows"


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


def _sibling_counter(store, n=4, step=0.005):
    """Spawn a goroutine that ticks while the test blocks, so we can prove
    the blocking call yielded the scheduler."""
    def sib():
        for _ in range(n):
            time.sleep(step)
            store.append(1)
    pygo_core.go(sib)


class TestSubprocessRun(unittest.TestCase):
    def test_run_capture_and_returncode(self):
        def body():
            ticks = []
            _sibling_counter(ticks)
            cp = subprocess.run(
                [sys.executable, "-c",
                 "import time,sys; time.sleep(0.03); "
                 "sys.stdout.write('out'); sys.stderr.write('err')"],
                capture_output=True, text=True)
            return cp.returncode, cp.stdout, cp.stderr, len(ticks)
        rc, out, err, ticks = _drive(body)
        self.assertEqual(rc, 0)
        self.assertEqual(out, "out")
        self.assertEqual(err, "err")
        self.assertGreaterEqual(ticks, 1)   # sibling ran during communicate()

    def test_run_nonzero_check_raises(self):
        def body():
            with self.assertRaises(subprocess.CalledProcessError) as cm:
                subprocess.run([sys.executable, "-c", "import sys; sys.exit(7)"],
                               check=True)
            return cm.exception.returncode
        self.assertEqual(_drive(body), 7)

    def test_check_output(self):
        def body():
            return subprocess.check_output(
                [sys.executable, "-c", "print('hi-there')"], text=True)
        self.assertEqual(_drive(body).strip(), "hi-there")

    def test_run_timeout_raises(self):
        def body():
            t0 = time.monotonic()
            with self.assertRaises(subprocess.TimeoutExpired):
                subprocess.run([sys.executable, "-c", "import time; time.sleep(5)"],
                               timeout=0.1)
            return time.monotonic() - t0
        dt = _drive(body)
        self.assertLess(dt, 4)              # timed out fast, did not wait 5s

    def test_large_output_communicate(self):
        """communicate() must drain a big pipe without deadlocking -- the
        classic fill-the-pipe-buffer hazard, here over cooperative selectors."""
        def body():
            cp = subprocess.run(
                [sys.executable, "-c",
                 "import sys; sys.stdout.write('A'*200000)"],
                capture_output=True)
            return len(cp.stdout)
        self.assertEqual(_drive(body), 200000)

    def test_stdin_roundtrip(self):
        def body():
            cp = subprocess.run(
                [sys.executable, "-c",
                 "import sys; sys.stdout.write(sys.stdin.read().upper())"],
                input="hello", capture_output=True, text=True)
            return cp.stdout
        self.assertEqual(_drive(body), "HELLO")


@unittest.skipIf(_IS_WINDOWS, "POSIX os.wait* semantics")
class TestOsWait(unittest.TestCase):
    def test_waitpid_exit_status(self):
        def body():
            pid = os.fork()
            if pid == 0:
                os._exit(42)
            ticks = []
            _sibling_counter(ticks)
            wpid, status = os.waitpid(pid, 0)
            return (wpid == pid, os.WIFEXITED(status),
                    os.WEXITSTATUS(status), len(ticks))
        ok, exited, code, ticks = _drive(body)
        self.assertTrue(ok)
        self.assertTrue(exited)
        self.assertEqual(code, 42)
        self.assertGreaterEqual(ticks, 1)

    def test_waitpid_killed_by_signal(self):
        def body():
            pid = os.fork()
            if pid == 0:
                time.sleep(10)
                os._exit(0)
            time.sleep(0.02)
            os.kill(pid, signal.SIGKILL)
            wpid, status = os.waitpid(pid, 0)
            return (wpid == pid, os.WIFSIGNALED(status), os.WTERMSIG(status))
        ok, signaled, sig = _drive(body)
        self.assertTrue(ok)
        self.assertTrue(signaled)
        self.assertEqual(sig, signal.SIGKILL)

    def test_waitpid_wnohang_passthrough(self):
        """WNOHANG must NOT be turned into a blocking poll: a still-running
        child returns (0, 0) immediately."""
        def body():
            pid = os.fork()
            if pid == 0:
                time.sleep(0.2)
                os._exit(0)
            r = os.waitpid(pid, os.WNOHANG)   # child still alive
            # reap it properly so we don't leak a zombie
            os.waitpid(pid, 0)
            return r
        self.assertEqual(_drive(body), (0, 0))

    def test_waitpid_echild(self):
        def body():
            with self.assertRaises(ChildProcessError) as cm:
                os.waitpid(-1, 0)            # no children at all
            return cm.exception.errno
        self.assertEqual(_drive(body), errno.ECHILD)

    def test_os_wait_any_child(self):
        def body():
            pid = os.fork()
            if pid == 0:
                time.sleep(0.02)
                os._exit(5)
            wpid, status = os.wait()
            return wpid == pid, os.WEXITSTATUS(status)
        ok, code = _drive(body)
        self.assertTrue(ok)
        self.assertEqual(code, 5)

    @unittest.skipUnless(hasattr(os, "waitid"), "no os.waitid")
    def test_waitid(self):
        def body():
            pid = os.fork()
            if pid == 0:
                time.sleep(0.02)
                os._exit(9)
            info = os.waitid(os.P_PID, pid, os.WEXITED)
            return info.si_pid == pid, info.si_status
        ok, status = _drive(body)
        self.assertTrue(ok)
        self.assertEqual(status, 9)


class TestOsSystem(unittest.TestCase):
    def test_system_returncode_and_cooperation(self):
        def body():
            ticks = []
            _sibling_counter(ticks)
            rc = os.system(sys.executable + " -c \"import sys; sys.exit(0)\"")
            return rc, len(ticks)
        rc, ticks = _drive(body)
        self.assertEqual(rc, 0)
        self.assertGreaterEqual(ticks, 1)

    @unittest.skipIf(_IS_WINDOWS, "POSIX exit-status encoding")
    def test_system_nonzero(self):
        def body():
            rc = os.system(sys.executable + " -c \"import sys; sys.exit(3)\"")
            return os.waitstatus_to_exitcode(rc)
        self.assertEqual(_drive(body), 3)


class TestPopenWait(unittest.TestCase):
    def test_wait_cooperative(self):
        def body():
            ticks = []
            _sibling_counter(ticks, n=5)
            p = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(0.04)"])
            rc = p.wait()
            return rc, len(ticks)
        rc, ticks = _drive(body)
        self.assertEqual(rc, 0)
        self.assertGreaterEqual(ticks, 1)

    def test_wait_timeout(self):
        def body():
            p = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(5)"])
            with self.assertRaises(subprocess.TimeoutExpired):
                p.wait(timeout=0.1)
            p.kill()
            p.wait()
            return True
        self.assertTrue(_drive(body))


@unittest.skipUnless(hasattr(os, "fork"), "no os.fork")
class TestPidfdReaping(unittest.TestCase):
    """The pidfd fast path: os.pidfd_open(pid) yields an fd that becomes
    readable on child exit, so the wait parks on netpoll instead of busy-
    polling.  Adapted from CPython Lib/test/test_os.py (test_pidfd_open) and
    the stop/continue semantics in the kernel's waitid(2)/wait(2) contract --
    a pidfd only signals *termination*, so WUNTRACED must keep the poll loop.
    """

    @unittest.skipUnless(pygo.monkey._HAVE_PIDFD, "no os.pidfd_open")
    def test_pidfd_open_basics(self):
        def body():
            pid = os.fork()
            if pid == 0:
                time.sleep(10)
                os._exit(0)
            # a live child -> a real, pollable fd
            pfd = pygo.monkey._pidfd_open(pid)
            ok = pfd is not None and pfd >= 0
            if pfd is not None:
                os.close(pfd)
            # -1 ("any child") and 0 are not single positive pids -> None
            none_for_any = pygo.monkey._pidfd_open(-1)
            none_for_zero = pygo.monkey._pidfd_open(0)
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
            return ok, none_for_any, none_for_zero
        ok, none_any, none_zero = _drive(body)
        self.assertTrue(ok)
        self.assertIsNone(none_any)
        self.assertIsNone(none_zero)

    def test_pidfd_wait_yields_to_sibling(self):
        """os.waitpid(pid, 0) on a child that exits after a delay must let a
        sibling goroutine make progress -- i.e. it parks, never spins."""
        def body():
            pid = os.fork()
            if pid == 0:
                time.sleep(0.05)
                os._exit(7)
            ticks = []
            _sibling_counter(ticks, n=6)
            wpid, status = os.waitpid(pid, 0)
            return wpid == pid, os.WEXITSTATUS(status), len(ticks)
        ok, code, ticks = _drive(body)
        self.assertTrue(ok)
        self.assertEqual(code, 7)
        self.assertGreaterEqual(ticks, 1)

    def test_sequential_waits_no_fd_leak(self):
        """Reaping several children in a row must not leak the pidfd or wedge
        the netpoll registration (each open/close is balanced)."""
        def body():
            codes = []
            for i in range(5):
                pid = os.fork()
                if pid == 0:
                    time.sleep(0.01)
                    os._exit(i)
                _wpid, status = os.waitpid(pid, 0)
                codes.append(os.WEXITSTATUS(status))
            return codes
        self.assertEqual(_drive(body), [0, 1, 2, 3, 4])

    @unittest.skipUnless(hasattr(os, "WUNTRACED"), "no WUNTRACED")
    def test_wuntraced_reports_stop_via_pollpath(self):
        """A stopped (not terminated) child is reported by waitpid(WUNTRACED).
        pidfd can't see stop events, so this must fall back to the poll loop
        and still observe the SIGSTOP -- validating the _PIDFD_INCOMPATIBLE
        gate."""
        def body():
            pid = os.fork()
            if pid == 0:
                time.sleep(10)
                os._exit(0)
            time.sleep(0.02)
            os.kill(pid, signal.SIGSTOP)
            wpid, status = os.waitpid(pid, os.WUNTRACED)
            stopped = os.WIFSTOPPED(status)
            stopsig = os.WSTOPSIG(status) if stopped else None
            # clean up: continue then kill then reap
            os.kill(pid, signal.SIGCONT)
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
            return wpid == pid, stopped, stopsig
        ok, stopped, stopsig = _drive(body)
        self.assertTrue(ok)
        self.assertTrue(stopped)
        self.assertEqual(stopsig, signal.SIGSTOP)


if __name__ == "__main__":
    unittest.main()
