"""Fork safety: after os.fork() the child keeps only the forking thread, so
the M:N hub threads and the blocking-offload workers are gone.  pygo registers
an os.register_at_fork(after_in_child=...) handler that resets the runtime so
the child neither hangs (run/mn_run waiting on dead hubs) nor deadlocks on an
inherited held lock, and gets its own netpoll fd.

These tests cover the SUPPORTED cases: a child that runs the single-thread
scheduler / pygo.aio (the multiprocessing-fork and pre-fork-server pattern),
and a child that starts a brand-new M:N scheduler when the parent never used
one.  Re-initialising M:N *inside* a fork-child of an already-active M:N parent
is NOT supported (use forkserver/spawn, or run single-thread in the child).
"""
import os
import sys
import time
import unittest

sys.path.insert(0, "src")

import pygo
import pygo_core


def run_child(child_fn, timeout=8.0):
    """Fork, run child_fn() in the child (its return int is the exit code),
    wait up to `timeout` for it, and return the exit code.  Raises if the
    child hangs (killed) -- that is the deadlock this whole module guards."""
    pid = os.fork()
    if pid == 0:
        code = 99
        try:
            code = int(child_fn())
        except BaseException as exc:  # noqa: BLE001
            sys.stderr.write("child exc: %r\n" % (exc,))
            code = 98
        finally:
            os._exit(code)
    deadline = time.monotonic() + timeout
    while True:
        done, status = os.waitpid(pid, os.WNOHANG)
        if done == pid:
            return os.waitstatus_to_exitcode(status)
        if time.monotonic() > deadline:
            os.kill(pid, 9)
            os.waitpid(pid, 0)
            raise AssertionError("child hung (fork deadlock)")
        time.sleep(0.01)


class TestSingleThreadChild(unittest.TestCase):
    def test_child_runs_fresh_scheduler(self):
        # Parent exercises the single-thread scheduler (so netpoll is inited),
        # then forks; the child must be able to run its own scheduler.
        def warm():
            pygo.sleep(0.005)
        pygo.run(warm)

        def child():
            out = []
            def w():
                out.append(1)
            for _ in range(4):
                pygo.go(w)
            pygo.run()
            return 0 if len(out) == 4 else 3

        rc = run_child(child)
        self.assertEqual(rc, 0)


class TestAioChildAfterMNParent(unittest.TestCase):
    def test_child_runs_fresh_aio_loop(self):
        import asyncio
        import pygo.aio as paio

        pygo_core.mn_init(4)
        try:
            for _ in range(8):
                pygo_core.mn_go(lambda: pygo.sleep(0.3))
            time.sleep(0.02)

            def child():
                async def main():
                    await asyncio.sleep(0.02)
                    return 42
                return 0 if paio.run(main()) == 42 else 5

            rc = run_child(child)
            self.assertEqual(rc, 0)
        finally:
            pygo_core.mn_run()
            pygo_core.mn_fini()

    def test_mn_run_in_child_does_not_hang(self):
        # The originally-reproduced deadlock: a child that calls mn_run() with
        # the parent's (now-dead) hubs.  The reset zeroes the pending counter so
        # mn_run returns immediately instead of waiting on hubs that don't exist.
        pygo_core.mn_init(4)
        try:
            for _ in range(8):
                pygo_core.mn_go(lambda: pygo.sleep(0.3))
            time.sleep(0.02)

            def child():
                pygo_core.mn_run()   # must return, not hang
                return 0

            rc = run_child(child, timeout=6.0)
            self.assertEqual(rc, 0)
        finally:
            pygo_core.mn_run()
            pygo_core.mn_fini()


class TestForkUnderLoad(unittest.TestCase):
    def test_repeated_forks_under_mn_load(self):
        import asyncio
        import pygo.aio as paio
        import threading

        pygo_core.mn_init(4)
        stop = [False]

        def churn():
            while not stop[0]:
                for _ in range(40):
                    pygo_core.mn_go(lambda: None)
                time.sleep(0.001)

        t = threading.Thread(target=churn, daemon=True)
        t.start()
        try:
            def child():
                async def m():
                    await asyncio.sleep(0.005)
                    return 1
                return 0 if paio.run(m()) == 1 else 4

            for _ in range(12):
                rc = run_child(child, timeout=8.0)
                self.assertEqual(rc, 0)
        finally:
            stop[0] = True
            t.join(timeout=1.0)
            pygo_core.mn_run()
            pygo_core.mn_fini()


class TestIntrospectionInChild(unittest.TestCase):
    def test_dump_works_in_child(self):
        def child():
            # registry was reset -> starts empty, populates with child goroutines
            out = []
            def w():
                pygo.sleep(0.02)
            def main():
                for _ in range(3):
                    pygo.go(w)
                pygo.sleep(0.005)
                out.append(pygo_core.goroutine_count())
            pygo.run(main)
            return 0 if out and out[0] == 4 else 6

        rc = run_child(child)
        self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main()
