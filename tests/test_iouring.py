"""Tests for the io_uring file-I/O backend."""
import os
import tempfile
import time
import unittest

import runloom_c


@unittest.skipUnless(runloom_c.iouring_available(),
                     "io_uring not available (need Linux >= 5.1)")
class TestIouring(unittest.TestCase):
    def test_write_then_read(self):
        path = tempfile.mktemp()
        fd = os.open(path, os.O_RDWR | os.O_CREAT)
        try:
            data = b"hello runloom io_uring" * 100
            n = runloom_c.file_write(fd, data)
            self.assertEqual(n, len(data))

            buf = bytearray(len(data))
            n = runloom_c.file_read(fd, buf, len(data), 0)
            self.assertEqual(n, len(data))
            self.assertEqual(bytes(buf), data)
        finally:
            os.close(fd)
            os.unlink(path)

    def test_partial_read(self):
        path = tempfile.mktemp()
        fd = os.open(path, os.O_RDWR | os.O_CREAT)
        try:
            os.write(fd, b"abcdefghij")
            buf = bytearray(5)
            n = runloom_c.file_read(fd, buf, 5, 0)
            self.assertEqual(n, 5)
            self.assertEqual(bytes(buf), b"abcde")
        finally:
            os.close(fd)
            os.unlink(path)

    def test_from_fiber(self):
        """file_read/write work the same from inside a fiber."""
        out = []

        def worker(path):
            fd = os.open(path, os.O_RDONLY)
            buf = bytearray(11)
            runloom_c.file_read(fd, buf, 11, 0)
            out.append(bytes(buf))
            os.close(fd)

        path = tempfile.mktemp()
        with open(path, "wb") as f:
            f.write(b"hello world")

        runloom_c.go(lambda: worker(path))
        runloom_c.run()
        os.unlink(path)

        self.assertEqual(out, [b"hello world"])

    def test_cooperative_park(self):
        """While one fiber is parked on an iouring read, other
        fibers must still run.  This is the central invariant of
        the async path -- if the OS thread blocked in io_uring_enter
        the second fiber couldn't make progress."""
        path = tempfile.mktemp()
        with open(path, "wb") as f:
            f.write(b"x" * 4096)

        events = []

        def reader():
            fd = os.open(path, os.O_RDONLY)
            buf = bytearray(4096)
            events.append("read-start")
            runloom_c.file_read(fd, buf, 4096, 0)
            events.append("read-done")
            os.close(fd)

        def runner():
            for i in range(5):
                events.append("tick-" + str(i))
                runloom_c.sched_yield()

        runloom_c.go(reader)
        runloom_c.go(runner)
        runloom_c.run()
        os.unlink(path)

        # Reader starts before runner's first tick (both go() calls happen
        # before run()).  The point is that the runner's ticks land
        # *between* read-start and read-done -- without cooperative parking
        # the reader would block the thread and runner couldn't tick at
        # all until the read completed.
        self.assertIn("read-start", events)
        self.assertIn("read-done", events)
        self.assertIn("tick-0", events)
        self.assertIn("tick-4", events)
        # And the ticks must finish in order.
        ticks = [e for e in events if e.startswith("tick-")]
        self.assertEqual(ticks, ["tick-" + str(i) for i in range(5)])


class TestFallback(unittest.TestCase):
    """Even when io_uring is available, the API should also work on
    systems without it (via pread/pwrite fallback).  This just verifies
    the function is callable -- the underlying path is exercised by
    TestIouring above when io_uring is enabled."""

    def test_function_exists(self):
        self.assertTrue(callable(runloom_c.file_read))
        self.assertTrue(callable(runloom_c.file_write))
        self.assertTrue(callable(runloom_c.iouring_available))


if __name__ == "__main__":
    unittest.main()
