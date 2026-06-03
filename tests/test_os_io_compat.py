"""Cooperative vectored os I/O (os.readv / os.writev) and the public
runloom.monkey.offload() escape hatch.

os.readv/os.writev are the vectored analogues of os.read/os.write: on a
pollable fd (pipe/socket) they park on wait_fd; on a regular file they offload
to the backend pool.  Adapted from CPython Lib/test/test_os.py
(ReadvWritevTests / test_readv / test_writev).

offload() runs a blocking callable on the backend pool, parking the goroutine
-- the sanctioned escape hatch for blocking calls runloom can't transparently make
cooperative (buffered FileIO on slow media, C DB drivers, CPU-bound work).
"""
import os
import time
import unittest

import runloom
import runloom.monkey
import runloom_c


def _drive(fn):
    box = [None, None]

    def runner():
        try:
            box[0] = fn()
        except BaseException as e:   # noqa: BLE001
            box[1] = e

    runloom_c.go(runner)
    runloom_c.run()
    if box[1] is not None:
        raise box[1]
    return box[0]


def setUpModule():
    runloom.monkey.patch()


def tearDownModule():
    runloom.monkey.unpatch()


@unittest.skipUnless(hasattr(os, "writev") and hasattr(os, "readv"),
                     "no os.readv/writev")
class TestVectoredIO(unittest.TestCase):
    def test_writev_readv_roundtrip(self):
        def body():
            r, w = os.pipe()
            try:
                n = os.writev(w, [b"hello ", b"vectored ", b"world"])
                bufs = [bytearray(6), bytearray(9), bytearray(5)]
                got = os.readv(r, bufs)
                return n, got, b"".join(bytes(b) for b in bufs)
            finally:
                os.close(r); os.close(w)
        n, got, data = _drive(body)
        self.assertEqual(n, len(b"hello vectored world"))
        self.assertEqual(got, n)
        self.assertEqual(data, b"hello vectored world")

    def test_readv_blocks_then_yields(self):
        """A goroutine blocked in os.readv on a pipe must let a sibling run."""
        def body():
            r, w = os.pipe()
            ticks, stop = [], {"v": False}

            def ticker():
                while not stop["v"]:
                    ticks.append(1)
                    runloom.sleep(0.003)

            def sender():
                for _ in range(6):
                    runloom.sleep(0.004)        # ~24 ms before data lands
                os.writev(w, [b"AB", b"CD"])

            runloom_c.go(ticker)
            runloom_c.go(sender)
            bufs = [bytearray(2), bytearray(2)]
            n = os.readv(r, bufs)            # blocks until the sender writes
            stop["v"] = True
            os.close(r); os.close(w)
            return n, bytes(bufs[0]), bytes(bufs[1]), len(ticks)
        n, b0, b1, ticks = _drive(body)
        self.assertEqual((n, b0, b1), (4, b"AB", b"CD"))
        self.assertGreaterEqual(ticks, 1)

    def test_readv_on_regular_file(self):
        import tempfile
        fd, path = tempfile.mkstemp(prefix="runloom_readv_")
        os.write(fd, b"0123456789")
        os.close(fd)

        def body():
            fd = os.open(path, os.O_RDONLY)
            try:
                bufs = [bytearray(4), bytearray(6)]
                n = os.readv(fd, bufs)       # regular file -> pool offload
                return n, bytes(bufs[0]), bytes(bufs[1])
            finally:
                os.close(fd)
        try:
            n, b0, b1 = _drive(body)
        finally:
            os.unlink(path)
        self.assertEqual(n, 10)
        self.assertEqual(b0, b"0123")
        self.assertEqual(b1, b"456789")


class TestOffload(unittest.TestCase):
    def test_offload_returns_value_and_yields(self):
        """offload() runs the callable on the pool and parks the goroutine; a
        sibling keeps running, and the result/exception propagate."""
        def slow_double(x):
            time.sleep(0.05)                # real blocking sleep on a worker
            return x * 2

        def body():
            ticks, stop = [], {"v": False}

            def ticker():
                while not stop["v"]:
                    ticks.append(1)
                    runloom.sleep(0.003)

            runloom_c.go(ticker)
            val = runloom.monkey.offload(slow_double, 21)
            stop["v"] = True
            return val, len(ticks)
        val, ticks = _drive(body)
        self.assertEqual(val, 42)
        self.assertGreaterEqual(ticks, 1)

    def test_offload_propagates_exception(self):
        def boom():
            raise ValueError("nope")

        def body():
            try:
                runloom.monkey.offload(boom)
            except ValueError as e:
                return str(e)
            return None
        self.assertEqual(_drive(body), "nope")


if __name__ == "__main__":
    unittest.main()
