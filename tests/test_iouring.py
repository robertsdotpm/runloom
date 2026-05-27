"""Tests for the io_uring file-I/O backend."""
import os
import tempfile
import unittest

import pygo_core


@unittest.skipUnless(pygo_core.iouring_available(),
                     "io_uring not available (need Linux >= 5.1)")
class TestIouring(unittest.TestCase):
    def test_write_then_read(self):
        path = tempfile.mktemp()
        fd = os.open(path, os.O_RDWR | os.O_CREAT)
        try:
            data = b"hello pygo io_uring" * 100
            n = pygo_core.file_write(fd, data)
            self.assertEqual(n, len(data))

            buf = bytearray(len(data))
            n = pygo_core.file_read(fd, buf, len(data), 0)
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
            n = pygo_core.file_read(fd, buf, 5, 0)
            self.assertEqual(n, 5)
            self.assertEqual(bytes(buf), b"abcde")
        finally:
            os.close(fd)
            os.unlink(path)

    def test_from_goroutine(self):
        """file_read/write work the same from inside a goroutine."""
        out = []

        def worker(path):
            fd = os.open(path, os.O_RDONLY)
            buf = bytearray(11)
            pygo_core.file_read(fd, buf, 11, 0)
            out.append(bytes(buf))
            os.close(fd)

        path = tempfile.mktemp()
        with open(path, "wb") as f:
            f.write(b"hello world")

        pygo_core.go(lambda: worker(path))
        pygo_core.run()
        os.unlink(path)

        self.assertEqual(out, [b"hello world"])


class TestFallback(unittest.TestCase):
    """Even when io_uring is available, the API should also work on
    systems without it (via pread/pwrite fallback).  This just verifies
    the function is callable -- the underlying path is exercised by
    TestIouring above when io_uring is enabled."""

    def test_function_exists(self):
        self.assertTrue(callable(pygo_core.file_read))
        self.assertTrue(callable(pygo_core.file_write))
        self.assertTrue(callable(pygo_core.iouring_available))


if __name__ == "__main__":
    unittest.main()
