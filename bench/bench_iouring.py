"""Benchmark io_uring file_read vs plain os.read for cooperative file
I/O.  io_uring should win on the round-trip cost (no thread handoff
for the monkey-patch fallback path -- one syscall vs. submit-to-pool +
wait + return)."""
import os
import sys
import tempfile
import time

sys.path.insert(0, "src")
import pygo_core


def make_tempfile(size):
    fd, path = tempfile.mkstemp()
    os.write(fd, b"x" * size)
    os.close(fd)
    return path


def bench(name, fn, iters):
    t0 = time.perf_counter()
    for _ in range(iters):
        fn()
    dt = time.perf_counter() - t0
    print("  %-22s  %.3f s  -> %7.1f us/op (%d iters)" %
          (name, dt, dt * 1e6 / iters, iters))


def main():
    if not pygo_core.iouring_available():
        print("io_uring not available on this system; skipping bench")
        return

    size = 4096
    path = make_tempfile(size)
    iters = 50_000

    fd_r = os.open(path, os.O_RDONLY)
    buf = bytearray(size)

    # plain os.read
    def plain():
        os.lseek(fd_r, 0, 0)
        os.read(fd_r, size)
    bench("os.read", plain, iters)

    # pygo_core.file_read with io_uring
    def fr():
        pygo_core.file_read(fd_r, buf, size, 0)
    bench("pygo_core.file_read", fr, iters)

    os.close(fd_r)
    os.unlink(path)


if __name__ == "__main__":
    main()
