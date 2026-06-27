"""D1: real foreign C extensions through the offload blockpool (differential).

The single most-cited OPEN runloom bug is the offload wedge (a goroutine parked in
runloom.blocking() never re-queued at high concurrency: p23/p17 on Linux, p92 on
mac). The existing offload tests use synthetic bodies; this drives REAL,
GIL-releasing foreign C extensions -- numpy (ufuncs/linalg), hashlib (sha256),
zlib (compress/crc32) -- THROUGH the blockpool from many goroutines at once, the
exact "a real C extension blocking inside the offload pool" surface.

Oracle: DIFFERENTIAL -- each goroutine computes a deterministic result both
DIRECTLY (in the goroutine) and via runloom.blocking() (offloaded to a worker
thread, parking the goroutine), and asserts they are EQUAL. A lost/corrupted
offload result, a torn return crossing the worker->goroutine boundary, or a wedge
(watchdog/timeout) all fail it. No hand-specified expected output -- the direct
computation IS the oracle. Plus conservation: every goroutine completes.

Good TSan target: the offload result crosses the foreign-waker boundary
(worker thread -> parked goroutine), where the lost-wake class lives.
"""
import hashlib
import sys
import threading
import unittest
import zlib

sys.path.insert(0, __file__.rsplit("/tests/", 1)[0] + "/src")
import runloom

try:
    import numpy as np
    _HAVE_NP = True
except ImportError:
    _HAVE_NP = False


def _np_op(seed):
    g = np.random.default_rng(seed)
    a = g.integers(0, 7, size=(16, 16)).astype(np.int64)
    return int((a @ a.T).sum())          # deterministic given seed


def _sha_op(seed):
    return hashlib.sha256(("payload-%d" % seed).encode() * 64).hexdigest()


def _zlib_op(seed):
    data = ("z-%d-" % seed).encode() * 128
    comp = zlib.compress(data, 6)
    return (zlib.crc32(data), len(comp), zlib.decompress(comp) == data)


def _run(n, hubs):
    ops = [_sha_op, _zlib_op] + ([_np_op] if _HAVE_NP else [])
    mismatches = [0] * 1024
    completed = [0] * 1024
    wg = runloom.WaitGroup()
    wg.add(n)

    def worker(i):
        try:
            op = ops[i % len(ops)]
            seed = (i * 2654435761) & 0xFFFFFFFF
            direct = op(seed)                       # compute in-goroutine
            offloaded = runloom.blocking(op, seed)  # offload -> park -> resume
            if direct != offloaded:
                mismatches[i & 1023] += 1
            completed[i & 1023] += 1
        finally:
            wg.done()

    def main():
        for i in range(n):
            runloom.fiber(worker, i)
        wg.wait()

    runloom.run(hubs, main)
    return sum(mismatches), sum(completed)


class ExtOffloadConformance(unittest.TestCase):

    def test_foreign_ext_offload_is_faithful(self):
        for hubs in (2, 4):
            mism, done = _run(n=600, hubs=hubs)
            self.assertEqual(mism, 0,
                             "offloaded foreign-C-ext result != direct result "
                             "({0} mismatches, hubs={1}) -- a corrupted/lost "
                             "offload return".format(mism, hubs))
            self.assertEqual(done, 600,
                             "only {0}/600 goroutines completed (hubs={1}) -- an "
                             "offload wedge / lost wake".format(done, hubs))


if __name__ == "__main__":
    unittest.main()
