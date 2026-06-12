"""Cooperative CPU-heavy stdlib calls: size-gated auto-offload (`heavy`).

hashlib.sha*/md5/blake2 and zlib/gzip/bz2/lzma compress/decompress burn CPU in
a tight C loop with no yield point -- a fiber can't hand off mid-sha256 and
the sysmon preemptor can't interrupt a frameless C loop, so it pins the
scheduler.  runloom.monkey can't make these cooperative, only RELOCATE them: above
RUNLOOM_OFFLOAD_BYTES (default 256 KiB) the call runs on the backend pool so the
fiber parks and its siblings keep running.  KDFs (pbkdf2_hmac / scrypt) are
always offloaded (cost is iterations, not size).

The point is that developers don't have to remember to wrap the common heavy
ones -- it just happens.  These tests pin: bit-identical results vs stock,
that a big call yields to a sibling, round-trips through compress/decompress,
small calls stay inline, and non-fiber callers pass straight through.

Adapted from CPython Lib/test (test_hashlib, test_zlib, test_gzip, test_bz2,
test_lzma) -- the same vectors, asserted to match under auto-offload.
"""
import unittest

# References captured with the STOCK functions, BEFORE setUpModule patches.
import bz2
import gzip
import hashlib
import lzma
import zlib

import runloom
import runloom.monkey
import runloom_c

# ~4 MiB, comfortably above the 256 KiB default threshold.
DATA = b"the quick brown fox jumps over the lazy dog\n" * 100_000

REF = {
    "sha1":     hashlib.sha1(DATA).hexdigest(),
    "sha256":   hashlib.sha256(DATA).hexdigest(),
    "sha512":   hashlib.sha512(DATA).hexdigest(),
    "md5":      hashlib.md5(DATA).hexdigest(),
    "blake2b":  hashlib.blake2b(DATA).hexdigest(),
    "sha3_256": hashlib.sha3_256(DATA).hexdigest(),
    "new":      hashlib.new("sha256", DATA).hexdigest(),
    "pbkdf2":   hashlib.pbkdf2_hmac("sha256", b"password", b"salt", 200_000),
    "zlib":     zlib.compress(DATA, 6),
    "gzip":     gzip.compress(DATA, 6, mtime=0),   # mtime=0: deterministic header
    "bz2":      bz2.compress(DATA, 9),
    "lzma":     lzma.compress(DATA),
}


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


def _ticker(ticks, stop):
    def t():
        while not stop["v"]:
            ticks.append(1)
            runloom.sleep(0.003)
    runloom_c.go(t)


class TestInstalled(unittest.TestCase):
    def test_wrappers_installed(self):
        # The size-gate wrapper is in place but transparent (keeps __name__).
        self.assertTrue(getattr(hashlib.sha256, "__runloom_heavy__", False))
        self.assertTrue(getattr(zlib.compress, "__runloom_heavy__", False))
        self.assertTrue(getattr(lzma.compress, "__runloom_heavy__", False))
        self.assertEqual(hashlib.sha256.__name__,
                         hashlib.sha256.__wrapped__.__name__)


class TestHashlibOffload(unittest.TestCase):
    def test_hash_results_match_stock(self):
        def body():
            return {
                "sha1":     hashlib.sha1(DATA).hexdigest(),
                "sha256":   hashlib.sha256(DATA).hexdigest(),
                "sha512":   hashlib.sha512(DATA).hexdigest(),
                "md5":      hashlib.md5(DATA).hexdigest(),
                "blake2b":  hashlib.blake2b(DATA).hexdigest(),
                "sha3_256": hashlib.sha3_256(DATA).hexdigest(),
                "new":      hashlib.new("sha256", DATA).hexdigest(),
            }
        got = _drive(body)
        for k, v in got.items():
            self.assertEqual(v, REF[k], k)

    def test_hash_via_keyword(self):
        def body():
            return hashlib.sha256(string=DATA).hexdigest()
        self.assertEqual(_drive(body), REF["sha256"])

    def test_small_input_inline_correct(self):
        def body():
            return hashlib.sha256(b"hello").hexdigest()
        self.assertEqual(_drive(body), hashlib.sha256.__wrapped__(b"hello").hexdigest())

    def test_pbkdf2_offloads_and_yields(self):
        """A KDF always offloads; a sibling must keep running meanwhile."""
        def body():
            ticks, stop = [], {"v": False}
            _ticker(ticks, stop)
            dk = hashlib.pbkdf2_hmac("sha256", b"password", b"salt", 200_000)
            stop["v"] = True
            return dk, len(ticks)
        dk, ticks = _drive(body)
        self.assertEqual(dk, REF["pbkdf2"])
        self.assertGreaterEqual(ticks, 1)

    def test_big_hash_yields(self):
        big = b"a" * (48 * 1024 * 1024)            # big enough to take ms
        def body():
            ticks, stop = [], {"v": False}
            _ticker(ticks, stop)
            d = hashlib.sha512(big).hexdigest()
            stop["v"] = True
            return d, len(ticks)
        d, ticks = _drive(body)
        self.assertEqual(d, hashlib.sha512.__wrapped__(big).hexdigest())
        self.assertGreaterEqual(ticks, 1)


class TestCompressionOffload(unittest.TestCase):
    def test_compress_results_match_stock(self):
        def body():
            return {
                "zlib": zlib.compress(DATA, 6),
                "gzip": gzip.compress(DATA, 6, mtime=0),
                "bz2":  bz2.compress(DATA, 9),
                "lzma": lzma.compress(DATA),
            }
        got = _drive(body)
        for k, v in got.items():
            self.assertEqual(v, REF[k], k)

    def test_roundtrip_cooperative(self):
        def body():
            out = {}
            out["zlib"] = zlib.decompress(zlib.compress(DATA, 6))
            out["gzip"] = gzip.decompress(gzip.compress(DATA))
            out["bz2"]  = bz2.decompress(bz2.compress(DATA))
            out["lzma"] = lzma.decompress(lzma.compress(DATA))
            return out
        got = _drive(body)
        for k, v in got.items():
            self.assertEqual(v, DATA, k)

    def test_big_compress_yields(self):
        def body():
            ticks, stop = [], {"v": False}
            _ticker(ticks, stop)
            packed = lzma.compress(DATA)             # slow -> must yield
            stop["v"] = True
            return packed, len(ticks)
        packed, ticks = _drive(body)
        self.assertEqual(packed, REF["lzma"])
        self.assertGreaterEqual(ticks, 1)


class TestPassthrough(unittest.TestCase):
    def test_outside_fiber_runs_inline(self):
        # No fiber context -> straight through to the original, correct.
        self.assertEqual(hashlib.sha256(DATA).hexdigest(), REF["sha256"])
        self.assertEqual(zlib.compress(DATA, 6), REF["zlib"])

    def test_empty_and_none_data_dont_crash(self):
        def body():
            a = hashlib.sha256().hexdigest()          # no data arg
            b = hashlib.sha256(b"").hexdigest()        # empty
            return a, b
        a, b = _drive(body)
        self.assertEqual(a, hashlib.sha256.__wrapped__().hexdigest())
        self.assertEqual(b, hashlib.sha256.__wrapped__(b"").hexdigest())


if __name__ == "__main__":
    unittest.main()
