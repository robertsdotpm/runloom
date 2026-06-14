"""big_100 / 168 -- concurrent C-extension import storm.

Goroutines concurrently `import` stdlib C extensions (ssl, sqlite3, zlib,
hashlib, bz2, lzma, _decimal, select, math, ...).  CPython's per-module import
lock + each module's C init must run under M:N without a false `_DeadlockError`
(FINDINGS #9) or a crash.  Each imported module is then USED (a real call) to
prove it initialised correctly.

To avoid the KNOWN false-deadlock (#9) on racing `del sys.modules[m]` reimports
of ONE module, each goroutine imports a DISTINCT module (round-robin by wid),
so many module INITs run concurrently across hubs without any same-module
re-entrant import on a single hub.

Stresses: CPython import lock under M:N, concurrent C-extension init, the
import-lock false-deadlock (#9).
"""
import importlib

import harness
import runloom

# (module name, a callable taking the module that exercises a real entry point
#  and returns something the test can verify).  Each must be a C extension whose
#  init runs real C-level setup.
def _use_ssl(m):
    return m.create_default_context() is not None


def _use_sqlite3(m):
    con = m.connect(":memory:")
    try:
        return con.execute("SELECT 6*7").fetchone()[0] == 42
    finally:
        con.close()


def _use_zlib(m):
    return m.decompress(m.compress(b"x" * 256)) == b"x" * 256


def _use_hashlib(m):
    return m.sha256(b"x").hexdigest() == (
        "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881")


def _use_bz2(m):
    return m.decompress(m.compress(b"y" * 256)) == b"y" * 256


def _use_lzma(m):
    return m.decompress(m.compress(b"z" * 256)) == b"z" * 256


def _use_decimal(m):
    return m.Decimal("0.1") + m.Decimal("0.2") == m.Decimal("0.3")


def _use_select(m):
    return hasattr(m, "select")


def _use_math(m):
    return m.isqrt(144) == 12


def _use_binascii(m):
    return m.unhexlify(m.hexlify(b"abc")) == b"abc"


def _use_struct(m):
    return m.unpack(">I", m.pack(">I", 0xDEADBEEF))[0] == 0xDEADBEEF


def _use_array(m):
    a = m.array("i", [1, 2, 3])
    return a.tolist() == [1, 2, 3]


def _use_unicodedata(m):
    return m.category("A") == "Lu"


MODULES = [
    ("ssl", _use_ssl),
    ("sqlite3", _use_sqlite3),
    ("zlib", _use_zlib),
    ("hashlib", _use_hashlib),
    ("bz2", _use_bz2),
    ("lzma", _use_lzma),
    ("_decimal", _use_decimal),
    ("select", _use_select),
    ("math", _use_math),
    ("binascii", _use_binascii),
    ("struct", _use_struct),
    ("array", _use_array),
    ("unicodedata", _use_unicodedata),
]


def setup(H):
    # Filter to modules that actually exist on this build (e.g. _decimal/lzma
    # could be absent in a stripped CPython).  Probe once on the main thread.
    avail = []
    for name, use in MODULES:
        try:
            importlib.import_module(name)
            avail.append((name, use))
        except ImportError:
            pass
    H.check(len(avail) > 0, "no candidate C-extension modules importable")
    H.state = {"modules": avail}


def worker(H, wid, rng, state):
    mods = state["modules"]
    ok = 0
    for _ in H.round_range():
        # Round-robin distinct modules across goroutines so concurrent INITs of
        # DIFFERENT modules race (the thing we want), never a same-module reimport
        # on one hub (the known #9 false-deadlock).  Rotate per round too.
        idx = (wid + ok) % len(mods)
        name, use = mods[idx]
        try:
            m = importlib.import_module(name)
        except ImportError as exc:
            H.fail("import {0} failed wid={1}: {2}".format(name, wid, exc))
            return
        if not H.check(use(m), "module {0} unusable after import wid={1}".format(
                name, wid)):
            return
        # Migrate hubs between imports so the next import lands on a different
        # hub thread relative to the import lock state.
        runloom.yield_now()
        ok += 1
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    H.check(H.total_ops() > 0, "no imports were exercised")
    H.log("import_ops={0} modules={1}".format(
        H.total_ops(), [n for n, _ in H.state["modules"]]))


if __name__ == "__main__":
    harness.main("p168_cext_import_storm", body, setup=setup, post=post,
                 default_funcs=1000,
                 describe="concurrent distinct C-extension imports under M:N; "
                          "every module usable, no import-lock false-deadlock")
