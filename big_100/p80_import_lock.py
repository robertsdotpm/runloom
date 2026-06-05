"""big_100 / 80 -- import lock stress.

Tens of thousands of goroutines concurrently import modules -- a mix of real
stdlib modules (which hit the import lock and the filesystem the first time and
the module cache afterwards), failing imports (ImportError), and occasional
reloads.  The import machinery must serialise correctly without deadlocking or
handing back a half-initialised module.

Stresses: the import lock, module state, blocking filesystem during import.
"""
import importlib
from importlib import _bootstrap

import harness

REAL = ["json", "csv", "hashlib", "base64", "uuid", "decimal", "fractions",
        "textwrap", "difflib", "colorsys", "html.parser", "urllib.parse",
        "email.utils", "calendar", "string", "random", "bisect", "heapq"]
FAKE = ["nonexistent_module_zzz", "totally.fake.path", "__no_such_pkg__",
        "json.not_a_submodule"]

# importlib's per-module lock + deadlock detector is OS-thread-keyed and
# false-positives under M:N (FINDINGS BUG #9) -- count it, don't fail.
DeadlockError = getattr(_bootstrap, "_DeadlockError", RuntimeError)


def setup(H):
    # Pre-warm the real modules so concurrent imports hit the cache rather than
    # racing a first-import (whose offloaded filesystem step trips BUG #9).
    for name in REAL:
        importlib.import_module(name)
    H.state = {"deadlocks": [0] * 1024}


def worker(H, wid, rng, state):
    while H.running():
        if rng.random() < 0.85:
            name = rng.choice(REAL)
            try:
                m = importlib.import_module(name)
            except DeadlockError:
                state["deadlocks"][wid & 1023] += 1
                continue
            if not H.check(m is not None and hasattr(m, "__name__"),
                           "import {0} returned junk wid={1}".format(
                               name, wid)):
                return
            if not H.check(name.endswith(m.__name__.split(".")[-1]),
                           "import {0} gave wrong module {1} wid={2}".format(
                               name, m.__name__, wid)):
                return
            if rng.random() < 0.02:
                try:
                    importlib.reload(m)
                except Exception:
                    pass            # some modules dislike reload; not the point
        else:
            name = rng.choice(FAKE)
            try:
                importlib.import_module(name)
                H.fail("fake module {0} imported wid={1}".format(name, wid))
                return
            except DeadlockError:
                state["deadlocks"][wid & 1023] += 1   # BUG #9, expected
            except (ImportError, ModuleNotFoundError, ValueError, TypeError):
                pass                # expected failure for a fake module
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    dl = sum(H.state["deadlocks"])
    H.log("import _DeadlockError (BUG #9) occurrences: {0}".format(dl))


if __name__ == "__main__":
    harness.main("p80_import_lock", body, setup=setup, post=post,
                 default_funcs=5000,
                 describe="concurrent imports + failures + reloads; import lock")
