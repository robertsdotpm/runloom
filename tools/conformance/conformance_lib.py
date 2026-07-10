"""Shared machinery for the runloom stdlib-conformance runners.

This is the uvloop-style pattern: take CPython's OWN stdlib test suites and run
them, unmodified, against runloom -- for asyncio, against
``runloom.aio.RunloomEventLoop`` (see ``run_asyncio.py``); for the pure
free-threaded stress suite, against ``runloom.monkey.patch()``'s cooperative
Co* locks (see ``run_free_threading.py``).  Any red test is either a real
runloom divergence, an intentional divergence, or an unsupported feature -- the
first two get characterised, the third gets parked in a per-suite
``*_known_failures.txt`` with a reason.

The helpers here are deliberately dependency-free (stdlib ``unittest`` only) so
both runners and the fast pytest slice can share them.  No leading-underscore
public names -- module-private helpers use the ``cl_`` prefix instead, per house
style.
"""
import os
import sys
import unittest
import warnings


HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
SRC = os.path.join(REPO, "src")


def ensure_src_on_path():
    """Put the repo's ``src/`` on sys.path so ``import runloom`` resolves to the
    in-tree build, regardless of the caller's cwd."""
    if SRC not in sys.path:
        sys.path.insert(0, SRC)


def have_cpython_tests():
    """True if CPython's stdlib ``test`` package is importable.  Embedded / some
    Windows builds ship without it; every entry point degrades to a friendly
    skip rather than an import error at collection."""
    try:
        import test  # noqa: F401
        return True
    except ImportError:
        return False


def load_known_failures(path):
    """Parse a known-failures file into a set of entries.

    Format: one entry per line, each of ``module``, ``module.Class`` or
    ``module.Class.test_method``.  ``#`` starts a comment (inline or full line);
    blank lines are ignored.  A missing file is treated as an empty set (a fresh
    suite starts green-or-honest with nothing parked)."""
    entries = set()
    if not path or not os.path.exists(path):
        return entries
    with open(path, "r") as fh:
        for raw in fh:
            line = raw.split("#", 1)[0].strip()
            if line:
                entries.add(line)
    return entries


def short_id(test, prefix):
    """Return a test's id with ``prefix`` (e.g. ``test.test_asyncio.``) stripped,
    so it reads as ``module.Class.test_method`` -- the known-failures format."""
    tid = test.id()
    if prefix and tid.startswith(prefix):
        tid = tid[len(prefix):]
    return tid


def is_known_failure(sid, known):
    """True if the short id ``sid`` (``module.Class.test_method``) is covered by a
    known-failures entry -- an exact match, or a ``module`` / ``module.Class``
    prefix (dot-bounded so ``test_loc`` never swallows ``test_locks``)."""
    if sid in known:
        return True
    for entry in known:
        if sid.startswith(entry + "."):
            return True
    return False


def iter_tests(suite):
    """Flatten a (possibly nested) TestSuite into its leaf TestCase instances."""
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            for leaf in iter_tests(item):
                yield leaf
        else:
            yield item


def install_asyncio_policy():
    """Install ``RunloomEventLoopPolicy`` globally so every bare
    ``asyncio.new_event_loop()`` (and the IsolatedAsyncioTestCase default path)
    yields a RunloomEventLoop.  The 3.14 policy system is deprecated (removal in
    3.16), so ``set_event_loop_policy`` emits one DeprecationWarning -- suppress
    exactly that call, not the whole run."""
    ensure_src_on_path()
    import asyncio
    with warnings.catch_warnings():
        # Two DeprecationWarnings live here, both from the 3.14 policy system's
        # scheduled 3.16 removal: (1) class-definition time, when importing
        # runloom.aio first defines `class RunloomEventLoopPolicy(
        # asyncio.AbstractEventLoopPolicy)`; (2) the set_event_loop_policy call.
        # Neither is under our control (the policy base is deprecated upstream);
        # suppress exactly this bootstrap, nothing else.
        warnings.simplefilter("ignore", DeprecationWarning)
        import runloom.aio as paio
        asyncio.set_event_loop_policy(paio.RunloomEventLoopPolicy())
    return paio.RunloomEventLoop


def patch_isolated_loop_factory(module, loop_factory):
    """Set ``loop_factory = loop_factory`` on every IsolatedAsyncioTestCase
    subclass defined in ``module``.  Belt-and-braces alongside the global policy:
    IsolatedAsyncioTestCase builds ``asyncio.Runner(loop_factory=self.loop_factory)``
    and only falls back to the policy when that is None, so pinning the class
    attribute makes the loop choice explicit and policy-independent.

    ``loop_factory`` is a plain class (not a descriptor), so ``self.loop_factory``
    returns it unbound -- callable with zero args, exactly as Runner wants.
    Returns the count of classes patched."""
    n = 0
    for name in dir(module):
        obj = getattr(module, name, None)
        if isinstance(obj, type) and issubclass(
                obj, unittest.IsolatedAsyncioTestCase):
            obj.loop_factory = loop_factory
            n += 1
    return n


class SummaryResult(unittest.TextTestResult):
    """A TextTestResult that also records each failure/error's short id, so the
    runner can print which tests went red (and confirm they are all known)."""

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.red_ids = []

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self.red_ids.append(test.id())

    def addError(self, test, err):
        super().addError(test, err)
        self.red_ids.append(test.id())


def run_module(module, known, prefix, verbosity=0, stream=None):
    """Load ``module``'s tests, drop those matching ``known``, run the rest, and
    return a stats dict::

        {ran, ok, fail, err, skip, dropped, red_short_ids}

    ``red_short_ids`` is the prefix-stripped id of every test that failed or
    errored despite NOT being a known failure -- i.e. genuine regressions.  The
    whole run happens under a warnings filter that ignores DeprecationWarning
    (the policy churn) so the summary stays readable; real errors still surface
    as failures."""
    stream = stream or sys.stdout
    loader = unittest.defaultTestLoader
    full = loader.loadTestsFromModule(module)

    kept = unittest.TestSuite()
    dropped = 0
    for test in iter_tests(full):
        sid = short_id(test, prefix)
        if is_known_failure(sid, known):
            dropped += 1
        else:
            kept.addTest(test)

    runner = unittest.TextTestRunner(
        stream=stream, verbosity=verbosity, resultclass=SummaryResult)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        result = runner.run(kept)

    red = [short_id_from_full(tid, prefix) for tid in result.red_ids]
    ran = result.testsRun
    fail = len(result.failures)
    err = len(result.errors)
    skip = len(result.skipped)
    ok = ran - fail - err
    return {
        "ran": ran, "ok": ok, "fail": fail, "err": err,
        "skip": skip, "dropped": dropped, "red_short_ids": red,
    }


def short_id_from_full(tid, prefix):
    if prefix and tid.startswith(prefix):
        return tid[len(prefix):]
    return tid


def print_summary(name, per_module, stream=None):
    """Print a per-module + total table.  Returns the number of genuine (non
    known-failure) red tests across all modules."""
    stream = stream or sys.stdout
    total = {"ran": 0, "ok": 0, "fail": 0, "err": 0, "skip": 0, "dropped": 0}
    red_all = []
    stream.write("\n===== %s conformance summary =====\n" % name)
    stream.write("%-26s %6s %6s %6s %6s %6s %8s\n" %
                 ("module", "ran", "pass", "fail", "err", "skip", "known"))
    for mod_name, st in per_module:
        stream.write("%-26s %6d %6d %6d %6d %6d %8d\n" % (
            mod_name, st["ran"], st["ok"], st["fail"], st["err"],
            st["skip"], st["dropped"]))
        for key in total:
            total[key] += st[key]
        red_all.extend(st["red_short_ids"])
    stream.write("%-26s %6d %6d %6d %6d %6d %8d\n" % (
        "TOTAL", total["ran"], total["ok"], total["fail"], total["err"],
        total["skip"], total["dropped"]))
    if red_all:
        stream.write("\nGENUINE (non-known) red tests -- %d:\n" % len(red_all))
        for sid in red_all:
            stream.write("  FAIL  %s\n" % sid)
    else:
        stream.write("\nAll red tests are known failures (or none). Green.\n")
    stream.flush()
    return len(red_all)
