"""Pillar B -- uvloop-style asyncio conformance runner for runloom.

Run CPython's OWN ``Lib/test/test_asyncio`` submodules, unmodified, against
``runloom.aio.RunloomEventLoop``.  This is exactly how uvloop proves it is a
drop-in asyncio loop: you don't re-author the scenarios, you make CPython's own
assertions turn red if the loop diverges.

How the loop gets swapped in (two belt-and-braces hooks, per the loop-injection
map for the 3.14t suite):

  1. A GLOBAL ``RunloomEventLoopPolicy`` -- so every bare
     ``asyncio.new_event_loop()`` (used directly by many test_utils.TestCase
     tests) AND the IsolatedAsyncioTestCase default path both hand back a
     RunloomEventLoop.
  2. ``loop_factory = RunloomEventLoop`` pinned on every IsolatedAsyncioTestCase
     subclass in each loaded submodule -- IsolatedAsyncioTestCase builds
     ``asyncio.Runner(loop_factory=self.loop_factory)`` and only consults the
     policy when that is None, so pinning it makes the choice explicit.

Known failures (genuine can't-pass divergences / unsupported features) live in
``asyncio_known_failures.txt`` (one ``module.Class.test_method`` /
``module.Class`` / ``module`` per line, ``#`` comments).  They are subtracted
before the run; the runner exits nonzero only on a GENUINE (non-known) red test.

Usage:
  PYTHON_GIL=0 PYTHONPATH=src \\
    $HOME/.pyenv/versions/3.14.4t/bin/python3 tools/conformance/run_asyncio.py
  ... run_asyncio.py test_locks test_queues        # a chosen subset
  ... run_asyncio.py --list                         # show the curated set
  ... run_asyncio.py -v                              # per-test verbosity
"""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import conformance_lib as cl  # noqa: E402


# The stdlib prefix every test id carries; stripped to match known-failures.
PREFIX = "test.test_asyncio."

KNOWN_FAILURES_FILE = os.path.join(HERE, "asyncio_known_failures.txt")

# Curated default set: submodules whose scenarios exercise loop primitives
# runloom actually implements (locks / queues / futures / tasks / streams /
# timeouts / selector-events / events).  Ordered cheap-first.  Extend by
# appending a name here once it is green-modulo-known-failures.
CURATED = [
    "test_locks",
    "test_queues",
    "test_futures",
    "test_futures2",
    "test_tasks",
    "test_events",
    "test_streams",
    "test_timeouts",
    "test_waitfor",
    "test_selector_events",
    "test_taskgroups",
    "test_transports",
    "test_protocols",
]


def import_submodule(name):
    """Import ``test.test_asyncio.<name>`` and return the module (or None on a
    genuinely-absent submodule, so the runner keeps going)."""
    full = "test.test_asyncio." + name
    try:
        __import__(full)
        return sys.modules[full]
    except Exception as exc:  # noqa: BLE001 -- report + continue, don't abort all
        sys.stdout.write("  !! could not import %s: %r\n" % (full, exc))
        return None


def main(argv):
    args = list(argv)
    verbosity = 0
    if "-v" in args:
        verbosity = 2
        args.remove("-v")
    if "--list" in args:
        for name in CURATED:
            print(name)
        return 0

    if not cl.have_cpython_tests():
        sys.stdout.write(
            "SKIP: CPython stdlib `test` package is not installed on this "
            "interpreter; nothing to run.\n")
        return 0

    loop_factory = cl.install_asyncio_policy()
    known = cl.load_known_failures(KNOWN_FAILURES_FILE)

    names = args if args else CURATED
    per_module = []
    for name in names:
        sys.stdout.write("\n--- %s ---\n" % name)
        sys.stdout.flush()
        mod = import_submodule(name)
        if mod is None:
            per_module.append((name, {
                "ran": 0, "ok": 0, "fail": 0, "err": 0,
                "skip": 0, "dropped": 0, "red_short_ids": []}))
            continue
        cl.patch_isolated_loop_factory(mod, loop_factory)
        st = cl.run_module(mod, known, PREFIX, verbosity=verbosity)
        per_module.append((name, st))

    genuine_red = cl.print_summary("asyncio", per_module)
    return 1 if genuine_red else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
