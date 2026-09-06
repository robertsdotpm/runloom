"""Swap the event loop to runloom.aio.RunloomEventLoop for the vendored asyncio
suite, and apply the committed skip baseline -- WITHOUT editing the vendored test
bodies.

Injection covers every way a test_asyncio module obtains a loop:
  * a global RunloomEventLoopPolicy  -> asyncio.new_event_loop() and
    IsolatedAsyncioTestCase (loop_factory=None falls back to the policy) and the
    hardcoded asyncio.Runner() paths;
  * loop_factory set on each IsolatedAsyncioTestCase subclass (belt-and-braces);
  * create_event_loop() overridden on the EventLoopTestsMixin subclasses
    (test_events / test_sock_lowlevel real-I/O suites);
  * new_loop() overridden on the FunctionalTestCaseMixin subclasses
    (test_server / test_buffered_proto).

The vendored bodies are untouched, so they stay diffable against CPython upstream.
"""
import asyncio
import os
import sys
import unittest
import warnings

import pytest

import runloom.aio as paio
from . import skips


# ---- interpreter floor ------------------------------------------------------
# The bodies in this package are pinned from CPython 3.14.4 (see __init__.py) and
# are a conformance suite against THAT asyncio, so they are not meaningful on an
# older interpreter -- they do not merely fail, they fail to import:
#
#   test_locks.py       re.compile(r'...\z')  -- \z is a 3.14 re escape; 3.13
#                       raises "bad escape \z" at module level.
#   utils.py            open(data_file('certdata', 'keycert3.pem.reference'))
#                       -- that data file only exists in 3.14's test tree, so
#                       test_server.py and test_sock_lowlevel.py die importing it.
#
# CI runs the matrix on 3.13.13 as well as 3.14.4, so those four modules were
# reddening every 3.13 leg with collection errors.  Ignoring collection makes
# pytest exit 5 ("no tests collected"), which tests/run_isolated.py already
# treats as a SKIP -- and a genuine import error still exits 2, so this does not
# mask a broken file on 3.14.
#
# Do NOT convert this into per-test entries in skips.py: those apply to
# collected items, and these modules fail before any item exists.
AIO_MIN_PYTHON = (3, 14)
_TOO_OLD = sys.version_info < AIO_MIN_PYTHON

# This covers `pytest tests/aio` (a directory walk) ONLY.  It does NOT cover a
# module named explicitly on the command line: pytest collects a path you ask
# for by name and consults neither collect_ignore_glob nor pytest_ignore_collect
# for it (verified on pytest 9.0.3 -- this conftest loads, its report header
# prints, and the module is imported and errors anyway).  tests/run_isolated.py
# runs exactly that way, one module per subprocess, so it carries its own copy
# of this floor; see the SUITE == "aio" guard there.  Both must agree.
if _TOO_OLD:
    collect_ignore_glob = ["test_*.py"]


def pytest_report_header(config):
    if _TOO_OLD:
        return ("tests/aio: skipped entirely -- bodies are pinned from CPython "
                "%d.%d and this is %d.%d"
                % (AIO_MIN_PYTHON + sys.version_info[:2]))
    return None


def install_policy():
    # The policy mechanism is deprecated on 3.14 (removal in 3.16) but still the
    # broadest single lever; suppress the one DeprecationWarning it emits.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        asyncio.set_event_loop_policy(paio.RunloomEventLoopPolicy())


# Skip on an interpreter below the floor: the suite is not going to run, and
# set_event_loop_policy is process-global, so installing it would leak the
# bridge policy into anything else sharing the session.
if not _TOO_OLD:
    install_policy()


def make_runloom_loop(self, *args, **kwargs):
    # The stock create_event_loop/new_loop take a selector or no arg; ignore
    # whatever they pass -- RunloomEventLoop drives its own netpoll.
    return paio.RunloomEventLoop()


def patch_module_loops(mod):
    for name in dir(mod):
        obj = getattr(mod, name, None)
        if not isinstance(obj, type):
            continue
        if issubclass(obj, unittest.IsolatedAsyncioTestCase):
            obj.loop_factory = paio.RunloomEventLoop
        # Only override where the class DEFINES the hook (a concrete loop-test
        # subclass), not where it merely inherits it.
        if "create_event_loop" in obj.__dict__:
            obj.create_event_loop = make_runloom_loop
        if "new_loop" in obj.__dict__:
            obj.new_loop = make_runloom_loop


def pytest_collection_modifyitems(config, items):
    patched = set()
    for it in items:
        mod = getattr(it, "module", None)
        if mod is not None and id(mod) not in patched:
            patch_module_loops(mod)
            patched.add(id(mod))
    # Apply the committed skip baseline (green on the default bridge).  Set
    # RUNLOOM_AIO_NOSKIP=1 to run the raw divergences (for closing the gaps: see
    # exactly what each skipped test needs before/while fixing the bridge).
    if os.environ.get("RUNLOOM_AIO_NOSKIP"):
        return
    for it in items:
        mod = getattr(it, "module", None)
        cls = getattr(it, "cls", None)
        if mod is None or cls is None:
            continue
        modname = mod.__name__.rsplit(".", 1)[-1]
        method = getattr(it, "originalname", None) or it.name.split("[")[0]
        reason = skips.lookup(modname, cls.__name__, method)
        if reason:
            it.add_marker(pytest.mark.skip(reason=reason))
