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
import unittest
import warnings

import pytest

import runloom.aio as paio
from . import skips


def install_policy():
    # The policy mechanism is deprecated on 3.14 (removal in 3.16) but still the
    # broadest single lever; suppress the one DeprecationWarning it emits.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        asyncio.set_event_loop_policy(paio.RunloomEventLoopPolicy())


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
    # Apply the committed skip baseline (green on the default bridge).
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
