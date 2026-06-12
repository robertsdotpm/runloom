"""getpass.getpass is offloaded to a pool worker inside a fiber (so the
human-latency-bound tty read doesn't wedge the hub), and runs inline otherwise.

The real getpass reads /dev/tty, which we can't drive in a test, so we stand a
fake in as the wrapped original and assert *where* it runs (a worker thread vs
the calling thread) and that the prompt/return value pass through.
"""
import threading
import getpass as _getpass_mod

import runloom            # noqa: F401  (runtime)
import runloom_c
from runloom.monkey import osio


def _with_fake(body):
    real = _getpass_mod.getpass
    seen = {}

    def fake(prompt="Password: ", stream=None):
        seen["tid"] = threading.get_ident()
        seen["prompt"] = prompt
        seen["stream"] = stream
        return "s3cret"

    _getpass_mod.getpass = fake     # becomes the wrapped original under the patch
    osio._patch_getpass()
    try:
        return body(seen)
    finally:
        osio._unpatch_getpass()
        _getpass_mod.getpass = real


def test_offloaded_in_fiber():
    main_tid = threading.get_ident()

    def body(seen):
        out = {}

        def work():
            out["v"] = _getpass_mod.getpass("Pwd: ")

        runloom_c.go(work)
        runloom_c.run()
        assert out["v"] == "s3cret"          # return value passes through
        assert seen["prompt"] == "Pwd: "     # prompt passes through
        assert seen["tid"] != main_tid       # ran on a pool worker (offloaded)

    _with_fake(body)


def test_inline_outside_fiber():
    main_tid = threading.get_ident()

    def body(seen):
        assert _getpass_mod.getpass("p") == "s3cret"
        assert seen["tid"] == main_tid       # inline -- no offload off a fiber

    _with_fake(body)


def test_patch_unpatch_restores():
    real = _getpass_mod.getpass
    osio._patch_getpass()
    assert _getpass_mod.getpass is not real
    osio._unpatch_getpass()
    assert _getpass_mod.getpass is real


def test_registered_in_default_patch_set():
    import runloom.monkey as monkey
    assert "getpass" in monkey._DEFAULTS
    assert "getpass" in monkey._PATCHERS
