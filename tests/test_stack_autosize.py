"""Tests for the adaptive stack auto-sizer (inspect.enable_stack_autosize).

Each goroutine kind starts large the first time it is seen and, once its real
C-stack use is measured, later goroutines of that kind start at the learned size
("start large, learn down"). In-memory only -- no persistence. An explicit
stack_size= always wins.

The observable is the advice report's `reserved` field (the stack size a kind's
goroutines actually ran with), which the auto-sizer drives.
"""
import json
import os

import pytest

import runloom
import runloom_c

NESTED = []
_cur = NESTED
for _ in range(80):
    _nx = []
    _cur.append(_nx)
    _cur = _nx


def heavy():
    json.dumps(NESTED)      # ~14 KiB of real C stack (json encoder recursion)


def light():
    return 1


START = 256 * 1024          # default RUNLOOM_STACK_AUTOSIZE_START


@pytest.fixture(autouse=True)
def _clean():
    runloom_c.reset_stack_advice()
    runloom.inspect.enable_stack_autosize(False)
    runloom.inspect.enable_stack_advice(False)
    yield
    runloom.inspect.enable_stack_autosize(False)
    runloom.inspect.enable_stack_advice(False)
    runloom_c.reset_stack_advice()


def _batch(fns, n, stack=None):
    for _ in range(n):
        for fn in fns:
            if stack is None:
                runloom_c.go(fn)
            else:
                runloom_c.go(fn, stack)
    runloom_c.run()


def _row(fn):
    name = "{0}.{1}".format(fn.__module__, fn.__qualname__)
    for r in runloom_c.stack_advice():
        if r["kind"].split(" (")[0] == name:
            return r
    return None


def _learned(hwm):
    """The size the auto-sizer derives for a kind: next_pow2(hwm*4), clamped."""
    sz = 1
    while sz < hwm * 4 and sz < 8 * 1024 * 1024:
        sz <<= 1
    return max(16 * 1024, min(sz, 8 * 1024 * 1024))


# --------------------------------------------------------------------------- #
def test_off_by_default():
    assert runloom_c.stack_autosize_enabled() is False
    # measure-only (no autosize): kinds run at the normal small default, not 256K
    runloom.inspect.enable_stack_advice(True)
    _batch([heavy], 20)
    assert _row(heavy)["reserved"] < START      # not started large


def test_enable_implies_measurement():
    runloom.inspect.enable_stack_autosize(True)
    assert runloom_c.stack_autosize_enabled() is True
    assert runloom_c.stack_advice_enabled() is True   # autosize implies advice


def test_unseen_kind_starts_large():
    runloom.inspect.enable_stack_autosize(True)
    _batch([heavy], 30)
    # every goroutine in this first batch was spawned before any completed,
    # so they all started at the large default
    assert _row(heavy)["reserved"] == START


def test_learn_down_on_next_batch():
    runloom.inspect.enable_stack_autosize(True)
    _batch([heavy], 30)                       # batch 1: all start large
    assert _row(heavy)["reserved"] == START
    hwm = _row(heavy)["max_hwm"]
    _batch([heavy], 30)                       # batch 2: start at the learned size
    learned = _row(heavy)["reserved"]
    assert learned < START                    # shrank from the large start
    assert learned >= hwm                     # but still covers what it used
    assert (learned & (learned - 1)) == 0     # power of two


def test_light_kind_learns_down_to_floor():
    runloom.inspect.enable_stack_autosize(True)
    _batch([light], 20)                       # batch 1: start large
    _batch([light], 20)                       # batch 2: learned
    learned = _row(light)["reserved"]
    assert learned == 16 * 1024               # ~0 use -> the 16 KiB floor


def test_explicit_stack_size_wins():
    runloom.inspect.enable_stack_autosize(True)
    _batch([heavy], 20, stack=128 * 1024)     # explicit override
    assert _row(heavy)["reserved"] == 128 * 1024   # autosizer did not touch it


def test_env_start_size(monkeypatch):
    monkeypatch.setenv("RUNLOOM_STACK_AUTOSIZE_START", str(64 * 1024))
    runloom.inspect.enable_stack_autosize(True)   # reads the env at enable time
    _batch([heavy], 10)
    assert _row(heavy)["reserved"] == 64 * 1024


def test_autosize_under_mn():
    # Under M:N the hubs run concurrently with spawning, so a kind learns down
    # within a batch; use a second batch for a deterministic learned size.
    runloom.inspect.enable_stack_autosize(True)
    runloom_c.mn_init(2)
    try:
        for _ in range(20):
            runloom_c.mn_go(heavy)        # batch 1: learn
        runloom_c.mn_run()
        for _ in range(20):
            runloom_c.mn_go(heavy)        # batch 2: all start at the learned size
        runloom_c.mn_run()
    finally:
        runloom_c.mn_fini()
    row = _row(heavy)
    assert row["samples"] == 40
    assert row["reserved"] == _learned(row["max_hwm"])   # autosizer applied it
    assert row["reserved"] < START                        # learned down from large
