"""Tests for the function-bound stack grow-down (runloom.set_grow_down).

The grow-down is the default-on, M:N-only auto-sizer: each fiber starts at
the fixed default stack ("cold start"), measures its real C-stack high-water-mark
on return, and writes a derived, smaller size back onto the callable itself
(fn.__dict__[GROW_DOWN_KEY] = [learned_bytes, spawns_measured]).  The next spawn
of that function reserves only next_pow2(hwm * MARGIN).

The observable is that learned store: a function spawned under run(n>1) ends up
with a learned size well below the default, floored at GROW_DOWN_MIN, while
run(1) / an explicit stack_size= / disabling / the opt-in C autosizer all leave
it untouched.
"""
import json
import os

import pytest

import runloom
import runloom_c
from runloom.runtime import GROW_DOWN_KEY, GROW_DOWN_MIN, GROW_DOWN_SAMPLES

# Stack high-water-mark is precise only on a POSIX guard-page backend
# (fcontext-asm / ucontext) with 4 KB pages -- see test_stack_autosize.py.
_RELIABLE_HWM = (os.name == "posix"
                 and runloom_c.backend() in ("fcontext-asm", "ucontext")
                 and os.sysconf("SC_PAGESIZE") == 4096)
pytestmark = pytest.mark.skipif(
    not _RELIABLE_HWM,
    reason="stack HWM is reliable only on a POSIX guard-page backend with 4 KB pages")

# 80-deep nested list -> json.dumps recurses ~14 KiB of real C stack.
NESTED = []
_cur = NESTED
for _ in range(80):
    _nx = []
    _cur.append(_nx)
    _cur = _nx


@pytest.fixture(autouse=True)
def _clean():
    runloom.set_grow_down(True)
    runloom.inspect.enable_stack_autosize(False)
    yield
    runloom.set_grow_down(True)
    runloom.inspect.enable_stack_autosize(False)


def _spawn_mn(fn, n, hubs=4, **go_kw):
    """Spawn fn n times under run(hubs); mn_run joins them all before returning."""
    def main():
        for _ in range(n):
            runloom.go(fn, **go_kw)
    runloom.run(hubs, main)


def test_on_by_default():
    assert runloom.grow_down_enabled() is True


def test_light_learns_to_floor():
    def worker():
        return 1
    _spawn_mn(worker, 80)
    store = worker.__dict__.get(GROW_DOWN_KEY)
    assert store is not None
    # a do-nothing fiber touches ~1 page -> shrinks to the floor
    assert store[0] == GROW_DOWN_MIN


def test_deep_learns_a_real_size_below_default():
    default = runloom_c.get_stack_size()
    def worker():
        json.dumps(NESTED)       # ~14 KiB of real C stack
    _spawn_mn(worker, 80)
    learned = worker.__dict__.get(GROW_DOWN_KEY)[0]
    # learned a size that covers the real HWM with margin, still well under the
    # default cold start (the whole point: reserve what's needed, not 512 KiB)
    assert GROW_DOWN_MIN <= learned < default
    assert learned & (learned - 1) == 0    # power of two
    assert learned >= 32 * 1024            # covers ~14 KiB * 4 margin


def test_n1_does_not_learn():
    # single-thread run(1) keeps the fixed default -- no learning, no store
    def worker():
        json.dumps(NESTED)
    runloom.run(1, lambda: [runloom.go(worker) for _ in range(40)])
    assert worker.__dict__.get(GROW_DOWN_KEY) is None


def test_disable_bypasses():
    runloom.set_grow_down(False)
    def worker():
        return 1
    _spawn_mn(worker, 40)
    assert worker.__dict__.get(GROW_DOWN_KEY) is None
    assert runloom.grow_down_enabled() is False


def test_explicit_pin_bypasses():
    def worker():
        return 1
    _spawn_mn(worker, 40, stack_size=128 * 1024)
    assert worker.__dict__.get(GROW_DOWN_KEY) is None


def test_defers_to_c_autosizer_when_enabled():
    def worker():
        return 1
    def main():
        runloom.inspect.enable_stack_autosize(True)
        for _ in range(40):
            runloom.go(worker)
    runloom.run(4, main)
    # the explicitly-enabled C autosizer wins; grow-down backs off entirely
    assert worker.__dict__.get(GROW_DOWN_KEY) is None


def test_freezes_after_samples():
    # spawn far more than GROW_DOWN_SAMPLES; the measured/wrapped count is capped,
    # so the steady state stops paying the per-completion measurement
    def worker():
        return 1
    total = GROW_DOWN_SAMPLES + 200
    _spawn_mn(worker, total)
    store = worker.__dict__.get(GROW_DOWN_KEY)
    assert store is not None
    assert store[0] == GROW_DOWN_MIN
    # froze: only ~GROW_DOWN_SAMPLES spawns were ever wrapped (a small concurrent
    # overshoot is fine), nowhere near the full `total`
    assert store[1] <= GROW_DOWN_SAMPLES + 8
    assert store[1] < total


def test_non_introspectable_callable_is_safe():
    # a callable with no writable __dict__ (slots) can't carry a learned size;
    # it must fall back to the cold start without crashing
    class SlotCallable:
        __slots__ = ()
        def __call__(self):
            return 1
    c = SlotCallable()
    assert getattr(c, "__dict__", None) is None
    _spawn_mn(c, 10)     # must not raise


def test_arg_bearing_binds_to_real_function():
    # runloom.go(fn, arg) wraps fn in an arg-binding lambda; the learned size must
    # bind to fn (shared across all arg variants), not the per-call wrapper
    def worker(x):
        json.dumps(NESTED)
        return x
    def main():
        for i in range(80):
            runloom.go(worker, i)
    runloom.run(4, main)
    store = worker.__dict__.get(GROW_DOWN_KEY)
    assert store is not None and store[0] >= 32 * 1024
