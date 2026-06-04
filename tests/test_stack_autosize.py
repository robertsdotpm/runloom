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
from decimal import Decimal

import pytest

import runloom
import runloom_c

import os as _hwm_os
import pytest as _hwm_pytest
# Stack high-water-mark is precise only on a POSIX guard-page backend
# (fcontext-asm / ucontext) with 4 KB pages.  Windows Fibers have no guard page,
# and macOS 16 KB pages make the mincore-based HWM over-report (it reports the
# whole stack resident), so these HWM/advice/sizing tests can't measure precisely
# there -- skip them (the diagnostic itself just over-reserves, which is safe).
_RELIABLE_HWM = (_hwm_os.name == "posix"
                 and runloom_c.backend() in ("fcontext-asm", "ucontext")
                 and _hwm_os.sysconf("SC_PAGESIZE") == 4096)
pytestmark = _hwm_pytest.mark.skipif(
    not _RELIABLE_HWM,
    reason="stack HWM is reliable only on a POSIX guard-page backend with 4 KB pages")

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
    # measure-only (no autosize): kinds run at the fixed program default, not a
    # per-kind started/learned size
    runloom.inspect.enable_stack_advice(True)
    _batch([heavy], 20)
    assert _row(heavy)["reserved"] == runloom_c.get_stack_size()


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


def pinned_worker():
    return 1


def test_friendly_go_honors_stack_size():
    # runloom.go(fn, stack_size=N) must PIN the goroutine stack, not forward
    # stack_size into fn (regression: the friendly wrapper used to swallow it
    # into **kwargs and pass it to the target, leaving the goroutine on the
    # default stack).  Covers both the single-thread and M:N spawn paths.
    out = {}

    def main():
        runloom.inspect.enable_stack_advice(True)
        runloom.go(pinned_worker, stack_size=128 * 1024)
        runloom.sleep(0.02)
        out["reserved"] = _row(pinned_worker)["reserved"]

    runloom.run(4, main)
    assert out["reserved"] == 128 * 1024


def crypto_with_arg(x):
    _ = (encrypt, sign)        # crypto names; arg-bearing -> goes via the wrapper
    return x


def plain_with_arg(x):
    return x


def test_friendly_go_with_args_preserves_kind_and_prescan():
    # runloom.go(fn, arg) wraps fn in an arg-binding lambda; __wrapped__ must
    # make the auto-sizer key on fn (not the shared wrapper), so distinct targets
    # are distinct kinds AND the crypto prescan reaches them through the wrapper.
    def main():
        runloom.inspect.enable_stack_autosize(True, prescan=True)
        for i in range(10):
            runloom.go(crypto_with_arg, i)
            runloom.go(plain_with_arg, i)
        runloom.sleep(0.05)

    runloom.run(2, main)
    # not collapsed into the wrapper lambda -> each target is its own kind
    assert _row(crypto_with_arg) is not None
    assert _row(plain_with_arg) is not None
    # and the crypto prescan reached the wrapped function
    assert _row(crypto_with_arg)["reserved"] == CRYPTO_COLD
    assert _row(plain_with_arg)["reserved"] == START


def test_env_start_size(monkeypatch):
    monkeypatch.setenv("RUNLOOM_STACK_AUTOSIZE_START", str(64 * 1024))
    runloom.inspect.enable_stack_autosize(True)   # reads the env at enable time
    _batch([heavy], 10)
    assert _row(heavy)["reserved"] == 64 * 1024


def decimal_kind():
    # references the `Decimal` symbol -> _decimal squaretrans_pow2 (256K frame)
    return Decimal(2) ** 8


# next_pow2(262376 * 1.5), the cold-start size for a Decimal-referencing kind
DECIMAL_COLD = 512 * 1024


def test_prescan_off_by_default():
    runloom.inspect.enable_stack_autosize(True)            # no prescan arg
    _batch([decimal_kind], 20)
    assert _row(decimal_kind)["reserved"] == START         # not bumped


def test_prescan_bumps_a_fat_frame_kind():
    runloom.inspect.enable_stack_autosize(True, prescan=True)
    _batch([decimal_kind, light], 20)
    # the Decimal kind cold-starts big enough to hold the 256K frame ...
    assert _row(decimal_kind)["reserved"] == DECIMAL_COLD
    # ... while a kind with no fat-frame symbols stays at the generic start
    assert _row(light)["reserved"] == START


def test_prescan_does_not_bump_plain_kind():
    runloom.inspect.enable_stack_autosize(True, prescan=True)
    _batch([heavy], 20)        # json -- not a fat-frame symbol
    assert _row(heavy)["reserved"] == START


def test_prescan_floor_holds_decimal():
    # Heuristic floor: a prescan-matched kind never learns DOWN below its
    # cold-start size, even when its measured samples are shallow -- the deep
    # path the symbol signals stays protected (a Decimal kind that did a small
    # op this time could do a big-integer pow next time).
    runloom.inspect.enable_stack_autosize(True, prescan=True)
    _batch([decimal_kind], 20)                  # batch 1: cold-start 512 KiB
    assert _row(decimal_kind)["reserved"] == DECIMAL_COLD
    assert _row(decimal_kind)["max_hwm"] * 4 < DECIMAL_COLD   # really used far less
    _batch([decimal_kind], 20)                  # batch 2: would shrink -> floored
    assert _row(decimal_kind)["reserved"] == DECIMAL_COLD     # held at the floor


# Crypto cold start: signing / verification / encryption route through deep
# OpenSSL / libsodium native math that can overflow a small goroutine stack on
# the first call.  These are HEURISTIC symbols (not measured): a crypto-
# referencing kind cold-starts at 1 MiB, then the auto-sizer learns it down.
encrypt = None      # module-level placeholders so the names land in a kind's
sign = None         # co_names (LOAD_GLOBAL) without importing a crypto library

CRYPTO_COLD = 1024 * 1024   # next_pow2(512K * 1.5) -- the crypto cold start


def crypto_kind():
    _ = (encrypt, sign)         # crypto symbol names -> cold-start bump
    return 1


def crypto_and_decimal():
    _ = (encrypt,)              # crypto -> 512K effective frame (the bigger one)
    return Decimal(2) ** 8      # Decimal -> 256K frame; MAX must pick crypto


def test_prescan_crypto_cold_start_is_1mib():
    runloom.inspect.enable_stack_autosize(True, prescan=True)
    _batch([crypto_kind, light], 20)
    assert _row(crypto_kind)["reserved"] == CRYPTO_COLD   # signing/encryption -> 1 MiB
    assert _row(light)["reserved"] == START                # a plain kind is untouched


def test_prescan_crypto_needs_prescan():
    # without the prescan arg the crypto heuristic does not fire
    runloom.inspect.enable_stack_autosize(True)
    _batch([crypto_kind], 20)
    assert _row(crypto_kind)["reserved"] == START


def test_prescan_crypto_outranks_decimal():
    # a kind referencing BOTH a fat-frame symbol (Decimal, 256K) and a crypto
    # symbol gets the MAX cold start (crypto's 1 MiB), never the sum
    runloom.inspect.enable_stack_autosize(True, prescan=True)
    _batch([crypto_and_decimal], 20)
    assert _row(crypto_and_decimal)["reserved"] == CRYPTO_COLD


def test_prescan_floor_holds_crypto():
    # The crypto heuristic floors at 1 MiB: a crypto kind that measures shallow
    # this run does NOT shrink below it, because the next op (bigger key, AEAD,
    # a different algorithm) may need the depth it didn't exercise.
    runloom.inspect.enable_stack_autosize(True, prescan=True)
    _batch([crypto_kind], 20)                   # batch 1: cold-start 1 MiB
    assert _row(crypto_kind)["reserved"] == CRYPTO_COLD
    assert _row(crypto_kind)["max_hwm"] * 4 < CRYPTO_COLD     # really used far less
    _batch([crypto_kind], 20)                   # batch 2: would shrink -> floored
    assert _row(crypto_kind)["reserved"] == CRYPTO_COLD       # held at the floor


def test_non_prescan_kind_still_learns_down():
    # The floor is prescan-specific: a kind with no heavy-frame symbol still
    # shrinks toward its real usage (the floor is 0 for it).
    runloom.inspect.enable_stack_autosize(True, prescan=True)
    _batch([heavy], 20)
    _batch([heavy], 20)
    assert _row(heavy)["reserved"] < START


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
