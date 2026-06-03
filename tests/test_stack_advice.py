"""Tests for the per-goroutine-kind stack-usage advisory profiler
(runloom_stackadvice.c / inspect.enable_stack_advice / stack_advice).

The profiler MEASURES each goroutine kind's real C-stack high-water mark and
recommends a stack_size; it never changes or persists sizes itself.  Note that
on 3.13 pure-Python recursion lives on the datastack, so it barely touches the
C stack -- real C-stack depth comes from C-extension recursion (json/repr/
OpenSSL/...), which is what these tests use to get a measurable signal.
"""
import json
import sys

import pytest

import runloom
import runloom_c


def _nested(depth):
    root = []
    cur = root
    for _ in range(depth):
        nxt = []
        cur.append(nxt)
        cur = nxt
    return root


# Depth chosen so the C json encoder uses a clear, measurable amount of C stack
# (~15 KiB) while staying well under the 32 KiB M:N default -- so the M:N test
# below does not trip the (separate, pre-existing) deep-recursion overflow.
NESTED = _nested(80)


def c_heavy():
    # the C json encoder recurses in C -> real C-stack depth
    json.dumps(NESTED)


def light():
    return 1


@pytest.fixture(autouse=True)
def _clean_advice():
    runloom_c.reset_stack_advice()
    runloom.inspect.enable_stack_advice(False)
    yield
    runloom.inspect.enable_stack_advice(False)
    runloom_c.reset_stack_advice()


def _run(kinds, n, stack=524288):
    for _ in range(n):
        for fn in kinds:
            runloom_c.go(fn, stack)
    runloom_c.run()


def _by_kind(rows):
    out = {}
    for r in rows:
        name = r["kind"].split(" (")[0]   # drop the " (file:line)" suffix
        out[name] = r
    return out


def _kname(fn):
    return "{0}.{1}".format(fn.__module__, fn.__qualname__)


# --------------------------------------------------------------------------- #
def test_disabled_by_default():
    assert runloom_c.stack_advice_enabled() is False
    # spawning with it off records nothing
    _run([light], 5)
    assert runloom_c.stack_advice() == []


def test_enable_disable_roundtrip():
    runloom.inspect.enable_stack_advice(True)
    assert runloom_c.stack_advice_enabled() is True
    runloom.inspect.enable_stack_advice(False)
    assert runloom_c.stack_advice_enabled() is False


def test_measures_per_kind():
    runloom.inspect.enable_stack_advice(True)
    _run([c_heavy, light], 30)
    rows = _by_kind(runloom.inspect.stack_advice())
    assert _kname(c_heavy) in rows
    assert _kname(light) in rows
    assert rows[_kname(c_heavy)]["samples"] == 30
    assert rows[_kname(light)]["samples"] == 30
    # the C-recursing kind must show a deeper high-water mark than the trivial one
    assert rows[_kname(c_heavy)]["max_hwm"] > rows[_kname(light)]["max_hwm"]
    assert rows[_kname(c_heavy)]["max_hwm"] > 8 * 1024


def test_suggested_covers_observed_and_flags_overreserved():
    runloom.inspect.enable_stack_advice(True)
    _run([c_heavy], 30, stack=524288)
    row = _by_kind(runloom.inspect.stack_advice())[_kname(c_heavy)]
    # suggested must cover the observed max (with margin) but be a power of two
    assert row["suggested"] >= row["max_hwm"]
    assert (row["suggested"] & (row["suggested"] - 1)) == 0
    # we reserved 512K but it uses far less -> suggestion is smaller than reserved
    assert row["reserved"] == 524288
    assert row["suggested"] < row["reserved"]


def test_suggestion_has_a_floor():
    runloom.inspect.enable_stack_advice(True)
    _run([light], 20)
    row = _by_kind(runloom.inspect.stack_advice())[_kname(light)]
    # even a near-zero user still gets at least the 16 KiB floor suggested
    assert row["suggested"] >= 16 * 1024


def test_reset_clears_samples():
    runloom.inspect.enable_stack_advice(True)
    _run([light], 5)
    assert runloom_c.stack_advice() != []
    runloom_c.reset_stack_advice()
    assert runloom_c.stack_advice() == []


def test_report_is_sorted_by_usage_desc():
    runloom.inspect.enable_stack_advice(True)
    _run([c_heavy, light], 20)
    rows = runloom.inspect.stack_advice()
    hwms = [r["max_hwm"] for r in rows]
    assert hwms == sorted(hwms, reverse=True)


def test_print_smoke():
    import io
    runloom.inspect.enable_stack_advice(True)
    _run([c_heavy, light], 10)
    buf = io.StringIO()
    runloom.inspect.print_stack_advice(buf)
    text = buf.getvalue()
    assert "runloom stack advice" in text
    assert "c_heavy" in text


def test_records_under_mn_scheduler():
    # The M:N spawn site (mn_go) is hooked independently of the single-sched one.
    runloom.inspect.enable_stack_advice(True)
    runloom_c.mn_init(2)
    try:
        for _ in range(20):
            runloom_c.mn_go(c_heavy)
        runloom_c.mn_run()
    finally:
        runloom_c.mn_fini()
    rows = _by_kind(runloom.inspect.stack_advice())
    assert _kname(c_heavy) in rows
    assert rows[_kname(c_heavy)]["samples"] == 20
    assert rows[_kname(c_heavy)]["max_hwm"] > 8 * 1024
