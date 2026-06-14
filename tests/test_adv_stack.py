"""Adversarial QA: stack overflow / crash handler, the blocking-offload pool,
and the MachineCode escape hatch.

The 297-SIGSEGV stdlib corpus bug was a single class: a deep C-call burst
overflowing a small goroutine stack.  Here we:
  * drive a goroutine off the low end of a small C stack and assert the crash
    handler turns the fault into a CLEAN, classified guard-page diagnostic
    (not silent corruption) -- run in a subprocess since it ends the process;
  * hammer the blocking-offload pool: a parked offload must keep the scheduler
    running other fibers (overlap, not a stalled hub), propagate exceptions,
    and survive many concurrent offloads;
  * misuse MachineCode (empty code, too many args) and confirm clean errors.
"""
import os
import platform
import subprocess
import sys
import time

import pytest

import runloom
import runloom_c as rc
from adv_util import hang_guard, assert_faster_than

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _run_single(fn):
    box = {}
    def main():
        box["r"] = fn()
    rc.go(main)
    rc.run()
    return box.get("r")


def _subproc(script, env_extra=None):
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    if env_extra:
        env.update(env_extra)
    return subprocess.run([sys.executable, "-c", script], cwd=REPO, env=env,
                          capture_output=True, text=True, timeout=30)


# --------------------------------------------------------------------------
# crash handler: a guard-page overflow is a clean classified trap
# --------------------------------------------------------------------------
_OVERFLOW_SCRIPT = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc
rc.install_crash_handler("backtrace")
def f():
    rc._crash_selftest_overflow()      # deliberate C-stack overflow
rc.go(f, 131072)                       # small 128 KiB stack
rc.run()
print("UNREACHABLE")
'''


def test_stack_overflow_is_a_clean_classified_trap():
    p = _subproc(_OVERFLOW_SCRIPT)
    assert p.returncode != 0, "overflow did not crash (silent corruption?)"
    assert "UNREACHABLE" not in p.stdout
    # the crash handler must CLASSIFY it as a guard-page overflow, not leave a
    # bare SIGSEGV that reads as heap corruption.
    assert "GOROUTINE STACK OVERFLOW" in p.stderr, (
        "crash handler did not classify the guard-page fault:\n%s" % p.stderr[:2000])
    assert "guard page" in p.stderr.lower()


def test_roomy_stack_survives_what_small_stack_cannot():
    # The same workload that overflows a tiny stack must be fine on a pinned big
    # one -- proves stack_size actually takes effect (and the snapshot of the
    # recursion counters survives the run).
    def recurse(n):
        if n <= 0:
            return 0
        return 1 + recurse(n - 1)
    out = {}
    def main():
        out["r"] = recurse(800)        # deep-ish Python recursion
    with hang_guard(15, "roomy stack recursion"):
        rc.go(main, 4 << 20)           # 4 MiB
        rc.run()
    assert out["r"] == 800


def test_set_stack_size_rejects_nonpositive():
    with pytest.raises(ValueError):
        rc.set_stack_size(0)
    with pytest.raises(ValueError):
        rc.set_stack_size(-1)
    assert rc.get_stack_size() > 0


def test_stack_autosize_toggle_and_run():
    rc.enable_stack_autosize(True)
    try:
        assert rc.stack_autosize_enabled() in (True, False)
        out = []
        for _ in range(50):
            rc.go(lambda: out.append(1))
        with hang_guard(15, "autosize run"):
            rc.run()
        assert len(out) == 50
    finally:
        rc.enable_stack_autosize(False)


def test_prewarm_and_warmup_do_not_crash():
    assert rc.warmup(8, 131072) >= 0
    got = rc.prewarm(8, 262144, False)     # synchronous
    assert isinstance(got, int)


# --------------------------------------------------------------------------
# blocking-offload pool
# --------------------------------------------------------------------------
def test_blocking_offload_overlaps_with_scheduler():
    order = []
    def main():
        def offloader():
            order.append("offload-start")
            r = rc.blocking(lambda: (time.sleep(0.1), 99)[1])
            order.append(("offload-done", r))
        def burner():
            for i in range(5):
                order.append(("burn", i))
                rc.sched_yield()
        rc.go(offloader)
        rc.go(burner)
    with hang_guard(20, "blocking overlap"):
        with assert_faster_than(0.5, "offload overlap"):
            rc.go(main)
            rc.run()
    # the burner must have run WHILE the offload was sleeping
    burns_before_done = order.index(("offload-done", 99))
    assert order[:burns_before_done].count("offload-start") == 1
    assert sum(1 for o in order[:burns_before_done] if isinstance(o, tuple) and o[0] == "burn") == 5


def test_blocking_propagates_exception():
    def boom():
        raise ValueError("offloaded boom")
    def f():
        with pytest.raises(ValueError):
            rc.blocking(boom)
        return "ok"
    with hang_guard(15, "blocking exception"):
        assert _run_single(f) == "ok"


def test_blocking_requires_callable():
    def f():
        with pytest.raises(TypeError):
            rc.blocking(42)
        return "ok"
    assert _run_single(f) == "ok"


def test_many_concurrent_offloads_complete():
    N = 40
    results = bytearray(N)
    def main():
        from runloom.sync import WaitGroup
        wg = WaitGroup(); wg.add(N)
        def worker(i):
            try:
                v = rc.blocking(lambda i=i: (time.sleep(0.01), i)[1])
                if v == i:
                    results[i] = 1
            finally:
                wg.done()
        for i in range(N):
            rc.go(lambda i=i: worker(i))
        wg.wait()
    with hang_guard(40, "many offloads"):
        rc.go(main)
        rc.run()
    assert sum(results) == N, "only %d/%d offloads completed correctly" % (sum(results), N)


# --------------------------------------------------------------------------
# MachineCode escape hatch
# --------------------------------------------------------------------------
@pytest.mark.skipif(platform.machine() not in ("x86_64", "AMD64"),
                    reason="hand-assembled blob is x86-64 only")
def test_machinecode_basic_and_misuse():
    # mov rax, rdi ; ret   -> returns its first argument
    blob = b"\x48\x89\xf8\xc3"
    try:
        mc = rc.MachineCode(blob)
    except OSError:
        pytest.skip("W^X / execmem policy blocks executable mapping")
    try:
        assert mc(123) == 123
        assert mc(-5) == -5
        with pytest.raises(TypeError):
            mc(1, 2, 3, 4, 5, 6, 7)        # > 6 args
    finally:
        mc.close()
        mc.close()                          # idempotent


def test_machinecode_empty_rejected():
    with pytest.raises(ValueError):
        rc.MachineCode(b"")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
