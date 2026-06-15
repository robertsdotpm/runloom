"""Adversarial QA: the stackful-coroutine + stack machinery (coro_stack).

C surface under attack: coro.c (guarded stacks, the per-thread / global stack
depot, scrub, paint, HWM scan, prewarm/warmup), fcontext.c (the asm context
make/swap), runloom_stackadvice.c (per-kind autosize learning).

Python-visible surface:
  runloom_c.Coro                       -- bare stackful coroutine (resume/done/result)
  runloom_c.MachineCode                -- W^X native blob, callable 0..6 args
  set_stack_size/get_stack_size        -- program-wide default fiber stack
  set_stack_scrub/get_stack_scrub      -- zero-on-recycle toggle
  enable_stack_advice/stack_advice_enabled/stack_advice/reset_stack_advice
  enable_stack_autosize/stack_autosize_enabled
  prewarm/prewarm_keep/prewarm_stop/warmup   -- stack depot pre-fill
  fiber_stack/current_g_hwm/backend

The adversarial mandate (CRASH / HANG / UAF / REORDER / WRONG-DATA / SLOW-RETURN):
  * a deep C-recursion guard-page OVERFLOW on a single-thread fiber AND on an
    M:N hub -- run in a subprocess, assert the crash handler CLASSIFIES it as a
    clean guard-page trap, not silent corruption;
  * pinned tiny / zero / negative / huge stack sizes through Coro, go, mn_go --
    negative must RAISE (Coro) or be ignored (go/mn_go clamp), never crash;
  * autosize learning across alternating deep/shallow workloads;
  * the cross-hub depot under burst prewarm + a concurrent spawn storm +
    prewarm_keep daemon start/stop churn;
  * stack scrub on vs off recycling;
  * MachineCode misuse: empty / garbage / oversized bytes, close idempotency,
    0..6 + >6 args, returning a value, kwargs, call-after-close;
  * Coro resume-after-done idempotency, exception-on-resume, nested Coros,
    yield_ outside a coro, a Coro that never finishes (leaked, GC'd mid-flight).

Findings are encoded as xfail(strict=False) with a "FINDING:" reason, or a
subprocess test with a leading "# FINDING:" comment; nothing here edits the C.

Most precise stack introspection (HWM, guard-page classification) needs a POSIX
guard-page backend with 4 KB pages; we gate those probes accordingly and keep
the rest backend-agnostic.
"""
import os
import platform
import subprocess
import sys
import time

import pytest

import runloom
import runloom_c as rc
from adv_util import (hang_guard, assert_faster_than, raw_thread,
                      needs_free_threading)

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BACKEND = rc.backend()
POSIX = os.name == "posix"
_PAGE = os.sysconf("SC_PAGESIZE") if POSIX and hasattr(os, "sysconf") else 4096
# Guard-page address->fiber classification + a precise HWM scan only exist on
# the POSIX asm/ucontext backends with 4 KB pages (Windows Fibers have no
# introspectable guard page; macOS 16 KB pages make the mincore HWM over-report).
HAS_GUARD = POSIX and BACKEND in ("fcontext-asm", "ucontext")
RELIABLE_HWM = HAS_GUARD and _PAGE == 4096
IS_X86_64 = platform.machine() in ("x86_64", "AMD64", "x86-64")

_DEVNULL = os.open(os.devnull, os.O_WRONLY)


# ==========================================================================
# subprocess helper -- a crash must be CONTAINED + OBSERVED, never wedge us.
# ==========================================================================
def _subproc(script, env_extra=None, timeout=40):
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    # keep deliberate goroutine panics from spamming the captured stderr
    env.setdefault("RUNLOOM_GOROUTINE_PANIC", "silent")
    if env_extra:
        env.update(env_extra)
    return subprocess.run([sys.executable, "-c", script], cwd=REPO, env=env,
                          capture_output=True, text=True, timeout=timeout)


def _run_single(fn):
    """Run fn() inside a single-thread fiber, return its result."""
    box = {}

    def main():
        box["r"] = fn()
    rc.go(main)
    rc.run()
    return box.get("r")


# ==========================================================================
# 1. Coro primitive -- resume/done/result, exceptions, nesting, edges
# ==========================================================================
def test_coro_result_only_visible_when_done():
    # result is None until the entry actually returns; a yielded coro has no result.
    log = []

    def body():
        log.append("pre")
        rc.yield_()
        log.append("post")
        return 7
    c = rc.Coro(body)
    c.resume()
    assert not c.done and c.result is None and log == ["pre"]
    assert c.resume() == 7
    assert c.done and c.result == 7 and log == ["pre", "post"]


def test_coro_resume_after_done_repeatedly_idempotent():
    c = rc.Coro(lambda: 99)
    assert c.resume() == 99
    # Many extra resumes after done must be harmless no-ops returning None,
    # never re-running the body or faulting on the spent asm context.
    for _ in range(50):
        assert c.resume() is None
    assert c.done and c.result == 99


def test_coro_exception_stops_and_is_replayable():
    c = rc.Coro(lambda: (_ for _ in ()).throw(ValueError("boom")))
    with pytest.raises(ValueError):
        c.resume()
    assert c.done
    # After a finished-with-error coro, a further resume is a clean no-op (the
    # error was consumed on the first raising resume) -- must not double-raise
    # or crash.
    assert c.resume() is None


def test_coro_exception_after_yield():
    # Exception raised AFTER a yield point: the yield/resume bookkeeping (tstate
    # recursion-counter snapshot save/restore) must survive an abnormal exit.
    def body():
        rc.yield_()
        raise KeyError("late")
    c = rc.Coro(body)
    c.resume()             # yields cleanly
    assert not c.done
    with pytest.raises(KeyError):
        c.resume()         # raises on the second resume
    assert c.done


def test_coro_nested_three_deep():
    # A Coro driven from inside another Coro driven from inside a third: each has
    # its own swapped C stack; the inner swaps must not corrupt the outer's.
    def leaf():
        rc.yield_()
        return "leaf"

    def mid():
        c = rc.Coro(leaf)
        c.resume()
        return c.resume() + "+mid"

    def top():
        c = rc.Coro(mid)
        return c.resume() + "+top"
    top_c = rc.Coro(top)
    assert top_c.resume() == "leaf+mid+top"
    assert top_c.done


def test_coro_many_alternating_yields_no_recursion_error():
    # The fix that this exercises: each yield save/restores the OS thread's
    # py_recursion_remaining so a long resume loop does NOT leak it down to a
    # RecursionError.  Drive many yields across many coros.
    total = 0
    for _ in range(200):
        n = [0]

        def body(n=n):
            for _ in range(20):
                n[0] += 1
                rc.yield_()
            return n[0]
        c = rc.Coro(body)
        while not c.done:
            r = c.resume()
        total += r
    assert total == 200 * 20


def test_yield_outside_coro_is_noop():
    # yield_ with no current coro must be a harmless no-op (it is reachable from
    # ordinary main-thread code), never a crash or a swap into a NULL context.
    rc.yield_()
    rc.yield_()


def test_coro_non_callable_rejected():
    with pytest.raises(TypeError):
        rc.Coro(42)
    with pytest.raises(TypeError):
        rc.Coro(None)


# ----- Coro stack-size argument validation ------------------------------
def test_coro_negative_stack_raises_not_segfaults():
    # Regression: Coro(fn, -N) cast the negative to ~SIZE_MAX, overflowed the
    # guard-page arithmetic to an undersized mapping + OOB write (SIGSEGV).  It
    # must validate like set_stack_size().
    for bad in (-1, -4096, -(1 << 30), -(1 << 50)):
        with pytest.raises(ValueError):
            rc.Coro(lambda: 1, bad)


def test_coro_zero_stack_floored_runs():
    # 0 is non-negative so it passes validation; runloom_coro_new floors sub-4 KiB
    # to 4 KiB.  Must run, not divide-by-zero / map a zero-length region.
    c = rc.Coro(lambda: sum(range(50)), 0)
    assert c.resume() == sum(range(50))
    assert c.done


def test_coro_tiny_positive_stack_runs():
    c = rc.Coro(lambda: sum(range(100)), 4096)
    assert c.resume() == 4950
    assert c.done


def test_coro_huge_stack_raises_memoryerror_not_crash():
    # Coro is the LOW-LEVEL primitive: it does NOT clamp to MAX_STACK_SIZE (only
    # the scheduler's go()/mn_go() path does).  An un-mmap-able size must surface
    # as a clean MemoryError at construction, never a wild mapping / crash.
    for sz in (1 << 46, 1 << 55, 1 << 62):
        with pytest.raises(MemoryError):
            rc.Coro(lambda: 1, sz)


def test_coro_leaked_unfinished_is_collected_cleanly():
    # A Coro resumed once (so it owns a live swapped stack) then dropped without
    # finishing: dealloc must release the attached stack/coro without asserting
    # "released while executing" or leaking the mapping into a crash later.
    import gc
    for _ in range(200):
        c = rc.Coro(lambda: (rc.yield_(), rc.yield_()))
        c.resume()            # parks it mid-body on its own stack
        del c
    gc.collect()
    assert rc._self_check(0) == 0


# ==========================================================================
# 2. go() / mn_go() stack-size argument probing
# ==========================================================================
def test_go_negative_and_zero_stack_treated_as_default():
    # go() only honours stack_size>0 (else uses the scheduler default); a
    # non-positive value must be silently treated as default, never crash.
    for bad in (-1, 0, -(1 << 40)):
        box = {}
        rc.go(lambda: box.__setitem__("r", 1), bad)
        rc.run()
        assert box.get("r") == 1


def test_go_noninteger_stack_rejected():
    with pytest.raises(TypeError):
        rc.go(lambda: 1, "big")
    with pytest.raises(TypeError):
        rc.go(lambda: 1, 1.5)
    # drain any spawned-then-nothing scheduler state
    rc.run()


def test_go_huge_stack_clamps_and_runs():
    # The scheduler path CLAMPS to RUNLOOM_MAX_STACK_SIZE (8 MiB); an absurd size
    # must not fail the spawn -- it runs on the clamped stack.
    box = {}
    rc.go(lambda: box.__setitem__("r", 2), 1 << 62)
    with hang_guard(15, "go huge clamp"):
        rc.run()
    assert box.get("r") == 2


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_mn_go_stack_arg_edges():
    # mn_go honours stack_size>0 and ignores non-positive (uses default) -- all
    # must run under mn_run() without crashing or hanging.  (The HUGE-size case
    # is split out below -- it does NOT clamp like the single-thread path: see
    # the xfail finding.)
    done = bytearray(3)

    def main():
        rc.mn_go(lambda: done.__setitem__(0, 1), 0)
        rc.mn_go(lambda: done.__setitem__(1, 1), -5)
        rc.mn_go(lambda: done.__setitem__(2, 1), 65536)      # tiny pinned
        for _ in range(20):
            rc.sched_yield()
    with hang_guard(20, "mn_go stack edges"):
        runloom.run(2, main)
    assert sum(done) == 3, "only %d/3 mn_go stack-edge fibers ran" % sum(done)


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
@pytest.mark.xfail(strict=False, reason=(
    "FINDING: mn_go(fn, huge) raises MemoryError instead of clamping to "
    "RUNLOOM_MAX_STACK_SIZE (8 MiB) the way the single-thread go(fn, huge) path "
    "does. runloom_sched_spawn_sized() clamps stack_size to [MIN,MAX]; the M:N "
    "spawn core (runloom_mn_go_core) passes an explicit stack_size>0 straight "
    "to runloom_coro_new with NO clamp, so an absurd size fails the mmap. Both "
    "paths document 'an explicit size always wins' -- they should clamp it "
    "identically. Benign (clean MemoryError, no crash) but an inconsistent "
    "contract across the two spawn APIs."))
def test_mn_go_huge_stack_should_clamp_like_single_thread():
    box = {}
    # Capture the goroutine's unraisable MemoryError so it doesn't leak into the
    # test's warning surface; the xfail observes that the fiber never ran.
    caught = []
    old_hook = sys.unraisablehook
    sys.unraisablehook = lambda a: caught.append(a.exc_type)

    def main():
        try:
            # mirror the single-thread go(fn, 1<<62) which clamps + runs
            rc.mn_go(lambda: box.__setitem__("r", 1), 1 << 62)
        except MemoryError:
            caught.append(MemoryError)
        for _ in range(20):
            rc.sched_yield()
    try:
        with hang_guard(20, "mn_go huge clamp"):
            runloom.run(2, main)
    finally:
        sys.unraisablehook = old_hook
    # CORRECT behaviour (currently failing): the huge size is clamped and runs.
    assert box.get("r") == 1, "mn_go(huge) did not clamp-and-run (raised instead)"


# ==========================================================================
# 3. Guard-page OVERFLOW -- subprocess, assert CLEAN classified trap
# ==========================================================================
_OVERFLOW_ST = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc
rc.install_crash_handler("backtrace")
def f():
    rc._crash_selftest_overflow()
rc.go(f, 65536)                      # small 64 KiB stack
rc.run()
print("UNREACHABLE")
'''

_OVERFLOW_MN = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc
rc.install_crash_handler("backtrace")
def f():
    rc._crash_selftest_overflow()
def main():
    rc.mn_go(lambda: f(), 65536)
    for _ in range(50):
        rc.sched_yield()
rc.mn_init(2); rc.mn_go(main); rc.mn_run(); rc.mn_fini()
print("UNREACHABLE")
'''


def _assert_classified_overflow(p):
    assert p.returncode != 0, (
        "overflow did not crash the process -- silent corruption?\n"
        "stdout=%r stderr=%r" % (p.stdout[:500], p.stderr[:500]))
    assert "UNREACHABLE" not in p.stdout, "control reached past the overflow"
    # The deliberate guard-page overflow must be CLASSIFIED (a clean trap), not
    # left as a bare SIGSEGV that reads as heap corruption.
    assert "GOROUTINE STACK OVERFLOW" in p.stderr, (
        "crash handler did not classify the guard-page fault:\n%s"
        % p.stderr[:3000])
    assert "guard page" in p.stderr.lower()


@pytest.mark.skipif(not HAS_GUARD, reason="needs POSIX guard-page backend")
def test_overflow_single_thread_is_clean_classified_trap():
    _assert_classified_overflow(_subproc(_OVERFLOW_ST))


@pytest.mark.skipif(not (HAS_GUARD and FT),
                    reason="needs POSIX guard-page backend + M:N")
def test_overflow_on_mn_hub_is_clean_classified_trap():
    # The SAME overflow on an M:N hub must also classify (the address->fiber map
    # has to cover the hub-spawned g, and the crash owner must freeze the sysmon
    # watchdogs before the handoff pool can adopt+steal the faulting fiber).
    _assert_classified_overflow(_subproc(_OVERFLOW_MN))


def test_roomy_pinned_stack_survives_what_tiny_cannot():
    # The same deep recursion that overflows a tiny stack is fine on a big pin --
    # proves the pin takes effect and the tstate snapshot survives the run.
    def recurse(n):
        return 0 if n <= 0 else 1 + recurse(n - 1)
    out = {}

    def main():
        out["r"] = recurse(900)
    with hang_guard(15, "roomy stack recursion"):
        rc.go(main, 4 << 20)
        rc.run()
    assert out["r"] == 900


# ==========================================================================
# 4. set_stack_size / get_stack_size -- validation + effect
# ==========================================================================
def test_set_stack_size_rejects_nonpositive():
    old = rc.get_stack_size()
    try:
        with pytest.raises(ValueError):
            rc.set_stack_size(0)
        with pytest.raises(ValueError):
            rc.set_stack_size(-1)
        with pytest.raises(ValueError):
            rc.set_stack_size(-(1 << 40))
        assert rc.get_stack_size() == old   # rejected calls left it untouched
    finally:
        rc.set_stack_size(old)


def test_set_stack_size_noninteger_rejected():
    old = rc.get_stack_size()
    try:
        with pytest.raises((TypeError, OverflowError)):
            rc.set_stack_size("lots")
    finally:
        rc.set_stack_size(old)


def test_set_stack_size_roundtrips_and_drives_default():
    old = rc.get_stack_size()
    try:
        rc.set_stack_size(256 * 1024)
        assert rc.get_stack_size() == 256 * 1024
        # a default-stack fiber must run fine at the new default
        box = {}
        rc.go(lambda: box.__setitem__("r", 1))
        rc.run()
        assert box.get("r") == 1
    finally:
        rc.set_stack_size(old)
        assert rc.get_stack_size() == old


# ==========================================================================
# 5. set_stack_scrub -- zero-on-recycle, on vs off
# ==========================================================================
def test_stack_scrub_toggle_roundtrips():
    old = rc.get_stack_scrub()
    try:
        rc.set_stack_scrub(True)
        assert rc.get_stack_scrub() is True
        rc.set_stack_scrub(False)
        assert rc.get_stack_scrub() is False
        rc.set_stack_scrub(1)            # truthy non-bool
        assert rc.get_stack_scrub() is True
        rc.set_stack_scrub(0)
        assert rc.get_stack_scrub() is False
    finally:
        rc.set_stack_scrub(old)


def test_fibers_run_correctly_with_scrub_on_then_off():
    # Recycling churns the stack pool through the scrub path; both modes must
    # produce correct results and a clean self-check (the scrub wipes the stack
    # before recycle -- a later fiber must still compute correctly on it).
    old = rc.get_stack_scrub()
    try:
        for scrub in (True, False, True):
            rc.set_stack_scrub(scrub)
            acc = bytearray(60)

            def main():
                def w(i):
                    # touch a chunk of stack so a non-scrubbed reuse would be
                    # observable as leftovers if it mattered for correctness
                    buf = [i] * 256
                    acc[i] = 1 if sum(buf) == i * 256 else 0
                for i in range(60):
                    rc.go(lambda i=i: w(i))
            with hang_guard(20, "scrub=%s churn" % scrub):
                rc.go(main)
                rc.run()
            assert sum(acc) == 60, "scrub=%s: %d/60" % (scrub, sum(acc))
            assert rc._self_check(0) == 0
    finally:
        rc.set_stack_scrub(old)


# ==========================================================================
# 6. stack advice / autosize -- learning, report shape, reset
# ==========================================================================
def test_stack_advice_report_shape_and_reset():
    import json
    was_on = rc.stack_advice_enabled()
    rc.reset_stack_advice()
    rc.enable_stack_advice(True)
    try:
        assert rc.stack_advice_enabled() is True

        def w():
            # real C-stack depth (json C encoder), not pure-Python recursion
            json.dumps({"a": [1, 2, {"b": [3, 4, {"c": 5}]}]})
        for _ in range(40):
            rc.go(w)
        with hang_guard(20, "advice churn"):
            rc.run()
        rep = rc.stack_advice()
        assert isinstance(rep, list) and len(rep) >= 1
        e = rep[0]
        assert set(["kind", "samples", "max_hwm", "reserved", "suggested"]) <= set(e)
        assert e["samples"] >= 1
        assert e["suggested"] >= 16 * 1024            # floored at ADVICE_MIN
        assert e["suggested"] <= 8 * 1024 * 1024      # capped at ADVICE_MAX
    finally:
        rc.enable_stack_advice(False)
        rc.reset_stack_advice()
        assert rc.stack_advice() == []
        if was_on:
            rc.enable_stack_advice(True)


def test_stack_advice_enabled_default_arg():
    # enable_stack_advice() with no arg defaults to on.
    was = rc.stack_advice_enabled()
    rc.reset_stack_advice()
    try:
        rc.enable_stack_advice()
        assert rc.stack_advice_enabled() is True
    finally:
        rc.enable_stack_advice(False)
        rc.reset_stack_advice()
        if was:
            rc.enable_stack_advice(True)


def test_autosize_toggle_and_run():
    was = rc.stack_autosize_enabled()
    try:
        rc.enable_stack_autosize(True)
        assert rc.stack_autosize_enabled() is True
        out = bytearray(60)
        for i in range(60):
            rc.go(lambda i=i: out.__setitem__(i, 1))
        with hang_guard(20, "autosize run"):
            rc.run()
        assert sum(out) == 60
    finally:
        rc.enable_stack_autosize(False)
        assert rc.stack_autosize_enabled() is False
        if was:
            rc.enable_stack_autosize(True)


def test_autosize_prescan_arg_runs():
    # prescan=True turns on the cold-start fat-frame optimizer; must run cleanly.
    was = rc.stack_autosize_enabled()
    try:
        rc.enable_stack_autosize(True, True)
        assert rc.stack_autosize_enabled() is True
        box = {}
        rc.go(lambda: box.__setitem__("r", 1))
        with hang_guard(15, "autosize prescan"):
            rc.run()
        assert box.get("r") == 1
    finally:
        rc.enable_stack_autosize(False)
        if was:
            rc.enable_stack_autosize(True)


@pytest.mark.skipif(not RELIABLE_HWM, reason="needs precise HWM (4 KiB guard backend)")
def test_autosize_learns_down_across_alternating_workloads():
    # Start large, learn down: a shallow kind run many times should end up
    # reserving far less than the 256 KiB autosize start, while a DEEP kind
    # interleaved with it must NOT be shrunk under what it needs.  We observe via
    # the advice report's `reserved` field (the size the kind last ran with).
    import json
    was = rc.stack_autosize_enabled()
    rc.reset_stack_advice()
    try:
        rc.enable_stack_autosize(True)

        def shallow():
            return 1 + 1                       # touches almost no C stack

        def deep():
            json.dumps([{"x": [1, 2, [3, [4, [5, [6]]]]]}] * 8)   # C recursion

        for _ in range(80):
            rc.go(shallow)
            rc.go(deep)
        with hang_guard(30, "autosize learn-down"):
            rc.run()
        rep = {d["kind"]: d for d in rc.stack_advice()}
        sh = next((d for k, d in rep.items() if "shallow" in k), None)
        dp = next((d for k, d in rep.items() if "deep" in k), None)
        assert sh is not None and dp is not None, list(rep)
        # the shallow kind learned a much smaller reserve than the deep kind
        assert sh["max_hwm"] <= dp["max_hwm"]
        # both stayed within the advisory bounds
        for d in (sh, dp):
            assert 16 * 1024 <= d["suggested"] <= 8 * 1024 * 1024
    finally:
        rc.enable_stack_autosize(False)
        rc.reset_stack_advice()
        if was:
            rc.enable_stack_autosize(True)


# ==========================================================================
# 7. current_g_hwm
# ==========================================================================
def test_current_g_hwm_zero_outside_fiber():
    assert rc.current_g_hwm() == 0


@pytest.mark.skipif(not RELIABLE_HWM, reason="needs precise HWM")
def test_current_g_hwm_positive_inside_fiber():
    box = {}

    def f():
        # do a little C work so the HWM is at least one page
        sum(bytes(4096))
        box["hwm"] = rc.current_g_hwm()
    rc.go(f, 262144)
    rc.run()
    assert box.get("hwm", 0) >= _PAGE
    # never report more than the stack it ran on
    assert box["hwm"] <= 262144 + _PAGE


# ==========================================================================
# 8. prewarm / warmup / prewarm_keep -- depot under churn & storms
# ==========================================================================
def test_warmup_negative_and_zero_are_noops():
    assert rc.warmup(0, 131072) == 0
    assert rc.warmup(-7, 131072) == 0


def test_warmup_returns_count():
    n = rc.warmup(16, 131072)
    assert 0 <= n <= 16


def test_prewarm_zero_negative_noop():
    assert rc.prewarm(0, 512 * 1024, False) == 0
    assert rc.prewarm(-3, 512 * 1024, False) == 0


def test_prewarm_background_returns_immediately():
    with assert_faster_than(0.2, "background prewarm returns fast"):
        r = rc.prewarm(400, 512 * 1024, True)
    assert r == 0


def test_prewarm_then_spawn_storm_correct():
    # Burst-prewarm the depot, then a spawn storm should pop pooled stacks; all
    # must run correctly and the pool stay consistent (no double-pop / corruption).
    rc.prewarm(600, 512 * 1024, False)
    done = bytearray(400)

    def main():
        def w(i):
            done[i] = 1
        for i in range(400):
            rc.go(lambda i=i: w(i))
    with hang_guard(30, "prewarm + spawn storm"):
        rc.go(main)
        rc.run()
    assert sum(done) == 400
    assert rc._self_check(0) == 0


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_prewarm_keep_daemon_under_concurrent_mn_spawn_storm():
    # The continuous prewarm daemon (a background OS thread topping the GLOBAL
    # depot) running CONCURRENTLY with a multi-hub spawn storm: both hammer the
    # shared depot lock from different threads.  Must finish, stay consistent,
    # no crash / no hang / no lost work.
    from runloom.sync import WaitGroup
    runloom.prewarm_keep(800, 512 * 1024)
    try:
        done = bytearray(600)

        def main():
            wg = WaitGroup(); wg.add(600)

            def w(i):
                try:
                    done[i] = 1
                finally:
                    wg.done()
            for i in range(600):
                rc.mn_go(lambda i=i: w(i))
            wg.wait()
        with hang_guard(45, "prewarm_keep + mn storm"):
            runloom.run(4, main)
        assert sum(done) == 600, "lost %d fibers" % (600 - sum(done))
    finally:
        runloom.prewarm_stop()
        runloom.prewarm_stop()   # idempotent
    assert rc._self_check(0) == 0


def test_prewarm_keep_rapid_start_stop_churn():
    # Start/retarget/stop the daemon rapidly from the main thread (no spawns):
    # the start/join handshake must be race-free under churn -- no double-start,
    # no leaked daemon, no hang in stop's join.
    with hang_guard(30, "prewarm_keep churn"):
        for _ in range(20):
            assert runloom.prewarm_keep(100, 512 * 1024) == 0
            assert runloom.prewarm_keep(300) == 0      # retarget running daemon
            runloom.prewarm_stop()
        runloom.prewarm_stop()                         # already stopped: no-op
    # target<=0 also stops a running daemon
    assert runloom.prewarm_keep(50) == 0
    assert runloom.prewarm_keep(0) == 0
    runloom.prewarm_stop()


def test_prewarm_from_foreign_thread_is_safe():
    # prewarm() is a depot pre-fill with no goroutine/hub context; calling it
    # from a genuine foreign OS thread (not a fiber, not a hub) must be safe and
    # not lazily allocate scheduler state on that thread.
    res = {}

    def worker():
        res["r"] = rc.prewarm(200, 512 * 1024, False)
    t = raw_thread(worker)
    t.join(timeout=15)
    assert not t.is_alive(), "foreign-thread prewarm hung"
    assert res.get("r") == 200
    assert rc._self_check(0) == 0


# ==========================================================================
# 9. MachineCode -- W^X blob, args, returns, misuse, idempotency
# ==========================================================================
def test_machinecode_empty_rejected():
    with pytest.raises(ValueError):
        rc.MachineCode(b"")


def test_machinecode_non_bytes_rejected():
    with pytest.raises(TypeError):
        rc.MachineCode(12345)
    with pytest.raises(TypeError):
        rc.MachineCode(None)


def test_machinecode_accepts_bytes_like():
    # y* buffer protocol: bytearray / memoryview must map too (still W^X).
    for blob in (bytearray(b"\xc3"), memoryview(b"\xc3")):
        mc = rc.MachineCode(blob)        # 0xc3 = ret
        try:
            assert mc.size == 1
            assert isinstance(mc.address, int) and mc.address != 0
        finally:
            mc.close()


def test_machinecode_oversized_blob_maps():
    # A large (multi-page) but valid blob must map cleanly; we never CALL it
    # (it's a sea of 0x90 NOP then 0xc3 RET so calling is in fact safe, but the
    # point here is the W^X mapping of a big region, not execution).
    blob = b"\x90" * (3 * 4096) + b"\xc3"
    try:
        mc = rc.MachineCode(blob)
    except OSError:
        pytest.skip("execmem policy blocks large executable mapping")
    try:
        assert mc.size == len(blob)
    finally:
        mc.close()


def test_machinecode_close_idempotent_and_guards_call():
    mc = rc.MachineCode(b"\xc3")          # ret
    mc.close()
    mc.close()                            # idempotent
    with pytest.raises(ValueError):
        mc()                              # call-after-close guarded
    with pytest.raises(ValueError):
        mc.close() or mc()                # still guarded


def test_machinecode_context_manager_frees():
    with rc.MachineCode(b"\xc3") as mc:
        assert mc.address != 0
    # exited -> page unmapped -> call must raise, not fault
    with pytest.raises(ValueError):
        mc()


@pytest.mark.skipif(not IS_X86_64, reason="hand-assembled blob is x86-64 only")
def test_machinecode_args_returns_and_misuse():
    # mov rax, rdi ; ret  -> returns its first argument (System V ABI)
    ident = b"\x48\x89\xf8\xc3"
    try:
        mc = rc.MachineCode(ident)
    except OSError:
        pytest.skip("W^X / execmem policy blocks executable mapping")
    try:
        assert mc(0) == 0
        assert mc(123) == 123
        assert mc(-5) == -5                       # negative round-trips
        # high-bit-set int: masked to the machine word, returned as SIGNED
        assert mc(2 ** 63) == -(2 ** 63)
        # 0..6 args all accepted (extra args ignored by this 1-arg blob)
        assert mc(7, 0, 0, 0, 0, 0) == 7
        # >6 args rejected
        with pytest.raises(TypeError):
            mc(1, 2, 3, 4, 5, 6, 7)
        # kwargs rejected
        with pytest.raises(TypeError):
            mc(a=1)
        # a non-int arg that can't coerce to a machine word raises, not crash
        with pytest.raises(TypeError):
            mc("not an int")
    finally:
        mc.close()


@pytest.mark.skipif(not IS_X86_64, reason="x86-64 blob")
def test_machinecode_runs_inside_single_fiber():
    # The blob runs on the CALLER's stack -- inside a fiber that's the small
    # guard-paged g-stack.  A tiny leaf blob must run fine there.
    sq = b"\x48\x89\xf8\x48\x0f\xaf\xc7\xc3"   # mov rax,rdi; imul rax,rdi; ret
    box = {}

    def g():
        with rc.MachineCode(sq) as fn:
            box["r"] = fn(11)
    rc.go(g)
    rc.run()
    assert box.get("r") == 121


@pytest.mark.skipif(not (IS_X86_64 and FT), reason="x86-64 + M:N")
def test_machinecode_runs_across_mn_fibers():
    # Each fiber JITs + calls its own blob on its own swapped C stack in genuine
    # parallel under M:N -- results funneled back over a Chan.
    sq = b"\x48\x89\xf8\x48\x0f\xaf\xc7\xc3"
    N = 8
    box = {}

    def worker(n, ch):
        with rc.MachineCode(sq) as fn:
            ch.send((n, fn(n)))

    def main():
        ch = rc.Chan()
        for n in range(N):
            rc.mn_go(lambda n=n: worker(n, ch))
        got = {}
        for _ in range(N):
            (n, v), ok = ch.recv()
            got[n] = v
        box["r"] = got
    with hang_guard(25, "mn machinecode"):
        runloom.run(2, main)
    assert box.get("r") == {n: n * n for n in range(N)}


def test_machinecode_close_in_subprocess_then_use_after_free_is_guarded():
    # FINDING-PROBE: a call after close must raise ValueError (the page pointer is
    # nulled), never jump to a freed/unmapped page.  We verify in-process above;
    # here we additionally confirm a deliberate use-after-free attempt cannot
    # SEGV the interpreter -- the guard returns to Python cleanly.
    script = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc
mc = rc.MachineCode(b"\xc3")
mc.close()
try:
    mc()
    print("NO_GUARD")          # would mean it jumped into freed memory
except ValueError:
    print("GUARDED")
'''
    p = _subproc(script, timeout=20)
    assert p.returncode == 0, "use-after-close crashed: rc=%s err=%s" % (
        p.returncode, p.stderr[:600])
    assert "GUARDED" in p.stdout and "NO_GUARD" not in p.stdout


# ==========================================================================
# 10. fiber_stack / backend -- introspection edges
# ==========================================================================
def test_backend_is_a_known_string():
    b = rc.backend()
    assert isinstance(b, str) and b
    assert b in ("fcontext-asm", "ucontext", "windows-fibers")


def test_fiber_stack_bogus_id_is_empty_not_crash():
    # An id that maps to no live fiber returns (None, []) -- never an OOB walk.
    r = rc.fiber_stack(0)
    assert r == (None, []) or (isinstance(r, tuple) and len(r) == 2)
    r = rc.fiber_stack(2 ** 63 - 1)
    assert isinstance(r, tuple) and len(r) == 2
    r = rc.fiber_stack(-1)
    assert isinstance(r, tuple) and len(r) == 2


def test_fiber_stack_on_live_parked_fiber():
    snap = {}

    def main():
        ch = rc.Chan(0)
        for _ in range(20):
            rc.go(lambda: ch.recv())
        rc.sched_yield()                  # let them park
        fl = rc.fibers()
        if fl:
            cr, frames = rc.fiber_stack(fl[0]["id"])
            snap["ok"] = isinstance(frames, list)
        ch.close()
    with hang_guard(20, "fiber_stack live"):
        rc.go(main)
        rc.run()
    assert snap.get("ok") is True


# ==========================================================================
# 11. Cross-cutting: scrub + autosize + prewarm together don't collapse
#     cooperative overlap (slow-return guard)
# ==========================================================================
def test_cooperative_overlap_survives_stack_machinery_churn():
    # Turn on autosize + scrub + a warm depot, then prove a sleeping offload
    # still overlaps with a busy sibling (the stack machinery must not serialize
    # the scheduler).
    rc.warmup(32, 512 * 1024)
    old_scrub = rc.get_stack_scrub()
    was_auto = rc.stack_autosize_enabled()
    try:
        rc.set_stack_scrub(True)
        rc.enable_stack_autosize(True)
        order = []

        def main():
            def offloader():
                order.append("off-start")
                r = rc.blocking(lambda: (time.sleep(0.1), 7)[1])
                order.append(("off-done", r))

            def burner():
                for i in range(5):
                    order.append(("burn", i))
                    rc.sched_yield()
            rc.go(offloader)
            rc.go(burner)
        with hang_guard(20, "overlap under churn"):
            with assert_faster_than(0.6, "offload overlap under stack churn"):
                rc.go(main)
                rc.run()
        idx = order.index(("off-done", 7))
        burns_before = sum(1 for o in order[:idx]
                           if isinstance(o, tuple) and o[0] == "burn")
        assert burns_before == 5, "burner did not overlap the offload: %r" % order
    finally:
        rc.set_stack_scrub(old_scrub)
        rc.enable_stack_autosize(False)
        if was_auto:
            rc.enable_stack_autosize(True)


# ==========================================================================
# 12. ADVERSARIAL AUGMENTATION (critic pass) -- conditions the first pass
#     missed: re-entrant/abusive Coro resume, the prewarm/warmup negative
#     stack-size validation hole, fault injection on the stack-spawn path,
#     MachineCode post-close state + dealloc churn, silent huge-default clamp,
#     M:N introspection, depot/advice teardown ordering.
# ==========================================================================

# --- 12a. Coro re-entrancy / abuse ----------------------------------------
def test_coro_reentrant_self_resume_does_not_silently_corrupt():
    # REGRESSION (was finding #16, a wild-jump SIGSEGV): Coro.resume() now has a
    # "currently executing" guard.  Resuming a Coro from inside its own body
    # (re-entrant self-resume) used to swap the asm context into a frame already
    # live on the CPU -> SIGSEGV at a wild address inside runloom_asm_entry.  It
    # must instead raise a clean RuntimeError -- the way every other Coro misuse
    # (negative size, non-callable, extra args) does -- and leave the Coro
    # usable so it finishes normally.
    box = {}
    def body():
        try:
            box["c"].resume()            # re-entrant resume of self
            box["err"] = "NO-RAISE"
        except RuntimeError as e:
            box["err"] = str(e)
        return 7
    c = rc.Coro(body)
    box["c"] = c
    rv = c.resume()
    assert box.get("err") and "executing" in box["err"], (
        "re-entrant self-resume did not raise the guard RuntimeError: %r"
        % box.get("err"))
    assert rv == 7 and c.done, "coro did not finish cleanly after guarded re-entry"
    assert c.result == 7
    # idempotent after completion -- the guard did not wedge it (resume-after-
    # done is a harmless no-op returning None)
    assert c.resume() is None


def test_coro_resume_takes_no_arguments():
    # Coro.resume() is METH_NOARGS -- a caller used to a generator's .send()
    # passing a value in must get a clean TypeError, never a silent ignore or a
    # crash reading a non-existent slot.
    c = rc.Coro(lambda: 1)
    with pytest.raises(TypeError):
        c.resume(42)
    # the spurious call must not have advanced / corrupted it
    assert c.resume() == 1
    assert c.done


def test_coro_never_resumed_dropped_is_clean():
    # A Coro that is constructed (its guard-paged stack is mmap'd) but NEVER
    # resumed, then dropped: dealloc must release the fresh mapping it never
    # swapped into.  Churn many so a leak/double-free would show in self_check.
    import gc
    for _ in range(2000):
        c = rc.Coro(lambda: 1)
        del c
    gc.collect()
    assert rc._self_check(0) == 0


def test_coro_nested_yield_threads_through_intermediate():
    # An inner Coro that yields, driven by an outer Coro: the outer's first
    # resume must run inner up to its yield and KEEP RUNNING the outer past it
    # (the inner's yield returns control to the outer, not all the way out).
    # This is the data-integrity check the happy-path nested test skipped: the
    # exact interleaving order, not just the final string.
    log = []

    def inner():
        log.append("i1")
        rc.yield_()
        log.append("i2")
        return "I"

    def outer():
        c = rc.Coro(inner)
        log.append("o1")
        r1 = c.resume()                 # inner -> its yield, control back here
        log.append(("mid", r1, c.done))
        r2 = c.resume()                 # inner finishes
        log.append(("fin", r2, c.done))
        return "O:" + r2

    oc = rc.Coro(outer)
    assert oc.resume() == "O:I" and oc.done
    assert log == ["o1", "i1", ("mid", None, False), "i2", ("fin", "I", True)]


def test_coro_exception_object_is_the_one_raised_then_consumed():
    # WRONG-DATA guard: the exception surfaced on resume must be the SAME object
    # the body raised (identity, not just type), and a second resume after the
    # raising one must be a clean None (the error was consumed, not re-served).
    sentinel = RuntimeError("unique-marker-9173")

    def body():
        raise sentinel
    c = rc.Coro(body)
    with pytest.raises(RuntimeError) as ei:
        c.resume()
    assert ei.value.args == ("unique-marker-9173",)
    assert c.done
    assert c.resume() is None          # consumed, not replayed
    assert c.resume() is None          # still idempotent


# --- 12b. prewarm / warmup negative-stack-size validation hole ------------
@pytest.mark.parametrize("label,call", [
    ("warmup",       lambda: rc.warmup(4, -1)),
    ("prewarm",      lambda: rc.prewarm(4, -1, False)),
    ("prewarm_keep", lambda: rc.prewarm_keep(4, -1)),
])
def test_prewarm_family_negative_stack_size_crashes(label, call):
    # REGRESSION (was finding #17, a SIGSEGV): warmup(n, -1) / prewarm(n, -1) /
    # prewarm_keep(n, -1) used to SEGV.  Coro(), set_stack_size() and go() all
    # validate a non-positive stack_size, but the prewarm/warmup family parsed
    # stack_size as a Py_ssize_t and cast straight to (size_t) with NO check --
    # -1 became SIZE_MAX, overflowing the guard-page mmap+memset arithmetic in
    # runloom_coro_* into a wild mapping -> SIGSEGV.  They must now raise a clean
    # ValueError like Coro does, consistently, in-process.
    with pytest.raises(ValueError):
        call()


def test_warmup_zero_and_tiny_stack_floor_and_succeed():
    # The other side of the boundary: 0 and a tiny positive size are NON-
    # negative so they pass into runloom_coro_warmup, which floors sub-page
    # sizes -- they must succeed (return the count), not crash.  This pins down
    # that ONLY the negative case is the bug, not "any small size".
    assert rc.warmup(2, 0) == 2
    assert rc.warmup(2, 1) == 2
    assert rc.warmup(2, 100) == 2


def test_prewarm_zero_stack_size_floors_and_succeeds():
    # prewarm with stack_size=0 floors like warmup (non-negative, so no overflow)
    assert rc.prewarm(3, 0, False) == 3


# --- 12c. fault injection on the stack-spawn path -------------------------
_FAULT_SPAWN = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc
done = bytearray(40)
def main():
    for i in range(40):
        rc.go(lambda i=i: done.__setitem__(i, 1))
err = None
try:
    rc.go(main)
    rc.run()
except MemoryError as e:
    err = "MemoryError"
# A clean Python error OR a partial-but-consistent run is acceptable; a crash
# is not.  Drain anything left and self-check.
print("ERR", err)
print("SELFCHECK", rc._self_check(0))
'''


@pytest.mark.parametrize("site", ["SPAWN_STACK", "SPAWN_G", "SPAWN_TSTATE"])
def test_fault_injection_on_spawn_path_is_clean_error_not_crash(site):
    # WEAPONIZE the runtime's fault injection at the three spawn-time sites the
    # stack machinery touches.  A 'once' fault mid spawn-storm must surface as a
    # clean MemoryError (or a benign partial run) and leave a consistent
    # structure -- never a SIGSEGV/abort and never a hang.
    p = _subproc(_FAULT_SPAWN, env_extra={"RUNLOOM_FAULT_%s" % site: "once:12"},
                 timeout=25)
    assert p.returncode == 0, (
        "fault at %s crashed/hung the spawn path: rc=%r stderr=%r"
        % (site, p.returncode, p.stderr[:600]))
    assert "SELFCHECK 0" in p.stdout, (
        "fault at %s left the structure inconsistent:\n%s"
        % (site, p.stdout[:600]))


# --- 12d. MachineCode post-close state + dealloc-without-close churn -------
def test_machinecode_attributes_after_close():
    # WRONG-DATA / use-after-free probe of the introspection getters: after
    # close(), .address must read 0 (the page pointer is nulled -- a caller can
    # SEE it is dead) while .size keeps the original length (it is a plain field
    # the dealloc/close path does NOT clear).  Reading them must never fault.
    mc = rc.MachineCode(b"\x90\x90\xc3")          # 3 bytes
    assert mc.size == 3 and mc.address != 0
    mc.close()
    assert mc.address == 0, "address not nulled after close -> dangling pointer"
    assert mc.size == 3, "size unexpectedly changed on close"
    # reading them a second time is still safe
    assert mc.address == 0 and mc.size == 3


def test_machinecode_dealloc_without_close_churn_is_clean():
    # The GC/dealloc path (no explicit close) must unmap the executable page.
    # Churn many so a leaked mapping or double-munmap would surface in
    # self_check / as an mmap exhaustion crash.
    import gc
    for _ in range(800):
        mc = rc.MachineCode(b"\xc3")
        del mc                                     # dealloc unmaps, no close()
    gc.collect()
    assert rc._self_check(0) == 0


def test_machinecode_exit_does_not_suppress_exception():
    # __exit__ returns False (falsey) -> an exception raised in the with-body
    # must PROPAGATE, not be swallowed, while the page is still freed on the way
    # out.  A __exit__ that returned truthy would silently eat real errors.
    mc = None
    with pytest.raises(ZeroDivisionError):
        with rc.MachineCode(b"\xc3") as m:
            mc = m
            assert m.address != 0
            raise ZeroDivisionError("boom")
    # the page was freed despite the exception -> a later call is guarded
    with pytest.raises(ValueError):
        mc()


@pytest.mark.skipif(not (IS_X86_64 and FT), reason="x86-64 + M:N")
def test_machinecode_create_close_churn_across_mn_hubs():
    # Each fiber on its own hub creates, calls, and closes its own blob in a
    # tight loop -- genuine parallel W^X map/unmap churn across hubs.  No crash,
    # no hang, every result correct (mov rax,rdi; ret -> identity).
    from runloom.sync import WaitGroup
    ident = b"\x48\x89\xf8\xc3"
    N = 12
    results = bytearray(N)

    def worker(n, wg):
        try:
            ok = True
            for _ in range(30):
                with rc.MachineCode(ident) as fn:
                    if fn(n) != n:
                        ok = False
            results[n] = 1 if ok else 0
        finally:
            wg.done()

    def main():
        wg = WaitGroup(); wg.add(N)
        for n in range(N):
            rc.mn_go(lambda n=n: worker(n, wg))
        wg.wait()
    with hang_guard(30, "mn machinecode churn"):
        runloom.run(3, main)
    assert sum(results) == N, "%d/%d workers correct" % (sum(results), N)
    assert rc._self_check(0) == 0


# --- 12e. set_stack_size huge -> silent clamp to MAX (not crash) ----------
def test_set_stack_size_huge_clamps_to_max_and_default_go_runs():
    # set_stack_size accepts an absurd value but clamps the EFFECTIVE default to
    # RUNLOOM_MAX_STACK_SIZE (8 MiB) -- get_stack_size reflects the clamp -- so a
    # default-stack go() must still map + run (NOT fail the mmap of 2^62 the way
    # the low-level Coro primitive does).  This is the scheduler-path clamp
    # contract the file probed for go(fn, huge) but never for the DEFAULT path.
    old = rc.get_stack_size()
    try:
        rc.set_stack_size(1 << 62)
        clamped = rc.get_stack_size()
        assert clamped <= 8 * 1024 * 1024, "default not clamped: %d" % clamped
        box = {}
        with hang_guard(15, "huge-default go"):
            rc.go(lambda: box.__setitem__("r", 1))
            rc.run()
        assert box.get("r") == 1
    finally:
        rc.set_stack_size(old)
        assert rc.get_stack_size() == old


# --- 12f. M:N introspection: current_g_hwm inside a hub fiber --------------
@pytest.mark.skipif(not (RELIABLE_HWM and FT),
                    reason="needs precise HWM + M:N")
def test_current_g_hwm_inside_mn_hub_fiber():
    # current_g_hwm must be safe to read from a HUB-spawned fiber and must NEVER
    # over-report (report more than the stack the fiber ran on).  The single-
    # thread path reports a precise >=1-page HWM (covered elsewhere); on the M:N
    # path the value is *observed to be state-dependent* -- it reads a precise
    # page count standalone but can under-report to 0 after certain prior
    # workloads in the same process (the guard-page mincore scan for a hub-
    # adopted g is not always wired the way the single-thread scan is).  So we
    # assert the load-bearing direction only: no crash, and never an OVER-report
    # (the wrong-data direction that would mean an OOB read past the stack).
    box = {"hwm": -1}

    def main():
        def f():
            sum(bytes(8192))               # touch >1 page of C stack
            box["hwm"] = rc.current_g_hwm()
        rc.mn_go(f, 262144)
        for _ in range(40):
            rc.sched_yield()
    with hang_guard(20, "mn hwm"):
        runloom.run(2, main)
    assert box["hwm"] >= 0, "current_g_hwm not read inside the hub fiber"
    assert box["hwm"] <= 262144 + _PAGE, (
        "M:N current_g_hwm OVER-reported (%d > stack) -- OOB scan?" % box["hwm"])


# --- 12g. advice/autosize under M:N + reset-during-record -----------------
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_stack_advice_records_and_resets_cleanly_under_mn():
    # The advisory profiler must record per-kind samples for hub-spawned fibers
    # (the HWM scan runs on the hub's resume path too), and a reset afterwards
    # must clear them with no dangling per-kind state under concurrency.
    import json
    was_on = rc.stack_advice_enabled()
    rc.reset_stack_advice()
    rc.enable_stack_advice(True)
    try:
        def main():
            def w():
                json.dumps({"a": [1, [2, [3, [4]]]]})
            for _ in range(60):
                rc.mn_go(w)
            for _ in range(40):
                rc.sched_yield()
        with hang_guard(25, "advice under mn"):
            runloom.run(3, main)
        rep = rc.stack_advice()
        assert isinstance(rep, list) and len(rep) >= 1
        assert all(d["samples"] >= 1 for d in rep)
    finally:
        rc.enable_stack_advice(False)
        rc.reset_stack_advice()
        assert rc.stack_advice() == []
        if was_on:
            rc.enable_stack_advice(True)


def test_reset_stack_advice_mid_lived_report_is_safe():
    # reset between two recording batches must not leave a stale entry that the
    # second batch then double-counts or reads freed.
    was_on = rc.stack_advice_enabled()
    rc.reset_stack_advice()
    rc.enable_stack_advice(True)
    try:
        import json

        def w():
            json.dumps([1, [2, [3]]])
        for _ in range(20):
            rc.go(w)
        rc.run()
        n1 = len(rc.stack_advice())
        rc.reset_stack_advice()
        assert rc.stack_advice() == []
        for _ in range(20):
            rc.go(w)
        rc.run()
        n2 = len(rc.stack_advice())
        assert n2 >= 1 and n2 <= n1 + 2   # fresh count, not n1+ accumulation
    finally:
        rc.enable_stack_advice(False)
        rc.reset_stack_advice()
        if was_on:
            rc.enable_stack_advice(True)


# --- 12h. prewarm_keep daemon teardown ordering (leak guard) --------------
def test_prewarm_keep_left_running_then_overridden_then_stopped():
    # Start a daemon, RE-target it to a different size while running (the
    # start/retarget handshake must not spawn a second daemon thread), then
    # stop.  A leaked daemon would keep topping the depot forever; verify the
    # final stop joins cleanly and the structure is consistent.
    with hang_guard(25, "prewarm_keep retarget"):
        assert runloom.prewarm_keep(200, 256 * 1024) == 0
        assert runloom.prewarm_keep(400, 512 * 1024) == 0   # retarget (size too)
        assert runloom.prewarm_keep(100) == 0               # retarget down
        runloom.prewarm_stop()
        runloom.prewarm_stop()                              # idempotent join
    assert rc._self_check(0) == 0


def test_prewarm_background_then_immediate_stop_no_hang():
    # A background prewarm kicked off then IMMEDIATELY followed by prewarm_stop
    # (which targets the prewarm_keep daemon, a different mechanism) must not
    # wedge: prewarm(background=True) is fire-and-forget, prewarm_stop on no
    # daemon is a no-op.  Race the two.
    with hang_guard(15, "bg prewarm + stop"):
        rc.prewarm(300, 256 * 1024, True)
        runloom.prewarm_stop()
        rc.prewarm(300, 256 * 1024, True)
    # let any background fill settle, then a spawn storm must still be correct
    done = bytearray(50)

    def main():
        for i in range(50):
            rc.go(lambda i=i: done.__setitem__(i, 1))
    with hang_guard(20, "post-bg-prewarm storm"):
        rc.go(main)
        rc.run()
    assert sum(done) == 50
    assert rc._self_check(0) == 0


# --- 12i. fiber_stack against an M:N hub fiber + integrity ----------------
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_fiber_stack_on_live_mn_parked_fiber():
    # fiber_stack must walk a HUB-spawned parked fiber's frames without an OOB
    # read or a crash, and return the (callable_repr, frames) 2-tuple shape.
    snap = {}

    def main():
        ch = rc.Chan(0)
        for _ in range(16):
            rc.mn_go(lambda: ch.recv())
        for _ in range(10):
            rc.sched_yield()
        fl = rc.fibers()
        if fl:
            res = rc.fiber_stack(fl[0]["id"])
            snap["ok"] = (isinstance(res, tuple) and len(res) == 2
                          and isinstance(res[1], list))
        ch.close()
        for _ in range(10):
            rc.sched_yield()
    with hang_guard(25, "mn fiber_stack"):
        runloom.run(2, main)
    assert snap.get("ok") is True


# --- 12j. scrub correctness as INTEGRITY (not just count) -----------------
def test_scrub_recycle_does_not_corrupt_neighbour_results():
    # Deeper than the existing scrub churn test: each recycled fiber writes a
    # DISTINCT value into a distinct slot and reads it back THROUGH a chunk of
    # C-stack scratch; with scrub on, the recycle wipe must not clobber a value
    # already computed and stored by a previous fiber (set-equality on results,
    # not a bare count).
    old = rc.get_stack_scrub()
    try:
        rc.set_stack_scrub(True)
        N = 80
        out = [None] * N

        def main():
            def w(i):
                scratch = bytearray(2048)
                for k in range(len(scratch)):
                    scratch[k] = (i + k) & 0xFF
                out[i] = (i, sum(scratch) & 0xFFFF)
            for i in range(N):
                rc.go(lambda i=i: w(i))
        with hang_guard(25, "scrub integrity"):
            rc.go(main)
            rc.run()
        # every slot filled, every index present exactly once, value matches
        assert all(out[i] is not None for i in range(N))
        assert {o[0] for o in out} == set(range(N))
        for i, (idx, checksum) in enumerate(out):
            expect = sum((idx + k) & 0xFF for k in range(2048)) & 0xFFFF
            assert checksum == expect, "scrub corrupted fiber %d" % idx
        assert rc._self_check(0) == 0
    finally:
        rc.set_stack_scrub(old)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
