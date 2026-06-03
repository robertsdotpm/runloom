"""Tests for the fatal-signal crash reporter (runloom_crash.c / inspect.install_crash_handler).

A real crash kills the process, so every crash-trigger test runs in a child
process and asserts on its exit signal and captured stderr.  The classification
(goroutine stack overflow vs wild pointer vs non-goroutine) and the per-thread
sigaltstack (so the handler survives the overflow it is reporting) are the
behaviours under test.
"""
import os
import signal
import subprocess
import sys
import textwrap

import pytest

import runloom            # noqa: F401  (import side effects: registers fork handler)
import runloom_c

POSIX = os.name == "posix"
BACKEND = runloom_c.backend()
# The address->goroutine guard-page mapping only exists on the POSIX stack
# backends; Windows Fibers have no introspectable stack / guard page.
HAS_GUARD = BACKEND in ("fcontext-asm", "ucontext")

requires_guard = pytest.mark.skipif(
    not (POSIX and HAS_GUARD),
    reason="crash classification needs a POSIX guard-page backend (got %s)" % BACKEND,
)


def run_child(body, extra_env=None, timeout=60):
    """Run `body` as a fresh child Python process; return (returncode, output).

    The child inherits this run's interpreter + PYTHONPATH (so it imports the
    same source tree) but starts with no RUNLOOM_CRASH* env unless the test
    sets it explicitly.
    """
    src = "import runloom, runloom_c, ctypes, sys\n" + textwrap.dedent(body)
    env = dict(os.environ)
    env.pop("RUNLOOM_CRASH", None)
    env.pop("RUNLOOM_CRASH_FILE", None)
    if extra_env:
        env.update(extra_env)
    p = subprocess.run(
        [sys.executable, "-c", src],
        capture_output=True, text=True, env=env, timeout=timeout,
    )
    return p.returncode, p.stdout + p.stderr


# --------------------------------------------------------------------------- #
#  In-process API
# --------------------------------------------------------------------------- #
def test_install_uninstall_roundtrip():
    assert runloom_c.crash_handler_installed() is False
    try:
        flags = runloom.inspect.install_crash_handler("on")
        assert isinstance(flags, int) and flags > 0
        assert runloom_c.crash_handler_installed() is True
    finally:
        runloom.inspect.uninstall_crash_handler()
    assert runloom_c.crash_handler_installed() is False


def test_install_idempotent():
    try:
        runloom.inspect.install_crash_handler("on")
        runloom.inspect.install_crash_handler("on")   # no error, still installed
        assert runloom_c.crash_handler_installed() is True
    finally:
        runloom.inspect.uninstall_crash_handler()


def test_off_level_uninstalls():
    try:
        runloom.inspect.install_crash_handler("on")
        assert runloom_c.crash_handler_installed() is True
        runloom.inspect.install_crash_handler("off")
        assert runloom_c.crash_handler_installed() is False
    finally:
        runloom.inspect.uninstall_crash_handler()


@pytest.mark.parametrize("level", ["on", "all", "backtrace", "pystack", "wait", "gdb",
                                   "backtrace,pystack"])
def test_level_strings_parse(level):
    try:
        flags = runloom.inspect.install_crash_handler(level)
        assert isinstance(flags, int) and flags > 0
        assert runloom_c.crash_handler_installed() is True
    finally:
        runloom.inspect.uninstall_crash_handler()


# --------------------------------------------------------------------------- #
#  Does not interfere with a normal (non-crashing) run
# --------------------------------------------------------------------------- #
def test_no_interference_on_clean_run():
    rc, out = run_child("""
        runloom.inspect.install_crash_handler("all")
        results = []
        def work():
            results.append(42)
        runloom_c.go(work)
        runloom_c.run()
        print("CLEAN-EXIT", results)
    """)
    assert rc == 0, out
    assert "CLEAN-EXIT [42]" in out
    assert "runloom crash" not in out


# --------------------------------------------------------------------------- #
#  Goroutine stack overflow -> classified, named, survived
# --------------------------------------------------------------------------- #
@requires_guard
def test_overflow_classified_single_thread():
    rc, out = run_child("""
        runloom.inspect.install_crash_handler("on")
        def boom():
            runloom_c._crash_selftest_overflow()   # unbounded real-C recursion
        runloom_c.go(boom, 16384)                  # small 16 KiB stack
        runloom_c.run()
    """)
    assert rc == -signal.SIGSEGV, (rc, out)          # chained to default -> cored
    assert "GOROUTINE STACK OVERFLOW" in out, out
    assert "16 KiB" in out, out                       # named its stack size
    assert "goroutine g" in out, out
    assert "=== runloom goroutine dump" in out, out   # full registry dump too


@requires_guard
def test_overflow_classified_under_mn_scheduler():
    # The fault fires on a HUB thread; this proves the per-thread sigaltstack was
    # armed via runloom_coro_thread_init at hub start.
    rc, out = run_child("""
        runloom.inspect.install_crash_handler("on")
        runloom_c.mn_init(2)
        def boom():
            runloom_c._crash_selftest_overflow()
        runloom_c.mn_go(boom)
        runloom_c.mn_run()
    """)
    assert rc == -signal.SIGSEGV, (rc, out)
    assert "GOROUTINE STACK OVERFLOW" in out, out
    assert "this thread was executing goroutine g" in out, out


# --------------------------------------------------------------------------- #
#  Wild pointer (NULL deref) -> NOT classified as overflow
# --------------------------------------------------------------------------- #
@requires_guard
def test_wild_pointer_not_classified_as_overflow():
    rc, out = run_child("""
        runloom.inspect.install_crash_handler("on")
        def boom():
            ctypes.string_at(0)        # read address 0 -- not a guard page
        runloom_c.go(boom)
        runloom_c.run()
    """)
    assert rc == -signal.SIGSEGV, (rc, out)
    assert "not in any goroutine stack" in out, out
    assert "GOROUTINE STACK OVERFLOW" not in out, out
    assert "=== runloom goroutine dump" in out, out


# --------------------------------------------------------------------------- #
#  Python traceback chains in (faulthandler) under pystack
# --------------------------------------------------------------------------- #
@requires_guard
def test_pystack_chains_python_traceback():
    rc, out = run_child("""
        runloom.inspect.install_crash_handler("all")   # all => +pystack
        def boom():
            ctypes.string_at(0)
        runloom_c.go(boom)
        runloom_c.run()
    """)
    assert rc == -signal.SIGSEGV, (rc, out)
    assert "runloom crash" in out, out                 # our dump ran first
    # ... then faulthandler printed the Python traceback and re-raised default.
    assert "Fatal Python error" in out, out
    assert "in boom" in out, out


# --------------------------------------------------------------------------- #
#  Report file (RUNLOOM_CRASH_FILE / file=)
# --------------------------------------------------------------------------- #
@requires_guard
def test_report_written_to_file(tmp_path):
    report = tmp_path / "crash.txt"
    rc, out = run_child("""
        runloom.inspect.install_crash_handler("on", %r)
        def boom():
            runloom_c._crash_selftest_overflow()
        runloom_c.go(boom, 16384)
        runloom_c.run()
    """ % str(report))
    assert rc == -signal.SIGSEGV, (rc, out)
    assert report.exists(), "report file not created"
    text = report.read_text()
    assert "runloom crash" in text, text
    assert "GOROUTINE STACK OVERFLOW" in text, text


# --------------------------------------------------------------------------- #
#  Env auto-install at import (RUNLOOM_CRASH=...)
# --------------------------------------------------------------------------- #
def test_env_autoinstall():
    rc, out = run_child("""
        # No explicit install -- the env var should have armed it at import.
        print("INSTALLED", runloom_c.crash_handler_installed())
    """, extra_env={"RUNLOOM_CRASH": "on"})
    assert rc == 0, out
    assert "INSTALLED True" in out, out


def test_env_off_does_not_install():
    rc, out = run_child("""
        print("INSTALLED", runloom_c.crash_handler_installed())
    """, extra_env={"RUNLOOM_CRASH": "off"})
    assert rc == 0, out
    assert "INSTALLED False" in out, out


@requires_guard
def test_env_autoinstall_actually_catches_crash():
    rc, out = run_child("""
        def boom():
            runloom_c._crash_selftest_overflow()
        runloom_c.go(boom, 16384)
        runloom_c.run()
    """, extra_env={"RUNLOOM_CRASH": "on"})
    assert rc == -signal.SIGSEGV, (rc, out)
    assert "GOROUTINE STACK OVERFLOW" in out, out
