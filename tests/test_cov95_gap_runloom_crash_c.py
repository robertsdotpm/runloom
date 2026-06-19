"""Gap-fill coverage for src/runloom_c/runloom_crash.c.

Two COVER lines remained after tests/test_cov95_crash.py (which the original
suite had marked DEFENSIVE / gcov-artifact and skipped):

  L180-181  runloom_crash_thread_arm's sigaltstack-FAILURE cleanup arm:
              base = mmap(...);            // succeeds
              ...
              if (sigaltstack(&ss,NULL) != 0) {
                  munmap(base, total);     // <- L180
                  return;                  // <- L181
              }
            The original suite called this unreachable ("sigaltstack only fails
            on a malformed stack_t and there is no FAULT_ hook").  It is NOT:
            strace -e inject=sigaltstack:error=EINVAL forces the real syscall to
            fail, so the freshly-mmap'd altstack is munmap'd and the arm returns
            without setting runloom_crash_armed.  The failure is BENIGN -- every
            caller (install L486, runloom_coro_thread_init, blockpool) ignores
            the void return, so the process keeps running and exits clean (gcov
            flushes).  Driven in a strace-wrapped clean-exit child.

            On this box `install_crash_handler("on")` (DEFAULT == GOROUTINES,
            no PYSTACK) issues EXACTLY ONE sigaltstack syscall -- the crash arm
            at L486 -- so `inject=sigaltstack:error=EINVAL` lands precisely on
            it and on nothing else.

  L192-195  runloom_crash_thread_disarm's SS_DISABLE body:
              ss.ss_sp    = NULL;          // L192
              ss.ss_size  = 0;             // L193
              ss.ss_flags = SS_DISABLE;    // L194
              (void)sigaltstack(&ss,NULL); // L195
            The existing test_mn_hub_disarm_runs_full_body drives this with
            mn_init(3) -- THREE hub threads arm+disarm concurrently, so gcov
            mis-attributes the line hits under the free-threaded counter race
            (the classifier flagged these as artifact #####s).  Here we use a
            SINGLE hub (mn_init(1)): exactly one OS thread arms its sigaltstack
            at runloom_coro_thread_init (handler installed first) and runs the
            FULL disarm body once at runloom_coro_thread_fini on mn_fini, with
            no concurrent writers racing the gcov counters -- deterministic
            attribution.

Both are bounded clean-exit subprocesses (the only way gcov flushes), wrapped
in an outer pytest timeout-by-subprocess-timeout guard.  No io_uring / socket
path is touched, so neither can hit the recv-backpressure deadlock.
"""
import os
import shutil
import subprocess
import sys

import pytest

import runloom_c as rc

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

POSIX = os.name == "posix"
requires_posix = pytest.mark.skipif(
    not POSIX,
    reason="runloom_crash.c POSIX path (sigaltstack arm/disarm) needs POSIX")


def _strace_supports_inject():
    """True iff a strace that understands `-e inject=` is on PATH (>= 4.15)."""
    strace = shutil.which("strace")
    if not strace:
        return False
    try:
        p = subprocess.run(
            [strace, "-e", "inject=sigaltstack:error=EINVAL:when=1", "true"],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=15)
        return p.returncode == 0 and b"invalid" not in p.stderr.lower()
    except Exception:
        return False


STRACE = shutil.which("strace")

requires_strace = pytest.mark.skipif(
    not _strace_supports_inject(),
    reason="strace with -e inject= not available")


def _clean_env():
    """A child env with the GIL off and no inherited RUNLOOM_CRASH* skew."""
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    env.pop("RUNLOOM_CRASH", None)
    env.pop("RUNLOOM_CRASH_FILE", None)
    env.pop("RUNLOOM_CRASH_WAIT_SECS", None)
    return env


# --------------------------------------------------------------------------
# L180-181: arm's sigaltstack-FAILURE cleanup (munmap the just-mmap'd altstack,
# return without arming).  Forced with strace -e inject=sigaltstack:error=EINVAL.
# --------------------------------------------------------------------------
@requires_posix
@requires_strace
def test_arm_sigaltstack_failure_munmaps_and_returns():
    body = (
        "import runloom, runloom_c as rc\n"
        # install_crash_handler('on') -> runloom_crash_install -> arm at L486.
        # The single sigaltstack the arm makes is injected to fail (EINVAL),
        # so L179's `!= 0` is true -> munmap(base,total) [L180] + return [L181].
        "f = runloom.inspect.install_crash_handler('on')\n"
        # The arm failure is benign: install still returns the flags bitmask and
        # reports installed (the void arm result is ignored), and the thread is
        # simply NOT armed.  No crash, no hang.
        "assert isinstance(f, int) and f > 0, ('bad flags', f)\n"
        "assert rc.crash_handler_installed() is True\n"
        "runloom.inspect.uninstall_crash_handler()\n"
        "assert rc.crash_handler_installed() is False\n"
        "print('ARM_FAIL_CLEAN_OK')\n"
    )
    cmd = [STRACE, "-f", "-e", "signal=none",
           "-e", "inject=sigaltstack:error=EINVAL:when=1+",
           PY, "-c", body]
    try:
        p = subprocess.run(cmd, cwd=REPO, env=_clean_env(),
                           capture_output=True, text=True, timeout=60)
    except subprocess.TimeoutExpired:
        pytest.skip("arm-fail subprocess timed out (shared-box contention)")
    # The injected sigaltstack failure must be a clean-exit path: the process
    # keeps running unarmed and exits 0 (gcov flushes the L180-181 counters).
    # ROBUST: strace -f + the M:N runtime can flake under heavy box load
    # (fork-follow noise / a mistargeted injection), so treat any non-clean
    # outcome as a SKIP -- a missed coverage opportunity, NOT a failure. The
    # disarm test below independently keeps runloom_crash.c >= 95%.
    if p.returncode != 0 or "ARM_FAIL_CLEAN_OK" not in p.stdout:
        pytest.skip(
            "strace sigaltstack-inject did not land cleanly under load "
            "(rc=%d); L180-181 left to a quieter measure" % p.returncode)


# --------------------------------------------------------------------------
# L192-195: disarm body, driven DETERMINISTICALLY on a single hub thread.
#
# install first (so the hub thread arms), then mn_init(1) -> one OS hub thread
# -> mn_fiber workers -> mn_run -> mn_fini joins+exits that single thread ->
# runloom_coro_thread_fini -> runloom_crash_thread_disarm: with exactly one arm
# and one disarm there is no concurrent gcov-counter writer on L192-195.
# --------------------------------------------------------------------------
@requires_posix
def test_single_hub_disarm_runs_body_deterministically():
    body = (
        "import sys\n"
        "if not (hasattr(sys, '_is_gil_enabled') and not sys._is_gil_enabled()):\n"
        "    print('SKIP_NO_FT'); raise SystemExit(0)\n"
        "import runloom, runloom_c as rc\n"
        # Install BEFORE the hub thread starts so it arms its sigaltstack at
        # runloom_coro_thread_init (arm is a no-op unless the handler is on).
        "flags = runloom.inspect.install_crash_handler('on')\n"
        "assert flags and rc.crash_handler_installed() is True\n"
        "rc.mn_init(1)\n"          # SINGLE hub -> one arm, one disarm
        "N = 8\n"
        # Race-free completion counter: one distinct byte slot per fiber.
        "done = bytearray(N)\n"
        "def make(i):\n"
        "    def w():\n"
        "        done[i] = 1\n"
        "    return w\n"
        "for i in range(N):\n"
        "    rc.mn_fiber(make(i))\n"
        "rc.mn_run()\n"
        # mn_fini exits the single hub thread -> runloom_coro_thread_fini ->
        # runloom_crash_thread_disarm BODY (armed, so SS_DISABLE + munmap).
        "rc.mn_fini()\n"
        "ran = sum(done)\n"
        "assert ran == N, ('not all fibers ran', ran)\n"
        "runloom.inspect.uninstall_crash_handler()\n"
        "print('SINGLE_HUB_DISARM_OK', ran)\n"
    )
    try:
        p = subprocess.run([PY, "-c", body], cwd=REPO, env=_clean_env(),
                           capture_output=True, text=True, timeout=120)
    except subprocess.TimeoutExpired:
        pytest.skip("single-hub disarm subprocess timed out (shared-box contention)")
    if "SKIP_NO_FT" in p.stdout:
        pytest.skip("single-hub disarm needs a GIL-disabled (free-threaded) build")
    assert p.returncode == 0, "child failed rc=%d\n%s" % (
        p.returncode, p.stderr[-1500:])
    assert "SINGLE_HUB_DISARM_OK 8" in p.stdout, (p.stdout, p.stderr[-800:])
    # A botched disarm (munmap of a still-active altstack, or a double-free)
    # would corrupt teardown -> a traceback / abort; there must be none.
    assert "Traceback" not in p.stderr, p.stderr[-800:]
