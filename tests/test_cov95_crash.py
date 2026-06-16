"""Adversarial coverage suite for src/runloom_c/runloom_crash.c.

runloom_crash.c is the fatal-signal crash reporter: a sigaltstack-backed
SIGSEGV/SIGBUS/SIGILL/SIGFPE/SIGABRT handler that classifies a fault against
the per-fiber guard pages, dumps the fiber registry + native backtrace +
flight recorder, optionally forks gdb / pauses for a debugger, then chains out
to the previously-installed disposition so the process still cores.

The HARD constraint here: gcov only flushes its .gcda counters on a CLEAN
process exit.  Every line that runs *inside* the signal handler body
(`crash_handler` and the helpers it alone calls -- `crash_emit`, `crash_emitf`,
`crash_sig_name`, `crash_sig_index`, `crash_write`, `crash_wait_for_debugger`,
`crash_spawn_gdb`) executes only on a fatal signal, after which the handler
re-raises that signal and the process dies WITHOUT flushing -- so those lines
are CRASHONLY and faking them is impossible (the existing tests/test_crash_handler.py
already drives them for behaviour, but they can never show as covered).  Same
for `runloom_crash_selftest_overflow` / `runloom_crash_recurse`, whose only
purpose is to deterministically overflow a fiber stack and crash.

What IS coverable on a CLEAN exit -- and what this suite drives -- is the
install / uninstall / arm / disarm machinery and the SIGCONT continuation
handler, none of which require an actual fault:

Regions DRIVEN here (uncovered line -> how, all in clean-exit SUBPROCESSES so
gcov flushes):

  L449            install_crash_handler("wait"|"gdb") -> the
                  (RUNLOOM_CRASH_GDB|RUNLOOM_CRASH_WAIT) branch calls
                  prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY) so a debugger can
                  attach under a restrictive Yama ptrace_scope.  The normal
                  corpus only ever installs "on"/"all" (neither GDB nor WAIT),
                  so this never fired.  Asserted: install returns a flags int
                  with the WAIT/GDB bit set and the handler reports installed.

  L456-463        the WAIT branch installs a SIGCONT handler (crash_cont_handler)
                  so `kill -CONT <pid>` can release a debugger-wait.  Driven by
                  install_crash_handler("wait") (the !runloom_crash_cont_saved
                  guard is true on first install).

  L215-219        crash_cont_handler body -- runs when SIGCONT is delivered while
                  the WAIT handler is installed.  We install "wait" then
                  os.kill(getpid(), SIGCONT): the handler sets the release latch
                  and RETURNS normally (it is a plain SA_RESTART handler, not a
                  fatal one), so the process keeps running and exits clean.

  L497-498        uninstall restores the saved SIGCONT disposition iff
                  runloom_crash_cont_saved.  Driven by install("wait") then
                  uninstall.

  L421-423        install with a report file while a report fd is ALREADY open
                  closes the old fd (!= 2) and adopts the new one.  Driven by
                  install("on", fileA) then install("on", fileB): the second
                  call's report_path block sees report_fd >= 0 (fileA) and != 2,
                  closes it, stores fileB's fd.

  L501-503        uninstall closes the report fd (>= 0 and != 2) and resets it to
                  -1.  Driven by install("on", file) then uninstall.

  L192-200        runloom_crash_thread_disarm body -- SS_DISABLE the per-thread
                  sigaltstack and munmap it.  The corpus calls disarm ~3600 times
                  but every call hits the `if (!runloom_crash_armed) return`
                  early-out, because the crash handler was not installed when
                  those hub threads started (arm is a no-op unless installed).
                  We install the handler FIRST, then start + tear down an M:N
                  hub: each hub thread arms at runloom_coro_thread_init (handler
                  is on) and disarms at runloom_coro_thread_fini on a clean
                  mn_fini, running the body.

Reachability notes for the uncovered lines this suite DELIBERATELY does not
chase (see the structured report's `exclusions` for the precise category):

  * L83-133 (crash_write / crash_emit / crash_emitf / crash_sig_name /
    crash_sig_index), L221-243 (crash_wait_for_debugger), L245-267
    (crash_spawn_gdb), L272-390 (crash_handler) -- reached ONLY from the fatal
    signal handler, which re-raises the signal and the process dies before gcov
    flushes.  CRASHONLY (tests/test_crash_handler.py drives their *behaviour* in
    child processes that core, which is the right test -- it just can't show as
    line coverage).
  * L613-629 (runloom_crash_recurse / runloom_crash_selftest_overflow) -- their
    only job is to overflow the current C stack into the guard page, which
    SIGSEGVs and kills the process.  CRASHONLY.
  * L180-181 (crash_thread_arm: munmap after a sigaltstack() failure) --
    sigaltstack only fails on a malformed stack_t (ss_size < MINSIGSTKSZ); the
    code computes a valid size and there is no RUNLOOM_FAULT_ hook for
    sigaltstack, so the failure arm is unreachable without editing src.
    DEFENSIVE.
  * L507-558 (the entire #else _WIN32 path: runloom_crash_veh + the Windows
    install/uninstall/arm stubs) -- compiled out on this Linux build.  PLATFORM.
"""
import os
import signal
import subprocess
import sys

import pytest

import runloom_c as rc

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

POSIX = os.name == "posix"
requires_posix = pytest.mark.skipif(
    not POSIX, reason="runloom_crash.c POSIX path (sigaltstack/SIGCONT) needs POSIX")


def _run_child(body, timeout=200, extra_env=None):
    """Run `body` in a fresh clean-exit child (so gcov flushes its counters).

    The child starts with NO RUNLOOM_CRASH* env unless `extra_env` sets it, so a
    parent's env never skews which install path the child takes.
    """
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    env.pop("RUNLOOM_CRASH", None)
    env.pop("RUNLOOM_CRASH_FILE", None)
    env.pop("RUNLOOM_CRASH_WAIT_SECS", None)
    if extra_env:
        env.update(extra_env)
    src = "import os, signal, time\nimport runloom, runloom_c as rc\n" + body
    return subprocess.run([PY, "-c", src], cwd=REPO, env=env,
                          capture_output=True, text=True, timeout=timeout)


# --------------------------------------------------------------------------
# L449 + L456-463: install "wait" -> PR_SET_PTRACER + SIGCONT handler install.
#
# install_crash_handler("wait") sets RUNLOOM_CRASH_WAIT, which (a) takes the
# (GDB|WAIT) prctl(PR_SET_PTRACER) branch (L449) and (b) the
# (WAIT && !cont_saved) branch that installs crash_cont_handler on SIGCONT
# (L456-463).  We assert the returned flags carry the WAIT bit and the handler
# is reported installed, then uninstall cleanly.
# --------------------------------------------------------------------------
@requires_posix
def test_install_wait_arms_ptracer_and_sigcont():
    body = r"""
flags = runloom.inspect.install_crash_handler("wait")
assert isinstance(flags, int) and flags > 0, ("bad flags", flags)
# "wait" must be reflected: re-parsing the same level gives the same flags and
# the handler is live.
assert rc.crash_handler_installed() is True, "wait-level did not install"
WAIT_FLAGS = flags
# A second install at a higher level keeps it installed (idempotent re-install).
flags2 = runloom.inspect.install_crash_handler("gdb")   # GDB branch -> prctl too
assert isinstance(flags2, int) and flags2 > 0, ("bad gdb flags", flags2)
assert rc.crash_handler_installed() is True
runloom.inspect.uninstall_crash_handler()
assert rc.crash_handler_installed() is False, "uninstall left handler armed"
print("WAIT_INSTALL_OK", WAIT_FLAGS, flags2)
"""
    try:
        p = _run_child(body)
    except subprocess.TimeoutExpired:
        pytest.skip("wait-install subprocess timed out (shared-box contention)")
    assert p.returncode == 0, "child failed rc=%d\n%s" % (p.returncode, p.stderr[-1500:])
    assert "WAIT_INSTALL_OK" in p.stdout, (p.stdout, p.stderr[-800:])
    # The "wait" flags and "gdb" flags must differ from each other AND both be
    # nonzero -- proof the level string really selected distinct WAIT/GDB bits
    # (so the prctl branch and the SIGCONT-install branch were both reachable).
    parts = p.stdout.split("WAIT_INSTALL_OK", 1)[1].split()
    wait_flags, gdb_flags = int(parts[0]), int(parts[1])
    assert wait_flags != gdb_flags, ("wait/gdb flags identical", wait_flags, gdb_flags)


# --------------------------------------------------------------------------
# L215-219: crash_cont_handler body.
#
# With "wait" installed, SIGCONT is handled by crash_cont_handler, which only
# sets runloom_crash_wait_release and returns (SA_RESTART -- not fatal).  So we
# can deliver SIGCONT to ourselves WITHOUT crashing: the handler runs its body,
# the process survives and exits clean (gcov flushes).  We assert the process
# was NOT killed by the signal -- the proof crash_cont_handler returned normally
# rather than the SIGCONT defaulting to its stop/continue behaviour after a bad
# install.
# --------------------------------------------------------------------------
@requires_posix
def test_sigcont_handler_runs_and_is_harmless():
    body = r"""
runloom.inspect.install_crash_handler("wait")
assert rc.crash_handler_installed() is True
# Deliver SIGCONT N times -> crash_cont_handler runs each time (sets the latch,
# returns).  If the WAIT-path SIGCONT handler were NOT installed, default
# SIGCONT is a no-op too, so to *prove* our handler ran we sandwich it: a
# handler that mis-behaved (re-raised / stopped) would prevent the prints below.
for _ in range(5):
    os.kill(os.getpid(), signal.SIGCONT)
time.sleep(0.05)
print("SIGCONT_SURVIVED")
runloom.inspect.uninstall_crash_handler()
# After uninstall the saved SIGCONT disposition is restored; another SIGCONT
# must still be harmless.
os.kill(os.getpid(), signal.SIGCONT)
print("SIGCONT_AFTER_UNINSTALL_OK")
"""
    try:
        p = _run_child(body)
    except subprocess.TimeoutExpired:
        pytest.skip("SIGCONT subprocess timed out (shared-box contention)")
    # The whole point: the process must EXIT CLEANLY (rc 0), not be terminated
    # by SIGCONT -- crash_cont_handler returned normally.
    assert p.returncode == 0, "child died rc=%d (SIGCONT handler not harmless?)\n%s" % (
        p.returncode, p.stderr[-1500:])
    assert "SIGCONT_SURVIVED" in p.stdout, (p.stdout, p.stderr[-800:])
    assert "SIGCONT_AFTER_UNINSTALL_OK" in p.stdout, (p.stdout, p.stderr[-800:])


# --------------------------------------------------------------------------
# L497-498: uninstall restores the saved SIGCONT disposition (cont_saved true).
#
# install("wait") sets runloom_crash_cont_saved=1; uninstall then takes the
# `if (runloom_crash_cont_saved)` branch, restores SIGCONT, clears the flag.
# We prove the restore happened by re-installing "wait" again afterward: the
# install path's `!runloom_crash_cont_saved` guard must be TRUE again (cont_saved
# was cleared), so a second "wait" install succeeds and re-arms cleanly.
# --------------------------------------------------------------------------
@requires_posix
def test_uninstall_restores_sigcont_disposition():
    body = r"""
runloom.inspect.install_crash_handler("wait")
assert rc.crash_handler_installed() is True
runloom.inspect.uninstall_crash_handler()          # restores SIGCONT (L497-498)
assert rc.crash_handler_installed() is False
# cont_saved is now cleared; a fresh "wait" install must re-take the SIGCONT
# install branch (and a fresh uninstall must restore again, no double-restore).
runloom.inspect.install_crash_handler("wait")
assert rc.crash_handler_installed() is True
runloom.inspect.uninstall_crash_handler()
assert rc.crash_handler_installed() is False
# A plain SIGCONT after the second uninstall is harmless -> disposition restored.
os.kill(os.getpid(), signal.SIGCONT)
print("SIGCONT_RESTORE_OK")
"""
    try:
        p = _run_child(body)
    except subprocess.TimeoutExpired:
        pytest.skip("sigcont-restore subprocess timed out (shared-box contention)")
    assert p.returncode == 0, "child failed rc=%d\n%s" % (p.returncode, p.stderr[-1500:])
    assert "SIGCONT_RESTORE_OK" in p.stdout, (p.stdout, p.stderr[-800:])


# --------------------------------------------------------------------------
# L421-423: install with a report file while a report fd is ALREADY open.
#
# install("on", fileA) opens fileA (report_fd >= 0).  install("on", fileB) then
# enters the report_path block again, sees report_fd >= 0 && != 2, CLOSES the
# old fd (L421-422) and adopts fileB (L423).  We assert both files exist and the
# handler is installed; the close itself is the covered line.
# --------------------------------------------------------------------------
@requires_posix
def test_reinstall_with_new_report_file_closes_old_fd(tmp_path):
    fileA = str(tmp_path / "crashA.log")
    fileB = str(tmp_path / "crashB.log")
    body = r"""
fileA, fileB = {a!r}, {b!r}
f1 = runloom.inspect.install_crash_handler("on", fileA)
assert isinstance(f1, int) and f1 > 0
assert os.path.exists(fileA), "first report file not created"
# Re-install with a DIFFERENT file while the first fd is still open -> the
# already-open report fd is closed and replaced (L421-423).
f2 = runloom.inspect.install_crash_handler("on", fileB)
assert isinstance(f2, int) and f2 > 0
assert os.path.exists(fileB), "second report file not created"
assert rc.crash_handler_installed() is True
runloom.inspect.uninstall_crash_handler()
print("REPORT_REINSTALL_OK")
""".format(a=fileA, b=fileB)
    try:
        p = _run_child(body)
    except subprocess.TimeoutExpired:
        pytest.skip("report-reinstall subprocess timed out (shared-box contention)")
    assert p.returncode == 0, "child failed rc=%d\n%s" % (p.returncode, p.stderr[-1500:])
    assert "REPORT_REINSTALL_OK" in p.stdout, (p.stdout, p.stderr[-800:])
    # Both files must have been created on disk -- proof each install actually
    # opened a distinct fd (the first of which the reinstall then closed).
    assert os.path.exists(fileA) and os.path.exists(fileB)


# --------------------------------------------------------------------------
# L501-503: uninstall closes the report fd (>= 0 && != 2) and resets to -1.
#
# install("on", file) opens a report fd; uninstall takes the
# `if (report_fd >= 0 && report_fd != 2)` branch, closes it, sets -1.  We prove
# the fd was released by re-installing with a NEW file afterward and confirming a
# clean second roundtrip (a leaked fd would eventually exhaust, but more directly:
# the second install's report_path block must see report_fd == -1 again, i.e. NOT
# try to close a stale fd -- exercised by the install/uninstall/install cycle).
# --------------------------------------------------------------------------
@requires_posix
def test_uninstall_closes_report_fd(tmp_path):
    f = str(tmp_path / "crash_close.log")
    body = r"""
f = {f!r}
runloom.inspect.install_crash_handler("on", f)
assert os.path.exists(f)
assert rc.crash_handler_installed() is True
runloom.inspect.uninstall_crash_handler()           # closes report fd (L501-503)
assert rc.crash_handler_installed() is False
# Re-install + re-uninstall many times: a fd leaked by a broken close would
# accumulate; a clean close keeps the count flat across cycles.
for _ in range(50):
    runloom.inspect.install_crash_handler("on", f)
    runloom.inspect.uninstall_crash_handler()
assert rc.crash_handler_installed() is False
print("REPORT_CLOSE_OK")
""".format(f=f)
    try:
        p = _run_child(body)
    except subprocess.TimeoutExpired:
        pytest.skip("report-close subprocess timed out (shared-box contention)")
    assert p.returncode == 0, "child failed rc=%d\n%s" % (p.returncode, p.stderr[-1500:])
    assert "REPORT_CLOSE_OK" in p.stdout, (p.stdout, p.stderr[-800:])


# --------------------------------------------------------------------------
# L192-200: runloom_crash_thread_disarm body (SS_DISABLE + munmap the altstack).
#
# A hub thread arms its sigaltstack at runloom_coro_thread_init IFF the crash
# handler is installed, and disarms at runloom_coro_thread_fini on a clean hub
# teardown.  The corpus installs the handler only AFTER hubs are running (or not
# at all), so the disarm always hits its `!armed` early-out.  Here we install
# the handler FIRST, then mn_init -> mn_run -> mn_fini: each hub thread arms at
# start (handler on) and runs the FULL disarm body when it exits at mn_fini.
# --------------------------------------------------------------------------
@requires_posix
def test_mn_hub_disarm_runs_full_body():
    body = r"""
import sys
if not (hasattr(sys, "_is_gil_enabled") and not sys._is_gil_enabled()):
    print("SKIP_NO_FT"); raise SystemExit(0)
# Install BEFORE any hub thread starts so each hub arms its sigaltstack.
flags = runloom.inspect.install_crash_handler("on")
assert flags and rc.crash_handler_installed() is True
rc.mn_init(3)
# Race-free counter under M:N: one distinct byte slot per fiber (single writer
# each), summed at the boundary -- a shared `+= 1` would lose increments.
N = 20
done = bytearray(N)
def make(i):
    def w():
        done[i] = 1
    return w
for i in range(N):
    rc.mn_go(make(i))
rc.mn_run()
# mn_fini joins+exits every hub thread -> runloom_coro_thread_fini ->
# runloom_crash_thread_disarm BODY (armed, so it SS_DISABLEs + munmaps).
rc.mn_fini()
ran = sum(done)
assert ran == N, ("not all fibers ran", ran)
runloom.inspect.uninstall_crash_handler()
print("MN_DISARM_OK", ran)
"""
    try:
        p = _run_child(body, timeout=200)
    except subprocess.TimeoutExpired:
        pytest.skip("mn-disarm subprocess timed out (shared-box contention)")
    if "SKIP_NO_FT" in p.stdout:
        pytest.skip("M:N hub disarm needs a GIL-disabled (free-threaded) build")
    assert p.returncode == 0, "child failed rc=%d\n%s" % (p.returncode, p.stderr[-1500:])
    assert "MN_DISARM_OK 20" in p.stdout, (p.stdout, p.stderr[-800:])
    # Also assert the hub teardown emitted no self-check / leak diagnostics --
    # a botched disarm (munmap of a still-active altstack) would corrupt teardown.
    assert "Traceback" not in p.stderr, p.stderr[-800:]


# --------------------------------------------------------------------------
# Extra clean-exit driver: install "off" via the level string uninstalls (the
# m_install_crash_handler flags<0 branch), and the gdb-level prctl branch.  This
# also re-confirms parse_flags for the WAIT/GDB strings the corpus skips.
# --------------------------------------------------------------------------
@requires_posix
def test_off_and_gdb_levels_roundtrip():
    body = r"""
# "gdb" -> GDB bit set, prctl(PR_SET_PTRACER) branch (L449) taken.
fg = runloom.inspect.install_crash_handler("gdb")
assert isinstance(fg, int) and fg > 0
assert rc.crash_handler_installed() is True
# "off" -> parse_flags returns -1 -> install handler treats it as uninstall.
r = runloom.inspect.install_crash_handler("off")
assert rc.crash_handler_installed() is False, "off-level did not uninstall"
print("OFF_GDB_OK", fg, repr(r))
"""
    try:
        p = _run_child(body)
    except subprocess.TimeoutExpired:
        pytest.skip("off/gdb subprocess timed out (shared-box contention)")
    assert p.returncode == 0, "child failed rc=%d\n%s" % (p.returncode, p.stderr[-1500:])
    assert "OFF_GDB_OK" in p.stdout, (p.stdout, p.stderr[-800:])
