"""Subprocess spawn helper for the big_100 subprocess projects.

WHY THIS EXISTS (see FINDINGS.md BUG #4): constructing a `subprocess.Popen`
with pipes *from inside a goroutine* makes `Popen.__init__` call
`io.open(pipe_fd, 'rb')`, which monkey routes to pure-Python `_pyio`, whose
`FileIO` does an **offloaded** `os.fstat`.  At high concurrent spawn rates the
offload-result wait (a cooperative Condition) intermittently loses its wakeup
and the goroutine hangs forever in `Popen.__init__`.

Constructing the Popen off-goroutine via `runloom.blocking` runs that fstat on
a pool thread (where `_in_goroutine()` is False, so no nested offload) and
sidesteps the deadlock.  The returned Popen's `communicate()` / `wait()` are
still used cooperatively from the goroutine.

SPAWN RATE LIMIT: glibc posix_spawn with a very large FD table (100k
goroutines each holding pipe FDs) crashes with many simultaneous callers on
3.13t.  The semaphore keeps concurrent Popen() calls to at most MAX_CONCURRENT
at any time.  threading.Semaphore is monkey-patched to a cooperative goroutine
semaphore when running inside runloom, so goroutines park rather than OS-block.

SHUTDOWN-AWARE SEMAPHORE: pass running=H.running so that goroutines queued
behind the semaphore abort immediately when the harness stops instead of each
running one more subprocess first.  Without this, drain time at 100k goroutines
is O(funcs / MAX_CONCURRENT * spawn_time) — hundreds of seconds.  With it,
drain is O(in-flight * spawn_time) — a few seconds.

CANCEL WATCHER: a single background goroutine polls running() every 50ms.
When running() goes False it calls sem.cancel_all(), which unparks ALL waiters
without giving them permits — acquire() returns False and each goroutine raises
OSError.  This avoids spawning a per-goroutine waker, which caused scheduler
starvation at 100k goroutines (100k waker goroutines overwhelmed the submission
deque, starving the timer heap so initial sleepers never ran).
"""
import os
import shlex
import subprocess
import sys
import threading

_WIN = (os.name == "nt")

# ---- cross-platform child-process argv -------------------------------------
# The big_100 subprocess projects were written against Unix coreutils
# (cat / sleep / true / sh); Windows ships none of them, so spawning the literal
# name fails with WinError 2 ("the system cannot find the file specified").
# Route through the running interpreter on Windows -- it is guaranteed present
# and gives identical OBSERVABLE behaviour (stdin->stdout copy, timed sleep,
# chosen exit code).  On Unix keep the original coreutils so the mac/Linux runs
# stay byte-for-byte unchanged (and cheap: a python child costs ~100x a
# coreutil spawn, which matters only at the over-scale end of the sweep).

# `cat`: copy stdin to stdout, byte for byte.
CAT = ([sys.executable, "-c",
        "import sys,shutil;"
        "shutil.copyfileobj(sys.stdin.buffer,sys.stdout.buffer)"]
       if _WIN else ["cat"])

# `true`: produce no output, exit 0.
TRUE = ([sys.executable, "-c", ""] if _WIN else ["true"])


def sleep_cmd(seconds):
    """argv for a child that sleeps `seconds` then exits 0 (Unix `sleep`)."""
    if _WIN:
        return [sys.executable, "-c",
                "import time;time.sleep({0})".format(float(seconds))]
    return ["sleep", str(seconds)]


def exit_cmd(code):
    """argv for a child that immediately exits with `code` (Unix `sh -c exit`)."""
    code = int(code)
    if _WIN:
        return [sys.executable, "-c", "import sys;sys.exit({0})".format(code)]
    return ["sh", "-c", "exit {0}".format(code)]


def print_exit_cmd(text, code=0):
    """argv for a child that writes `text` to stdout (no trailing newline) then
    exits with `code` (Unix `sh -c 'printf %s ...; exit N'`)."""
    code = int(code)
    if _WIN:
        return [sys.executable, "-c",
                "import sys;sys.stdout.write({0!r});"
                "sys.exit({1})".format(text, code)]
    return ["sh", "-c", "printf %s {0}; exit {1}".format(shlex.quote(text), code)]


def abort_cmd():
    """(argv, expected_returncode) for a child that terminates ABNORMALLY.

    Unix dies from SIGABRT, which subprocess reports as returncode == -SIGABRT.
    Windows has no POSIX signals: model the abnormal termination as the CRT
    abort exit code 3 (a distinguished nonzero status the parent can classify),
    since a real os.abort() there can pop a Windows Error Reporting dialog and
    wedge an unattended run."""
    if _WIN:
        return ([sys.executable, "-c", "import sys;sys.exit(3)"], 3)
    import signal
    return (["sh", "-c", "kill -ABRT $$"], -signal.SIGABRT)


MAX_CONCURRENT = 32
# Lazy-initialized after monkey.patch() so it becomes a CoSemaphore (cooperative,
# non-hub-blocking) rather than a real threading.Semaphore (whose _cond.wait()
# blocks the hub OS thread, freezing all goroutines on that hub).
_spawn_sem = None
_cancel_started = [False]


def popen(*args, running=None, **kwargs):
    """Construct a Popen off-goroutine (via runloom.blocking) to avoid nested
    offload deadlocks.  Pass running=H.running for fast shutdown: a single
    cancel-watcher goroutine polls running() every 50ms and calls
    sem.cancel_all() when it goes False, waking all waiting goroutines which
    then raise OSError("cancelled") instead of spawning another subprocess.
    """
    import runloom
    global _spawn_sem, _cancel_started
    sem = _spawn_sem
    if sem is None:
        # First call from inside a goroutine (after monkey.patch()); at this
        # point threading.Semaphore == CoSemaphore, so the created semaphore is
        # cooperative.  A benign double-init race can create two semaphores; the
        # loser is GC'd and the resulting brief >32 concurrency ceiling is safe.
        sem = threading.Semaphore(MAX_CONCURRENT)
        _spawn_sem = sem
    if running is not None and not _cancel_started[0]:
        _cancel_started[0] = True
        # ONE watcher goroutine instead of one waker per acquire() call.
        # 50ms polls = 20 goroutine dispatches/s = negligible overhead.
        # When running() goes False, cancel_all() unparks ALL waiters within 50ms.
        #
        # IMPORTANT: read _spawn_sem at cancel time, NOT via the local `sem`
        # captured at watcher creation time.  Multiple goroutines can
        # simultaneously reach popen() when _spawn_sem is None, each creating a
        # different CoSemaphore; the last writer wins the global.  If the watcher
        # captured s=sem (its own creation-time local), it would call cancel_all()
        # on an ABANDONED semaphore whose waiters list is empty, leaving the 98k
        # goroutines parked in the canonical _spawn_sem permanently stuck.
        def _cancel_watcher(r=running):
            while r():
                runloom.sleep(0.05)
            s = _spawn_sem  # read the canonical global at cancel time
            if s is not None:
                s.cancel_all()
        runloom.fiber(_cancel_watcher)
    if running is not None:
        # Infinite park — cancel_all() wakes us if running() goes False.
        if not sem.acquire():
            raise OSError("popen cancelled: harness stopping")
    else:
        sem.acquire()
    try:
        return runloom.blocking(subprocess.Popen, *args, **kwargs)
    finally:
        sem.release()
