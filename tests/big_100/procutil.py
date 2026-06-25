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


# ---- Windows: kill spawned children when THIS process dies -----------------
# On Unix an orphaned child is reparented to init, but the big_100 subprocess
# programs spawn short-lived children that self-exit and the loop driver kills
# whole process groups, so Unix needs no in-process reaper here (and procutil
# keeps Unix behaviour byte-for-byte, per the module note above).
#
# On Windows there is no process-group kill: if the test process is terminated
# by the loop driver's watchdog while children are mid-flight (e.g. p135 at
# over-scale), every child is orphaned and keeps running, leaking processes +
# sockets + handles that snowball across loop iterations and degrade the box.
# Fix: put every spawned child in a Job Object created with
# JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE.  The job handle is held ONLY by this
# process, so when it dies for ANY reason (clean exit, TerminateProcess, crash)
# the kernel closes the handle and tears down the whole job -> all children die.
_job_handle = None          # None=unset, False=create failed, else HANDLE int
_job_lock = threading.Lock()

if _WIN:
    import ctypes
    from ctypes import wintypes

    _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000
    _JobObjectExtendedLimitInformation = 9

    class _JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [("PerProcessUserTimeLimit", ctypes.c_int64),
                    ("PerJobUserTimeLimit", ctypes.c_int64),
                    ("LimitFlags", wintypes.DWORD),
                    ("MinimumWorkingSetSize", ctypes.c_size_t),
                    ("MaximumWorkingSetSize", ctypes.c_size_t),
                    ("ActiveProcessLimit", wintypes.DWORD),
                    ("Affinity", ctypes.c_size_t),
                    ("PriorityClass", wintypes.DWORD),
                    ("SchedulingClass", wintypes.DWORD)]

    class _IO_COUNTERS(ctypes.Structure):
        _fields_ = [("ReadOperationCount", ctypes.c_uint64),
                    ("WriteOperationCount", ctypes.c_uint64),
                    ("OtherOperationCount", ctypes.c_uint64),
                    ("ReadTransferCount", ctypes.c_uint64),
                    ("WriteTransferCount", ctypes.c_uint64),
                    ("OtherTransferCount", ctypes.c_uint64)]

    class _JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [("BasicLimitInformation",
                     _JOBOBJECT_BASIC_LIMIT_INFORMATION),
                    ("IoInfo", _IO_COUNTERS),
                    ("ProcessMemoryLimit", ctypes.c_size_t),
                    ("JobMemoryLimit", ctypes.c_size_t),
                    ("PeakProcessMemoryUsed", ctypes.c_size_t),
                    ("PeakJobMemoryUsed", ctypes.c_size_t)]

    _k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _k32.CreateJobObjectW.restype = wintypes.HANDLE
    _k32.CreateJobObjectW.argtypes = [wintypes.LPVOID, wintypes.LPCWSTR]
    _k32.SetInformationJobObject.restype = wintypes.BOOL
    _k32.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int,
                                             ctypes.c_void_p, wintypes.DWORD]
    _k32.AssignProcessToJobObject.restype = wintypes.BOOL
    _k32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]

    def _ensure_job():
        global _job_handle
        if _job_handle is not None:
            return _job_handle
        with _job_lock:
            if _job_handle is not None:
                return _job_handle
            h = _k32.CreateJobObjectW(None, None)
            if not h:
                _job_handle = False
                return False
            info = _JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
            info.BasicLimitInformation.LimitFlags = \
                _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            if not _k32.SetInformationJobObject(
                    h, _JobObjectExtendedLimitInformation,
                    ctypes.byref(info), ctypes.sizeof(info)):
                _k32.CloseHandle(h)
                _job_handle = False
                return False
            _job_handle = h
            return h

    def _assign_to_job(proc):
        # Best-effort: a failed assignment must never break the spawn itself.
        try:
            h = _ensure_job()
            if not h:
                return
            ph = int(getattr(proc, "_handle", 0) or 0)
            if ph:
                _k32.AssignProcessToJobObject(h, wintypes.HANDLE(ph))
        except Exception:
            pass
else:
    def _assign_to_job(proc):
        return


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
        proc = runloom.blocking(subprocess.Popen, *args, **kwargs)
        _assign_to_job(proc)   # Windows: child dies with us; no-op on Unix
        return proc
    finally:
        sem.release()
