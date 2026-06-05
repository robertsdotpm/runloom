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
"""
import subprocess
import threading

MAX_CONCURRENT = 32
_spawn_sem = threading.Semaphore(MAX_CONCURRENT)


def popen(*args, **kwargs):
    import runloom
    with _spawn_sem:
        return runloom.blocking(subprocess.Popen, *args, **kwargs)
