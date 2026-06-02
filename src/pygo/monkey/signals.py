"""Cooperative signal.sigwait/sigwaitinfo/sigtimedwait/pause."""
from ._base import *  # noqa: F401,F403  (shared foundation)

# ============================================================
# signal -- cooperative sigwait / sigtimedwait / pause
#
# sigwait(set) and sigtimedwait(set, timeout) require their signals to be
# blocked (pthread_sigmask) so delivery queues them as *pending* rather than
# running a handler -- that is their whole contract.  A zero-timeout
# sigtimedwait is a non-blocking reap of a pending signal, so we poll that on
# a backoff via _co_sleep instead of blocking the OS thread.
#
# pause() has no pending-signal form: it returns once any *handled* signal is
# caught.  When the goroutine is running on the interpreter's main thread (the
# single-threaded scheduler), signal.set_wakeup_fd lets us turn "a signal was
# caught" into a pollable event -- park on a pipe the signal machinery writes
# to, then restore the previous wakeup fd.  Under the M:N scheduler a goroutine
# runs on a hub thread (set_wakeup_fd is main-thread-only), so there we offload
# the real blocking pause().  Caveat for that fallback: a process-directed
# signal delivered to another thread won't interrupt the worker's pause(); code
# needing a hard guarantee should block the signal and sigwait() it instead.
# ============================================================
try:
    import signal as _signal_mod
except ImportError:
    _signal_mod = None

_HAVE_SIGTIMEDWAIT = (_signal_mod is not None and
                      hasattr(_signal_mod, "sigtimedwait"))

_orig_sigwait        = None
_orig_sigwaitinfo    = None
_orig_sigtimedwait   = None
_orig_signal_pause   = None


def _patched_sigwait(sigset):
    if not _in_goroutine() or not _HAVE_SIGTIMEDWAIT:
        return _orig_sigwait(sigset)
    step = 0.0005
    while True:
        try:
            info = _orig_sigtimedwait(sigset, 0)   # non-blocking reap
        except InterruptedError:
            continue                               # EINTR by an out-of-set sig
        if info is not None:
            return info.si_signo
        _co_sleep(step)
        if step < 0.02:
            step *= 2


def _patched_sigwaitinfo(sigset):
    # Like sigwait but returns the full struct_siginfo (no timeout form).
    if not _in_goroutine() or not _HAVE_SIGTIMEDWAIT:
        return _orig_sigwaitinfo(sigset)
    step = 0.0005
    while True:
        try:
            info = _orig_sigtimedwait(sigset, 0)
        except InterruptedError:
            continue
        if info is not None:
            return info
        _co_sleep(step)
        if step < 0.02:
            step *= 2


def _patched_sigtimedwait(sigset, timeout):
    if not _in_goroutine():
        return _orig_sigtimedwait(sigset, timeout)
    deadline = None if timeout is None else time.monotonic() + max(0.0, timeout)
    step = 0.0005
    while True:
        try:
            info = _orig_sigtimedwait(sigset, 0)
        except InterruptedError:
            continue
        if info is not None:
            return info
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None                        # timed out
            _co_sleep(min(step, remaining))
        else:
            _co_sleep(step)
        if step < 0.02:
            step *= 2


def _patched_signal_pause():
    if not _in_goroutine():
        return _orig_signal_pause()
    # M:N hub thread: set_wakeup_fd would raise -- offload the blocking pause.
    if _th.current_thread() is not _th.main_thread():
        return _blocking_call(_orig_signal_pause)
    # Main thread: arm a self-pipe as the signal wakeup fd, park on it until a
    # handled signal writes its number, then restore the previous wakeup fd.
    r, w = os.pipe()
    os.set_blocking(r, False)
    os.set_blocking(w, False)
    try:
        try:
            prev = _signal_mod.set_wakeup_fd(w)
        except (ValueError, OSError):
            # Not actually the main thread of the main interpreter, etc.
            return _blocking_call(_orig_signal_pause)
        try:
            pygo_core.wait_fd(r, READ)
        finally:
            _signal_mod.set_wakeup_fd(prev)
    finally:
        os.close(r)
        os.close(w)
    return None


def _patch_signal():
    global _orig_sigwait, _orig_sigwaitinfo, _orig_sigtimedwait, _orig_signal_pause
    if _signal_mod is None:
        return
    if hasattr(_signal_mod, "sigwait"):
        _orig_sigwait = _signal_mod.sigwait
        _signal_mod.sigwait = _patched_sigwait
    if hasattr(_signal_mod, "sigwaitinfo"):
        _orig_sigwaitinfo = _signal_mod.sigwaitinfo
        _signal_mod.sigwaitinfo = _patched_sigwaitinfo
    if hasattr(_signal_mod, "sigtimedwait"):
        _orig_sigtimedwait = _signal_mod.sigtimedwait
        _signal_mod.sigtimedwait = _patched_sigtimedwait
    if hasattr(_signal_mod, "pause"):
        _orig_signal_pause = _signal_mod.pause
        _signal_mod.pause = _patched_signal_pause


def _unpatch_signal():
    if _signal_mod is None:
        return
    if _orig_sigwait is not None:
        _signal_mod.sigwait = _orig_sigwait
    if _orig_sigwaitinfo is not None:
        _signal_mod.sigwaitinfo = _orig_sigwaitinfo
    if _orig_sigtimedwait is not None:
        _signal_mod.sigtimedwait = _orig_sigtimedwait
    if _orig_signal_pause is not None:
        _signal_mod.pause = _orig_signal_pause
