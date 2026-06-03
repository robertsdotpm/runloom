"""Goroutine-backed concurrent.futures + multiprocessing cooperation."""
from ._base import *  # noqa: F401,F403  (shared foundation)
from .events import CoEvent, CoSemaphore  # noqa: F401
from . import osio  # _orig_os_read/_orig_os_write are rebound at patch-time -> read live

# ============================================================
# concurrent.futures -- goroutine-backed ThreadPoolExecutor
#
# The stock ThreadPoolExecutor runs work on real OS threads and resolves each
# Future by notifying its threading.Condition from the worker thread.  After
# patch() that Condition is a CoCondition, and a cross-thread notify of a
# cooperative waiter does not wake it -- a goroutine calling future.result()
# deadlocks (and the worker's work_queue.get() blocks on a CoSimpleQueue from
# a non-goroutine thread).  Real-threaded executors are fundamentally at odds
# with cooperative primitives: the worker side needs real locks, the goroutine
# caller side needs cooperative ones, and Future._condition cannot be both.
#
# So make ThreadPoolExecutor goroutine-backed: submitted work runs as a
# goroutine on the cooperative scheduler.  Both the setter (the task) and the
# waiter (future.result()/wait()/as_completed()) are then in the goroutine
# domain, so the stock Future's CoCondition works.  Work is cooperative rather
# than parallel; a task that makes a truly-blocking call still offloads via the
# patched blocking APIs.  max_workers bounds concurrency with a CoSemaphore.
#
# Future.result/wait/as_completed themselves need no patch -- they already
# cooperate whenever the Future is completed from within a goroutine.
# ============================================================
_orig_ThreadPoolExecutor = None
_co_tpe_cls = None


def _build_co_tpe():
    import concurrent.futures as cf

    class CoThreadPoolExecutor(cf.Executor):
        """ThreadPoolExecutor that runs submitted callables as goroutines."""

        def __init__(self, max_workers=None, thread_name_prefix="",
                     initializer=None, initargs=()):
            if max_workers is None:
                max_workers = min(32, (os.cpu_count() or 1) + 4)
            if max_workers <= 0:
                raise ValueError("max_workers must be greater than 0")
            self._max_workers = max_workers
            self._sem = CoSemaphore(max_workers)
            self._initializer = initializer
            self._initargs = initargs
            self._shutdown = False
            self._pending = []          # CoEvent per in-flight task

        def submit(self, fn, /, *args, **kwargs):
            if self._shutdown:
                raise RuntimeError(
                    "cannot schedule new futures after shutdown")
            fut = cf.Future()
            done = CoEvent()
            self._pending.append(done)
            sem, initializer, initargs = self._sem, self._initializer, \
                self._initargs

            def task():
                sem.acquire()           # honour max_workers
                try:
                    if not fut.set_running_or_notify_cancel():
                        return          # future was cancelled before start
                    try:
                        if initializer is not None:
                            initializer(*initargs)
                        result = fn(*args, **kwargs)
                    except BaseException as exc:
                        fut.set_exception(exc)
                    else:
                        fut.set_result(result)
                finally:
                    sem.release()
                    done.set()

            runloom_c.go(task)
            return fut

        def shutdown(self, wait=True, *, cancel_futures=False):
            self._shutdown = True
            pending, self._pending = self._pending, []
            if wait:
                for done in pending:
                    done.wait()

    return CoThreadPoolExecutor


def _patch_futures():
    global _orig_ThreadPoolExecutor, _co_tpe_cls
    try:
        import concurrent.futures as cf
    except ImportError:
        return
    if _co_tpe_cls is None:
        _co_tpe_cls = _build_co_tpe()
    # Touch the attribute first so the lazy submodule import (and its
    # os.register_at_fork) runs against the real class, then override.
    _orig_ThreadPoolExecutor = cf.ThreadPoolExecutor
    cf.ThreadPoolExecutor = _co_tpe_cls
    thread_mod = sys.modules.get("concurrent.futures.thread")
    if thread_mod is not None:
        thread_mod.ThreadPoolExecutor = _co_tpe_cls


def _unpatch_futures():
    if _orig_ThreadPoolExecutor is None:
        return
    import concurrent.futures as cf
    cf.ThreadPoolExecutor = _orig_ThreadPoolExecutor
    thread_mod = sys.modules.get("concurrent.futures.thread")
    if thread_mod is not None:
        thread_mod.ThreadPoolExecutor = _orig_ThreadPoolExecutor


# ============================================================
# multiprocessing -- cooperate regardless of import order
#
# multiprocessing.connection.Connection._recv / _send (POSIX) capture os.read /
# os.write as *default arguments* at import time:
#
#     _read = os.read
#     def _recv(self, size, read=_read): ...
#
# If multiprocessing was imported before patch(), those defaults are the
# original *blocking* os.read/os.write -- so Connection.recv/send (and
# everything built on Connection: Pipe, Queue, Pool, Process.join's sentinel
# wait) does a blocking read that freezes the whole scheduler instead of
# parking on wait_fd.  The data still flows, so it "works", but it doesn't
# yield.  Rebind the captured defaults (and the module _read/_write) to the
# now-patched cooperative os.read/os.write.  Runs after the "os" patch, so
# os.read/os.write already point at the cooperative versions.  If the module
# isn't imported yet, it's a no-op -- a later import binds the patched os.read.
#
# Connection._close(self, _close=os.close) captures os.close the same way:
# the patched os.close clears the fd's netpoll registration bit so a reused fd
# re-arms cleanly under the edge-triggered register-once scheme, but the
# original os.close does not -- so a Connection closed with the unpatched
# default leaves a stale registration, and the next Connection that reuses that
# fd number never wakes from wait_fd (a hang).  Rebind it too.
#
# Windows Connection uses _multiprocessing.recv/send (overlapped I/O), a
# separate path this does not touch.
# ============================================================
_orig_mp_recv_defaults  = None
_orig_mp_send_defaults  = None
_orig_mp_close_defaults = None
_orig_semlock_make_methods = None


def _co_semlock_acquire(semlock):
    """Cooperative SemLock.acquire.  A blocking acquire from a goroutine does a
    non-blocking sem_trywait + _co_sleep backoff instead of sem_wait, so a
    contended cross-process Lock/Semaphore/Event/Condition/Barrier doesn't
    freeze the scheduler thread.  Real threads and explicit non-blocking
    acquires fall straight through to the C call.  (POSIX semaphores have no
    readiness fd, so this is a backoff poll -- same shape as the fcntl shim.)"""
    def acquire(block=True, timeout=None):
        if not block or not _in_goroutine():
            return semlock.acquire(block, timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        step = 0.0005
        while True:
            if semlock.acquire(False):          # sem_trywait
                return True
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                _co_sleep(min(step, remaining))
            else:
                _co_sleep(step)
            if step < 0.02:
                step *= 2
    return acquire


def _patched_semlock_make_methods(self):
    _orig_semlock_make_methods(self)            # binds the C acquire/release
    self.acquire = _co_semlock_acquire(self._semlock)


def _patch_mp_connection():
    global _orig_mp_recv_defaults, _orig_mp_send_defaults, _orig_mp_close_defaults
    conn = sys.modules.get("multiprocessing.connection")
    if conn is None:
        return
    Connection = getattr(conn, "Connection", None)
    if Connection is None:
        return
    recv  = Connection.__dict__.get("_recv")
    send  = Connection.__dict__.get("_send")
    close = Connection.__dict__.get("_close")
    # Only the POSIX os.read/os.write/os.close-based Connection has these as
    # plain functions with a captured default; skip otherwise.
    if recv is not None and getattr(recv, "__defaults__", None):
        _orig_mp_recv_defaults = recv.__defaults__
        recv.__defaults__ = (os.read,)
    if send is not None and getattr(send, "__defaults__", None):
        _orig_mp_send_defaults = send.__defaults__
        send.__defaults__ = (os.write,)
    # Rebind _close only when its captured default is the real os.close (the
    # POSIX variant); the win/socket variants capture a different closer.
    if close is not None and getattr(close, "__defaults__", None) == (_raw_os_close,):
        _orig_mp_close_defaults = close.__defaults__
        close.__defaults__ = (os.close,)
    if hasattr(conn, "_read"):
        conn._read = os.read
    if hasattr(conn, "_write"):
        conn._write = os.write


def _patch_mp_synchronize():
    # SemLock._make_methods binds self.acquire to the C sem_wait; replace it
    # with a cooperative version.  Only SemLock instances created after this
    # runs cooperate (patch early).  Force-import synchronize only when the
    # program already uses multiprocessing, so non-mp users pay nothing.
    global _orig_semlock_make_methods
    if _orig_semlock_make_methods is not None:
        return
    if "multiprocessing" not in sys.modules:
        return
    try:
        import multiprocessing.synchronize as sync
    except ImportError:
        return
    SemLock = getattr(sync, "SemLock", None)
    if SemLock is None or not hasattr(SemLock, "_make_methods"):
        return
    _orig_semlock_make_methods = SemLock._make_methods
    SemLock._make_methods = _patched_semlock_make_methods


def _patch_multiprocessing():
    _patch_mp_connection()
    _patch_mp_synchronize()


def _unpatch_multiprocessing():
    global _orig_semlock_make_methods
    sync = sys.modules.get("multiprocessing.synchronize")
    if sync is not None and _orig_semlock_make_methods is not None:
        sync.SemLock._make_methods = _orig_semlock_make_methods
        _orig_semlock_make_methods = None
    conn = sys.modules.get("multiprocessing.connection")
    if conn is None:
        return
    Connection = getattr(conn, "Connection", None)
    if Connection is None:
        return
    recv  = Connection.__dict__.get("_recv")
    send  = Connection.__dict__.get("_send")
    close = Connection.__dict__.get("_close")
    if recv is not None and _orig_mp_recv_defaults is not None:
        recv.__defaults__ = _orig_mp_recv_defaults
    if send is not None and _orig_mp_send_defaults is not None:
        send.__defaults__ = _orig_mp_send_defaults
    if close is not None and _orig_mp_close_defaults is not None:
        close.__defaults__ = _orig_mp_close_defaults
    if hasattr(conn, "_read"):
        conn._read = osio._orig_os_read if osio._orig_os_read is not None else os.read
    if hasattr(conn, "_write"):
        conn._write = osio._orig_os_write if osio._orig_os_write is not None else os.write
