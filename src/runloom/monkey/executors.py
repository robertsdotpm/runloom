"""Goroutine-backed concurrent.futures + multiprocessing cooperation."""
from ._base import *  # noqa: F401,F403  (shared foundation)
from .events import CoEvent, CoSemaphore  # noqa: F401
from . import osio  # _orig_os_read/_orig_os_write are rebound at patch-time -> read live


def _spawn(fn):
    """Spawn the worker fiber on whichever scheduler is active: mn_fiber under
    M:N (mn_hub_count() > 0), else the single-thread go.  A task spawned via the
    single-thread fiber() never runs under mn_run, so future.result() would hang."""
    if runloom_c.mn_hub_count() > 0:
        return runloom_c.mn_fiber(fn)
    return runloom_c.fiber(fn)

# ============================================================
# concurrent.futures -- fiber-backed ThreadPoolExecutor
#
# The stock ThreadPoolExecutor runs work on real OS threads and resolves each
# Future by notifying its threading.Condition from the worker thread.  After
# patch() that Condition is a CoCondition, and a cross-thread notify of a
# cooperative waiter does not wake it -- a fiber calling future.result()
# deadlocks (and the worker's work_queue.get() blocks on a CoSimpleQueue from
# a non-fiber thread).  Real-threaded executors are fundamentally at odds
# with cooperative primitives: the worker side needs real locks, the fiber
# caller side needs cooperative ones, and Future._condition cannot be both.
#
# So make ThreadPoolExecutor fiber-backed: submitted work runs as a
# fiber on the cooperative scheduler.  Both the setter (the task) and the
# waiter (future.result()/wait()/as_completed()) are then in the fiber
# domain, so the stock Future's CoCondition works.  Work is cooperative rather
# than parallel; a task that makes a truly-blocking call still offloads via the
# patched blocking APIs.  max_workers bounds concurrency with a CoSemaphore.
#
# Future.result/wait/as_completed themselves need no patch -- they already
# cooperate whenever the Future is completed from within a fiber.
# ============================================================
_orig_ThreadPoolExecutor = None
_co_tpe_cls = None

# Real (never-patched) lock for the executor's O(1) bookkeeping; captured at
# import time -- before patch() rebinds _thread.allocate_lock to the cooperative
# CoLock -- so it stays a real OS mutex (same idiom as _raw_get_ident below).
# Held only for set/flag updates, never across a park.
_raw_allocate_lock = _thread.allocate_lock


def _build_co_tpe():
    import concurrent.futures as cf
    try:
        from concurrent.futures.thread import BrokenThreadPool as _BrokenThreadPool
    except Exception:                       # pragma: no cover - ancient Python
        _BrokenThreadPool = RuntimeError

    class CoThreadPoolExecutor(cf.Executor):
        """ThreadPoolExecutor that runs submitted callables as fibers."""

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
            self._broken = None             # BrokenThreadPool once init fails
            self._guard = _raw_allocate_lock()   # real lock, O(1) bookkeeping
            # Only the IN-FLIGHT futures (submitted but not finished): each task
            # discards its own future on completion, so the set stays
            # O(in-flight) -- never O(total submits) -- and shutdown() has the
            # not-yet-started futures to cancel.
            self._pending = set()
            # Set whenever _pending is empty; shutdown(wait=True) parks on it
            # until every task fiber has run its finally (i.e. after set_result
            # and its inline done-callbacks), matching stdlib join semantics.
            self._idle = CoEvent()
            self._idle.set()
            # The initializer runs once per POOL (a fiber-backed pool has no
            # persistent worker threads), not once per task.
            self._init_started = False
            self._init_done = CoEvent()

        def _run_initializer(self):
            """Run the pool initializer exactly once for this executor, before
            the first task's callable runs.  stdlib runs it once per worker
            THREAD; the fiber-backed pool has no persistent workers, so
            once-per-pool is the faithful, bounded analogue -- unlike the old
            once-per-TASK, which re-ran per-worker setup (DB connect, license
            slot, thread-local cache) for every submit.  A task that arrives
            while another fiber is initializing waits on _init_done; if the
            initializer raised, the pool is broken and every task/submit
            surfaces BrokenThreadPool, as stdlib does."""
            if self._initializer is None:
                return
            if self._init_done.is_set():    # fast path once settled
                if self._broken is not None:
                    raise self._broken
                return
            do_init = False
            with self._guard:
                if self._broken is not None:
                    raise self._broken
                if self._init_done.is_set():
                    return
                if not self._init_started:
                    self._init_started = True
                    do_init = True
            if not do_init:
                # Another fiber owns the one-shot init; wait for it to finish.
                self._init_done.wait()
                if self._broken is not None:
                    raise self._broken
                return
            try:
                self._initializer(*self._initargs)
            except BaseException as exc:
                broken = _BrokenThreadPool(
                    "A worker initializer raised in CoThreadPoolExecutor")
                broken.__cause__ = exc
                with self._guard:
                    self._broken = broken
                self._init_done.set()       # wake waiters -> they see _broken
                raise broken
            self._init_done.set()

        def _task_done(self, fut):
            with self._guard:
                self._pending.discard(fut)
                if not self._pending:
                    self._idle.set()

        def submit(self, fn, /, *args, **kwargs):
            if self._shutdown:
                raise RuntimeError(
                    "cannot schedule new futures after shutdown")
            if self._broken is not None:
                raise self._broken
            fut = cf.Future()
            with self._guard:
                # Re-check under the guard so a concurrent shutdown() cannot race
                # a fresh future into _pending after it has snapshotted the set.
                if self._shutdown:
                    raise RuntimeError(
                        "cannot schedule new futures after shutdown")
                if self._broken is not None:
                    raise self._broken
                self._pending.add(fut)
                self._idle.clear()
            sem = self._sem

            def task():
                sem.acquire()           # honour max_workers
                try:
                    if not fut.set_running_or_notify_cancel():
                        return          # future was cancelled before start
                    try:
                        self._run_initializer()   # once per pool
                        result = fn(*args, **kwargs)
                    except BaseException as exc:
                        fut.set_exception(exc)
                    else:
                        fut.set_result(result)
                finally:
                    sem.release()
                    self._task_done(fut)

            # A fiber-spawned task only runs while a scheduler is driving it.
            # With no runtime live (a plain main thread before/after run(), a
            # background OS thread with no hub), run it inline so the future
            # still resolves -- stock ThreadPoolExecutor works in any context.
            if _runtime_live():
                _spawn(task)
            else:
                task()
            return fut

        def shutdown(self, wait=True, *, cancel_futures=False):
            with self._guard:
                self._shutdown = True
                pending = list(self._pending) if cancel_futures else ()
            if cancel_futures:
                # Cancel every not-yet-started future.  A running future refuses
                # the cancel (Future.cancel() -> False) and finishes normally; a
                # cancelled one is skipped by set_running_or_notify_cancel().
                for fut in pending:
                    fut.cancel()
            if wait:
                self._idle.wait()

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

# multiprocessing.synchronize.RECURSIVE_MUTEX -- the SemLock.kind of an RLock
# (Value/Array.get_lock()); SEMAPHORE (1) is Lock/Semaphore/Event/Condition.
_SEMLOCK_RECURSIVE = 0

# Raw OS-thread id, for tracking recursion ownership on a non-fiber (real OS
# thread) caller -- captured pre-patch, same as locks.CoRLock uses.
_raw_get_ident = _thread.get_ident


def _co_recursion_owner():
    """Identity that OWNS a recursive cooperative SemLock: the current fiber
    when one is running, else the OS-thread id.  A G handle (fiber) and an int
    (thread) never compare equal, so the two domains can't be confused -- the
    same scheme locks.CoRLock uses.  This replaces the C SemLock's OS-thread
    `_is_mine()` ownership, which is WRONG under M:N: many fibers share one hub
    OS thread, so the C check would treat fiber B's re-entry as fiber A's."""
    return runloom.current() if _in_fiber() else _raw_get_ident()


def _co_semlock_acquire(semlock):
    """Cooperative SemLock.acquire.  A blocking acquire from a fiber does a
    non-blocking sem_trywait + _co_sleep backoff instead of sem_wait, so a
    contended cross-process Lock/Semaphore/Event/Condition/Barrier doesn't
    freeze the scheduler thread.  Real threads and explicit non-blocking
    acquires fall straight through to the C call.  (POSIX semaphores have no
    readiness fd, so this is a backoff poll -- same shape as the fcntl shim.)"""
    def acquire(block=True, timeout=None):
        if not block or not _in_fiber():
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


def _co_recursive_semlock_methods(semlock):
    """Cooperative acquire/release for a RECURSIVE_MUTEX SemLock (an RLock, e.g.
    Value/Array.get_lock()).  The C semlock_acquire tracks re-entrancy by OS
    thread id (`_is_mine()`); under M:N that is broken, because many fibers
    share ONE hub OS thread, so the C code would let fiber B re-enter a critical
    section fiber A still holds (a double-entered lock -> a torn RMW / lost
    increment).  We instead track recursion ownership + count by the CURRENT
    FIBER and only touch the C semlock on the OUTERMOST acquire/release, so the
    kernel sem is the cross-process gate and Python is the per-fiber re-entrancy.
    A fiber that does NOT already own it never calls the C acquire while another
    fiber holds it, so the C `_is_mine()` shortcut is unreachable across fibers.

    `state` = [owner, count]; mutated only by the owning fiber (set under the
    held lock, cleared on the final release), so a non-owner only ever READS
    owner to compare against itself -- a benign stale read at worst costs one
    extra backoff spin.  Real-thread / non-blocking callers fall through to the
    raw C path unchanged (a real thread's OS id never collides with a fiber G)."""
    state = [None, 0]                           # [owner identity, recursion count]

    def acquire(block=True, timeout=None):
        if not _in_fiber():
            return semlock.acquire(block, timeout)
        cur = _co_recursion_owner()
        if state[0] is not None and state[0] == cur:
            # Already held by THIS fiber -> pure re-entrant bump, no C call (the
            # kernel sem stays at "1 holder", recursion lives only in Python).
            # A re-entrant acquire ALWAYS deepens the recursion, blocking or not
            # -- stdlib RLock bumps the count on a non-blocking re-entrant
            # acquire too.  Returning True without the bump would drop the count
            # by one level, so the matching release() would sem_post the kernel
            # semaphore while we logically still hold an outer level, releasing
            # the cross-process lock out from under the caller.
            state[1] += 1
            return True
        if not block:
            # A non-blocking acquire by a fiber that is not the owner: only the
            # C trywait can win it, and only when no fiber owns it (else the C
            # `_is_mine()` shortcut on a sibling's hub thread would falsely pass).
            if state[0] is not None:
                return False
            if semlock.acquire(False):          # sem_trywait
                state[0] = cur
                state[1] = 1
                return True
            return False
        deadline = None if timeout is None else time.monotonic() + timeout
        step = 0.0005
        while True:
            # Only attempt the C acquire when NO fiber currently owns it; this is
            # what prevents the cross-fiber `_is_mine()` double-enter -- a sibling
            # on the same hub thread never reaches the C call while we hold it.
            if state[0] is None and semlock.acquire(False):   # sem_trywait
                state[0] = cur
                state[1] = 1
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

    def release():
        cur = _co_recursion_owner()
        if state[0] is None or state[0] != cur:
            # Not held by us at the Python layer -- either a real-thread holder
            # (whose recursion lives in C, never in `state`) or a forked child
            # whose fiber identity changed; defer to the C release, which raises
            # the same AssertionError the stock SemLock would on a bad release.
            semlock.release()
            return
        state[1] -= 1
        if state[1] == 0:
            state[0] = None
            semlock.release()                   # final release -> sem_post

    return acquire, release


def _patched_semlock_make_methods(self):
    _orig_semlock_make_methods(self)            # binds the C acquire/release
    if getattr(self._semlock, "kind", None) == _SEMLOCK_RECURSIVE:
        # RLock (Value/Array.get_lock()): re-entrancy must be per-FIBER, so own
        # both ends -- the recursion count lives in Python, not the C semlock.
        self.acquire, self.release = _co_recursive_semlock_methods(self._semlock)
    else:
        # Non-recursive (Lock/Semaphore/Event/Condition): every acquire goes to
        # the real sem, no `_is_mine()` shortcut exists, so only acquire needs
        # the cooperative backoff; release stays the C sem_post.
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
