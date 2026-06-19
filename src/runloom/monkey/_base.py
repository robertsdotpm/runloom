"""Shared foundation for the runloom monkey-patch package: stdlib
re-exports, fiber-context detection, the self-pipe Parker, the
blocking-call backend, and cooperative sleep.  Every section module
does `from ._base import *`."""

import _thread
import builtins
import collections
import errno
import io
import os
import platform as _platform
import select as _select_mod
import socket
import ssl as _ssl_mod
import stat
import subprocess
import sys
import threading as _th
import time

_IS_WINDOWS = _platform.system() == "Windows"
_IS_DARWIN  = _platform.system() == "Darwin"

# The pid that imported this module -- i.e. the process whose runloom scheduler
# our cooperative identity (runloom.current()) is valid in.  After os.fork() the
# child inherits a *copy* of the scheduler that is not actually running, so
# runloom.current() there returns a fresh, meaningless G on every call.  This is
# captured once and never reassigned, so a pid mismatch reliably means "we are
# in a forked child" regardless of os.register_at_fork handler ordering.  It is
# only ever consulted on cold error paths (see _is_forked_child), never in the
# I/O hot path -- os.getpid() costs ~1.3us, far too much per recv/send.
_PID_IMPORT = os.getpid()


def _is_forked_child():
    return os.getpid() != _PID_IMPORT

import runloom
import runloom_c

READ  = 1
WRITE = 2

# Per-fd cooperative timeout side table.  socket.socket has no __dict__, so we
# cannot stash the caller's settimeout() on the instance; _make_nonblocking
# records it here (keyed by fd) before forcing the socket non-blocking, and
# _coop_timeout reads it back.  Cleared in the patched close()/detach().  Plain
# dict ops are thread-safe on the free-threaded build (PEP 703).
_SOCK_TIMEOUTS = {}

# Snapshots of stdlib primitives that the patches themselves call.
# Capturing here (at module import time) protects us against the
# patched versions calling back into themselves.
_raw_os_read  = os.read
_raw_os_write = os.write
_raw_os_close = os.close
_raw_time_sleep = time.sleep
# Captured before _patch_socket installs the cooperative versions.  Parker
# uses these to talk to its self-pipe / self-socketpair without going
# through the cooperative wrappers (which would, for instance, park
# forever on a non-blocking recv that returns BlockingIOError).
_raw_sock_recv = socket.socket.recv
_raw_sock_send = socket.socket.send
# Raw select (before polling.py installs the cooperative select).  A _Parker on
# a FOREIGN OS thread (not a fiber) blocks the thread on its wake fd with
# this -- the patched select would re-enter the cooperative path on a thread
# with no fiber/hub.
_raw_select = _select_mod.select
# Raw os.sendfile (before _patch_syscalls offloads it to the pool).  The
# cooperative socket.sendfile drives this directly in non-blocking mode and
# parks on wait_fd, rather than blocking a pool worker for the whole transfer.
_raw_os_sendfile = getattr(os, "sendfile", None)


# ---------- fiber-context detection ----------
# runloom_c (C scheduler) does not expose a "current fiber"
# accessor, so we wrap runloom_c.fiber / mn_fiber and bump a thread-local
# counter for the duration of every user callable.  The Python
# scheduler still uses runloom.current() (which works there).
_g_state = _th.local()


def _bump_in(value):
    _g_state.count = getattr(_g_state, "count", 0) + value


def _wrap_fiber_callable(fn):
    def wrapper():
        _bump_in(1)
        try:
            return fn()
        finally:
            _bump_in(-1)
    return wrapper


def _in_fiber():
    """True when called from inside a running fiber.

    Handles both the C scheduler (via the thread-local counter set by
    our runloom_c.fiber wrapper) and the Python scheduler (via
    runloom.current())."""
    if getattr(_g_state, "count", 0) > 0:
        return True
    try:
        return runloom.current() is not None
    except Exception:
        return False


def _make_nonblocking(sock):
    """Set the underlying fd to non-blocking, idempotent.

    Also flips TCP_NODELAY on stream sockets the first time we see them.
    Without it, Nagle + delayed-ack stalls the loopback echo path by
    up to 40 ms per round trip -- which is what made the README's
    116 us/RT number look respectable when native Go does ~50 us.

    Called at the top of EVERY recv/send/accept/connect, so the per-op cost
    here is hot.  A fresh socket reads back gettimeout() != 0.0 (sockets are
    created blocking); after we flip it non-blocking it reads 0.0 forever, so
    that one check doubles as the "first time we've seen this socket" guard --
    we do the setblocking AND the TCP_NODELAY setsockopt under it, exactly once
    per socket.  (TCP_NODELAY used to sit OUTSIDE the guard, firing a redundant
    setsockopt syscall + sock.type/sock.family attribute lookups on every
    single recv/send -- a measurable chunk of the steady-state hot path.)
    """
    if sock.gettimeout() != 0.0:
        # Record the caller's intended timeout BEFORE setblocking(False) zeroes
        # it: once forced non-blocking, gettimeout() reads 0.0 forever, so the
        # cooperative I/O layer would otherwise lose the user's settimeout() and
        # park with no deadline (a timed socket that never receives hangs the
        # fiber).  socket.socket has no __dict__, so we keep this in a side
        # table keyed by fd (see _coop_timeout).  The guard re-fires whenever
        # the user changes the timeout (gettimeout != 0.0 again), so this tracks
        # later settimeout() calls AND self-heals a reused fd (a fresh socket's
        # first sighting overwrites any stale entry).
        try:
            _SOCK_TIMEOUTS[sock.fileno()] = sock.gettimeout()
        except OSError:
            pass
        sock.setblocking(False)
        # First sighting of this socket -> flip TCP_NODELAY once.  Cheap (sets
        # a flag), safe (request-response apps benefit unconditionally), and
        # matches Go / asyncio's default for low-latency TCP.
        try:
            if sock.type == socket.SOCK_STREAM and \
               sock.family in (socket.AF_INET, socket.AF_INET6):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except (OSError, AttributeError):
            pass


def _fd_pollable(fd):
    """Is this fd pollable by the C-side netpoll?

    POSIX (epoll/kqueue/select):
        sockets, fifos/pipes, ttys, char/block devices  -> yes
        regular files                                    -> no
    Windows (WSAPoll/select):
        SOCKET handles  -> yes (handled by the socket-layer patches,
                           not by this function -- os.read/write
                           callers never see SOCKET fds)
        pipe / file / tty fds -> no.  Win32 select() refuses anything
                                 except SOCKETs; pipe fds are kernel
                                 HANDLEs wrapped by the CRT and are
                                 not selectable.

    On Windows we therefore always return False from this helper --
    the os.read/write path can only ever see non-socket fds, which
    must go through the thread-pool backend instead of wait_fd.
    """
    if _IS_WINDOWS:
        return False
    try:
        st = os.fstat(fd)
    except OSError:
        return False
    m = st.st_mode
    return (stat.S_ISFIFO(m) or stat.S_ISSOCK(m) or
            stat.S_ISCHR(m)  or stat.S_ISBLK(m))


# ---------- self-pipe parker ----------
#
# Two implementations, chosen at import time:
#   POSIX  -> os.pipe() + os.read/os.write.  Pipes are kernel-fd ints
#             that wait_fd's epoll/kqueue/select backends can all poll.
#   Windows -> socket.socketpair() + sock.recv/sock.send.  Windows
#             select() refuses pipe fds (those are Python-side fakes
#             over Win32 HANDLEs); it ONLY polls SOCKET handles.
#             socket.socketpair() on Windows returns two AF_INET TCP
#             sockets whose fileno() values are real SOCKET handles
#             and therefore work with wait_fd's select backend.
#
# Either way, the Parker is single-thread cooperative -- the
# unpark()-before-park() race is not handled because in cooperative
# mode the parker can't be signalled until the parking fiber has
# yielded.
class _Parker(object):
    __slots__ = ("r", "w", "_sockets", "_g_handle", "_via_park")
    # Free-list of reusable (r, w, _sockets) tuples.  The pool lock uses a
    # NON-BLOCKING acquire so that no fiber ever waits for it (blocking
    # would freeze the fiber's hub if sysmon preempted the holder).  A
    # fiber that finds the lock contended skips pooling and closes its FDs
    # instead -- never blocks, never freezes the hub.  list.pop() in __init__
    # still races lock-free via try/except IndexError.
    _pool = []
    _pool_lock = _thread.allocate_lock()   # captured pre-patch → real OS mutex

    def __init__(self, inmem=False):
        # The handle of the fiber parked here.  In FD mode it is read by
        # _unpark_all -> runloom_c.unpark_many for a batched netpoll DIRECT wake (no
        # per-waiter os.write); None means a foreign-thread waiter -> pipe-write.
        # In IN-MEMORY mode (inmem=True) it is the WAKE HOLDER itself: g.wake()
        # re-queues the fiber parked via runloom_c.park(), and it is set HERE,
        # before the parker is published to setters, so a set() that races the park
        # still finds it (the park_generic Dekker then catches a pre-commit wake).
        self._g_handle = None
        # In-memory park: ZERO per-waiter fds (woken by g.wake(); shares the one
        # process-wide run-alive anchor).  Opt-in ONLY for callers that wake via
        # the _via_park-aware _unpark_all (Event/Condition/Semaphore) -- callers
        # that wake via a direct p.unpark() (queues/dns/threadpool) must NOT pass
        # inmem (os.write needs the fd).  Requires a fiber context.
        self._via_park = bool(inmem)
        if self._via_park:
            self._g_handle = runloom_c.current_g()
            self.r = self.w = -1
            self._sockets = None
            return
        try:
            reused = _Parker._pool.pop()
        except IndexError:
            reused = None
        if reused is not None:
            self.r, self.w, self._sockets = reused
        elif _IS_WINDOWS:
            s1, s2 = socket.socketpair()
            s1.setblocking(False)
            s2.setblocking(False)
            self._sockets = (s1, s2)
            self.r = s1.fileno()
            self.w = s2.fileno()
        else:
            r, w = os.pipe()
            os.set_blocking(r, False)
            os.set_blocking(w, False)
            self.r = r
            self.w = w
            self._sockets = None

    def park(self, timeout=None):
        if self._via_park:
            # In-memory M:N park: 0 fds.  Woken by g.wake() (from _unpark_all).
            # foreign_wakeable=True arms the shared run-alive anchor so a foreign
            # SETTER's g.wake() can't race a single-thread run()'s exit.  The
            # park_generic Dekker makes a wake that beats this commit a no-op
            # (returns immediately) -- no lost wakeup.  The caller's flag loop
            # re-parks on a spurious return.  No fd byte to drain.
            #
            # A TIMED wait passes the deadline straight to the C park: it wakes at
            # the monotonic deadline via the scheduler's per-hub timer heap (the
            # SAME parked_safe CAS, exactly-once vs a real wake) -- still 0 fds, no
            # waker fiber.  The caller re-checks its own deadline (a spurious
            # early return is possible), exactly as the old wait_fd(timeout) did.
            if timeout is None:
                runloom_c.park(foreign_wakeable=True)
            else:
                runloom_c.park(foreign_wakeable=True, timeout=timeout)
            return
        # Clear FIRST: a _Parker object reused for a second park must not carry
        # the previous park's fiber handle (a stale handle would let a setter
        # direct-wake the WRONG g -> this waiter hangs).  The foreign-thread
        # branch leaves it None, which is exactly the "no direct wake" marker.
        self._g_handle = None
        if _in_fiber():
            # Publish our fiber handle so a fan-in setter can wake us via the
            # batched runloom_c.unpark_many instead of an os.write per waiter.
            self._g_handle = runloom_c.current_g()
            # Pass the deadline straight to the netpoll wait: wait_fd returns on
            # the unpark byte OR at timeout_ms, so a TIMED wait needs no separate
            # waker fiber + heap timer (the old per-primitive _spawn(waker)
            # cost that sat behind every Event/Condition/Semaphore timed wait).
            # -1 == block forever.  max(1, ...) so a sub-ms timeout still waits a
            # tick rather than busy-returning.
            timeout_ms = -1 if timeout is None else max(1, int(timeout * 1000))
            runloom_c.wait_fd(self.r, READ, timeout_ms)
        else:
            # FOREIGN OS thread (e.g. a multiprocessing.Queue _feed daemon
            # thread taking a monkey-patched threading.Condition): block the
            # THREAD on the wake fd with a real select.  runloom_c.wait_fd parks
            # a GOROUTINE on a hub's netpoll -- there is no fiber/hub on this
            # thread, so calling it here is undefined and raced -> SIGSEGV under
            # M:N.  timeout=None blocks until unpark() writes the wake byte.
            try:
                _raw_select([self.r], [], [], timeout)
            except (OSError, ValueError):
                pass
        if self._sockets is not None:
            try:
                _raw_sock_recv(self._sockets[0], 64)
            except (BlockingIOError, OSError):
                pass
        else:
            try:
                _raw_os_read(self.r, 64)
            except (BlockingIOError, OSError):
                pass

    def unpark(self):
        if self._via_park:
            # In-memory parker: no fd to write -- re-queue the fiber directly.
            # Defensive: _via_park parkers are normally woken via _unpark_all's
            # batched g.wake path, but a direct unpark() must still work.
            h = self._g_handle
            if h is not None:
                h.wake()
            return
        if self._sockets is not None:
            try:
                _raw_sock_send(self._sockets[1], b"\x01")
            except (BlockingIOError, BrokenPipeError, OSError):
                pass
        else:
            try:
                _raw_os_write(self.w, b"\x01")
            except (BlockingIOError, BrokenPipeError, OSError):
                pass

    def release(self):
        # Drop the fiber handle (and its g incref): the wait is over, so this
        # parker is no longer wakeable.  Also a stale-handle guard for the pooled
        # fd-tuple's next user.
        self._g_handle = None
        if self._via_park:
            return                     # in-memory parker: no fd to drain / pool
        # Drain any stale wake bytes before returning to the pool.  Must
        # use raw recv/read -- the patched versions would park forever
        # on BlockingIOError instead of returning empty.
        if self._sockets is not None:
            try:
                while _raw_sock_recv(self._sockets[0], 64):
                    pass
            except (BlockingIOError, OSError):
                pass
        else:
            try:
                while _raw_os_read(self.r, 64):
                    pass
            except (BlockingIOError, OSError):
                pass
        # Non-blocking try: if another fiber is in release() concurrently,
        # skip pooling and close the FDs.  acquire(False) never blocks, so the
        # hub is never frozen even if sysmon preempts the lock holder.
        pooled = False
        if _Parker._pool_lock.acquire(False):
            try:
                if len(_Parker._pool) < 64:
                    _Parker._pool.append((self.r, self.w, self._sockets))
                    pooled = True
            finally:
                _Parker._pool_lock.release()
        if not pooled:
            if self._sockets is not None:
                for s in self._sockets:
                    try: s.close()
                    except OSError: pass
            else:
                try: os.close(self.r)
                except OSError: pass
                try: os.close(self.w)
                except OSError: pass


def _unpark_all(parkers):
    """Wake a batch of waiters (the fan-in wake side of Event/Condition/
    Semaphore).  GOROUTINE waiters are woken by ONE batched runloom_c.unpark_many
    (a direct claim+re-queue per g, no per-waiter os.write -> epoll -> drain
    round-trip -- ~85% of the fan-in cost at scale); FOREIGN-thread waiters (no
    fiber handle) keep the os.write path.  unpark_many returns the indices it
    could not direct-wake -- a waiter that appended itself but has not yet
    committed its wait_fd park (the edge-before-park window) -- and those fall
    back to the pipe-write backstop so the wake is never lost.

    IN-MEMORY (park()-based, 0-fd) waiters are re-queued via g.wake() -- the only
    wake that works for them (no fd to write) and the only one safe for a FOREIGN
    SETTER (g.wake = wake_safe + pump kick; the waiter's foreign_wakeable park
    armed the run-alive anchor so run() can't exit out from under it).  The
    park_generic Dekker makes a g.wake() that beats the park commit a no-op, so no
    backstop is needed for these."""
    inmem_gs = None
    rest = None
    for p in parkers:
        if p._via_park:
            h = p._g_handle
            if h is not None:
                if inmem_gs is None:
                    inmem_gs = []
                inmem_gs.append(h)
        else:
            if rest is None:
                rest = []
            rest.append(p)
    if inmem_gs:
        for h in inmem_gs:                    # in-memory: g.wake() (foreign-safe)
            h.wake()
    if not rest:
        return
    if not _in_fiber():
        # FOREIGN OS-thread setter (e.g. Thread.start()'s self._started.set()
        # from the real worker thread, or a multiprocessing feeder thread):
        # a direct unpark_many here is NOT race-safe.  unpark_many unlinks the
        # parker (netpoll_parked--) then re-queues the g, but on a foreign thread
        # those are not serialized against run()'s drain loop -- it can observe
        # netpoll_parked==0 with the ready-push not yet visible and EXIT, dropping
        # the wake.  os.write IS race-free: the pump does the unlink+wake on
        # run()'s OWN thread.  (This is the whole reason FD waiters park on an fd --
        # see CoEvent's class comment / chan_waiters.c.inc park_waiter FINDING.)
        for p in rest:
            p.unpark()
        return
    gor = None
    gor_parkers = None
    for p in rest:
        h = p._g_handle
        if h is not None:
            if gor is None:
                gor = []
                gor_parkers = []
            gor.append(h)
            gor_parkers.append(p)
        else:
            p.unpark()                        # foreign thread: one os.write
    if gor:
        missed = runloom_c.unpark_many(gor)   # one C call for all fibers
        for idx in missed:
            gor_parkers[idx].unpark()         # edge-before-park byte backstop


# ============================================================
# blocking-call backend (files, disk syscalls, any non-pollable I/O)
#
# Per-OS slot.  Today: thread pool everywhere.  io_uring backend on
# Linux 5.6+ can slot in with no caller-side changes: backends only
# need to expose submit(fn, args, kwargs) -> result and a fini().
# ============================================================
_real_Lock_for_backend      = _th.Lock         # captured before any patch
_real_Condition_for_backend = _th.Condition
# The raw C SimpleQueue (the `_queue` module attribute is never monkey-patched;
# only `queue.SimpleQueue` is swapped for the cooperative shim).  Its put() is
# a single non-blocking atomic C call and get() blocks in C.
import _queue as _real_queue_mod              # noqa: E402
_real_SimpleQueue_for_backend = _real_queue_mod.SimpleQueue
_BACKEND_SHUTDOWN = object()


class _BlockingBackend(object):
    name = "abstract"
    def submit(self, fn, args, kwargs):
        raise NotImplementedError
    def fini(self):
        pass


class _ThreadPoolBackend(_BlockingBackend):
    """Pre-started worker pool.  Each submitted task gets a self-pipe (from the
    Parker pool) for wakeup -- the fiber parks on wait_fd, the worker
    writes a byte when done.

    Uses a raw C `_queue.SimpleQueue` (never monkey-patched) as the job queue.
    Why not a Lock/Condition + deque: a fiber that took a real
    threading.Lock here could be PREEMPTED (sysmon) while holding it, which
    parked it; every sibling on its hub then trying submit() would block the
    hub's OS thread on Lock.acquire() -- a real lock blocks the whole thread --
    waiting on a lock held by a fiber only that frozen hub can resume.
    Under heavy offload every hub froze simultaneously (big_100 BUG #4).
    SimpleQueue.put() is a single atomic C call; runloom only preempts at
    Python frame boundaries, so a fiber can never be parked mid-put
    holding the queue's internal lock.  Workers block in .get() on a real OS
    thread -- exactly the point of the pool."""
    name = "thread-pool"

    def __init__(self, size=None):
        if size is None:
            try:
                size = min(8, (os.cpu_count() or 4))
            except Exception:
                size = 4
        self.size = max(1, size)
        # SHARDED job queues -- ONE raw C SimpleQueue per worker, NOT one shared
        # queue.  Raw C SimpleQueue (never monkey-patched): its put() is a single
        # atomic C call, so a fiber can never be PREEMPTED mid-put holding the
        # queue lock and freeze its hub (the old Lock/Condition+deque freeze,
        # big_100 BUG #4); workers block in get() on a real OS thread.
        #
        # WHY SHARDED (the p23/p17 @1M wedge): under FREE-THREADED CPython a
        # SimpleQueue's per-object `Py_BEGIN_CRITICAL_SECTION` PyMutex CONVOYS
        # when many threads put/get the SAME object.  At ~1M concurrent offloads,
        # 8 hubs (put) + N workers (get) all contend one mutex; the loser threads
        # `_PyParkingLot_Park` on its futex -- and a PARKED HUB OS THREAD is OFF
        # the scheduler, so it stops pumping netpoll, so FD-mode offload-
        # completion self-pipe wakes are never delivered -> ~1M fibers strand ->
        # total wedge (gdb: 18/19 threads in futex on the SimpleQueue mutex).
        # One queue per worker spreads put/get across N distinct critical
        # sections (each shard: 1 worker get + the hubs that hash to it), so no
        # single mutex is contended by every thread and the convoy can't form.
        self._qs = [_real_SimpleQueue_for_backend() for _ in range(self.size)]
        self._started = self.size
        # Start workers eagerly in __init__ -- we are not yet inside any
        # fiber (the backend is built on first offload, called from the
        # scheduler root fiber), so there is no concurrent fiber racing on
        # _started.  Each worker owns shard i.
        for i in range(self.size):
            _thread.start_new_thread(self._worker_loop, (i,))

    def _worker_loop(self, shard):
        q = self._qs[shard]               # this worker's own shard queue
        while True:
            item = q.get()                 # real C blocking get on a real thread
            if item is _BACKEND_SHUTDOWN:
                return
            fn, args, kwargs, box, parker = item
            try:
                box[0] = fn(*args, **kwargs)
            except BaseException as e:
                box[1] = e
            # Publish the done flag BEFORE the wake so a submit() that
            # observes box[2] (with or without parking) always sees the
            # result/exception too.
            box[2] = True
            parker.unpark()

    def submit(self, fn, args, kwargs):
        if kwargs is None:
            kwargs = {}
        # Parker mode.  DEFAULT = FD-mode (pipe + netpoll): correct but each
        # offload churns ~10 syscalls, so throughput caps near ~52K/s under high
        # concurrency.  RUNLOOM_BLOCKPOOL_INMEM=1 opts into an inmem parker (0 fds,
        # woken by g.wake()) -> ~169K/s (3.2x), BUT an inmem park is NOT on the
        # netpoll, so it loses the wait_fd signal-interrupt delivery: a signal
        # handler that RAISES while parked on an offloaded blocking call no longer
        # propagates out of the call (breaks EINTR-during-offload semantics, e.g.
        # test_select_interrupt_exc).  So inmem stays opt-in for offload-heavy
        # workloads that don't rely on signal-interrupting a blocking offload.
        p = _Parker(inmem=(os.environ.get("RUNLOOM_BLOCKPOOL_INMEM") == "1"))
        # box = [result, exception, done].  The done flag is essential:
        # a pooled _Parker can carry a stale wake byte and runloom_c.wait_fd
        # can wake spuriously, so a single park() may return BEFORE the
        # worker has stored the result -- which made submit() return box[0]
        # (None) intermittently under M:N.  Looping until done makes the
        # wakeup edge-insensitive: we only return once the worker actually
        # finished (any stale byte is then drained by release()).
        box = [None, None, False]
        # Shard by OS-thread (hub) id -- stateless, so it adds no shared counter
        # to convoy on.  Spreads puts across the per-worker queues so no single
        # SimpleQueue critical section is hammered by every hub (see __init__).
        self._qs[_thread.get_ident() % self.size].put(
            (fn, args, kwargs, box, p))            # atomic C call; never freezes the hub
        try:
            while not box[2]:
                p.park()
        finally:
            p.release()
        if box[1] is not None:
            raise box[1]
        return box[0]

    def fini(self):
        # One shutdown sentinel per worker, on that worker's OWN shard queue.
        for q in self._qs:
            q.put(_BACKEND_SHUTDOWN)


_backend = None


def _select_backend():
    """Pick the fastest available blocking backend for this OS.

    Today: thread pool everywhere.  Linux io_uring (5.6+) slots in by
    returning an _IoUringBackend() here when available.  The submit()
    signature stays the same so no call site needs to change."""
    return _ThreadPoolBackend()


def _get_backend():
    global _backend
    if _backend is None:
        _backend = _select_backend()
    return _backend


def _after_fork_child():
    """Drop scheduler-derived state the child can't reuse after os.fork().

    fork() copies only the forking thread, so the thread-pool backend's
    workers are gone in the child and its self-pipe parkers are shared with
    the parent.  Null the backend so it is rebuilt on first use and drop the
    pooled parkers so the child never writes wake bytes into the parent's
    pipes.  Cheap and only runs on an actual fork."""
    global _backend
    _backend = None
    _Parker._pool = []
    _Parker._pool_lock = _thread.allocate_lock()   # fresh lock; fork may have copied a held one


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork_child)


def _blocking_call(fn, *args, **kwargs):
    """Run fn(*args, **kwargs) off-scheduler.  In a fiber, dispatch
    to the backend (other fibers keep running).  Outside a
    fiber, call inline -- no dispatch overhead."""
    if not _in_fiber():
        return fn(*args, **kwargs)
    return _get_backend().submit(fn, args, kwargs)


def offload(fn, *args, **kwargs):
    """Run a blocking callable on the backend thread pool, parking the current
    fiber until it returns (run inline when not in a fiber).

    The sanctioned escape hatch for blocking calls runloom cannot transparently
    make cooperative: buffered file .read()/.write() on slow media (io.FileIO /
    io.Buffered* are immutable C types and can't be patched), C-extension
    database drivers, CPU-bound hashing/compression, etc.

        data = runloom.monkey.offload(f.read, 65536)

    sqlite3 is NOT auto-patched: a Connection is thread-affine (it raises
    "SQLite objects created in a thread can only be used in that same thread"),
    and the pool runs the call on a worker thread.  To offload sqlite, open the
    connection with ``check_same_thread=False`` and keep your own access to it
    serialized -- one in-flight DB call at a time per connection::

        conn = sqlite3.connect(path, check_same_thread=False)
        rows = runloom.monkey.offload(conn.execute, sql, params).fetchall()
    """
    return _blocking_call(fn, *args, **kwargs)



def _co_sleep(seconds):
    """Cooperative sleep that dispatches to whichever scheduler is live.

    Inside the C scheduler (thread-local count > 0) call
    runloom_c.sched_sleep directly -- runloom.sleep there would route to
    the Python scheduler, see no current fiber, and call time.sleep
    again (us), recursing.  Inside the Python scheduler use runloom.sleep.
    """
    if getattr(_g_state, "count", 0) > 0:
        runloom_c.sched_sleep(seconds)
    else:
        runloom.sleep(seconds)


# Re-export every name defined above (stdlib aliases, constants, helpers, the
# Parker and the backend) so a section module gets the whole foundation with a
# single `from ._base import *`.  Underscore names are included on purpose.
__all__ = [name for name in list(globals()) if not name.startswith("__")]

