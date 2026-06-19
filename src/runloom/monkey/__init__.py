"""runloom monkey-patches for blocking Python APIs.

Replaces stdlib calls that would block the OS thread with versions that
park the current fiber via runloom_c.wait_fd / runloom.sleep / a
self-pipe parker.  Other fibers keep running while one is "blocked".

Apply once at startup:
    import runloom, runloom.monkey
    runloom.monkey.patch()                      # all categories
    runloom.monkey.patch(threading=False)       # opt out of one

Categories (all default True):
    socket       socket.socket recv/recv_into/send/sendall/accept/connect/
                 recvfrom/recvfrom_into/sendto/sendfile  +  recvmsg/
                 recvmsg_into/sendmsg (fd passing, ancillary data) where the
                 platform provides them.  sendfile reimplements the stdlib's
                 zero-copy os.sendfile fast path + read()/send() fallback,
                 parking on wait_fd instead of a selector.
    time         time.sleep
    os           os.read / os.write / os.readv / os.writev -- wait_fd for
                 pollable fds (pipes, sockets, ttys), thread-pool offload for
                 regular files
    select       select.select  (fast path for 1 fd; busy-poll otherwise)
    selectors    select.poll / select.epoll / select.kqueue made cooperative,
                 which transparently makes the high-level `selectors` module
                 (DefaultSelector / PollSelector / EpollSelector /
                 KqueueSelector) cooperative too -- this is what
                 subprocess.communicate(), socketserver, http.server, wsgiref
                 and most hand-rolled poll loops actually block on.  epoll /
                 kqueue wait on their own backing fd via wait_fd (event-driven,
                 no busy-poll); poll has no backing fd so it probe+yields.
    stdio        builtins.input  +  sys.stdin.read/readline
    ssl          ssl.SSLSocket recv/send/sendall/do_handshake
    subprocess   subprocess.Popen.wait  (and, via `selectors` + `os`,
                 subprocess.run / call / check_output / communicate).  On Linux
                 5.3+ it parks on a pidfd (event-driven) instead of busy-poll.
    process      os.waitpid / os.wait / os.waitid / os.wait3 / os.wait4: park
                 on a pidfd until the child exits, then reap WNOHANG (busy-poll
                 fallback for pid<=0, stop/continue waits, or no pidfd) +
                 os.system (offload)
    threading    Lock, RLock (+ full _recursion_count/_is_owned/_release_save/
                 _acquire_restore/_at_fork_reinit API), Event, Condition,
                 Semaphore, BoundedSemaphore + Thread.join (cooperative
                 is_alive() poll).  threading.Barrier builds on Condition.
    queue        queue.SimpleQueue -> cooperative CoSimpleQueue.  queue.Queue
                 needs nothing extra: it builds on threading.Condition, which
                 is already cooperative once `threading` is patched.
    futures      concurrent.futures.ThreadPoolExecutor -> fiber-backed
                 (work runs as fibers so Future.result/wait/as_completed
                 resolve in-domain; a real-threaded executor would notify a
                 CoCondition cross-thread and deadlock the cooperative waiter).
    multiprocessing  (1) rebind Connection._recv/_send/_close's captured
                 os.read/os.write/os.close defaults to the cooperative versions,
                 so Pipe/Queue/Pool/Process.join cooperate regardless of import
                 order; (2) SemLock.acquire -> sem_trywait + backoff park, so
                 Lock/Semaphore/Event/Condition/Barrier cooperate under
                 contention.  POSIX.  Use forkserver/spawn -- "fork" inherits
                 runloom's threads and can deadlock the child.
    file         builtins.open (open syscall offloaded to backend)
    syscalls     os.stat/lstat/listdir/scandir/mkdir/rename/unlink/fsync/
                 splice/copy_file_range/... -- disk / zero-copy os.* calls
                 dispatched to the backend pool
    fcntl        fcntl.flock / fcntl.lockf -- non-blocking acquire + _co_sleep
                 backoff park (no readiness fd for a file lock)
    signal       signal.sigwait / sigwaitinfo / sigtimedwait (poll a zero-
                 timeout sigtimedwait) + signal.pause (set_wakeup_fd self-pipe
                 on the main thread, else offload)
    heavy        size-gated auto-offload of CPU-bound stdlib C calls --
                 hashlib.sha*/md5/blake2 + zlib/gzip/bz2/lzma compress/
                 decompress above RUNLOOM_OFFLOAD_BYTES (default 256 KiB), KDFs
                 (pbkdf2_hmac/scrypt) always.  A tight C loop has no yield point
                 and can't be preempted, so the only fix is to relocate it to
                 the pool; the size gate keeps small calls inline (zero cost).
    dns          pure-async UDP resolver (Go-netgo-style): parses
                 /etc/resolv.conf + /etc/hosts, sends queries via
                 cooperatively-patched UDP sockets, parallel A/AAAA,
                 60s result cache.  No threads.
    compile      builtins.compile offloaded to the pool when called inside a
                 fiber (covers ast.parse + source imports too).  compile's
                 ~1.5 KB/level recursion on nested source overflows a 32 KB
                 g-stack before CPython's recursion counter fires; the pool
                 thread's full-size stack runs it safely.  No-op on the main
                 thread (where compiles normally happen).

Backend layer:
    The non-pollable I/O patches (file, syscalls, os.read/write on
    regular files) dispatch through runloom.monkey._get_backend(), which
    today returns a pre-started thread pool with self-pipe wakeup.
    Linux io_uring (5.6+) can slot in here without caller changes --
    backends only expose submit(fn, args, kwargs).

Limitations:
    * Designed for the C scheduler (runloom_c.fiber / runloom_c.run).  The
      pure-Python scheduler in runloom.runtime has no netpoll integration.
    * select.select with >1 fd, and select.poll(), are yield-backoff
      busy-polls (no backing fd to park on).  epoll/kqueue ARE event-driven
      (they park on their own fd).
    * Replacing threading.Lock etc. is best-effort coordination with real
      OS threads -- the single-thread cooperative model is the design target.
    * `queue.Queue` / `queue.SimpleQueue` and `selectors.*Selector` instances
      created before patch() keep the original (blocking) primitives; patch()
      early.
    * Buffered file .read()/.write() can't be made cooperative: io.FileIO and
      io.Buffered* are immutable C types (their methods are unassignable), so
      a blocking buffered read on slow media (NFS/FUSE/cold spindle) or a
      pipe stream (proc.stdout.read(), os.popen()) still stalls the scheduler.
      Use os.read/os.write on the raw fd (cooperative), or runloom.monkey.offload()
      for the blocking call.  open() itself IS offloaded.
    * concurrent.futures.ProcessPoolExecutor is not supported: its result is
      delivered by an internal manager *thread* coordinating worker processes
      over multiprocessing queues, and that real-thread/forkserver machinery is
      nondeterministic under the cooperative scheduler.  Use the (fiber-
      backed) ThreadPoolExecutor, or drive multiprocessing directly.

Platform notes:
    * Linux, macOS, *BSD: fully supported by the C-side netpoll (epoll,
      kqueue, select fallback).
    * Windows: the Python monkey-patch layer is Windows-aware -- Parker
      uses socket.socketpair() (the only thing Win select() will poll),
      subprocess.Popen.wait uses portable Popen.poll(), DNS falls back
      to libc getaddrinfo via the backend pool because Windows has no
      /etc/resolv.conf, hosts file resolves to
      %SystemRoot%\\System32\\drivers\\etc\\hosts.  The C extension itself
      still needs Windows support (IOCP backend) before any of this is
      usable end-to-end on Windows; that's separate from this module.
"""

from ._base import *  # noqa: F401,F403  (stdlib re-exports, Parker, backend, offload)

# Cooperative-primitive classes and the per-category patchers, re-exported so
# `runloom.monkey.CoLock` etc. keep working and patch()/unpatch() below can
# dispatch to each section module.
from .timers import _patch_time, _unpatch_time
from .sockets import _patch_socket, _unpatch_socket
from .osio import (_patch_os, _unpatch_os, _patch_stdio, _unpatch_stdio,
                   _patch_getpass, _unpatch_getpass)
from .files import (_patch_file, _unpatch_file, _patch_syscalls,
                    _unpatch_syscalls, _patch_fcntl, _unpatch_fcntl)
from .polling import (_patch_select, _unpatch_select,
                      _patch_selectors, _unpatch_selectors)
from .tls import _patch_ssl, _unpatch_ssl
from .subproc import (_patch_subprocess, _unpatch_subprocess,
                      _patch_process, _unpatch_process,
                      _HAVE_PIDFD, _pidfd_open)
from .signals import _patch_signal, _unpatch_signal
from .locks import CoLock, CoRLock
from .events import (CoEvent, CoCondition, CoSemaphore, CoBoundedSemaphore,
                     _patch_threading, _unpatch_threading)
from .queues import CoSimpleQueue, _patch_queue, _unpatch_queue
from .executors import (_patch_futures, _unpatch_futures,
                        _patch_multiprocessing, _unpatch_multiprocessing)
from .dns import _patch_dns, _unpatch_dns
from .heavy import (_patch_heavy, _unpatch_heavy,
                    _patch_compile, _unpatch_compile)

# Section modules, kept as objects so any internal name a caller reads for
# (runloom.monkey._orig_recv, _dns_result_cache, _patched_send, ...) resolves
# *live* through __getattr__ below -- this module used to be one flat file and
# tools/tests read its internals directly.
from . import (_base, timers, sockets, osio, files, polling, tls, subproc,
               signals, locks, events, queues, executors, dns, dns_proto, heavy)

_SECTIONS = (_base, timers, sockets, osio, files, polling, tls, subproc,
             signals, locks, events, queues, executors, dns, dns_proto, heavy)


def __getattr__(name):
    """Resolve a section-internal name against the submodules, live.

    PEP 562 hook: only called for names not already bound on the package, so the
    public API and patchers above win.  Reaching here returns the section's
    current binding, so a patch-time-rebound original (_orig_*) reads back its
    live value -- preserving the old flat module's read surface.

    (This is a module-level function on purpose: swapping the module's __class__
    to a ModuleType subclass with __getattr__ segfaults the C scheduler when the
    lookup happens inside a fiber.  To *write* a section internal -- the
    fault-injection tests do -- assign to the section module directly, e.g.
    runloom.monkey.files._orig_flock, which is also where the patched code reads.)"""
    for section in _SECTIONS:
        try:
            return getattr(section, name)
        except AttributeError:
            continue
    raise AttributeError("module %r has no attribute %r" % (__name__, name))


# ============================================================
# top-level patch() / unpatch()
# ============================================================
_orig_runloom_c_fiber = None
_orig_runloom_c_mn_fiber = None


def _patched_runloom_c_fiber(fn, stack_size=0, **kwargs):
    # runloom.runtime.fiber() calls runloom_c.fiber(target, stack_size) with the
    # stack size as a 2nd POSITIONAL arg, so this wrapper must accept and
    # forward it.  A bare (fn, **kwargs) signature raised TypeError and broke
    # every runloom.fiber() once monkey.patch() was applied (worst under M:N,
    # where runtime.go always passes the grow-down stack size).
    return _orig_runloom_c_fiber(_wrap_fiber_callable(fn), stack_size, **kwargs)


def _patched_runloom_c_mn_fiber(fn, stack_size=0, **kwargs):
    return _orig_runloom_c_mn_fiber(_wrap_fiber_callable(fn), stack_size,
                                 **kwargs)


def _install_fiber_wrapper():
    """Wrap runloom_c.fiber / mn_fiber so user callables run with the
    fiber-context flag set.  Idempotent."""
    global _orig_runloom_c_fiber, _orig_runloom_c_mn_fiber
    if _orig_runloom_c_fiber is None:
        _orig_runloom_c_fiber = runloom_c.fiber
        runloom_c.fiber = _patched_runloom_c_fiber
    if _orig_runloom_c_mn_fiber is None:
        _orig_runloom_c_mn_fiber = runloom_c.mn_fiber
        runloom_c.mn_fiber = _patched_runloom_c_mn_fiber


def _uninstall_fiber_wrapper():
    global _orig_runloom_c_fiber, _orig_runloom_c_mn_fiber
    if _orig_runloom_c_fiber is not None:
        runloom_c.fiber = _orig_runloom_c_fiber
        _orig_runloom_c_fiber = None
    if _orig_runloom_c_mn_fiber is not None:
        runloom_c.mn_fiber = _orig_runloom_c_mn_fiber
        _orig_runloom_c_mn_fiber = None


_DEFAULTS = ("socket", "time", "os", "select", "selectors", "stdio", "getpass",
             "ssl", "subprocess", "process", "threading", "queue", "futures",
             "multiprocessing", "file", "syscalls", "fcntl", "signal",
             "heavy", "compile", "dns")

_PATCHERS = {
    "socket":     (_patch_socket,     _unpatch_socket),
    "time":       (_patch_time,       _unpatch_time),
    "os":         (_patch_os,         _unpatch_os),
    "select":     (_patch_select,     _unpatch_select),
    "selectors":  (_patch_selectors,  _unpatch_selectors),
    "stdio":      (_patch_stdio,      _unpatch_stdio),
    "getpass":    (_patch_getpass,    _unpatch_getpass),
    "ssl":        (_patch_ssl,        _unpatch_ssl),
    "subprocess": (_patch_subprocess, _unpatch_subprocess),
    "process":    (_patch_process,    _unpatch_process),
    "threading":  (_patch_threading,  _unpatch_threading),
    "queue":      (_patch_queue,      _unpatch_queue),
    "futures":    (_patch_futures,    _unpatch_futures),
    "multiprocessing": (_patch_multiprocessing, _unpatch_multiprocessing),
    "file":       (_patch_file,       _unpatch_file),
    "syscalls":   (_patch_syscalls,   _unpatch_syscalls),
    "fcntl":      (_patch_fcntl,      _unpatch_fcntl),
    "signal":     (_patch_signal,     _unpatch_signal),
    "heavy":      (_patch_heavy,      _unpatch_heavy),
    "compile":    (_patch_compile,    _unpatch_compile),
    "dns":        (_patch_dns,        _unpatch_dns),
}

_applied = set()


def patch(**flags):
    """Apply runloom monkey-patches.  Idempotent.

    All categories default to True.  Pass keyword False to opt out:
        runloom.monkey.patch(threading=False, dns=False)

    Categories: socket, time, os, select, selectors, stdio, ssl,
    subprocess, process, threading, queue, futures, multiprocessing, file,
    syscalls, fcntl, signal, dns.  See module docstring.
    """
    unknown = set(flags) - set(_PATCHERS)
    if unknown:
        raise TypeError("patch() got unknown category: " +
                        ", ".join(sorted(unknown)))
    _install_fiber_wrapper()
    # Threading must come before queue (queue is a no-op but kept for
    # symmetry); socket has to come before dns (dns wraps socket fns).
    order = list(_DEFAULTS)
    for name in order:
        if not flags.get(name, True):
            continue
        if name in _applied:
            continue
        _PATCHERS[name][0]()
        _applied.add(name)


def unpatch(**flags):
    """Reverse patches.  Without args, reverses every applied category."""
    unknown = set(flags) - set(_PATCHERS)
    if unknown:
        raise TypeError("unpatch() got unknown category: " +
                        ", ".join(sorted(unknown)))
    targets = [n for n in _DEFAULTS if flags.get(n, True)] if flags \
              else list(_DEFAULTS)
    for name in reversed(targets):
        if name not in _applied:
            continue
        _PATCHERS[name][1]()
        _applied.discard(name)
    if not _applied:
        _uninstall_fiber_wrapper()
