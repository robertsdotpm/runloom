"""runloom runtime: scheduler + goroutine helpers.

This module delegates to the C scheduler (runloom_c.go / .run / .sched_*)
for all goroutine state management.  An earlier version implemented the
scheduler in Python on top of raw runloom_c.Coro -- that worked for one
goroutine at a time but tangled Python's tstate.cframe chain across
multiple concurrent goroutines, crashing the process on Windows Fibers.
The C scheduler does per-g tstate snapshots which is the only correct
way to multiplex Python frames across stacks.

The public API (runloom.go, .yield_, .sleep, .run, .current) is preserved
so existing code keeps working.
"""
import sys
import time
import runloom_c


_prewarmed = False


def gil_enabled():
    """True if the GIL is active in this interpreter.

    Only a free-threaded ("t") build run with the GIL off (e.g. 3.13t under
    PYTHON_GIL=0) returns False -- and that is the one configuration where
    run(n > 1) gives real multi-core parallelism.  On every other build the
    GIL is always on (pre-3.13 has no toggle), so we report True.  Checked
    at run() time, not import time, because a C extension can re-enable the
    GIL on a "t" build after start-up."""
    is_enabled = getattr(sys, "_is_gil_enabled", None)
    if is_enabled is None:
        return True
    return is_enabled()


def prewarm_stdlib():
    """Resolve lazy, deep, *synchronous* stdlib imports on the main
    thread's (large) stack once, before any goroutine runs on a small
    stack.

    The motivating case: ``socket.getaddrinfo``'s first call lazily
    imports an ``encodings`` codec through the import machinery
    (``encodings.__init__.search_function`` -> ``importlib._bootstrap``).
    That is a deep, non-yielding C-stack burst -- and because it never
    yields, the goroutine copy-grow path (which only grows at yield
    points) cannot rescue it; a small-stack goroutine that hit it cold
    would overflow into the guard page and die.  Resolving it here caches
    the codec + bootstrap state process-wide, so the path a goroutine
    later takes through ``getaddrinfo`` is shallow.

    Idempotent and best-effort: never raises into the caller.  Called
    from runloom.run() and the aio loop before the scheduler drives any
    goroutine.  This is the enabler for small default goroutine stacks."""
    global _prewarmed
    if _prewarmed:
        return
    _prewarmed = True
    try:
        import codecs
        # Hostname/text codecs getaddrinfo + str.encode reach for.
        for _name in ("idna", "utf-8", "ascii", "latin-1", "utf-16"):
            try:
                codecs.lookup(_name)
            except LookupError:
                pass
    except Exception:
        pass
    try:
        # Exercise the actual deep path once so any remaining lazy
        # imports it triggers are cached on the big stack.
        import socket
        socket.getaddrinfo("127.0.0.1", 0, socket.AF_UNSPEC,
                           socket.SOCK_STREAM)
    except Exception:
        pass


class Goroutine(object):
    """Public-facing handle for a spawned goroutine.

    Backed by a runloom_c.G; the .coro property exposes the underlying
    coroutine.  Compat: older code reads .coro.done, .coro.result, etc."""
    __slots__ = ("_g", "name")

    def __init__(self, g, name=None):
        self._g = g
        self.name = name or "goroutine"

    @property
    def done(self):
        return self._g.done

    @property
    def result(self):
        return self._g.result

    @property
    def coro(self):
        # Compat shim: callers used to do `g.coro.done` / `g.coro.result`.
        # The C-scheduler G already exposes these directly, so we just
        # forward via self.
        return self

    def __repr__(self):
        return "<Goroutine {0} done={1}>".format(self.name, self.done)


def go(callable_, *args, **kwargs):
    """Spawn a goroutine.  Mirrors Go's `go fn(a, b)`: schedules
    fn(*args, **kwargs) to run cooperatively and returns immediately.

    Dispatches on the active scheduler so the same call works in both modes:
      - single-thread (run_single / run(1, ...)): spawns on this thread's
        scheduler and returns a Goroutine handle.
      - M:N (run(n > 1, ...)): spawns onto a hub via mn_go.  M:N v1 is
        run-to-completion with no join handle, so this returns None.

    The dispatch uses runloom_c.mn_hub_count() rather than a mode flag, so a
    go() called from anywhere -- inside a hub goroutine or from the main
    thread while hubs run -- routes correctly (mn_go round-robins a non-hub
    caller).  Spawning via the plain scheduler inside a hub would skip the
    M:N pending-counter accounting that mn_run() joins on, so this dispatch
    is required for correctness, not just convenience.
    """
    if args or kwargs:
        target = lambda: callable_(*args, **kwargs)
        target.__name__ = getattr(callable_, "__name__", "goroutine")
    else:
        target = callable_
    if runloom_c.mn_hub_count() > 0:
        runloom_c.mn_go(target)
        return None
    g = runloom_c.go(target)
    return Goroutine(g, name=getattr(target, "__name__", "goroutine"))


def yield_():
    """Cooperative yield -- equivalent to runtime.Gosched()."""
    runloom_c.sched_yield_classic()


def sleep(seconds):
    """Sleep without blocking the OS thread (other goroutines run).

    Outside a goroutine, falls back to time.sleep so callers can use the
    same name in either context."""
    if runloom_c.current_g() is None:
        time.sleep(seconds)
        return
    runloom_c.sched_sleep(seconds)


def blocking(fn, *args, **kwargs):
    """Run a blocking call without wedging the goroutine's OS thread.

    Offloads fn(*args, **kwargs) to a thread pool and parks the calling
    goroutine until it returns, so a non-preemptible blocking call (DNS,
    blocking sockets/files, a GIL-releasing C extension) doesn't strand the
    other goroutines sharing its hub.  fn runs off any goroutine and must
    not call runloom scheduler ops (yield/sleep/channels/wait_fd).

    Delegates to runloom_c.blocking, which runs fn inline when the caller
    isn't on a goroutine -- so the same call is safe in either context."""
    return runloom_c.blocking(fn, *args, **kwargs)


def current():
    """Return the currently-running Goroutine handle, or None.

    Note: returns the bare runloom_c.G handle, not the Python wrapper.
    Existing callers only check for None-ness or compare identity, so
    this is backwards-compatible enough."""
    return runloom_c.current_g()


def run_single(main_fn=None):
    """Drive the single-thread (M:1) scheduler until idle.

    Many goroutines multiplexed onto ONE OS thread: overlapping in-flight
    I/O, but never two goroutines running Python at the same instant (the
    asyncio / Go-with-GOMAXPROCS=1 model).  If main_fn is given it's spawned
    first -- the moral equivalent of Go's `func main()`; otherwise the
    scheduler just drains goroutines you've already go()'d.

    run_single(main_fn) is exactly run(1, main_fn); it's the name to reach
    for when you never want parallelism and would rather not pass a count.
    Returns the number of goroutines completed.
    """
    prewarm_stdlib()
    if main_fn is not None:
        go(main_fn)
    return runloom_c.run()


def run(n, main_fn):
    """Run main_fn on the scheduler with n OS-thread hubs, then drain.

        n == 1   single-thread (M:1): cooperative concurrency, no two
                 goroutines run Python at once.  Same as run_single(main_fn).
        n  > 1   M:N: goroutines spread across n hub threads with the GIL
                 off -> real multi-core parallelism.  REQUIRES a free-threaded
                 build (CPython 3.13t, PYTHON_GIL=0); n > 1 with the GIL on
                 raises rather than silently running serially.

    n is a required, explicit argument on purpose.  M:N is not just a faster
    run() -- it is a different correctness model: goroutines execute Python
    in parallel, so shared state that is safe under M:1 can race.  You opt
    into that by typing the number, never by accident or by which interpreter
    happens to be free-threaded.

    main_fn is the root goroutine; it may go() more (those dispatch to the
    hubs automatically).  This single call collapses the raw
    mn_init / mn_go / mn_run / mn_fini envelope.  Returns the number of
    goroutines completed.
    """
    if isinstance(n, bool) or not isinstance(n, int) or n < 1:
        raise ValueError(
            "run(n, main_fn): n must be an int >= 1 (got {0!r})".format(n))
    if not callable(main_fn):
        raise TypeError(
            "run(n, main_fn): main_fn must be callable (got {0!r}); for a "
            "drain-only single-thread run use run_single()".format(main_fn))
    if n == 1:
        return run_single(main_fn)
    if gil_enabled():
        raise RuntimeError(
            "run(n={0}) needs a free-threaded build with the GIL off, but "
            "the GIL is enabled. Use n=1 for single-thread, or run on "
            "CPython 3.13t with PYTHON_GIL=0 for real M:N parallelism."
            .format(n))
    prewarm_stdlib()
    runloom_c.mn_init(n)
    try:
        runloom_c.mn_go(main_fn)
        return runloom_c.mn_run()
    finally:
        runloom_c.mn_fini()
