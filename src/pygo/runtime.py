"""pygo runtime: scheduler + goroutine helpers.

This module delegates to the C scheduler (pygo_core.go / .run / .sched_*)
for all goroutine state management.  An earlier version implemented the
scheduler in Python on top of raw pygo_core.Coro -- that worked for one
goroutine at a time but tangled Python's tstate.cframe chain across
multiple concurrent goroutines, crashing the process on Windows Fibers.
The C scheduler does per-g tstate snapshots which is the only correct
way to multiplex Python frames across stacks.

The public API (pygo.go, .yield_, .sleep, .run, .current) is preserved
so existing code keeps working.
"""
import time
import pygo_core


_prewarmed = False


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
    from pygo.run() and the aio loop before the scheduler drives any
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

    Backed by a pygo_core.G; the .coro property exposes the underlying
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
    """Spawn a goroutine.  Returns a Goroutine handle.

    Mirrors Go's `go fn(a, b)`: schedules fn(*args, **kwargs) to run
    cooperatively, returns immediately.
    """
    if args or kwargs:
        target = lambda: callable_(*args, **kwargs)
        target.__name__ = getattr(callable_, "__name__", "goroutine")
    else:
        target = callable_
    g = pygo_core.go(target)
    return Goroutine(g, name=getattr(target, "__name__", "goroutine"))


def yield_():
    """Cooperative yield -- equivalent to runtime.Gosched()."""
    pygo_core.sched_yield_classic()


def sleep(seconds):
    """Sleep without blocking the OS thread (other goroutines run).

    Outside a goroutine, falls back to time.sleep so callers can use the
    same name in either context."""
    if pygo_core.current_g() is None:
        time.sleep(seconds)
        return
    pygo_core.sched_sleep(seconds)


def blocking(fn, *args, **kwargs):
    """Run a blocking call without wedging the goroutine's OS thread.

    Offloads fn(*args, **kwargs) to a thread pool and parks the calling
    goroutine until it returns, so a non-preemptible blocking call (DNS,
    blocking sockets/files, a GIL-releasing C extension) doesn't strand the
    other goroutines sharing its hub.  fn runs off any goroutine and must
    not call pygo scheduler ops (yield/sleep/channels/wait_fd).

    Delegates to pygo_core.blocking, which runs fn inline when the caller
    isn't on a goroutine -- so the same call is safe in either context."""
    return pygo_core.blocking(fn, *args, **kwargs)


def current():
    """Return the currently-running Goroutine handle, or None.

    Note: returns the bare pygo_core.G handle, not the Python wrapper.
    Existing callers only check for None-ness or compare identity, so
    this is backwards-compatible enough."""
    return pygo_core.current_g()


def run(main_fn=None):
    """Drive the scheduler until idle.

    If main_fn is given it's spawned first, so:
        pygo.run(my_main)
    is the moral equivalent of Go's `func main()`.  If you've already
    called pygo.go(...) yourself, pass main_fn=None to just drain.
    """
    prewarm_stdlib()
    if main_fn is not None:
        go(main_fn)
    return pygo_core.run()
