"""runloom runtime: scheduler + goroutine helpers.

This module delegates to the C scheduler (runloom_c.go / .run / .sched_*)
for all goroutine state management.  An earlier version implemented the
scheduler in Python on top of raw runloom_c.Coro -- that worked for one
goroutine at a time but tangled Python's tstate.cframe chain across
multiple concurrent goroutines, crashing the process on Windows Fibers.
The C scheduler does per-g tstate snapshots which is the only correct
way to multiplex Python frames across stacks.

The public API (runloom.go, .yield_now, .sleep, .run, .current) is preserved
so existing code keeps working.
"""
import os
import sys
import time
import runloom_c


_prewarmed = False


# --- function-bound stack grow-down (auto-size-down), default-on under M:N --
#
# Every goroutine reserves a C stack.  A fixed default (512 KiB) is safe but
# wasteful: most goroutines touch only a few KiB, so 99% of the reservation is
# never paged in.  The grow-down learns each function's real need and reserves
# only that, bound to the function itself rather than a side table -- "the
# function IS the database row".
#
# Active under M:N (run(n>1)) only; single-thread run(1) keeps the fixed default
# (see the dispatch in go() for why).
#
# On the first runloom.go(fn, ...) spawn we let the C side use the default
# stack (a "cold start" we know is safe -- the function completes on it).  The
# goroutine measures its real C-stack high-water-mark on return (page-granular,
# paint-free via mincore) and writes a derived, smaller size back onto fn's
# __dict__.  The next spawn of that same function reads it and reserves only
# next_pow2(hwm * MARGIN).  The stored size is the monotone MAX over the first
# GROW_DOWN_SAMPLES measured runs (so one shallow run can't under-provision a
# function that sometimes goes deep), then frozen -- after that, spawning is a
# single dict lookup with zero measurement overhead.
#
# We freeze by SPAWN count, not completion count: a tight loop that spawns a
# burst of goroutines before any of them runs (common single-thread pattern)
# would otherwise wrap and mincore-measure every one of them, since no
# completion ever bumps a completion-based counter mid-burst.  Counting at spawn
# time caps the measured/wrapped goroutines at GROW_DOWN_SAMPLES regardless of
# when they run, so the steady state is always a plain dict lookup.
#
# Safety: the learned size only ever SHRINKS from the cold start, a size that
# already ran the function, so it never reserves more than a known-safe amount.
# The one residual risk -- an input deeper than every sampled run, hitting the
# now-smaller stack -- lands on the PROT_NONE guard page as a clean crash (a
# classified overflow, never silent corruption), and re-learns next process.
# Pin an exact size with runloom.go(fn, stack_size=N) to opt a function out
# entirely; disable globally with RUNLOOM_GROW_DOWN=0 or set_grow_down(False).
#
# Free-threaded note: the learned size lives in a 2-element list on fn.__dict__,
# read/written under free-threaded CPython's per-object locks.  Concurrent
# completions of the same function race only on the max-update; a lost update
# just delays convergence by one spawn (and is bounded below by the guard page),
# so no lock is needed on the hot path.
GROW_DOWN_KEY = "runloom_stack"      # key stamped on each learned callable's __dict__
GROW_DOWN_SAMPLES = 64               # measure this many runs of a function, then freeze
GROW_DOWN_MARGIN = 4                 # reserve next_pow2(measured_hwm * MARGIN)
GROW_DOWN_MIN = 16 * 1024            # never reserve below this (matches C MIN_STACK_SIZE)

grow_down_active = (
    os.environ.get("RUNLOOM_GROW_DOWN", "").strip().lower()
    not in ("0", "off", "false", "no")
)


def set_grow_down(enabled=True):
    """Enable/disable the function-bound stack grow-down auto-sizer.

    On by default, and active under M:N scheduling (run(n>1)) only -- single-
    thread run(1) always uses the fixed default stack.  When off, runloom.go()
    reserves the fixed default stack for every goroutine (no per-function
    learning).  Also settable at import via the RUNLOOM_GROW_DOWN=0 environment
    variable.  Per-call ``stack_size=`` pins always win regardless of this
    setting."""
    global grow_down_active
    grow_down_active = bool(enabled)


def grow_down_enabled():
    """True if the function-bound stack grow-down is currently active."""
    return grow_down_active


def _next_pow2(n):
    if n <= GROW_DOWN_MIN:
        return GROW_DOWN_MIN
    p = GROW_DOWN_MIN
    while p < n:
        p <<= 1
    return p


def grow_down_prepare(real_fn, target):
    """Return (stack_size, target') for a grow-down-managed spawn of real_fn.

    stack_size is 0 (let the C side use its default cold start) until real_fn
    has been measured, then its learned size.  target' is either the original
    target (frozen -- enough samples collected) or a thin wrapper that measures
    the goroutine's stack high-water-mark on return and updates the learned
    size bound to real_fn.  Functions with no writable __dict__ (most C
    builtins) can't carry a learned size, so they fall back to the cold start
    with no wrapper."""
    d = getattr(real_fn, "__dict__", None)
    if d is None:
        return 0, target                      # not introspectable: cold start, no learning
    store = d.get(GROW_DOWN_KEY)
    if store is None:
        store = [0, 0]                         # [learned_size_bytes, spawns_measured]
        try:
            d[GROW_DOWN_KEY] = store
        except (TypeError, AttributeError):
            return 0, target                   # read-only mapping proxy etc.
    size = store[0]                            # 0 on first spawn -> C cold start
    if store[1] >= GROW_DOWN_SAMPLES:
        return size, target                    # frozen: spawn at learned size, no wrapper
    store[1] += 1                              # count at SPAWN time (see freeze note above)

    def measured():
        try:
            return target()
        finally:
            hwm = runloom_c.current_g_hwm()
            if hwm:
                want = _next_pow2(hwm * GROW_DOWN_MARGIN)
                ceiling = runloom_c.get_stack_size()   # known-safe cold start
                if want > ceiling:
                    want = ceiling
                if want > store[0]:
                    store[0] = want

    # Keep the wrapper transparent to the (opt-in) C-side autosizer and to
    # diagnostics: __wrapped__ lets runloom_advice_unwrap see through `measured`
    # to the real callable so its kind/prescan keying still works when both
    # sizers are enabled, and __name__ keeps inspect/dumps readable.
    measured.__wrapped__ = target
    measured.__name__ = getattr(target, "__name__", "goroutine")
    return size, measured


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

    Pass `stack_size=<bytes>` to pin this goroutine's C stack (it is consumed
    here, never forwarded to fn); an explicit size always wins over the
    auto-sizer.  Omit it (the default) to let the default / auto-sizer choose.

    Dispatches on the active scheduler so the same call works in both modes:
      - single-thread (run(1, ...)): spawns on this thread's scheduler and
        returns a Goroutine handle.
      - M:N (run(n > 1, ...)): spawns onto a hub via mn_go.  M:N v1 is
        run-to-completion with no join handle, so this returns None.

    The dispatch uses runloom_c.mn_hub_count() rather than a mode flag, so a
    go() called from anywhere -- inside a hub goroutine or from the main
    thread while hubs run -- routes correctly (mn_go round-robins a non-hub
    caller).  Spawning via the plain scheduler inside a hub would skip the
    M:N pending-counter accounting that mn_run() joins on, so this dispatch
    is required for correctness, not just convenience.
    """
    # stack_size= is OUR keyword (the per-goroutine C stack), not an argument
    # for the target -- pop it before binding args so it pins the goroutine's
    # stack instead of being forwarded into the call.  0 = use the default /
    # auto-sizer.  An explicit value always wins over the auto-sizer.
    stack_size = kwargs.pop("stack_size", 0)
    name = getattr(callable_, "__name__", "goroutine")
    if args or kwargs:
        target = lambda: callable_(*args, **kwargs)
        target.__name__ = name
        # The C auto-sizer keys a goroutine's "kind" (and its crypto/fat-frame
        # prescan) on the callable's code identity.  Without this, every
        # arg-bearing go() would look like the SAME kind -- this wrapper lambda
        # -- so they'd share one stack size and the prescan would scan the
        # wrapper, not the target.  __wrapped__ points the auto-sizer at the
        # real function (runloom_advice_unwrap follows it).
        target.__wrapped__ = callable_
    else:
        target = callable_

    # Function-bound grow-down (default-on under M:N): unless the caller pinned
    # an exact stack_size, learn callable_'s real stack need and reserve only
    # that.  The learned size binds to callable_ (the real function), so every
    # spawn of it -- with or without args -- shares one size.  An explicit
    # stack_size always wins and skips the auto-sizer.  See grow_down_prepare.
    #
    # Restricted to M:N (run(n>1)): single-thread run(1) is the GIL/compat path
    # with modest goroutine counts, where the per-spawn learning is pure overhead
    # (a tight spawn loop runs nothing until it finishes, so the sampler never
    # amortises) and the memory win -- which only pays off at scale -- isn't on
    # the table.  mn_hub_count() also selects the spawn path below, so read once.
    #
    # Defer to the opt-in C autosizer when it's explicitly enabled: it may
    # DELIBERATELY over-reserve (e.g. the crypto/fat-frame prescan reserves 1 MiB
    # by name as a safety margin), and grow-down's measured shrink would silently
    # override that conservative choice.  Two sizers shouldn't fight -- the one
    # the user explicitly turned on wins.
    mn = runloom_c.mn_hub_count()
    if (mn > 0 and stack_size <= 0 and grow_down_active
            and not runloom_c.stack_autosize_enabled()):
        stack_size, target = grow_down_prepare(callable_, target)
    if mn > 0:
        runloom_c.mn_go(target, stack_size)
        return None
    g = runloom_c.go(target, stack_size)
    return Goroutine(g, name=name)


def yield_now():
    """Cooperatively yield the hub so other goroutines run, then resume here.

    The goroutine analogue of ``await asyncio.sleep(0)`` -- a scheduling point
    you drop into a long CPU loop so siblings get a turn.  Equivalent to Go's
    ``runtime.Gosched()``."""
    runloom_c.sched_yield_classic()


# Backwards-compatible alias for the original keyword-dodge name (`yield` is a
# reserved word, hence the historical trailing underscore).  Prefer yield_now().
yield_ = yield_now


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


def run(n, main_fn=None):
    """Run the scheduler on n OS-thread hubs until every goroutine finishes.

    The one and only entry point:

        run(1, main)   single-thread (M:1): cooperative concurrency, no two
                       goroutines run Python at once (asyncio / Go GOMAXPROCS=1).
        run(8, main)   M:N: goroutines spread across 8 hub threads with the GIL
                       off -> real multi-core parallelism.  Needs a free-threaded
                       build (CPython 3.13t, PYTHON_GIL=0); n > 1 with the GIL on
                       raises rather than silently running serial.
        run(n)         main_fn omitted -> just drain goroutines you've already
                       go()'d (n == 1 is the common drain-only case).

    n is required and explicit: M:N is a different correctness model (goroutines
    execute Python in parallel, so shared state can race), opted into by typing
    the number -- never by accident or by which interpreter is free-threaded.
    main_fn, when given, is the root goroutine and may go() more (those dispatch
    to the hubs automatically).  Collapses the raw mn_init / mn_go / mn_run /
    mn_fini envelope.  Returns the number of goroutines completed.
    """
    if isinstance(n, bool) or not isinstance(n, int) or n < 1:
        raise ValueError(
            "run(n, main_fn): n must be an int >= 1 (got {0!r})".format(n))
    if main_fn is not None and not callable(main_fn):
        raise TypeError(
            "run(n, main_fn): main_fn must be callable or None (got {0!r})"
            .format(main_fn))
    if n == 1:
        prewarm_stdlib()
        if main_fn is not None:
            go(main_fn)
        return runloom_c.run()
    if gil_enabled():
        raise RuntimeError(
            "run(n={0}) needs free-threaded CPython with the GIL off "
            "(3.13t + PYTHON_GIL=0) for M:N parallelism, but the GIL is "
            "enabled here.\n"
            "  -> Use run(1, ...) to run single-threaded under the GIL.\n"
            "  -> Or run on CPython 3.13t with PYTHON_GIL=0 for real "
            "multi-core parallelism with n>1."
            .format(n))
    prewarm_stdlib()
    runloom_c.mn_init(n)
    try:
        if main_fn is not None:
            runloom_c.mn_go(main_fn)
        return runloom_c.mn_run()
    finally:
        runloom_c.mn_fini()
