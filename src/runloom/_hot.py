"""``@runloom.hot`` -- mark a hot handler so it scales cleanly across all cores.

The problem it quietly fixes (you don't need the details): when a handler is a
CLOSURE -- it *captures* something, e.g. ``handler = make_app(config)`` -- and the
SAME closure runs flat-out on many cores at once, every core hammers the same
captured slots and they start fighting over them, so adding cores stops helping.
``@runloom.hot`` gives each core its own private copy of those captured slots
(pointing at the same values), so they stop colliding and your scaling returns.

    config = load_config()

    @runloom.hot
    def handle(conn):
        serve(conn, config)          # `config` is captured -> shared across cores

A plain module-level ``def`` that captures nothing already scales perfectly --
there's nothing shared to fight over -- so ``@runloom.hot`` is a safe no-op there
(leave it on, it costs nothing).  When it does kick in it costs a little memory:
one copy of the captured slots **per core**, NOT per fiber.
``runloom.optimize("memory")`` turns it off everywhere to spend that memory back.

It stays correct: it only splits captures your handler just READS (the usual
config/state case).  If the handler REBINDS a captured name (``nonlocal x; x =
...``), per-core copies could drift, so runloom leaves it shared instead.

FASTEST PATH FIRST: if a handler is hot enough to want this, *compiling* it (a
Cython ``cdef`` handler) beats it outright -- that removes the interpreter cost
entirely.  ``@runloom.hot`` is the zero-rewrite option.  Stacking with other
decorators: put ``@runloom.hot`` CLOSEST to your ``def`` so it sees your real
closure, not another decorator's wrapper.
"""
import dis
import functools
import os
import threading
import types
import warnings

_OFF = frozenset(("0", "off", "false", "no", ""))


def _active():
    # Default ON for a decorated handler -- decorating IS the opt-in.
    # optimize("memory") sets RUNLOOM_HOT_HANDLERS=0 to spend the RAM back; read
    # live so optimize() can be called before run().
    return os.environ.get("RUNLOOM_HOT_HANDLERS", "1").strip().lower() not in _OFF


def _rebinds_capture(code):
    # True iff the function REBINDS one of its captured (free) variables, i.e.
    # ``nonlocal x; x = ...`` -> compiles to STORE_DEREF/DELETE_DEREF on a
    # freevar.  Then per-core copies would diverge, so we must not split them.
    # Mutating a captured OBJECT in place (config.x = ..., d[k] = v) is fine --
    # that's STORE_ATTR/STORE_SUBSCR, and every copy points at the same object.
    free = frozenset(code.co_freevars)
    if not free:
        return False
    # The rebind can also live in a NESTED function that shares the cell via
    # ``nonlocal`` -- its STORE_DEREF sits in a nested code object hanging off
    # co_consts, which dis.get_instructions() does not descend into.  Scan those
    # too, recursively, matching on the captured NAME (name-matching may
    # over-report a same-named cell from a deeper scope, but that only keeps a
    # cell shared -- the safe side of the "leave it shared rather than be subtly
    # wrong" contract).
    return _rebinds_names(code, free)


def _rebinds_names(code, free):
    for ins in dis.get_instructions(code):
        if ins.opname in ("STORE_DEREF", "DELETE_DEREF") and ins.argval in free:
            return True
    for const in code.co_consts:
        if isinstance(const, types.CodeType) and _rebinds_names(const, free):
            return True
    return False


def hot(fn):
    """Mark a hot handler for per-core scaling.  See the module docstring.

    Returns a thin wrapper that, the first time your handler runs on a given
    core, hands that core its own copy of the captured slots and reuses it
    thereafter.  A safe no-op on anything that isn't a closure that only reads
    its captures, and a no-op at runtime under ``optimize("memory")``.
    """
    # Only a plain Python function carries a closure we can split.  Anything else
    # (a builtin, a class, an already-wrapped C callable) passes straight through.
    if not isinstance(fn, types.FunctionType):
        return fn
    # The contention is SHARED CLOSURE CELLS.  No captures -> nothing shared ->
    # it already scales, so @hot is a no-op.  Rebinds a capture -> splitting it
    # would change behaviour, so leave it shared rather than be subtly wrong.
    if not fn.__closure__ or _rebinds_capture(fn.__code__):
        return fn

    copies = {}              # core (OS-thread id) -> that core's private copy
    lock = threading.Lock()  # guards first-touch insertion only

    def _copy_for(core):
        c = copies.get(core)
        if c is not None:
            return c
        with lock:
            c = copies.get(core)
            if c is None:
                # Fresh cells holding the SAME values -> identical behaviour, but
                # this core stops sharing the cells with the others.  The code
                # object is shared on purpose: it isn't the contended part.
                cells = tuple(types.CellType(cell.cell_contents)
                              for cell in fn.__closure__)
                c = types.FunctionType(fn.__code__, fn.__globals__, fn.__name__,
                                       fn.__defaults__, cells)
                c.__kwdefaults__ = fn.__kwdefaults__
                c.__dict__.update(fn.__dict__)
                copies[core] = c
        return c

    @functools.wraps(fn)
    def runner(*args, **kwargs):
        if not _active():
            return fn(*args, **kwargs)
        # One hub == one OS thread, so the thread id is the core key; the copy
        # count is bounded by hub count, never by fiber count.  (A fiber that
        # work-steals to another hub mid-run keeps its origin copy for that call;
        # the sharing factor still drops from "all fibers" to "a few per core".)
        return _copy_for(threading.get_ident())(*args, **kwargs)

    runner.__runloom_hot__ = True
    runner._runloom_copies = copies          # introspection / tests
    return runner


# --------------------------------------------------------------------------
# Auto mode: the same per-core scaling with NO decorator.
#
# Turned on by ``runloom.optimize("throughput")`` (or ``RUNLOOM_HOT_AUTO=1``).
# The runtime watches which CLOSURE handlers get spawned a LOT and quietly gives
# the busiest few the ``@runloom.hot`` treatment -- under a hard budget, so it
# can never clone its way through your RAM.  Module-level handlers (no captures)
# already scale and are never touched.  A closure spawned fewer than
# ``RUNLOOM_HOT_AUTO_AFTER`` times (default 64), or past the
# ``RUNLOOM_HOT_AUTO_BUDGET`` distinct-handler cap (default 32), is left shared;
# when the cap bites it says so (no silent truncation) and points you at @hot.
# --------------------------------------------------------------------------

def _intenv(name, default):
    try:
        return max(1, int(os.environ.get(name, "")))
    except ValueError:
        return default


class _AutoHot:
    def __init__(self):
        self.after = _intenv("RUNLOOM_HOT_AUTO_AFTER", 64)
        self.budget = _intenv("RUNLOOM_HOT_AUTO_BUDGET", 32)
        self._counts = {}     # code -> spawns seen so far (pre-promotion)
        self._runners = {}    # id(closure) -> promoted per-core runner (hot path)
        self._shared = set()  # code we'd promote but the budget was full
        self._warned = False
        self._lock = threading.Lock()

    def resolve(self, fn):
        # Only a shared CLOSURE can contend; a module-level def already scales,
        # so never track or count it.  Hot path after warmup: a lock-free read.
        if type(fn) is not types.FunctionType or not fn.__closure__:
            return fn
        code = fn.__code__
        # Count by CODE (bounded: one counter per handler body), but cache the
        # promoted runner by the CLOSURE INSTANCE.  Sibling closures from one
        # factory share this code object yet carry their OWN captured cells, so a
        # per-code runner would run every sibling with a single instance's data.
        # hot(fn) closes over fn, so a cached runner keeps its fn alive and its
        # id() stays unique+stable for exactly as long as it is cached.
        key = id(fn)
        runner = self._runners.get(key)
        if runner is not None:
            return runner
        with self._lock:
            runner = self._runners.get(key)
            if runner is not None:
                return runner
            if code in self._shared:
                return fn
            n = self._counts.get(code, 0) + 1
            if n < self.after:
                self._counts[code] = n
                return fn
            if len(self._runners) >= self.budget:
                self._shared.add(code)
                self._counts.pop(code, None)
                self._warn_budget()
                return fn
            runner = hot(fn)                  # reuse the per-core cell-split path
            self._runners[key] = runner
            self._counts.pop(code, None)
            return runner

    def _warn_budget(self):
        if not self._warned:
            self._warned = True
            warnings.warn(
                "runloom: per-core hot-handler budget of {0} reached; busier "
                "handlers past it stay shared. Raise RUNLOOM_HOT_AUTO_BUDGET, or "
                "mark the ones you care about with @runloom.hot.".format(self.budget),
                stacklevel=2)

    def stats(self):
        return {"promoted": len(self._runners),
                "watching": len(self._counts),
                "left_shared_over_budget": len(self._shared)}


# Module-level switch.  None == auto OFF (the default): fiber() then pays only a
# single `is None` check per spawn.  run() flips it on once if the env is set.
_AUTO = None
_AUTO_READY = False


def _install_auto():
    """Called once from runloom.run(): enable auto-promotion iff its env is set
    (optimize('throughput') sets RUNLOOM_HOT_AUTO).  No-op when off."""
    global _AUTO, _AUTO_READY
    if _AUTO_READY:
        return
    _AUTO_READY = True
    if os.environ.get("RUNLOOM_HOT_AUTO", "0").strip().lower() not in _OFF:
        _AUTO = _AutoHot()
