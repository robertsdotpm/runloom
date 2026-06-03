"""pygo.inspect -- see what every goroutine is doing.

The runtime equivalent of Go's `kill -QUIT` goroutine dump and
``runtime.Stack`` -- the thing you reach for when a process is hung and
you need to know *which* goroutines are stuck and *where* in your code.

    import pygo.inspect as gi

    gi.count()                 # how many goroutines are live
    gi.goroutines()            # list of per-goroutine dicts
    gi.goroutines(stacks=True) # ... each with a reconstructed Python stack
    gi.stack(gid)              # one goroutine's Python stack
    print(gi.format(stacks=True))   # a formatted, human dump (a string)
    gi.dump()                  # write that dump to stderr

    gi.enable_timestamps()     # track how long each goroutine has been parked
    gi.install_dump_signal()   # SIGQUIT -> dump (works even when wedged)

Each goroutine dict has:
    id          int        per-goroutine id (Go's goid)
    state       str        running | runnable | io-wait | sleep |
                           chan-wait | park | done | ...
    blocked_on  str        coarse class: io | timer | chan | sync | running
    fd          int|None   the fd, when io-wait
    events      str|None   'R' / 'W' / 'RW', when io-wait
    wake_in     float|None seconds until wakeup, when sleeping
    age         float|None seconds in the current parked state, if tracking on
    refcount    int
    noyield     bool
    owner       int        owning OS-thread scheduler (group goroutines by it)

Notes on the Python stack (``stack`` / ``stacks=True``):
  * Under the single-thread scheduler -- which is what ``pygo.aio`` uses --
    the full stack of any parked goroutine is reconstructed.  asyncio
    Tasks additionally expose their own stack via ``Task.get_stack()``;
    this fills in the raw goroutines (channel ops, the netpoll pump,
    accept loops) that ``asyncio.all_tasks()`` never sees.
  * Under the default M:N scheduler a parked goroutine can be resumed by
    its hub at any instant, so its stack is withheld (there is no safe way
    to freeze it) -- the structural fields above still tell the story.
  * The currently-running goroutine has no *saved* stack; use the normal
    ``traceback`` / ``sys._getframe`` for your own frames.
"""
import sys

import pygo_core as _core


def count():
    """Number of live goroutines."""
    return _core.goroutine_count()


def stack(gid):
    """Reconstructed Python stack of goroutine `gid`, deepest frame first.

    Returns a list of (filename, lineno, funcname) tuples (possibly empty;
    see the module docstring for when a stack is available)."""
    _repr, frames = _core.goroutine_stack(int(gid))
    return frames


def entry(gid):
    """repr() of goroutine `gid`'s entry callable, or None if unavailable."""
    rep, _frames = _core.goroutine_stack(int(gid))
    return rep


def goroutines(stacks=False):
    """A snapshot of every live goroutine as a list of dicts.

    With stacks=True each dict also gets:
        entry  str|None             the entry callable's repr
        stack  list[(file,line,fn)] deepest frame first
    """
    gs = _core.goroutines()
    if stacks:
        for g in gs:
            rep, frames = _core.goroutine_stack(g["id"])
            g["entry"] = rep
            g["stack"] = frames
    return gs


def enable_timestamps(on=True):
    """Track how long each goroutine has been parked (enables the 'age'
    field).  Costs one monotonic-clock read per park; off by default."""
    _core.set_introspect_timestamps(bool(on))


def install_dump_signal(sig=None):
    """Install a SIGQUIT (or `sig`) handler that dumps all goroutines to
    stderr -- Go's GOTRACEBACK / `kill -QUIT <pid>`.

    Uses a RAW C handler so the dump fires even when the interpreter is
    wedged (a Python signal handler only runs at a bytecode boundary).
    Returns the signal number installed.  POSIX only."""
    import signal
    signum = int(sig) if sig is not None else int(signal.SIGQUIT)
    return _core.install_traceback_signal(signum)


def _detail(g):
    bits = []
    if g["state"] == "io-wait" and g["fd"] is not None:
        bits.append("fd={0} {1}".format(g["fd"], g["events"] or ""))
    if g["wake_in"] is not None:
        bits.append("wake_in={0:.3f}s".format(g["wake_in"]))
    if g["age"] is not None:
        bits.append("age={0:.1f}s".format(g["age"]))
    return (", " + ", ".join(bits)) if bits else ""


def format(stacks=True):
    """Render a human-readable dump of every live goroutine as a string.

    The state histogram first, then one block per goroutine (with its
    Python stack when stacks=True and one is available)."""
    gs = sorted(_core.goroutines(), key=lambda g: g["id"])
    lines = []
    hist = {}
    for g in gs:
        hist[g["state"]] = hist.get(g["state"], 0) + 1
    lines.append("=== pygo goroutines: {0} live ===".format(len(gs)))
    for state in sorted(hist):
        lines.append("  {0:<11}{1}".format(state, hist[state]))
    lines.append("")
    for g in gs:
        rep = None
        frames = []
        if stacks:
            rep, frames = _core.goroutine_stack(g["id"])
        head = "goroutine {0} [{1}{2}]".format(g["id"], g["state"], _detail(g))
        if rep:
            head += "  {0}".format(rep)
        lines.append(head + ":")
        for fn, ln, name in frames:
            lines.append("    {0} ({1}:{2})".format(name, fn, ln))
    return "\n".join(lines)


def dump(file=None, stacks=True):
    """Write format(stacks=...) to `file` (default sys.stderr).

    This is the rich, Python-formatted dump.  For a dump that is safe to
    take from a signal handler / when the interpreter is wedged, use the C
    primitive ``pygo_core.dump_goroutines(fd)`` (which is what the SIGQUIT
    handler from install_dump_signal() runs)."""
    if file is None:
        file = sys.stderr
    file.write(format(stacks=stacks))
    file.write("\n")
    file.flush()
