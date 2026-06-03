"""runloom.inspect -- see what every goroutine is doing.

The runtime equivalent of Go's `kill -QUIT` goroutine dump and
``runtime.Stack`` -- the thing you reach for when a process is hung and
you need to know *which* goroutines are stuck and *where* in your code.

    import runloom.inspect as gi

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
  * Under the single-thread scheduler -- which is what ``runloom.aio`` uses --
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

import runloom_c as _core


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


DEADLOCK_MODES = {"off": 0, "warn": 1, "raise": 2}


def set_deadlock_mode(mode):
    """Control deadlock detection when the single-thread scheduler quiesces
    with goroutines still blocked on channels/parks (Go's "all goroutines
    are asleep - deadlock!").

        "off"    do nothing
        "warn"   print the goroutine dump (default; non-fatal)
        "raise"  raise RuntimeError out of run()

    Also settable via env RUNLOOM_DEADLOCK=off|warn|raise.  aio's clean loop
    shutdown is excluded, so this won't fire on a normal aio teardown with
    pending background tasks."""
    if isinstance(mode, str):
        mode = DEADLOCK_MODES[mode]
    _core.set_deadlock_mode(int(mode))


def deadlock_mode():
    """Current deadlock-detection mode as a string ('off'/'warn'/'raise')."""
    inv = {v: k for k, v in DEADLOCK_MODES.items()}
    return inv.get(_core.get_deadlock_mode(), "warn")


def set_max_goroutines(n):
    """Cap the number of live goroutines (0 = unlimited, the default).  Over
    the cap, runloom.go / spawn raises RuntimeError -- an admission gate so an
    unbounded spawn loop can't OOM the process.  The caller applies
    backpressure (retry / shed load) on the rejection.  Zero hot-path cost
    when unset; also via env RUNLOOM_MAX_GOROUTINES."""
    _core.set_max_goroutines(int(n))


def max_goroutines():
    """The current live-goroutine cap (0 = unlimited)."""
    return _core.get_max_goroutines()


def live_goroutines():
    """Goroutines admitted under the cap and not yet finished (0 if no cap)."""
    return _core.live_goroutines()


PARKED_STATES = ("io-wait", "chan-wait", "park", "sleep")


def leaked(min_age=60.0, states=PARKED_STATES):
    """Goroutines that have been parked in one of `states` for longer than
    `min_age` seconds -- the candidates for a leak: an orphaned accept loop,
    a never-awaited task, a waiter whose waker is gone, a stuck timer.

    Needs park-age tracking, so this enables it on first call (via
    enable_timestamps()); ages are measured from when tracking turned on, so
    the first call right after enabling returns nothing useful -- run it from
    a periodic watchdog.

    A long-lived server legitimately has old io-wait goroutines (its accept
    loops) and old sleep goroutines (tickers); narrow `states` or raise
    `min_age` to suit, e.g. leaked(min_age=300, states=('chan-wait','park'))
    for "stuck waiting on another goroutine for 5 minutes"."""
    if not _core.get_introspect_timestamps():
        enable_timestamps(True)
    out = []
    for g in goroutines():
        if (g["state"] in states and g["age"] is not None
                and g["age"] >= min_age):
            out.append(g)
    return out


def watch_leaks(min_age=60.0, interval=30.0, states=PARKED_STATES, on_leak=None):
    """Spawn a goroutine that every `interval` seconds reports goroutines
    parked longer than `min_age` (see leaked()).  `on_leak(list_of_dicts)`
    is called with any hits; the default logs a one-line summary + the dump
    to stderr.  Returns the watchdog's goroutine handle.

    Run this inside your scheduler (runloom.run / runloom.aio); it uses cooperative
    sleep, so it never blocks an OS thread."""
    import runloom as _runloom

    enable_timestamps(True)

    def report(hits):
        sys.stderr.write(
            "runloom: {0} goroutine(s) parked > {1:.0f}s (possible leak):\n"
            .format(len(hits), min_age))
        for g in hits:
            sys.stderr.write("    goroutine {0} [{1}{2}]\n".format(
                g["id"], g["state"], _detail(g)))
        sys.stderr.flush()

    cb = on_leak if on_leak is not None else report

    def loop():
        while True:
            _runloom.sleep(interval)
            hits = leaked(min_age=min_age, states=states)
            if hits:
                cb(hits)

    return _runloom.go(loop)


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
    lines.append("=== runloom goroutines: {0} live ===".format(len(gs)))
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
    primitive ``runloom_c.dump_goroutines(fd)`` (which is what the SIGQUIT
    handler from install_dump_signal() runs)."""
    if file is None:
        file = sys.stderr
    file.write(format(stacks=stacks))
    file.write("\n")
    file.flush()
