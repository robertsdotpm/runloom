"""runloom.inspect -- see what every fiber is doing.

The runtime equivalent of Go's `kill -QUIT` fiber dump and
``runtime.Stack`` -- the thing you reach for when a process is hung and
you need to know *which* fibers are stuck and *where* in your code.

    import runloom.inspect as gi

    gi.count()                 # how many fibers are live
    gi.fibers()            # list of per-fiber dicts
    gi.fibers(stacks=True) # ... each with a reconstructed Python stack
    gi.stack(gid)              # one fiber's Python stack
    print(gi.format(stacks=True))   # a formatted, human dump (a string)
    gi.dump()                  # write that dump to stderr

    gi.enable_timestamps()     # track how long each fiber has been parked
    gi.install_dump_signal()   # SIGQUIT -> dump (works even when wedged)

Each fiber dict has:
    id          int        per-fiber id (Go's goid)
    state       str        running | runnable | io-wait | sleep |
                           chan-wait | park | done | ...
    blocked_on  str        coarse class: io | timer | chan | sync | running
    fd          int|None   the fd, when io-wait
    events      str|None   'R' / 'W' / 'RW', when io-wait
    wake_in     float|None seconds until wakeup, when sleeping
    age         float|None seconds in the current parked state, if tracking on
    refcount    int
    noyield     bool
    owner       int        owning OS-thread scheduler (group fibers by it)

Notes on the Python stack (``stack`` / ``stacks=True``):
  * Under the single-thread scheduler -- which is what ``runloom.aio`` uses --
    the full stack of any parked fiber is reconstructed.  asyncio
    Tasks additionally expose their own stack via ``Task.get_stack()``;
    this fills in the raw fibers (channel ops, the netpoll pump,
    accept loops) that ``asyncio.all_tasks()`` never sees.
  * Under the default M:N scheduler a parked fiber can be resumed by
    its hub at any instant, so its stack is withheld (there is no safe way
    to freeze it) -- the structural fields above still tell the story.
  * The currently-running fiber has no *saved* stack; use the normal
    ``traceback`` / ``sys._getframe`` for your own frames.
"""
import os
import sys

import runloom_c as _core


def count():
    """Number of live fibers."""
    return _core.fiber_count()


def stack(gid):
    """Reconstructed Python stack of fiber `gid`, deepest frame first.

    Returns a list of (filename, lineno, funcname) tuples (possibly empty;
    see the module docstring for when a stack is available)."""
    _repr, frames = _core.fiber_stack(int(gid))
    return frames


def entry(gid):
    """repr() of fiber `gid`'s entry callable, or None if unavailable."""
    rep, _frames = _core.fiber_stack(int(gid))
    return rep


def fibers(stacks=False):
    """A snapshot of every live fiber as a list of dicts.

    With stacks=True each dict also gets:
        entry  str|None             the entry callable's repr
        stack  list[(file,line,fn)] deepest frame first
    """
    gs = _core.fibers()
    if stacks:
        for g in gs:
            rep, frames = _core.fiber_stack(g["id"])
            g["entry"] = rep
            g["stack"] = frames
    return gs


def enable_timestamps(on=True):
    """Track how long each fiber has been parked (enables the 'age'
    field).  Costs one monotonic-clock read per park; off by default."""
    _core.set_introspect_timestamps(bool(on))


DEADLOCK_MODES = {"off": 0, "warn": 1, "raise": 2}


def set_deadlock_mode(mode):
    """Control deadlock detection when the single-thread scheduler quiesces
    with fibers still blocked on channels/parks (Go's "all fibers
    are asleep - deadlock!").

        "off"    do nothing
        "warn"   print the fiber dump (default; non-fatal)
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


def set_max_fibers(n):
    """Cap the number of live fibers (0 = unlimited, the default).  Over
    the cap, runloom.go / spawn raises RuntimeError -- an admission gate so an
    unbounded spawn loop can't OOM the process.  The caller applies
    backpressure (retry / shed load) on the rejection.  Zero hot-path cost
    when unset; also via env RUNLOOM_MAX_GOROUTINES."""
    _core.set_max_fibers(int(n))


def max_fibers():
    """The current live-fiber cap (0 = unlimited)."""
    return _core.get_max_fibers()


def live_fibers():
    """Goroutines admitted under the cap and not yet finished (0 if no cap)."""
    return _core.live_fibers()


PARKED_STATES = ("io-wait", "chan-wait", "park", "sleep")


def leaked(min_age=60.0, states=PARKED_STATES):
    """Goroutines that have been parked in one of `states` for longer than
    `min_age` seconds -- the candidates for a leak: an orphaned accept loop,
    a never-awaited task, a waiter whose waker is gone, a stuck timer.

    Needs park-age tracking, so this enables it on first call (via
    enable_timestamps()); ages are measured from when tracking turned on, so
    the first call right after enabling returns nothing useful -- run it from
    a periodic watchdog.

    A long-lived server legitimately has old io-wait fibers (its accept
    loops) and old sleep fibers (tickers); narrow `states` or raise
    `min_age` to suit, e.g. leaked(min_age=300, states=('chan-wait','park'))
    for "stuck waiting on another fiber for 5 minutes"."""
    if not _core.get_introspect_timestamps():
        enable_timestamps(True)
    out = []
    for g in fibers():
        if (g["state"] in states and g["age"] is not None
                and g["age"] >= min_age):
            out.append(g)
    return out


def watch_leaks(min_age=60.0, interval=30.0, states=PARKED_STATES, on_leak=None):
    """Spawn a fiber that every `interval` seconds reports fibers
    parked longer than `min_age` (see leaked()).  `on_leak(list_of_dicts)`
    is called with any hits; the default logs a one-line summary + the dump
    to stderr.  Returns the watchdog's fiber handle.

    Run this inside your scheduler (runloom.run / runloom.aio); it uses cooperative
    sleep, so it never blocks an OS thread."""
    import runloom as _runloom

    enable_timestamps(True)

    def report(hits):
        sys.stderr.write(
            "runloom: {0} fiber(s) parked > {1:.0f}s (possible leak):\n"
            .format(len(hits), min_age))
        for g in hits:
            sys.stderr.write("    fiber {0} [{1}{2}]\n".format(
                g["id"], g["state"], _detail(g)))
        sys.stderr.flush()

    cb = on_leak if on_leak is not None else report

    def loop():
        while True:
            _runloom.sleep(interval)
            hits = leaked(min_age=min_age, states=states)
            if hits:
                cb(hits)

    return _runloom.fiber(loop)


def install_dump_signal(sig=None):
    """Install a SIGQUIT (or `sig`) handler that dumps all fibers to
    stderr -- Go's GOTRACEBACK / `kill -QUIT <pid>`.

    Uses a RAW C handler so the dump fires even when the interpreter is
    wedged (a Python signal handler only runs at a bytecode boundary).
    Returns the signal number installed.  POSIX only."""
    import signal
    signum = int(sig) if sig is not None else int(signal.SIGQUIT)
    return _core.install_traceback_signal(signum)


def install_crash_handler(level=None, file=None):
    """Install a fatal-signal handler (SIGSEGV / SIGBUS / SIGILL / SIGFPE /
    SIGABRT) that turns a crash into a structured dump instead of a silent core.

    On a fault it CLASSIFIES the faulting address against the per-fiber
    guard pages -- a fiber stack overflow is named and distinguished from a
    wild pointer -- then dumps the full live-fiber registry, optionally a
    native backtrace and the Python traceback, and finally chains to the default
    handler so a core dump / correct exit code still follow.

    `level` selects behaviour (comma/space separated; default from the
    RUNLOOM_CRASH env var, else just the fiber dump):

        on / fibers  dump the fiber registry (the default)
        all              fibers + native backtrace + Python traceback
        backtrace        add a native C backtrace (needs execinfo)
        pystack          add the Python traceback (enables faulthandler)
        wait             after the dump, BLOCK for a debugger to attach
                         (prints `gdb -p <pid>`; resume with `kill -CONT <pid>`)
        gdb              fork+exec `gdb -batch -ex 'thread apply all bt'` on self
        off              uninstall

    `file` (or RUNLOOM_CRASH_FILE) also appends the report to that path.

    For full per-thread coverage call this BEFORE starting the runtime, so the
    scheduler hubs are armed as they spawn.  Returns the installed flag bitmask
    (or None if uninstalled).  POSIX has the rich path; Windows dumps via a
    Vectored Exception Handler.  Idempotent; chains to any existing handler."""
    return _core.install_crash_handler(level, file)


def uninstall_crash_handler():
    """Restore the signal dispositions saved by install_crash_handler()."""
    _core.uninstall_crash_handler()


def crash_handler_installed():
    """True if install_crash_handler() is currently active."""
    return _core.crash_handler_installed()


def enable_stack_advice(on=True):
    """Turn the per-fiber-kind stack-usage profiler on or off.

    While on, every fiber's actual C-stack high-water mark is measured and
    grouped by its entry function, so stack_advice() can tell you how much of
    each kind's reserved stack it really uses and suggest a stack_size.

    Purely ADVISORY: the runtime never changes or persists a stack size itself
    -- a remembered-small size is only a lower bound on what a future input
    might need, so you read the advice and apply it yourself via
    ``runloom_c.fiber(fn, stack_size=...)``, with the guard page + crash reporter
    still backstopping every choice.  Off by default; enabling it keeps stack
    painting on (a small profiling cost) for the session."""
    _core.enable_stack_advice(bool(on))


def stack_advice_enabled():
    """True if enable_stack_advice() is currently active."""
    return _core.stack_advice_enabled()


def enable_stack_autosize(on=True, prescan=False):
    """Turn on the adaptive per-fiber-kind stack auto-sizer.

    Each fiber kind (its entry callable) starts *large* the first time it is
    seen; once runloom has measured how much C stack it actually uses, later
    fibers of that kind start at the learned size -- "start large, learn
    down". This right-sizes stacks per kind without you setting `stack_size=` by
    hand, while keeping the high-concurrency memory profile (the large first
    starts are returned to the OS on park, so they cost address space, not RSS).

    In-memory only: the learned sizes are **never written to disk** -- a
    remembered-small size is only a lower bound on what a future input needs, so
    persisting it would be a foot-gun. The guard page, on-demand stack growth,
    and the crash reporter remain the safety net for any underestimate.

    Enabling autosize implies `enable_stack_advice()` (so `stack_advice()` keeps
    reporting) and turns on park-time idle-page reclaim. An explicit
    `runloom.fiber(fn, stack_size=...)` always overrides the auto-sizer. Off by
    default; also enable via `RUNLOOM_STACK_AUTOSIZE=1` (start size via
    `RUNLOOM_STACK_AUTOSIZE_START`, default 256 KiB). Best enabled before the
    runtime starts so kinds are sized from their first spawn.

    `prescan=True` additionally runs the cold-start optimizer: before an unseen
    kind has been measured, its bytecode is loosely scanned for symbols whose C
    implementation has an unusually fat single stack frame (chiefly `Decimal`
    arithmetic, which can use 256 KiB in one frame -- see
    tools/heavy_frames/). A kind that references one starts big enough to hold
    that frame, so it doesn't overflow on its very first run before the
    auto-sizer has had a chance to measure it."""
    _core.enable_stack_autosize(bool(on), bool(prescan))


def stack_autosize_enabled():
    """True if enable_stack_autosize() is currently active."""
    return _core.stack_autosize_enabled()


def reset_stack_advice():
    """Clear the accumulated per-kind stack samples."""
    _core.reset_stack_advice()


def stack_advice():
    """Return per-fiber-kind stack usage as a list of dicts, each with:

        kind       -- "module.qualname (file:line)" of the entry callable
        samples    -- fibers of this kind measured
        max_hwm    -- deepest C-stack bytes any of them actually used
        reserved   -- the stack_size they ran with
        suggested  -- a stack_size that covers the observed max with margin
                      (next power of two of max_hwm * 4)

    Sorted by max_hwm descending (the stack-hungriest kinds first)."""
    rows = _core.stack_advice()
    rows.sort(key=lambda r: r["max_hwm"], reverse=True)
    return rows


def print_stack_advice(file=None):
    """Write a human-readable stack_advice() table to ``file`` (default stderr).

    Flags kinds that are over-reserved (using a small fraction of their stack)
    or close to their reservation (worth a bigger stack_size)."""
    if file is None:
        file = sys.stderr
    rows = stack_advice()
    if not rows:
        file.write("no stack advice yet "
                   "(enable_stack_advice() then run the workload)\n")
        file.flush()
        return

    def kib(n):
        return "{0}K".format(n // 1024)

    file.write("=== runloom stack advice ({0} kinds) ===\n".format(len(rows)))
    file.write("{0:>7} {1:>8} {2:>9} {3:>10}  kind\n".format(
        "samples", "max_use", "reserved", "suggested"))
    for r in rows:
        used, res, sug = r["max_hwm"], r["reserved"], r["suggested"]
        note = ""
        if res and used * 8 < res:
            note = "  (over-reserved)"
        elif res and used * 4 > res * 3:
            note = "  (tight -- consider a bigger stack)"
        file.write("{0:>7} {1:>8} {2:>9} {3:>10}  {4}{5}\n".format(
            r["samples"], kib(used), kib(res), kib(sug), r["kind"], note))
    file.flush()


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
    """Render a human-readable dump of every live fiber as a string.

    The state histogram first, then one block per fiber (with its
    Python stack when stacks=True and one is available)."""
    gs = sorted(_core.fibers(), key=lambda g: g["id"])
    lines = []
    hist = {}
    for g in gs:
        hist[g["state"]] = hist.get(g["state"], 0) + 1
    lines.append("=== runloom fibers: {0} live ===".format(len(gs)))
    for state in sorted(hist):
        lines.append("  {0:<11}{1}".format(state, hist[state]))
    lines.append("")
    for g in gs:
        rep = None
        frames = []
        if stacks:
            rep, frames = _core.fiber_stack(g["id"])
        head = "fiber {0} [{1}{2}]".format(g["id"], g["state"], _detail(g))
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
    primitive ``runloom_c.dump_fibers(fd)`` (which is what the SIGQUIT
    handler from install_dump_signal() runs)."""
    if file is None:
        file = sys.stderr
    file.write(format(stacks=stacks))
    file.write("\n")
    file.flush()


def hubs():
    """A snapshot of every M:N hub as a list of dicts -- the "what is each hub
    doing right now" view, the hub-level companion to fibers().

    Each entry has:
        id            -- dense hub index
        state         -- 'detached' (released its tstate: a blocking call or
                         idle), 'attached' (running Python / CPU-bound),
                         'suspended' (a stop-the-world is in progress)
        running_g     -- goid of the fiber being resumed, or None if idle
        dwell_ms      -- how long the current resume has run (None if idle); a
                         large value with state 'detached' is a wedged hub
        pending       -- fibers owned + queued on this hub
        preempt_requested -- sysmon has asked this hub to yield (a CPU wedge)
        instrumented  -- whether sysmon resume-tracking is live (it is by
                         default on free-threaded 3.13t; running_g / dwell_ms /
                         blocked_at need it)
        blocked_at    -- best-effort Python call site of a DETACHED-wedged hub's
                         blocking call, e.g. 'cursor.execute (db.py:88)', or None
        stack_cmd     -- a ready-to-run command that dumps the full C+Python
                         stack of every thread in THIS process, out-of-process
                         and always safe: 'py-spy dump --pid <PID>'.  Use it when
                         blocked_at is None (a CPU wedge, or the frame could not
                         be read safely) or when you want the complete stack.

    Returns [] when the M:N scheduler is not running (n=1 / outside run())."""
    cmd = "py-spy dump --pid {0}".format(os.getpid())
    hs = _core.mn_hub_states()
    for h in hs:
        h["stack_cmd"] = cmd
    return hs


# A hub past the ~50 ms sysmon wedge budget is stuck, not merely busy.
WEDGE_MS = 50.0


def _hub_label(h):
    """A one-word health label for a hub row."""
    if not h["instrumented"]:
        return h["state"]
    if h["running_g"] is None:
        return "idle"
    if (h["dwell_ms"] or 0.0) >= WEDGE_MS:
        if h["state"] == "detached":
            return "WEDGED/io"
        if h["state"] == "attached":
            return "WEDGED/cpu"
        return "WEDGED"
    return "running"


def print_hubs(file=None):
    """Write a human-readable hubs() table to ``file`` (default stderr).

    One row per hub; wedged hubs are flagged and, when known, show the blocking
    call site, with a py-spy command for the full stack of every thread."""
    if file is None:
        file = sys.stderr
    hs = hubs()
    if not hs:
        file.write("no hubs (the M:N scheduler is not running -- "
                   "use run(n>1, ...) or mn_init)\n")
        file.flush()
        return

    file.write("=== runloom hubs ({0}) ===\n".format(len(hs)))
    file.write("{0:>3}  {1:<10} {2:>10} {3:>9} {4:>4}  what\n".format(
        "id", "label", "running_g", "dwell_ms", "pend"))
    wedged = []
    for h in sorted(hs, key=lambda x: x["id"]):
        label = _hub_label(h)
        rg, dwell = h["running_g"], h["dwell_ms"]
        file.write("{0:>3}  {1:<10} {2:>10} {3:>9} {4:>4}  {5}\n".format(
            h["id"], label,
            "-" if rg is None else rg,
            "-" if dwell is None else "{0:.0f}".format(dwell),
            h["pending"], h["blocked_at"] or ""))
        if label.startswith("WEDGED"):
            wedged.append(h)

    if wedged:
        file.write("\n{0} hub(s) wedged.  Full C+Python stack of every "
                   "thread:\n    {1}\n".format(len(wedged), wedged[0]["stack_cmd"]))
    file.flush()
