"""runloom.tools.watchdog -- deadlock / hang detector with full state dump.

A goroutine deadlock or a scheduler lost-wake shows up as a process that
simply stops making progress: `run()` / `mn_run()` never returns and no
exception is raised.  This module turns that silent hang into a loud,
debuggable artifact.

On a deadline breach it dumps, to stderr:
  1. every OS thread's C+Python stack            (faulthandler)
  2. runloom's per-thread lifecycle event ring      (runloom_c._diag_dump)
  3. the scheduler/netpoll self-check result     (runloom_c._self_check)
  4. scheduler stats                             (runloom_c.stats)

Usage
-----
    from tools.watchdog import watchdog, run_guarded

    with watchdog(5.0, label="chan ping-pong"):
        runloom_c.run()

    # or wrap a callable:
    run_guarded(lambda: runloom_c.run(), seconds=5.0)

For the event ring (#2) to contain anything, start the process with
    RUNLOOM_DEBUG=ring,gstate        (or RUNLOOM_DEBUG=all)
which runloom_c reads once at import.

Notes
-----
* faulthandler (#1) works even when the interpreter is wedged in a C
  call holding the GIL -- it runs from a dedicated watchdog thread and
  writes the dump directly.  On free-threaded 3.13t there is no GIL, so
  the runloom diag calls (#2-#4) also run reliably from the timer thread.
* By default a breach RAISES TimeoutError in the watchdog thread context
  and (optionally) aborts the process for a core dump.  In tests, prefer
  raising; for live hunting (hunt_hang-style), pass abort=True.
"""
import faulthandler
import os
import sys
import threading
import time

try:
    import runloom_c
except ImportError:  # allow importing this module without the ext built
    runloom_c = None


def hang_dump(file=sys.stderr, label=""):
    """Dump every available piece of runtime state.  Safe to call from
    any thread; never raises."""
    sep = "=" * 70
    print("\n" + sep, file=file)
    print("RUNLOOM HANG DUMP" + (": " + label if label else ""), file=file)
    print("pid={0}  time={1}".format(os.getpid(), time.strftime("%H:%M:%S")),
          file=file)
    print(sep, file=file)

    # 1. all OS-thread stacks (C-safe).
    print("\n--- all thread tracebacks ---", file=file)
    try:
        faulthandler.dump_traceback(file=file, all_threads=True)
    except Exception as e:                     # pragma: no cover - defensive
        print("  (faulthandler failed: {0!r})".format(e), file=file)

    if runloom_c is not None:
        # 2. scheduler self-check (walks parker lists / fd buckets / counters).
        print("\n--- runloom_c._self_check(verbose=1) ---", file=file)
        try:
            file.flush()
            violations = runloom_c._self_check(1)
            print("  violations: {0}".format(violations), file=file)
        except Exception as e:
            print("  (self_check failed: {0!r})".format(e), file=file)

        # 3. scheduler / netpoll stats.
        print("\n--- runloom_c.stats() ---", file=file)
        try:
            print("  {0}".format(runloom_c.stats()), file=file)
        except Exception as e:
            print("  (stats failed: {0!r})".format(e), file=file)

        # 4. per-thread lifecycle event ring (needs RUNLOOM_DEBUG=ring).
        print("\n--- runloom_c._diag_dump() (event ring) ---", file=file)
        try:
            file.flush()
            runloom_c._diag_dump(file.fileno() if hasattr(file, "fileno") else -1)
        except Exception as e:
            print("  (diag_dump failed: {0!r})".format(e), file=file)

    print("\n" + sep, file=file)
    file.flush()


class _Watchdog(object):
    def __init__(self, seconds, label="", abort=False, on_timeout=None):
        self.seconds = float(seconds)
        self.label = label
        self.abort = abort
        self.on_timeout = on_timeout
        self._timer = None
        self._fired = threading.Event()

    def _fire(self):
        self._fired.set()
        hang_dump(label=self.label or "watchdog timeout after {0}s".format(self.seconds))
        if self.on_timeout is not None:
            try:
                self.on_timeout()
            except Exception:
                pass
        if self.abort:
            # Produce a core dump for gdb post-mortem; faster than waiting.
            sys.stderr.flush()
            os.abort()

    def __enter__(self):
        # faulthandler's own timer is C-level and fires even under a held
        # GIL; we ALSO run a Python timer so the runloom diag calls happen.
        faulthandler.enable()
        faulthandler.dump_traceback_later(self.seconds, exit=False)
        self._timer = threading.Timer(self.seconds + 0.05, self._fire)
        self._timer.daemon = True
        self._timer.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        faulthandler.cancel_dump_traceback_later()
        if self._timer is not None:
            self._timer.cancel()
        if self._fired.is_set() and exc_type is None:
            # The deadline elapsed but the work eventually returned: still
            # a failure (it should have been fast).  Surface it.
            raise TimeoutError(
                "watchdog fired after {0}s: {1}".format(self.seconds, self.label))
        return False


def watchdog(seconds, label="", abort=False, on_timeout=None):
    """Context manager: arms a hang detector for `seconds`.  On breach it
    dumps full runtime state; if `abort` is True it os.abort()s for a core
    dump, otherwise a TimeoutError is raised on context exit."""
    return _Watchdog(seconds, label=label, abort=abort, on_timeout=on_timeout)


def run_guarded(fn, seconds=10.0, label="", dump=True):
    """Run fn() in a worker thread and join with a deadline.  Returns
    fn()'s result on success.  If fn() does not finish within `seconds`
    -- e.g. the scheduler wedged on a lost-wake or a true deadlock and
    `run()`/`mn_run()` never returns -- this dumps full runtime state
    and raises TimeoutError, abandoning the (daemon) worker thread.

    This is the test-friendly mode: it can interrupt a wedged main loop
    because the wedge is on the worker thread, not the caller.  (A plain
    `with watchdog(...)` cannot, since its __exit__ only runs once the
    guarded block returns -- which a true hang never does.  Use the
    context manager with abort=True, or an outer OS `timeout`, for that.)
    """
    label = label or getattr(fn, "__name__", "fn")
    box = {}

    def worker():
        try:
            box["result"] = fn()
        except BaseException as e:                  # noqa: BLE001
            box["exc"] = e

    t = threading.Thread(target=worker, name="runloom-guarded", daemon=True)
    t.start()
    t.join(seconds)
    if t.is_alive():
        if dump:
            hang_dump(label="run_guarded timeout after {0}s: {1}".format(seconds, label))
        raise TimeoutError(
            "run_guarded: {0!r} did not finish within {1}s "
            "(state dumped above)".format(label, seconds))
    if "exc" in box:
        raise box["exc"]
    return box.get("result")


if __name__ == "__main__":
    # Self-demo: a goroutine that never terminates (infinite yield loop),
    # so run() never returns -- a genuine "scheduler never makes the work
    # finish" hang.  run_guarded catches it, dumps state, and raises.
    sys.path.insert(0, "src")
    import runloom_c as pc

    def never_finishes():
        def spinner():
            while True:
                pc.sched_yield_classic()   # always runnable -> run() never drains
        pc.fiber(spinner)
        pc.run()

    print("running a non-terminating scheduler under a 2s guard...")
    try:
        run_guarded(never_finishes, seconds=2.0, label="infinite yield loop")
    except TimeoutError as e:
        print("\nwatchdog caught the hang as expected:\n  {0}".format(e))
        sys.exit(0)
    print("ERROR: expected a hang")
    sys.exit(1)
