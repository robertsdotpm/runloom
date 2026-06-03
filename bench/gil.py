"""Free-threading (no-GIL) guard for the benchmark suite.

The hazard: a C extension that has NOT declared free-threading support
re-enables the GIL the moment it is imported.  On this box that happens
transitively -- pytest-benchmark's collection path probes for a Brotli
codec (brotlicffi -> brotli -> _brotli), and `_brotli` has no nogil
opt-in, so importing it silently flips the GIL back ON.  Every subsequent
"free-threaded" parallel measurement then secretly runs GIL'd: the numbers
look plausible and are wrong.

This module makes that impossible, two ways:

  * ensure_nogil(): called at process start by every CLI bench entry point.
    On a free-threaded build it re-execs the interpreter with `-X gil=0`
    (and PYTHON_GIL=0) unless that is already in force, so NO later import
    can re-enable the GIL.  No-op on a normal GIL'd build.

  * assert_nogil(): a tripwire the harness calls before recording any
    sample (and pytest's conftest calls at session start).  If the GIL is
    somehow on, it raises -- we refuse to write a GIL-on number into a file
    labelled free-threaded rather than corrupt the dataset.

Kept import-light (os / sys / sysconfig only) so it can run first.
"""
import os
import sys
import sysconfig

REEXEC_FLAG = "PYGO_BENCH_NOGIL_REEXEC"


def is_free_threaded_build():
    """True on a CPython 't' build (Py_GIL_DISABLED == 1)."""
    return bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


def gil_enabled():
    """Is the GIL currently active?  (Always True on a GIL'd build.)"""
    f = getattr(sys, "_is_gil_enabled", None)
    return f() if f is not None else True


def ensure_nogil():
    """Guarantee the GIL stays off for this entire process.

    Free-threaded builds only; no-op otherwise and no-op once `-X gil=0`
    is already in force.  Otherwise re-execs the interpreter with the flag
    set so any C extension imported later cannot turn the GIL back on.
    Set PYGO_NO_REEXEC=1 to opt out of the re-exec (the harness tripwire
    still protects you).
    """
    if not is_free_threaded_build():
        return
    if os.environ.get("PYTHON_GIL") == "0":
        return  # already forced off; later imports cannot re-enable
    if os.environ.get("PYGO_NO_REEXEC") == "1":
        return
    if os.environ.get(REEXEC_FLAG) == "1":
        # We already re-exec'd; if it still didn't take, fail loudly here
        # rather than spin.
        if gil_enabled():
            raise RuntimeError(
                "GIL still enabled after re-exec with PYTHON_GIL=0 -- "
                "cannot guarantee a free-threaded benchmark")
        return
    argv = reexec_argv()
    if argv is None:
        # Can't safely rebuild the command (stdin '-' or `-c ...`): don't
        # re-exec into an empty/wrong program. The assert_nogil tripwire
        # still protects any measurement that follows.
        return
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env[REEXEC_FLAG] = "1"
    os.execve(sys.executable, argv, env)


def reexec_argv():
    """Rebuild argv to re-launch with the GIL forced off, preserving the
    `-m module` form.  Returns None when the entry point can't be
    reconstructed (a `-` stdin script or `python -c`)."""
    base = [sys.executable, "-X", "gil=0"]
    main = sys.modules.get("__main__")
    spec = getattr(main, "__spec__", None)
    if spec is not None and getattr(spec, "name", None):
        return base + ["-m", spec.name] + sys.argv[1:]
    argv0 = sys.argv[0] if sys.argv else ""
    if argv0 in ("", "-", "-c") or not os.path.isfile(argv0):
        return None
    return base + sys.argv


def assert_nogil(where=""):
    """Refuse to proceed if the GIL is on (free-threaded build only)."""
    if is_free_threaded_build() and gil_enabled():
        loc = " (%s)" % where if where else ""
        raise RuntimeError(
            "GIL is ENABLED on a free-threaded build%s -- a C extension "
            "without free-threading support was imported and flipped it on. "
            "Relaunch with `-X gil=0` (or PYTHON_GIL=0). Refusing to record "
            "GIL-on numbers as free-threaded." % loc)
