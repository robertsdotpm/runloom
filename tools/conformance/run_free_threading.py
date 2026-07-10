"""Pillar C -- gevent-style free-threading conformance runner for runloom.

Run CPython's OWN ``Lib/test/test_free_threading`` package (pure free-threaded
stress on dict / list / set / heapq / gc / io / str / type / ...) under
``runloom.monkey.patch()``, so the cooperative Co* locks
(Lock/RLock/Event/Condition/Semaphore, foreign-OS-thread-safe) take REAL
free-threaded contention: every module here drives genuine ``threading.Thread``
workers hammering a shared object behind a Barrier, so the patched primitives are
exercised from real OS threads (the ``peek_current``-NULL foreign-thread path),
not from goroutines.

What this proves: the monkey layer's Co* primitives don't corrupt or deadlock
under exactly the race conditions the free-threading build was hardened against.
A red test is a real cooperative-semantics divergence in the monkey layer.

The 3.14t heapq FT note: test_heapq sits near a known CPython 3.13t heapq
free-threading SIGSEGV.  3.14t carries the gh-116738 fix (heapq holds the list
critical section); verified this session test_heapq passes 11/11 BOTH with and
without monkey.patch, so any future red there is CPython, not runloom -- run it
once WITHOUT --monkey (below) to confirm.

Known failures / deadlocks live in ``free_threading_known_failures.txt`` (a bare
module entry empties that module's suite, keeping a module-level DEADLOCK out of
the in-process run).  The one such entry today is test_func_annotations -- the
only module using a ThreadPoolExecutor, which monkey turns into a fiber-backed
pool that can't satisfy an N-way Barrier without a running scheduler (see that
file for the full reason).

Usage:
  PYTHON_GIL=0 PYTHONPATH=src \\
    $HOME/.pyenv/versions/3.14.4t/bin/python3 tools/conformance/run_free_threading.py
  ... run_free_threading.py test_dict test_list       # a chosen subset
  ... run_free_threading.py --no-monkey                # CONTROL: pure CPython
  ... run_free_threading.py --list                     # show the curated set
  ... run_free_threading.py -v
"""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import conformance_lib as cl  # noqa: E402


PREFIX = "test.test_free_threading."

KNOWN_FAILURES_FILE = os.path.join(HERE, "free_threading_known_failures.txt")

# Curated default set: the whole 3.14t test_free_threading package as it stands.
# All of these pass under monkey.patch except the known-failure(s) subtracted via
# free_threading_known_failures.txt.  Hardcoded (not glob-discovered) so the
# in-process run is deterministic and a newly-added upstream module can't wedge
# it with a surprise deadlock -- add a name here after probing it.
CURATED = [
    "test_bisect", "test_capi", "test_code", "test_collections",
    "test_cprofile", "test_csv", "test_dbm_gnu", "test_dict",
    "test_enumerate", "test_frame", "test_func_annotations",
    "test_functools", "test_gc", "test_grp", "test_heapq", "test_io",
    "test_iteration", "test_itertools_batched", "test_list",
    "test_methodcaller", "test_mmap", "test_monitoring", "test_races",
    "test_re", "test_resource", "test_reversed", "test_set", "test_slots",
    "test_str", "test_tokenize", "test_type", "test_uuid", "test_zip",
]


def import_submodule(name):
    """Import ``test.test_free_threading.<name>``.

    Returns (module, None) on success, or (None, reason) if the module
    SkipTest's at import (e.g. test_dbm_gnu without _gdbm) or is genuinely
    absent -- either way the runner keeps going and records the skip."""
    full = "test.test_free_threading." + name
    try:
        __import__(full)
        return sys.modules[full], None
    except unittest.SkipTest as exc:
        return None, "module-skip: %s" % exc
    except ImportError as exc:
        return None, "import error: %r" % exc


def main(argv):
    args = list(argv)
    verbosity = 0
    use_monkey = True
    if "-v" in args:
        verbosity = 2
        args.remove("-v")
    if "--no-monkey" in args:
        use_monkey = False
        args.remove("--no-monkey")
    if "--list" in args:
        for name in CURATED:
            print(name)
        return 0

    if not cl.have_cpython_tests():
        sys.stdout.write(
            "SKIP: CPython stdlib `test` package is not installed on this "
            "interpreter; nothing to run.\n")
        return 0

    cl.ensure_src_on_path()
    import runloom
    import runloom.monkey

    if use_monkey:
        runloom.monkey.patch()
        sys.stdout.write("[runloom.monkey.patch() ACTIVE -- Co* locks under "
                         "real free-threaded contention]\n")
    else:
        sys.stdout.write("[--no-monkey CONTROL -- pure CPython, no patch]\n")

    known = cl.load_known_failures(KNOWN_FAILURES_FILE)
    names = args if args else CURATED

    per_module = []
    module_skips = []
    try:
        for name in names:
            sys.stdout.write("\n--- %s ---\n" % name)
            sys.stdout.flush()
            mod, reason = import_submodule(name)
            if mod is None:
                sys.stdout.write("  (skipped: %s)\n" % reason)
                module_skips.append((name, reason))
                per_module.append((name, {
                    "ran": 0, "ok": 0, "fail": 0, "err": 0,
                    "skip": 0, "dropped": 0, "red_short_ids": []}))
                continue
            st = cl.run_module(mod, known, PREFIX, verbosity=verbosity)
            per_module.append((name, st))
    finally:
        if use_monkey:
            runloom.monkey.unpatch()

    genuine_red = cl.print_summary("free_threading", per_module)
    if module_skips:
        sys.stdout.write("\nModule-level skips (environmental / known "
                         "deadlock subtracted):\n")
        for name, reason in module_skips:
            sys.stdout.write("  SKIP  %s -- %s\n" % (name, reason))
    return 1 if genuine_red else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
