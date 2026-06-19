#!/usr/bin/env python3
"""Run ONE CPython stdlib test module inside a runloom goroutine on the M:N
scheduler (free-threaded, GIL off).

This is the child process spawned one-per-module by ``sweep_mn.py``.  Running a
single module per subprocess means a SIGSEGV, abort, or lost-wake hang is
*contained* and *attributed* to the module that triggered it instead of
poisoning the whole sweep -- exactly the pattern tests/run_isolated.py uses for
the native suite, lifted to the vendored stdlib corpus.

What "port to run in a goroutine with N:M on" means here:
  * we do NOT monkey-patch the stdlib (threading/asyncio/socket stay native);
  * we spin up ``hubs`` OS-thread hubs with mn_init(),
  * load + run the module's unittest suite *inside* a goroutine via mn_go(),
  * and let the M:N scheduler drive it to completion with mn_run().
So the stackful-coroutine engine and the work-stealing M:N scheduler are
exercised against real, diverse Python workloads -- the bug-hunting surface.

The module is *imported inside the goroutine* (loadTestsFromName imports it), so
even an import-time crash happens under the scheduler, which is intentional.

Usage:
    run_one_mn.py <dotted.module.name> [hubs]

stdout gets exactly one machine-readable line:
    RESULT module=... mn_rc=... ran=... fail=... err=... skip=... ok=... exc=...
unittest's own output (and any traceback) goes to stderr, with verbosity=2 so
the LAST line before a segfault names the test that was executing.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))      # tests_stdlib/
REPO = os.path.dirname(HERE)

# Put the runloom build first so ``import runloom_c`` finds the in-tree .so, and
# put tests_stdlib/ first so ``import test.test_xxx`` resolves to our *vendored*
# copy (which shadows the installed stdlib `test` package), not the original.
sys.path.insert(0, os.path.join(REPO, "src"))
sys.path.insert(0, HERE)

import unittest
import runloom_c as rc


def main():
    if len(sys.argv) < 2:
        print("RESULT module=? mn_rc=? ran=0 fail=0 err=0 skip=0 ok=False "
              "exc='usage: run_one_mn.py <module> [hubs]'")
        return 2
    modname = sys.argv[1]
    hubs = int(sys.argv[2]) if len(sys.argv) > 2 else 4

    holder = {"ran": 0, "fail": 0, "err": 0, "skip": 0, "ok": False, "exc": None}

    def body():
        try:
            loader = unittest.TestLoader()
            suite = loader.loadTestsFromName(modname)
            runner = unittest.TextTestRunner(verbosity=2, stream=sys.stderr)
            result = runner.run(suite)
            holder["ran"] = result.testsRun
            holder["fail"] = len(result.failures)
            holder["err"] = len(result.errors)
            holder["skip"] = len(result.skipped)
            holder["ok"] = result.wasSuccessful()
        except BaseException as exc:           # import error, load error, etc.
            import traceback
            holder["exc"] = repr(exc)
            traceback.print_exc()

    # RUNLOOM_MN_STACK (bytes) overrides the default 128 KB goroutine stack.
    # RUNLOOM_RUN_MODE selects the scheduler:
    #   "mn" (default) -> mn_init/mn_go/mn_run on `hubs` hubs (the real target);
    #   "go"           -> the 1:1 scheduler go()/run(), which (unlike mn_go)
    #                     accepts a stack_size, used as a STACK-ISOLATION control
    #                     to measure how many crashes are pure C-stack overflow.
    stack = int(os.environ.get("RUNLOOM_MN_STACK", "0"))
    mode = os.environ.get("RUNLOOM_RUN_MODE", "mn")

    if mode == "go":
        if stack > 0:
            rc.fiber(body, stack)
        else:
            rc.fiber(body)
        mn_rc = rc.run()
    else:
        rc.mn_init(hubs)
        if stack > 0:
            rc.mn_fiber(body, stack)   # roomy stack for deep stdlib C bursts
        else:
            rc.mn_fiber(body)          # hub default (128 KB) -- raw crash baseline
        mn_rc = rc.mn_run()
        rc.mn_fini()

    # One line, easy to grep / parse from the driver.  Leading newline so the
    # RESULT token always starts a line even when a test left stdout without a
    # trailing newline (e.g. concurrent.futures timing prints) -- otherwise the
    # driver's line scan misses it and miscalls a passing module ERROR.
    print("\nRESULT module=%s mn_rc=%s ran=%d fail=%d err=%d skip=%d ok=%s exc=%r"
          % (modname, mn_rc, holder["ran"], holder["fail"], holder["err"],
             holder["skip"], holder["ok"], holder["exc"]))
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
