#!/usr/bin/env python3
"""fuzz_api.py -- coverage-guided (Atheris) fuzzing of the runloom C-API surface.

The adversarial-QA pass did a fixed 37-case bad-input sweep of the module API
(negative sizes, bad fds, wrong types) and it found the Coro(stack_size=-1)
SIGSEGV.  This is the COVERAGE-GUIDED version: Atheris drives a FuzzedDataProvider
into the scheduler-free constructor/validation surface, and libFuzzer's feedback
steers toward the deep arg-parse / overflow / ENOMEM error branches that
`coverage.sh` flags as uncovered and that a fixed case list never reaches.

CONTRACT (the oracle): every malformed input must raise a clean Python exception
-- it must NEVER crash the interpreter (segfault / abort) or produce a C-level
overflow.  A crash is what libFuzzer catches.  So a clean Python exception is the
CORRECT outcome (swallowed); anything that kills the process is a finding.

SAFETY: this fuzzes only scheduler-free constructors/setters -- it never starts
the M:N scheduler, spawns a goroutine, or makes a blocking call, so it cannot
hang.  Integer bands are bounded so a fuzzed stack size yields a clean
MemoryError (validation), not an actual multi-GB mmap; one transient object per
iteration is created and GC'd, so no mmap accumulation.  Bounded by
-max_total_time.

Run:   tools/fuzz/atheris/run.sh [seconds]
       PYTHON_GIL=0 PYTHONPATH=src python tools/fuzz/atheris/fuzz_api.py -max_total_time=30
Needs: atheris (pip install atheris into the 3.13t).
"""
import sys

import atheris

with atheris.instrument_imports():
    import runloom
    import runloom_c


def _noop():
    return None


def one_input(data):
    fdp = atheris.FuzzedDataProvider(data)
    nops = fdp.ConsumeIntInRange(1, 8)
    for _ in range(nops):
        op = fdp.ConsumeIntInRange(0, 8)
        try:
            if op == 0:
                # channel capacity: negative -> ValueError, valid -> allocates buf.
                # BOUNDED: Chan eagerly allocates buf[cap], so an unbounded fuzzed
                # cap (~2^28) would request multi-GB and OOM the fuzzer (a false
                # crash). The validation paths live at the small/negative end.
                runloom_c.Chan(fdp.ConsumeIntInRange(-16, 8192))
            elif op == 1:
                # the historical Coro(stack_size=-1) SIGSEGV class. Band the size so a
                # huge value yields a clean MemoryError, not a real giant mmap.
                ss = fdp.ConsumeIntInRange(-(1 << 40), 1 << 40)
                c = runloom_c.Coro(_noop, ss)
                del c
            elif op == 2:
                runloom.optimize(fdp.ConsumeUnicodeNoSurrogates(16))
            elif op == 3:
                runloom.set_grow_down(fdp.ConsumeIntInRange(-2, 2))
            elif op == 4:
                # Chan with a fuzzed non-int type sometimes (TypeError path)
                runloom_c.Chan(fdp.ConsumeUnicodeNoSurrogates(4))
            elif op == 5:
                # MachineCode misuse surface (test_adv_stack flagged it)
                mc = getattr(runloom_c, "MachineCode", None)
                if mc is not None:
                    mc(fdp.ConsumeBytes(fdp.ConsumeIntInRange(0, 32)))
            elif op == 6:
                # optimize with a fuzzed non-str (TypeError path)
                runloom.optimize(fdp.ConsumeInt(2))
            elif op == 7:
                runloom_c.Coro(fdp.ConsumeInt(1), fdp.ConsumeIntInRange(0, 1 << 18))
            else:
                runloom_c.Chan(fdp.ConsumeIntInRange(0, 4096))
        except Exception:
            # A clean Python exception on malformed input is the CORRECT, expected
            # behavior. The bug class is a process crash, which libFuzzer catches
            # directly -- not a raised exception. So swallow exceptions here.
            pass


def main():
    atheris.Setup(sys.argv, one_input)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
