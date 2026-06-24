#!/usr/bin/env python3
"""
stackrewrite.py -- pure-Python (stdlib only) static stack-overflow rewriter.

Usage:
    python3 stackrewrite.py <input.so> <output.so>
    python3 stackrewrite.py --scan <input.so>        # report sites only

Inserts software stack-overflow checks into x86-64 ELF shared objects by
detouring each `sub rsp,imm32` frame-allocation to an injected trampoline that
compares the prospective rsp against a per-thread limit and aborts (with a
software report) BEFORE the stack is corrupted.

NO external dependencies. Only the Python standard library is used.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from elf64 import ELF64                 # noqa: E402
from scanner import scan_elf            # noqa: E402
from injector import instrument         # noqa: E402

BANNER = r"""
============================================================
  stackrewrite -- pure-Python x86-64 static stack-check
  injector (stdlib only; NO capstone/keystone/pyelftools)
============================================================
"""


def cmd_scan(path):
    print(BANNER)
    elf = ELF64(open(path, "rb").read())
    sites, agg = scan_elf(elf, verbose=True)
    patchable = [s for s in sites if s.patchable]
    print("\n=== SITES ===")
    for s in sites:
        print("  ", s)
    print("\n=== COVERAGE ===")
    print(f"  exec instructions linearly decoded : {agg['insns']}")
    print(f"  conservative scan stops            : {agg['stops']}")
    print(f"  stack-alloc sites found            : {len(sites)}")
    print(f"  patchable (would instrument)       : {len(patchable)}")
    print(f"  skipped (fail-safe)                : {len(sites)-len(patchable)}")
    if sites:
        pct = 100.0 * len(patchable) / len(sites)
        print(f"  coverage of recognised sites       : {pct:.1f}%")


def cmd_rewrite(in_path, out_path):
    print(BANNER)
    meta = instrument(in_path, out_path, verbose=True)
    if meta is None:
        print("[stackrewrite] no patchable sites; output not written.")
        sys.exit(2)
    sites = meta["sites"]
    patchable = meta["patchable"]
    print("\n============================================================")
    print("  FINAL COVERAGE")
    print("============================================================")
    print(f"  stack-alloc sites found      : {len(sites)}")
    print(f"  instrumented (sub rsp,imm32) : {len(patchable)}")
    print(f"  skipped (fail-safe)          : {len(sites)-len(patchable)}")
    if sites:
        pct = 100.0 * len(patchable) / len(sites)
        print(f"  coverage of recognised sites : {pct:.1f}%")
    print(f"  scan stops (unknown insns)   : {meta['agg']['stops']}")
    print("\n  Skipped sites (and why):")
    for s in meta["skipped"]:
        print(f"    VA 0x{s.va:08x} {s.kind:14s} -- {s.reason}")
    print("\n  Harness pokes:")
    print(f"    g_stack_limit @ VA 0x{meta['limit_va']:x}")
    print(f"    g_hit_count   @ VA 0x{meta['hitcount_va']:x}")
    print(f"  Output: {out_path}")


def main():
    args = sys.argv[1:]
    if len(args) == 2 and args[0] == "--scan":
        cmd_scan(args[1])
    elif len(args) == 2:
        cmd_rewrite(args[0], args[1])
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
