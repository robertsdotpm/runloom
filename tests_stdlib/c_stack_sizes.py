#!/usr/bin/env python3
"""Catalog every CPython stdlib C function and its C-stack FRAME size.

Why: a runloom goroutine runs on a small swapped C stack; a single C function
whose prologue frame is bigger than that overflows the guard page -> SIGSEGV
(BUG-001's "large C call" face -- e.g. select_select_impl's ~49 KB FD_SETSIZE
arrays).  This enumerates the danger surface so the fat single frames are known
up front instead of discovered by a crash.

Method: read each ELF's `.eh_frame` CFI and take the maximum CFA offset over a
function's unwind table (minus 8 for the return address) = the deepest the stack
pointer goes = the function's prologue frame size, INCLUDING -fstack-clash
big-frame allocations.  This is the *per-function frame*, NOT the cumulative
call-chain depth (which is unbounded under recursion).

Covers libpython (interpreter core + built-in modules) and every lib-dynload
extension module (.so).  Output: stdlib_c_stack_sizes.json.

Usage: python3.13t tests_stdlib/c_stack_sizes.py [output.json]
"""
import datetime
import glob
import json
import os
import sys
import sysconfig

from elftools.elf.elffile import ELFFile
from elftools.dwarf.callframe import FDE


def func_symbols(elf):
    """addr -> function name, from .symtab (richest; includes statics) then
    .dynsym."""
    m = {}
    for secname in (".symtab", ".dynsym"):
        sec = elf.get_section_by_name(secname)
        if sec is None:
            continue
        for s in sec.iter_symbols():
            if s["st_info"]["type"] == "STT_FUNC" and s["st_value"]:
                m.setdefault(s["st_value"], s.name)
    return m


def frame_sizes(path):
    """name -> max prologue frame size in bytes, for one ELF binary."""
    out = {}
    with open(path, "rb") as f:
        elf = ELFFile(f)
        if not elf.has_dwarf_info():
            return out
        sym = func_symbols(elf)
        dw = elf.get_dwarf_info()
        if not dw.has_EH_CFI():
            return out
        for e in dw.EH_CFI_entries():
            if not isinstance(e, FDE):
                continue
            loc = e.header["initial_location"]
            try:
                rows = e.get_decoded().table
            except Exception:
                continue
            mx = 0
            for r in rows:
                cfa = r.get("cfa")
                # rsp/rbp-relative CFA: the offset is how far CFA is above the
                # current SP; its max over the function is frame + return addr.
                off = getattr(cfa, "offset", None) if cfa is not None else None
                if off and off > mx:
                    mx = off
            name = sym.get(loc) or ("sub_%x" % loc)
            fb = max(0, mx - 8)
            if fb > out.get(name, -1):
                out[name] = fb
    return out


def module_of(path):
    base = os.path.basename(path)
    if base.startswith("libpython"):
        return "libpython"
    # foo.cpython-313t-x86_64-linux-gnu.so -> foo
    return base.split(".", 1)[0]


def binaries():
    libdir = sysconfig.get_config_var("LIBDIR") or ""
    dyn = sysconfig.get_config_var("EXT_SUFFIX")  # noqa: F841
    bins = []
    # libpython shared object (interpreter core + built-in modules)
    for cand in glob.glob(os.path.join(libdir, "libpython3.*.so*")):
        if os.path.isfile(cand) and not os.path.islink(cand):
            bins.append(cand)
    # lib-dynload extension modules
    dynload = os.path.join(libdir, "python%d.%d%s" % (
        sys.version_info[0], sys.version_info[1],
        "t" if sysconfig.get_config_var("Py_GIL_DISABLED") else ""),
        "lib-dynload")
    bins += sorted(glob.glob(os.path.join(dynload, "*.so")))
    # de-dup
    seen, uniq = set(), []
    for b in bins:
        rp = os.path.realpath(b)
        if rp not in seen:
            seen.add(rp)
            uniq.append(b)
    return uniq


def main():
    out_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "stdlib_c_stack_sizes.json")
    funcs = []
    bins = binaries()
    for b in bins:
        mod = module_of(b)
        for name, fb in frame_sizes(b).items():
            funcs.append({"name": name, "module": mod, "stack_bytes": fb})
    funcs.sort(key=lambda d: -d["stack_bytes"])

    doc = {
        "meta": {
            "generated_utc": datetime.datetime.now(
                datetime.timezone.utc).isoformat(),
            "python": "%d.%d.%d%s" % (
                sys.version_info[0], sys.version_info[1], sys.version_info[2],
                "t" if sysconfig.get_config_var("Py_GIL_DISABLED") else ""),
            "platform": sysconfig.get_platform(),
            "method": ("max .eh_frame CFA offset minus 8 (return address); "
                       "per-function PROLOGUE frame size in bytes, incl. "
                       "-fstack-clash big frames. NOT cumulative call depth."),
            "binaries": [os.path.basename(b) for b in bins],
            "function_count": len(funcs),
            "default_goroutine_stack_bytes": 32 * 1024,
        },
        # quick actionable subset: frames that alone overflow / threaten a
        # 32 KB goroutine stack.
        "fat_frames_over_8k": [d for d in funcs if d["stack_bytes"] >= 8 * 1024],
        "functions": funcs,
    }
    with open(out_path, "w") as f:
        json.dump(doc, f, indent=1)
    print("wrote %s" % out_path)
    print("  %d functions across %d binaries" % (len(funcs), len(bins)))
    print("  fat frames (>=8K): %d ; (>=32K, overflow a default g-stack alone): %d"
          % (len(doc["fat_frames_over_8k"]),
             sum(1 for d in funcs if d["stack_bytes"] >= 32 * 1024)))
    print("  top 8:")
    for d in funcs[:8]:
        print("    %8d (%.1fK)  %-28s [%s]"
              % (d["stack_bytes"], d["stack_bytes"] / 1024.0, d["name"], d["module"]))


if __name__ == "__main__":
    main()
