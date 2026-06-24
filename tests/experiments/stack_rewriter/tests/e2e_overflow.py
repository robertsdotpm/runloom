#!/usr/bin/env python3
"""
End-to-end test of the INSTRUMENTED extension.

Demonstrates:
  (A) normal within-limit calls STILL WORK through the instrumented binary
  (B) when recursion would cross the per-thread limit, the injected software
      check fires: it prints "STACK OVERFLOW caught ..." and aborts (ud2 ->
      SIGILL) -- DETECTION BEFORE CORRUPTION, not a silent segfault.

How the limit is set: the rewriter emitted a sidecar JSON with the link-time
VA of g_stack_limit and an anchor symbol. We dlopen the module, read the anchor
symbol's RUNTIME address (via ctypes), compute the load base, and poke the
limit at base + (limit_va) using ctypes. No re-parsing of the ELF at runtime.

We run the actual overflow in a CHILD PROCESS so the SIGILL abort is observed
cleanly (the child prints the software report to stderr then dies on ud2).

Usage:
  python3 e2e_overflow.py <instrumented.so> <sidecar.json> [mode]
    mode = "normal"   -> call within limit, expect clean return (default both)
    mode = "overflow" -> drive recursion past the limit, expect software report
If no mode given, runs the child twice (normal then overflow) and reports.
"""
import ctypes
import json
import os
import struct
import sys
import importlib.util


def load_via_ctypes(so_path):
    # RTLD_NOW so all relocations are resolved; we then read an anchor symbol.
    return ctypes.CDLL(so_path, mode=os.RTLD_NOW | os.RTLD_GLOBAL)


def load_module(so_path):
    spec = importlib.util.spec_from_file_location("stacktest", so_path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def compute_limit_addr(so_path, sidecar):
    """Return the runtime address of g_stack_limit."""
    meta = json.load(open(sidecar))
    anchor = meta["anchor_symbol"]
    limit_va = meta["limit_va"]

    # link-time st_value of the anchor symbol (read from the ELF dynsym, pure
    # stdlib -- we reuse our own ELF reader to avoid shelling out).
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "rewriter"))
    from elf64 import ELF64
    e = ELF64(open(so_path, "rb").read())
    anchor_va = _dynsym_value(e, anchor)
    if anchor_va is None:
        raise RuntimeError(f"anchor symbol {anchor} not found")

    lib = load_via_ctypes(so_path)
    anchor_rt = ctypes.cast(getattr(lib, anchor), ctypes.c_void_p).value
    base = anchor_rt - anchor_va
    limit_addr = base + limit_va
    hit_addr = base + meta["hitcount_va"]
    return base, limit_addr, hit_addr, meta


def _dynsym_value(e, name):
    """Find a symbol's st_value in .dynsym (minimal, pure stdlib)."""
    # locate .dynsym and .dynstr
    dynsym = dynstr = None
    for s in e.shdrs:
        if s.name == ".dynsym":
            dynsym = s
        elif s.name == ".dynstr":
            dynstr = s
    if not dynsym or not dynstr:
        return None
    # Elf64_Sym: I(st_name) B(info) B(other) H(shndx) Q(value) Q(size) = 24
    SYM = "<IBBHQQ"
    n = dynsym.sh_size // 24
    for i in range(n):
        off = dynsym.sh_offset + i * 24
        st_name, info, other, shndx, value, size = struct.unpack_from(
            SYM, e.data, off)
        end = e.data.find(b"\x00", dynstr.sh_offset + st_name)
        nm = e.data[dynstr.sh_offset + st_name:end].decode("latin-1")
        if nm == name:
            return value
    return None


def poke_u64(addr, value):
    ctypes.memmove(ctypes.c_void_p(addr),
                   struct.pack("<Q", value & 0xFFFFFFFFFFFFFFFF), 8)


def read_u64(addr):
    buf = (ctypes.c_char * 8).from_address(addr)
    return struct.unpack("<Q", bytes(buf))[0]


def run_child(so_path, sidecar, mode):
    base, limit_addr, hit_addr, meta = compute_limit_addr(so_path, sidecar)
    m = load_module(so_path)

    print(f"[e2e] mode={mode}")
    print(f"[e2e] load base       = 0x{base:x}")
    print(f"[e2e] g_stack_limit @  = 0x{limit_addr:x} (runtime)")
    print(f"[e2e] g_hit_count   @  = 0x{hit_addr:x} (runtime)")

    # Use the EXTENSION's own rsp -- the C code may run on a different stack
    # than the Python interpreter (true on free-threaded builds), so we must
    # anchor the limit to where recurse_c actually executes.
    sp = m.current_sp()
    print(f"[e2e] extension-reported rsp = 0x{sp:x}")

    if mode == "normal":
        # Set the limit FAR below the extension's rsp so within-limit calls pass.
        limit = sp - 64 * 1024 * 1024     # 64 MiB headroom
        poke_u64(limit_addr, limit)
        print(f"[e2e] set limit = 0x{limit:x} (ext_sp - 64MiB; lots of headroom)")
        print(f"[e2e] calling big_frame(0)  -> {m.big_frame(0)}")
        print(f"[e2e] calling recurse(8)    -> {m.recurse(8)}")
        hits = read_u64(hit_addr)
        print(f"[e2e] hit_count = {hits} (expected 0 -- no overflow)")
        print("[e2e] NORMAL PATH OK: instrumented binary works within limit.")
        return 0

    if mode == "overflow":
        # Set the limit a modest distance below the extension's rsp so a bunch
        # of recurse_c frames (each ~0x1000 + call overhead) cross it WELL
        # before the real 8 MiB hardware stack is exhausted -- proving the
        # SOFTWARE check fires first.
        floor = 256 * 1024                # 256 KiB below ext rsp
        limit = sp - floor
        poke_u64(limit_addr, limit)
        print(f"[e2e] set limit = 0x{limit:x} (ext_sp - {floor//1024}KiB floor)")
        print(f"[e2e] driving recurse(100000) -- expect SOFTWARE report + abort")
        print(f"[e2e] (real hardware stack is ~8 MiB, far below this floor, so")
        print(f"[e2e]  the software check MUST trip first if it works)")
        sys.stdout.flush()
        sys.stderr.flush()
        # This should trip the injected check and ud2 (SIGILL).
        r = m.recurse(100000)
        # If we reach here, the check did NOT fire -- that's a FAILURE.
        print(f"[e2e] recurse returned {r} -- NO overflow detected (FAIL)")
        return 3

    raise SystemExit(f"unknown mode {mode}")


if __name__ == "__main__":
    so = sys.argv[1]
    sc = sys.argv[2]
    mode = sys.argv[3] if len(sys.argv) > 3 else "normal"
    sys.exit(run_child(so, sc, mode))
