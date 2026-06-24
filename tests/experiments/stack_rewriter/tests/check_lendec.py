#!/usr/bin/env python3
"""
Cross-check the pure-Python length decoder against objdump's ground-truth
instruction boundaries over every executable section of a real .so.

objdump is used ONLY here, as an oracle during development/testing. The
rewriter tool itself never shells out to it.

We parse `objdump -d --insn-width=16` output to get the (addr -> next addr)
sequence per function, then run LenDecoder linearly and compare lengths.
A mismatch or premature UNKNOWN is reported. We tolerate UNKNOWN only as a
*conservative stop* (the decoder is allowed to bail), but we flag any case
where it returns a WRONG non-UNKNOWN length -- that would be a real bug.
"""
import re
import subprocess
import sys

sys.path.insert(0, "rewriter")
from lendec import LenDecoder, UNKNOWN  # noqa: E402
from elf64 import ELF64                  # noqa: E402

LINE_RE = re.compile(r"^\s+([0-9a-f]+):\s+((?:[0-9a-f]{2} )+)\s*(.*)$")


def objdump_insns(path):
    """Return list of (addr, raw_bytes, mnemonic) from objdump."""
    out = subprocess.check_output(
        ["objdump", "-d", "--insn-width=16", path],
        text=True)
    insns = []
    for line in out.splitlines():
        m = LINE_RE.match(line)
        if not m:
            continue
        addr = int(m.group(1), 16)
        raw = bytes(int(b, 16) for b in m.group(2).split())
        mnem = m.group(3).strip()
        # objdump sometimes wraps long byte runs onto a continuation line
        # with no addr; --insn-width=16 avoids that for our small insns.
        insns.append((addr, raw, mnem))
    return insns


def main():
    path = sys.argv[1]
    e = ELF64(open(path, "rb").read())
    dec = LenDecoder(mode64=True)

    # Build a map addr -> objdump length, but only inside exec sections.
    exec_ranges = [(s.sh_addr, s.sh_addr + s.sh_size, s.sh_offset, s.name)
                   for s in e.executable_sections()]

    insns = objdump_insns(path)
    by_addr = {a: (raw, mnem) for a, raw, mnem in insns}

    total = 0
    correct = 0
    conservative_stops = 0
    wrong = []

    for a, raw, mnem in insns:
        # only check addresses that fall in an exec section we scan
        in_exec = any(lo <= a < hi for lo, hi, _, _ in exec_ranges)
        if not in_exec:
            continue
        total += 1
        # find file offset for this VA
        try:
            off = e.va_to_off(a)
        except ValueError:
            continue
        # read a generous window (max insn 15 bytes)
        window = e.read_off(off, 16)
        length, info = dec.length(window, 0)
        true_len = len(raw)
        if length == UNKNOWN:
            conservative_stops += 1
            continue
        if length == true_len:
            correct += 1
        else:
            wrong.append((a, mnem, true_len, length, raw.hex()))

    print(f"[check] section insns checked : {total}")
    print(f"[check] exact-length matches  : {correct}")
    print(f"[check] conservative UNKNOWNs : {conservative_stops}")
    print(f"[check] WRONG lengths         : {len(wrong)}")
    for a, mnem, tl, gl, hx in wrong[:50]:
        print(f"   !! 0x{a:x}  {mnem:<28} true={tl} got={gl}  [{hx}]")

    if wrong:
        print("[check] FAIL: decoder produced wrong lengths (would corrupt).")
        sys.exit(1)
    print("[check] PASS: no wrong lengths. (UNKNOWNs are safe stops.)")


if __name__ == "__main__":
    main()
