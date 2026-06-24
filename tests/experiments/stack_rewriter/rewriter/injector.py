"""
injector.py -- pure-Python ELF surgery: append a new RX PT_LOAD segment that
holds the relocated program-header table + the injected stub/trampolines, fix
the ELF header, and patch each instrumented call site with a `jmp rel32` into
its trampoline.

The classic "extend the phdr table" problem: there is normally NO slack after
the existing program headers to add another entry in place. So we relocate the
ENTIRE phdr table to the new segment at end-of-file (where we have all the room
we want), bump e_phnum, point e_phoff there, and -- crucially -- make the new
PT_LOAD's file/mem range COVER the relocated phdr table so the kernel maps it.

Loader requirements we satisfy:
  * For each PT_LOAD: (p_vaddr - p_offset) % p_align == 0      [congruence]
  * Segments don't overlap in VA; new VA is chosen above all existing.
  * The new segment is R-X (flags PF_R|PF_X). The phdr table living inside an
    RX page is fine (it's read-only data to the loader).
  * The new phdr table includes a PT_LOAD that maps itself (self-referential,
    exactly like a normal ELF's first LOAD covers the original phdrs).

Layout we build at EOF (file offset `seg_off`, virtual address `seg_va`):

    seg_off ------> [ new program-header table : e_phnum_new * 56 bytes ]
                    [ pad to 16 ]
    stub_off -----> [ injected stub+trampolines (from stub.build_segment) ]

Both regions live in ONE new PT_LOAD so they're mapped together.
"""

import struct

from elf64 import (ELF64, Phdr, PT_LOAD, PT_PHDR, PF_R, PF_W, PF_X, PHDR_SIZE)
from scanner import scan_elf
from stub import build_segment

PAGE = 0x1000


def _align_up(x, a):
    return (x + a - 1) & ~(a - 1)


def _highest_vaddr_end(elf):
    hi = 0
    for ph in elf.phdrs:
        if ph.p_type == PT_LOAD:
            hi = max(hi, ph.p_vaddr + ph.p_memsz)
    return hi


def instrument(in_path, out_path, verbose=True):
    raw = open(in_path, "rb").read()
    elf = ELF64(raw)

    if verbose:
        print(f"[inject] input : {in_path} ({len(raw)} bytes)")
        print(f"[inject] e_phoff=0x{elf.e_phoff:x} e_phnum={elf.e_phnum} "
              f"e_shoff=0x{elf.e_shoff:x}")

    # 1) scan for sites ---------------------------------------------------
    sites, agg = scan_elf(elf, verbose=verbose)
    patchable = [s for s in sites if s.patchable]
    skipped = [s for s in sites if not s.patchable]

    if verbose:
        print(f"\n[inject] === COVERAGE ===")
        print(f"[inject] stack-alloc sites found : {len(sites)}")
        print(f"[inject]   patchable (instrument) : {len(patchable)}")
        print(f"[inject]   skipped (fail-safe)    : {len(skipped)}")

    if not patchable:
        print("[inject] no patchable sites; nothing to do.")
        return None

    # 2) choose VA + file offset for the new segment ----------------------
    # File offset: append at current EOF, page-aligned for cleanliness.
    seg_file_off = _align_up(len(elf.data), PAGE)
    # Virtual address: above the highest existing segment, page-aligned, and
    # congruent with the file offset modulo PAGE (trivially true since both
    # are page-aligned).
    seg_va = _align_up(_highest_vaddr_end(elf) + PAGE, PAGE)
    # enforce congruence p_vaddr % align == p_offset % align (both 0 here)
    assert (seg_va % PAGE) == (seg_file_off % PAGE)

    # 3) build the new program-header table layout ------------------------
    # The new phdr table will sit at the very start of the new segment.
    # We add ONE new PT_LOAD entry, so the new count is e_phnum + 1.
    new_phnum = elf.e_phnum + 1
    phtab_bytes_len = new_phnum * PHDR_SIZE
    phtab_off = seg_file_off
    phtab_va = seg_va

    # stub goes after the phdr table, padded to 16.
    stub_off = _align_up(phtab_off + phtab_bytes_len, 16)
    stub_va = seg_va + (stub_off - seg_file_off)

    if verbose:
        print(f"\n[inject] new segment VA  = 0x{seg_va:x}")
        print(f"[inject] new segment off = 0x{seg_file_off:x}")
        print(f"[inject] relocated phdr table: off=0x{phtab_off:x} "
              f"va=0x{phtab_va:x} count={new_phnum}")
        print(f"[inject] stub: off=0x{stub_off:x} va=0x{stub_va:x}")

    # 4) build the stub at its final VA -----------------------------------
    back_targets = [s.va + s.length for s in patchable]
    stub_bytes, meta = build_segment(stub_va, patchable, back_targets)
    stub_end_off = stub_off + len(stub_bytes)
    seg_filesz = stub_end_off - seg_file_off
    seg_memsz = seg_filesz

    # 5) construct the new PT_LOAD phdr (R-W-X) ---------------------------
    # We keep W so the test harness can poke g_stack_limit via ctypes at the
    # datum's runtime address. (Documented trade-off: a production build would
    # split a tiny RW data page from the RX code page so code stays W^X. See
    # README "limitations".)
    new_load = Phdr(
        p_type=PT_LOAD,
        p_flags=PF_R | PF_W | PF_X,
        p_offset=seg_file_off,
        p_vaddr=seg_va,
        p_paddr=seg_va,
        p_filesz=seg_filesz,
        p_memsz=seg_memsz,
        p_align=PAGE,
    )

    # 6) update the in-memory phdr list -----------------------------------
    # Also fix the PT_PHDR entry (if present) to point at the new table so a
    # dynamic loader using PT_PHDR finds the right vector. The PT_PHDR must be
    # covered by a PT_LOAD -- our new RX LOAD covers it.
    for ph in elf.phdrs:
        if ph.p_type == PT_PHDR:
            ph.p_offset = phtab_off
            ph.p_vaddr = phtab_va
            ph.p_paddr = phtab_va
            ph.p_filesz = phtab_bytes_len
            ph.p_memsz = phtab_bytes_len
            if verbose:
                print(f"[inject] fixed PT_PHDR -> va=0x{phtab_va:x} "
                      f"filesz=0x{phtab_bytes_len:x}")

    elf.phdrs.append(new_load)
    elf.e_phnum = len(elf.phdrs)
    assert elf.e_phnum == new_phnum

    # 7) lay out the file --------------------------------------------------
    # Pad file to seg_file_off, then place phdr table, pad, then stub.
    elf.pad_to(PAGE)
    # ensure we are exactly at seg_file_off (pad_to may overshoot if EOF was
    # already aligned; recompute)
    while len(elf.data) < seg_file_off:
        elf.data.append(0x00)
    assert len(elf.data) == seg_file_off, (len(elf.data), seg_file_off)

    # reserve space for the phdr table (write zeros now; finalize() writes
    # the actual phdr bytes at e_phoff).
    elf.data.extend(b"\x00" * (stub_off - seg_file_off))
    assert len(elf.data) == stub_off
    elf.data.extend(stub_bytes)

    # 8) point e_phoff at the relocated table -----------------------------
    elf.set_phoff(phtab_off)

    # 9) patch each instrumented call site --------------------------------
    # Overwrite the 7 original bytes with: E9 <rel32> (5 bytes) + 2x NOP.
    # rel32 = tramp_va - (site_va + 5)
    if verbose:
        print(f"\n[inject] === PATCHING CALL SITES ===")
    for s, tramp_va in zip(patchable, meta["tramp_vas"]):
        rel = tramp_va - (s.va + 5)
        if not (-(2**31) <= rel < 2**31):
            raise ValueError(f"site 0x{s.va:x}: jmp out of rel32 range")
        patch = b"\xE9" + struct.pack("<i", rel) + b"\x90\x90"
        assert len(patch) == s.length == 7
        elf.patch_off(s.off, patch)
        if verbose:
            print(f"[inject]   VA 0x{s.va:08x}: {s.raw.hex()} -> {patch.hex()}"
                  f"  (jmp -> tramp 0x{tramp_va:x})")

    # 10) finalize: repack header + phdrs ---------------------------------
    out = elf.finalize()
    with open(out_path, "wb") as f:
        f.write(out)

    # 11) write a sidecar with the injected datum offsets so a harness can
    #     poke g_stack_limit at runtime without re-parsing the ELF.
    sidecar = out_path + ".inject.json"
    import json
    with open(sidecar, "w") as f:
        json.dump({
            "limit_va": meta["limit_va"],
            "hitcount_va": meta["hitcount_va"],
            "report_va": meta["report_va"],
            "msg_va": meta["msg_va"],
            "seg_va": seg_va,
            "tramp_vas": meta["tramp_vas"],
            "anchor_symbol": "PyInit_stacktest",
            "instrumented_sites": [
                {"va": s.va, "imm": s.imm} for s in patchable],
        }, f, indent=2)
    if verbose:
        print(f"[inject] wrote sidecar {sidecar}")

    if verbose:
        print(f"\n[inject] wrote {out_path} ({len(out)} bytes)")
        print(f"[inject] limit_va    = 0x{meta['limit_va']:x}  "
              f"(poke this to set the per-thread stack floor)")
        print(f"[inject] hitcount_va = 0x{meta['hitcount_va']:x}")
        print(f"[inject] report_va   = 0x{meta['report_va']:x}")

    meta["sites"] = sites
    meta["patchable"] = patchable
    meta["skipped"] = skipped
    meta["agg"] = agg
    meta["seg_va"] = seg_va
    return meta
