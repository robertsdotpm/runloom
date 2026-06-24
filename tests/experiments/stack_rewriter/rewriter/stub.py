"""
stub.py -- build the injected code segment: a writable limit global, a shared
report-and-abort routine, and one trampoline per instrumented site.

POSITION-INDEPENDENCE: the output .so is an ET_DYN object loaded at an
arbitrary base. We therefore must NOT bake absolute virtual addresses. All
intra-segment data references use RIP-relative addressing, and all control
transfers (trampoline <-> report, trampoline -> back to .text) use rel32
displacements computed from link-time VAs (which are base-invariant deltas).

LAYOUT of the injected segment (offsets are within the segment; seg_va is the
segment's link-time virtual address):

    +0x00  g_stack_limit : u64    (writable-at-runtime; harness pokes via ctypes
                                    -- the page is RX, but ctypes writes the
                                    file image? NO. See note below.)
    +0x08  g_hit_count   : u64
    +0x10  message bytes ...
    ...    report_and_abort:
             raw write(2,msg,len) ; inc g_hit_count ; ud2
    ...    trampoline_i: save; compute new rsp; cmp [rip+limit]; jb report;
             restore; <displaced sub rsp,imm32>; jmp back-to-.text

NOTE on writability: the harness sets the limit by writing g_stack_limit via
ctypes at the datum's RUNTIME address (module base + offset). Because the whole
injected segment is mapped RX (read+exec, NOT write), the harness instead pokes
the limit through a tiny exported helper? -- no. Simplest real scheme that
works: we map the segment RW-X is disallowed by GNU_STACK/NX on many systems
for a single segment, but R-X is fine and ctypes can still write to it IF the
page is writable. To keep the datum writable we place g_stack_limit in its own
behaviour: the injector marks the new segment PF_R|PF_W|PF_X. We keep W so the
harness can poke the limit. (Documented trade-off; a production system would
split a tiny RW data page from the RX code page.)
"""

import struct

from emit import (Emit, RAX, RDI, RSI, RDX, R11)

OFF_LIMIT = 0x00
OFF_HITCOUNT = 0x08
OFF_MSG = 0x10

SYS_write = 1


def build_segment(seg_va, sites, back_targets):
    """
    seg_va        : link-time virtual address of the injected segment.
    sites         : patchable Site objects (sub rsp,imm32).
    back_targets  : return VAs (site.va + length), parallel to sites.

    Returns (segment_bytes, meta).
    """
    msg = (b"\n*** STACK OVERFLOW caught by injected software check ***\n"
           b"    (prospective rsp would cross the per-thread limit)\n"
           b"    aborting via ud2 BEFORE the stack is corrupted.\n")

    # ---- data area ------------------------------------------------------
    data = bytearray()
    data += struct.pack("<Q", 0)        # g_stack_limit
    data += struct.pack("<Q", 0)        # g_hit_count
    assert len(data) == OFF_MSG
    msg_off = len(data)
    data += msg
    msglen = len(msg)
    while len(data) % 16:
        data.append(0x00)

    limit_off = OFF_LIMIT
    hitcount_off = OFF_HITCOUNT

    # ---- report_and_abort ----------------------------------------------
    # Emitted at a known segment offset so RIP-relative disps are exact.
    report_off = len(data)
    rep = Emit()
    # write(2, msg, msglen): rax=1, rdi=2, rsi=&msg, rdx=msglen, syscall
    rep.mov_reg_imm64(RAX, SYS_write)
    rep.mov_reg_imm64(RDI, 2)
    rep.lea_reg_rip(RSI, msg_off, report_off)        # rsi = &msg (rip-rel)
    rep.mov_reg_imm64(RDX, msglen)
    rep.syscall()
    # g_hit_count += 1 via rip-relative: load, inc, store.
    rep.mov_reg_mem_rip(RAX, hitcount_off, report_off)   # rax = [hitcount]
    rep.db(0x48, 0x83, 0xC0, 0x01)                       # add rax, 1
    # mov [rip+disp], rax : 48 89 05 <disp32>  (store rax to hitcount)
    rep.db(0x48, 0x89, 0x05)
    _store_end = report_off + len(rep.buf) + 4
    rep.buf.extend(struct.pack("<i", hitcount_off - _store_end))
    rep.ud2()
    rep_bytes = rep.get()

    # ---- trampolines ----------------------------------------------------
    # Lay them out sequentially after the report stub; each tramp's offset is
    # known so cross-segment rel32 (back to .text) and rip-relative loads are
    # computed exactly.
    tramp_blobs = []
    tramp_offs = []
    cursor_off = report_off + len(rep_bytes)

    for site, back_va in zip(sites, back_targets):
        imm = site.imm
        toff = cursor_off
        t = Emit()
        # save flags + 2 scratch regs (rsp drops 24 total)
        t.pushfq()
        t.push_reg(RAX)
        t.push_reg(R11)
        # rax = rsp at trampoline entry (undo 24 bytes)
        t.lea_reg_mem_disp32(RAX, 4, 24)     # 4 == RSP
        # rax = prospective new rsp = rsp_entry - imm
        t.sub_reg_imm32(RAX, imm)
        # r11 = [g_stack_limit]  (RIP-relative)
        t.mov_reg_mem_rip(R11, limit_off, toff)
        # cmp new_rsp(rax) vs limit(r11); overflow if rax < r11 (unsigned)
        t.cmp_reg_reg(RAX, R11)
        # placeholder for jb -> overflow; patched after we know overflow_off
        t.db(0x0F, 0x82)
        jb_disp_at = len(t.buf)
        t.buf.extend(b"\x00\x00\x00\x00")
        # ---- pass path ----
        t.pop_reg(R11)
        t.pop_reg(RAX)
        t.popfq()
        t.bytes_(site.raw)                   # displaced original sub rsp,imm32
        # jmp back to .text (cross-segment rel32). VA of this jmp:
        jmp_va = seg_va + toff + len(t.buf)
        t.db(0xE9)
        rel_back = back_va - (jmp_va + 5)
        if not (-(2**31) <= rel_back < 2**31):
            raise ValueError(f"back-edge jmp out of rel32 range: {rel_back}")
        t.buf.extend(struct.pack("<i", rel_back))
        # ---- overflow path ----
        overflow_off_in_t = len(t.buf)
        t.pop_reg(R11)
        t.pop_reg(RAX)
        t.popfq()
        # jmp report (intra-segment rel32, rip-relative by construction)
        jmp2_off_in_seg = toff + len(t.buf)
        t.db(0xE9)
        rel_report = report_off - (jmp2_off_in_seg + 5)
        t.buf.extend(struct.pack("<i", rel_report))
        # patch the jb displacement: target = overflow path
        jb_end = jb_disp_at + 4
        struct.pack_into("<i", t.buf, jb_disp_at, overflow_off_in_t - jb_end)

        blob = t.get()
        tramp_blobs.append(blob)
        tramp_offs.append(toff)
        cursor_off += len(blob)

    # ---- assemble -------------------------------------------------------
    seg = bytearray()
    seg += data
    seg += rep_bytes
    for blob in tramp_blobs:
        seg += blob

    meta = {
        "limit_va": seg_va + limit_off,
        "hitcount_va": seg_va + hitcount_off,
        "msg_va": seg_va + msg_off,
        "report_va": seg_va + report_off,
        "tramp_vas": [seg_va + o for o in tramp_offs],
        "seg_size": len(seg),
    }
    return bytes(seg), meta
