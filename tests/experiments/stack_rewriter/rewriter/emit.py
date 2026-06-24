"""
emit.py -- hand-rolled x86-64 machine-code emitter (the bytes we INJECT).

Pure Python; we assemble raw bytes with struct/bytearray. Only the handful of
instruction forms the trampoline + report stub need are implemented. Every
encoding below is commented with its exact byte layout so it can be audited
against the Intel SDM.

Register numbering (for REX + ModRM):
    rax=0 rcx=1 rdx=2 rbx=3 rsp=4 rbp=5 rsi=6 rdi=7
    r8=8  r9=9  r10=10 r11=11 r12=12 r13=13 r14=14 r15=15
"""

import struct

RAX, RCX, RDX, RBX, RSP, RBP, RSI, RDI = range(8)
R8, R9, R10, R11, R12, R13, R14, R15 = range(8, 16)


def _rex(w=0, r=0, x=0, b=0):
    return 0x40 | (w << 3) | (r << 2) | (x << 1) | b


def _modrm(mod, reg, rm):
    return (mod << 6) | ((reg & 7) << 3) | (rm & 7)


class Emit:
    def __init__(self):
        self.buf = bytearray()
        # fixups: list of (offset_in_buf, label, kind) to patch rel32 later
        self.fixups = []
        self.labels = {}

    # -- raw helpers ------------------------------------------------------
    def db(self, *bs):
        self.buf.extend(bs)

    def bytes_(self, b):
        self.buf.extend(b)

    def here(self):
        return len(self.buf)

    def label(self, name):
        self.labels[name] = len(self.buf)

    # -- stack ------------------------------------------------------------
    def pushfq(self):
        self.db(0x9C)

    def popfq(self):
        self.db(0x9D)

    def push_reg(self, r):
        # 50+rd ; REX.B for r8..r15
        if r >= 8:
            self.db(_rex(b=1))
        self.db(0x50 + (r & 7))

    def pop_reg(self, r):
        if r >= 8:
            self.db(_rex(b=1))
        self.db(0x58 + (r & 7))

    # -- mov --------------------------------------------------------------
    def mov_reg_reg(self, dst, src):
        # 48 89 /r : mov r/m64, r64  (src in reg field, dst in r/m)
        rex = _rex(w=1, r=(1 if src >= 8 else 0), b=(1 if dst >= 8 else 0))
        self.db(rex, 0x89, _modrm(3, src, dst))

    def mov_reg_imm64(self, dst, imm):
        # REX.W B8+rd io : mov r64, imm64
        rex = _rex(w=1, b=(1 if dst >= 8 else 0))
        self.db(rex, 0xB8 + (dst & 7))
        self.buf.extend(struct.pack("<Q", imm & 0xFFFFFFFFFFFFFFFF))

    def mov_reg_mem_rip(self, dst, target_off, seg_base_off):
        """
        mov r64, [rip + disp32]  (48 8B /r, mod=00, rm=101 = RIP-rel).
        `target_off`   : offset (within the segment) of the datum to load.
        `seg_base_off` : offset (within the segment) where THIS emitter's bytes
                         will be placed, so rip = seg_base_off + here() + insn_len.
        Position-INDEPENDENT: the displacement is segment-internal, so it is
        correct at any load base.
        """
        rex = _rex(w=1, r=(1 if dst >= 8 else 0))
        self.db(rex, 0x8B, _modrm(0, dst, 5))   # rm=5 -> RIP-relative
        # rip points at the byte AFTER the 4-byte disp.
        insn_end_off = seg_base_off + len(self.buf) + 4
        disp = target_off - insn_end_off
        self.buf.extend(struct.pack("<i", disp))

    def lea_reg_rip(self, dst, target_off, seg_base_off):
        """lea r64, [rip + disp32]  (48 8D /r, mod=00 rm=101)."""
        rex = _rex(w=1, r=(1 if dst >= 8 else 0))
        self.db(rex, 0x8D, _modrm(0, dst, 5))
        insn_end_off = seg_base_off + len(self.buf) + 4
        disp = target_off - insn_end_off
        self.buf.extend(struct.pack("<i", disp))

    def jmp_rip_abs_off(self, target_off, seg_base_off):
        """
        Position-independent jump to a segment-internal offset, expressed as a
        direct E9 rel32 (rip-relative by construction).
        """
        self.db(0xE9)
        insn_end_off = seg_base_off + len(self.buf) + 4
        disp = target_off - insn_end_off
        self.buf.extend(struct.pack("<i", disp))

    def mov_reg_mem_at_reg(self, dst, base):
        # 48 8B /r : mov r64, [base]  (mod=00, rm=base). base must not be
        # rsp/rbp/r12/r13 for the simple no-SIB/no-disp form; we use a disp0
        # SIB-free form only for "clean" bases. For [rip]-style we don't need.
        # Here base is a general reg holding an absolute address.
        assert (base & 7) != RSP, "use disp form for rsp base"
        assert (base & 7) != RBP, "rbp base needs disp; use mov_reg_mem_disp"
        rex = _rex(w=1, r=(1 if dst >= 8 else 0), b=(1 if base >= 8 else 0))
        self.db(rex, 0x8B, _modrm(0, dst, base))

    # -- arithmetic / compare --------------------------------------------
    def sub_reg_imm32(self, dst, imm):
        # REX.W 81 /5 id : sub r/m64, imm32  (reg field = 5)
        rex = _rex(w=1, b=(1 if dst >= 8 else 0))
        self.db(rex, 0x81, _modrm(3, 5, dst))
        self.buf.extend(struct.pack("<I", imm & 0xFFFFFFFF))

    def cmp_reg_reg(self, a, b):
        # 48 39 /r : cmp r/m64, r64  (b in reg field, a in r/m) -> sets flags
        # for (a - b). We want to compare a vs b; use cmp a, b.
        rex = _rex(w=1, r=(1 if b >= 8 else 0), b=(1 if a >= 8 else 0))
        self.db(rex, 0x39, _modrm(3, b, a))

    def lea_reg_mem_disp32(self, dst, base, disp):
        # 48 8D /r : lea r64, [base + disp32]  (mod=10)
        rex = _rex(w=1, r=(1 if dst >= 8 else 0), b=(1 if base >= 8 else 0))
        self.db(rex, 0x8D, _modrm(2, dst, base))
        if (base & 7) == RSP:
            self.db(0x24)   # SIB for rsp base: scale=0 index=none base=rsp
        self.buf.extend(struct.pack("<i", disp))

    # -- control flow -----------------------------------------------------
    def jmp_rel32_label(self, label):
        # E9 cd
        self.db(0xE9)
        self.fixups.append((len(self.buf), label, "rel32"))
        self.buf.extend(b"\x00\x00\x00\x00")

    def jb_rel32_label(self, label):
        # 0F 82 cd : jb (below, unsigned) -- taken when CF=1 (a < b unsigned)
        self.db(0x0F, 0x82)
        self.fixups.append((len(self.buf), label, "rel32"))
        self.buf.extend(b"\x00\x00\x00\x00")

    def ja_rel32_label(self, label):
        # 0F 87 cd : ja (above, unsigned)
        self.db(0x0F, 0x87)
        self.fixups.append((len(self.buf), label, "rel32"))
        self.buf.extend(b"\x00\x00\x00\x00")

    def syscall(self):
        self.db(0x0F, 0x05)

    def ud2(self):
        self.db(0x0F, 0x0B)

    def nop(self, count=1):
        self.db(*([0x90] * count))

    # -- finalisation -----------------------------------------------------
    def resolve(self):
        """Patch all rel32 fixups now that labels are known."""
        for at, label, kind in self.fixups:
            if label not in self.labels:
                raise KeyError(f"undefined label {label!r}")
            target = self.labels[label]
            rel = target - (at + 4)   # rel32 is relative to end of the disp
            struct.pack_into("<i", self.buf, at, rel)
        self.fixups = []

    def get(self):
        return bytes(self.buf)
