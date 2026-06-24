"""
lendec.py -- MINIMAL x86 instruction LENGTH decoder.

This is NOT a disassembler. It does one job: given a byte cursor inside a
code stream, return the LENGTH (in bytes) of the instruction at that cursor,
or signal "I am not confident" so the scanner can STOP and conservatively
skip the rest of the run.

We model enough of the x86 encoding pipeline to compute lengths for the
common forms emitted by a C compiler in function prologues/bodies:

    [legacy prefixes]* [REX]? opcode [ModRM [SIB]]? [disp]? [imm]?

Coverage (64-bit mode, the only one we TEST):
  * legacy prefix groups 1-4 (lock/rep, segment, 0x66 operand-size,
    0x67 address-size)
  * REX prefixes 0x40-0x4f
  * 1-byte opcodes, the 0x0F two-byte map, and the 0x0F38 / 0x0F3A
    three-byte maps (enough to length them; immediates handled for 3A)
  * ModRM/SIB/displacement computation (the part everyone gets wrong)
  * per-opcode immediate sizes for the common opcodes, with operand-size
    and the special "Iz" rule (66 -> imm16) handled

When we meet an opcode whose immediate size we do not positively know, we
return UNKNOWN. FAIL-SAFE: the caller then stops scanning that run. We would
rather skip real sites than mis-length one byte and corrupt everything after.

A 32-bit mode flag exists for structural completeness but is NOT validated
(see README). The only difference wired in is the absence of REX and the
default address size; 64-bit is what we exercise.
"""

# Return sentinel for "cannot length this with confidence".
UNKNOWN = -1

# Legacy prefix bytes (groups 1-4).
_LEGACY_PREFIXES = frozenset({
    0xF0,  # LOCK
    0xF2,  # REPNE / scalar-double prefix
    0xF3,  # REP / scalar-single prefix
    0x2E, 0x36, 0x3E, 0x26, 0x64, 0x65,  # segment overrides (incl FS/GS)
    0x66,  # operand-size override
    0x67,  # address-size override
})


# --- immediate-size tables for the 1-byte opcode map ----------------------
# Values are immediate size in bytes for the DEFAULT operand size (32-bit
# operand in 64-bit mode). "z" opcodes shrink to 2 bytes under a 0x66 prefix;
# we encode that by listing them in _IMM_Z. "v"/address forms are rare in the
# compiler output we target and are handled where they occur.
#
# Anything NOT in these tables and not handled structurally below yields
# UNKNOWN (fail-safe).

# opcodes with an 8-bit immediate (Ib)
_IMM_IB = frozenset({
    0x04, 0x0C, 0x14, 0x1C, 0x24, 0x2C, 0x34, 0x3C,  # ALU al, ib
    0x6A,                                            # push ib
    0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77,  # jcc rel8
    0x78, 0x79, 0x7A, 0x7B, 0x7C, 0x7D, 0x7E, 0x7F,
    0x80,                                            # grp1 Eb,Ib (modrm)
    0x82,                                            # grp1 (legacy)
    0x83,                                            # grp1 Ev,Ib (modrm)
    0xA8,                                            # test al,ib
    0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7,  # mov r8,ib
    0xC0, 0xC1,                                      # shift grp2 Ev,Ib (modrm)
    0xC6,                                            # mov Eb,Ib (modrm)
    0xCD,                                            # int ib
    0xD4, 0xD5,                                      # aam/aad ib (legacy)
    0xE4, 0xE5, 0xE6, 0xE7,                          # in/out ib
    0xEB,                                            # jmp rel8
})

# opcodes with a "z" immediate: 4 bytes normally, 2 bytes under 66 prefix
_IMM_IZ = frozenset({
    0x05, 0x0D, 0x15, 0x1D, 0x25, 0x2D, 0x35, 0x3D,  # ALU eax, iz
    0x68,                                            # push iz
    0x69,                                            # imul Gv,Ev,Iz (modrm)
    0x81,                                            # grp1 Ev,Iz (modrm)
    0xA9,                                            # test eax,iz
    0xC7,                                            # mov Ev,Iz (modrm)
})

# near call/jmp rel: in 64-bit mode the operand size is FORCED to 64-bit, so
# the relative is ALWAYS 32 bits -- a leading 0x66 does NOT shrink it. (This
# matters for TLS-GD sequences like `66 66 48 E8 <rel32>`.)  We handle these
# separately so the 66-prefix rule never applies.
_REL32_ALWAYS = frozenset({
    0xE8, 0xE9,                                      # call/jmp rel32
})

# opcodes (no ModRM) carrying NO immediate -- single-byte instructions
_NO_IMM_NOMODRM = frozenset({
    0x06, 0x07, 0x0E, 0x16, 0x17, 0x1E, 0x1F,        # legacy push/pop seg
    0x27, 0x2F, 0x37, 0x3F,                          # daa/das/aaa/aas legacy
    0x90,                                            # nop / xchg eax (pause=F3)
    0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97,        # xchg eax,r
    0x98, 0x99,                                      # cwde/cdqe, cdq/cqo
    0x9B, 0x9C, 0x9D, 0x9E, 0x9F,                    # fwait/pushf/popf/sahf/lahf
    0xA4, 0xA5, 0xA6, 0xA7,                          # movs/cmps
    0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF,              # stos/lods/scas
    0xC3, 0xCB,                                      # ret/retf (no imm)
    0xC9,                                            # leave
    0xCC,                                            # int3
    0xCE, 0xCF,                                      # into/iret
    0xD7,                                            # xlat
    0xEC, 0xED, 0xEE, 0xEF,                          # in/out dx
    0xF1, 0xF4, 0xF5,                                # int1/hlt/cmc
    0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD,              # clc/stc/cli/sti/cld/std
    0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57,  # push r
    0x58, 0x59, 0x5A, 0x5B, 0x5C, 0x5D, 0x5E, 0x5F,  # pop r
})

# 1-byte opcodes that have a ModRM byte but NO immediate (the big ALU group).
_MODRM_NO_IMM = frozenset({
    # ALU Eb/Ev,Gb/Gv and reverse, for add/or/adc/sbb/and/sub/xor/cmp
    0x00, 0x01, 0x02, 0x03,
    0x08, 0x09, 0x0A, 0x0B,
    0x10, 0x11, 0x12, 0x13,
    0x18, 0x19, 0x1A, 0x1B,
    0x20, 0x21, 0x22, 0x23,
    0x28, 0x29, 0x2A, 0x2B,
    0x30, 0x31, 0x32, 0x33,
    0x38, 0x39, 0x3A, 0x3B,
    0x84, 0x85,                                      # test Eb/Ev,Gb/Gv
    0x86, 0x87,                                      # xchg
    0x88, 0x89, 0x8A, 0x8B,                          # mov
    0x8D,                                            # lea
    0x8F,                                            # pop Ev (grp1a)
    0x63,                                            # movsxd Gv,Ed
    0xD0, 0xD1, 0xD2, 0xD3,                          # shift grp2 by 1/cl
    0xFE,                                            # grp4 inc/dec Eb
    0xFF,                                            # grp5 inc/dec/call/jmp/push
})

# mov r64,imm64 family: B8..BF carry a "v" immediate = 8 bytes under REX.W,
# else 4 (or 2 under 66). Handled specially below.

# 0F two-byte map opcodes that carry a ModRM but no immediate -- the common
# ones a compiler emits. (movzx/movsx, setcc, cmovcc, imul, the SSE move/alu
# forms, bt/bsf/bsr, etc.)  Anything else in the 0F map -> UNKNOWN.
_0F_MODRM_NO_IMM = frozenset({
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,  # SSE mov*
    0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F,  # prefetch/hint/nop Ev
    0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F,  # movaps/cvt*/ucomis
    0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47,  # cmovcc
    0x48, 0x49, 0x4A, 0x4B, 0x4C, 0x4D, 0x4E, 0x4F,
    0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57,  # movmsk/sqrt/and*/xor*
    0x58, 0x59, 0x5A, 0x5B, 0x5C, 0x5D, 0x5E, 0x5F,  # add/mul/cvt/sub/min/div*
    0x60, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67,  # punpck*/packs*
    0x68, 0x69, 0x6A, 0x6B, 0x6C, 0x6D, 0x6E, 0x6F,  # punpck/movd/movq/movdqa
    0x74, 0x75, 0x76,                                # pcmpeq*
    0x7E, 0x7F,                                      # movd/movq store
    0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97,  # setcc Eb
    0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F,
    0xA3,                                            # bt Ev,Gv
    0xAB,                                            # bts
    0xAF,                                            # imul Gv,Ev
    0xB0, 0xB1,                                      # cmpxchg
    0xB3,                                            # btr
    0xB6, 0xB7,                                      # movzx
    0xBB,                                            # btc
    0xBC, 0xBD,                                      # bsf/bsr (also tzcnt/lzcnt)
    0xBE, 0xBF,                                      # movsx
    0xC0, 0xC1,                                      # xadd
    0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7,  # psr*/paddq/pmullw/movq
    0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF,  # psub*/pand*/pmins
    0xE0, 0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7,
    0xE8, 0xE9, 0xEA, 0xEB, 0xEC, 0xED, 0xEE, 0xEF,
    0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7,
    0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE,        # padd*
})

# 0F map opcodes with ModRM AND an 8-bit immediate
_0F_MODRM_IB = frozenset({
    0x70, 0x71, 0x72, 0x73,                          # pshuf*/ps*l/r Ib
    0xA4,                                            # shld Ev,Gv,Ib
    0xAC,                                            # shrd Ev,Gv,Ib
    0xC2,                                            # cmpps/cmpss Ib
    0xC4, 0xC5, 0xC6,                                # pinsrw/pextrw/shufps Ib
    0xBA,                                            # grp8 bt*,Ib
})

# 0F map opcodes that are jcc rel32 (no ModRM, 4-byte rel)
_0F_JCC = frozenset(range(0x80, 0x90))               # 0F 80..8F

# 0F map opcodes with NO ModRM and NO immediate
_0F_NO_MODRM_NO_IMM = frozenset({
    0x05,                                            # syscall
    0x06, 0x07,                                      # clts/sysret
    0x08, 0x09, 0x0B,                                # invd/wbinvd/ud2
    0x0E,                                            # femms
    0x30, 0x31, 0x32, 0x33, 0x34, 0x35,              # wrmsr/rdtsc/rdmsr/.../sysenter
    0x77,                                            # emms
    0xA0, 0xA1, 0xA8, 0xA9,                          # push/pop fs/gs
    0xAA,                                            # rsm
    0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF,  # bswap r
    0x0D,                                            # prefetch group is modrm actually; keep out
}) - {0x0D}


class LenDecoder:
    def __init__(self, mode64=True):
        self.mode64 = mode64

    def _modrm_extra(self, buf, i, addr_size_32):
        """
        Given buf[i] is the ModRM byte, return the number of bytes consumed by
        ModRM + optional SIB + displacement, or UNKNOWN if out of range.
        Works for 64-bit (and 32-bit, identical ModRM rules) addressing.
        """
        if i >= len(buf):
            return UNKNOWN
        modrm = buf[i]
        mod = (modrm >> 6) & 3
        rm = modrm & 7
        n = 1  # the ModRM byte itself

        if mod == 3:
            return n  # register-direct: no SIB/disp

        # memory forms
        if addr_size_32 and False:
            pass  # 16-bit addressing not modelled (not emitted in our targets)

        if rm == 4:
            # SIB byte present
            if i + n >= len(buf):
                return UNKNOWN
            sib = buf[i + n]
            n += 1
            base = sib & 7
            if mod == 0 and base == 5:
                n += 4   # disp32 (no base)
            elif mod == 1:
                n += 1   # disp8
            elif mod == 2:
                n += 4   # disp32
            # mod==0 with base!=5 -> no disp
            return n

        if mod == 0 and rm == 5:
            # RIP-relative (64-bit) / disp32 (32-bit): 4-byte displacement
            n += 4
            return n

        if mod == 1:
            n += 1
        elif mod == 2:
            n += 4
        return n

    def length(self, buf, start):
        """
        Compute the length of the instruction at buf[start].
        Returns (length, info_dict) or (UNKNOWN, info_dict).
        info_dict carries decoded fields useful to the recognizer.
        """
        i = start
        n = len(buf)
        info = {
            "prefixes": [],
            "rex": None,
            "opsize16": False,
            "addrsize32": False,
            "rex_w": False,
            "opcode": None,
            "twobyte": False,
            "modrm_off": None,
        }

        # --- legacy prefixes ---
        while i < n and buf[i] in _LEGACY_PREFIXES:
            p = buf[i]
            info["prefixes"].append(p)
            if p == 0x66:
                info["opsize16"] = True
            elif p == 0x67:
                info["addrsize32"] = True
            i += 1
            if i - start > 14:        # x86 max instruction length is 15 bytes
                return UNKNOWN, info

        if i >= n:
            return UNKNOWN, info

        # --- REX (64-bit only) ---
        if self.mode64 and 0x40 <= buf[i] <= 0x4F:
            rex = buf[i]
            info["rex"] = rex
            info["rex_w"] = bool(rex & 0x08)
            i += 1
            if i >= n:
                return UNKNOWN, info

        # --- opcode ---
        op = buf[i]
        i += 1

        if op == 0x0F:
            # two- or three-byte map
            if i >= n:
                return UNKNOWN, info
            op2 = buf[i]
            i += 1
            info["twobyte"] = True
            info["opcode"] = op2

            if op2 in (0x38, 0x3A):
                # three-byte maps 0F38 / 0F3A: opcode3 + ModRM (+ Ib for 3A)
                if i >= n:
                    return UNKNOWN, info
                i += 1                      # third opcode byte
                info["modrm_off"] = i
                extra = self._modrm_extra(buf, i, info["addrsize32"])
                if extra == UNKNOWN:
                    return UNKNOWN, info
                i += extra
                if op2 == 0x3A:             # 0F3A forms carry an Ib
                    i += 1
                return (i - start), info

            # standard 0F two-byte
            if op2 in _0F_JCC:
                # jcc rel32: in 64-bit mode the rel is always 32-bit (the
                # 0x66 prefix does not shrink branch displacements).
                imm = 2 if (info["opsize16"] and not self.mode64) else 4
                i += imm
                return (i - start) if i <= n else UNKNOWN, info

            if op2 in _0F_NO_MODRM_NO_IMM:
                return (i - start), info

            if op2 in _0F_MODRM_NO_IMM or op2 in _0F_MODRM_IB:
                info["modrm_off"] = i
                extra = self._modrm_extra(buf, i, info["addrsize32"])
                if extra == UNKNOWN:
                    return UNKNOWN, info
                i += extra
                if op2 in _0F_MODRM_IB:
                    i += 1
                return (i - start) if i <= n else UNKNOWN, info

            # Unknown 0F opcode -> fail-safe.
            return UNKNOWN, info

        # --- one-byte opcode map ---
        info["opcode"] = op

        # mov r(8/16/32/64), imm  (B0..BF)
        if 0xB8 <= op <= 0xBF:
            if info["rex_w"]:
                imm = 8
            elif info["opsize16"]:
                imm = 2
            else:
                imm = 4
            i += imm
            return (i - start) if i <= n else UNKNOWN, info

        # near call/jmp rel32: always 32-bit displacement in 64-bit mode.
        if op in _REL32_ALWAYS:
            imm = 2 if (info["opsize16"] and not self.mode64) else 4
            i += imm
            return (i - start) if i <= n else UNKNOWN, info

        if op in _NO_IMM_NOMODRM:
            return (i - start), info

        # ret imm16 / retf imm16
        if op in (0xC2, 0xCA):
            i += 2
            return (i - start) if i <= n else UNKNOWN, info

        # enter iw, ib
        if op == 0xC8:
            i += 3
            return (i - start) if i <= n else UNKNOWN, info

        has_modrm = (op in _MODRM_NO_IMM or op in _IMM_IB and op in (
            0x80, 0x82, 0x83, 0xC0, 0xC1, 0xC6) or op in (0x69, 0x81, 0xC7))

        # group: opcodes that take ModRM + Ib
        if op in (0x80, 0x82, 0x83, 0xC0, 0xC1, 0xC6):
            info["modrm_off"] = i
            extra = self._modrm_extra(buf, i, info["addrsize32"])
            if extra == UNKNOWN:
                return UNKNOWN, info
            i += extra + 1   # + Ib
            return (i - start) if i <= n else UNKNOWN, info

        # group: opcodes that take ModRM + Iz
        if op in (0x69, 0x81, 0xC7):
            info["modrm_off"] = i
            extra = self._modrm_extra(buf, i, info["addrsize32"])
            if extra == UNKNOWN:
                return UNKNOWN, info
            i += extra
            i += 2 if info["opsize16"] else 4
            return (i - start) if i <= n else UNKNOWN, info

        # plain ModRM, no immediate
        if op in _MODRM_NO_IMM:
            info["modrm_off"] = i
            extra = self._modrm_extra(buf, i, info["addrsize32"])
            if extra == UNKNOWN:
                return UNKNOWN, info
            i += extra
            return (i - start) if i <= n else UNKNOWN, info

        # accumulator-immediate (no ModRM)
        if op in _IMM_IB:
            i += 1
            return (i - start) if i <= n else UNKNOWN, info
        if op in _IMM_IZ:
            i += 2 if info["opsize16"] else 4
            return (i - start) if i <= n else UNKNOWN, info

        # Everything else: fail-safe.
        return UNKNOWN, info
