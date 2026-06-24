"""
scanner.py -- linear scan of executable sections to find stack-allocation
sites, using the minimal length decoder to advance the cursor and targeted
pattern recognition to classify each instruction boundary.

FAIL-SAFE model:
  * We advance instruction-by-instruction with LenDecoder. The moment the
    decoder returns UNKNOWN, we STOP scanning the current run and resume at
    the next section (we do not guess past an unknown instruction -- doing so
    would risk mis-aligning the cursor and mis-classifying bytes).
  * Only positively-recognised `sub rsp, ...` encodings become candidate
    sites. Each candidate is tagged with whether it is PATCHABLE (>= 5 bytes
    so a `jmp rel32` fits) and WHY if not.

Recognised stack-allocation encodings (REX.W = 0x48 for rsp ops):
  PRIMARY  sub rsp, imm32 : 48 81 EC <imm32>      (7 bytes)  -> PATCHABLE
  small    sub rsp, imm8  : 48 83 EC <imm8>       (4 bytes)  -> SKIP (too small)
  reg      sub rsp, r64   : 48 29 /r (mod=11, rm=100=rsp)    (3 bytes) -> SKIP (too small)
           sub rsp, r64   : 48 2B /r (mod=11, reg=rsp dest)  (3 bytes) -> SKIP (too small)
  add-form add rsp, ... (epilogue) is NOT a growth site; ignored.

We additionally guard against instrumenting a `sub rsp,imm32` that sits
inside a stack-clash *probing loop* (the `... ; cmp r,%rsp ; jne` idiom) --
if the 7 bytes are immediately followed by the probe's `orq $0,(%rsp)` /
`cmp`/`jne` shape we DO NOT instrument (those subs run many times per call
and the displaced-original trampoline would loop wrongly). Fail-safe: skip.
"""

from lendec import LenDecoder, UNKNOWN

# Encodings (REX.W prefix 0x48 assumed; rsp is reg 4)
SUB_RSP_IMM32 = bytes([0x48, 0x81, 0xEC])   # + imm32
SUB_RSP_IMM8 = bytes([0x48, 0x83, 0xEC])    # + imm8
SUB_RSP_R64_29 = bytes([0x48, 0x29])        # ModRM mod=11 rm=100 -> 0xE? with reg in bits 3-5
SUB_RSP_R64_2B = bytes([0x48, 0x2B])

PATCH_JMP_LEN = 5     # E9 <rel32>
MIN_PATCH_LEN = PATCH_JMP_LEN


class Site:
    __slots__ = ("va", "off", "raw", "length", "kind", "imm",
                 "patchable", "reason")

    def __init__(self, va, off, raw, length, kind, imm, patchable, reason):
        self.va = va
        self.off = off
        self.raw = raw
        self.length = length
        self.kind = kind
        self.imm = imm
        self.patchable = patchable
        self.reason = reason

    def __repr__(self):
        imm = f"0x{self.imm:x}" if self.imm is not None else "-"
        flag = "PATCH" if self.patchable else "SKIP "
        return (f"<Site VA=0x{self.va:08x} off=0x{self.off:x} {flag} "
                f"{self.kind:14s} imm={imm:>8} len={self.length} "
                f"[{self.raw.hex()}] {self.reason}>")


def _classify(buf, p, length, info):
    """
    Given the instruction at buf[p] of decoded `length`, return a Site kind
    tuple (kind, imm, patchable, reason) if it is a stack-alloc site, else
    None.
    """
    b = buf[p:p + length]

    # sub rsp, imm32  -> 48 81 EC <imm32>
    if length >= 7 and b[:3] == SUB_RSP_IMM32:
        imm = int.from_bytes(b[3:7], "little", signed=False)
        return ("sub rsp,imm32", imm, True, "primary 7-byte target")

    # sub rsp, imm8   -> 48 83 EC <imm8>
    if length == 4 and b[:3] == SUB_RSP_IMM8:
        imm = b[3]
        return ("sub rsp,imm8", imm, False,
                "only 4 bytes; no room for jmp rel32 (skipped)")

    # sub rsp, r64    -> 48 29 /r  (sub r/m64, r64). dest = r/m = rsp.
    # ModRM mod=11 (reg-direct), rm=100 (rsp). reg field = source.
    if length == 3 and b[:2] == SUB_RSP_R64_29:
        modrm = b[2]
        mod = (modrm >> 6) & 3
        rm = modrm & 7
        if mod == 3 and rm == 4:    # dest rsp
            return ("sub rsp,reg", None, False,
                    "3 bytes; variable size; no room for jmp rel32 (skipped)")

    # sub rsp, r64    -> 48 2B /r  (sub r64, r/m64). dest = reg = rsp.
    if length == 3 and b[:2] == SUB_RSP_R64_2B:
        modrm = b[2]
        reg = (modrm >> 3) & 7
        if reg == 4:                # dest rsp
            return ("sub reg<-rsp", None, False,
                    "3 bytes; variable size; no room for jmp rel32 (skipped)")

    return None


def _looks_like_probe(buf, after):
    """
    Heuristic: detect the stack-clash probe idiom that follows a
    `sub rsp,0x1000` inside a probing loop:
        orq $0x0,(%rsp)        (48 83 0c 24 00)
        cmp r,%rsp ; jne ...
    If present, the sub is part of a loop and must NOT be instrumented.
    Returns True if it looks like a probe.
    """
    # 48 83 0c 24 00  =  orq $0x0,(%rsp)
    if buf[after:after + 5] == bytes([0x48, 0x83, 0x0C, 0x24, 0x00]):
        return True
    return False


def scan_section(elf, sh, dec=None, verbose=True):
    """
    Scan one executable section. Returns (sites, stats) where stats has
    coverage counters.
    """
    if dec is None:
        dec = LenDecoder(mode64=True)

    base_off = sh.sh_offset
    base_va = sh.sh_addr
    size = sh.sh_size
    buf = elf.read_off(base_off, size)

    sites = []
    stats = {
        "insns": 0,
        "stops": 0,
        "bytes_scanned": 0,
        "bytes_skipped_after_stop": 0,
    }

    p = 0
    while p < size:
        length, info = dec.length(buf, p)
        if length == UNKNOWN or length <= 0 or p + length > size:
            # FAIL-SAFE: stop this run, skip the remainder of the section.
            stats["stops"] += 1
            stats["bytes_skipped_after_stop"] += (size - p)
            if verbose:
                va = base_va + p
                print(f"[scan]   STOP at VA 0x{va:08x} (off 0x{base_off+p:x}): "
                      f"cannot length-decode {buf[p:p+8].hex()} -- "
                      f"skipping rest of {sh.name} ({size - p} bytes) [fail-safe]")
            break

        stats["insns"] += 1
        stats["bytes_scanned"] += length

        cls = _classify(buf, p, length, info)
        if cls is not None:
            kind, imm, patchable, reason = cls
            va = base_va + p
            off = base_off + p
            # extra guard for probe-loop subs
            if patchable and _looks_like_probe(buf, p + length):
                patchable = False
                reason = ("inside stack-clash probe loop "
                          "(orq $0,(%rsp) follows) -- skipped [fail-safe]")
            sites.append(Site(va, off, buf[p:p + length], length,
                              kind, imm, patchable, reason))

        p += length

    return sites, stats


def scan_elf(elf, verbose=True):
    dec = LenDecoder(mode64=True)
    all_sites = []
    agg = {"insns": 0, "stops": 0, "bytes_scanned": 0,
           "bytes_skipped_after_stop": 0}
    for sh in elf.executable_sections():
        if verbose:
            print(f"[scan] section {sh.name}: VA=0x{sh.sh_addr:x} "
                  f"off=0x{sh.sh_offset:x} size=0x{sh.sh_size:x}")
        sites, stats = scan_section(elf, sh, dec, verbose=verbose)
        all_sites.extend(sites)
        for k in agg:
            agg[k] += stats[k]
    return all_sites, agg
