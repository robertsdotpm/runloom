"""
elf64.py -- minimal pure-Python ELF64 little-endian parser + writer.

Standard library ONLY (struct). No pyelftools, no lief.

We parse exactly what the rewriter needs:
  * the ELF header (e_phoff, e_phnum, e_phentsize, e_shoff, e_entry, ...)
  * the program header table (PT_LOAD segments give us file<->VA mapping)
  * the section header table (so we can find executable sections / .text)

And we support the surgery the rewriter performs:
  * read/patch bytes at a virtual address (within an existing segment)
  * append a brand-new PT_LOAD segment at end-of-file and relocate the
    program-header table to make room for one more phdr.

References: System V ABI / ELF-64 spec. All struct layouts below are the
canonical ELF64 little-endian forms.
"""

import struct

# ---- ELF constants -------------------------------------------------------
ELFCLASS64 = 2
ELFDATA2LSB = 1
ET_DYN = 3            # shared object / PIE

PT_NULL = 0
PT_LOAD = 1
PT_DYNAMIC = 2
PT_INTERP = 3
PT_NOTE = 4
PT_PHDR = 6
PT_GNU_EH_FRAME = 0x6474e550
PT_GNU_STACK = 0x6474e551
PT_GNU_RELRO = 0x6474e552

PF_X = 0x1
PF_W = 0x2
PF_R = 0x4

SHT_PROGBITS = 1
SHT_NOBITS = 8

SHF_EXECINSTR = 0x4
SHF_ALLOC = 0x2

# ELF64 header: 64 bytes
#   e_ident[16] H(type) H(machine) I(version) Q(entry) Q(phoff) Q(shoff)
#   I(flags) H(ehsize) H(phentsize) H(phnum) H(shentsize) H(shnum) H(shstrndx)
_EHDR_FMT = "<16sHHIQQQIHHHHHH"
EHDR_SIZE = struct.calcsize(_EHDR_FMT)        # 64

# ELF64 program header: 56 bytes
#   I(type) I(flags) Q(offset) Q(vaddr) Q(paddr) Q(filesz) Q(memsz) Q(align)
_PHDR_FMT = "<IIQQQQQQ"
PHDR_SIZE = struct.calcsize(_PHDR_FMT)        # 56

# ELF64 section header: 64 bytes
#   I(name) I(type) Q(flags) Q(addr) Q(offset) Q(size)
#   I(link) I(info) Q(addralign) Q(entsize)
_SHDR_FMT = "<IIQQQQIIQQ"
SHDR_SIZE = struct.calcsize(_SHDR_FMT)        # 64

assert EHDR_SIZE == 64
assert PHDR_SIZE == 56
assert SHDR_SIZE == 64


class Phdr:
    __slots__ = ("p_type", "p_flags", "p_offset", "p_vaddr", "p_paddr",
                 "p_filesz", "p_memsz", "p_align")

    def __init__(self, p_type, p_flags, p_offset, p_vaddr, p_paddr,
                 p_filesz, p_memsz, p_align):
        self.p_type = p_type
        self.p_flags = p_flags
        self.p_offset = p_offset
        self.p_vaddr = p_vaddr
        self.p_paddr = p_paddr
        self.p_filesz = p_filesz
        self.p_memsz = p_memsz
        self.p_align = p_align

    def pack(self):
        return struct.pack(_PHDR_FMT, self.p_type, self.p_flags,
                           self.p_offset, self.p_vaddr, self.p_paddr,
                           self.p_filesz, self.p_memsz, self.p_align)

    @classmethod
    def unpack(cls, buf, off):
        return cls(*struct.unpack_from(_PHDR_FMT, buf, off))

    def __repr__(self):
        names = {PT_LOAD: "LOAD", PT_DYNAMIC: "DYNAMIC", PT_INTERP: "INTERP",
                 PT_NOTE: "NOTE", PT_PHDR: "PHDR",
                 PT_GNU_EH_FRAME: "GNU_EH_FRAME", PT_GNU_STACK: "GNU_STACK",
                 PT_GNU_RELRO: "GNU_RELRO", PT_NULL: "NULL"}
        t = names.get(self.p_type, hex(self.p_type))
        f = "".join(c for c, b in (("R", PF_R), ("W", PF_W), ("X", PF_X))
                    if self.p_flags & b)
        return (f"Phdr({t} off=0x{self.p_offset:x} va=0x{self.p_vaddr:x} "
                f"filesz=0x{self.p_filesz:x} memsz=0x{self.p_memsz:x} "
                f"flags={f} align=0x{self.p_align:x})")


class Shdr:
    __slots__ = ("sh_name", "sh_type", "sh_flags", "sh_addr", "sh_offset",
                 "sh_size", "sh_link", "sh_info", "sh_addralign", "sh_entsize",
                 "name")

    def __init__(self, vals):
        (self.sh_name, self.sh_type, self.sh_flags, self.sh_addr,
         self.sh_offset, self.sh_size, self.sh_link, self.sh_info,
         self.sh_addralign, self.sh_entsize) = vals
        self.name = ""   # resolved later from .shstrtab

    @classmethod
    def unpack(cls, buf, off):
        return cls(struct.unpack_from(_SHDR_FMT, buf, off))


class ELF64:
    def __init__(self, data: bytes):
        self.data = bytearray(data)
        self._parse_header()
        self._parse_phdrs()
        self._parse_shdrs()

    # -- parsing ----------------------------------------------------------
    def _parse_header(self):
        d = self.data
        if d[:4] != b"\x7fELF":
            raise ValueError("not an ELF file (bad magic)")
        if d[4] != ELFCLASS64:
            raise ValueError("not ELF64 (EI_CLASS != ELFCLASS64)")
        if d[5] != ELFDATA2LSB:
            raise ValueError("not little-endian (EI_DATA != ELFDATA2LSB)")
        (self.e_ident, self.e_type, self.e_machine, self.e_version,
         self.e_entry, self.e_phoff, self.e_shoff, self.e_flags,
         self.e_ehsize, self.e_phentsize, self.e_phnum, self.e_shentsize,
         self.e_shnum, self.e_shstrndx) = struct.unpack_from(_EHDR_FMT, d, 0)
        if self.e_machine != 62:  # EM_X86_64
            raise ValueError(f"not x86-64 (e_machine={self.e_machine})")
        if self.e_phentsize != PHDR_SIZE:
            raise ValueError(f"unexpected e_phentsize={self.e_phentsize}")

    def _parse_phdrs(self):
        self.phdrs = []
        for i in range(self.e_phnum):
            off = self.e_phoff + i * self.e_phentsize
            self.phdrs.append(Phdr.unpack(self.data, off))

    def _parse_shdrs(self):
        self.shdrs = []
        if self.e_shoff == 0 or self.e_shnum == 0:
            return
        for i in range(self.e_shnum):
            off = self.e_shoff + i * self.e_shentsize
            self.shdrs.append(Shdr.unpack(self.data, off))
        # resolve names from the section-header string table
        if 0 <= self.e_shstrndx < len(self.shdrs):
            strtab = self.shdrs[self.e_shstrndx]
            base = strtab.sh_offset
            for s in self.shdrs:
                end = self.data.find(b"\x00", base + s.sh_name)
                s.name = self.data[base + s.sh_name:end].decode(
                    "latin-1", "replace")

    # -- VA <-> file offset ----------------------------------------------
    def va_to_off(self, va):
        """Map a virtual address to a file offset via PT_LOAD segments."""
        for ph in self.phdrs:
            if ph.p_type != PT_LOAD:
                continue
            if ph.p_vaddr <= va < ph.p_vaddr + ph.p_filesz:
                return ph.p_offset + (va - ph.p_vaddr)
        raise ValueError(f"VA 0x{va:x} not in any PT_LOAD file range")

    def off_to_va(self, off):
        for ph in self.phdrs:
            if ph.p_type != PT_LOAD:
                continue
            if ph.p_offset <= off < ph.p_offset + ph.p_filesz:
                return ph.p_vaddr + (off - ph.p_offset)
        raise ValueError(f"offset 0x{off:x} not in any PT_LOAD file range")

    def executable_sections(self):
        """Return SHF_EXECINSTR PROGBITS sections (what we scan)."""
        out = []
        for s in self.shdrs:
            if (s.sh_type == SHT_PROGBITS and (s.sh_flags & SHF_EXECINSTR)
                    and s.sh_size > 0):
                out.append(s)
        return out

    def read_off(self, off, n):
        return bytes(self.data[off:off + n])

    def patch_off(self, off, payload: bytes):
        """In-place overwrite at a file offset (must stay same length)."""
        self.data[off:off + len(payload)] = payload

    # -- header re-serialisation -----------------------------------------
    def _repack_header(self):
        struct.pack_into(_EHDR_FMT, self.data, 0,
                         self.e_ident, self.e_type, self.e_machine,
                         self.e_version, self.e_entry, self.e_phoff,
                         self.e_shoff, self.e_flags, self.e_ehsize,
                         self.e_phentsize, self.e_phnum, self.e_shentsize,
                         self.e_shnum, self.e_shstrndx)

    def _write_phdrs_at(self, off):
        for i, ph in enumerate(self.phdrs):
            self.data[off + i * PHDR_SIZE:off + (i + 1) * PHDR_SIZE] = ph.pack()

    def relocate_phdrs_to_eof_and_add(self, new_phdr: Phdr, align=0x1000):
        """
        Move the program-header table to the end of the file (where there is
        room) and append `new_phdr`. This is the classic technique used when
        there is no slack to grow e_phnum in place.

        IMPORTANT: there must already exist a PT_LOAD segment that COVERS the
        new phdr-table location in memory, otherwise the loader won't map the
        headers. We append a NEW PT_LOAD (the caller's segment) that does
        exactly that -- so we order operations so the new segment's file range
        includes the relocated phdr table. See injector.py for the full dance.

        This helper ONLY appends the phdr to the in-memory list and bumps
        e_phnum; the caller is responsible for placing the table bytes and
        fixing e_phoff (we expose write_phdr_table_at for that).
        """
        self.phdrs.append(new_phdr)
        self.e_phnum = len(self.phdrs)

    def append_bytes(self, payload: bytes) -> int:
        """Append raw bytes at EOF, return the file offset they start at."""
        off = len(self.data)
        self.data.extend(payload)
        return off

    def pad_to(self, alignment) -> int:
        """Pad file with zeros so len(data) is a multiple of alignment."""
        rem = len(self.data) % alignment
        if rem:
            self.data.extend(b"\x00" * (alignment - rem))
        return len(self.data)

    def set_phoff(self, off):
        self.e_phoff = off

    def finalize(self) -> bytes:
        """Re-pack header + all phdrs into the byte buffer and return it."""
        self._repack_header()
        self._write_phdrs_at(self.e_phoff)
        return bytes(self.data)
