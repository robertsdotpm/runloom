# stackrewriter-proto

A **working, tested prototype of a static binary rewriter, in pure Python
(standard library only)**, that inserts software stack-overflow checks into a
compiled Linux **x86-64** shared object (a CPython C extension).

The goal: let uninstrumented C **self-check its stack pointer** so a stack
overflow is caught *in software* — **before** the stack is corrupted — instead
of relying on a hardware guard page. The motivating use case is a fiber runtime
that wants to pack many fibers onto small stacks **without paying for a
per-fiber guard page**.

## Hard constraint, honored: zero external dependencies

Everything is hand-rolled with the Python **standard library only**
(`struct`, `os`, `sys`, `json`). There is **no** capstone, keystone,
pyelftools, lief, or any pip package. ELF parsing/writing, x86-64 instruction
length-decoding, and machine-code emission are all implemented from scratch.

Proof it's stdlib-only — the rewriter runs under isolated, no-site-packages
mode:

```
$ python3 -SI rewriter/stackrewrite.py --scan ext/stacktest...so
  stack-alloc sites found            : 6
  patchable (would instrument)       : 2
  coverage of recognised sites       : 33.3%
```

`gcc`, `objdump`, and `readelf` are used **only** to build the test input and
to **cross-check** our work during development/testing. The rewriter tool
itself (`rewriter/*.py`) never shells out to them.

---

## Design: fail-safe targeted recognition (NOT a full disassembler)

This is deliberately **not** a complete x86 disassembler. It has two pieces:

### 1. A minimal instruction *length* decoder (`rewriter/lendec.py`)

Just enough of the x86 encoding pipeline
(`[legacy prefixes]* [REX]? opcode [ModRM [SIB]]? [disp]? [imm]?`) to compute
the **length** of an instruction so we can linearly advance a cursor. It models
legacy prefixes, REX, the 1-byte map, the `0F` two-byte map, the `0F38`/`0F3A`
three-byte maps, ModRM/SIB/displacement, and per-opcode immediate sizes
(including the operand-size `0x66` rule and the "branch rel is always 32-bit in
64-bit mode" rule for `E8`/`E9`/`0F 8x`).

**When it meets an encoding it cannot length with confidence, it returns
`UNKNOWN`** and the scanner **stops that run and conservatively skips the
rest** — it never guesses past an unknown byte, because a single wrong length
would mis-align the cursor and corrupt everything after it.

The decoder is **validated against `objdump` as an oracle**
(`tests/check_lendec.py`) over real binaries:

| binary | insns checked | exact-length | conservative `UNKNOWN` | **wrong** |
|---|---|---|---|---|
| `stacktest...so` (test ext) | 191 | 191 | 0 | **0** |
| `libc.so.6` | 385,439 | 371,290 | 14,149 | **0** |
| `libpython3.13t.so` | 772,074 | 769,443 | 2,631 | **0** |

Over **~1.16 million** real instructions: **zero wrong lengths**. Everything it
doesn't model (mostly AVX/VEX-encoded SIMD) becomes a safe conservative stop,
never a mis-decode. That is the whole point of the fail-safe design.

### 2. Targeted pattern recognition of stack-alloc encodings (`rewriter/scanner.py`)

At each instruction boundary it recognizes the stack-allocation forms (REX.W
`0x48`, rsp = reg 4):

| encoding | bytes | length | disposition |
|---|---|---|---|
| `sub rsp, imm32` | `48 81 EC <imm32>` | 7 | **PRIMARY — instrumented** (room for a 5-byte `jmp rel32` + 2 NOP) |
| `sub rsp, imm8`  | `48 83 EC <imm8>`  | 4 | **SKIP** — too small to patch with a `jmp rel32` |
| `sub rsp, r64`   | `48 29 /r` (dest rsp) | 3 | **SKIP** — variable size, too small |
| `sub reg, rsp`   | `48 2B /r` (dest rsp) | 3 | **SKIP** — variable size, too small |

Plus a guard: a `sub rsp,imm32` that sits **inside a stack-clash probing loop**
(detected by a following `orq $0,(%rsp)`) is **skipped**, because such a sub
runs many times per call and a displaced-original trampoline would loop wrongly.

**Fail-safe is the law:** only a site that is *positively recognized* **and**
*safely patchable* is instrumented. Everything else is skipped + logged. A
skipped site stays correct and runnable. In the real system those fall back to
a runtime hardware guard; here we just report coverage %.

---

## The inserted check (`rewriter/stub.py`, `rewriter/emit.py`)

Each instrumented `sub rsp,imm32` (7 bytes) is overwritten with
`E9 <rel32>` + `90 90` (a `jmp` into a trampoline + 2 NOP pad). The trampoline
lives in a **new segment** appended to the ELF. It is fully **position-
independent** (the `.so` loads at an arbitrary base): all data references are
RIP-relative and all control transfers are rel32 deltas.

Trampoline logic (verified disassembly):

```
pushf ; push rax ; push r11              ; save flags + 2 scratch regs
lea  rax, [rsp + 24]                     ; rax = rsp at entry (undo the pushes)
sub  rax, imm32                          ; rax = prospective NEW rsp
mov  r11, [rip + g_stack_limit]          ; r11 = per-thread limit (position-indep)
cmp  rax, r11
jb   .overflow                           ; if new_rsp < limit (unsigned) -> trap
pop  r11 ; pop rax ; popf                ; restore
sub  rsp, imm32                          ; re-execute the displaced original
jmp  <site_va + 7>                       ; back to right after the patched bytes
.overflow:
  pop r11 ; pop rax ; popf
  jmp report_and_abort
report_and_abort:
  write(2, msg, len)                     ; raw syscall (no libc linkage needed)
  g_hit_count += 1
  ud2                                    ; deliberate abort (SIGILL) -> DETECTION
```

The **limit mechanism** is a writable 64-bit global `g_stack_limit` at a known
injected VA. The rewriter prints that VA and writes a sidecar
`<output>.inject.json`; the test harness sets the limit by poking that address
via `ctypes` (computing the runtime address from the load base, derived from an
anchor symbol). `g_hit_count` is a second global bumped on each trap.

---

## ELF surgery (`rewriter/elf64.py`, `rewriter/injector.py`)

Pure-`struct` ELF64 parse/write. To add the trampoline segment we use the
classic technique:

1. Append a new **PT_LOAD** segment at a fresh page-aligned VA above all
   existing segments, holding the **relocated program-header table** + the
   stub/trampolines.
2. Because there's no slack to grow `e_phnum` in place, the **whole phdr table
   is relocated** into the new segment, `e_phoff`/`e_phnum` are fixed, the new
   `PT_LOAD` is added (and it *covers* the relocated phdr table so the loader
   maps it), and `PT_PHDR` is fixed if present.
3. Patch each instrumented call site's 7 bytes.

The result is verified with `readelf -h`/`readelf -l` and — the real test — by
actually `dlopen`-ing it and running it. It even round-trips on real-world
extensions: rewriting the system `_sqlite3.cpython-313t...so` (885 KB) produces
a clean ELF that `readelf` accepts and that loads and reports
`sqlite_version 3.45.1`.

The injected segment is mapped **R-W-X** (not W^X) so the harness can poke the
limit via ctypes — see *Limitations*.

A new instrumented `.so` is always emitted; the input is never mutated.

---

## Coverage on the test extension

The test extension `ext/stacktest.c` has two functions with large single-shot
`sub rsp,imm32` frames (`big_frame_work`, `recurse_c`) plus small `imm8` frames
the compiler emits for the Python wrapper functions.

```
stack-alloc sites found      : 6
instrumented (sub rsp,imm32) : 2     <- big_frame_work + recurse_c
skipped (fail-safe)          : 4     <- all sub rsp,imm8 (4 bytes, too small)
coverage of recognised sites : 33.3%
scan stops (unknown insns)   : 0
```

33.3% of *recognized stack-alloc sites* are instrumented — and that is the
**correct, honest** number: the other 4 are `imm8` frames that physically
cannot hold a 5-byte detour, so they are safely skipped, not broken. The 2 that
matter (the large frames that actually threaten a small stack) are both caught.

---

## How to run

Prereqs: `gcc`, `objdump`, `readelf`, a CPython 3.13t at
`~/.pyenv/versions/3.14.4t/bin/python3` (built cleanly against the free-
threaded 3.13t headers — that's the one with dev headers on this box), and any
`python3` with a stdlib for the rewriter itself.

```bash
./run_all.sh          # builds, validates, rewrites, and runs all 6 stages
```

Or piecewise:

```bash
bash ext/build.sh                                   # build INPUT .so (gcc)
python3 tests/check_lendec.py ext/stacktest...so    # decoder vs objdump
python3 rewriter/stackrewrite.py --scan in.so       # report sites only
python3 rewriter/stackrewrite.py in.so out.so       # rewrite (pure stdlib)
PYTHON_GIL=0 .../python3 tests/e2e_overflow.py out.so out.so.inject.json overflow
```

---

## Real captured test output

**Baseline (uninstrumented) — silent hardware crash:**
```
[baseline] calling recurse(100000) on tiny stack (expect HARDWARE crash)...
Segmentation fault            (exit code 139 = SIGSEGV; no software report)
```

**Rewriter coverage log:** see "Coverage" above (2 instrumented, 4 skipped).

**Instrumented — overflow caught in software BEFORE corruption:**
```
[e2e] set limit = 0x... (ext_sp - 256KiB floor)
[e2e] driving recurse(100000) -- expect SOFTWARE report + abort

*** STACK OVERFLOW caught by injected software check ***
    (prospective rsp would cross the per-thread limit)
    aborting via ud2 BEFORE the stack is corrupted.
Illegal instruction          (exit code 132 = SIGILL = our ud2 = DETECTED)
```

**Instrumented — normal within-limit calls STILL WORK:**
```
[e2e] calling big_frame(0)  -> -4096
[e2e] calling recurse(8)    -> 179
[e2e] hit_count = 0 (expected 0 -- no overflow)
[e2e] NORMAL PATH OK: instrumented binary works within limit.
```

Baseline `139 (SIGSEGV, silent)` vs instrumented `132 (SIGILL, after a clean
software report)` — that contrast *is* the deliverable: **detection before
corruption**.

---

## What works / what's stubbed / honest limitations

**Works (tested end-to-end):**
- Pure-stdlib ELF64 parse + segment injection + phdr relocation + call-site
  patching; output verified by `readelf` and by `dlopen`+execute.
- Length decoder: 0 wrong lengths over 1.16M real instructions; fail-safe stops
  otherwise.
- Targeted `sub rsp,imm32` recognition with correct offsets; `imm8`/`reg`/
  probe-loop forms correctly skipped.
- Position-independent trampoline + raw-syscall report + `ud2` abort.
- Catches a real overflow in software; preserves normal behavior; round-trips
  on a real 885 KB `_sqlite3` extension.

**Stubbed / not validated:**
- **32-bit (x86) mode** is *structurally* selectable (`LenDecoder(mode64=False)`
  toggles REX handling and the branch-operand rule) but is **not tested or
  validated** — treat it as a stub. Only x86-64 is exercised.
- The injected segment is **R-W-X**, not W^X, purely so the test harness can
  poke `g_stack_limit` via ctypes. A production build would split a tiny RW
  data page from the RX code page (trivial extension: emit two segments).
- Only the **primary 7-byte `sub rsp,imm32`** form is instrumented. `imm8`
  frames are skipped (a real system could absorb the following instruction to
  make room, or relocate a small basic-block — not done here).

**Intrinsic limitations of static rewriting (honest):**
- **JIT'd / self-modifying code can't be statically rewritten** — only what's
  in the file image is seen. (For a fiber runtime this is fine for the C
  extension/runtime itself; not for arbitrary runtime-generated code.)
- **Per-version / per-build re-rewrite**: offsets and frame encodings are
  specific to the exact compiled `.so`; recompiling means re-running the tool.
- The length decoder is **incomplete by design** — weirder binaries (heavy
  AVX-512/VEX/EVEX, exotic prefixes) will simply produce **more conservative
  skips**, never wrong patches. More coverage = more `UNKNOWN` stops, not more
  risk. Pure-stdlib means we hand-rolled the decoder and ELF logic, so the
  honest trade is breadth-of-coverage for guaranteed safety.
- The check compares against a single per-thread limit global; a real runtime
  would store the limit in TLS (e.g. `%fs`-relative) so each fiber/thread has
  its own — a localized change to the trampoline's limit-load.

---

## Files

```
ext/stacktest.c          test CPython C extension (C11; large sub rsp,imm32 frames)
ext/build.sh             builds it against 3.13t (-fno-stack-clash-protection)
rewriter/elf64.py        pure-struct ELF64 parser/writer
rewriter/lendec.py       minimal x86 instruction LENGTH decoder (fail-safe)
rewriter/scanner.py      linear scan + targeted sub-rsp recognition
rewriter/emit.py         hand-rolled x86-64 machine-code emitter
rewriter/stub.py         builds the injected limit global + report + trampolines
rewriter/injector.py     ELF surgery: new PT_LOAD + phdr reloc + call-site patch
rewriter/stackrewrite.py CLI entry point (--scan or rewrite)
tests/check_lendec.py    validates the decoder against objdump (oracle)
tests/baseline_overflow.py  shows the uninstrumented silent SIGSEGV
tests/e2e_overflow.py    end-to-end: normal-works + overflow-caught
run_all.sh               runs every stage and prints real output
```
