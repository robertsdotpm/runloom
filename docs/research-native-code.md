# Research: executing native machine code from a fiber

> **Status: research / experimental.** `runloom_c.MachineCode` is a toy-grade
> primitive kept for exploration and demonstration. It is **not** a supported,
> stable, or sandboxed API, and it is orthogonal to runloom's actual job
> (cooperative concurrency for blocking code). Treat everything here as a
> notebook entry, not a contract.

## The idea

A runloom fiber is not an interpreter task. It is a **real C stack** that a
**real OS thread** executes with a hand-written assembly context switch. The CPU
running a fiber is running native instructions on silicon, exactly as it
runs libc or the Python binary itself — the "fiber" is just which stack
`rsp` points at.

Follow that to its conclusion: if a fiber already executes native code on a
real stack, then you can hand it **arbitrary native code at runtime** and have
it jump straight in. Map a blob of machine-code bytes as executable, take its
address as a function pointer, and call it. The CPU's instruction pointer moves
into your bytes and the hardware decodes and runs them. **There is no
interpreter, no VM, no dispatch loop, no sandbox** between the bytes and the
processor — the same bare-metal path as any compiled function.

So you can go from a Python `bytes` object all the way down to *the CPU eating
those bytes*, from inside a green thread, and — under the M:N scheduler — across
many fibers in genuine parallel, each on its own swapped stack. That is the
interesting part: it collapses the whole abstraction tower in one move and shows
concretely that runloom's fibers are first-class native execution contexts,
not bytecode-interpreter coroutines.

Contrast with CPython bytecode (`LOAD_FAST`, `BINARY_OP`): that *is* interpreted
— a big C `switch` reads opcodes and dispatches. A `MachineCode` blob has none
of that. The bytes **are** the instructions.

## The API

```python
import runloom_c

# x86-64 SysV: 1st arg in rdi, return in rax.
#   mov rax, rdi ; inc rax ; ret      ->  f(x) = x + 1
INC = bytes([0x48, 0x89, 0xf8, 0x48, 0xff, 0xc0, 0xc3])

fn = runloom_c.MachineCode(INC)
fn(41)            # -> 42   (a real native call; rip jumps into the page)
fn.address        # address of the mapped executable page (int)
fn.size           # length of the blob in bytes
fn.close()        # unmap now (idempotent); also a context manager

with runloom_c.MachineCode(INC) as fn:
    fn(41)
```

Calls take **0–6 integer/pointer arguments** and return the result register as a
signed machine word:

```python
# mov rax, rdi ; add rax, rsi ; ret   ->  f(a, b) = a + b
ADD = bytes([0x48, 0x89, 0xf8, 0x48, 0x01, 0xf0, 0xc3])
runloom_c.MachineCode(ADD)(20, 22)    # -> 42
```

Pass an address (a Python int, e.g. the address of a `ctypes`/`array` buffer) to
hand the blob memory for richer inputs/outputs.

### From a fiber

It runs on the **caller's** stack, so inside a fiber it executes on that
fiber's stack — and under M:N, many fibers run their blobs in parallel:

```python
import runloom, runloom_c
SQ = bytes([0x48, 0x89, 0xf8, 0x48, 0x0f, 0xaf, 0xc7, 0xc3])  # f(x) = x*x

def worker(n, ch):
    with runloom_c.MachineCode(SQ) as fn:
        ch.send((n, fn(n)))          # native imul, on this fiber's stack

def main():
    ch = runloom_c.Chan()
    for n in range(8):
        runloom_c.mn_go(lambda n=n: worker(n, ch))
    return dict(ch.recv()[0] for _ in range(8))   # {n: n*n}

runloom_c.mn_init(2); runloom_c.mn_go(main); runloom_c.mn_run(); runloom_c.mn_fini()
```

## How it works

`MachineCode(blob)` maps a page **W^X** — writable while the bytes are copied in,
then flipped to read+execute, never both at once:

- **POSIX:** `mmap(PROT_READ|PROT_WRITE)` → copy → `__builtin___clear_cache`
  (instruction-cache coherency; a no-op on x86, required on ARM/POWER) →
  `mprotect(PROT_READ|PROT_EXEC)`.
- **Windows:** `VirtualAlloc(PAGE_READWRITE)` → copy →
  `VirtualProtect(PAGE_EXECUTE_READ)` → `FlushInstructionCache`.

The call itself is a **portable C trampoline**: the page address is cast to a
typed function pointer (`intptr_t f(intptr_t, ...)`, one cast per arity 0–6) and
called, so the C compiler emits the host calling convention. Only the *blob* is
architecture-specific:

| arch | int args | return |
| --- | --- | --- |
| x86-64 SysV | `rdi, rsi, rdx, rcx, r8, r9` | `rax` |
| AArch64 AAPCS | `x0`–`x5` | `x0` |

## Caveats (these are the whole story)

Because the blob runs on the fiber's stack and is opaque native code, it
inherits the fiber model's hard edges:

- **Small stack.** It runs on the fiber's C stack (default 32 KB with a
  PROT_NONE guard page). The same fat-frame rule as the rest of runloom applies:
  a blob that pushes a large frame or recurses deeply overflows into the guard
  page → a clean SIGSEGV. For a big compute kernel, give that fiber a roomy
  stack: `runloom_c.go(fn, stack_size=...)`.
- **No cooperation.** Raw machine code knows nothing about the scheduler — it
  can't park, yield, or do I/O cooperatively, and the sysmon preemptor (which
  acts at Python bytecode boundaries) can't interrupt it mid-blob. A long blob
  **holds the hub** like any tight C loop. Keep blobs short, or relocate a heavy
  kernel with `runloom.monkey.offload()` so it runs on a pool thread (and then
  size that thread's stack for it).
- **No safety, at all.** This is `exec(arbitrary_native_code)`. No bounds
  checks, no memory safety, no sandbox. A wrong blob corrupts or crashes the
  process. **Never** build a blob from untrusted input.
- **Unstable.** Experimental surface; the API may change or be removed.

## Directions worth exploring (so the idea isn't lost)

- **Tiny JITs.** Compile a hot predicate / filter / arithmetic expression to a
  blob once, call it per row from each fiber — a per-connection JIT with no
  GIL contention under 3.13t.
- **Hand-rolled SIMD kernels** (AVX2/AVX-512/NEON) called from fibers for
  number-crunching that the interpreter is too slow for and that's awkward to
  ship as a separate `.so`.
- **An assembler layer.** Wrap `keystone` (or shell out to `as`) so callers
  write asm text instead of opcode bytes; or a tiny builder for common shapes.
- **Buffer-passing convention.** Standardize "blob takes one pointer to an
  in/out struct" so kernels can have many args and multiple results cleanly.
- **Cross-arch blob sets.** Ship x86-64 + arm64 encodings of the same kernel and
  pick by `platform.machine()`.
- **Measurement.** Quantify the call overhead vs a `ctypes` call and vs a
  fiber switch — how cheap is "native call from a green thread" really?
- **Calling back into Python from a blob** (advanced, dangerous): a blob that
  invokes a C function pointer which re-enters the interpreter — needs a valid
  `PyThreadState` and great care; mostly a curiosity.

The PoC that started this lives at `_mntests/asm_poc.py` (pure `ctypes`, no
extension changes — proof that the fiber, not the wrapper, is what makes it
work). The supported-shaped version is `runloom_c.MachineCode`
(`src/runloom_c/module_machinecode.c.inc`); tests in `tests/test_machinecode.py`.
