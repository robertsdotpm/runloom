# 16 — Portability: platform, arch, compiler, backend selection

Ground truth: `plat.h`, `plat_atomic.h`, `plat_compat.h`, `compat.h`,
`fcontext.{h,c}`, `arch/swap_*.S`, `coro.c`, `netpoll_iocp.{c,h}`, `setup.py`,
the README "Platform & Python support" table.

## The principle

All platform/compiler/arch divergence is **detected once in `plat.h`** into a flat
set of `RUNLOOM_OS_* / RUNLOOM_ARCH_* / RUNLOOM_CC_* / RUNLOOM_HAVE_*` symbols, so
the rest of the codebase branches on capability flags, never on nested
`#if defined(__linux__) && ...` ladders. C99-strict, no GNU extensions in the
public surface; targets GCC 3+, Clang 3+, MSVC 2008+ (with shims), ICC, MinGW,
Watcom, Sun Studio. "One Windows binary spans Vista→11" because the netpoll backend
is probed at *runtime*, not chosen at compile time.

## Two pluggable axes (the only things that vary per platform)

Almost everything in runloom is portable C. Exactly two subsystems have
per-platform backends, each behind a uniform interface:

### 1. Context-switch backend (spec 01) — selected at compile time

`plat.h` picks exactly one, priority highest-first:

| Backend | Condition | Notes |
|---|---|---|
| **fcontext** (asm) | Linux/macOS/BSD/Android on x86-64 or aarch64 | the fast path; one `.S` file per arch. ~20× faster than ucontext (no `sigprocmask`). |
| **Fibers** | Windows | `CreateFiberEx` (reserve big, commit small). |
| **ucontext** | everything else | POSIX correctness fallback. |

`RUNLOOM_FORCE_UCONTEXT` (`RUNLOOM_BACKEND=ucontext`) forces the fallback so it gets
exercised on asm-capable hosts. The public API (`runloom_coro_*`) is identical
across all three — adding an arch is one `swap_<arch>.S` implementing `make_ctx` +
`swap` + the trampoline.

### 2. Netpoll backend (spec 06)

| Backend | OS | Selection |
|---|---|---|
| **epoll** | Linux | compile-time (`RUNLOOM_HAVE_EPOLL`) |
| **kqueue** | BSD / macOS | compile-time |
| **IOCP → WSAPoll → select** | Windows | **runtime-probed** (`runloom_win_use_iocp`) |
| **event ports** | Solaris | fallback to select for v0 |
| **select** | everything else | always available |

`RUNLOOM_FORCE_SELECT` (`RUNLOOM_NETPOLL=select`) suppresses the kernel pollers so
the `select` fallback gets exercised on Linux/BSD without exotic hardware. The
per-hub parker-pool routing is gated to the kernel by-fd backends (epoll/kqueue/
IOCP via `pump_dispatch_event`); WSAPoll/select rebuild fdsets by walking the list,
so on those backends every parker is forced into the single default pool (spec 06).

## The shims that keep the source single-version

- **`plat_atomic.h`** — the GCC/Clang `__atomic_*` builtins are the lingua franca;
  on MSVC they are macro-shimmed onto `_Interlocked*` so the bodies (cldeque,
  park/wake) compile unchanged. MinGW/Clang-on-Windows use the real builtins.
- **`plat.h` GCC-extension shims for MSVC** — `__attribute__((...))`,
  `__builtin_expect`, `__builtin_unreachable` are no-op/`__assume`-shimmed so the
  `hot`/branch-hint-decorated source compiles on MSVC without `#ifdef`s at each
  site.
- **`plat_compat.h`** — `runloom_mutex_t` and the thread primitives, so the
  cross-thread wake list / hub locks are one type across pthreads and Windows.
- **TLS model** — `RUNLOOM_TLS` is `__thread` with **`initial-exec`** on
  GCC/Clang (drops the `__tls_get_addr()` call that was ~7% of the chan ping-pong
  hot path — current-g and the per-thread sched are touched every switch). Falls
  back to global-dynamic under a sanitizer (ASan/TSan ship their own initial-exec
  TLS and can exhaust the static-TLS surplus a dlopen'd extension needs → import
  failure). `__declspec(thread)` on MSVC; pthread-specific where neither exists.

## Windows specifics worth flagging

- **Fibers**, not asm, for the switch. `CreateFiberEx` is chosen over `CreateFiber`
  deliberately: `CreateFiber` *commits* the whole `stack_size` (charged against the
  commit limit, no overcommit) so 1000 × 1 MiB ≈ 1 GiB committed; `CreateFiberEx`
  reserves big and commits a small floor, growing on demand — the same "pay for
  what you touch" as the POSIX mmap path (1000 × 1 MiB ≈ 76 MiB). No introspectable
  stack on Fibers, so the HWM scan / guard page / madvise-reclaim are no-ops there
  (the OS manages stacks and provides its own guard).
- **The Parker uses `socket.socketpair()`**, not `os.pipe()` — Win `select` only
  polls SOCKET handles (spec 14).
- **Crash path** is a Vectored Exception Handler (the OS still produces the crash);
  the rich POSIX `sigaltstack`/classify path doesn't apply.
- **A Vectored fault-injection surface** (`runloom_fault_*` with named sites
  WSAPOLL/SELECT/IOCP) exists because Windows has no syscall-injecting tracer like
  Linux's strace (spec 15).

## Build knobs (`setup.py`)

`RUNLOOM_BACKEND` (switch backend), `RUNLOOM_NETPOLL` (netpoll backend),
`RUNLOOM_NO_IOCP`, `RUNLOOM_DEBUG`, `RUNLOOM_EXTRA_CFLAGS`, plus the hardening flags
on optimized builds (`-fstack-protector-strong`, `-D_FORTIFY_SOURCE=2`,
`-Wformat-security`). Wheels are built per-platform **by hand** (no hosted CI —
`RELEASING.md`) for CPython 3.11–3.14 on Linux (x86_64/aarch64), macOS
(arm64/x86_64), Windows (AMD64); the sdist compiles the C ext locally elsewhere.
No runtime dependencies.

## The validation reality (honesty in the spec)

Linux x86_64 / 3.13t is the **primary, heavily-validated** target (the 2M-conn
runs, fuzzing, sanitizers, the formal models). macOS/BSD/Windows and aarch64
backends are **code-complete and maintained in-step** but validated more lightly;
aarch64 is exercised via qemu + review, not yet on real ARM hardware for the deep
runs — though the *weak-memory* negative controls (spec 15) *are* checked on real
Apple-silicon arm64, which is the one place x86-TSO's masking is lifted. A
re-implementer should treat the non-Linux backends as "the same design, less
soak," not "unsupported."

## Invariants

1. **All platform divergence is detected once in `plat.h`** into capability flags;
   the rest branches on flags, not raw `#ifdef`s.
2. **Exactly two subsystems are pluggable per platform** — context switch
   (compile-time) and netpoll (compile-time except Windows runtime-probe). Adding a
   platform touches only these.
3. **The `__atomic_*` / `__attribute__` / TLS shims keep the source
   single-version**; the concurrency bodies compile unchanged on MSVC.
4. **`CreateFiberEx` (reserve-big/commit-small)**, the socketpair Parker, and the
   VEH crash path are the Windows-specific must-keeps.
5. **`initial-exec` TLS** on GCC/Clang (perf), with a sanitizer fallback to
   global-dynamic (import safety).
