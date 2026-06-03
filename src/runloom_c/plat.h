/* plat.h -- platform / compiler / arch detection for runloom_c.
 *
 * C99-strict.  Supports GCC 3+, Clang 3+, MSVC 2008+ (with shims), ICC,
 * MinGW, Watcom, Sun Studio.  No GNU extensions in the public surface.
 *
 * Each block defines symbols downstream code can use without nested ifdefs.
 *   RUNLOOM_OS_LINUX / OS_MACOS / OS_BSD / OS_WINDOWS / OS_SOLARIS / OS_ANDROID
 *   RUNLOOM_ARCH_X86_64 / ARCH_X86 / ARCH_AARCH64 / ARCH_ARM / ARCH_RISCV
 *   RUNLOOM_CC_GCC / CC_CLANG / CC_MSVC / CC_ICC / CC_WATCOM / CC_SUN
 *   RUNLOOM_HAVE_UCONTEXT / HAVE_FIBERS / HAVE_EPOLL / HAVE_KQUEUE /
 *   HAVE_IOCP / HAVE_EVENT_PORTS
 */
#ifndef RUNLOOM_PLAT_H
#define RUNLOOM_PLAT_H

/* ---- OS ---- */
#if defined(__linux__) || defined(__linux)
#  define RUNLOOM_OS_LINUX 1
#  if defined(__ANDROID__)
#    define RUNLOOM_OS_ANDROID 1
#  endif
#elif defined(__APPLE__) && defined(__MACH__)
#  define RUNLOOM_OS_MACOS 1
#  define RUNLOOM_OS_BSD 1
#elif defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__) || \
      defined(__DragonFly__)
#  define RUNLOOM_OS_BSD 1
#elif defined(__sun) && defined(__SVR4)
#  define RUNLOOM_OS_SOLARIS 1
#elif defined(_WIN32) || defined(_WIN64) || defined(__CYGWIN__)
#  define RUNLOOM_OS_WINDOWS 1
#else
#  define RUNLOOM_OS_UNKNOWN 1
#endif

/* ---- Arch ---- */
#if defined(__x86_64__) || defined(_M_X64) || defined(__amd64__)
#  define RUNLOOM_ARCH_X86_64 1
#elif defined(__i386__) || defined(_M_IX86) || defined(__i386)
#  define RUNLOOM_ARCH_X86 1
#elif defined(__aarch64__) || defined(_M_ARM64)
#  define RUNLOOM_ARCH_AARCH64 1
#elif defined(__arm__) || defined(_M_ARM)
#  define RUNLOOM_ARCH_ARM 1
#elif defined(__riscv) || defined(__riscv__)
#  define RUNLOOM_ARCH_RISCV 1
#elif defined(__powerpc64__) || defined(__ppc64__)
#  define RUNLOOM_ARCH_PPC64 1
#elif defined(__powerpc__) || defined(__ppc__)
#  define RUNLOOM_ARCH_PPC 1
#else
#  define RUNLOOM_ARCH_UNKNOWN 1
#endif

/* ---- Compiler ---- */
#if defined(__clang__)
#  define RUNLOOM_CC_CLANG 1
#elif defined(__INTEL_COMPILER) || defined(__ICC)
#  define RUNLOOM_CC_ICC 1
#elif defined(__GNUC__)
#  define RUNLOOM_CC_GCC 1
#elif defined(_MSC_VER)
#  define RUNLOOM_CC_MSVC 1
#elif defined(__WATCOMC__)
#  define RUNLOOM_CC_WATCOM 1
#elif defined(__SUNPRO_C)
#  define RUNLOOM_CC_SUN 1
#else
#  define RUNLOOM_CC_UNKNOWN 1
#endif

/* ---- Stack-switch backend ---- */
/* Selection priority (highest first):
 *   1. Hand-rolled inline asm (RUNLOOM_HAVE_FCONTEXT)         -- ~20x faster
 *      than ucontext, no sigprocmask syscalls.  Currently only x86_64
 *      System V (Linux, macOS, BSD).  Extends to aarch64 / arm / riscv
 *      with one .S file per arch.
 *   2. Windows Fibers (RUNLOOM_HAVE_FIBERS)                   -- best Windows
 *      choice; available since Win95.
 *   3. ucontext (RUNLOOM_HAVE_UCONTEXT)                       -- POSIX fallback.
 * Exactly one of these is defined per platform. */
#if defined(RUNLOOM_FORCE_UCONTEXT) && !defined(RUNLOOM_OS_WINDOWS)
/* Build-time override (setup.py: RUNLOOM_BACKEND=ucontext / RUNLOOM_NO_ASM).
 * Skip the asm fast path even where it is available so the POSIX
 * ucontext fallback can be exercised on asm-capable hosts. */
#  define RUNLOOM_HAVE_UCONTEXT 1
#elif (defined(RUNLOOM_OS_LINUX) || defined(RUNLOOM_OS_MACOS) || defined(RUNLOOM_OS_BSD) \
     || defined(RUNLOOM_OS_ANDROID)) \
    && (defined(RUNLOOM_ARCH_X86_64) || defined(RUNLOOM_ARCH_AARCH64))
#  define RUNLOOM_HAVE_FCONTEXT 1
#elif defined(RUNLOOM_OS_WINDOWS)
#  define RUNLOOM_HAVE_FIBERS 1
#else
#  define RUNLOOM_HAVE_UCONTEXT 1
#endif

/* ---- Netpoll backend ---- */
#if defined(RUNLOOM_FORCE_SELECT)
/* Build-time override (setup.py: RUNLOOM_NETPOLL=select).  Suppress the
 * kernel pollers so netpoll.c falls through to its select() path -- the
 * same configuration a platform with neither epoll/kqueue/event_ports
 * (e.g. Solaris/illumos) compiles.  Lets the POSIX select fallback be
 * exercised on Linux/BSD without exotic hardware.  On Windows select is
 * forced at runtime via RUNLOOM_NETPOLL=select (the pump keys on
 * RUNLOOM_OS_WINDOWS, not on a RUNLOOM_HAVE_* macro), so this define is a
 * POSIX-only knob. */
#elif defined(RUNLOOM_OS_LINUX)
#  define RUNLOOM_HAVE_EPOLL 1
#elif defined(RUNLOOM_OS_BSD) || defined(RUNLOOM_OS_MACOS)
#  define RUNLOOM_HAVE_KQUEUE 1
#elif defined(RUNLOOM_OS_WINDOWS)
#  define RUNLOOM_HAVE_IOCP 1
#elif defined(RUNLOOM_OS_SOLARIS)
#  define RUNLOOM_HAVE_EVENT_PORTS 1
#endif
/* select() is always available as a fallback. */
#define RUNLOOM_HAVE_SELECT 1

/* ---- Inline keyword ---- */
#if defined(RUNLOOM_CC_MSVC) && _MSC_VER < 1900
#  define RUNLOOM_INLINE __inline
#elif defined(__cplusplus)
#  define RUNLOOM_INLINE inline
#else
#  define RUNLOOM_INLINE static inline
#endif

/* ---- noreturn ---- */
#if defined(RUNLOOM_CC_GCC) || defined(RUNLOOM_CC_CLANG) || defined(RUNLOOM_CC_ICC)
#  define RUNLOOM_NORETURN __attribute__((noreturn))
#elif defined(RUNLOOM_CC_MSVC)
#  define RUNLOOM_NORETURN __declspec(noreturn)
#else
#  define RUNLOOM_NORETURN /* nothing */
#endif

/* ---- Thread-local storage ---- */
/* ASan/TSan ship their own initial-exec TLS and can exhaust the static-TLS
 * surplus a dlopen'd extension needs -> "cannot allocate memory in static
 * TLS block" at import. Sanitizer builds are never used for perf, so fall
 * back to the default (global-dynamic) TLS model under any sanitizer. */
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#  define RUNLOOM_TLS_GLOBAL_DYNAMIC 1
#elif defined(__has_feature)
#  if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
#    define RUNLOOM_TLS_GLOBAL_DYNAMIC 1
#  endif
#endif

#if defined(RUNLOOM_CC_MSVC)
#  define RUNLOOM_TLS __declspec(thread)
#elif defined(RUNLOOM_CC_GCC) || defined(RUNLOOM_CC_CLANG) || defined(RUNLOOM_CC_ICC)
#  if defined(RUNLOOM_TLS_GLOBAL_DYNAMIC)
#    define RUNLOOM_TLS __thread
#  else
/* initial-exec drops the __tls_get_addr() function call the default
 * global-dynamic model emits on every thread-local access (it was ~7% of
 * the chan ping-pong hot path -- perf finding F6a; current-g and the
 * per-thread sched pointer are touched on every context switch / wake).
 * runloom's whole TLS footprint is a few hundred bytes, well within glibc's
 * static-TLS surplus for a dlopen'd extension. Define
 * RUNLOOM_TLS_GLOBAL_DYNAMIC to fall back if a platform's surplus is tight. */
#    define RUNLOOM_TLS __thread __attribute__((tls_model("initial-exec")))
#  endif
#else
#  define RUNLOOM_TLS /* must use pthread_setspecific */
#  define RUNLOOM_TLS_FALLBACK_PTHREAD 1
#endif

/* ---- Atomic ops (subset we need: load/store ptr, CAS, fetch-add) ---- */
#if defined(RUNLOOM_CC_GCC) || defined(RUNLOOM_CC_CLANG) || defined(RUNLOOM_CC_ICC)
#  define RUNLOOM_ATOMIC_BUILTIN_GCC 1
#elif defined(RUNLOOM_CC_MSVC)
#  define RUNLOOM_ATOMIC_BUILTIN_MSVC 1
#endif

/* ---- GCC-extension shims for MSVC ----
 *
 * MSVC has no `__attribute__((...))` and no `__builtin_expect`.  We use
 * both in the hot paths (snap/load tagged "hot", branch hints around
 * common-case fast paths).  Shim them to no-ops so the existing source
 * compiles unchanged on MSVC; native compilers keep the real builtins.
 *
 * GCC/Clang/ICC/MinGW: skip -- their compilers handle these. */
#if defined(RUNLOOM_CC_MSVC) && !defined(__GNUC__) && !defined(__clang__)
#  define __attribute__(x)               /* drop attribute decoration */
#  define __builtin_expect(expr, val)    (expr)
#  define __builtin_unreachable()        __assume(0)
#endif

#endif /* RUNLOOM_PLAT_H */
