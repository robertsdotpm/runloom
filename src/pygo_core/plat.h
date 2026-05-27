/* plat.h -- platform / compiler / arch detection for pygo_core.
 *
 * C99-strict.  Supports GCC 3+, Clang 3+, MSVC 2008+ (with shims), ICC,
 * MinGW, Watcom, Sun Studio.  No GNU extensions in the public surface.
 *
 * Each block defines symbols downstream code can use without nested ifdefs.
 *   PYGO_OS_LINUX / OS_MACOS / OS_BSD / OS_WINDOWS / OS_SOLARIS / OS_ANDROID
 *   PYGO_ARCH_X86_64 / ARCH_X86 / ARCH_AARCH64 / ARCH_ARM / ARCH_RISCV
 *   PYGO_CC_GCC / CC_CLANG / CC_MSVC / CC_ICC / CC_WATCOM / CC_SUN
 *   PYGO_HAVE_UCONTEXT / HAVE_FIBERS / HAVE_EPOLL / HAVE_KQUEUE /
 *   HAVE_IOCP / HAVE_EVENT_PORTS
 */
#ifndef PYGO_PLAT_H
#define PYGO_PLAT_H

/* ---- OS ---- */
#if defined(__linux__) || defined(__linux)
#  define PYGO_OS_LINUX 1
#  if defined(__ANDROID__)
#    define PYGO_OS_ANDROID 1
#  endif
#elif defined(__APPLE__) && defined(__MACH__)
#  define PYGO_OS_MACOS 1
#  define PYGO_OS_BSD 1
#elif defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__) || \
      defined(__DragonFly__)
#  define PYGO_OS_BSD 1
#elif defined(__sun) && defined(__SVR4)
#  define PYGO_OS_SOLARIS 1
#elif defined(_WIN32) || defined(_WIN64) || defined(__CYGWIN__)
#  define PYGO_OS_WINDOWS 1
#else
#  define PYGO_OS_UNKNOWN 1
#endif

/* ---- Arch ---- */
#if defined(__x86_64__) || defined(_M_X64) || defined(__amd64__)
#  define PYGO_ARCH_X86_64 1
#elif defined(__i386__) || defined(_M_IX86) || defined(__i386)
#  define PYGO_ARCH_X86 1
#elif defined(__aarch64__) || defined(_M_ARM64)
#  define PYGO_ARCH_AARCH64 1
#elif defined(__arm__) || defined(_M_ARM)
#  define PYGO_ARCH_ARM 1
#elif defined(__riscv) || defined(__riscv__)
#  define PYGO_ARCH_RISCV 1
#elif defined(__powerpc64__) || defined(__ppc64__)
#  define PYGO_ARCH_PPC64 1
#elif defined(__powerpc__) || defined(__ppc__)
#  define PYGO_ARCH_PPC 1
#else
#  define PYGO_ARCH_UNKNOWN 1
#endif

/* ---- Compiler ---- */
#if defined(__clang__)
#  define PYGO_CC_CLANG 1
#elif defined(__INTEL_COMPILER) || defined(__ICC)
#  define PYGO_CC_ICC 1
#elif defined(__GNUC__)
#  define PYGO_CC_GCC 1
#elif defined(_MSC_VER)
#  define PYGO_CC_MSVC 1
#elif defined(__WATCOMC__)
#  define PYGO_CC_WATCOM 1
#elif defined(__SUNPRO_C)
#  define PYGO_CC_SUN 1
#else
#  define PYGO_CC_UNKNOWN 1
#endif

/* ---- Stack-switch backend ---- */
/* Selection priority (highest first):
 *   1. Hand-rolled inline asm (PYGO_HAVE_FCONTEXT)         -- ~20x faster
 *      than ucontext, no sigprocmask syscalls.  Currently only x86_64
 *      System V (Linux, macOS, BSD).  Extends to aarch64 / arm / riscv
 *      with one .S file per arch.
 *   2. Windows Fibers (PYGO_HAVE_FIBERS)                   -- best Windows
 *      choice; available since Win95.
 *   3. ucontext (PYGO_HAVE_UCONTEXT)                       -- POSIX fallback.
 * Exactly one of these is defined per platform. */
#if (defined(PYGO_OS_LINUX) || defined(PYGO_OS_MACOS) || defined(PYGO_OS_BSD)) \
    && defined(PYGO_ARCH_X86_64)
#  define PYGO_HAVE_FCONTEXT 1
#elif defined(PYGO_OS_WINDOWS)
#  define PYGO_HAVE_FIBERS 1
#else
#  define PYGO_HAVE_UCONTEXT 1
#endif

/* ---- Netpoll backend ---- */
#if defined(PYGO_OS_LINUX)
#  define PYGO_HAVE_EPOLL 1
#elif defined(PYGO_OS_BSD) || defined(PYGO_OS_MACOS)
#  define PYGO_HAVE_KQUEUE 1
#elif defined(PYGO_OS_WINDOWS)
#  define PYGO_HAVE_IOCP 1
#elif defined(PYGO_OS_SOLARIS)
#  define PYGO_HAVE_EVENT_PORTS 1
#endif
/* select() is always available as a fallback. */
#define PYGO_HAVE_SELECT 1

/* ---- Inline keyword ---- */
#if defined(PYGO_CC_MSVC) && _MSC_VER < 1900
#  define PYGO_INLINE __inline
#elif defined(__cplusplus)
#  define PYGO_INLINE inline
#else
#  define PYGO_INLINE static inline
#endif

/* ---- noreturn ---- */
#if defined(PYGO_CC_GCC) || defined(PYGO_CC_CLANG) || defined(PYGO_CC_ICC)
#  define PYGO_NORETURN __attribute__((noreturn))
#elif defined(PYGO_CC_MSVC)
#  define PYGO_NORETURN __declspec(noreturn)
#else
#  define PYGO_NORETURN /* nothing */
#endif

/* ---- Thread-local storage ---- */
#if defined(PYGO_CC_MSVC)
#  define PYGO_TLS __declspec(thread)
#elif defined(PYGO_CC_GCC) || defined(PYGO_CC_CLANG) || defined(PYGO_CC_ICC)
#  define PYGO_TLS __thread
#else
#  define PYGO_TLS /* must use pthread_setspecific */
#  define PYGO_TLS_FALLBACK_PTHREAD 1
#endif

/* ---- Atomic ops (subset we need: load/store ptr, CAS, fetch-add) ---- */
#if defined(PYGO_CC_GCC) || defined(PYGO_CC_CLANG) || defined(PYGO_CC_ICC)
#  define PYGO_ATOMIC_BUILTIN_GCC 1
#elif defined(PYGO_CC_MSVC)
#  define PYGO_ATOMIC_BUILTIN_MSVC 1
#endif

#endif /* PYGO_PLAT_H */
