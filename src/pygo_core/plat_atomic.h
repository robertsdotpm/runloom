/* plat_atomic.h -- atomic op shim.
 *
 * Goal: let the rest of pygo_core use GCC's __atomic_* builtins as-is
 * (which work natively on GCC, Clang including clang-cl, ICC, and
 * MinGW-w64), AND get equivalent semantics on MSVC where those
 * builtins don't exist.
 *
 * Strategy on MSVC:
 *   - Define the __ATOMIC_* memory-order tag macros (they're constants
 *     anyway, so the compiler just sees integer literals).
 *   - Map __atomic_load_n / store_n / add_fetch / sub_fetch /
 *     compare_exchange_n / thread_fence to MSVC _Interlocked*
 *     intrinsics plus volatile dereferences.
 *   - Use C11 _Generic to dispatch the right typed helper at each call
 *     site.  Required: MSVC 19.20+ (VS 2019 16.0, May 2019) for
 *     _Generic; earlier MSVC isn't supported.
 *
 * Memory-order knobs are honoured semantically:
 *   - x86 / x64: hardware is TSO -- every load is implicitly acquire,
 *     every store is implicitly release, every RMW is SEQ_CST.  We
 *     drop the order argument and let the hardware do the right thing,
 *     using `volatile` to keep the compiler from reordering.
 *   - ARM64 (MSVC): not yet supported; the compiler does provide
 *     _Interlocked*_acq / _rel variants but we'd need an extra round
 *     of typed helpers.  Documented in setup.py as "use clang for
 *     Windows-on-ARM".
 *
 * Types supported (covering every site in pygo_core today):
 *   int, long, long long, void* (and Py_ssize_t via long-long alias)
 *
 * On any compiler that already provides __atomic_*, this header is a
 * no-op -- we test for that via __has_builtin / __GNUC__ / __clang__.
 */
#ifndef PYGO_PLAT_ATOMIC_H
#define PYGO_PLAT_ATOMIC_H

#include "plat.h"

/* Detect "compiler provides __atomic_* natively".  GCC 4.7+, Clang 3.1+,
 * ICC 17+, and MinGW-w64 (which is GCC) all do. */
#if defined(__GNUC__) || defined(__clang__) || defined(__INTEL_COMPILER)
#  define PYGO_HAS_GCC_ATOMICS 1
#endif

#if !defined(PYGO_HAS_GCC_ATOMICS) && defined(_MSC_VER)
   /* ============================================================
    * MSVC path: provide __atomic_* macros backed by Interlocked*
    * ============================================================ */
   /* WIN32_LEAN_AND_MEAN keeps <windows.h> from pulling in the OLD
    * <winsock.h> (Winsock 1.x), which clashes with <winsock2.h> that
    * plat_compat.h includes for the rest of the codebase.  NOMINMAX
    * stops the windows.h min/max macros from breaking std uses. */
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN 1
#  endif
#  ifndef NOMINMAX
#    define NOMINMAX 1
#  endif
#  include <intrin.h>
#  include <windows.h>

#  if !defined(_M_X64) && !defined(_M_IX86) && !defined(_M_AMD64)
#    error "pygo on MSVC currently requires x86 or x64; for ARM64 use clang or MinGW-w64"
#  endif

#  ifndef __ATOMIC_RELAXED
#    define __ATOMIC_RELAXED  0
#    define __ATOMIC_CONSUME  1
#    define __ATOMIC_ACQUIRE  2
#    define __ATOMIC_RELEASE  3
#    define __ATOMIC_ACQ_REL  4
#    define __ATOMIC_SEQ_CST  5
#  endif

   /* ---- typed load helpers (x86/x64: aligned loads <= 64-bit are atomic) ---- */
   static __forceinline int       pygo_atomic_load_i(const volatile int       *p) { return *p; }
   static __forceinline long      pygo_atomic_load_l(const volatile long      *p) { return *p; }
   static __forceinline long long pygo_atomic_load_ll(const volatile long long *p) { return *p; }
   static __forceinline void *    pygo_atomic_load_p(void * const volatile *p)   { return *p; }

   /* ---- typed store helpers ---- */
   static __forceinline void pygo_atomic_store_i(volatile int       *p, int v)       { *p = v; }
   static __forceinline void pygo_atomic_store_l(volatile long      *p, long v)      { *p = v; }
   static __forceinline void pygo_atomic_store_ll(volatile long long *p, long long v) { *p = v; }

   /* ---- typed add_fetch helpers (return new value, like GCC builtin) ---- */
   static __forceinline int       pygo_atomic_add_i (volatile int       *p, int v)       { return (int)_InterlockedExchangeAdd((volatile long *)p, (long)v) + v; }
   static __forceinline long      pygo_atomic_add_l (volatile long      *p, long v)      { return _InterlockedExchangeAdd(p, v) + v; }
   static __forceinline long long pygo_atomic_add_ll(volatile long long *p, long long v) { return _InterlockedExchangeAdd64(p, v) + v; }

   /* ---- typed sub_fetch helpers ---- */
   static __forceinline int       pygo_atomic_sub_i (volatile int       *p, int v)       { return (int)_InterlockedExchangeAdd((volatile long *)p, -(long)v) - v; }
   static __forceinline long      pygo_atomic_sub_l (volatile long      *p, long v)      { return _InterlockedExchangeAdd(p, -v) - v; }
   static __forceinline long long pygo_atomic_sub_ll(volatile long long *p, long long v) { return _InterlockedExchangeAdd64(p, -v) - v; }

   /* ---- typed CAS helpers (return 1 on success, 0 on mismatch + update *expected) ---- */
   static __forceinline int pygo_atomic_cas_l(volatile long *p, long *expected, long desired) {
       long prev = _InterlockedCompareExchange(p, desired, *expected);
       if (prev == *expected) return 1;
       *expected = prev;
       return 0;
   }
   static __forceinline int pygo_atomic_cas_ll(volatile long long *p, long long *expected, long long desired) {
       long long prev = _InterlockedCompareExchange64(p, desired, *expected);
       if (prev == *expected) return 1;
       *expected = prev;
       return 0;
   }

   /* ---- _Generic dispatch.  Match by pointer type to the typed
    *      helper.  Requires C11 _Generic (MSVC 19.20+). ---- */

#  define __atomic_load_n(p, ord)                                     \
       _Generic((p),                                                  \
           int *:                pygo_atomic_load_i,                  \
           const int *:          pygo_atomic_load_i,                  \
           volatile int *:       pygo_atomic_load_i,                  \
           const volatile int *: pygo_atomic_load_i,                  \
           long *:                pygo_atomic_load_l,                 \
           const long *:          pygo_atomic_load_l,                 \
           volatile long *:       pygo_atomic_load_l,                 \
           const volatile long *: pygo_atomic_load_l,                 \
           long long *:                pygo_atomic_load_ll,           \
           const long long *:          pygo_atomic_load_ll,           \
           volatile long long *:       pygo_atomic_load_ll,           \
           const volatile long long *: pygo_atomic_load_ll            \
       )((p))

#  define __atomic_store_n(p, v, ord)                                 \
       _Generic((p),                                                  \
           int *:           pygo_atomic_store_i,                      \
           volatile int *:  pygo_atomic_store_i,                      \
           long *:          pygo_atomic_store_l,                      \
           volatile long *: pygo_atomic_store_l,                      \
           long long *:           pygo_atomic_store_ll,               \
           volatile long long *:  pygo_atomic_store_ll                \
       )((p), (v))

#  define __atomic_add_fetch(p, v, ord)                               \
       _Generic((p),                                                  \
           int *:           pygo_atomic_add_i,                        \
           volatile int *:  pygo_atomic_add_i,                        \
           long *:          pygo_atomic_add_l,                        \
           volatile long *: pygo_atomic_add_l,                        \
           long long *:           pygo_atomic_add_ll,                 \
           volatile long long *:  pygo_atomic_add_ll                  \
       )((p), (v))

#  define __atomic_sub_fetch(p, v, ord)                               \
       _Generic((p),                                                  \
           int *:           pygo_atomic_sub_i,                        \
           volatile int *:  pygo_atomic_sub_i,                        \
           long *:          pygo_atomic_sub_l,                        \
           volatile long *: pygo_atomic_sub_l,                        \
           long long *:           pygo_atomic_sub_ll,                 \
           volatile long long *:  pygo_atomic_sub_ll                  \
       )((p), (v))

#  define __atomic_compare_exchange_n(p, expp, des, weak, sord, ford) \
       _Generic((p),                                                  \
           long *:          pygo_atomic_cas_l,                        \
           volatile long *: pygo_atomic_cas_l,                        \
           long long *:           pygo_atomic_cas_ll,                 \
           volatile long long *:  pygo_atomic_cas_ll                  \
       )((p), (expp), (des))

#  define __atomic_thread_fence(ord)  MemoryBarrier()

#endif /* MSVC path */

#endif /* PYGO_PLAT_ATOMIC_H */
