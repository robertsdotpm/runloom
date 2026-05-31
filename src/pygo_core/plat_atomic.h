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
 *   int, long, long long, unsigned char, unsigned int,
 *   unsigned long long (== size_t on x64), and object pointers
 *   (unsigned char*, struct pygo_g* -- dispatched via a (void*) slot).
 *   Py_ssize_t rides the long-long alias; uint32/64_t ride unsigned
 *   int / unsigned long long.  fetch_or/fetch_and are byte-only (the
 *   netpoll pending-wake bitmap); add a wider helper if a new site needs it.
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
   static __forceinline unsigned char      pygo_atomic_load_uc (const volatile unsigned char      *p) { return *p; }
   static __forceinline unsigned int       pygo_atomic_load_u  (const volatile unsigned int       *p) { return *p; }
   static __forceinline unsigned long long pygo_atomic_load_ull(const volatile unsigned long long *p) { return *p; }
   /* Pointer load/store take the slot as a generic (volatile void *) so any
    * T** converts implicitly (T** -> void* is allowed; T** -> void** is not).
    * On x86/x64 a pointer-sized aligned access is atomic. */
   static __forceinline void *pygo_atomic_load_ptr(volatile void *p) { return *(void * volatile *)p; }

   /* ---- typed store helpers ---- */
   static __forceinline void pygo_atomic_store_i(volatile int       *p, int v)       { *p = v; }
   static __forceinline void pygo_atomic_store_l(volatile long      *p, long v)      { *p = v; }
   static __forceinline void pygo_atomic_store_ll(volatile long long *p, long long v) { *p = v; }
   static __forceinline void pygo_atomic_store_uc (volatile unsigned char      *p, unsigned char v)      { *p = v; }
   static __forceinline void pygo_atomic_store_ull(volatile unsigned long long *p, unsigned long long v) { *p = v; }
   static __forceinline void pygo_atomic_store_ptr(volatile void *p, void *v) { *(void * volatile *)p = v; }

   /* ---- typed add_fetch helpers (return new value, like GCC builtin)
    *
    * MSVC _Interlocked* signatures use LONG / __int64 (== long /
    * long-long on Windows MSVC but distinct type identities), so we
    * cast through (volatile LONG *) and (volatile __int64 *) at the
    * call to keep the compiler happy without changing semantics. */
   static __forceinline int       pygo_atomic_add_i (volatile int       *p, int v)       { return (int)_InterlockedExchangeAdd((volatile LONG *)p, (LONG)v) + v; }
   static __forceinline long      pygo_atomic_add_l (volatile long      *p, long v)      { return (long)_InterlockedExchangeAdd((volatile LONG *)p, (LONG)v) + v; }
   static __forceinline long long pygo_atomic_add_ll(volatile long long *p, long long v) { return (long long)_InterlockedExchangeAdd64((volatile __int64 *)p, (__int64)v) + v; }
   static __forceinline unsigned int       pygo_atomic_add_u  (volatile unsigned int       *p, unsigned int v)       { return (unsigned int)_InterlockedExchangeAdd((volatile LONG *)p, (LONG)v) + v; }
   static __forceinline unsigned long long pygo_atomic_add_ull(volatile unsigned long long *p, unsigned long long v) { return (unsigned long long)_InterlockedExchangeAdd64((volatile __int64 *)p, (__int64)v) + v; }

   /* ---- typed sub_fetch helpers ---- */
   static __forceinline int       pygo_atomic_sub_i (volatile int       *p, int v)       { return (int)_InterlockedExchangeAdd((volatile LONG *)p, -(LONG)v) - v; }
   static __forceinline long      pygo_atomic_sub_l (volatile long      *p, long v)      { return (long)_InterlockedExchangeAdd((volatile LONG *)p, -(LONG)v) - v; }
   static __forceinline long long pygo_atomic_sub_ll(volatile long long *p, long long v) { return (long long)_InterlockedExchangeAdd64((volatile __int64 *)p, -(__int64)v) - v; }

   /* ---- typed CAS helpers (return 1 on success, 0 on mismatch + update *expected) ---- */
   static __forceinline int pygo_atomic_cas_i(volatile int *p, int *expected, int desired) {
       LONG prev = _InterlockedCompareExchange((volatile LONG *)p, (LONG)desired, (LONG)*expected);
       if ((int)prev == *expected) return 1;
       *expected = (int)prev;
       return 0;
   }
   static __forceinline int pygo_atomic_cas_l(volatile long *p, long *expected, long desired) {
       LONG prev = _InterlockedCompareExchange((volatile LONG *)p, (LONG)desired, (LONG)*expected);
       if ((long)prev == *expected) return 1;
       *expected = (long)prev;
       return 0;
   }
   static __forceinline int pygo_atomic_cas_ll(volatile long long *p, long long *expected, long long desired) {
       __int64 prev = _InterlockedCompareExchange64((volatile __int64 *)p, (__int64)desired, (__int64)*expected);
       if ((long long)prev == *expected) return 1;
       *expected = (long long)prev;
       return 0;
   }
   static __forceinline int pygo_atomic_cas_uc(volatile unsigned char *p, unsigned char *expected, unsigned char desired) {
       char prev = _InterlockedCompareExchange8((char volatile *)p, (char)desired, (char)*expected);
       if ((unsigned char)prev == *expected) return 1;
       *expected = (unsigned char)prev;
       return 0;
   }

   /* ---- fetch_or / fetch_and on a byte.  Return the OLD value, matching
    *      the GCC __atomic_fetch_* contract.  _InterlockedOr8/_And8 are
    *      x86/x64 intrinsics that return the prior byte.  Used by the
    *      netpoll pending-wake bitmap (set/consume one fd's mask). ---- */
   static __forceinline unsigned char pygo_atomic_or_uc (volatile unsigned char *p, unsigned char v) { return (unsigned char)_InterlockedOr8 ((char volatile *)p, (char)v); }
   static __forceinline unsigned char pygo_atomic_and_uc(volatile unsigned char *p, unsigned char v) { return (unsigned char)_InterlockedAnd8((char volatile *)p, (char)v); }

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
           const volatile long long *: pygo_atomic_load_ll,           \
           unsigned char *:                pygo_atomic_load_uc,       \
           const unsigned char *:          pygo_atomic_load_uc,       \
           volatile unsigned char *:       pygo_atomic_load_uc,       \
           const volatile unsigned char *: pygo_atomic_load_uc,       \
           unsigned int *:                pygo_atomic_load_u,         \
           const unsigned int *:          pygo_atomic_load_u,         \
           volatile unsigned int *:       pygo_atomic_load_u,         \
           const volatile unsigned int *: pygo_atomic_load_u,         \
           unsigned long long *:                pygo_atomic_load_ull, \
           const unsigned long long *:          pygo_atomic_load_ull, \
           volatile unsigned long long *:       pygo_atomic_load_ull, \
           const volatile unsigned long long *: pygo_atomic_load_ull, \
           unsigned char **:           pygo_atomic_load_ptr,          \
           struct pygo_g **:           pygo_atomic_load_ptr           \
       )((p))

#  define __atomic_store_n(p, v, ord)                                 \
       _Generic((p),                                                  \
           int *:           pygo_atomic_store_i,                      \
           volatile int *:  pygo_atomic_store_i,                      \
           long *:          pygo_atomic_store_l,                      \
           volatile long *: pygo_atomic_store_l,                      \
           long long *:           pygo_atomic_store_ll,               \
           volatile long long *:  pygo_atomic_store_ll,               \
           unsigned char *:          pygo_atomic_store_uc,            \
           volatile unsigned char *: pygo_atomic_store_uc,            \
           unsigned long long *:           pygo_atomic_store_ull,     \
           volatile unsigned long long *:  pygo_atomic_store_ull,     \
           unsigned char **:         pygo_atomic_store_ptr            \
       )((p), (v))

#  define __atomic_add_fetch(p, v, ord)                               \
       _Generic((p),                                                  \
           int *:           pygo_atomic_add_i,                        \
           volatile int *:  pygo_atomic_add_i,                        \
           long *:          pygo_atomic_add_l,                        \
           volatile long *: pygo_atomic_add_l,                        \
           long long *:           pygo_atomic_add_ll,                 \
           volatile long long *:  pygo_atomic_add_ll,                 \
           unsigned int *:          pygo_atomic_add_u,                \
           volatile unsigned int *: pygo_atomic_add_u,                \
           unsigned long long *:           pygo_atomic_add_ull,       \
           volatile unsigned long long *:  pygo_atomic_add_ull        \
       )((p), (v))

   /* fetch_add (returns OLD value -- contrast with add_fetch which
    * returns the post-increment value).  Used by pygo_mn_spawn_counter
    * to give each spawned g a unique index. */
   static __forceinline int       pygo_atomic_fetch_add_i (volatile int       *p, int v)       { return (int)_InterlockedExchangeAdd((volatile LONG *)p, (LONG)v); }
   static __forceinline long      pygo_atomic_fetch_add_l (volatile long      *p, long v)      { return (long)_InterlockedExchangeAdd((volatile LONG *)p, (LONG)v); }
   static __forceinline long long pygo_atomic_fetch_add_ll(volatile long long *p, long long v) { return (long long)_InterlockedExchangeAdd64((volatile __int64 *)p, (__int64)v); }

#  define __atomic_fetch_add(p, v, ord)                               \
       _Generic((p),                                                  \
           int *:           pygo_atomic_fetch_add_i,                  \
           volatile int *:  pygo_atomic_fetch_add_i,                  \
           long *:          pygo_atomic_fetch_add_l,                  \
           volatile long *: pygo_atomic_fetch_add_l,                  \
           long long *:           pygo_atomic_fetch_add_ll,           \
           volatile long long *:  pygo_atomic_fetch_add_ll            \
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
           int *:           pygo_atomic_cas_i,                        \
           volatile int *:  pygo_atomic_cas_i,                        \
           long *:          pygo_atomic_cas_l,                        \
           volatile long *: pygo_atomic_cas_l,                        \
           long long *:           pygo_atomic_cas_ll,                 \
           volatile long long *:  pygo_atomic_cas_ll,                 \
           unsigned char *:          pygo_atomic_cas_uc,              \
           volatile unsigned char *: pygo_atomic_cas_uc              \
       )((p), (expp), (des))

#  define __atomic_fetch_or(p, v, ord)                                \
       _Generic((p),                                                  \
           unsigned char *:          pygo_atomic_or_uc,               \
           volatile unsigned char *: pygo_atomic_or_uc                \
       )((p), (v))

#  define __atomic_fetch_and(p, v, ord)                               \
       _Generic((p),                                                  \
           unsigned char *:          pygo_atomic_and_uc,              \
           volatile unsigned char *: pygo_atomic_and_uc               \
       )((p), (v))

#  define __atomic_thread_fence(ord)  MemoryBarrier()

#endif /* MSVC path */

#endif /* PYGO_PLAT_ATOMIC_H */
