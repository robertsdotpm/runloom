/* plat_compat.h -- runtime shims that let the rest of pygo_core use one
 * uniform API across POSIX (Linux / macOS / BSD / Solaris) and Win32.
 *
 * Three groups:
 *   1. mutex          pygo_mutex_t + init/lock/unlock/destroy
 *   2. thread         pygo_thread_t + create/join
 *   3. clock & sleep  pygo_monotonic_ns / pygo_sleep_ns
 *
 * POSIX maps each to pthread / clock_gettime / nanosleep.
 * Windows maps each to CRITICAL_SECTION / _beginthreadex /
 * QueryPerformanceCounter / Sleep.  Everything else compiles unchanged.
 *
 * Header-only: every function is `static inline` so consumers don't link
 * an extra TU.  PYGO_INLINE is defined in plat.h to expand to whatever
 * the compiler accepts.
 */
#ifndef PYGO_PLAT_COMPAT_H
#define PYGO_PLAT_COMPAT_H

#include "plat.h"
#include "plat_atomic.h"

#include <stdint.h>

#if defined(PYGO_OS_WINDOWS)
/* WIN32_LEAN_AND_MEAN trims windows.h from ~700 KB to ~80 KB pre-processed.
 * NOMINMAX prevents windows.h's min()/max() macros from clobbering std uses. */
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN 1
#  endif
#  ifndef NOMINMAX
#    define NOMINMAX 1
#  endif
#  include <winsock2.h>
#  include <ws2tcpip.h>
#  include <windows.h>
#  include <process.h>     /* _beginthreadex */
#  include <timeapi.h>     /* timeBeginPeriod (winmm) */
#  include <stdlib.h>      /* getenv */
#  include <string.h>      /* strcmp */
#else
#  include <pthread.h>
#  include <time.h>
#  include <errno.h>
#  if defined(PYGO_OS_LINUX) || defined(PYGO_OS_BSD) || defined(PYGO_OS_MACOS) \
      || defined(PYGO_OS_SOLARIS) || defined(PYGO_OS_ANDROID)
#    include <unistd.h>
#  endif
#endif

/* ============================================================ */
/* mutex                                                        */
/* ============================================================ */
#if defined(PYGO_OS_WINDOWS)
typedef CRITICAL_SECTION pygo_mutex_t;

PYGO_INLINE int  pygo_mutex_init(pygo_mutex_t *m) {
    InitializeCriticalSection(m); return 0;
}
PYGO_INLINE void pygo_mutex_destroy(pygo_mutex_t *m) {
    DeleteCriticalSection(m);
}
PYGO_INLINE void pygo_mutex_lock(pygo_mutex_t *m) {
    EnterCriticalSection(m);
}
PYGO_INLINE void pygo_mutex_unlock(pygo_mutex_t *m) {
    LeaveCriticalSection(m);
}
/* CRITICAL_SECTION cannot be statically initialised; callers must call
 * pygo_mutex_init() once before first use.  POSIX gets a real static
 * initialiser; on Windows we expose a zero-initialiser + an init-once
 * helper consumers can call from PyInit. */
#  define PYGO_MUTEX_STATIC_INIT  {0}
#else
typedef pthread_mutex_t pygo_mutex_t;
PYGO_INLINE int  pygo_mutex_init(pygo_mutex_t *m) {
    return pthread_mutex_init(m, NULL);
}
PYGO_INLINE void pygo_mutex_destroy(pygo_mutex_t *m) {
    pthread_mutex_destroy(m);
}
PYGO_INLINE void pygo_mutex_lock(pygo_mutex_t *m) {
    pthread_mutex_lock(m);
}
PYGO_INLINE void pygo_mutex_unlock(pygo_mutex_t *m) {
    pthread_mutex_unlock(m);
}
#  define PYGO_MUTEX_STATIC_INIT  PTHREAD_MUTEX_INITIALIZER
#endif

/* ============================================================ */
/* thread                                                       */
/* ============================================================ */
#if defined(PYGO_OS_WINDOWS)
typedef HANDLE pygo_thread_t;
typedef unsigned (__stdcall *pygo_thread_fn)(void *);

PYGO_INLINE int pygo_thread_create(pygo_thread_t *t,
                                   pygo_thread_fn fn,
                                   void *arg) {
    /* _beginthreadex (not CreateThread) so the CRT thread state is
     * initialised properly -- prevents printf/errno corruption when the
     * thread later calls into the C runtime. */
    uintptr_t h = _beginthreadex(NULL, 0, fn, arg, 0, NULL);
    if (h == 0) return -1;
    *t = (HANDLE)h;
    return 0;
}
PYGO_INLINE int pygo_thread_join(pygo_thread_t t) {
    WaitForSingleObject(t, INFINITE);
    CloseHandle(t);
    return 0;
}
/* Adapter for POSIX-style thread entry signature (void * -> void *) so
 * the same hub_main body works.  Casts the void* return through. */
#  define PYGO_THREAD_RET     unsigned __stdcall
#  define PYGO_THREAD_RETURN(v)  return (unsigned)(uintptr_t)(v)
#else
typedef pthread_t pygo_thread_t;
typedef void *(*pygo_thread_fn)(void *);

PYGO_INLINE int pygo_thread_create(pygo_thread_t *t,
                                   pygo_thread_fn fn,
                                   void *arg) {
    return pthread_create(t, NULL, fn, arg);
}
PYGO_INLINE int pygo_thread_join(pygo_thread_t t) {
    return pthread_join(t, NULL);
}
#  define PYGO_THREAD_RET     void *
#  define PYGO_THREAD_RETURN(v)  return (v)
#endif

/* ============================================================ */
/* condition variable (pairs with pygo_mutex_t)                 */
/* ============================================================ */
/* Used by the blocking-offload pool's worker threads to sleep on an
 * empty job queue.  Windows CONDITION_VARIABLE is Vista+; that's fine --
 * the M:N / blocking-pool machinery only matters on free-threaded
 * Python, which is far newer than XP.  Like pygo_mutex_t there is no
 * static initialiser on Windows, so callers must pygo_cond_init() once. */
#if defined(PYGO_OS_WINDOWS)
typedef CONDITION_VARIABLE pygo_cond_t;
PYGO_INLINE int  pygo_cond_init(pygo_cond_t *c) {
    InitializeConditionVariable(c); return 0;
}
PYGO_INLINE void pygo_cond_destroy(pygo_cond_t *c) { (void)c; }
PYGO_INLINE void pygo_cond_wait(pygo_cond_t *c, pygo_mutex_t *m) {
    SleepConditionVariableCS(c, m, INFINITE);
}
PYGO_INLINE void pygo_cond_signal(pygo_cond_t *c)    { WakeConditionVariable(c); }
PYGO_INLINE void pygo_cond_broadcast(pygo_cond_t *c) { WakeAllConditionVariable(c); }
#else
typedef pthread_cond_t pygo_cond_t;
PYGO_INLINE int  pygo_cond_init(pygo_cond_t *c) {
    return pthread_cond_init(c, NULL);
}
PYGO_INLINE void pygo_cond_destroy(pygo_cond_t *c) { pthread_cond_destroy(c); }
PYGO_INLINE void pygo_cond_wait(pygo_cond_t *c, pygo_mutex_t *m) {
    pthread_cond_wait(c, m);
}
PYGO_INLINE void pygo_cond_signal(pygo_cond_t *c)    { pthread_cond_signal(c); }
PYGO_INLINE void pygo_cond_broadcast(pygo_cond_t *c) { pthread_cond_broadcast(c); }
#endif

/* ============================================================ */
/* monotonic clock                                              */
/* ============================================================ */
#if defined(PYGO_OS_WINDOWS)
PYGO_INLINE long long pygo_monotonic_ns(void) {
    /* QueryPerformanceCounter ticks at 10 MHz on modern Windows (10+)
     * which gives 100 ns resolution -- more than enough for our scheduler.
     * The QueryPerformanceFrequency value is constant per boot. */
    static LARGE_INTEGER freq = {0};
    LARGE_INTEGER now;
    if (freq.QuadPart == 0) {
        QueryPerformanceFrequency(&freq);
        if (freq.QuadPart == 0) return 0;
    }
    QueryPerformanceCounter(&now);
    return (long long)((now.QuadPart * 1000000000LL) / freq.QuadPart);
}
#else
PYGO_INLINE long long pygo_monotonic_ns(void) {
    struct timespec ts;
#  if defined(CLOCK_MONOTONIC)
    if (clock_gettime(CLOCK_MONOTONIC, &ts) == 0) {
        return (long long)ts.tv_sec * 1000000000LL + (long long)ts.tv_nsec;
    }
#  endif
    return 0;
}
#endif

PYGO_INLINE double pygo_monotonic_seconds_compat(void) {
    return (double)pygo_monotonic_ns() * 1e-9;
}

/* ============================================================ */
/* sleep                                                        */
/* ============================================================ */
#if defined(PYGO_OS_WINDOWS)
PYGO_INLINE void pygo_sleep_ns(long long ns) {
    /* Windows Sleep() takes milliseconds.  Round up so a 1 ns request
     * sleeps at least 1 ms (the smallest Sleep can express).  Sleep(0)
     * is yield-equivalent and we never want that on a timer path -- a
     * 0 ns argument means "don't sleep at all" -> early return. */
    if (ns <= 0) return;
    DWORD ms = (DWORD)((ns + 999999LL) / 1000000LL);
    if (ms == 0) ms = 1;
    Sleep(ms);
}
#else
PYGO_INLINE void pygo_sleep_ns(long long ns) {
    struct timespec req, rem;
    if (ns <= 0) return;
    req.tv_sec  = (time_t)(ns / 1000000000LL);
    req.tv_nsec = (long)(ns % 1000000000LL);
    /* EINTR -> resume with the remainder; otherwise we'd silently
     * truncate the sleep on signal delivery. */
    while (nanosleep(&req, &rem) == -1 && errno == EINTR) {
        req = rem;
    }
}
#endif

/* ============================================================ */
/* CPU count                                                    */
/* ============================================================ */
#if defined(PYGO_OS_WINDOWS)
PYGO_INLINE int pygo_cpu_count(void) {
    /* GetActiveProcessorCount accounts for processor groups (>64 cores
     * on Windows 7+).  ALL_PROCESSOR_GROUPS = 0xFFFF.  Falls back to
     * GetSystemInfo for very old hosts. */
    typedef DWORD (WINAPI *gapc_fn)(WORD);
    HMODULE k32 = GetModuleHandleA("kernel32.dll");
    if (k32 != NULL) {
        gapc_fn gapc = (gapc_fn)(void *)
            GetProcAddress(k32, "GetActiveProcessorCount");
        if (gapc != NULL) {
            DWORD n = gapc((WORD)0xFFFF);
            if (n > 0) return (int)n;
        }
    }
    {
        SYSTEM_INFO si;
        GetSystemInfo(&si);
        if (si.dwNumberOfProcessors > 0) return (int)si.dwNumberOfProcessors;
    }
    return 4;
}
#else
#  if defined(PYGO_OS_BSD) || defined(PYGO_OS_MACOS)
#    include <sys/sysctl.h>
#  endif
PYGO_INLINE int pygo_cpu_count(void) {
    long n;
#  if defined(_SC_NPROCESSORS_ONLN)
    n = sysconf(_SC_NPROCESSORS_ONLN);
    if (n > 0) return (int)n;
#  endif
#  if defined(PYGO_OS_BSD) || defined(PYGO_OS_MACOS)
    /* Some BSDs / older macOS lack _SC_NPROCESSORS_ONLN. */
    {
        int mib[2] = { CTL_HW, HW_NCPU };
        int cpu = 0;
        size_t len = sizeof(cpu);
        if (sysctl(mib, 2, &cpu, &len, NULL, 0) == 0 && cpu > 0) {
            return cpu;
        }
    }
#  endif
    return 4;
}
#endif

/* ============================================================ */
/* one-time process init for things that need explicit setup    */
/* ============================================================ */
#if defined(PYGO_OS_WINDOWS)
PYGO_INLINE void pygo_winsock_init(void) {
    static volatile LONG done = 0;
    if (InterlockedCompareExchange(&done, 1, 0) == 0) {
        WSADATA wsa;
        WSAStartup(MAKEWORD(2, 2), &wsa);
        /* No matching WSACleanup -- the process is going down anyway when
         * we'd want one, and CRT atexit ordering vs sockets is brittle. */
    }
}

/* Raise the system timer resolution to 1ms.  Windows defaults to a ~15.6ms
 * tick, and the scheduler waits on sleep/timer deadlines at that granularity --
 * so EVERY sub-15ms sched_sleep costs a full ~15.6ms tick.  That makes
 * timer-bound workloads (the aio keepalive's 2ms poll that delivers
 * call_soon_threadsafe results, asyncio.sleep, etc.) 10-15x slower and trips
 * test timeouts (aiosqlite's 1000-op close test ran ~16ms/op -> ~16s).  1ms is
 * what Go's runtime requests on Windows.  Called UNCONDITIONALLY from module
 * init -- not winsock_init -- because socket-less workloads (pure sched_sleep /
 * call_soon_threadsafe) never touch netpoll.  Opt out with PYGO_WIN_TIMER_RES=0.
 * No timeEndPeriod: held for the process lifetime, like winsock above. */
PYGO_INLINE void pygo_timer_res_init(void) {
    static volatile LONG done = 0;
    if (InterlockedCompareExchange(&done, 1, 0) == 0) {
        const char *res = getenv("PYGO_WIN_TIMER_RES");
        if (res == NULL || strcmp(res, "0") != 0) {
            timeBeginPeriod(1);
        }
    }
}
#else
PYGO_INLINE void pygo_winsock_init(void) { /* no-op on POSIX */ }
PYGO_INLINE void pygo_timer_res_init(void) { /* no-op on POSIX */ }
#endif

#endif /* PYGO_PLAT_COMPAT_H */
