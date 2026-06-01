/*
 * faultinj.c -- a tiny LD_PRELOAD fault injector for pygo's cleanup paths.
 *
 * No libfiu dependency.  Interposes the allocators and syscalls pygo's C
 * extension uses for setup, and fails a *chosen* call so the error/cleanup
 * branch runs -- the 170 uncovered ENOMEM / `return -1` / arg-parse-fail
 * lines that tools/coverage.sh surfaces.  A graceful failure (Python
 * MemoryError / OSError, or clean shutdown) is correct; a SIGSEGV / abort /
 * hang on the failure path is a cleanup bug.
 *
 * Build:  gcc -shared -fPIC -O2 -o faultinj.so faultinj.c -ldl
 * Use:    LD_PRELOAD=./faultinj.so \
 *         FAULTINJ_TARGET=malloc FAULTINJ_NTH=42 python prog.py
 *
 * Env:
 *   FAULTINJ_TARGET  malloc|calloc|realloc|mmap|epoll_ctl|eventfd|timerfd
 *   FAULTINJ_NTH     1-based index of the matching call to fail (0 = off)
 *   FAULTINJ_ALL     "1" -> fail the Nth and every subsequent matching call
 *   FAULTINJ_ERRNO   errno to set on the injected failure (default 12=ENOMEM)
 *   FAULTINJ_VERBOSE "1" -> log each injected fault to stderr
 *
 * Only the targeted function counts/fails; everything else is pass-through.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <stdint.h>
#include <stdatomic.h>
#include <sys/mman.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>

enum { T_NONE, T_MALLOC, T_CALLOC, T_REALLOC, T_MMAP, T_EPOLL_CTL, T_EVENTFD, T_TIMERFD };

static int          g_target  = T_NONE;
static long         g_nth     = 0;
static int          g_all     = 0;
static int          g_errno   = 12;   /* ENOMEM */
static int          g_verbose = 0;
static atomic_long  g_count   = 0;

static void *(*real_malloc)(size_t);
static void *(*real_calloc)(size_t, size_t);
static void *(*real_realloc)(void *, size_t);
static void  (*real_free)(void *);
static void *(*real_mmap)(void *, size_t, int, int, int, off_t);
static int   (*real_epoll_ctl)(int, int, int, struct epoll_event *);
static int   (*real_eventfd)(unsigned int, int);
static int   (*real_timerfd_create)(int, int);

/* bump buffer to satisfy allocations made *during* dlsym bootstrap, so
 * resolving real_malloc cannot recurse into a not-yet-resolved real_malloc. */
static char   boot[1 << 16];
static size_t boot_off;
static int    in_dlsym;

static int boot_owns(void *p) {
    return (char *)p >= boot && (char *)p < boot + sizeof boot;
}
static void *boot_alloc(size_t n) {
    size_t a = (n + 15u) & ~(size_t)15u;
    if (boot_off + a > sizeof boot) return NULL;
    void *p = boot + boot_off;
    boot_off += a;
    return p;
}

static int parse_target(const char *s) {
    if (!s) return T_NONE;
    if (!strcmp(s, "malloc"))    return T_MALLOC;
    if (!strcmp(s, "calloc"))    return T_CALLOC;
    if (!strcmp(s, "realloc"))   return T_REALLOC;
    if (!strcmp(s, "mmap"))      return T_MMAP;
    if (!strcmp(s, "epoll_ctl")) return T_EPOLL_CTL;
    if (!strcmp(s, "eventfd"))   return T_EVENTFD;
    if (!strcmp(s, "timerfd"))   return T_TIMERFD;
    return T_NONE;
}

__attribute__((constructor))
static void faultinj_init(void) {
    g_target = parse_target(getenv("FAULTINJ_TARGET"));
    const char *n = getenv("FAULTINJ_NTH");   if (n) g_nth   = atol(n);
    const char *e = getenv("FAULTINJ_ERRNO"); if (e) g_errno = atoi(e);
    const char *a = getenv("FAULTINJ_ALL");      g_all     = (a && a[0] == '1');
    const char *v = getenv("FAULTINJ_VERBOSE");  g_verbose = (v && v[0] == '1');
}

/* returns 1 iff this call to the active target should be failed. */
static int should_fail(int which) {
    if (g_target != which || g_nth <= 0) return 0;
    long c = atomic_fetch_add(&g_count, 1) + 1;
    int hit = (c == g_nth) || (g_all && c >= g_nth);
    if (hit && g_verbose)
        fprintf(stderr, "[faultinj] inject failure on call #%ld (target=%d errno=%d)\n",
                c, which, g_errno);
    return hit;
}

void *malloc(size_t n) {
    if (!real_malloc) {
        in_dlsym = 1;
        real_malloc = dlsym(RTLD_NEXT, "malloc");
        in_dlsym = 0;
    }
    if (in_dlsym || !real_malloc) return boot_alloc(n);
    if (should_fail(T_MALLOC)) { errno = g_errno; return NULL; }
    return real_malloc(n);
}

void *calloc(size_t nmemb, size_t size) {
    if (!real_calloc) {
        in_dlsym = 1;
        real_calloc = dlsym(RTLD_NEXT, "calloc");
        in_dlsym = 0;
    }
    if (in_dlsym || !real_calloc) {
        void *p = boot_alloc(nmemb * size);
        if (p) memset(p, 0, nmemb * size);
        return p;
    }
    if (should_fail(T_CALLOC)) { errno = g_errno; return NULL; }
    return real_calloc(nmemb, size);
}

void *realloc(void *ptr, size_t n) {
    if (!real_realloc) real_realloc = dlsym(RTLD_NEXT, "realloc");
    if (boot_owns(ptr)) {                 /* grew a bootstrap alloc */
        void *p = real_realloc ? real_malloc(n) : boot_alloc(n);
        if (p && ptr) memcpy(p, ptr, n);  /* over-copies, but boot region is ours */
        return p;
    }
    if (!real_realloc) return boot_alloc(n);
    if (should_fail(T_REALLOC)) { errno = g_errno; return NULL; }
    return real_realloc(ptr, n);
}

void free(void *ptr) {
    if (boot_owns(ptr)) return;           /* never free the bump buffer */
    if (!real_free) real_free = dlsym(RTLD_NEXT, "free");
    if (real_free) real_free(ptr);
}

void *mmap(void *addr, size_t len, int prot, int flags, int fd, off_t off) {
    if (!real_mmap) real_mmap = dlsym(RTLD_NEXT, "mmap");
    if (should_fail(T_MMAP)) { errno = g_errno; return MAP_FAILED; }
    return real_mmap(addr, len, prot, flags, fd, off);
}

int epoll_ctl(int epfd, int op, int fd, struct epoll_event *ev) {
    if (!real_epoll_ctl) real_epoll_ctl = dlsym(RTLD_NEXT, "epoll_ctl");
    if (should_fail(T_EPOLL_CTL)) { errno = g_errno; return -1; }
    return real_epoll_ctl(epfd, op, fd, ev);
}

int eventfd(unsigned int initval, int flags) {
    if (!real_eventfd) real_eventfd = dlsym(RTLD_NEXT, "eventfd");
    if (should_fail(T_EVENTFD)) { errno = g_errno; return -1; }
    return real_eventfd(initval, flags);
}

int timerfd_create(int clockid, int flags) {
    if (!real_timerfd_create) real_timerfd_create = dlsym(RTLD_NEXT, "timerfd_create");
    if (should_fail(T_TIMERFD)) { errno = g_errno; return -1; }
    return real_timerfd_create(clockid, flags);
}
