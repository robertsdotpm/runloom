/* runloom_introspect.c -- fiber registry + developer-facing dump.
 * See runloom_introspect.h for the contract and the lifetime reasoning. */

#if !defined(_WIN32)
#  define _POSIX_C_SOURCE 200809L
#endif
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "runloom_introspect.h"
#include "runloom_sched.h"
#include "runloom_gstate.h"
#include "coro.h"
#include "netpoll.h"
#include "mn_sched.h"
#include "runloom_iframe.h"
#include "runloom_blockpool.h"
#include "runloom_diag.h"
#include "runloom_crash.h"
#include "runloom_stackadvice.h"
#include "plat.h"
#include "plat_compat.h"
#include "runloom_lockrank.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#if defined(_WIN32)
#  include <io.h>
#else
#  include <unistd.h>
#endif

/* ---------------------------------------------------------------- *
 *  Monotonic clock                                                 *
 * ---------------------------------------------------------------- */
long long runloom_introspect_monotonic_ns(void)
{
    /* Cross-platform (QueryPerformanceCounter on Windows, CLOCK_MONOTONIC on
     * POSIX) -- the raw CLOCK_MONOTONIC path used to return 0 on MSVC, which
     * left park-age tracking dead on Windows. */
    return runloom_monotonic_ns();
}

/* ---------------------------------------------------------------- *
 *  Per-incarnation fiber id (Go's goid)                        *
 *                                                                  *
 *  Contention-free: each thread grabs a block of ids from the      *
 *  global counter and hands them out locally, so the shared        *
 *  cacheline is touched only once per GOID_BLOCK spawns.  Ids are   *
 *  compact + roughly monotonic, which is what a human wants.        *
 * ---------------------------------------------------------------- */
/* `long long` (not uint64_t): the MSVC _Generic atomic shim names long long
 * but not unsigned __int64 (== uint64_t there) -- and __atomic_fetch_add has
 * no unsigned-long-long slot at all.  Ids are always positive, so signed is
 * fine. */
#define RUNLOOM_GOID_BLOCK 1024
static long long runloom_fiberid_global = 1;   /* next id to hand out (1-based) */
static RUNLOOM_TLS long long runloom_tls_goid_next = 0;
static RUNLOOM_TLS long long runloom_tls_goid_end  = 0;

long long runloom_next_goid(void)
{
    if (runloom_tls_goid_next >= runloom_tls_goid_end) {
        long long base = __atomic_fetch_add(&runloom_fiberid_global,
                                            RUNLOOM_GOID_BLOCK, __ATOMIC_RELAXED);
        runloom_tls_goid_next = base;
        runloom_tls_goid_end  = base + RUNLOOM_GOID_BLOCK;
    }
    return runloom_tls_goid_next++;
}

/* Reserve a contiguous block of n goids in ONE atomic; returns the first.
 * The bulk-spawn loop then assigns base+0..base+n-1 inline (no per-g call). */
long long runloom_next_goid_block(long n)
{
    long long base = __atomic_fetch_add(&runloom_fiberid_global,
                                        (long long)n, __ATOMIC_RELAXED);
    return base;
}

/* ---------------------------------------------------------------- *
 *  State name tables                                               *
 * ---------------------------------------------------------------- */
const char *runloom_g_state_name(unsigned int s)
{
    switch ((runloom_g_state_t)s) {
    case RUNLOOM_GST_INIT:           return "init";
    case RUNLOOM_GST_SPAWNING:       return "spawning";
    case RUNLOOM_GST_RUNNABLE:       return "runnable";
    case RUNLOOM_GST_SUBMITTED:      return "submitted";
    case RUNLOOM_GST_RUNNING:        return "running";
    case RUNLOOM_GST_PARKED_NETPOLL: return "io-wait";
    case RUNLOOM_GST_PARKED_CHAN:    return "chan-wait";
    case RUNLOOM_GST_PARKED_SLEEP:   return "sleep";
    case RUNLOOM_GST_PARKED_SAFE:    return "park";
    case RUNLOOM_GST_WAKING:         return "waking";
    case RUNLOOM_GST_DONE:           return "done";
    case RUNLOOM_GST_FREED:          return "freed";
    default:                      return "?";
    }
}

const char *runloom_wait_reason_name(unsigned char r)
{
    switch ((runloom_wait_reason_t)r) {
    case RUNLOOM_WR_SYNC:      return "sync";
    case RUNLOOM_WR_FUTURE:    return "future";
    case RUNLOOM_WR_WAITGROUP: return "waitgroup";
    case RUNLOOM_WR_LOCK:      return "lock";
    case RUNLOOM_WR_EVENT:     return "event";
    case RUNLOOM_WR_CONDITION: return "condition";
    case RUNLOOM_WR_BARRIER:   return "barrier";
    case RUNLOOM_WR_SELECT:    return "select";
    case RUNLOOM_WR_EXECUTOR:  return "executor";
    case RUNLOOM_WR_SEMAPHORE: return "semaphore";
    case RUNLOOM_WR_QUEUE:     return "queue";
    default:                   return NULL;   /* WR_NONE -> no suffix */
    }
}

const char *runloom_g_state_blockclass(unsigned int s)
{
    switch ((runloom_g_state_t)s) {
    case RUNLOOM_GST_PARKED_NETPOLL: return "io";
    case RUNLOOM_GST_PARKED_CHAN:    return "chan";
    case RUNLOOM_GST_PARKED_SLEEP:   return "timer";
    case RUNLOOM_GST_PARKED_SAFE:    return "sync";
    case RUNLOOM_GST_RUNNING:        return "running";
    case RUNLOOM_GST_RUNNABLE:
    case RUNLOOM_GST_SUBMITTED:      return "runnable";
    case RUNLOOM_GST_WAKING:         return "waking";
    default:                      return "other";
    }
}

/* Is `to` one of the parked states (the ones worth timestamping)? */
static int state_is_parked(unsigned int to)
{
    return to == RUNLOOM_GST_PARKED_NETPOLL || to == RUNLOOM_GST_PARKED_CHAN ||
           to == RUNLOOM_GST_PARKED_SLEEP   || to == RUNLOOM_GST_PARKED_SAFE;
}

/* ---------------------------------------------------------------- *
 *  Age timestamping (opt-in)                                       *
 * ---------------------------------------------------------------- */
static int runloom_introspect_ts_on = 0;

void runloom_introspect_set_timestamps(int on)
{
    __atomic_store_n(&runloom_introspect_ts_on, on ? 1 : 0, __ATOMIC_RELAXED);
}

int runloom_introspect_get_timestamps(void)
{
    return __atomic_load_n(&runloom_introspect_ts_on, __ATOMIC_RELAXED);
}

void runloom_introspect_note_transition(runloom_g_t *g, unsigned int to)
{
    if (g == NULL) return;
    if (!__atomic_load_n(&runloom_introspect_ts_on, __ATOMIC_RELAXED)) return;
    if (state_is_parked(to))
        __atomic_store_n(&g->state_since_ns,
                         runloom_introspect_monotonic_ns(), __ATOMIC_RELAXED);
}

/* ---------------------------------------------------------------- *
 *  Registry                                                        *
 * ---------------------------------------------------------------- */
static runloom_mutex_t runloom_greg_lock;
static int          runloom_greg_inited = 0;
static runloom_g_t    *runloom_greg_head   = NULL;   /* doubly-linked, no tail */
static long         runloom_greg_total  = 0;      /* live + cached structs */

void runloom_introspect_init(void)
{
    const char *env;
    if (runloom_greg_inited) return;
    runloom_mutex_init(&runloom_greg_lock);
    runloom_greg_inited = 1;
    env = getenv("RUNLOOM_INTROSPECT_TIME");
    if (env != NULL && env[0] && env[0] != '0')
        runloom_introspect_set_timestamps(1);
    env = getenv("RUNLOOM_DEADLOCK");   /* off | warn | raise (default warn) */
    if (env != NULL && env[0]) {
        if (strcmp(env, "off") == 0 || env[0] == '0')      runloom_set_deadlock_mode(0);
        else if (strcmp(env, "raise") == 0 || env[0] == '2') runloom_set_deadlock_mode(2);
        else                                                 runloom_set_deadlock_mode(1);
    }
    env = getenv("RUNLOOM_MAX_GOROUTINES");
    if (env != NULL && env[0]) {
        long n = atol(env);
        if (n > 0) runloom_set_max_fibers(n);
    }
}

void runloom_introspect_fini(void)
{
    /* The g structs themselves are owned by the slab / PyMem; we only
     * drop our list head so a fresh init starts clean.  Leaving the
     * structs linked would be a dangling-list bug after PyMem_Free, but
     * fini runs at interpreter teardown when nothing walks the list. */
    if (!runloom_greg_inited) return;
    RUNLOOM_RLOCK(&runloom_greg_lock, RUNLOOM_RANK_GREG);
    runloom_greg_head = NULL;
    __atomic_store_n(&runloom_greg_total, 0L, __ATOMIC_RELAXED);
    RUNLOOM_RUNLOCK(&runloom_greg_lock, RUNLOOM_RANK_GREG);
}

/* TEMP ablation gate (RUNLOOM_GREG_OFF=1): skip the global registry to measure
 * its lock-contention cost on spawn-heavy workloads.  Remove after diagnosis. */
static int runloom_greg_off(void)
{
    static int v = -1;
    int cur = __atomic_load_n(&v, __ATOMIC_RELAXED);
    if (cur < 0) {
        const char *e = getenv("RUNLOOM_GREG_OFF");
        cur = (e != NULL && *e != '0' && *e != '\0') ? 1 : 0;
        __atomic_store_n(&v, cur, __ATOMIC_RELAXED);
    }
    return cur;
}

void runloom_greg_link(runloom_g_t *g)
{
    if (g == NULL || !runloom_greg_inited) return;
    if (runloom_greg_off()) return;
    RUNLOOM_RLOCK(&runloom_greg_lock, RUNLOOM_RANK_GREG);
    g->reg_prev = NULL;
    g->reg_next = runloom_greg_head;
    if (runloom_greg_head != NULL) runloom_greg_head->reg_prev = g;
    runloom_greg_head = g;
    /* Atomic add (still under the lock -- zero cost on this cold slab-alloc
     * path) so runloom_greg_total_count() can read it LOCK-FREE from m_stats,
     * which must never take runloom_greg_lock (the spawn path holds it). */
    __atomic_add_fetch(&runloom_greg_total, 1L, __ATOMIC_RELAXED);
    RUNLOOM_RUNLOCK(&runloom_greg_lock, RUNLOOM_RANK_GREG);
}

void runloom_greg_unlink(runloom_g_t *g)
{
    if (g == NULL || !runloom_greg_inited) return;
    if (runloom_greg_off()) return;
    RUNLOOM_RLOCK(&runloom_greg_lock, RUNLOOM_RANK_GREG);
    /* Defensive: only unlink a g that is actually linked.  A g whose
     * reg_next/reg_prev are both NULL AND is not the head was never
     * linked (e.g. allocated before init); skip it. */
    if (runloom_greg_head == g || g->reg_prev != NULL || g->reg_next != NULL) {
        if (g->reg_prev != NULL) g->reg_prev->reg_next = g->reg_next;
        else if (runloom_greg_head == g) runloom_greg_head = g->reg_next;
        if (g->reg_next != NULL) g->reg_next->reg_prev = g->reg_prev;
        g->reg_prev = NULL;
        g->reg_next = NULL;
        __atomic_sub_fetch(&runloom_greg_total, 1L, __ATOMIC_RELAXED);
    }
    RUNLOOM_RUNLOCK(&runloom_greg_lock, RUNLOOM_RANK_GREG);
}

/* Reset the registry in a forked child: re-init the lock (a dead thread may
 * have held it at fork) and drop the inherited fiber list -- the parent's
 * fibers don't exist in the child.  Single-thread child only. */
void runloom_introspect_reset_after_fork(void)
{
    runloom_mutex_init(&runloom_greg_lock);
    runloom_greg_head = NULL;
    __atomic_store_n(&runloom_greg_total, 0L, __ATOMIC_RELAXED);
    runloom_greg_inited = 1;
}

/* Lock-free gauge (R0): live + retained runloom_g structs the process has
 * taken from the OS and not yet freed.  Per the "a freed g never returns to
 * the OS" invariant this only falls at mn_fini reclaim, so within a run it is
 * a high-water of peak concurrency; a value that CLIMBS across soak iterations
 * is leaked g structs.  Reads 0 when the registry is disabled
 * (RUNLOOM_GREG_OFF).  Safe from m_stats: a bare relaxed atomic load, no
 * runloom_greg_lock (which the spawn cold-path holds). */
long runloom_greg_total_count(void)
{
    return __atomic_load_n(&runloom_greg_total, __ATOMIC_RELAXED);
}

long runloom_fiber_count(void)
{
    long n = 0;
    runloom_g_t *g;
    if (!runloom_greg_inited) return 0;
    RUNLOOM_RLOCK(&runloom_greg_lock, RUNLOOM_RANK_GREG);
    for (g = runloom_greg_head; g != NULL; g = g->reg_next) {
        unsigned int st = __atomic_load_n(&g->state, __ATOMIC_ACQUIRE);
        if (st != RUNLOOM_GST_FREED) n++;
    }
    RUNLOOM_RUNLOCK(&runloom_greg_lock, RUNLOOM_RANK_GREG);
    return n;
}

/* Count fibers owned by `owner` parked on a channel or via park_safe --
 * the "blocked on each other" set.  At a quiescent drain exit (no ready /
 * sleep / netpoll / io / blockpool work left) these are unwakeable: a
 * deadlock.  owner==NULL counts every sched's such fibers. */
long runloom_count_deadlockable_fibers(const void *owner)
{
    long n = 0;
    runloom_g_t *g;
    if (!runloom_greg_inited) return 0;
    RUNLOOM_RLOCK(&runloom_greg_lock, RUNLOOM_RANK_GREG);
    for (g = runloom_greg_head; g != NULL; g = g->reg_next) {
        unsigned int st = __atomic_load_n(&g->state, __ATOMIC_ACQUIRE);
        if (st != RUNLOOM_GST_PARKED_CHAN && st != RUNLOOM_GST_PARKED_SAFE) continue;
        if (owner != NULL && (const void *)g->owner != owner) continue;
        n++;
    }
    RUNLOOM_RUNLOCK(&runloom_greg_lock, RUNLOOM_RANK_GREG);
    return n;
}

/* ---- max-fibers admission gate (backpressure) ----
 * 0 = unlimited (default).  When set, the spawn paths call runloom_fiber_admit
 * before allocating; over the limit it returns 0 and the spawn raises.  The
 * live counter is maintained ONLY while a limit is active (admit increments,
 * the g's final decref releases via runloom_fiber_release iff it was counted)
 * -- so an unset limit costs nothing on the hot path. */
static long runloom_max_g  = 0;
static long runloom_live_g = 0;   /* admitted-but-not-yet-released fibers */

long runloom_get_max_fibers(void)
{
    return __atomic_load_n(&runloom_max_g, __ATOMIC_RELAXED);
}

void runloom_set_max_fibers(long n)
{
    if (n < 0) n = 0;
    __atomic_store_n(&runloom_max_g, n, __ATOMIC_RELAXED);
}

long runloom_live_fibers(void)
{
    return __atomic_load_n(&runloom_live_g, __ATOMIC_RELAXED);
}

/* Try to admit one fiber.  Returns 0 = rejected (over the limit; caller
 * raises), 1 = admitted but NOT counted (no limit active), 2 = admitted AND
 * counted (caller sets g->limit_counted so the final decref releases it). */
int runloom_fiber_admit(void)
{
    long max = __atomic_load_n(&runloom_max_g, __ATOMIC_RELAXED);
    long now;
    if (max <= 0) return 1;
    now = __atomic_add_fetch(&runloom_live_g, 1, __ATOMIC_ACQ_REL);
    if (now > max) {
        __atomic_sub_fetch(&runloom_live_g, 1, __ATOMIC_ACQ_REL);   /* back out */
        return 0;
    }
    return 2;
}

void runloom_fiber_release(void)
{
    __atomic_sub_fetch(&runloom_live_g, 1, __ATOMIC_ACQ_REL);
}

/* ---- deadlock-detection mode: 0=off, 1=warn (dump), 2=raise ---- */
static int runloom_deadlock_mode_v = 1;   /* default: warn */

int runloom_deadlock_mode(void)
{
    return __atomic_load_n(&runloom_deadlock_mode_v, __ATOMIC_RELAXED);
}

void runloom_set_deadlock_mode(int mode)
{
    if (mode < 0) mode = 0;
    if (mode > 2) mode = 2;
    __atomic_store_n(&runloom_deadlock_mode_v, mode, __ATOMIC_RELAXED);
}

/* ---------------------------------------------------------------- *
 *  Async-signal-safe-ish structural dump                           *
 *                                                                  *
 *  Reads ONLY plain data off each g (never a parker/coro/callable   *
 *  pointer), writes with snprintf + write only, and try-locks the   *
 *  registry so a SIGQUIT handler can't deadlock on it.              *
 * ---------------------------------------------------------------- */
static void emit(int fd, const char *buf, size_t len)
{
    if (fd < 0) { (void)fwrite(buf, 1, len, stderr); return; }
#if defined(_WIN32)
    (void)_write(fd, buf, (unsigned)len);
#else
    {
        ssize_t off = 0;
        while ((size_t)off < len) {
            ssize_t w = write(fd, buf + off, len - off);
            if (w <= 0) break;
            off += w;
        }
    }
#endif
}

void runloom_dump_fibers_fd(int fd)
{
    char buf[256];
    int  m;
    runloom_g_t *g;
    long live = 0;
    /* histogram by state index */
    long counts[RUNLOOM_GST__LAST];
    long long now;
    size_t i;

    if (!runloom_greg_inited) {
        emit(fd, "[runloom] fiber dump: registry not initialised\n", 48);
        return;
    }
    for (i = 0; i < (size_t)RUNLOOM_GST__LAST; i++) counts[i] = 0;
    now = runloom_introspect_monotonic_ns();

    if (runloom_mutex_trylock(&runloom_greg_lock) != 0) {
        /* Contended -- almost certainly a spawn/teardown holding the lock
         * for a few instructions.  Do NOT fall back to any blocking lock
         * (this runs from a SIGQUIT handler); just report and bail. */
        emit(fd, "[runloom] fiber dump: registry busy, retry\n", 44);
        return;
    }

    for (g = runloom_greg_head; g != NULL; g = g->reg_next) {
        unsigned int st = __atomic_load_n(&g->state, __ATOMIC_ACQUIRE);
        if (st == RUNLOOM_GST_FREED) continue;
        if (st < (unsigned)RUNLOOM_GST__LAST) counts[st]++;
        live++;
    }

    m = snprintf(buf, sizeof buf,
        "\n=== runloom fiber dump: %ld live (default stack %zu KiB) ===\n",
        live, runloom_sched_get_default_stack_size() / 1024u);
    if (m > 0) emit(fd, buf, (size_t)m);
    for (i = 0; i < (size_t)RUNLOOM_GST__LAST; i++) {
        if (counts[i] == 0) continue;
        m = snprintf(buf, sizeof buf, "  %-10s %ld\n",
                     runloom_g_state_name((unsigned)i), counts[i]);
        if (m > 0) emit(fd, buf, (size_t)m);
    }

    for (g = runloom_greg_head; g != NULL; g = g->reg_next) {
        unsigned int st = __atomic_load_n(&g->state, __ATOMIC_ACQUIRE);
        long long id;
        int rc;
        long long since;
        char detail[96];
        if (st == RUNLOOM_GST_FREED) continue;
        id    = __atomic_load_n(&g->id, __ATOMIC_RELAXED);
        rc    = __atomic_load_n(&g->refcount, __ATOMIC_RELAXED);
        since = __atomic_load_n(&g->state_since_ns, __ATOMIC_RELAXED);
        detail[0] = '\0';
        if (st == RUNLOOM_GST_PARKED_NETPOLL) {
            int pfd = __atomic_load_n(&g->park_fd, __ATOMIC_RELAXED);
            int pev = __atomic_load_n(&g->park_events, __ATOMIC_RELAXED);
            snprintf(detail, sizeof detail, " fd=%d ev=%s%s", pfd,
                     (pev & 1) ? "R" : "", (pev & 2) ? "W" : "");
        } else if (st == RUNLOOM_GST_PARKED_SLEEP) {
            double dt = g->wake_at - runloom_sched_monotonic_seconds();
            snprintf(detail, sizeof detail, " wake_in=%.3fs", dt);
        }
        if (since > 0 && now > 0 && state_is_parked(st)) {
            char age[40];
            snprintf(age, sizeof age, " age=%.1fs",
                     (double)(now - since) / 1e9);
            strncat(detail, age, sizeof detail - strlen(detail) - 1);
        }
        {
            /* For PARKED_SAFE, subdivide the opaque "park" with the fiber's
             * wait reason (future / waitgroup / lock / ...) so the dump says
             * WHY it is blocked. */
            const char *sname = runloom_g_state_name(st);
            const char *wr = (st == RUNLOOM_GST_PARKED_SAFE)
                             ? runloom_wait_reason_name(g->wait_reason) : NULL;
            char stlabel[28];
            if (wr != NULL) {
                snprintf(stlabel, sizeof stlabel, "%s:%s", sname, wr);
                sname = stlabel;
            }
            m = snprintf(buf, sizeof buf,
                "  g%-8llu %-12s rc=%d owner=%p%s\n",
                (unsigned long long)id, sname, rc, (void *)g->owner, detail);
        }
        if (m > 0) emit(fd, buf, (size_t)m);
    }
    emit(fd, "=== end fiber dump ===\n", 27);
    RUNLOOM_RUNLOCK(&runloom_greg_lock, RUNLOOM_RANK_GREG);
}

/* ---------------------------------------------------------------- *
 *  Crash-handler helpers                                           *
 *                                                                  *
 *  runloom_fiber_for_addr maps a faulting address onto a live    *
 *  fiber's stack region.  Unlike the dump it DOES read g->coro   *
 *  (to recover the stack base/size) -- acceptable only because the   *
 *  caller is the crash path, where a nested fault is caught by the   *
 *  handler's in-progress latch and the process is dying anyway.      *
 * ---------------------------------------------------------------- */
long long runloom_g_id(const runloom_g_t *g)
{
    if (g == NULL) return -1;
    return (long long)__atomic_load_n(&g->id, __ATOMIC_RELAXED);
}

long long runloom_fiber_for_addr(const void *addr, int *kind,
                                     unsigned *stack_kib)
{
    runloom_g_t *g;
    size_t guard = runloom_coro_guard_size();
    if (kind != NULL) *kind = 0;
    if (stack_kib != NULL) *stack_kib = 0;
    if (!runloom_greg_inited || addr == NULL) return 0;
    if (runloom_mutex_trylock(&runloom_greg_lock) != 0) return 0;

    for (g = runloom_greg_head; g != NULL; g = g->reg_next) {
        unsigned int st = __atomic_load_n(&g->state, __ATOMIC_ACQUIRE);
        runloom_coro_t *c;
        const char *base;
        size_t size;
        if (st == RUNLOOM_GST_FREED) continue;
        c = g->coro;
        if (c == NULL) continue;
        base = (const char *)runloom_coro_stack_base(c);
        size = runloom_coro_stack_size(c);
        if (base == NULL || size == 0) continue;
        if ((const char *)addr >= base && (const char *)addr < base + size) {
            long long id = __atomic_load_n(&g->id, __ATOMIC_RELAXED);
            if (kind != NULL) *kind = 2;
            if (stack_kib != NULL) *stack_kib = (unsigned)(size / 1024u);
            RUNLOOM_RUNLOCK(&runloom_greg_lock, RUNLOOM_RANK_GREG);
            return id;
        }
        if (guard > 0 &&
            (const char *)addr >= base - guard && (const char *)addr < base) {
            long long id = __atomic_load_n(&g->id, __ATOMIC_RELAXED);
            if (kind != NULL) *kind = 1;
            if (stack_kib != NULL) *stack_kib = (unsigned)(size / 1024u);
            RUNLOOM_RUNLOCK(&runloom_greg_lock, RUNLOOM_RANK_GREG);
            return id;
        }
    }
    RUNLOOM_RUNLOCK(&runloom_greg_lock, RUNLOOM_RANK_GREG);
    return 0;
}

/* ---------------------------------------------------------------- *
 *  Rich snapshot (POD copy under the lock)                         *
 * ---------------------------------------------------------------- */
runloom_g_info_t *runloom_fiber_snapshot(long *count_out)
{
    runloom_g_info_t *arr;
    runloom_g_t *g;
    long cap, n = 0;
    long long now;
    if (count_out != NULL) *count_out = 0;
    if (!runloom_greg_inited) return NULL;

    now = runloom_introspect_monotonic_ns();
    RUNLOOM_RLOCK(&runloom_greg_lock, RUNLOOM_RANK_GREG);
    cap = runloom_greg_total > 0 ? runloom_greg_total : 1;
    arr = (runloom_g_info_t *)malloc((size_t)cap * sizeof(*arr));
    if (arr == NULL) {
        RUNLOOM_RUNLOCK(&runloom_greg_lock, RUNLOOM_RANK_GREG);
        return NULL;
    }
    for (g = runloom_greg_head; g != NULL && n < cap; g = g->reg_next) {
        runloom_g_info_t *o;
        unsigned int st = __atomic_load_n(&g->state, __ATOMIC_ACQUIRE);
        long long since;
        /* Skip pre-PUBLISH states too, not just FREED.  spawn_common writes
         * g->refcount/owner/noyield (sched_core.c.inc:966-968) and only THEN
         * publishes with state_set(RUNNABLE) (:989, __ATOMIC_RELEASE).  This
         * ACQUIRE-load pairs with that RELEASE, so gating on st >= RUNNABLE
         * guarantees those field writes are complete-and-visible before we read
         * o->refcount/noyield/owner below.  Reading a g still in INIT/SPAWNING
         * (registry-linked at first alloc, but fields not yet published) is a
         * real data race a TSan-gold run caught on the fiber_snapshot reader. */
        if (st < RUNLOOM_GST_RUNNABLE || st == RUNLOOM_GST_FREED) continue;
        o = &arr[n++];
        o->id          = __atomic_load_n(&g->id, __ATOMIC_RELAXED);
        o->state       = st;
        o->park_fd     = (st == RUNLOOM_GST_PARKED_NETPOLL)
                         ? __atomic_load_n(&g->park_fd, __ATOMIC_RELAXED) : -1;
        o->park_events = (st == RUNLOOM_GST_PARKED_NETPOLL)
                         ? __atomic_load_n(&g->park_events, __ATOMIC_RELAXED) : 0;
        o->wake_at     = (st == RUNLOOM_GST_PARKED_SLEEP) ? g->wake_at : 0.0;
        since          = __atomic_load_n(&g->state_since_ns, __ATOMIC_RELAXED);
        /* Gate on the timestamps flag: note_transition only stamps
         * state_since_ns while tracking is ON, so a g recycled from the slab
         * while tracking is OFF carries a STALE state_since_ns from a prior
         * incarnation -- reporting it as an age would be a nonsensical
         * cross-incarnation value.  When tracking is on, every park re-stamps,
         * so `since` is always current. */
        o->age_ns      = (runloom_introspect_get_timestamps()
                          && since > 0 && now > 0 && state_is_parked(st))
                         ? (now - since) : -1;
        o->refcount    = __atomic_load_n(&g->refcount, __ATOMIC_RELAXED);
        o->noyield     = g->noyield;
        o->owner       = (const void *)g->owner;
    }
    RUNLOOM_RUNLOCK(&runloom_greg_lock, RUNLOOM_RANK_GREG);
    if (count_out != NULL) *count_out = n;
    return arr;
}

void runloom_fiber_snapshot_free(runloom_g_info_t *arr, long count)
{
    (void)count;
    free(arr);
}

/* ---------------------------------------------------------------- *
 *  Fork safety                                                     *
 *                                                                  *
 *  After os.fork() the child has only the forking thread; every    *
 *  other OS thread (M:N hubs, blocking-offload workers) is gone,   *
 *  but the inherited state still references them -- so a child that *
 *  drives the runtime hangs (runloom_mn_run waits on dead hubs) or     *
 *  deadlocks on a lock a dead thread held at fork.  This resets     *
 *  every subsystem to a clean single-process state so the child can *
 *  run runloom afresh (single-thread, or a fresh runloom_mn_init).  The   *
 *  child is single-threaded here, so the resets take no locks; they *
 *  only re-init the global locks (to clear any inherited-held       *
 *  state) and drop bookkeeping that named the parent's fibers.  *
 *  Registered as an os.register_at_fork(after_in_child=...) handler *
 *  by runloom/__init__.py.                                            *
 * ---------------------------------------------------------------- */
void runloom_after_fork_child(void)
{
    runloom_introspect_reset_after_fork();   /* registry + greg lock */
    runloom_mn_reset_after_fork();           /* abandon dead hubs, unhang mn_run */
    runloom_netpoll_reset_after_fork();      /* own poll fd, drop stale parkers */
    runloom_blockpool_reset_after_fork();    /* dead offload workers -> re-create */
    runloom_diag_reset_after_fork();         /* diag ring lock */
    runloom_crash_reset_after_fork();        /* clear crash latch (keep altstack) */
    runloom_advice_reset_after_fork();       /* stack-advice table lock */
    runloom_cal_reset_after_fork();          /* default-stack calibration lock */
    runloom_g_global_reset_after_fork();     /* cross-hub g-slab balance lock + pool */
    runloom_coro_reset_after_fork();         /* cross-hub coro balance lock + pool */
}

/* ---------------------------------------------------------------- *
 *  Per-fiber Python stack reconstruction (claim-protected)     *
 *                                                                  *
 *  Implemented in runloom_introspect_frames.c.inc to keep the         *
 *  CPython-internal frame-walk gated by #if PY_VERSION_HEX in one   *
 *  place.                                                          *
 * ---------------------------------------------------------------- */
#include "runloom_introspect_frames.c.inc"
