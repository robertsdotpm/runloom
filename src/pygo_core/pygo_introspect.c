/* pygo_introspect.c -- goroutine registry + developer-facing dump.
 * See pygo_introspect.h for the contract and the lifetime reasoning. */

#if !defined(_WIN32)
#  define _POSIX_C_SOURCE 200809L
#endif
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "pygo_introspect.h"
#include "pygo_sched.h"
#include "pygo_gstate.h"
#include "coro.h"
#include "netpoll.h"
#include "mn_sched.h"
#include "pygo_iframe.h"
#include "pygo_blockpool.h"
#include "pygo_diag.h"
#include "plat.h"
#include "plat_compat.h"

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
long long pygo_introspect_monotonic_ns(void)
{
    /* Cross-platform (QueryPerformanceCounter on Windows, CLOCK_MONOTONIC on
     * POSIX) -- the raw CLOCK_MONOTONIC path used to return 0 on MSVC, which
     * left park-age tracking dead on Windows. */
    return pygo_monotonic_ns();
}

/* ---------------------------------------------------------------- *
 *  Per-incarnation goroutine id (Go's goid)                        *
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
#define PYGO_GOID_BLOCK 1024
static long long pygo_goid_global = 1;   /* next id to hand out (1-based) */
static PYGO_TLS long long pygo_tls_goid_next = 0;
static PYGO_TLS long long pygo_tls_goid_end  = 0;

long long pygo_next_goid(void)
{
    if (pygo_tls_goid_next >= pygo_tls_goid_end) {
        long long base = __atomic_fetch_add(&pygo_goid_global,
                                            PYGO_GOID_BLOCK, __ATOMIC_RELAXED);
        pygo_tls_goid_next = base;
        pygo_tls_goid_end  = base + PYGO_GOID_BLOCK;
    }
    return pygo_tls_goid_next++;
}

/* ---------------------------------------------------------------- *
 *  State name tables                                               *
 * ---------------------------------------------------------------- */
const char *pygo_g_state_name(unsigned int s)
{
    switch ((pygo_g_state_t)s) {
    case PYGO_GST_INIT:           return "init";
    case PYGO_GST_SPAWNING:       return "spawning";
    case PYGO_GST_RUNNABLE:       return "runnable";
    case PYGO_GST_SUBMITTED:      return "submitted";
    case PYGO_GST_RUNNING:        return "running";
    case PYGO_GST_PARKED_NETPOLL: return "io-wait";
    case PYGO_GST_PARKED_CHAN:    return "chan-wait";
    case PYGO_GST_PARKED_SLEEP:   return "sleep";
    case PYGO_GST_PARKED_SAFE:    return "park";
    case PYGO_GST_WAKING:         return "waking";
    case PYGO_GST_DONE:           return "done";
    case PYGO_GST_FREED:          return "freed";
    default:                      return "?";
    }
}

const char *pygo_g_state_blockclass(unsigned int s)
{
    switch ((pygo_g_state_t)s) {
    case PYGO_GST_PARKED_NETPOLL: return "io";
    case PYGO_GST_PARKED_CHAN:    return "chan";
    case PYGO_GST_PARKED_SLEEP:   return "timer";
    case PYGO_GST_PARKED_SAFE:    return "sync";
    case PYGO_GST_RUNNING:        return "running";
    case PYGO_GST_RUNNABLE:
    case PYGO_GST_SUBMITTED:      return "runnable";
    case PYGO_GST_WAKING:         return "waking";
    default:                      return "other";
    }
}

/* Is `to` one of the parked states (the ones worth timestamping)? */
static int state_is_parked(unsigned int to)
{
    return to == PYGO_GST_PARKED_NETPOLL || to == PYGO_GST_PARKED_CHAN ||
           to == PYGO_GST_PARKED_SLEEP   || to == PYGO_GST_PARKED_SAFE;
}

/* ---------------------------------------------------------------- *
 *  Age timestamping (opt-in)                                       *
 * ---------------------------------------------------------------- */
static int pygo_introspect_ts_on = 0;

void pygo_introspect_set_timestamps(int on)
{
    __atomic_store_n(&pygo_introspect_ts_on, on ? 1 : 0, __ATOMIC_RELAXED);
}

int pygo_introspect_get_timestamps(void)
{
    return __atomic_load_n(&pygo_introspect_ts_on, __ATOMIC_RELAXED);
}

void pygo_introspect_note_transition(pygo_g_t *g, unsigned int to)
{
    if (g == NULL) return;
    if (!__atomic_load_n(&pygo_introspect_ts_on, __ATOMIC_RELAXED)) return;
    if (state_is_parked(to))
        __atomic_store_n(&g->state_since_ns,
                         pygo_introspect_monotonic_ns(), __ATOMIC_RELAXED);
}

/* ---------------------------------------------------------------- *
 *  Registry                                                        *
 * ---------------------------------------------------------------- */
static pygo_mutex_t pygo_greg_lock;
static int          pygo_greg_inited = 0;
static pygo_g_t    *pygo_greg_head   = NULL;   /* doubly-linked, no tail */
static long         pygo_greg_total  = 0;      /* live + cached structs */

void pygo_introspect_init(void)
{
    const char *env;
    if (pygo_greg_inited) return;
    pygo_mutex_init(&pygo_greg_lock);
    pygo_greg_inited = 1;
    env = getenv("PYGO_INTROSPECT_TIME");
    if (env != NULL && env[0] && env[0] != '0')
        pygo_introspect_set_timestamps(1);
    env = getenv("PYGO_DEADLOCK");   /* off | warn | raise (default warn) */
    if (env != NULL && env[0]) {
        if (strcmp(env, "off") == 0 || env[0] == '0')      pygo_set_deadlock_mode(0);
        else if (strcmp(env, "raise") == 0 || env[0] == '2') pygo_set_deadlock_mode(2);
        else                                                 pygo_set_deadlock_mode(1);
    }
    env = getenv("PYGO_MAX_GOROUTINES");
    if (env != NULL && env[0]) {
        long n = atol(env);
        if (n > 0) pygo_set_max_goroutines(n);
    }
}

void pygo_introspect_fini(void)
{
    /* The g structs themselves are owned by the slab / PyMem; we only
     * drop our list head so a fresh init starts clean.  Leaving the
     * structs linked would be a dangling-list bug after PyMem_Free, but
     * fini runs at interpreter teardown when nothing walks the list. */
    if (!pygo_greg_inited) return;
    pygo_mutex_lock(&pygo_greg_lock);
    pygo_greg_head = NULL;
    pygo_greg_total = 0;
    pygo_mutex_unlock(&pygo_greg_lock);
}

void pygo_greg_link(pygo_g_t *g)
{
    if (g == NULL || !pygo_greg_inited) return;
    pygo_mutex_lock(&pygo_greg_lock);
    g->reg_prev = NULL;
    g->reg_next = pygo_greg_head;
    if (pygo_greg_head != NULL) pygo_greg_head->reg_prev = g;
    pygo_greg_head = g;
    pygo_greg_total++;
    pygo_mutex_unlock(&pygo_greg_lock);
}

void pygo_greg_unlink(pygo_g_t *g)
{
    if (g == NULL || !pygo_greg_inited) return;
    pygo_mutex_lock(&pygo_greg_lock);
    /* Defensive: only unlink a g that is actually linked.  A g whose
     * reg_next/reg_prev are both NULL AND is not the head was never
     * linked (e.g. allocated before init); skip it. */
    if (pygo_greg_head == g || g->reg_prev != NULL || g->reg_next != NULL) {
        if (g->reg_prev != NULL) g->reg_prev->reg_next = g->reg_next;
        else if (pygo_greg_head == g) pygo_greg_head = g->reg_next;
        if (g->reg_next != NULL) g->reg_next->reg_prev = g->reg_prev;
        g->reg_prev = NULL;
        g->reg_next = NULL;
        pygo_greg_total--;
    }
    pygo_mutex_unlock(&pygo_greg_lock);
}

/* Reset the registry in a forked child: re-init the lock (a dead thread may
 * have held it at fork) and drop the inherited goroutine list -- the parent's
 * goroutines don't exist in the child.  Single-thread child only. */
void pygo_introspect_reset_after_fork(void)
{
    pygo_mutex_init(&pygo_greg_lock);
    pygo_greg_head = NULL;
    pygo_greg_total = 0;
    pygo_greg_inited = 1;
}

long pygo_goroutine_count(void)
{
    long n = 0;
    pygo_g_t *g;
    if (!pygo_greg_inited) return 0;
    pygo_mutex_lock(&pygo_greg_lock);
    for (g = pygo_greg_head; g != NULL; g = g->reg_next) {
        unsigned int st = __atomic_load_n(&g->state, __ATOMIC_ACQUIRE);
        if (st != PYGO_GST_FREED) n++;
    }
    pygo_mutex_unlock(&pygo_greg_lock);
    return n;
}

/* Count goroutines owned by `owner` parked on a channel or via park_safe --
 * the "blocked on each other" set.  At a quiescent drain exit (no ready /
 * sleep / netpoll / io / blockpool work left) these are unwakeable: a
 * deadlock.  owner==NULL counts every sched's such goroutines. */
long pygo_count_deadlockable_goroutines(const void *owner)
{
    long n = 0;
    pygo_g_t *g;
    if (!pygo_greg_inited) return 0;
    pygo_mutex_lock(&pygo_greg_lock);
    for (g = pygo_greg_head; g != NULL; g = g->reg_next) {
        unsigned int st = __atomic_load_n(&g->state, __ATOMIC_ACQUIRE);
        if (st != PYGO_GST_PARKED_CHAN && st != PYGO_GST_PARKED_SAFE) continue;
        if (owner != NULL && (const void *)g->owner != owner) continue;
        n++;
    }
    pygo_mutex_unlock(&pygo_greg_lock);
    return n;
}

/* ---- max-goroutines admission gate (backpressure) ----
 * 0 = unlimited (default).  When set, the spawn paths call pygo_goroutine_admit
 * before allocating; over the limit it returns 0 and the spawn raises.  The
 * live counter is maintained ONLY while a limit is active (admit increments,
 * the g's final decref releases via pygo_goroutine_release iff it was counted)
 * -- so an unset limit costs nothing on the hot path. */
static long pygo_max_g  = 0;
static long pygo_live_g = 0;   /* admitted-but-not-yet-released goroutines */

long pygo_get_max_goroutines(void)
{
    return __atomic_load_n(&pygo_max_g, __ATOMIC_RELAXED);
}

void pygo_set_max_goroutines(long n)
{
    if (n < 0) n = 0;
    __atomic_store_n(&pygo_max_g, n, __ATOMIC_RELAXED);
}

long pygo_live_goroutines(void)
{
    return __atomic_load_n(&pygo_live_g, __ATOMIC_RELAXED);
}

/* Try to admit one goroutine.  Returns 0 = rejected (over the limit; caller
 * raises), 1 = admitted but NOT counted (no limit active), 2 = admitted AND
 * counted (caller sets g->limit_counted so the final decref releases it). */
int pygo_goroutine_admit(void)
{
    long max = __atomic_load_n(&pygo_max_g, __ATOMIC_RELAXED);
    long now;
    if (max <= 0) return 1;
    now = __atomic_add_fetch(&pygo_live_g, 1, __ATOMIC_ACQ_REL);
    if (now > max) {
        __atomic_sub_fetch(&pygo_live_g, 1, __ATOMIC_ACQ_REL);   /* back out */
        return 0;
    }
    return 2;
}

void pygo_goroutine_release(void)
{
    __atomic_sub_fetch(&pygo_live_g, 1, __ATOMIC_ACQ_REL);
}

/* ---- deadlock-detection mode: 0=off, 1=warn (dump), 2=raise ---- */
static int pygo_deadlock_mode_v = 1;   /* default: warn */

int pygo_deadlock_mode(void)
{
    return __atomic_load_n(&pygo_deadlock_mode_v, __ATOMIC_RELAXED);
}

void pygo_set_deadlock_mode(int mode)
{
    if (mode < 0) mode = 0;
    if (mode > 2) mode = 2;
    __atomic_store_n(&pygo_deadlock_mode_v, mode, __ATOMIC_RELAXED);
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

void pygo_dump_goroutines_fd(int fd)
{
    char buf[256];
    int  m;
    pygo_g_t *g;
    long live = 0;
    /* histogram by state index */
    long counts[PYGO_GST__LAST];
    long long now;
    size_t i;

    if (!pygo_greg_inited) {
        emit(fd, "[pygo] goroutine dump: registry not initialised\n", 48);
        return;
    }
    for (i = 0; i < (size_t)PYGO_GST__LAST; i++) counts[i] = 0;
    now = pygo_introspect_monotonic_ns();

    if (pygo_mutex_trylock(&pygo_greg_lock) != 0) {
        /* Contended -- almost certainly a spawn/teardown holding the lock
         * for a few instructions.  Do NOT fall back to any blocking lock
         * (this runs from a SIGQUIT handler); just report and bail. */
        emit(fd, "[pygo] goroutine dump: registry busy, retry\n", 44);
        return;
    }

    for (g = pygo_greg_head; g != NULL; g = g->reg_next) {
        unsigned int st = __atomic_load_n(&g->state, __ATOMIC_ACQUIRE);
        if (st == PYGO_GST_FREED) continue;
        if (st < (unsigned)PYGO_GST__LAST) counts[st]++;
        live++;
    }

    m = snprintf(buf, sizeof buf,
        "\n=== pygo goroutine dump: %ld live (default stack %zu KiB) ===\n",
        live, pygo_sched_get_default_stack_size() / 1024u);
    if (m > 0) emit(fd, buf, (size_t)m);
    for (i = 0; i < (size_t)PYGO_GST__LAST; i++) {
        if (counts[i] == 0) continue;
        m = snprintf(buf, sizeof buf, "  %-10s %ld\n",
                     pygo_g_state_name((unsigned)i), counts[i]);
        if (m > 0) emit(fd, buf, (size_t)m);
    }

    for (g = pygo_greg_head; g != NULL; g = g->reg_next) {
        unsigned int st = __atomic_load_n(&g->state, __ATOMIC_ACQUIRE);
        long long id;
        int rc;
        long long since;
        char detail[96];
        if (st == PYGO_GST_FREED) continue;
        id    = __atomic_load_n(&g->id, __ATOMIC_RELAXED);
        rc    = __atomic_load_n(&g->refcount, __ATOMIC_RELAXED);
        since = __atomic_load_n(&g->state_since_ns, __ATOMIC_RELAXED);
        detail[0] = '\0';
        if (st == PYGO_GST_PARKED_NETPOLL) {
            int pfd = __atomic_load_n(&g->park_fd, __ATOMIC_RELAXED);
            int pev = __atomic_load_n(&g->park_events, __ATOMIC_RELAXED);
            snprintf(detail, sizeof detail, " fd=%d ev=%s%s", pfd,
                     (pev & 1) ? "R" : "", (pev & 2) ? "W" : "");
        } else if (st == PYGO_GST_PARKED_SLEEP) {
            double dt = g->wake_at - pygo_sched_monotonic_seconds();
            snprintf(detail, sizeof detail, " wake_in=%.3fs", dt);
        }
        if (since > 0 && now > 0 && state_is_parked(st)) {
            char age[40];
            snprintf(age, sizeof age, " age=%.1fs",
                     (double)(now - since) / 1e9);
            strncat(detail, age, sizeof detail - strlen(detail) - 1);
        }
        m = snprintf(buf, sizeof buf,
            "  g%-8llu %-10s rc=%d owner=%p%s\n",
            (unsigned long long)id, pygo_g_state_name(st), rc,
            (void *)g->owner, detail);
        if (m > 0) emit(fd, buf, (size_t)m);
    }
    emit(fd, "=== end goroutine dump ===\n", 27);
    pygo_mutex_unlock(&pygo_greg_lock);
}

/* ---------------------------------------------------------------- *
 *  Rich snapshot (POD copy under the lock)                         *
 * ---------------------------------------------------------------- */
pygo_g_info_t *pygo_goroutine_snapshot(long *count_out)
{
    pygo_g_info_t *arr;
    pygo_g_t *g;
    long cap, n = 0;
    long long now;
    if (count_out != NULL) *count_out = 0;
    if (!pygo_greg_inited) return NULL;

    now = pygo_introspect_monotonic_ns();
    pygo_mutex_lock(&pygo_greg_lock);
    cap = pygo_greg_total > 0 ? pygo_greg_total : 1;
    arr = (pygo_g_info_t *)malloc((size_t)cap * sizeof(*arr));
    if (arr == NULL) {
        pygo_mutex_unlock(&pygo_greg_lock);
        return NULL;
    }
    for (g = pygo_greg_head; g != NULL && n < cap; g = g->reg_next) {
        pygo_g_info_t *o;
        unsigned int st = __atomic_load_n(&g->state, __ATOMIC_ACQUIRE);
        long long since;
        if (st == PYGO_GST_FREED) continue;
        o = &arr[n++];
        o->id          = __atomic_load_n(&g->id, __ATOMIC_RELAXED);
        o->state       = st;
        o->park_fd     = (st == PYGO_GST_PARKED_NETPOLL)
                         ? __atomic_load_n(&g->park_fd, __ATOMIC_RELAXED) : -1;
        o->park_events = (st == PYGO_GST_PARKED_NETPOLL)
                         ? __atomic_load_n(&g->park_events, __ATOMIC_RELAXED) : 0;
        o->wake_at     = (st == PYGO_GST_PARKED_SLEEP) ? g->wake_at : 0.0;
        since          = __atomic_load_n(&g->state_since_ns, __ATOMIC_RELAXED);
        o->age_ns      = (since > 0 && now > 0 && state_is_parked(st))
                         ? (now - since) : -1;
        o->refcount    = __atomic_load_n(&g->refcount, __ATOMIC_RELAXED);
        o->noyield     = g->noyield;
        o->owner       = (const void *)g->owner;
    }
    pygo_mutex_unlock(&pygo_greg_lock);
    if (count_out != NULL) *count_out = n;
    return arr;
}

void pygo_goroutine_snapshot_free(pygo_g_info_t *arr, long count)
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
 *  drives the runtime hangs (pygo_mn_run waits on dead hubs) or     *
 *  deadlocks on a lock a dead thread held at fork.  This resets     *
 *  every subsystem to a clean single-process state so the child can *
 *  run pygo afresh (single-thread, or a fresh pygo_mn_init).  The   *
 *  child is single-threaded here, so the resets take no locks; they *
 *  only re-init the global locks (to clear any inherited-held       *
 *  state) and drop bookkeeping that named the parent's goroutines.  *
 *  Registered as an os.register_at_fork(after_in_child=...) handler *
 *  by pygo/__init__.py.                                            *
 * ---------------------------------------------------------------- */
void pygo_after_fork_child(void)
{
    pygo_introspect_reset_after_fork();   /* registry + greg lock */
    pygo_mn_reset_after_fork();           /* abandon dead hubs, unhang mn_run */
    pygo_netpoll_reset_after_fork();      /* own poll fd, drop stale parkers */
    pygo_blockpool_reset_after_fork();    /* dead offload workers -> re-create */
    pygo_diag_reset_after_fork();         /* diag ring lock */
}

/* ---------------------------------------------------------------- *
 *  Per-goroutine Python stack reconstruction (claim-protected)     *
 *                                                                  *
 *  Implemented in pygo_introspect_frames.c.inc to keep the         *
 *  CPython-internal frame-walk gated by #if PY_VERSION_HEX in one   *
 *  place.                                                          *
 * ---------------------------------------------------------------- */
#include "pygo_introspect_frames.c.inc"
