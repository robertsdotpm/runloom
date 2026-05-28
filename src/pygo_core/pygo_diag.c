/* pygo_diag.c -- diagnostic infrastructure: env-driven flags,
 * lock-free per-thread lifecycle event rings, self_check invariant
 * pass.  See pygo_diag.h for the contract. */

#if !defined(_WIN32)
#  define _POSIX_C_SOURCE 200809L
#endif

#include "pygo_diag.h"
#include "plat.h"
#include "plat_compat.h"
#include "plat_atomic.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#if defined(_WIN32)
#  include <io.h>
#else
#  include <unistd.h>
#endif

/* ---------------------------------------------------------------- *
 *  Flag parsing                                                    *
 * ---------------------------------------------------------------- */

unsigned int pygo_debug_flags = 0;

static unsigned int parse_one_token(const char *t, size_t n)
{
    if (n == 0) return 0;
    if (n == 4 && memcmp(t, "none", 4) == 0)    return 0;
    if (n == 3 && memcmp(t, "all",  3) == 0)    return PYGO_DBG_ALL;
    if (n == 6 && memcmp(t, "parker", 6) == 0)  return PYGO_DBG_PARKER;
    if (n == 6 && memcmp(t, "gstate", 6) == 0)  return PYGO_DBG_GSTATE;
    if (n == 10 && memcmp(t, "invariants", 10) == 0) return PYGO_DBG_INVARIANTS;
    if (n == 4 && memcmp(t, "ring", 4) == 0)    return PYGO_DBG_RING;
    /* "1" -- legacy: tag for "build-style debug" only, no diag flags. */
    /* Unknown token: keep silent.  setup.py also uses PYGO_DEBUG=1
     * which we ignore here. */
    return 0;
}

static void parse_pygo_debug_env(void)
{
    const char *env = getenv("PYGO_DEBUG_DIAG");
    if (env == NULL || *env == '\0') {
        /* Fall back to PYGO_DEBUG, but ignore the legacy "=1" form
         * (that's the build-style debug flag, not a diag selector). */
        env = getenv("PYGO_DEBUG");
        if (env == NULL || *env == '\0') return;
        /* Skip if it looks like the build-style "1"/"0"/"true"/etc.
         * value -- we don't want PYGO_DEBUG=1 to enable runtime
         * checks unintentionally. */
        if ((env[0] == '0' || env[0] == '1') && env[1] == '\0') return;
        if (strcmp(env, "true") == 0 || strcmp(env, "false") == 0) return;
        if (strcmp(env, "yes")  == 0 || strcmp(env, "no")    == 0) return;
    }
    {
        const char *p = env;
        unsigned int flags = 0;
        while (*p != '\0') {
            const char *start;
            size_t n;
            while (*p == ',' || *p == ' ' || *p == '\t') p++;
            start = p;
            while (*p != '\0' && *p != ',' && *p != ' ' && *p != '\t') p++;
            n = (size_t)(p - start);
            /* lowercase in place would mutate the env; do a stack
             * copy for short tokens. */
            if (n > 0 && n < 32) {
                char buf[32];
                size_t i;
                for (i = 0; i < n; i++) buf[i] = (char)tolower((unsigned char)start[i]);
                flags |= parse_one_token(buf, n);
            }
        }
        pygo_debug_flags = flags;
    }
}


/* ---------------------------------------------------------------- *
 *  Lifecycle event ring                                            *
 * ---------------------------------------------------------------- */

/* Power-of-two size; head is monotonic, slot = head & mask.  Each
 * record is 32 bytes so the ring fits in cache lines cleanly. */
#define PYGO_RING_LOG2 10        /* 1024 entries = 32 KiB per thread */
#define PYGO_RING_CAP  (1u << PYGO_RING_LOG2)
#define PYGO_RING_MASK (PYGO_RING_CAP - 1u)

typedef struct pygo_evt {
    unsigned int   op;            /* pygo_evt_op_t */
    unsigned int   tid;           /* lightweight: owning ring's seq id */
    const void    *p1;
    const void    *p2;
    long long      aux;
    long long      ts_ns;
} pygo_evt_t;

typedef struct pygo_ring {
    pygo_evt_t      slots[PYGO_RING_CAP];
    unsigned long   head;         /* monotonic counter; only owner writes */
    unsigned int    tid;          /* registry seq */
    struct pygo_ring *next;       /* registry list */
} pygo_ring_t;

/* TLS: each thread's own ring.  Lazily allocated. */
static PYGO_TLS pygo_ring_t *pygo_tls_ring = NULL;

/* Registry list head + lock.  Used only during ring creation and
 * during pygo_diag_dump; emission never touches the registry. */
static pygo_ring_t  *pygo_ring_list = NULL;
static pygo_mutex_t  pygo_ring_list_lock;
static int           pygo_ring_list_lock_inited = 0;
static unsigned int  pygo_ring_next_tid = 0;

static long long monotonic_ns(void)
{
#if defined(CLOCK_MONOTONIC)
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) == 0)
        return (long long)ts.tv_sec * 1000000000LL + (long long)ts.tv_nsec;
#endif
    return 0;
}

static pygo_ring_t *ring_acquire(void)
{
    pygo_ring_t *r = pygo_tls_ring;
    if (r != NULL) return r;
    r = (pygo_ring_t *)calloc(1, sizeof(*r));
    if (r == NULL) return NULL;
    pygo_mutex_lock(&pygo_ring_list_lock);
    r->tid  = ++pygo_ring_next_tid;
    r->next = pygo_ring_list;
    pygo_ring_list = r;
    pygo_mutex_unlock(&pygo_ring_list_lock);
    pygo_tls_ring = r;
    return r;
}

void pygo_evt_log_(pygo_evt_op_t op,
                   const void *p1, const void *p2, long long aux)
{
    pygo_ring_t *r;
    pygo_evt_t  *s;
    unsigned long idx;
    if (!pygo_ring_list_lock_inited) return;     /* before init */
    r = ring_acquire();
    if (r == NULL) return;
    idx = r->head++;
    s = &r->slots[idx & PYGO_RING_MASK];
    s->op    = (unsigned int)op;
    s->tid   = r->tid;
    s->p1    = p1;
    s->p2    = p2;
    s->aux   = aux;
    s->ts_ns = monotonic_ns();
}

int pygo_diag_registered_thread_count(void)
{
    int n = 0;
    pygo_ring_t *r;
    if (!pygo_ring_list_lock_inited) return 0;
    pygo_mutex_lock(&pygo_ring_list_lock);
    for (r = pygo_ring_list; r != NULL; r = r->next) n++;
    pygo_mutex_unlock(&pygo_ring_list_lock);
    return n;
}

static const char *op_name(unsigned int op)
{
    switch ((pygo_evt_op_t)op) {
    case PYGO_EVT_PARKER_LINK:    return "PARK_LINK";
    case PYGO_EVT_PARKER_UNLINK:  return "PARK_UNLINK";
    case PYGO_EVT_PARKER_WAKE:    return "PARK_WAKE";
    case PYGO_EVT_PARKER_TIMEOUT: return "PARK_TIMEOUT";
    case PYGO_EVT_PARKER_GHOST:   return "PARK_GHOST";
    case PYGO_EVT_PARKER_FORCE:   return "PARK_FORCE";
    case PYGO_EVT_G_TRANSITION:   return "G_TRANSITION";
    case PYGO_EVT_G_SUBMIT:       return "G_SUBMIT";
    case PYGO_EVT_G_POP:          return "G_POP";
    case PYGO_EVT_G_DECREF:       return "G_DECREF";
    case PYGO_EVT_G_COMPLETE:     return "G_COMPLETE";
    case PYGO_EVT_CHAN_PARK:      return "CHAN_PARK";
    case PYGO_EVT_CHAN_WAKE:      return "CHAN_WAKE";
    default:                      return "?";
    }
}

static void emit(int fd, const char *buf, size_t len)
{
    if (fd < 0) { (void)fwrite(buf, 1, len, stderr); return; }
#if defined(_WIN32)
    (void)_write(fd, buf, (unsigned)len);
#else
    ssize_t off = 0;
    while ((size_t)off < len) {
        ssize_t w = write(fd, buf + off, len - off);
        if (w <= 0) break;
        off += w;
    }
#endif
}

void pygo_diag_dump(int fd)
{
    pygo_ring_t *r;
    char hdr[256];
    int n;
    if (!pygo_ring_list_lock_inited) {
        emit(fd, "[pygo-diag] not initialised\n", 28);
        return;
    }
    n = snprintf(hdr, sizeof hdr,
        "[pygo-diag] flags=0x%x threads=%d ring_cap=%u\n",
        pygo_debug_flags, pygo_diag_registered_thread_count(),
        (unsigned)PYGO_RING_CAP);
    if (n > 0) emit(fd, hdr, (size_t)n);

    pygo_mutex_lock(&pygo_ring_list_lock);
    for (r = pygo_ring_list; r != NULL; r = r->next) {
        unsigned long head = r->head;
        unsigned long count = head < PYGO_RING_CAP ? head : PYGO_RING_CAP;
        unsigned long i;
        n = snprintf(hdr, sizeof hdr,
            "[pygo-diag] tid=%u events=%lu (head=%lu)\n",
            r->tid, count, head);
        if (n > 0) emit(fd, hdr, (size_t)n);
        /* Newest-first walk. */
        for (i = 0; i < count; i++) {
            unsigned long idx = (head - 1 - i) & PYGO_RING_MASK;
            const pygo_evt_t *e = &r->slots[idx];
            char line[200];
            int m;
            m = snprintf(line, sizeof line,
                "  ts=%lld op=%-12s p1=%p p2=%p aux=%lld\n",
                e->ts_ns, op_name(e->op),
                (void *)e->p1, (void *)e->p2, e->aux);
            if (m > 0) emit(fd, line, (size_t)m);
        }
    }
    pygo_mutex_unlock(&pygo_ring_list_lock);
}


/* ---------------------------------------------------------------- *
 *  Self check                                                      *
 *                                                                  *
 *  Implemented as a friend of netpoll.c: netpoll exposes the       *
 *  inspection primitives below, this just orchestrates the walk.   *
 * ---------------------------------------------------------------- */

/* Hooks into netpoll.c.  Implemented there because they need access
 * to the static parker structures; declared here so callers don't
 * have to include netpoll.h directly. */
struct pygo_self_check_stats {
    int  global_list_count;
    int  global_list_cycle;
    int  parked_total_atomic;
    int  bucket_count_total;
    int  bucket_self_loops;
    int  bucket_unreachable;
};

extern int pygo_netpoll_inspect_for_self_check(
    struct pygo_self_check_stats *out);

/* Setter exposed so netpoll.c can fill in the stats without including
 * pygo_diag.c's private struct definition. */
void pygo_self_check_stats_set(struct pygo_self_check_stats *out,
                               int global_list_count,
                               int global_list_cycle,
                               int parked_total_atomic,
                               int bucket_count_total,
                               int bucket_self_loops,
                               int bucket_unreachable)
{
    if (out == NULL) return;
    out->global_list_count    = global_list_count;
    out->global_list_cycle    = global_list_cycle;
    out->parked_total_atomic  = parked_total_atomic;
    out->bucket_count_total   = bucket_count_total;
    out->bucket_self_loops    = bucket_self_loops;
    out->bucket_unreachable   = bucket_unreachable;
}

int pygo_self_check(int verbose)
{
    struct pygo_self_check_stats s;
    int violations = 0;
    char buf[400];
    int n;
    memset(&s, 0, sizeof s);
    if (pygo_netpoll_inspect_for_self_check(&s) != 0) {
        emit(-1, "[pygo-diag] self_check: netpoll inspect failed\n", 47);
        return 1;
    }
    if (s.global_list_cycle) {
        n = snprintf(buf, sizeof buf,
            "[pygo-diag] self_check: GLOBAL LIST CYCLE detected "
            "(walked %d entries before cycle)\n", s.global_list_count);
        emit(-1, buf, (size_t)n);
        violations++;
    }
    if (s.bucket_self_loops > 0) {
        n = snprintf(buf, sizeof buf,
            "[pygo-diag] self_check: %d per-fd buckets self-loop\n",
            s.bucket_self_loops);
        emit(-1, buf, (size_t)n);
        violations++;
    }
    if (!s.global_list_cycle &&
        s.global_list_count != s.parked_total_atomic) {
        n = snprintf(buf, sizeof buf,
            "[pygo-diag] self_check: parked_total=%d but walked=%d\n",
            s.parked_total_atomic, s.global_list_count);
        emit(-1, buf, (size_t)n);
        violations++;
    }
    if (s.bucket_unreachable > 0) {
        n = snprintf(buf, sizeof buf,
            "[pygo-diag] self_check: %d bucket entries not in global list\n",
            s.bucket_unreachable);
        emit(-1, buf, (size_t)n);
        violations++;
    }
    if (verbose && violations == 0) {
        n = snprintf(buf, sizeof buf,
            "[pygo-diag] self_check: OK (parked=%d buckets=%d)\n",
            s.global_list_count, s.bucket_count_total);
        emit(-1, buf, (size_t)n);
    }
    return violations;
}


/* ---------------------------------------------------------------- *
 *  Init / fini                                                     *
 * ---------------------------------------------------------------- */

static int pygo_diag_inited = 0;

void pygo_diag_init(void)
{
    if (pygo_diag_inited) return;
    pygo_mutex_init(&pygo_ring_list_lock);
    pygo_ring_list_lock_inited = 1;
    parse_pygo_debug_env();
    pygo_diag_inited = 1;
}

void pygo_diag_fini(void)
{
    pygo_ring_t *r, *next;
    if (!pygo_diag_inited) return;
    pygo_mutex_lock(&pygo_ring_list_lock);
    r = pygo_ring_list;
    pygo_ring_list = NULL;
    pygo_mutex_unlock(&pygo_ring_list_lock);
    while (r != NULL) {
        next = r->next;
        free(r);
        r = next;
    }
    pygo_mutex_destroy(&pygo_ring_list_lock);
    pygo_ring_list_lock_inited = 0;
    pygo_diag_inited = 0;
    pygo_tls_ring = NULL;   /* don't free TLS rings on fini: they're freed above */
}
