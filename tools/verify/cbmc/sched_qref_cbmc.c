/* SOURCE-ANCHOR: runloom_mn_hub_submit runloom_g_try_incref  (guards this hand-model vs src drift; tools/verify/model_source_drift.py) */
/*
 * sched_qref_cbmc.c -- CBMC model of the default-path goroutine queue-membership
 * + refcount protocol the per-hub-kqueue crash centred on, and of the CORRECT
 * fix for it.
 *
 * FAITHFUL SLICE of hub_main's default-block consume path
 * (mn_sched_hub_main.c.inc) + hub_submit (mn_sched_mn_api.c.inc):
 *
 *   - g is spawned holding its SPAWN ref and submitted, taking in_sub_queue
 *     0->1 (queue entry E1).
 *   - The owning hub (HUB) consumes E1: clears in_sub_queue (1->0, the
 *     clear-before-resume so a wake during the run can re-enqueue), resumes
 *     (reads g), drops E1's queue ref, and on completion drops the spawn ref
 *     (freeing on the last ref).
 *   - Concurrently a STALE wake (WAKER) re-submits: it wants to take a queue
 *     ref and CAS in_sub_queue 0->1 (entry E2).
 *   - E2 is later consumed -> reads g.
 *
 * INVARIANT (NoUAF): no read of g ever happens after it was freed; refcount
 * never goes negative.
 *
 * THREE configurations (run_sched_cbmc.sh: default want_ok, both -D want_bug):
 *
 *   default (the FIX): WAKER takes the ref with TRY_INCREF *before* it touches
 *     the g (incref only if refcount>0).  If the g is already being freed
 *     (refcount==0) try_incref fails and WAKER does nothing -- it never
 *     resurrects a freed g, never touches freed memory.  On a successful ref it
 *     CASes in_sub_queue; if that loses (already queued) it drops the ref again.
 *     Expect VERIFICATION SUCCESSFUL.
 *
 *   -DBUG_INCREF_AFTER_CAS: the NAIVE queue ref (the first patch attempt) --
 *     CAS in_sub_queue 0->1 then a plain incref.  CBMC finds the UAF: HUB frees
 *     the g between the CAS and the incref, so WAKER resurrects a freed g / E2
 *     points at freed memory.  Expect VERIFICATION FAILED.
 *
 *   -DBUG_NO_QUEUE_REF: the original code -- no queue ref at all.  Expect
 *     VERIFICATION FAILED.
 */
#include <pthread.h>
#include <assert.h>

extern int nondet_int(void);

static volatile int refcount;        /* total references */
static volatile int in_sub_queue;    /* queue-membership flag (0/1) */
static volatile int done;            /* g ran to completion */
static volatile int freed;           /* refcount hit 0 -> g released */
static volatile int uaf;             /* set if g is touched while freed */

static void touch(void) { if (freed) uaf = 1; }

static void g_decref(void) {
    int n = __atomic_sub_fetch(&refcount, 1, __ATOMIC_ACQ_REL);
    assert(n >= 0);
    if (n == 0) freed = 1;
}
#if defined(BUG_INCREF_AFTER_CAS)
static void g_incref(void) { __atomic_add_fetch(&refcount, 1, __ATOMIC_ACQ_REL); }
#endif
#if !defined(BUG_INCREF_AFTER_CAS) && !defined(BUG_NO_QUEUE_REF)
/* incref only if the object is still alive (refcount>0); fail if it hit 0. */
static int g_try_incref(void) {
    int old = __atomic_load_n(&refcount, __ATOMIC_RELAXED);
    while (old > 0) {
        if (__atomic_compare_exchange_n(&refcount, &old, old + 1, 0,
                                        __ATOMIC_ACQ_REL, __ATOMIC_RELAXED))
            return 1;
    }
    return 0;
}
#endif

/* HUB: consume entry E1, resume, possibly complete. */
static void *hub(void *a) {
    (void)a;
    int had = (__atomic_exchange_n(&in_sub_queue, 0, __ATOMIC_ACQ_REL) == 1);
    int complete = nondet_int();
    touch();                              /* resume reads g */
#if !defined(BUG_NO_QUEUE_REF)
    if (had) g_decref();                  /* drop E1's queue ref */
#endif
    if (complete) {
        done = 1;
        touch();                          /* done branch reads g */
        g_decref();                       /* drop spawn ref (frees on last) */
    }
    return 0;
}

/* WAKER: a stale wake re-submits the g while HUB is resuming/completing it. */
static void *waker(void *a) {
    (void)a;
#if defined(BUG_NO_QUEUE_REF)
    int expected = 0;
    if (__atomic_compare_exchange_n(&in_sub_queue, &expected, 1, 0,
                                    __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
        touch();                          /* hub_submit reads g */
#elif defined(BUG_INCREF_AFTER_CAS)
    int expected = 0;
    if (__atomic_compare_exchange_n(&in_sub_queue, &expected, 1, 0,
                                    __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        g_incref();                       /* WINDOW: g may already be freed here */
        touch();
    }
#else
    /* FIX: hold the g BEFORE touching it.  try_incref fails if it is freeing. */
    if (!g_try_incref()) return 0;        /* g gone -> ignore the stale wake */
    int expected = 0;
    if (__atomic_compare_exchange_n(&in_sub_queue, &expected, 1, 0,
                                    __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        touch();                          /* safe: ref held */
    } else {
        g_decref();                       /* already queued -> drop our ref */
    }
#endif
    return 0;
}

int main(void) {
    pthread_t th, tw;
    refcount = 1;                         /* spawn ref */
#if !defined(BUG_NO_QUEUE_REF)
    refcount = 2;                         /* + E1 queue ref */
#endif
    in_sub_queue = 1;
    done = 0; freed = 0; uaf = 0;

    pthread_create(&th, 0, hub, 0);
    pthread_create(&tw, 0, waker, 0);
    pthread_join(th, 0);
    pthread_join(tw, 0);

    if (in_sub_queue) {                   /* a surviving entry E2 is consumed */
        touch();
#if !defined(BUG_NO_QUEUE_REF)
        g_decref();
#endif
    }
    assert(!uaf);
    if (done && !in_sub_queue) assert(freed);
    return 0;
}
