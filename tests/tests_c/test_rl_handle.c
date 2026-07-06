/* test_rl_handle.c -- threaded torture for the generation-stamped handle
 * substrate (item 3, src/runloom_c/rl_handle.c).  Compiles rl_handle.c directly
 * (its deps are header-inline).  Run under ASan/TSan (Makefile variants).
 *
 * The hazard being tortured: a RESOLVER holds a handle to an object that a
 * PRODUCER concurrently releases and FREES, then re-registers a different heap
 * object into the reused slot.  With a raw pointer this is a UAF / wrong-object
 * deref.  The substrate must make rl_handle_resolve return NULL for the stale
 * handle -- never the freed or reused object.  Each resolver DEREFERENCES a
 * non-NULL resolve, so a stale resolve is a UAF ASan flags and a wrong-object a
 * self-handle mismatch this test asserts.
 *
 * Usage: test_rl_handle [seconds] [nproducers] [nresolvers]. */
#include <stdatomic.h>
#include "rl_handle.c"          /* compile the unit under test directly */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct {
    rl_handle_t self;           /* the handle this object was registered under */
    unsigned    magic;          /* 0xA11VE while live; scribbled on free */
    char        pad[48];
} obj_t;

#define MAGIC_LIVE 0x0A11FE0Du

/* A small ring of "recently published" handles resolvers race against.  A
 * producer publishes each new handle here (overwriting an old one) so resolvers
 * mostly chase handles that are being freed/reused -- the interesting window. */
#define RING 256
static _Atomic rl_handle_t g_ring[RING];
static _Atomic int         g_stop;
static _Atomic long        g_resolved_live;   /* non-NULL resolves that were valid */
static _Atomic long        g_iters;

static unsigned rng(unsigned *s) { *s ^= *s << 13; *s ^= *s >> 17; *s ^= *s << 5; return *s; }

/* Deferred free: called by the substrate when the LAST reference is dropped, so
 * it never runs while a resolver is pinned.  Scribble then free -> a resolver
 * that (wrongly) touched a freed object would see the poison / ASan-fault. */
static void free_obj(void *p)
{
    obj_t *o = (obj_t *)p;
    __atomic_store_n(&o->magic, 0xDEADDEADu, __ATOMIC_RELEASE);
    free(o);
}

static void *producer(void *arg)
{
    unsigned s = (unsigned)(uintptr_t)arg * 2654435761u + 1u;
    while (!atomic_load_explicit(&g_stop, memory_order_relaxed)) {
        obj_t *o = (obj_t *)malloc(sizeof(*o));
        if (!o) continue;
        o->magic = MAGIC_LIVE;
        rl_handle_t h = rl_handle_register(o, free_obj);
        if (h == RL_HANDLE_NULL) { free(o); continue; }
        o->self = h;
        atomic_store_explicit(&g_ring[rng(&s) % RING], h, memory_order_release);
        /* Tiny lifetime, then drop the owner ref.  The object is freed (via
         * free_obj) by whoever drops the LAST ref -- deferred past any pin, so a
         * concurrent resolver never derefs freed memory. */
        rl_handle_release(h);
        atomic_fetch_add_explicit(&g_iters, 1, memory_order_relaxed);
    }
    return 0;
}

static void *resolver(void *arg)
{
    unsigned s = (unsigned)(uintptr_t)arg * 40503u + 7u;
    while (!atomic_load_explicit(&g_stop, memory_order_relaxed)) {
        rl_handle_t h = atomic_load_explicit(&g_ring[rng(&s) % RING],
                                             memory_order_acquire);
        if (h == RL_HANDLE_NULL) continue;
        obj_t *o = (obj_t *)rl_handle_pin(h);   /* upgrade weak handle -> strong */
        if (o == NULL) continue;                /* stale -> correctly nothing */
        /* PINNED: the object cannot be reclaimed/freed until we unpin, so this
         * deref is UAF-safe.  A stale pin returning a freed/reused object would
         * trip the magic / self-handle checks (and ASan). */
        unsigned m = __atomic_load_n(&o->magic, __ATOMIC_ACQUIRE);
        if (m != MAGIC_LIVE) {
            fprintf(stderr, "STALE PIN: handle %llu -> object magic=%08x "
                    "(freed/reused)\n", (unsigned long long)h, m);
            abort();
        }
        if (o->self != h) {
            fprintf(stderr, "WRONG OBJECT: handle %llu -> object.self %llu\n",
                    (unsigned long long)h, (unsigned long long)o->self);
            abort();
        }
        atomic_fetch_add_explicit(&g_resolved_live, 1, memory_order_relaxed);
        rl_handle_unpin(h);
    }
    return 0;
}

int main(int argc, char **argv)
{
    int secs = (argc > 1) ? atoi(argv[1]) : 3;
    int nprod = (argc > 2) ? atoi(argv[2]) : 4;
    int nres  = (argc > 3) ? atoi(argv[3]) : 4;
    if (nprod < 1) nprod = 1;
    if (nres < 1) nres = 1;

    pthread_t th[64];
    int n = 0;
    for (int i = 0; i < nprod && n < 64; i++)
        pthread_create(&th[n++], 0, producer, (void *)(uintptr_t)(i + 1));
    for (int i = 0; i < nres && n < 64; i++)
        pthread_create(&th[n++], 0, resolver, (void *)(uintptr_t)(i + 1));

    struct timespec ts = { secs, 0 };
    nanosleep(&ts, 0);
    atomic_store_explicit(&g_stop, 1, memory_order_release);
    for (int i = 0; i < n; i++) pthread_join(th[i], 0);

    printf("OK: %ld register/free cycles, %ld valid resolves, %ld live handles "
           "leaked\n", atomic_load(&g_iters), atomic_load(&g_resolved_live),
           rl_handle_live_count());
    /* Every registered handle was released, so nothing should leak. */
    if (rl_handle_live_count() != 0) {
        fprintf(stderr, "LEAK: %ld handles still live after teardown\n",
                rl_handle_live_count());
        return 1;
    }
    return 0;
}
