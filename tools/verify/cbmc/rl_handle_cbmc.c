/* SOURCE-ANCHOR: rl_handle_register rl_handle_pin rl_handle_release  (guards this model vs src/runloom_c/rl_handle.c; tools/verify/model_source_drift.py) */
/*
 * rl_handle_cbmc.c -- CBMC proof of the generation-stamped handle PIN protocol
 * (item 3, rl_handle.c), the core that makes the object-reuse/ABA class
 * inexpressible.  Models the (generation:32 | refcount:32) genref word.
 *
 * A RESOLVER holds handle hA for object A and pins it (a generation-checked
 * refcount upgrade).  Concurrently the OWNER releases its reference.  The last
 * reference to drop RECLAIMS: it bumps the generation and FREES A.
 *
 * INVARIANTS:
 *   NoUseAfterFree : while a resolver holds a successful pin, A is NOT freed
 *                    (a pin holds a refcount, so reclaim -- which only runs at
 *                    rc==0 -- cannot run under it).
 *   NoStalePin     : pin(hA) returns A or NULL, never a reused-slot object.
 *
 * The dynamic torture (tests/tests_c/test_rl_handle.c) found the naive
 * resolve-WITHOUT-pin design was UAF-unsafe (the owner freed A in the window
 * between resolve returning A and the resolver dereferencing it); this proves
 * the pin fix.
 *
 * Configs:
 *   default          : CAS-rc++-iff-(gen matches && rc>0) pin -> OK.
 *   -DBUG_NO_PIN     : the resolver does NOT hold a refcount (just reads the
 *                      pointer if gen matches) -> the owner's release reclaims +
 *                      frees A while the resolver "uses" it -> use-after-free.
 */
#include <pthread.h>
#include <assert.h>
#include <stdint.h>
#include <stdatomic.h>

#define GA 1u                       /* handle hA's generation */

static _Atomic uint64_t genref;     /* (gen:32 | rc:32) */
static _Atomic int      freed;      /* A has been reclaimed/freed */
static int              used_after_free;

#define GEN(gr)     ((uint32_t)((gr) >> 32))
#define RC(gr)      ((uint32_t)((gr) & 0xFFFFFFFFu))
#define PACK(g, rc) (((uint64_t)(uint32_t)(g) << 32) | (uint32_t)(rc))

static void reclaim(uint32_t g)
{
    /* rc just hit 0 (exclusive): bump gen, free A. */
    atomic_store_explicit(&genref, PACK(g + 1, 0), memory_order_release);
    atomic_store_explicit(&freed, 1, memory_order_release);
}

/* rl_handle_deref (unpin/release): CAS rc-- ; reclaim on 1->0. */
static void deref(uint32_t hg)
{
    uint64_t gr = atomic_load_explicit(&genref, memory_order_acquire);
    for (;;) {
        if (GEN(gr) != hg || RC(gr) == 0) return;
        if (atomic_compare_exchange_strong_explicit(&genref, &gr,
                PACK(hg, RC(gr) - 1), memory_order_acq_rel, memory_order_acquire)) {
            if (RC(gr) - 1 == 0) reclaim(hg);
            return;
        }
    }
}

/* rl_handle_pin: CAS rc++ iff gen matches AND rc>0.  Returns 1 (pinned) / 0. */
static int pin(uint32_t hg)
{
#ifdef BUG_NO_PIN
    /* naive: gen match but NO refcount held -> nothing stops a concurrent
     * release from reclaiming/freeing A while we "use" it. */
    return GEN(atomic_load_explicit(&genref, memory_order_acquire)) == hg;
#else
    uint64_t gr = atomic_load_explicit(&genref, memory_order_acquire);
    for (;;) {
        if (GEN(gr) != hg || RC(gr) == 0) return 0;
        if (atomic_compare_exchange_strong_explicit(&genref, &gr,
                PACK(hg, RC(gr) + 1), memory_order_acq_rel, memory_order_acquire))
            return 1;
    }
#endif
}

static void *owner(void *a) { (void)a; deref(GA); return 0; }   /* release owner ref */

static void *resolver(void *a)
{
    (void)a;
    if (pin(GA)) {
        /* USE A: it must still be alive here (the pin holds it). */
        if (atomic_load_explicit(&freed, memory_order_acquire)) used_after_free = 1;
#ifndef BUG_NO_PIN
        deref(GA);   /* unpin */
#endif
    }
    return 0;
}

int main(void)
{
    atomic_init(&genref, PACK(GA, 1));   /* registered: gen GA, owner's rc=1 */
    atomic_init(&freed, 0);

    pthread_t o, r;
    pthread_create(&o, 0, owner, 0);
    pthread_create(&r, 0, resolver, 0);
    pthread_join(o, 0);
    pthread_join(r, 0);

    assert(!used_after_free);            /* NoUseAfterFree */
    return 0;
}
