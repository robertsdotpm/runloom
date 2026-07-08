/* rl_handle.c -- generation-stamped handle table with pin/refcount (item 3).
 * See rl_handle.h. */
#include "rl_handle.h"
#include "plat.h"
#include "plat_compat.h"
#include "plat_atomic.h"
#include "runloom_kcsan.h"

#include <stdlib.h>
#include <string.h>

#if defined(__x86_64__) || defined(__i386__)
#  define RL_CPU_RELAX() __builtin_ia32_pause()
#else
#  define RL_CPU_RELAX() __asm__ __volatile__("" ::: "memory")
#endif

/* Slots are FIXED (never returned to the OS), so a stale pin always reads a real
 * slot (never freed memory) -- it just finds a mismatched generation or rc==0.
 * The table grows in SEGMENTS on demand. */
#ifndef RL_HANDLE_SEG_BITS
#  if defined(RUNLOOM_SHRINK)
#    define RL_HANDLE_SEG_BITS 4u                     /* test-shrink: 16 slots/seg -> frequent segment growth + freelist churn */
#  else
#    define RL_HANDLE_SEG_BITS 12u                    /* 4096 slots / segment */
#  endif
#endif
#define RL_HANDLE_SEG_SLOTS  (1u << RL_HANDLE_SEG_BITS)
#define RL_HANDLE_SEG_MASK   (RL_HANDLE_SEG_SLOTS - 1u)
#define RL_HANDLE_MAX_SEGS   4096u                    /* up to 16M slots */

/* genref packs (generation:32 | refcount:32).  A pin is one CAS on it. */
#define RL_GEN(gr)     ((uint32_t)((gr) >> 32))
#define RL_RC(gr)      ((uint32_t)((gr) & 0xFFFFFFFFu))
#define RL_PACK(g, rc) (((uint64_t)(uint32_t)(g) << 32) | (uint32_t)(rc))

typedef struct {
    void            *ptr;
    void           (*free_fn)(void *);
    _Atomic uint64_t genref;      /* (gen:32 | rc:32); rc==0 => reclaimable */
    uint32_t         next_free;   /* freelist link (slot idx) */
} rl_handle_slot_t;

static rl_handle_slot_t *rl_handle_segs[RL_HANDLE_MAX_SEGS];
static uint32_t          rl_handle_nsegs;
static uint32_t          rl_handle_free_head;        /* slot idx, 0 = empty */
static _Atomic long      rl_handle_live;
static runloom_mutex_t   rl_handle_lock;             /* guards freelist + growth */
static int               rl_handle_lock_ready;

#define RL_SLOT_NONE 0u   /* freelist terminator (slot 0 reserved) */

static void rl_handle_init_once(void)
{
    if (__atomic_load_n(&rl_handle_lock_ready, __ATOMIC_ACQUIRE)) return;
    int expected = 0;
    if (__atomic_compare_exchange_n(&rl_handle_lock_ready, &expected, 2, 0,
                                    __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        runloom_mutex_init(&rl_handle_lock);
        __atomic_store_n(&rl_handle_lock_ready, 1, __ATOMIC_RELEASE);
        return;
    }
    while (__atomic_load_n(&rl_handle_lock_ready, __ATOMIC_ACQUIRE) != 1)
        ; /* spin until the winner finishes mutex_init (once, microseconds) */
}

static rl_handle_slot_t *rl_handle_slot(uint32_t idx)
{
    uint32_t seg = idx >> RL_HANDLE_SEG_BITS;
    if (seg >= __atomic_load_n(&rl_handle_nsegs, __ATOMIC_ACQUIRE)) return NULL;
    return &rl_handle_segs[seg][idx & RL_HANDLE_SEG_MASK];
}

static int rl_handle_grow_locked(void)
{
    uint32_t seg = rl_handle_nsegs;
    if (seg >= RL_HANDLE_MAX_SEGS) return 0;
    rl_handle_slot_t *s =
        (rl_handle_slot_t *)calloc(RL_HANDLE_SEG_SLOTS, sizeof(*s));
    if (s == NULL) return 0;
    rl_handle_segs[seg] = s;
    __atomic_store_n(&rl_handle_nsegs, seg + 1, __ATOMIC_RELEASE);
    uint32_t base  = seg << RL_HANDLE_SEG_BITS;
    uint32_t start = (seg == 0) ? 1u : 0u;            /* reserve slot 0 */
    for (uint32_t i = start; i < RL_HANDLE_SEG_SLOTS; i++) {
        s[i].next_free = rl_handle_free_head;
        rl_handle_free_head = base + i;
    }
    return 1;
}

/* Last reference dropped: bump the generation (invalidate every outstanding
 * handle), run the deferred free, and recycle the slot.  Caller established rc==0
 * exclusively (no pin can succeed at rc==0), so this is race-free. */
static void rl_handle_reclaim(rl_handle_slot_t *slot, uint32_t idx, uint32_t gen)
{
    void (*free_fn)(void *) = slot->free_fn;
    void *ptr = slot->ptr;
    /* KCSAN exclusive-access watchpoint (item #8): the caller established rc==0
     * exclusively, so no pin can succeed and nothing else may touch genref until
     * we bump it below.  Sample it: if a concurrent write lands during the stall,
     * a pin raced a reclaim at rc==0 -- the ABA/UAF class this handle table exists
     * to prevent.  Zero cost unless -DRUNLOOM_KCSAN. */
    RUNLOOM_ASSERT_EXCLUSIVE64("rl_handle_reclaim.genref", &slot->genref);
    slot->ptr = NULL;
    slot->free_fn = NULL;
    /* Bump gen with rc left 0 so a re-register on this slot gets gen+1 and old
     * handles mismatch. */
    __atomic_store_n(&slot->genref, RL_PACK(gen + 1, 0), __ATOMIC_RELEASE);
    if (free_fn != NULL && ptr != NULL) free_fn(ptr);
    __atomic_sub_fetch(&rl_handle_live, 1, __ATOMIC_RELAXED);
    runloom_mutex_lock(&rl_handle_lock);
    slot->next_free = rl_handle_free_head;
    rl_handle_free_head = idx;
    runloom_mutex_unlock(&rl_handle_lock);
}

rl_handle_t rl_handle_register(void *ptr, void (*free_fn)(void *))
{
    uint32_t idx, gen;
    rl_handle_slot_t *slot;

    if (ptr == NULL) return RL_HANDLE_NULL;
    rl_handle_init_once();

    runloom_mutex_lock(&rl_handle_lock);
    if (rl_handle_free_head == RL_SLOT_NONE && !rl_handle_grow_locked()) {
        runloom_mutex_unlock(&rl_handle_lock);
        return RL_HANDLE_NULL;
    }
    idx = rl_handle_free_head;
    slot = rl_handle_slot(idx);
    rl_handle_free_head = slot->next_free;
    runloom_mutex_unlock(&rl_handle_lock);

    /* The slot is exclusively ours (off the freelist, rc==0, no valid handle to
     * it since the last reclaim bumped gen).  Publish ptr/free_fn, then set
     * rc=1 with the current gen using a RELEASE store so a pin that later
     * acquire-reads the new state also sees ptr. */
    gen = RL_GEN(__atomic_load_n(&slot->genref, __ATOMIC_RELAXED));
    slot->ptr = ptr;
    slot->free_fn = free_fn;
    __atomic_store_n(&slot->genref, RL_PACK(gen, 1), __ATOMIC_RELEASE);
    __atomic_add_fetch(&rl_handle_live, 1, __ATOMIC_RELAXED);

    return ((rl_handle_t)gen << 32) | (rl_handle_t)idx;
}

void *rl_handle_pin(rl_handle_t h)
{
    uint32_t idx = (uint32_t)(h & 0xFFFFFFFFu);
    uint32_t hg  = (uint32_t)(h >> 32);
    rl_handle_slot_t *slot;
    uint64_t gr;

    if (h == RL_HANDLE_NULL) return NULL;
    slot = rl_handle_slot(idx);
    if (slot == NULL) return NULL;

    /* try_incref-with-generation: CAS rc++ iff gen still matches AND rc>0 (not
     * being reclaimed).  Any mismatch -> stale -> NULL, never a dangling deref. */
    gr = __atomic_load_n(&slot->genref, __ATOMIC_ACQUIRE);
    for (;;) {
        if (RL_GEN(gr) != hg || RL_RC(gr) == 0) return NULL;
        if (__atomic_compare_exchange_n(&slot->genref, &gr,
                                        RL_PACK(hg, RL_RC(gr) + 1), 0,
                                        __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
            /* pinned: rc>0 pins the object; ptr is this registration's (gen
             * matched) and cannot be reclaimed until we unpin. */
            return slot->ptr;
        }
        /* CAS failed: gr reloaded; loop re-checks gen/rc. */
    }
}

/* Shared decrement for unpin + release.  Reclaims on the 1->0 transition. */
static void rl_handle_deref(rl_handle_t h)
{
    uint32_t idx = (uint32_t)(h & 0xFFFFFFFFu);
    uint32_t hg  = (uint32_t)(h >> 32);
    rl_handle_slot_t *slot;
    uint64_t gr;

    if (h == RL_HANDLE_NULL) return;
    slot = rl_handle_slot(idx);
    if (slot == NULL) return;

    gr = __atomic_load_n(&slot->genref, __ATOMIC_ACQUIRE);
    for (;;) {
        if (RL_GEN(gr) != hg || RL_RC(gr) == 0) return;   /* already reclaimed */
        if (__atomic_compare_exchange_n(&slot->genref, &gr,
                                        RL_PACK(hg, RL_RC(gr) - 1), 0,
                                        __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
            if (RL_RC(gr) - 1 == 0) rl_handle_reclaim(slot, idx, hg);  /* last ref */
            return;
        }
    }
}

void rl_handle_unpin(rl_handle_t h)   { rl_handle_deref(h); }
void rl_handle_release(rl_handle_t h) { rl_handle_deref(h); }

void rl_handle_release_wait(rl_handle_t h)
{
    uint32_t idx = (uint32_t)(h & 0xFFFFFFFFu);
    uint32_t hg  = (uint32_t)(h >> 32);
    rl_handle_slot_t *slot = rl_handle_slot(idx);

    rl_handle_deref(h);                        /* drop the owner ref */
    if (slot == NULL) return;
    /* Wait until this registration is fully reclaimed (gen moves past hg): once
     * gen != hg, no resolver can hold or acquire a pin on THIS registration, so
     * the object (a stack frame) is safe to abandon. */
    while (RL_GEN(__atomic_load_n(&slot->genref, __ATOMIC_ACQUIRE)) == hg)
        RL_CPU_RELAX();
}

long rl_handle_live_count(void)
{
    return __atomic_load_n(&rl_handle_live, __ATOMIC_ACQUIRE);
}

/* Handle-table integrity sweep (QA-steal-V2 #2, "runtime fsck" extension of
 * runloom_self_check, which was netpoll-only).  Two invariants from
 * docs/dev/LIFECYCLE_INVARIANTS.md:
 *   (a) the number of live slots (rc>0) equals rl_handle_live -- a mismatch is a
 *       lost incref/decref or a torn genref (handle-table corruption / ABA);
 *   (b) no live slot has a NULL ptr -- a pinned handle pointing at nothing is a
 *       reclaim that raced a pin (a UAF-class corruption).
 * Held under the growth lock (register + reclaim both take it), so the segment
 * table and the freelist are stable across the walk and the count is exact at a
 * quiescent checkpoint.  Fills the out-counts (any may be NULL) for the caller's
 * diagnostic and returns the number of violated invariants (0 = clean). */
int rl_handle_self_check(long *out_live_walked, long *out_live_atomic,
                         long *out_dangling)
{
    long walked = 0, dangling = 0, live_atomic;
    uint32_t nsegs, seg, i;

    if (out_live_walked) *out_live_walked = 0;
    if (out_live_atomic) *out_live_atomic = 0;
    if (out_dangling)    *out_dangling = 0;
    if (__atomic_load_n(&rl_handle_lock_ready, __ATOMIC_ACQUIRE) != 1)
        return 0;                          /* table never initialised: vacuous */

    runloom_mutex_lock(&rl_handle_lock);
    nsegs = __atomic_load_n(&rl_handle_nsegs, __ATOMIC_ACQUIRE);
    for (seg = 0; seg < nsegs; seg++) {
        rl_handle_slot_t *S = rl_handle_segs[seg];
        if (S == NULL) continue;
        for (i = 0; i < RL_HANDLE_SEG_SLOTS; i++) {
            uint64_t gr;
            if (seg == 0 && i == 0) continue;              /* slot 0 reserved */
            gr = __atomic_load_n(&S[i].genref, __ATOMIC_ACQUIRE);
            if (RL_RC(gr) > 0) {
                walked++;
                if (S[i].ptr == NULL) dangling++;
            }
        }
    }
    live_atomic = __atomic_load_n(&rl_handle_live, __ATOMIC_ACQUIRE);
    runloom_mutex_unlock(&rl_handle_lock);

    if (out_live_walked) *out_live_walked = walked;
    if (out_live_atomic) *out_live_atomic = live_atomic;
    if (out_dangling)    *out_dangling = dangling;
    return (walked != live_atomic ? 1 : 0) + (dangling > 0 ? 1 : 0);
}
