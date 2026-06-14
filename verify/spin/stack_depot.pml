/*
 * stack_depot.pml -- Promela model of the cross-hub coroutine STACK-MEMORY
 * magazine allocator (coro.c: runloom_stack_pop_local / refill_from_global /
 * flush_to_global + the per-thread TLS cache and the shared global depot).
 *
 * This is the one cross-hub-recycled resource with NO prior FV coverage and TWO
 * distinct failure surfaces (see docs/dev/LIFECYCLE_INVARIANTS.md, Tier-1 #1):
 *
 *   (a) ALIAS / UAF -- the `next`/`size` header is written INTO the free stack's
 *       low bytes; a size-mismatched mapping handed back to a coro would be used
 *       as the wrong size (its guard page in the wrong place) -> silent stack
 *       corruption.  The code defends this with a SIZE GUARD on every handout
 *       path: pop_local munmaps head-mismatches, refill munmaps mismatched depot
 *       entries, so a pool only ever yields a matching-size mapping.
 *   (b) VMA EXHAUSTION -- a stack freed on hub A piles into the depot for hub B
 *       to reuse; the depot is CAPPED (past cap -> munmap) so retained cross-hub
 *       mappings stay finite and `live + pool` VMAs stay under vm.max_map_count
 *       (the documented N>>100K scale ceiling).
 *
 * LIFE-CYCLE invariants proved (2 hubs sharing the depot under a lock, the TLS
 * cache per-hub, a bounded op sequence; counts-per-size abstraction -- faithful
 * for these properties because every pool only ever holds/yields matching-size
 * mappings, so internal list order is irrelevant to the guard).  Each token-move
 * (a pointer swing under the depot lock, or an atomic counter step) is a single
 * d_step, so a peer hub never observes a half-applied move:
 *
 *   PARTITION   -- every mapping ever created is in EXACTLY ONE of
 *                  {live, a TLS cache, the depot, munmap'd}: the ledger
 *                  `created == live + tls + depot + unmapped` holds always
 *                  (no double-free, no leak, no use-after-munmap, no double-own).
 *   SIZE-MATCH  -- every acquire hands back a mapping of the REQUESTED size.
 *   DEPOT-BOUND -- the depot never exceeds its cap (the VMA bound).
 *
 * Negative controls (must FAIL = pan finds the bug):
 *   -DBUG_NO_SIZE_GUARD : a pool pop ignores the size header -> a size-mismatched
 *                         mapping is handed to a coro (the alias/UAF surface).
 *   -DBUG_NO_DEPOT_CAP  : flush pushes to the depot without the cap check -> the
 *                         depot grows unbounded (the VMA-exhaustion surface).
 *
 * Two sizes (0/1) -- the mixed-size case is what exercises the guard; the common
 * single-size workload is the size-equal projection.
 */

#define NHUB     2
#define NSIZE    2
#define DEPOTCAP 2          /* shrunk depot cap (production: auto ~1.5x live HWM) */
#define TLSCAP   1          /* per-hub cache high-water: flush when total > this */
#define TLSKEEP  1          /* kept local on a flush */
#define HOLDMAX  2          /* max stacks a hub holds live at once */
#define NOPS     5          /* ops per hub (bounds the state space) */

/* counts-per-size: tls[hub*NSIZE + size], depot[size]; plus the ledger. */
int tls[NHUB * NSIZE];
int depot[NSIZE];
int live      = 0;          /* mappings in use by a coro */
int unmapped  = 0;          /* munmap'd (gone) */
int created   = 0;          /* mappings ever mmap'd */
bit glock     = 0;          /* the global_stack_lock */

#define TLS(h,s)  tls[(h)*NSIZE + (s)]
#define DEPOT_TOTAL (depot[0] + depot[1])

inline lock()   { atomic { glock == 0 -> glock = 1 } }
inline unlock() { glock = 0 }

/* PARTITION ledger + DEPOT-BOUND, checked after every op (each token-move is a
 * d_step, so this read is never split across a move). */
inline check_invariants() {
    assert(live + tls[0] + tls[1] + tls[2] + tls[3]
           + depot[0] + depot[1] + unmapped == created);
    assert(DEPOT_TOTAL <= DEPOTCAP);
}

/* refill_from_global(req): under the lock, munmap mismatched-size depot entries
 * and move matching-size ones into this hub's TLS.  Faithful for SIZE-MATCH:
 * only matching-size mappings ever enter a TLS cache. */
inline refill(h, req) {
    lock();
    do
    :: depot[1 - req] > 0 -> d_step { depot[1 - req]--; unmapped++ }      /* munmap mismatch */
    :: else -> break
    od;
    do
    :: depot[req] > 0 -> d_step { depot[req]--; TLS(h, req) = TLS(h, req) + 1 }  /* depot->tls */
    :: else -> break
    od;
    unlock();
}

/* acquire(h, req): pop a matching mapping from TLS; else refill + retry; else
 * mmap a fresh one.  Sets acq=1, gotsz=<size handed back>. */
inline acquire(h, req) {
    acq = 0; gotsz = 9;
#ifdef BUG_NO_SIZE_GUARD
    /* INJECTED BUG: take ANY cached mapping, ignoring the size header. */
    if
    :: TLS(h, req) > 0 -> d_step { TLS(h, req) = TLS(h, req) - 1; live++ }; acq = 1; gotsz = req
    :: (TLS(h, req) == 0 && TLS(h, 1 - req) > 0) ->
        d_step { TLS(h, 1 - req) = TLS(h, 1 - req) - 1; live++ }; acq = 1; gotsz = 1 - req
    :: else -> skip
    fi;
#else
    if
    :: TLS(h, req) > 0 -> d_step { TLS(h, req) = TLS(h, req) - 1; live++ }; acq = 1; gotsz = req
    :: else -> skip
    fi;
#endif
    if
    :: (acq == 0) ->                       /* nothing matched locally: refill, retry */
        refill(h, req);
        if
        :: TLS(h, req) > 0 -> d_step { TLS(h, req) = TLS(h, req) - 1; live++ }; acq = 1; gotsz = req
        :: else -> d_step { created++; live++ }; acq = 1; gotsz = req   /* mmap fresh */
        fi
    :: else -> skip
    fi;
    assert(gotsz == req);                  /* SIZE-MATCH: handed the requested size */
    check_invariants();
}

/* release(h, sz): push to the TLS cache; on overflow flush all-but-KEEP to the
 * depot (past the cap -> munmap). */
inline release(h, sz) {
    d_step { live--; TLS(h, sz) = TLS(h, sz) + 1 }
    if
    :: (TLS(h, 0) + TLS(h, 1) > TLSCAP) ->
        lock();
        do
        :: (TLS(h, 0) + TLS(h, 1) > TLSKEEP) ->
            if
            :: TLS(h, 0) > 0 -> ms = 0
            :: TLS(h, 1) > 0 -> ms = 1
            fi;
#ifdef BUG_NO_DEPOT_CAP
            d_step { TLS(h, ms) = TLS(h, ms) - 1; depot[ms]++ }   /* INJECTED BUG: no cap */
#else
            if
            :: DEPOT_TOTAL < DEPOTCAP -> d_step { TLS(h, ms) = TLS(h, ms) - 1; depot[ms]++ }
            :: else -> d_step { TLS(h, ms) = TLS(h, ms) - 1; unmapped++ }   /* past cap: munmap */
            fi
#endif
        :: else -> break
        od;
        unlock()
    :: else -> skip
    fi;
    check_invariants();
}

active [NHUB] proctype hub()
{
    int me = _pid;            /* 0 or 1 */
    int i = 0;
    int held = 0;             /* stacks currently held live by this hub */
    int req;
    int acq; int gotsz; int ms;   /* inline scratch (proctype-scoped: one per hub) */
    int hs0 = 0; int hs1 = 0;     /* sizes of held stacks, to release the right size */

    do
    :: (i < NOPS) ->
        i++;
        if
        :: (held < HOLDMAX) ->                 /* acquire a stack of a chosen size */
            if :: req = 0 :: req = 1 fi;
            acquire(me, req);
            if
            :: (held == 0) -> hs0 = req
            :: else -> hs1 = req
            fi;
            held++
        :: (held > 0) ->                       /* release one we hold */
            if
            :: (held == 1) -> release(me, hs0)
            :: else -> release(me, hs1)
            fi;
            held--
        fi
    :: (i >= NOPS) -> break
    od;

    /* drain held stacks so the ledger is clean at the end */
    do
    :: (held > 0) ->
        if
        :: (held == 1) -> release(me, hs0)
        :: else -> release(me, hs1)
        fi;
        held--
    :: (held == 0) -> break
    od
}
