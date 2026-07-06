/*
 * netpoll_parker_link.pml -- Promela model of the netpoll parker linked-list
 * surgery in src/runloom_c/netpoll_parker_link.c.inc
 * (runloom_parker_link / runloom_parker_unlink) over the parker pool defined in
 * netpoll_parkers.c.inc.  This proves the reused-stack-address defence.
 *
 * HISTORICAL NOTE: parkers now come from a HEAP freelist
 * (netpoll_parkers.c.inc runloom_parker_pool_acquire), so the stack-address
 * aliasing below can no longer occur -- a freelisted parker has no global
 * pointers, an in-flight one is at a unique heap address.  This model is kept as
 * a standing defense-in-depth proof that the LINK PROTOCOL is safe even IF an
 * alias were reintroduced; the "COROUTINE STACK" premise describes the pre-heap
 * design the freelist replaced.
 *
 * THE HAZARD (modelled faithfully -- see the header comment on
 * runloom_parker_link and the "Why heap, not stack" note in netpoll_parkers).
 * A runloom_parked_t lives on the parking fiber's COROUTINE STACK.  When a g
 * completes, its stack returns to a per-hub TLS pool (runloom_stack_release)
 * and is re-issued to the next g spawned on that hub, whose wait_fd places its
 * parker at the SAME stack offset -- a BYTE-IDENTICAL address to the prior
 * occupant's parker.  So if any unlink for the PRIOR occupant "missed" (the
 * documented residual M:N + free-threaded race "not yet fully isolated
 * upstream"), pool->head or pool->by_fd[fd] can still reference that address
 * when the NEW parker links there.
 *
 * In the model an "address" is a node SLOT (0 or 1).  Two distinct heap
 * addresses (slot 0, slot 1) let us probe cross-chain references; the ALIAS
 * happens when the new parker is placed at a slot a stale pointer still names.
 * runloom_netpoll_wait_fd freshly zeroes all four link fields before the call
 * (modelled: do_link zeroes next/slot/next_by_fd/prev_by_fd of the new slot),
 * but the POOL's head / by_fd[fd] can still name the slot.
 *
 * THE DEFENCE (runloom_parker_link, faithful):
 *   if (pool->head == p)            pool->head = NULL;        // global self-ref
 *   if (by_fd[fd] == p)             by_fd[fd]  = NULL;        // bucket self-ref
 *   p->next = pool->head; if(next) next->slot = &p->next;     // slot-pointer
 *   pool->head = p; p->slot = &pool->head;                    //   trick push
 *   p->prev_by_fd = p->next_by_fd = NULL;                     // bucket push
 *   next_by_fd = by_fd[fd]; if(head) head->prev_by_fd = p; by_fd[fd] = p;
 *   total++;
 *
 * runloom_parker_unlink (faithful, idempotent, exactly-once `total` dec):
 *   touched=0;
 *   if (p->slot)      { *p->slot = p->next; if(next) next->slot=p->slot;
 *                       slot=next=nil; touched=1; }            // global
 *   if (p->prev_by_fd){ prev->next_by_fd = p->next_by_fd; touched=1; }
 *   if (by_fd[fd]==p) { by_fd[fd] = p->next_by_fd; touched=1; }   // both forms
 *   if (p->next_by_fd) next->prev_by_fd = p->prev_by_fd;
 *   prev_by_fd=next_by_fd=nil;
 *   if (touched) total--;
 *
 * Encoding of the link fields.  Each node i has:
 *   nxt[i]   -- p->next        (global singly-linked, traversed by the pump)
 *   slot[i]  -- which "slot pointer" p->slot names: HEAD, or &nxt[j] (=SN+j),
 *               or NIL.  *p->slot = p->next is a write to head or nxt[j].
 *   nbf[i]   -- p->next_by_fd  (bucket doubly-linked)
 *   pbf[i]   -- p->prev_by_fd
 * Pool: head (pool->head), byfd (pool->by_fd[fd]), total (atomic count).
 *
 * PROVEN (positive -- under EVERY missed-unlink setup + EVERY interleaving):
 *   ACYCLIC GLOBAL  -- the pump's bounded walk of pool->head never exceeds N
 *                      steps: no p->next==p self-loop, no multi-node cycle.  A
 *                      walk longer than the node count == a cycle == the
 *                      pump wedge == assert(steps<=N) violation.
 *   ACYCLIC BUCKET  -- the pump's bounded walk along next_by_fd terminates too.
 *   SLOT INVARIANT  -- the slot-pointer trick keeps the global list a valid
 *                      chain: head's slot names &head; every successor's slot
 *                      names its predecessor's nxt field.  No dangling slot.
 *   BALANCED TOTAL  -- the surgery's OWN accounting is exact: total tracks the
 *                      injected baseline + (#links - #touched-unlinks); each
 *                      link +1, each real removal -1, a no-op unlink 0, never
 *                      double, never underflow below the baseline.
 *
 * NEGATIVE CONTROLS (each MUST fail):
 *   BUG_NO_STALE_CLEAR -- drop the two stale self-ref clears in link.  A parker
 *                      linked at a reused address that pool->head still names
 *                      gets p->next = pool->head = p -> a global self-cycle; the
 *                      pump's bounded walk wedges (assert(steps<=N) fires).
 *   BUG_DOUBLE_DEC -- unlink decrements `total` unconditionally (not gated on
 *                      `touched`).  An idempotent second unlink (a no-op that
 *                      removes nothing) still decrements -> total drifts below
 *                      the surgery's ledger while a real parker remains linked
 *                      -> the idle path that gates the pump on total>0 mis-
 *                      sleeps (the BALANCED TOTAL assert fires).
 *
 * THE MISSED-UNLINK probe (the crux the task asks to resolve).  The init block
 * nondeterministically injects a PRIOR occupant q whose unlink "missed", so the
 * pool still references q's slot, THEN reuses that slot for the new parker p.
 * Three shapes (see init): (A) self-ref alias, (B) cross-chain bucket residual,
 * (C) clean reuse baseline.  FINDING (see report): the stale-clear FULLY masks
 * the STRUCTURAL hazard in every shape -- the global list and both buckets stay
 * acyclic, the slot trick stays valid, the walks always terminate.  What it
 * does NOT and structurally CANNOT repair is the `total` OVER-COUNT a missed
 * unlink left from the prior life (the prior decrement that never happened);
 * the surgery only owns its own +1/-1 pairing.  The model exposes this by
 * tracking the injected baseline skew separately: BALANCED TOTAL holds for the
 * surgery's accounting, but `total` carries forward the prior leak.  That
 * residual is exactly the "not yet fully isolated upstream" the source flags,
 * and is why netpoll_parkers.c.inc moved parkers to a non-aliasing HEAP pool.
 */

#define N   2          /* node slots = distinct addresses (0,1)            */
#define NIL 255        /* null node ref                                    */

/* slot[] target encoding: where p->slot points (the lvalue *p->slot writes). */
#define HEAD 254       /* p->slot == &pool->head                            */
#define SN   100       /* p->slot == &nxt[t-SN]  (t in SN..SN+N-1)          */

byte nxt[N];           /* p->next                                           */
byte slot[N];          /* p->slot target (HEAD | SN+j | NIL)                */
byte nbf[N];           /* p->next_by_fd                                     */
byte pbf[N];           /* p->prev_by_fd                                     */

byte head = NIL;       /* pool->head                                        */
byte byfd = NIL;       /* pool->by_fd[fd]  (single fd)                      */
int  total = 0;        /* pool->total (atomic linked count)                 */

/* Surgery ledger: the count the surgery's OWN +1/-1 pairing should produce,
 * = injected baseline + links_done - touched_unlinks_done.  BALANCED TOTAL
 * asserts total == ledger, independent of any prior-life skew. */
int  baseline = 0;     /* total at init (the injected missed-unlink skew)    */
int  ledger   = 0;     /* expected total per the surgery's own operations    */

bit  lock = 0;         /* pool->lock                                        */
#define LOCK   d_step { (lock == 0) -> lock = 1 }
#define UNLOCK lock = 0

/* -------- write through a slot target: *p->slot = v -------------------- */
inline write_slot(t, v) {
    if
    :: t == HEAD -> head = v
    :: t != HEAD && t != NIL -> nxt[t - SN] = v
    :: t == NIL  -> skip            /* p->slot == NULL: nothing to write     */
    fi
}

/* ============================ runloom_parker_link ====================== *
 * Caller holds pool->lock.  `i` is the node slot the new parker occupies
 * (its address); do_link freshly zeroes its link fields first (wait_fd does
 * this before the call).                                                    */
inline do_link(i) {
    /* fresh-zeroed link fields (runloom_netpoll_wait_fd zeroes before link) */
    nxt[i] = NIL; slot[i] = NIL; nbf[i] = NIL; pbf[i] = NIL;

#ifndef BUG_NO_STALE_CLEAR
    /* Stale self-reference clears -- the reused-address defence. */
    if :: head == i -> head = NIL :: else -> skip fi;
    if :: byfd == i -> byfd = NIL :: else -> skip fi;
#endif

    /* Global list: push at head, slot-pointer trick. */
    nxt[i] = head;                          /* p->next = pool->head          */
    if :: nxt[i] != NIL -> slot[nxt[i]] = SN + i  /* next->slot = &p->next   */
       :: else -> skip
    fi;
    head    = i;                            /* pool->head = p                */
    slot[i] = HEAD;                         /* p->slot = &pool->head         */

    /* Per-fd bucket: push at head, doubly-linked. */
    pbf[i] = NIL; nbf[i] = NIL;
    nbf[i] = byfd;                          /* p->next_by_fd = bucket head    */
    if :: byfd != NIL -> pbf[byfd] = i      /* head->prev_by_fd = p           */
       :: else -> skip
    fi;
    byfd = i;                               /* pool->by_fd[fd] = p            */

    total  = total + 1;
    ledger = ledger + 1;                    /* surgery owns this +1           */
}

/* ============================ runloom_parker_unlink =================== *
 * Caller holds pool->lock.  Idempotent; sets touched_out.                  */
inline do_unlink(i, touched_out) {
    byte t;
    touched_out = 0;

    /* Global list removal via the slot pointer. */
    if
    :: slot[i] != NIL ->
        t = slot[i];
        write_slot(t, nxt[i]);              /* *p->slot = p->next             */
        if :: nxt[i] != NIL -> slot[nxt[i]] = t  /* p->next->slot = p->slot   */
           :: else -> skip
        fi;
        slot[i] = NIL; nxt[i] = NIL;
        touched_out = 1;
    :: else -> skip
    fi;

    /* Bucket: BOTH forms checked (middle-of-chain AND head), per source. */
    if
    :: pbf[i] != NIL -> nbf[pbf[i]] = nbf[i]; touched_out = 1;
    :: else -> skip
    fi;
    if
    :: byfd == i -> byfd = nbf[i]; touched_out = 1;
    :: else -> skip
    fi;
    if :: nbf[i] != NIL -> pbf[nbf[i]] = pbf[i] :: else -> skip fi;
    pbf[i] = NIL; nbf[i] = NIL;

    /* (Heap omitted: the source's heap remove is an independent `touched`
     * source on the deadline path; this model targets the list/bucket surgery
     * and the total balance, so a deadline-less parker -- heap_index<0 -- is
     * the case that exercises link/unlink alone.) */

    if
    :: touched_out == 1 ->
#ifndef BUG_DOUBLE_DEC
        total  = total - 1;                 /* exactly-once per real removal  */
#endif
        ledger = ledger - 1;                /* surgery owns this -1           */
    :: else -> skip
    fi;
#ifdef BUG_DOUBLE_DEC
    /* BUG: decrement UNCONDITIONALLY, even when nothing was touched (a no-op
     * idempotent second unlink) -> total drifts below the surgery's ledger. */
    total = total - 1;
#endif
}

/* ===================== the pump's BOUNDED list walks ================== *
 * A cycle = a walk that cannot terminate.  We bound the walk at N steps; a
 * correct acyclic list of <= N nodes always terminates within N steps, so
 * exceeding the bound is a cycle == an assertion violation (the pump wedge). */
inline walk_global() {
    byte cur = head; byte steps = 0;
    do
    :: cur == NIL -> break
    :: cur != NIL ->
        assert(steps <= N);                 /* >N steps on <=N nodes == cycle */
        steps = steps + 1;
        cur = nxt[cur];
    od;
}
inline walk_bucket() {
    byte cur = byfd; byte steps = 0;
    do
    :: cur == NIL -> break
    :: cur != NIL ->
        assert(steps <= N);
        steps = steps + 1;
        cur = nbf[cur];
    od;
}

/* ----- slot-pointer doubly-linked invariant over the global list ------- *
 * For the head node h: slot[h]==HEAD.  For every node c with a predecessor
 * (some p with nxt[p]==c): slot[c]==SN+p.  Verified structurally. */
inline check_slot_invariant() {
    if :: head != NIL -> assert(slot[head] == HEAD) :: else -> skip fi;
    byte a = 0;
    do
    :: a >= N -> break
    :: a < N ->
        if :: nxt[a] != NIL -> assert(slot[nxt[a]] == SN + a) :: else -> skip fi;
        a = a + 1;
    od;
}

/* ==================== ACTORS ========================================== */

/* The new parker: links at a (possibly reused) slot, then -- on some runs --
 * its own unlink path runs (wait_fd exit / pump claim handed the wake). */
byte newslot;          /* which node-slot the new parker occupies          */

proctype linker()
{
    byte tch;
    LOCK;
    do_link(newslot);
    UNLOCK;

    /* Optionally the new parker is later unlinked (its g resumed and exits
     * wait_fd, or force_unlink cleans it).  Idempotent + lock-held. */
    if
    :: LOCK; do_unlink(newslot, tch); UNLOCK
    :: skip                              /* stays linked (still parked)       */
    fi;
}

/* A concurrent unlink of the SAME new parker -- the residual M:N race where two
 * paths (pump + pending-bits / force_unlink) both try to clean one parker.
 * Idempotent: the second is a no-op that must NOT double-decrement. */
proctype racer()
{
    byte tch;
    LOCK; do_unlink(newslot, tch); UNLOCK;
}

/* The pump: walks pool->head and the per-fd bucket under the lock.  A cycle
 * wedges it (caught by the bounded-walk assert).  Also checks the slot
 * invariant and the surgery's total balance (lock held). */
proctype pump()
{
    LOCK;
    walk_global();                          /* ACYCLIC GLOBAL                 */
    walk_bucket();                          /* ACYCLIC BUCKET                 */
    check_slot_invariant();                 /* SLOT INVARIANT                 */
    /* BALANCED TOTAL: the surgery's accounting is exact.  `ledger` = baseline
     * + (#links - #touched-unlinks); total must equal it under all paths.
     * This is independent of the prior-life skew baked into `baseline`. */
    assert(total == ledger);
    assert(total >= 0);                     /* never go negative              */
    UNLOCK;
}

/* ==================== THE MISSED-UNLINK SETUP ========================= *
 * Initialise the pool so a PRIOR occupant q LEFT A STALE REFERENCE, then the
 * stack is reused for the new parker.  Nondeterministic worst cases:
 *
 *  (A) self-ref alias: q used slot 0 and MISSED BOTH unlinks, so pool->head ==
 *      by_fd[fd] == 0 and total counts q.  The new parker p reuses slot 0.
 *      This is the documented 1-cycle hazard the stale-clear defends.
 *
 *  (B) cross-chain bucket residual: a DIFFERENT live node r (slot 1) is
 *      genuinely linked and is the global head, but a MISSED BUCKET unlink of
 *      the prior occupant q (slot 0) left the bucket head naming slot 0, with
 *      q's nbf chaining to r -- a bucket entry pointing at a node ALSO
 *      reachable from a different chain.  The new parker p reuses slot 0.  This
 *      is the exact "bucket points to us but we also have a predecessor from a
 *      different chain" the unlink header comment warns about.
 *
 *  (C) clean reuse baseline: pool empty, slot 0 fresh.
 */
init {
    /* all link fields nil, pool empty */
    nxt[0]=NIL; nxt[1]=NIL; slot[0]=NIL; slot[1]=NIL;
    nbf[0]=NIL; nbf[1]=NIL; pbf[0]=NIL; pbf[1]=NIL;
    head=NIL; byfd=NIL; total=0;

    if
    /* ---- (A) self-ref alias: q used slot 0, missed BOTH unlinks ---- */
    :: newslot = 0;
       head = 0; byfd = 0;          /* pool still names q's slot         */
       total = 1; baseline = 1;     /* the missed-unlink over-count       */
       nxt[0]=NIL; slot[0]=HEAD; nbf[0]=NIL; pbf[0]=NIL;   /* q's prior chain */

    /* ---- (B) cross-chain: r (slot 1) genuinely linked; q's bucket missed --- */
    :: newslot = 0;
       /* r is the genuine head of the global list */
       head = 1; slot[1]=HEAD; nxt[1]=NIL;
       /* a missed BUCKET unlink of q (slot 0) left the bucket head naming q,
        * with q chaining forward to r -- the cross-chain residual. */
       byfd = 0; nbf[0] = 1; pbf[1] = 0; pbf[0] = NIL;
       total = 2; baseline = 2;     /* r (real) + q (the missed-unlink skew) */

    /* ---- (C) clean reuse baseline ---- */
    :: newslot = 0;
       head = NIL; byfd = NIL; total = 0; baseline = 0;
    fi;

    ledger = baseline;              /* surgery starts from the injected count */

    atomic {
        run linker();
        run racer();
        run pump();
        run pump();             /* two pumps walk concurrently               */
    }
}
