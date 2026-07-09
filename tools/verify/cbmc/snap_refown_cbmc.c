/* SOURCE-ANCHOR: runloom_pystate_snap runloom_pystate_load runloom_pystate_snap_clear  (guards this hand-model vs src drift; tools/verify/model_source_drift.py) */
/*
 * snap_refown_cbmc.c -- CBMC proof of the REFERENCE-OWNERSHIP discipline of
 * runloom's per-g tstate snapshot (runloom_pystate_snap / _load / _snap_clear in
 * runloom_sched_pystate.c.inc).  Companion to sched_pystate_cbmc.c, which proves
 * field-SET completeness + cross-g isolation and explicitly disclaims ownership
 * (Tier-1 #5; the layer the critic flagged as where the proof earns its keep).
 *
 * THE DISCIPLINE.  When a g parks, runloom_pystate_snap ACQUIRES a strong ref to
 * each owned execution-state field it stashes (context, exc_state.exc_value,
 * current_exception, c_profileobj, c_traceobj) so a sibling g can't free it during
 * the suspension.  That ref is RELEASED EXACTLY ONCE, via the snap's terminal
 * transition: either runloom_pystate_load (the g resumes -- the ref is dropped /
 * transferred back to the live tstate) XOR runloom_pystate_snap_clear (the g died
 * while parked -- Py_CLEAR drops it).  Two special cases are the bug-prone part:
 *   - IMMORTAL context: snap SKIPS the incref (an immortal's refcount is saturated;
 *     line ~72), so load/clear's Py_XDECREF must be a no-op for it too -- acquire
 *     and release must AGREE on whether a ref was taken.
 *   - delete_later: the trashcan deferred-dealloc chain is carried as a RAW pointer
 *     and must NEVER be Py_INCREF/Py_DECREF'd (its ob_tid is a heap next-pointer;
 *     refcounting it scrambles the dying-object chain -> UAF).
 *   - exc_owners[]: a VARIABLE-count lane (0..MAXOWN) of strong refs to the
 *     gen/coro/async-gen OWNERS of the per-g exception chain, pinned so a transient
 *     generator can't be freed while the fiber is parked mid-iteration (p69 residual
 *     UAF).  snap Py_INCREFs each owner it walks (src :208) ONLY on the branch that
 *     also saves a chain (exc_info != NULL, :171); the trivial branch (:167) saves
 *     no chain and pins nothing.  Released EXACTLY ONCE: load's non-default path
 *     drains them (:473-475) XOR snap_clear drains them (:538).  Load's DEFAULT path
 *     (exc_info == NULL, :444) sets exc_owner_count = 0 WITHOUT a decref (:450) --
 *     sound ONLY because of the coupling "no chain => 0 owners pinned"; if snap ever
 *     pinned owners without saving a chain, that path would LEAK them.
 *
 * INVARIANTS (over a nondeterministic snap -> {load XOR clear} lifecycle):
 *   BALANCED  -- each owned field: acquired == released (exactly once), nothing left
 *                held at the end (no leak, no over-release).  Includes the
 *                variable-count exc_owners lane.
 *   IMMORTAL  -- an immortal context's refcount is NEVER changed (acquire and
 *                release both no-op).
 *   RAW-CHAIN -- delete_later is NEVER refcounted (acquired == released == 0).
 *   EXC-COUPLING -- the load default path (which zeroes exc_owner_count without a
 *                decref) is reachable only when 0 owners were pinned, so it leaks
 *                nothing.
 *
 * Negative controls (must FAIL):
 *   -DBUG_LOAD_FORGETS_FIELD     : load releases all but one field -> that ref leaks.
 *   -DBUG_INCREF_IMMORTAL        : snap increfs the context even when immortal ->
 *                                  load's Py_XDECREF no-ops on it -> the taken ref
 *                                  can never be released -> leak.
 *   -DBUG_DELETE_LATER_REFCOUNTED: snap refcounts the raw delete_later chain.
 *   -DBUG_LOAD_FORGETS_EXC_OWNER : load's non-default path drains all but one
 *                                  exc_owner -> that ref leaks.
 *   -DBUG_SNAP_PINS_WITHOUT_CHAIN: snap pins an exc_owner but leaves exc_info NULL,
 *                                  so load's default path zeroes the count without
 *                                  releasing it -> leak (the coupling is broken).
 */

extern _Bool nondet_bool(void);
extern int   nondet_int(void);

#define NF   5                 /* CTX, EXCV, CUREXC, PROF, TRC */
#define CTX  0                 /* the context field -- can be immortal */

static int acquired[NF];       /* refs snap took on this field */
static int released[NF];       /* refs load/clear gave back */
static int held[NF];           /* refs the snap currently holds */
static int immortal_ctx;       /* nondet: is the saved context immortal? */
static int del_refops;         /* any refcount op on delete_later (must stay 0) */

/* exc_owners[] variable-count lane (0..MAXOWN). */
#define MAXOWN 3
static int own_acquired, own_released, own_held;
static int snap_has_chain;     /* snap saved a per-g exc chain (exc_info != NULL) */
static int own_count;          /* exc_owner_count snap pinned */

/* runloom_pystate_snap: take a strong ref to each owned field. */
static void pystate_snap(void)
{
    for (int f = 0; f < NF; f++) {
        int take = 1;                       /* Py_XINCREF (exc/cur_exc/prof/trc) */
        if (f == CTX) {
#ifdef BUG_INCREF_IMMORTAL
            take = 1;                       /* BUG: incref even an immortal context */
#else
            take = immortal_ctx ? 0 : 1;    /* immortal: assign WITHOUT incref */
#endif
        }
        acquired[f] += take;
        held[f]     += take;
    }
    /* delete_later: transferred RAW (ts->delete_later = NULL); NEVER refcounted. */
#ifdef BUG_DELETE_LATER_REFCOUNTED
    del_refops += 1;                         /* BUG: Py_XINCREF the dying chain */
#endif

    /* exc_owners: pin 0..MAXOWN gen/coro owners, ONLY on the save-a-chain branch. */
    snap_has_chain = nondet_bool() ? 1 : 0;
    if (snap_has_chain) {
        int n = nondet_int();
        __CPROVER_assume(n >= 0 && n <= MAXOWN);
        own_count = n;                       /* Py_INCREF each walked owner (src :208) */
    } else {
        own_count = 0;                       /* trivial branch (:167): no chain, no pins */
#ifdef BUG_SNAP_PINS_WITHOUT_CHAIN
        own_count = 1;                       /* BUG: pin an owner but leave exc_info NULL */
#endif
    }
    own_acquired += own_count;
    own_held     += own_count;
}

/* Effective Py_XDECREF of the ref snap holds on field f (no-op on an immortal). */
static void release_field(int f)
{
    int rel = held[f];
    if (f == CTX && immortal_ctx) rel = 0;   /* Py_XDECREF is a no-op on an immortal */
    released[f] += rel;
    held[f]     -= rel;
}

/* runloom_pystate_load: g resumes -- each held ref is dropped / transferred to ts. */
static void pystate_load(void)
{
    for (int f = 0; f < NF; f++) {
#ifdef BUG_LOAD_FORGETS_FIELD
        if (f == NF - 1) continue;           /* forgets to release the last field */
#endif
        release_field(f);
    }
#ifdef BUG_DELETE_LATER_REFCOUNTED
    del_refops += 1;
#endif
    /* exc_owners: NON-default path (chain saved) drains every pinned owner
     * (src :473-475); DEFAULT path (no chain) zeroes the count with NO decref
     * (:450) -- releasing nothing, which is sound iff none were held. */
    if (snap_has_chain) {
#ifdef BUG_LOAD_FORGETS_EXC_OWNER
        int rel = own_held > 0 ? own_held - 1 : 0;   /* BUG: leaves one pinned */
#else
        int rel = own_held;
#endif
        own_released += rel;
        own_held     -= rel;
    }
    /* else: default path -- exc_owner_count = 0, no release.  If own_held > 0 here
     * (the coupling was violated), the terminal held==0 assertion catches the leak. */
}

/* runloom_pystate_snap_clear: g died while parked -- Py_CLEAR every held ref. */
static void pystate_snap_clear(void)
{
    for (int f = 0; f < NF; f++)
        release_field(f);
    /* exc_owners: drain every pinned owner (src :538, unconditional while loop). */
    own_released += own_held;
    own_held      = 0;
    /* delete_later: dropped as a raw pointer (snap->delete_later = NULL); no decref. */
}

int main(void)
{
    immortal_ctx = nondet_bool() ? 1 : 0;
    for (int f = 0; f < NF; f++) { acquired[f] = released[f] = held[f] = 0; }
    del_refops = 0;
    own_acquired = own_released = own_held = 0;
    snap_has_chain = 0;
    own_count = 0;

    /* A parking g snaps its state... */
    pystate_snap();

    /* ...then EITHER resumes (load) XOR is destroyed while parked (clear). */
    if (nondet_bool()) pystate_load();
    else               pystate_snap_clear();

    /* BALANCED: every acquired ref released exactly once; nothing left held. */
    for (int f = 0; f < NF; f++) {
        __CPROVER_assert(acquired[f] == released[f],
                         "each owned snap field is released exactly once (load XOR clear)");
        __CPROVER_assert(held[f] == 0,
                         "no snap ref is left held at the snap's terminal transition");
    }

    /* IMMORTAL: an immortal context is never refcounted by snap/load/clear. */
    __CPROVER_assert(!immortal_ctx || (acquired[CTX] == 0 && released[CTX] == 0),
                     "an immortal context's refcount is never touched");

    /* RAW-CHAIN: delete_later is never Py_INCREF/Py_DECREF'd. */
    __CPROVER_assert(del_refops == 0,
                     "delete_later carried raw -- never refcounted");

    /* BALANCED (exc_owners lane): every pinned owner released exactly once, and the
     * load default path (which zeroes the count without a decref) leaks nothing. */
    __CPROVER_assert(own_acquired == own_released,
                     "each pinned exc_owner is released exactly once (load XOR clear)");
    __CPROVER_assert(own_held == 0,
                     "no exc_owner ref is left held at the snap's terminal transition");
    return 0;
}
