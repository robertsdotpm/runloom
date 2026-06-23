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
 *
 * INVARIANTS (over a nondeterministic snap -> {load XOR clear} lifecycle):
 *   BALANCED  -- each owned field: acquired == released (exactly once), nothing left
 *                held at the end (no leak, no over-release).
 *   IMMORTAL  -- an immortal context's refcount is NEVER changed (acquire and
 *                release both no-op).
 *   RAW-CHAIN -- delete_later is NEVER refcounted (acquired == released == 0).
 *
 * Negative controls (must FAIL):
 *   -DBUG_LOAD_FORGETS_FIELD     : load releases all but one field -> that ref leaks.
 *   -DBUG_INCREF_IMMORTAL        : snap increfs the context even when immortal ->
 *                                  load's Py_XDECREF no-ops on it -> the taken ref
 *                                  can never be released -> leak.
 *   -DBUG_DELETE_LATER_REFCOUNTED: snap refcounts the raw delete_later chain.
 */

extern _Bool nondet_bool(void);

#define NF   5                 /* CTX, EXCV, CUREXC, PROF, TRC */
#define CTX  0                 /* the context field -- can be immortal */

static int acquired[NF];       /* refs snap took on this field */
static int released[NF];       /* refs load/clear gave back */
static int held[NF];           /* refs the snap currently holds */
static int immortal_ctx;       /* nondet: is the saved context immortal? */
static int del_refops;         /* any refcount op on delete_later (must stay 0) */

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
}

/* runloom_pystate_snap_clear: g died while parked -- Py_CLEAR every held ref. */
static void pystate_snap_clear(void)
{
    for (int f = 0; f < NF; f++)
        release_field(f);
    /* delete_later: dropped as a raw pointer (snap->delete_later = NULL); no decref. */
}

int main(void)
{
    immortal_ctx = nondet_bool() ? 1 : 0;
    for (int f = 0; f < NF; f++) { acquired[f] = released[f] = held[f] = 0; }
    del_refops = 0;

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
    return 0;
}
