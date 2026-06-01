/*
 * sched_pystate_cbmc.c -- CBMC harness for pygo's per-goroutine tstate
 * save/restore (pygo_sched.c: pygo_pystate_snap / pygo_pystate_load), which
 * swaps a slice of PyThreadState (c_recursion_remaining, datastack_chunk /
 * datastack_top / datastack_limit, current_frame) in and out as goroutines are
 * context-switched on one OS thread.
 *
 * FAITHFUL-MODEL (not byte-shared).  The real snap/load are heavily
 * CPython-version-#ifdef'd and touch concrete PyThreadState pointers, so this
 * models the field SET as an abstract vector and the snap=copy-out /
 * load=copy-in logic.  It does NOT prove the CPython-internals fidelity; it
 * proves the property a model CAN: SAVE/RESTORE COMPLETENESS + CROSS-G
 * ISOLATION.  The context switch is cooperative (one g runs at a time on the
 * thread), so this is a bounded SEQUENTIAL proof over a nondeterministic switch
 * sequence.
 *
 * THE BUG CLASS it guards: as CPython adds tstate fields across versions, the
 * snap/load pair must stay matched -- a field saved but not restored (or added
 * to neither) leaks the PREVIOUSLY-running goroutine's value into the resumed
 * goroutine's tstate (stale datastack -> "error return without exception" / SEGV
 * -- exactly the failure mode the per-g tstate machinery exists to prevent).
 *
 * SPEC: after switch_to(g), every live tstate field equals g's OWN saved value
 * -- no field carries a stale value from the g that ran before it.
 *
 * Negative control (must FAIL):
 *   -DBUG_DROP_FIELD : load restores every field EXCEPT the last -> that field
 *                      leaks the prior goroutine's value across the switch.
 */

#define NFIELDS 5        /* c_recursion, datastack_chunk/top/limit, current_frame */
#define NG      3        /* goroutines sharing the OS thread's tstate */
#define NSW     5        /* context switches to explore */

extern int nondet_int(void);

/* the OS thread's live tstate slice (shared; one g's values at a time) */
static unsigned long tstate[NFIELDS];

/* per-g saved snapshot */
static unsigned long snap[NG][NFIELDS];

/* src: pygo_pystate_snap -- copy the live tstate fields OUT into g's snap */
static void pystate_snap(int g)
{
    for (int f = 0; f < NFIELDS; f++)
        snap[g][f] = tstate[f];
}

/* src: pygo_pystate_load -- copy g's snap back INTO the live tstate */
static void pystate_load(int g)
{
#ifdef BUG_DROP_FIELD
    for (int f = 0; f < NFIELDS - 1; f++)   /* forgets the last field */
#else
    for (int f = 0; f < NFIELDS; f++)
#endif
        tstate[f] = snap[g][f];
}

int main(void)
{
    /* Each g is "spawned" with a distinct signature in every field; its snap
     * starts holding that signature (its initial tstate). */
    for (int g = 0; g < NG; g++)
        for (int f = 0; f < NFIELDS; f++)
            snap[g][f] = (unsigned long)(g + 1) * 100u + (unsigned)f;

    int current = -1;                       /* nothing running yet */

    for (int step = 0; step < NSW; step++) {
        int g = nondet_int();
        __CPROVER_assume(g >= 0 && g < NG);

        /* the scheduler's switch: save the outgoing g, restore the incoming g */
        if (current >= 0)
            pystate_snap(current);
        pystate_load(g);
        current = g;

        /* On resume, tstate must hold g's OWN fields -- no leak from the g that
         * ran immediately before.  (g's expected per-field value is its
         * signature, since g only ever wrote its own signature.) */
        for (int f = 0; f < NFIELDS; f++)
            __CPROVER_assert(tstate[f] == (unsigned long)(g + 1) * 100u + (unsigned)f,
                             "resumed g sees its OWN tstate field (no cross-g leak)");

        /* g runs: it may rewrite its fields (idempotent here -- same signature),
         * modelling that a running g owns the live tstate. */
        for (int f = 0; f < NFIELDS; f++)
            tstate[f] = (unsigned long)(g + 1) * 100u + (unsigned)f;
    }
    return 0;
}
