/*
 * fiber_admit_cbmc.c -- CBMC conservation proof for runloom's max-fibers admission
 * gate (runloom_fiber_admit / runloom_fiber_release in runloom_introspect.c, driven
 * by the spawn exit paths in mn_sched_init_fini.c.inc runloom_mn_go_core).
 *
 * FAITHFUL SLICE.  admit() returns 0 (rejected, over the limit -- and BACKS OUT the
 * speculative increment), 1 (admitted, NO limit active -> not counted), or 2
 * (admitted AND counted -> the caller sets g->limit_counted so the g's final decref
 * releases the slot).  The spawn has FIVE exit paths, each of which must balance the
 * slot exactly once: rejected (admit's own back-out), uncounted (never touched),
 * coro_new fail (release iff counted), per-g tstate_new fail (release iff counted),
 * and success -> completion (release iff counted).
 *
 * Several fibers are IN FLIGHT at once (a slot pool), so the live count actually
 * reaches the cap and the rejection path is exercised -- the regime where a missed
 * back-out leaks a slot.
 *
 * LIFE-CYCLE invariants (Tier-2 #7):
 *   CONSERVATION -- across any interleaving of spawn/complete, every counted admit is
 *                   released EXACTLY once: at quiescence live_g == 0 (no leaked slot
 *                   that ratchets the cap down to a "fiber limit exceeded" hang with
 *                   zero live fibers; no double-release that underflows the counter).
 *   GATE         -- a counted admit never takes the live count above the limit.
 *
 * Negative controls (must FAIL = CBMC finds the imbalance):
 *   -DBUG_NO_BACKOUT     : an over-limit admit forgets to back out its speculative
 *                          increment -> live_g leaks up -> CONSERVATION fails (and the
 *                          cap permanently shrinks).
 *   -DBUG_DOUBLE_RELEASE : the success path releases WITHOUT the limit_counted check
 *                          -> an uncounted (adm==1) fiber releases a slot it never
 *                          took -> live_g underflows.
 *   -DBUG_BULK_COUNTED   : a bulk go_n fiber (never admitted) is marked counted and
 *                          releases on completion -> a phantom release -> underflow.
 */

extern _Bool nondet_bool(void);
extern long nondet_long(void);
extern int  nondet_int(void);

static long max_g;       /* the configured limit (0 = unlimited) */
static long live_g;      /* admitted-but-not-yet-released counted fibers */

/* slot state: 0 empty, 1 counted-alive, 2 uncounted-alive, 3 bulk-alive */
#define SLOTS   3
#define NSTEPS  5
static int slot[SLOTS];

/* runloom_fiber_admit: 0 rejected, 1 admitted-uncounted, 2 admitted-counted. */
static int fiber_admit(void)
{
    if (max_g <= 0) return 1;
    live_g++;
    if (live_g > max_g) {
#ifndef BUG_NO_BACKOUT
        live_g--;                /* back out the speculative increment */
#endif
        return 0;
    }
    /* GATE: a counted admit never leaves the live count above the limit. */
    __CPROVER_assert(live_g <= max_g, "counted admit keeps live_g <= max (the gate)");
    return 2;
}

static void fiber_release(void) { live_g--; }

static int find_empty(void) {
    for (int i = 0; i < SLOTS; i++) if (slot[i] == 0) return i;
    return -1;
}

/* Begin a spawn lifecycle (faithful to runloom_mn_go_core's pre-run exit paths). */
static void start_fiber(void)
{
    int i = find_empty();
    if (i < 0) return;

    if (nondet_bool()) {                      /* a bulk go_n fiber: NEVER admitted */
        slot[i] = 3;
        return;
    }

    int adm = fiber_admit();
    if (adm == 0) return;                     /* rejected: admit already backed out */
    int limit_counted = (adm == 2);

    if (nondet_bool()) {                      /* coro_new OR tstate_new failed mid-spawn */
        if (limit_counted) fiber_release();
        return;                               /* slot never occupied */
    }
    slot[i] = (adm == 2) ? 1 : 2;             /* alive: counted or uncounted */
}

/* Complete an in-flight fiber: its final decref releases the slot iff counted. */
static void finish_slot(int i)
{
    if (slot[i] == 1) {                        /* counted */
#ifdef BUG_DOUBLE_RELEASE
        fiber_release();                       /* (the success path here is correct;
                                                * the bug is modelled on slot==2 below) */
#else
        fiber_release();
#endif
    } else if (slot[i] == 2) {                 /* uncounted (adm==1) */
#ifdef BUG_DOUBLE_RELEASE
        fiber_release();                       /* BUG: releases a slot it never took */
#endif
    } else if (slot[i] == 3) {                 /* bulk go_n */
#ifdef BUG_BULK_COUNTED
        fiber_release();                       /* BUG: arena g wrongly counted */
#endif
    }
    slot[i] = 0;
}

static void finish_fiber(void)
{
    for (int i = 0; i < SLOTS; i++) {
        if (slot[i] != 0 && nondet_bool()) { finish_slot(i); return; }
    }
}

int main(void)
{
    long m = nondet_long();
    __CPROVER_assume(m >= 0 && m <= 2);       /* unlimited, or a small cap */
    max_g  = m;
    live_g = 0;
    for (int i = 0; i < SLOTS; i++) slot[i] = 0;

    for (int s = 0; s < NSTEPS; s++) {
        if (nondet_bool()) start_fiber();
        else               finish_fiber();
    }

    /* drain every still-in-flight fiber */
    for (int i = 0; i < SLOTS; i++)
        if (slot[i] != 0) finish_slot(i);

    /* CONSERVATION: every counted admit released exactly once -> back to zero. */
    __CPROVER_assert(live_g == 0,
        "at quiescence live_g == 0 (every admitted-counted fiber released exactly once)");
    return 0;
}
