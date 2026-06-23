/*
 * io_classify_cbmc.c -- CBMC proof of the I/O-return classifier FSM
 * (src/runloom_c/runloom_io_fsm.h), the (rc, errno) -> event mapping every
 * cooperative I/O syscall routes through.
 *
 * Two properties, proved over ALL (call, rc, errno) -- rc and errno fully
 * symbolic, so every sign of rc and every errno value is explored:
 *
 *   TOTALITY      -- for any valid call kind, runloom_io_classify always returns
 *                    an IN-RANGE event [0, RUNLOOM_IO_EVENT_COUNT).  No input
 *                    escapes unclassified; the classifier never falls through to
 *                    its runloom_fsm_violation abort for a real call kind.
 *   MASK-SOUNDNESS-- the returned event is always one the acting call kind may
 *                    emit (it is set in runloom_io_call_events[call]).  This is
 *                    the load-bearing link between the classifier and a consumer
 *                    switch: a site that handles exactly its kind's mask can
 *                    never be handed an event outside it.
 *
 * The classifier under test is the REAL header inline function (verified
 * unmodified).  Teeth perturb its OUTPUT to show each property is non-vacuous:
 *   -DBUG_SEND_EOF  -- a SEND yields EOF (not in SEND's mask) -> MASK fails.
 *   -DBUG_OOR       -- yields an out-of-range event           -> TOTALITY fails.
 * Both MUST report VERIFICATION FAILED.
 *
 * Run via verify/run_verify.sh (cbmc), or directly:
 *   cbmc io_classify_cbmc.c -I ../../src/runloom_c
 *   cbmc io_classify_cbmc.c -I ../../src/runloom_c -DBUG_SEND_EOF   (expect FAILED)
 */

#include "runloom_io_fsm.h"

long long nondet_llong(void);
int       nondet_int(void);

static runloom_io_event_t
classify_under_test(runloom_io_call_t call, long long rc, int err)
{
    runloom_io_event_t ev = runloom_io_classify(call, rc, err);
#ifdef BUG_SEND_EOF
    if (call == RUNLOOM_IO_SEND && ev == RUNLOOM_IO_PROGRESS)
        ev = RUNLOOM_IO_EOF;          /* EOF is NOT in SEND's mask */
#endif
#ifdef BUG_OOR
    if (ev == RUNLOOM_IO_ERROR)
        ev = (runloom_io_event_t)RUNLOOM_IO_EVENT_COUNT;  /* out of range */
#endif
    return ev;
}

int main(void)
{
    int       call = nondet_int();
    long long rc   = nondet_llong();
    int       err  = nondet_int();

    /* Only valid call kinds reach the classifier in practice (the call kind is a
     * compile-time constant at every site); rc and errno stay fully symbolic. */
    __CPROVER_assume(call >= 0 && call < RUNLOOM_IO_CALL_COUNT);

    runloom_io_event_t ev = classify_under_test((runloom_io_call_t)call, rc, err);

    /* TOTALITY: the result is a real, in-range event. */
    __CPROVER_assert(ev >= 0 && ev < RUNLOOM_IO_EVENT_COUNT,
                     "classify returns an in-range event (totality)");

    /* MASK-SOUNDNESS: the result is an event this call kind may emit. */
    __CPROVER_assert((runloom_io_call_events[call] & RUNLOOM_IO_BIT(ev)) != 0u,
                     "classify result is within the call kind's event mask");

    return 0;
}
