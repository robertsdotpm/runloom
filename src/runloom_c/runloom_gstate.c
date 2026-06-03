/* runloom_gstate.c -- observational g-state machine implementation.
 * See runloom_gstate.h for the contract. */

#if !defined(_WIN32)
#  define _POSIX_C_SOURCE 200809L
#endif
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "runloom_gstate.h"
#include "runloom_sched.h"
#include "runloom_diag.h"
#include "runloom_introspect.h"

#include <stdio.h>
#include <stdlib.h>

/* The state field lives on runloom_g_t.  We access it via a helper
 * because runloom_sched.h doesn't pull runloom_gstate.h (avoids include
 * cycle runloom_sched.h<->runloom_gstate.h).  runloom_g_t::state is the new
 * field added in this commit. */

void runloom_g_state_set(struct runloom_g *g, runloom_g_state_t to)
{
    runloom_g_state_t prev;
    if (g == NULL) return;
    prev = (runloom_g_state_t)__atomic_load_n(&g->state, __ATOMIC_ACQUIRE);
    __atomic_store_n(&g->state, (unsigned char)to, __ATOMIC_RELEASE);
    runloom_introspect_note_transition(g, (unsigned int)to);
    RUNLOOM_EVT(RUNLOOM_EVT_G_TRANSITION, g, NULL,
             ((long long)prev << 8) | (long long)to);
}

int runloom_g_state_cas(struct runloom_g *g, runloom_g_state_t from, runloom_g_state_t to)
{
    unsigned char expected = (unsigned char)from;
    if (g == NULL) return 0;
    if (__atomic_compare_exchange_n(&g->state,
                                    &expected, (unsigned char)to,
                                    0,    /* strong CAS */
                                    __ATOMIC_ACQ_REL,
                                    __ATOMIC_ACQUIRE)) {
        runloom_introspect_note_transition(g, (unsigned int)to);
        RUNLOOM_EVT(RUNLOOM_EVT_G_TRANSITION, g, NULL,
                 ((long long)from << 8) | (long long)to);
        return 1;
    }
    if (RUNLOOM_DBG_ON(RUNLOOM_DBG_GSTATE)) {
        fprintf(stderr,
                "[runloom-gstate] CAS failed: g=%p expected=%d actual=%d to=%d\n",
                (const void *)g, (int)from, (int)expected, (int)to);
    }
    return 0;
}

int runloom_g_state_in(const struct runloom_g *g, unsigned int mask)
{
    unsigned char s;
    if (g == NULL) return 0;
    s = __atomic_load_n(&((struct runloom_g *)g)->state, __ATOMIC_ACQUIRE);
    return (mask & (1u << (unsigned)s)) != 0;
}

runloom_g_state_t runloom_g_state_get(const struct runloom_g *g)
{
    if (g == NULL) return RUNLOOM_GST_FREED;
    return (runloom_g_state_t)__atomic_load_n(
        &((struct runloom_g *)g)->state, __ATOMIC_ACQUIRE);
}

void runloom_g_assert_failure_(const struct runloom_g *g, unsigned int mask,
                            const char *file, int line)
{
    unsigned char s = g ? __atomic_load_n(
        &((struct runloom_g *)g)->state, __ATOMIC_ACQUIRE) : (unsigned char)RUNLOOM_GST_FREED;
    fprintf(stderr,
            "[runloom-gstate] ASSERT FAILED at %s:%d: g=%p state=%d "
            "(forbidden by mask=0x%x)\n",
            file, line, (const void *)g, (int)s, mask);
    /* Dump the ring so we can see what led here. */
    runloom_diag_dump(-1);
    abort();
}
