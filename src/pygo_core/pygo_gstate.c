/* pygo_gstate.c -- observational g-state machine implementation.
 * See pygo_gstate.h for the contract. */

#if !defined(_WIN32)
#  define _POSIX_C_SOURCE 200809L
#endif
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "pygo_gstate.h"
#include "pygo_sched.h"
#include "pygo_diag.h"

#include <stdio.h>
#include <stdlib.h>

/* The state field lives on pygo_g_t.  We access it via a helper
 * because pygo_sched.h doesn't pull pygo_gstate.h (avoids include
 * cycle pygo_sched.h<->pygo_gstate.h).  pygo_g_t::state is the new
 * field added in this commit. */

void pygo_g_state_set(struct pygo_g *g, pygo_g_state_t to)
{
    pygo_g_state_t prev;
    if (g == NULL) return;
    prev = (pygo_g_state_t)__atomic_load_n(&g->state, __ATOMIC_ACQUIRE);
    __atomic_store_n(&g->state, (unsigned char)to, __ATOMIC_RELEASE);
    PYGO_EVT(PYGO_EVT_G_TRANSITION, g, NULL,
             ((long long)prev << 8) | (long long)to);
}

int pygo_g_state_cas(struct pygo_g *g, pygo_g_state_t from, pygo_g_state_t to)
{
    unsigned char expected = (unsigned char)from;
    if (g == NULL) return 0;
    if (__atomic_compare_exchange_n(&g->state,
                                    &expected, (unsigned char)to,
                                    0,    /* strong CAS */
                                    __ATOMIC_ACQ_REL,
                                    __ATOMIC_ACQUIRE)) {
        PYGO_EVT(PYGO_EVT_G_TRANSITION, g, NULL,
                 ((long long)from << 8) | (long long)to);
        return 1;
    }
    if (PYGO_DBG_ON(PYGO_DBG_GSTATE)) {
        fprintf(stderr,
                "[pygo-gstate] CAS failed: g=%p expected=%d actual=%d to=%d\n",
                (const void *)g, (int)from, (int)expected, (int)to);
    }
    return 0;
}

int pygo_g_state_in(const struct pygo_g *g, unsigned int mask)
{
    unsigned char s;
    if (g == NULL) return 0;
    s = __atomic_load_n(&((struct pygo_g *)g)->state, __ATOMIC_ACQUIRE);
    return (mask & (1u << (unsigned)s)) != 0;
}

pygo_g_state_t pygo_g_state_get(const struct pygo_g *g)
{
    if (g == NULL) return PYGO_GST_FREED;
    return (pygo_g_state_t)__atomic_load_n(
        &((struct pygo_g *)g)->state, __ATOMIC_ACQUIRE);
}

void pygo_g_assert_failure_(const struct pygo_g *g, unsigned int mask,
                            const char *file, int line)
{
    unsigned char s = g ? __atomic_load_n(
        &((struct pygo_g *)g)->state, __ATOMIC_ACQUIRE) : (unsigned char)PYGO_GST_FREED;
    fprintf(stderr,
            "[pygo-gstate] ASSERT FAILED at %s:%d: g=%p state=%d "
            "(forbidden by mask=0x%x)\n",
            file, line, (const void *)g, (int)s, mask);
    /* Dump the ring so we can see what led here. */
    pygo_diag_dump(-1);
    abort();
}
