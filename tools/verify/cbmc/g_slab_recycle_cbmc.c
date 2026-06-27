/* SOURCE-ANCHOR: runloom_g_slab_alloc runloom_g_slab_free  (guards this hand-model vs src drift; tools/verify/model_source_drift.py) */
/*
 * g_slab_recycle_cbmc.c -- CBMC layout proof for the runloom_g_t slab-recycle
 * field scrub (runloom_g_slab_alloc reuse path, runloom_sched_core.c.inc ~449-484).
 *
 * FAITHFUL SLICE (not byte-shared -- runloom_sched.c pulls in Python.h).  The
 * reuse path clears a recycled g in TWO memsets that straddle the load-bearing
 * atomic `state` byte (so a concurrent registry dump's atomic load of `state` is
 * never torn by a non-atomic memset):
 *
 *   memset(g, 0, offsetof(runloom_g_t, state));                 // part 1: [0, state)
 *   atomic_store(&g->state, RUNLOOM_GST_INIT);                  // the state byte
 *   memset((char*)g + offsetof(runloom_g_t, arena), 0,         // part 2: [arena, id)
 *          offsetof(runloom_g_t, id) - offsetof(runloom_g_t, arena));
 *
 * LIFE-CYCLE invariant being proved (the recycled-g field-clear contract that
 * runloom_sched.h documents): EVERY byte in [0, offsetof(id)) is zeroed-or-
 * overwritten by the scrub -- no recycled g carries a stale value in the
 * pre-introspection region.  This already produced one real wrong-result bug
 * (a recycled g kept a stale `pass_index` from a prior fiber_n(indexed=True) and
 * mis-called fn(stale_index) instead of fn()).  The contract holds IFF part 2's
 * start (offsetof(arena)) is immediately after the state byte -- i.e. ANY field
 * inserted in the [state, arena) gap leaks: part 1 stops at `state`, part 2
 * starts at `arena`, and the gap is never cleared.  This harness is the
 * DRIFT-GUARD for that gap as fields are added to the struct.
 *
 * The field SEQUENCE from `state` through `id` is reproduced verbatim from
 * runloom_sched.h so the relative offsets (and the gap risk) are faithful; the
 * pre-`state` fields are abstracted to a `prefix[]` (part 1 covers them whole).
 * Keep in sync with runloom_sched.h -- the run script drift-guards the field list.
 *
 * Negative control (must FAIL = CBMC finds the leak):
 *   -DBUG_GAP_AFTER_STATE : insert a field between `state` and `arena` -> a gap
 *                           the two memsets miss -> a stale (sentinel) byte
 *                           survives in [0, offsetof(id)).
 */

#include <stddef.h>
#include <string.h>

#define SENTINEL    0xAAu     /* pre-fill: any non-zero, != GST_INIT */
#define GST_INIT    1u        /* runloom_gstate.h: RUNLOOM_GST_INIT */

struct gon_batch;             /* opaque (only its pointer's size matters) */

/* Faithful slice of runloom_g_t from `state` through the introspection block.
 * prefix[] stands for every field before `state` (part 1 clears them en masse). */
typedef struct {
    char prefix[64];                  /* coro/callable/.../wake_state/park_hub/... */

    unsigned char state;              /* load-bearing atomic byte (between the memsets) */

#ifdef BUG_GAP_AFTER_STATE
    int inserted_field;               /* a NEW field dropped into the [state,arena) gap */
#endif

    /* ---- part-2 region: [arena, id), cleared by the second memset ---- */
    unsigned char arena;
    struct gon_batch *batch;
    unsigned char pass_index;
    unsigned char wait_reason;
    unsigned char wait_reason_hint;

    /* ---- introspection block: PRESERVED across recycle (NOT cleared) ---- */
    long long id;
    long long state_since_ns;
    int park_fd;
    int park_events;
    unsigned char limit_counted;
    void *reg_prev;
    void *reg_next;
} g_t;

/* Exact reproduction of the reuse-path scrub, on a byte buffer. */
static void slab_recycle_scrub(unsigned char *g)
{
    memset(g, 0, offsetof(g_t, state));                       /* part 1: [0, state) */
    g[offsetof(g_t, state)] = (unsigned char)GST_INIT;        /* the state byte */
    memset(g + offsetof(g_t, arena), 0,                       /* part 2: [arena, id) */
           offsetof(g_t, id) - offsetof(g_t, arena));
}

int main(void)
{
    unsigned char buf[sizeof(g_t)];
    size_t i;

    /* A recycled g arrives holding ARBITRARY prior-incarnation bytes; model the
     * worst case as an all-sentinel fill, so any surviving sentinel == a leaked
     * stale byte. */
    memset(buf, SENTINEL, sizeof buf);

    slab_recycle_scrub(buf);

    /* Diagnostic: part 2's start must sit immediately after the state byte, or
     * there is a gap the scrub cannot reach. */
    __CPROVER_assert(offsetof(g_t, arena) == offsetof(g_t, state) + 1,
                     "part-2 start (arena) immediately follows the state byte -- no gap");

    /* The contract: every byte before the (preserved) introspection block is
     * cleared or overwritten -- no stale byte survives recycling. */
    for (i = 0; i < offsetof(g_t, id); i++) {
        __CPROVER_assert(buf[i] != (unsigned char)SENTINEL,
                         "every pre-id byte is cleared/overwritten by the recycle scrub");
    }

    /* And the state byte is the freshly-stored INIT, not a stale value. */
    __CPROVER_assert(buf[offsetof(g_t, state)] == (unsigned char)GST_INIT,
                     "state byte re-initialised to GST_INIT between the two memsets");

    return 0;
}
