/* cldeque.h -- Chase-Lev work-stealing deque.
 *
 * Single-owner / multi-thief.  Owner does push/pop at the bottom
 * without any synchronization in the common path; thieves CAS the
 * top.  Items are void* (we put runloom_g_t* in there).
 *
 * Reference: Chase & Lev, "Dynamic Circular Work-Stealing Deque"
 * SPAA 2005.  Implementation uses C11-style atomics via gcc/clang
 * __atomic_* builtins (works on MSVC too via /experimental:c11atomics
 * or our fallback path).
 *
 * Capacity is fixed at construction (we use 4096 — enough for very
 * deep ready queues; growable if needed later).
 */
#ifndef RUNLOOM_CLDEQUE_H
#define RUNLOOM_CLDEQUE_H

#include "compat.h"

/* Capacity is overridable at compile time (must stay a power of two) so
 * a bounded model checker (tools/verify/cbmc) can verify a small instance of
 * the algorithm quickly.  The production default is 4096. */
#ifndef RUNLOOM_CLDEQUE_CAP
#  if defined(RUNLOOM_SHRINK)
#    define RUNLOOM_CLDEQUE_CAP 8   /* test-shrink: fill after 8 fresh gs -> hit the full-deque fallback + steal collisions every few ops */
#  else
#    define RUNLOOM_CLDEQUE_CAP 4096
#  endif
#endif
#define RUNLOOM_CLDEQUE_MASK (RUNLOOM_CLDEQUE_CAP - 1)

typedef struct runloom_cldeque {
    /* top: head of the deque, incremented by thieves on steal.
     * bottom: tail, incremented by owner on push, decremented on pop. */
    volatile long top;
    volatile long bottom;
    void *buf[RUNLOOM_CLDEQUE_CAP];
} runloom_cldeque_t;

void runloom_cldeque_init(runloom_cldeque_t *d);

/* Owner-only.  Push onto the bottom.  Returns 0 on success,
 * -1 if the deque is full. */
int runloom_cldeque_push(runloom_cldeque_t *d, void *item);

/* Owner-only.  Pop from the bottom.  Returns NULL if empty.
 * May race a steal -- resolved via CAS. */
void *runloom_cldeque_pop(runloom_cldeque_t *d);

/* Thief.  Pop from the top.  Returns NULL if empty / racing-conflict. */
void *runloom_cldeque_steal(runloom_cldeque_t *d);

/* Approximate size (snapshot, may be stale under concurrent ops). */
long runloom_cldeque_size(const runloom_cldeque_t *d);

#endif /* RUNLOOM_CLDEQUE_H */
