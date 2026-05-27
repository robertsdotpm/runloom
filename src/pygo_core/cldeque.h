/* cldeque.h -- Chase-Lev work-stealing deque.
 *
 * Single-owner / multi-thief.  Owner does push/pop at the bottom
 * without any synchronization in the common path; thieves CAS the
 * top.  Items are void* (we put pygo_g_t* in there).
 *
 * Reference: Chase & Lev, "Dynamic Circular Work-Stealing Deque"
 * SPAA 2005.  Implementation uses C11-style atomics via gcc/clang
 * __atomic_* builtins (works on MSVC too via /experimental:c11atomics
 * or our fallback path).
 *
 * Capacity is fixed at construction (we use 4096 — enough for very
 * deep ready queues; growable if needed later).
 */
#ifndef PYGO_CLDEQUE_H
#define PYGO_CLDEQUE_H

#include "compat.h"

#define PYGO_CLDEQUE_CAP 4096
#define PYGO_CLDEQUE_MASK (PYGO_CLDEQUE_CAP - 1)

typedef struct pygo_cldeque {
    /* top: head of the deque, incremented by thieves on steal.
     * bottom: tail, incremented by owner on push, decremented on pop. */
    volatile long top;
    volatile long bottom;
    void *buf[PYGO_CLDEQUE_CAP];
} pygo_cldeque_t;

void pygo_cldeque_init(pygo_cldeque_t *d);

/* Owner-only.  Push onto the bottom.  Returns 0 on success,
 * -1 if the deque is full. */
int pygo_cldeque_push(pygo_cldeque_t *d, void *item);

/* Owner-only.  Pop from the bottom.  Returns NULL if empty.
 * May race a steal -- resolved via CAS. */
void *pygo_cldeque_pop(pygo_cldeque_t *d);

/* Thief.  Pop from the top.  Returns NULL if empty / racing-conflict. */
void *pygo_cldeque_steal(pygo_cldeque_t *d);

/* Approximate size (snapshot, may be stale under concurrent ops). */
long pygo_cldeque_size(const pygo_cldeque_t *d);

#endif /* PYGO_CLDEQUE_H */
