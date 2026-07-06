/* rl_handle.h -- generation-stamped handle substrate (item 3).
 *
 * The object-reuse / ABA class (10 bugs in the history: stale-wake UAF, parker
 * cycles, chan-waiter reuse, kernel writing through a freed stack op record) all
 * share one shape: a raw pointer to a recyclable object is dereferenced AFTER the
 * object was freed and its memory reused.  This substrate makes that shape
 * inexpressible: a party holds a {slot, generation} HANDLE, not a pointer, and
 * to touch the object it must PIN it (a generation-checked refcount upgrade,
 * runloom's try_incref discipline).  A pin of a stale handle (released, or its
 * slot reused) fails -> NULL, never the wrong object; and while pinned the object
 * cannot be reclaimed, so the deref is UAF-safe.
 *
 *   owner:     h = rl_handle_register(obj);  ... ; rl_handle_release[_wait](h);
 *   resolver:  obj = rl_handle_pin(h);  if (obj) { use obj; rl_handle_unpin(h); }
 *
 * A 64-bit handle packs (generation:32 | slot:32).  Per-slot state packs
 * (generation:32 | refcount:32) in one atomic so a pin is a single CAS.  register
 * takes the owner's ref (rc=1); pin CAS-increments IFF gen matches and rc>0;
 * unpin/release decrement; the last drop RECLAIMS (bumps the generation so every
 * outstanding handle mismatches, then frees via the optional free_fn / recycles
 * the slot).  A STACK-owned object uses rl_handle_release_wait, which drops the
 * owner ref then spins until no resolver is pinned -- so the frame is safe to
 * abandon (this generalises io_uring's cancel-and-wait at zero kernel cost).
 *
 * Proven by tools/verify/cbmc/rl_handle_cbmc.c and tortured by
 * tests/tests_c/test_rl_handle.c (ASan/TSan).  RL_HANDLE_NULL (0) is never valid.
 * Plain C99. */
#ifndef RL_HANDLE_H
#define RL_HANDLE_H

#include <stdint.h>

typedef uint64_t rl_handle_t;
#define RL_HANDLE_NULL ((rl_handle_t)0)

/* Register `ptr` (non-NULL), taking the owner's reference.  `free_fn` (may be
 * NULL for stack-owned objects) is called on `ptr` when the LAST reference is
 * dropped -- deferred reclamation for heap objects.  Returns a fresh handle, or
 * RL_HANDLE_NULL if the table is full. */
rl_handle_t rl_handle_register(void *ptr, void (*free_fn)(void *));

/* Pin: resolve `h` to its object AND hold a reference so it cannot be reclaimed
 * until unpinned.  Returns the object, or NULL if `h` is stale/invalid.  The
 * returned pointer is safe to dereference until rl_handle_unpin.  Lock-free. */
void *rl_handle_pin(rl_handle_t h);

/* Drop a pin (paired with a successful rl_handle_pin).  Reclaims the object if
 * this was the last reference. */
void rl_handle_unpin(rl_handle_t h);

/* Drop the OWNER's reference.  Reclaims immediately if no resolver is pinned;
 * otherwise the last unpin reclaims.  For a HEAP object with a free_fn, this is
 * all the owner does. */
void rl_handle_release(rl_handle_t h);

/* Drop the owner's reference, then spin until the object is fully reclaimed (no
 * resolver still pinned).  For a STACK-owned object: on return the frame is safe
 * to abandon.  free_fn should be NULL for these. */
void rl_handle_release_wait(rl_handle_t h);

/* Diagnostics: live (registered, not-yet-reclaimed) count, for a leak gauge. */
long rl_handle_live_count(void);

#endif /* RL_HANDLE_H */
