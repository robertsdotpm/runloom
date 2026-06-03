/* coro.h -- portable stackful coroutines.
 *
 * Three properties matter:
 *   1. Each pygo_coro owns its own C stack (heap-allocated, fixed at create).
 *   2. pygo_coro_resume / pygo_coro_yield transfer control with no syscall.
 *   3. The current coroutine pointer is thread-local, so multiple OS
 *      threads each run their own scheduler independently.
 *
 * Backend is chosen at compile time via plat.h:
 *   - Windows: ConvertThreadToFiber + CreateFiber + SwitchToFiber.
 *              Available since Windows 95.  Fastest + simplest on Win.
 *   - POSIX:   getcontext / makecontext / swapcontext.  POSIX.1-2001.
 *              Still works on every Unix we care about despite being
 *              deprecated by POSIX.1-2008.
 *
 * The public API is the same on both.
 */
#ifndef PYGO_CORO_H
#define PYGO_CORO_H

#include "compat.h"

typedef struct pygo_coro pygo_coro_t;

typedef void (*pygo_entry_fn)(void *user);

/* Lifecycle: returns NULL on alloc failure (errno set, or
 * GetLastError on Windows). */
pygo_coro_t *pygo_coro_new(size_t stack_size,
                           pygo_entry_fn entry,
                           void *user);

void pygo_coro_destroy(pygo_coro_t *c);

/* Switch into the coroutine.  Must be called from the same OS thread on
 * which pygo_coro_new was called.  Returns when the coroutine yields or
 * returns.  Calling resume on a done coroutine is undefined. */
void pygo_coro_resume(pygo_coro_t *c);

/* Yield from inside a coroutine.  Returns control to whatever called
 * pygo_coro_resume on us; on next resume, execution continues just
 * past the pygo_coro_yield call.  Calling yield from outside any
 * coroutine is undefined. */
void pygo_coro_yield(void);

/* Predicates. */
int pygo_coro_done(const pygo_coro_t *c);

/* This coro's stack size in bytes, or 0 if the backend has no
 * introspectable stack (Fibers).  Used by the goroutine dump. */
size_t pygo_coro_stack_size(const pygo_coro_t *c);

/* Backend identifier ("fibers", "ucontext"); useful for tests. */
const char *pygo_coro_backend(void);

/* Per-thread setup / teardown.  Must be called once per OS thread
 * before any coro on that thread.  Idempotent. */
int pygo_coro_thread_init(void);
void pygo_coro_thread_fini(void);

/* Pre-warm the stack pool with n pre-mmaped stacks of the given
 * size.  Eliminates the first-spawn mmap stall for servers that
 * know they're about to spawn a known number of goroutines.
 * No-op on the Fibers backend (CreateFiber handles its own pool).
 * No-op if n <= 0.  Returns the number actually pre-allocated. */
int pygo_coro_warmup(size_t stack_size, int n);

/* Drop the physical page frames of c's currently-idle (low) stack
 * region without releasing the stack -- the coro stays bound to its
 * goroutine.  The scheduler calls this when a g parks on a waiter
 * (netpoll/chan/sleep/park_safe); the next resume re-faults the few
 * touched pages (~one page fault).  MUST be called only while c is
 * SUSPENDED (so its saved stack pointer is valid).  No-op unless
 * PYGO_STACK_PARK_DONTNEED=1, and on backends without an inspectable
 * saved SP (ucontext / Fibers).
 *
 * M:N SAFETY: race-free against a concurrent resume even though a netpoll
 * parker is wakeable (commit==PARKED) before its yield returns control
 * here.  A pump on another hub only *claims + re-queues* the g; it never
 * resumes it.  The wake routes to the g's OWNING hub (netpoll.c:1693,
 * pygo_mn_wake_g(p->hub, ...)), and a woken g (snap.valid) lands in that
 * hub's LOCAL ready FIFO, which is never work-stolen (only the Chase-Lev
 * deque is stealable -- mn_sched.c:248-286).  So the sole thread that
 * resumes g is the same hub that runs this madvise at its post-resume
 * site: madvise happens-before the next resume on one thread, and no
 * other hub ever touches the stack.  PYGO_STACK_PARK_DONTNEED stays
 * default-OFF only for the throughput cost (madvise+refault per park
 * hurts short-park churn), not for safety; the path to default-ON is a
 * long-park heuristic that skips short parks.  See HANDOFF. */
void pygo_coro_park(pygo_coro_t *c);

/* Unconditional variant: madvise c's below-SP idle pages with no env
 * gate.  Used by the hub-idle dwell-based sweep (PYGO_STACK_PARK_SWEEP),
 * which does its own gating + threshold.  Same SUSPENDED + owning-hub
 * safety contract as pygo_coro_park. */
void pygo_coro_madvise_idle(pygo_coro_t *c);

/* ------------------------------------------------------------------ */
/* Stack-usage measurement (used by sched calibration)                */
/* ------------------------------------------------------------------ */

/* When painting is enabled, every pygo_coro_new paints the stack body
 * with a known sentinel pattern (8-byte chunks).  pygo_coro_scan_hwm
 * then walks low->high and reports how many bytes were actually
 * touched by the coroutine.
 *
 * Disable painting (e.g. after calibration) to drop the per-spawn
 * paint cost (~few µs at 256 KB). */
void pygo_coro_paint_set(int enabled);
int  pygo_coro_paint_enabled(void);

/* Opt-in security scrub of recycled goroutine stacks (default off). */
void pygo_coro_scrub_set(int enabled);
int  pygo_coro_scrub_enabled(void);

/* Returns the high-water mark in bytes (deepest write detected by
 * scanning for the sentinel).  Returns 0 if painting was disabled or
 * the coro hasn't been used.  Backend may return 0 on Fibers
 * (no introspectable stack). */
size_t pygo_coro_scan_hwm(pygo_coro_t *c);

#endif /* PYGO_CORO_H */
