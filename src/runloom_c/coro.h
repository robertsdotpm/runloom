/* coro.h -- portable stackful coroutines.
 *
 * Three properties matter:
 *   1. Each runloom_coro owns its own C stack (heap-allocated, fixed at create).
 *   2. runloom_coro_resume / runloom_coro_yield transfer control with no syscall.
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
#ifndef RUNLOOM_CORO_H
#define RUNLOOM_CORO_H

#include "compat.h"

typedef struct runloom_coro runloom_coro_t;

typedef void (*runloom_entry_fn)(void *user);

/* Lifecycle: returns NULL on alloc failure (errno set, or
 * GetLastError on Windows). */
runloom_coro_t *runloom_coro_new(size_t stack_size,
                           runloom_entry_fn entry,
                           void *user);

void runloom_coro_destroy(runloom_coro_t *c);

/* ---- bulk/arena fast path (go_n) ----
 * Placement coro init in caller-provided memory (>= runloom_coro_struct_size())
 * on a caller-provided stack: no malloc, no stack-acquire, no lock.  Do NOT
 * runloom_coro_destroy a placement coro -- the arena is reclaimed wholesale. */
runloom_coro_t *runloom_coro_init_at(void *mem, size_t stack_size, void *stack,
                                     runloom_entry_fn entry, void *user);
size_t runloom_coro_struct_size(void);
/* Carve one stack from the bulk arena (NULL if off/exhausted/size-mismatch). */
void *runloom_coro_arena_stack(size_t stack_size);
/* Release a bulk stack block (n slots from start_slot): MADV_DONTNEED the pages
 * (keeps the virtual reservation) AND return the slots to the allocator for
 * reuse.  Called by the go_n batch teardown.  No-op off-POSIX (slots still
 * returned). */
void runloom_coro_arena_release(size_t start_slot, long n);
/* Fill an entire coro arena (n structs) inline, one stack each from one reserved
 * arena block, and set each g's coro pointer (g_arena[i] + g_coro_off).  One
 * call for all N: zero per-g calls into the coro layer.  0 ok, -1 on arena
 * unavailable/exhausted (caller falls back to the per-g path). */
int runloom_coro_bulk_init(void *coro_arena, size_t coro_stride,
                           void *g_arena, size_t g_stride, size_t g_coro_off,
                           size_t stack_size, long n, runloom_entry_fn entry,
                           size_t *start_slot_out);

/* Switch into the coroutine.  Must be called from the same OS thread on
 * which runloom_coro_new was called.  Returns when the coroutine yields or
 * returns.  Calling resume on a done coroutine is undefined. */
void runloom_coro_resume(runloom_coro_t *c);

/* Yield from inside a coroutine.  Returns control to whatever called
 * runloom_coro_resume on us; on next resume, execution continues just
 * past the runloom_coro_yield call.  Calling yield from outside any
 * coroutine is undefined. */
void runloom_coro_yield(void);

/* Predicates. */
int runloom_coro_done(const runloom_coro_t *c);

/* This coro's stack size in bytes, or 0 if the backend has no
 * introspectable stack (Fibers).  Used by the fiber dump. */
size_t runloom_coro_stack_size(const runloom_coro_t *c);

/* Lowest usable byte of this coro's stack (the PROT_NONE guard page is the page
 * immediately below it), or NULL on backends with no introspectable stack.
 * Used by the crash handler to map a faulting address back to a fiber. */
void *runloom_coro_stack_base(const runloom_coro_t *c);

/* Size in bytes of the guard page below each coro stack (0 if the backend
 * installs no guard, e.g. Windows Fibers). */
size_t runloom_coro_guard_size(void);

/* Force park-time idle-page reclaim on/off programmatically (in addition to the
 * RUNLOOM_STACK_PARK_DONTNEED env).  The stack auto-sizer enables it so that
 * starting fibers large stays RSS-free. */
void runloom_coro_park_reclaim_set(int on);

/* Backend identifier ("fibers", "ucontext"); useful for tests. */
const char *runloom_coro_backend(void);

/* Per-thread setup / teardown.  Must be called once per OS thread
 * before any coro on that thread.  Idempotent. */
int runloom_coro_thread_init(void);
void runloom_coro_thread_fini(void);

/* Pre-warm the stack pool with n pre-mmaped stacks of the given
 * size.  Eliminates the first-spawn mmap stall for servers that
 * know they're about to spawn a known number of fibers.
 * No-op on the Fibers backend (CreateFiber handles its own pool).
 * No-op if n <= 0.  Returns the number actually pre-allocated. */
int runloom_coro_warmup(size_t stack_size, int n);

/* Prewarm `n` stacks into the GLOBAL depot (cross-hub, unlike warmup's per-thread
 * cache).  background=1 runs it on a detached OS thread and returns 0 immediately
 * (the spawn burst then pops instead of mmap'ing); background=0 runs synchronously
 * and returns the count retained (-1 if a background thread couldn't start).
 * Bounded by the depot cap (RUNLOOM_STACK_DEPOT_CAP) -- raise it near the target
 * for a large prewarm.  No-op on the Windows Fibers backend. */
int runloom_coro_prewarm(size_t stack_size, int n, int background);

/* CONTINUOUS prewarm daemon: keep the GLOBAL depot topped to `target` stacks so a
 * spawn burst always finds a ready backlog (it refills as the pool drains, idling
 * when full).  One daemon per process: _keep starts it or re-targets a running
 * one (target<=0 stops it); _stop halts + joins it.  Returns 0 ok / -1 if the
 * thread couldn't start.  No-op on Windows Fibers.  _reset_after_fork zeroes the
 * (copied, threadless) daemon state in a fork child. */
int  runloom_coro_prewarm_keep(size_t stack_size, int target);
void runloom_coro_prewarm_stop(void);
void runloom_coro_prewarm_reset_after_fork(void);

/* Depot auto-cap: the stack pool sizes itself to the live-stack high-water-mark
 * (no RUNLOOM_STACK_DEPOT_CAP needed).  _init resolves SAFE_MAX once (mn_init);
 * _tick decays the watermark + recomputes the cap (called once per sysmon tick);
 * _reset forgets the watermark (mn_fini + fork child). */
void runloom_stack_autocap_init(void);
void runloom_stack_autocap_tick(void);
void runloom_stack_autocap_reset(void);

/* Drop the physical page frames of c's currently-idle (low) stack
 * region without releasing the stack -- the coro stays bound to its
 * fiber.  The scheduler calls this when a g parks on a waiter
 * (netpoll/chan/sleep/park_safe); the next resume re-faults the few
 * touched pages (~one page fault).  MUST be called only while c is
 * SUSPENDED (so its saved stack pointer is valid).  No-op unless
 * RUNLOOM_STACK_PARK_DONTNEED=1, and on backends without an inspectable
 * saved SP (ucontext / Fibers).
 *
 * M:N SAFETY: race-free against a concurrent resume even though a netpoll
 * parker is wakeable (commit==PARKED) before its yield returns control
 * here.  A pump on another hub only *claims + re-queues* the g; it never
 * resumes it.  The wake routes to the g's OWNING hub (netpoll.c:1693,
 * runloom_mn_wake_g(p->hub, ...)), and a woken g (snap.valid) lands in that
 * hub's LOCAL ready FIFO, which is never work-stolen (only the Chase-Lev
 * deque is stealable -- mn_sched.c:248-286).  So the sole thread that
 * resumes g is the same hub that runs this madvise at its post-resume
 * site: madvise happens-before the next resume on one thread, and no
 * other hub ever touches the stack.  RUNLOOM_STACK_PARK_DONTNEED stays
 * default-OFF only for the throughput cost (madvise+refault per park
 * hurts short-park churn), not for safety; the path to default-ON is a
 * long-park heuristic that skips short parks.  See HANDOFF. */
void runloom_coro_park(runloom_coro_t *c);

/* Unconditional variant: madvise c's below-SP idle pages with no env
 * gate.  Used by the hub-idle dwell-based sweep (RUNLOOM_STACK_PARK_SWEEP),
 * which does its own gating + threshold.  Same SUSPENDED + owning-hub
 * safety contract as runloom_coro_park. */
void runloom_coro_madvise_idle(runloom_coro_t *c);

/* ------------------------------------------------------------------ */
/* Stack-usage measurement (used by sched calibration)                */
/* ------------------------------------------------------------------ */

/* When painting is enabled, every runloom_coro_new paints the stack body
 * with a known sentinel pattern (8-byte chunks).  runloom_coro_scan_hwm
 * then walks low->high and reports how many bytes were actually
 * touched by the coroutine.
 *
 * Disable painting (e.g. after calibration) to drop the per-spawn
 * paint cost (~few µs at 256 KB). */
void runloom_coro_paint_set(int enabled);
int  runloom_coro_paint_enabled(void);

/* Opt-in security scrub of recycled fiber stacks (default off). */
void runloom_coro_scrub_set(int enabled);
int  runloom_coro_scrub_enabled(void);

/* Returns the high-water mark in bytes (deepest write detected by
 * scanning for the sentinel).  Returns 0 if painting was disabled or
 * the coro hasn't been used.  Backend may return 0 on Fibers
 * (no introspectable stack). */
size_t runloom_coro_scan_hwm(runloom_coro_t *c);

#endif /* RUNLOOM_CORO_H */
