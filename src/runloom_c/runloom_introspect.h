/* runloom_introspect.h -- fiber registry + developer-facing dump.
 *
 * Go has a SIGQUIT fiber dump and runtime.Stack(); asyncio has
 * asyncio.all_tasks().  runloom had neither: a wedged process gave the
 * operator nothing to look at.  This module is the answer.
 *
 *   1. A global registry of every live fiber STRUCT.  A g is linked
 *      once, when its struct is first allocated from the OS, and
 *      unlinked only when the struct is returned to the OS.  Because
 *      runloom recycles g structs through a per-thread slab, a cached
 *      (RUNLOOM_GST_FREED) g stays linked -- the dump just skips FREED
 *      entries.  The upshot: linking/unlinking happens only on the cold
 *      slab-miss / slab-overflow paths, so the hot spawn/complete path
 *      pays NOTHING (no registry lock, no global atomic).  See the
 *      field-ordering contract on runloom_g_t::id and the reuse branch in
 *      runloom_sched_core.c.inc.
 *
 *   2. runloom_dump_fibers_fd(fd): an async-signal-safe-ish structural
 *      dump -- id / state / what-it's-blocked-on (fd, channel, sleep
 *      deadline) / owner thread / age / refcount / stack size -- written
 *      with nothing but snprintf + write(2).  This is the hung-process
 *      path: it runs from a SIGQUIT handler and try-locks the registry
 *      so a contended lock degrades to a partial dump rather than a
 *      deadlock.  No Python is touched, so it is safe even when the
 *      interpreter is wedged.
 *
 *   3. runloom_fiber_snapshot(): the rich path for runloom.fibers() --
 *      a point-in-time copy of every live fiber's structural fields
 *      plus a strong ref to its entry callable, built for the Python
 *      layer to format (and, on request, augment with a reconstructed
 *      Python stack via runloom_fiber_frames_by_id).  Runs only in
 *      normal (non-signal) interpreter context.
 *
 * Threading: the registry list is guarded by runloom_greg_lock.  The id
 * counter is per-thread (no shared cacheline).  Timestamping is opt-in.
 */
#ifndef RUNLOOM_INTROSPECT_H
#define RUNLOOM_INTROSPECT_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdint.h>
#include <stddef.h>

#include "plat.h"

#ifdef __cplusplus
extern "C" {
#endif

struct runloom_g;
typedef struct runloom_g runloom_g_t;

/* Initialise the registry lock + read RUNLOOM_INTROSPECT_TIME.  Idempotent;
 * called from PyInit_runloom_c (Python path) and runloom_mn_init (C-harness
 * path), both single-threaded at the time of call. */
void runloom_introspect_init(void);
void runloom_introspect_fini(void);

/* Fork safety: reset every runloom subsystem to a clean single-process state
 * in a forked child (hub + offload threads are gone).  Registered as an
 * os.register_at_fork(after_in_child=...) handler.  Single-thread child
 * only.  runloom_introspect_reset_after_fork resets just the registry. */
void runloom_after_fork_child(void);
void runloom_introspect_reset_after_fork(void);

/* ---- registry link/unlink ----
 * Called ONLY from the g-struct OS alloc / OS free sites (slab miss /
 * slab overflow / mn teardown).  Both take runloom_greg_lock.  Never call
 * from the hot reuse path. */
void runloom_greg_link(runloom_g_t *g);
void runloom_greg_unlink(runloom_g_t *g);

/* Per-incarnation fiber id, Go's goid analogue.  Contention-free:
 * a per-thread counter ORed with a per-thread base, so spawning on many
 * hubs never touches a shared cacheline.  Unique for the process life. */
long long runloom_next_goid(void);
/* Reserve a contiguous block of n goids in one atomic; returns the first. */
long long runloom_next_goid_block(long n);

/* Number of live (non-FREED) fibers.  Takes runloom_greg_lock. */
long runloom_fiber_count(void);

/* R0 gauge: live + retained runloom_g structs (OS-taken, not yet freed).
 * Lock-free relaxed load -- safe to call from m_stats while pumps run. */
long runloom_greg_total_count(void);

/* Count fibers owned by `owner` (a runloom_sched_t*; NULL = any) parked on a
 * channel or via park_safe -- the deadlockable set.  Used by the drain's
 * deadlock detector. */
long runloom_count_deadlockable_fibers(const void *owner);

/* Deadlock-detection mode: 0=off, 1=warn (print the fiber dump), 2=raise
 * a RuntimeError.  Default 1; also via RUNLOOM_DEADLOCK=off|warn|raise. */
int  runloom_deadlock_mode(void);
void runloom_set_deadlock_mode(int mode);

/* ---- max-fibers admission gate (backpressure) ----
 * 0 = unlimited (default; zero hot-path cost).  Also via RUNLOOM_MAX_GOROUTINES. */
long runloom_get_max_fibers(void);
void runloom_set_max_fibers(long n);
long runloom_live_fibers(void);
/* Spawn paths: admit before allocating (1=ok + mark the g limit_counted;
 * 0=over the limit, raise), release at the g's final decref iff counted. */
int  runloom_fiber_admit(void);
void runloom_fiber_release(void);

/* RUNLOOM_GST_* -> short stable name ("running", "io-wait", ...).  Never
 * NULL (returns "?" for an out-of-range value). */
const char *runloom_g_state_name(unsigned int state);

/* One-word "what is it blocked on" class for a state, for the dump
 * header histogram.  Never NULL. */
const char *runloom_g_state_blockclass(unsigned int state);

/* RUNLOOM_WR_* -> short name ("future", "waitgroup", "lock", ...) for the dump's
 * PARKED_SAFE subdivision, or NULL for WR_NONE / out-of-range (no suffix). */
const char *runloom_wait_reason_name(unsigned char reason);

/* Set the wait-reason hint for the current fiber (a runloom_wait_reason value);
 * consumed at its next park_safe.  A no-op off a fiber.  Diagnostic only. */
void runloom_set_current_wait_reason(int reason);

/* ---- age timestamping (opt-in) ----
 * Off by default so the park hot path stays clean.  When on, a g's
 * state_since_ns is stamped each time it enters a PARKED_* state, and
 * the dump reports how long it has been wedged there. */
void runloom_introspect_set_timestamps(int on);
int  runloom_introspect_get_timestamps(void);
/* Hook from the g-state machine (runloom_gstate.c).  Cheap predicted-not-
 * taken branch when timestamping is off. */
void runloom_introspect_note_transition(runloom_g_t *g, unsigned int to);

/* Monotonic nanoseconds (the clock state_since_ns is measured against). */
long long runloom_introspect_monotonic_ns(void);

/* ---- structural dump (async-signal-safe-ish) ----
 * Dump every live fiber to fd (fd < 0 -> stderr) using only
 * snprintf + write.  try-locks the registry; on contention prints a
 * note and the parker pool's own dump instead of blocking.  Touches NO
 * Python objects, so it is safe from a signal handler and when the
 * interpreter is wedged. */
void runloom_dump_fibers_fd(int fd);

/* ---- crash-handler helpers ----
 * runloom_fiber_for_addr: find the live fiber whose stack region (its
 * usable stack or the guard page below it) contains `addr`.  Returns its goid
 * (>0) and sets *kind to 1 if addr is in the guard page (a STACK OVERFLOW), 2
 * if in the usable stack (a wild pointer / UAF on that g), or 0 if no match;
 * *stack_kib is set to that fiber's stack size in KiB.  Try-locks the
 * registry (returns 0 / kind 0 if busy or unmatched).  Reads g->coro -- only
 * call from the crash path, where best-effort is acceptable.
 *
 * runloom_g_id: the goid of a g (acquire/relaxed read), or -1 if NULL.  A tiny
 * accessor so the crash handler can name the running g without the full
 * runloom_sched.h struct layout. */
long long runloom_fiber_for_addr(const void *addr, int *kind,
                                     unsigned *stack_kib);
long long runloom_g_id(const runloom_g_t *g);

/* ---- rich snapshot (Python context only) ----
 * Every field is PLAIN DATA copied out of the g struct under the registry
 * lock.  Deliberately no owned-object pointers (callable / coro / parker):
 * those are freed by fiber teardown, which does NOT take the registry
 * lock, so dereferencing them in the dump would be a use-after-free.  The
 * "what is it blocked on" detail rides on POD fields the g maintains
 * itself (park_fd) or that are values not pointers (wake_at).  Callable
 * identity + Python stack come from the claim-protected
 * runloom_fiber_frames_by_id path instead. */
typedef struct runloom_g_info {
    long long     id;
    unsigned int  state;        /* runloom_g_state_t */
    int           park_fd;      /* netpoll fd parked on, or -1 */
    int           park_events;  /* netpoll events bitmask (1=R 2=W), or 0 */
    double        wake_at;      /* sleep-heap wake_at deadline, or 0 */
    long long     age_ns;       /* now - state_since_ns, or -1 if untracked */
    int           refcount;
    int           noyield;
    const void   *owner;        /* owning runloom_sched_t (opaque thread id) */
} runloom_g_info_t;

/* Snapshot every live fiber.  Returns a malloc'd array (caller frees
 * via runloom_fiber_snapshot_free) and sets *count_out.  Returns NULL
 * with *count_out = 0 on OOM or before init.  Holds the registry lock only
 * for the POD copy, never while building Python objects. */
runloom_g_info_t *runloom_fiber_snapshot(long *count_out);
void runloom_fiber_snapshot_free(runloom_g_info_t *arr, long count);

/* Reconstruct the Python call stack (and entry-callable repr) of the
 * fiber with the given id.  Returns a NEW tuple
 *   (callable_repr_or_None, [ (filename, lineno, funcname), ... ])
 * with frames deepest-first, or (None, []) if the fiber is gone /
 * running / unclaimable / has no reconstructable frame.  Safe: under the
 * M:N scheduler it CLAIMS the fiber via the sweeper handshake (so it
 * can neither resume nor tear down mid-walk); under the single-thread
 * scheduler the calling thread already owns it.  Normal interpreter
 * context only.  Returns NULL with a Python exception set only on a hard
 * error (OOM). */
PyObject *runloom_fiber_frames_by_id(long long id);

#ifdef __cplusplus
}
#endif

#endif /* RUNLOOM_INTROSPECT_H */
