/* pygo_introspect.h -- goroutine registry + developer-facing dump.
 *
 * Go has a SIGQUIT goroutine dump and runtime.Stack(); asyncio has
 * asyncio.all_tasks().  pygo had neither: a wedged process gave the
 * operator nothing to look at.  This module is the answer.
 *
 *   1. A global registry of every live goroutine STRUCT.  A g is linked
 *      once, when its struct is first allocated from the OS, and
 *      unlinked only when the struct is returned to the OS.  Because
 *      pygo recycles g structs through a per-thread slab, a cached
 *      (PYGO_GST_FREED) g stays linked -- the dump just skips FREED
 *      entries.  The upshot: linking/unlinking happens only on the cold
 *      slab-miss / slab-overflow paths, so the hot spawn/complete path
 *      pays NOTHING (no registry lock, no global atomic).  See the
 *      field-ordering contract on pygo_g_t::id and the reuse branch in
 *      pygo_sched_core.c.inc.
 *
 *   2. pygo_dump_goroutines_fd(fd): an async-signal-safe-ish structural
 *      dump -- id / state / what-it's-blocked-on (fd, channel, sleep
 *      deadline) / owner thread / age / refcount / stack size -- written
 *      with nothing but snprintf + write(2).  This is the hung-process
 *      path: it runs from a SIGQUIT handler and try-locks the registry
 *      so a contended lock degrades to a partial dump rather than a
 *      deadlock.  No Python is touched, so it is safe even when the
 *      interpreter is wedged.
 *
 *   3. pygo_goroutine_snapshot(): the rich path for pygo.goroutines() --
 *      a point-in-time copy of every live goroutine's structural fields
 *      plus a strong ref to its entry callable, built for the Python
 *      layer to format (and, on request, augment with a reconstructed
 *      Python stack via pygo_goroutine_frames_by_id).  Runs only in
 *      normal (non-signal) interpreter context.
 *
 * Threading: the registry list is guarded by pygo_greg_lock.  The id
 * counter is per-thread (no shared cacheline).  Timestamping is opt-in.
 */
#ifndef PYGO_INTROSPECT_H
#define PYGO_INTROSPECT_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdint.h>
#include <stddef.h>

#include "plat.h"

#ifdef __cplusplus
extern "C" {
#endif

struct pygo_g;
typedef struct pygo_g pygo_g_t;

/* Initialise the registry lock + read PYGO_INTROSPECT_TIME.  Idempotent;
 * called from PyInit_pygo_core (Python path) and pygo_mn_init (C-harness
 * path), both single-threaded at the time of call. */
void pygo_introspect_init(void);
void pygo_introspect_fini(void);

/* ---- registry link/unlink ----
 * Called ONLY from the g-struct OS alloc / OS free sites (slab miss /
 * slab overflow / mn teardown).  Both take pygo_greg_lock.  Never call
 * from the hot reuse path. */
void pygo_greg_link(pygo_g_t *g);
void pygo_greg_unlink(pygo_g_t *g);

/* Per-incarnation goroutine id, Go's goid analogue.  Contention-free:
 * a per-thread counter ORed with a per-thread base, so spawning on many
 * hubs never touches a shared cacheline.  Unique for the process life. */
uint64_t pygo_next_goid(void);

/* Number of live (non-FREED) goroutines.  Takes pygo_greg_lock. */
long pygo_goroutine_count(void);

/* PYGO_GST_* -> short stable name ("running", "io-wait", ...).  Never
 * NULL (returns "?" for an out-of-range value). */
const char *pygo_g_state_name(unsigned int state);

/* One-word "what is it blocked on" class for a state, for the dump
 * header histogram.  Never NULL. */
const char *pygo_g_state_blockclass(unsigned int state);

/* ---- age timestamping (opt-in) ----
 * Off by default so the park hot path stays clean.  When on, a g's
 * state_since_ns is stamped each time it enters a PARKED_* state, and
 * the dump reports how long it has been wedged there. */
void pygo_introspect_set_timestamps(int on);
int  pygo_introspect_get_timestamps(void);
/* Hook from the g-state machine (pygo_gstate.c).  Cheap predicted-not-
 * taken branch when timestamping is off. */
void pygo_introspect_note_transition(pygo_g_t *g, unsigned int to);

/* Monotonic nanoseconds (the clock state_since_ns is measured against). */
long long pygo_introspect_monotonic_ns(void);

/* ---- structural dump (async-signal-safe-ish) ----
 * Dump every live goroutine to fd (fd < 0 -> stderr) using only
 * snprintf + write.  try-locks the registry; on contention prints a
 * note and the parker pool's own dump instead of blocking.  Touches NO
 * Python objects, so it is safe from a signal handler and when the
 * interpreter is wedged. */
void pygo_dump_goroutines_fd(int fd);

/* ---- rich snapshot (Python context only) ----
 * Every field is PLAIN DATA copied out of the g struct under the registry
 * lock.  Deliberately no owned-object pointers (callable / coro / parker):
 * those are freed by goroutine teardown, which does NOT take the registry
 * lock, so dereferencing them in the dump would be a use-after-free.  The
 * "what is it blocked on" detail rides on POD fields the g maintains
 * itself (park_fd) or that are values not pointers (wake_at).  Callable
 * identity + Python stack come from the claim-protected
 * pygo_goroutine_frames_by_id path instead. */
typedef struct pygo_g_info {
    uint64_t      id;
    unsigned int  state;        /* pygo_g_state_t */
    int           park_fd;      /* netpoll fd parked on, or -1 */
    int           park_events;  /* netpoll events bitmask (1=R 2=W), or 0 */
    double        wake_at;      /* sleep-heap wake_at deadline, or 0 */
    long long     age_ns;       /* now - state_since_ns, or -1 if untracked */
    int           refcount;
    int           noyield;
    const void   *owner;        /* owning pygo_sched_t (opaque thread id) */
} pygo_g_info_t;

/* Snapshot every live goroutine.  Returns a malloc'd array (caller frees
 * via pygo_goroutine_snapshot_free) and sets *count_out.  Returns NULL
 * with *count_out = 0 on OOM or before init.  Holds the registry lock only
 * for the POD copy, never while building Python objects. */
pygo_g_info_t *pygo_goroutine_snapshot(long *count_out);
void pygo_goroutine_snapshot_free(pygo_g_info_t *arr, long count);

/* Reconstruct the Python call stack (and entry-callable repr) of the
 * goroutine with the given id.  Returns a NEW tuple
 *   (callable_repr_or_None, [ (filename, lineno, funcname), ... ])
 * with frames deepest-first, or (None, []) if the goroutine is gone /
 * running / unclaimable / has no reconstructable frame.  Safe: under the
 * M:N scheduler it CLAIMS the goroutine via the sweeper handshake (so it
 * can neither resume nor tear down mid-walk); under the single-thread
 * scheduler the calling thread already owns it.  Normal interpreter
 * context only.  Returns NULL with a Python exception set only on a hard
 * error (OOM). */
PyObject *pygo_goroutine_frames_by_id(uint64_t id);

#ifdef __cplusplus
}
#endif

#endif /* PYGO_INTROSPECT_H */
