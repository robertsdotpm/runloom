/* runloom_sched.h -- C-level cooperative scheduler.
 *
 * The Python-side `runloom.go(fn)` ultimately creates a goroutine here.
 * yield, sleep, run -- all do their bookkeeping in C, calling into
 * Python only to invoke the user's entry function.
 *
 * Single OS thread per scheduler in v0.  Multi-thread is Phase C
 * (free-threaded Python with one scheduler per OS thread, work-stealing).
 *
 * Phase B (this file): per-goroutine snapshot of the CPython thread
 * state fields that a raw C-stack swap doesn't preserve.  Algorithm
 * copied from greenlet (MIT licensed; see TPythonState.cpp).
 */
#ifndef RUNLOOM_SCHED_H
#define RUNLOOM_SCHED_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "coro.h"
#include "plat_compat.h"   /* runloom_mutex_t for cross-thread wake list */

typedef struct runloom_g runloom_g_t;
typedef struct runloom_sched runloom_sched_t;
typedef struct runloom_pystate_snap runloom_pystate_snap_t;

/* Per-g wake state machine for the RUNLOOM_PER_G_TSTATE global run-queue.
 * See the wake_state field on struct runloom_g for the protocol and the legal
 * edges.  PARKED is 0 so a slab-zeroed g is in a defined state; spawn lifts a
 * fresh g to RUNNING under per-g-tstate before it can be resumed. */
#define RUNLOOM_WS_PARKED         0
#define RUNLOOM_WS_QUEUED         1
#define RUNLOOM_WS_RUNNING        2
#define RUNLOOM_WS_RUNNING_WOKEN  3
/* SWEEPING/SWEEPING_WOKEN mirror RUNNING/RUNNING_WOKEN for the idle stack
 * sweep: an idle hub claims a long-parked g (PARKED -> SWEEPING) to own its
 * stack exclusively while it MADV_DONTNEEDs the below-SP idle pages, so no
 * other hub can resume the g into pages mid-zeroing.  A wake during the sweep
 * is remembered (SWEEPING -> SWEEPING_WOKEN) and re-enqueued at release, never
 * lost.  Lets per-g-tstate run the sweep it otherwise has to disable.  See the
 * sweeper edges in the wake_state field comment. */
#define RUNLOOM_WS_SWEEPING       4
#define RUNLOOM_WS_SWEEPING_WOKEN 5

/* Per-goroutine CPython thread state snapshot.
 *
 * Fields here are everything the interpreter keeps on PyThreadState that
 * a raw asm stack switch cannot preserve on its own.  Each save copies
 * them out of tstate into the snap; each load copies them back AND
 * transfers ownership (context, top_frame, delete_later) so the snap is
 * empty after a load.  Save and load must be balanced.
 *
 * Layout matches greenlet's PythonState/ExceptionState, transcribed to
 * C99 with #if PY_VERSION_HEX gates for 3.12 vs 3.13 vs older.  See
 * https://github.com/python-greenlet/greenlet src/greenlet/TPythonState.cpp.
 */
struct runloom_pystate_snap {
    int valid;
    /* CPython per-object critical-section chain held by this g when it parked
     * (free-threaded 3.13t only; 0 otherwise).  Saved + the mutexes released on
     * snap, restored + re-locked on load -- so a g that parks mid-critical-
     * section (e.g. inside a dict key __eq__) does not strand the dict's mutex
     * locked across the swap and deadlock every other hub.  See snap/load. */
    uintptr_t critical_section;
#if PY_VERSION_HEX >= 0x030B0000
    /* 3.11+ common fields.  All of: contextvars, datastack arena
     * pointers, exc state, exist on 3.11/3.12/3.13. */
    PyObject *context;                       /* contextvars; owned ref */
    _PyStackChunk *datastack_chunk;
    PyObject **datastack_top;
    PyObject **datastack_limit;
    _PyErr_StackItem *exc_info;
    _PyErr_StackItem exc_state;
    /* The in-flight unraised exception (tstate->current_exception).
     * Set when PyErr_SetObject is mid-call and an exception object has
     * been associated with the tstate but not yet raised through the
     * eval loop.  Critical to save/restore: at high concurrency,
     * goroutines yield while their current_exception is non-NULL and
     * other goroutines overwrite it, causing tstate to read a freed/
     * stale object on resume.  Manifests as a segfault in
     * _PyErr_SetObject during the next exception cascade (e.g., async
     * function's StopIteration on return). */
    PyObject *current_exception;
    /* Cross-hub snap migration (RUNLOOM_STEAL_WOKEN): when the g suspended inside
     * active exception handling (exc_info != &tstate->exc_state), the bottom
     * per-g _PyErr_StackItem's previous_item points at the ORIGIN hub tstate's
     * &exc_state -- hub-bound.  Recorded here at snap so load can re-root it
     * onto the TARGET hub's &exc_state.  NULL in the common exc_info==base case.
     * Borrowed (the item lives in a per-g gen/coro object kept alive by the g's
     * frames); no ref held. */
    _PyErr_StackItem *exc_chain_bottom;
#endif
#if PY_VERSION_HEX >= 0x030B0000 && PY_VERSION_HEX < 0x030C0000
    /* 3.11: single recursion counter, named recursion_remaining. */
    int recursion_remaining;
#endif
#if PY_VERSION_HEX >= 0x030C0000
    /* 3.12+: split into Python-level and C-level counters. */
    int py_recursion_remaining;
    int c_recursion_remaining;
#endif
#if PY_VERSION_HEX >= 0x030B0000
    /* Per-goroutine sys.setprofile / sys.settrace hooks (BUG #11).  These are
     * tstate-global, so without snap/restore a hook one goroutine installs
     * leaks onto every other goroutine sharing the hub (and is cleared from
     * under it on resume).  Saved/restored so each goroutine carries its own. */
    Py_tracefunc c_profilefunc;
    Py_tracefunc c_tracefunc;
    PyObject *c_profileobj;                   /* owned ref while suspended */
    PyObject *c_traceobj;                     /* owned ref while suspended */
    int tracing;
#endif
#if PY_VERSION_HEX >= 0x030B0000 && PY_VERSION_HEX < 0x030D0000
    /* 3.11 and 3.12: cframe lives on the C stack, threaded through
     * the linked list.  3.13 removed cframe; current_frame lives
     * directly on tstate instead. */
    _PyCFrame *cframe;
    int trash_delete_nesting;
#endif
#if PY_VERSION_HEX >= 0x030D0000
    /* 3.13+ fields. */
    struct _PyInterpreterFrame *current_frame;
    PyObject *delete_later;                  /* owned ref */
#endif
    /* DIAG (RUNLOOM_DIAG_MIGRATE): the PyThreadState bound when this snap was
     * saved -- i.e. the tstate pointer baked into the g's suspended CPython
     * eval-loop C frame.  A cross-hub resume loads the snap onto a DIFFERENT
     * bound tstate; if current_frame != NULL the eval loop still threads this
     * origin_tstate while the bound tstate differs -> the H>=2 corruption.
     * Borrowed pointer; compared, never dereferenced through here. */
    PyThreadState *origin_tstate;
};

/* One goroutine (the "G" in Go's M:P:G nomenclature).
 *
 * Lifetime: refcounted.  Two parties hold refs:
 *   - the scheduler, while g is in the ready queue or sleep heap
 *   - the RunloomG Python wrapper, while the user holds it
 * Both decrement on release; the g is freed when both are gone.
 */
/* C-only entry point.  Set on a g spawned via runloom_mn_go_c (no Python
 * callable).  When set, runloom_g_entry calls c_entry(c_arg) instead of
 * PyObject_CallNoArgs(callable).  Used by the C test harness in
 * tests_c/ to exercise the M:N + netpoll core without the Python
 * interpreter, so sanitizers / valgrind have a clean view. */
typedef void (*runloom_c_entry_fn)(void *);

struct runloom_g {
    runloom_coro_t *coro;
    /* Stack-advice kind key (FNV hash of the entry callable's identity), or 0.
     * Set at spawn only while stack-advice profiling is enabled; read at
     * completion to fold this g's C-stack HWM into its kind.  See
     * runloom_stackadvice.c. */
    size_t advice_key;
    PyObject *callable;     /* Python callable (NULL if c_entry set) */
    runloom_c_entry_fn c_entry;
    void *c_arg;
    PyObject *result;
    PyObject *error;
    runloom_pystate_snap_t snap;     /* saved tstate; valid only when suspended */
    PyThreadState *tstate;        /* per-g tstate, non-NULL only under
                                   * RUNLOOM_PER_G_TSTATE; the g's own Python
                                   * execution state, migratable across hubs */
    double wake_at;
    uint64_t sleep_seq;  /* FIFO tiebreak for equal wake_at (asyncio (when,seq) order) */
    runloom_g_t *next;
    /* Owning per-thread scheduler (Phase C: one sched per OS thread).  Set at
     * spawn to the spawning thread's sched.  A cross-thread wake_safe (e.g. a
     * run_in_executor pool worker resolving a future the owner awaits) must
     * route the g back to THIS sched's wake_list, not the waker thread's. */
    runloom_sched_t *owner;
    int done;
    int refcount;
    /* Caller-asserted "this goroutine will never yield".  Spawned via
     * runloom_sched_spawn_noyield (Python: runloom_c.go_noyield(fn)).
     * When set, drain skips the per-g datastack install + drain +
     * sched_snap load/resnap dance, because g runs to completion
     * within one resume + uses the scheduler's existing Python state
     * without leaving anything behind.  Saves ~150-400 ns per g
     * lifetime depending on workload.
     *
     * If a noyield-marked g actually yields (calls sched_yield,
     * sched_sleep, wait_fd, or any monkey-patched I/O), the result
     * is undefined -- frames will alias across goroutines.  Use
     * only for pure-compute callables. */
    int noyield;
    /* PCT (Probabilistic Concurrency Testing) priority -- 0 = unassigned;
     * only read/written on the single-hub ready path when RUNLOOM_PCT_SEED is
     * set (testing only, zero cost otherwise).  See runloom_sched.c. */
    int pct_prio;
    /* PCT FIFO marker: when set, PCT must NOT reorder this g relative to other
     * FIFO-marked gs -- it preserves their spawn (ready-ring) order.  The aio
     * bridge marks every _go_io goroutine (call_soon callbacks, task steps,
     * io/timer drivers) so PCT respects asyncio's call_soon-FIFO contract
     * instead of permuting it (which was a false positive, not a bug -- asyncio
     * scheduling has no legal reordering freedom).  PCT still freely interleaves
     * un-marked raw goroutines/channels in a mixed program.  Slab-zeroed to 0
     * (reorderable) by default; testing-only, zero cost when PCT is off. */
    int pct_fifo;
    /* This sleep uses the WALL clock even when the logical clock is on (set per
     * sched_sleep).  The aio loop keepalive is a real-time heartbeat (poll the
     * cross-thread queue every few ms), so it must NOT ride the logical clock --
     * else it busy-loops advancing logical time.  Only app sched_sleep / call_at
     * are logical.  Default 0 (logical); set 1 by runloom_sched_sleep_until_real. */
    int sleep_real;
    /* Race-safe park/wake counter.  runloom_sched_park_safe decrements;
     * if >0, the wake already arrived and we skip the yield.
     * runloom_sched_wake_safe increments and (if g is currently parked)
     * adds it back to ready.  Used by runloom.aio's RunloomTask to replace
     * the per-task Chan(1) wake channel with a much cheaper primitive
     * -- saves ~5 us per task at fan-out time. */
    int wake_pending;
    /* MPSC sub-queue membership flag.  Set with CAS by runloom_mn_hub_submit
     * before linking g into the hub's sub_head chain; cleared by
     * hub_main when it drains g out of the sub chain.  Prevents the
     * same g from being submitted twice (e.g., a spurious wake_g after
     * the legitimate one) -- the second submit becomes a no-op so g
     * isn't enqueued and later popped twice, which would resume a
     * freed coro on the second pop. */
    int in_sub_queue;
    /* ---- RUNLOOM_PER_G_TSTATE global run-queue: per-g wake state machine ----
     * A single atomic that makes the woken-g global run-queue safe for ANY
     * idle hub to drain (so a hub wedged in a blocking C call can't strand its
     * woken work) WITHOUT duplicate entries, double-resume, or lost wakes.  The
     * one field unifies what two independent flags (an exactly-once-wake dedup
     * + an exclusive-resume claim) used to split -- and which raced into a
     * re-push livelock, because "one entry per park" and "one resumer" were
     * separate invariants that could disagree.  Here they are the SAME
     * invariant: a g holds at most one runq entry exactly when it is QUEUED,
     * and exactly one hub owns it exactly when it is RUNNING, so there is no
     * re-push and no duplicate.  Untouched by the default (per-hub-tstate)
     * scheduler; valid only under RUNLOOM_PER_G_TSTATE.
     *
     * States and the (only) legal edges, each a CAS by the named actor:
     *
     *   PARKED  -- suspended at a park; wakeable; no entry, no owner.
     *   QUEUED  -- exactly one runq entry exists; not yet owned.
     *   RUNNING -- one hub owns it (resuming, or finishing the resume up to
     *              the post-detach release); wakes are remembered, not enqueued.
     *   RUNNING_WOKEN -- RUNNING and a wake arrived while owned; the owner
     *              enqueues it at release (so the wake during the
     *              commit->detach window is never lost and never lets a second
     *              hub attach the g's live tstate mid-detach).
     *   SWEEPING -- an idle hub owns the g's stack for an MADV_DONTNEED idle
     *              sweep; un-resumable for the madvise's duration (mirrors
     *              RUNNING but with no tstate attached -- the g is still parked,
     *              just held).
     *   SWEEPING_WOKEN -- SWEEPING and a wake arrived; the sweeper enqueues it
     *              at release, so a wake landing mid-madvise is never lost.
     *
     *   wake_g (any thread):   PARKED -> QUEUED   (winner enqueues + increfs)
     *                          RUNNING -> RUNNING_WOKEN  (remember; no enqueue)
     *                          SWEEPING -> SWEEPING_WOKEN (remember; no enqueue)
     *                          QUEUED / RUNNING_WOKEN / SWEEPING_WOKEN: a wake is
     *                              already pending -> drop (no duplicate entry).
     *   hub pull+resume:       QUEUED -> RUNNING  (the entry's holder; the sole
     *                              consumer of that entry, so this never fails).
     *   hub release, parked:   RUNNING -> PARKED, or, if a wake landed in the
     *                              window, RUNNING_WOKEN -> QUEUED (+enqueue).
     *   sweeper claim/release: PARKED -> SWEEPING (try-claim; loses to any
     *                              non-PARKED state and skips the g), then after
     *                              the madvise SWEEPING -> PARKED, or, if a wake
     *                              landed, SWEEPING_WOKEN -> QUEUED (+enqueue).
     *
     * A fresh g (never parked) starts RUNNING (set at spawn under per-g-tstate);
     * gs from the deque/local FIFO/steal are already RUNNING by this invariant,
     * so they resume with no CAS.  The g's own scheduler ref + one queue ref per
     * entry (incref at enqueue, decref at consume/drain) cover lifetime; at most
     * one entry ever references a g, so the proven sub_list/deque ref model holds. */
    int wake_state;
    /* Active netpoll parker, set by runloom_netpoll_wait_fd on link and
     * cleared on unlink.  Each g has at most one parker in flight (a
     * g calls wait_fd sequentially), so a single pointer suffices.
     * Cleared force-unlinks any leaked parker at g completion -- the
     * defense against missed unlink paths under M:N + free-threaded
     * that would otherwise have pump waking a freed g. */
    void *netpoll_parker;   /* really runloom_parked_t *, void* to avoid include cycle */
    /* park_safe / wake_safe lost-wake guard.  Set to 1 by park_safe
     * just before runloom_coro_yield; CAS'd back to 0 by the first
     * wake_safe to observe it.  Replaces the original s->current
     * check, which read tstate state across threads -- a cross-thread
     * wake_safe (e.g., from a hub thread processing an iouring CQE
     * for a goroutine parked on the single-thread sched) could
     * observe s->current==g (still set by drain) and skip the push,
     * losing the wake.  The CAS-based handoff is independent of any
     * tstate observation and gives wake_safe a deterministic "did we
     * own the wake?" answer regardless of caller thread. */
    int parked_safe;
    /* MPSC link for the home sched's cross-thread wake list.  Used
     * only while g is parked via park_safe AND a cross-thread wake
     * is in flight (between wake_safe's enqueue and drain's
     * drain_wake_list).  Kept separate from `next` so an M:N sub
     * queue + wake list cannot collide on the same g. */
    runloom_g_t *wake_next;
    /* Observational lifecycle state.  See runloom_gstate.h for the enum.
     * Independent of (but consistent with) the load-bearing
     * coro/done/in_sub_queue/wake_pending fields above; set at every
     * transition point so the diag ring records the trajectory and
     * RUNLOOM_G_ASSERT_NOT can flag invalid arrivals (e.g. submit on a
     * g already in DONE).  Single atomic byte; cost is one store
     * per transition. */
    unsigned char state;

    /* ---- bulk-arena ownership (go_n) ----
     * When `arena` is set, this g, its coro, and its stack are SLICES of a bulk
     * arena (one calloc / one mmap for the whole batch), NOT individually
     * malloc'd.  Its final decref must therefore NOT runloom_coro_destroy the coro
     * nor runloom_g_slab_free the g (either would free()/pool a slice -> heap
     * corruption).  Instead it decrements the owning batch's live count; the
     * LAST goroutine to finish tears the whole batch down (free the g/coro
     * arenas, MADV_DONTNEED the stack block).  0 for every normal goroutine.
     * Both live BEFORE the id introspection block so slab reuse clears them. */
    unsigned char arena;
    struct runloom_gon_batch *batch;

    /* go_n(indexed=True): call the entry as fn(index) rather than fn().  The
     * index is stashed in c_arg (a void*, unused on the Python-callable path
     * since c_entry is NULL there); g_entry builds the PyLong lazily on the hub.
     * 0 = fn() (slab-cleared default). */
    unsigned char pass_index;

    /* ---- introspection block (runloom_introspect.c) ----
     * These fields are deliberately the LAST members of runloom_g_t.  The
     * per-thread slab reuse path (runloom_g_slab_alloc) bulk-clears a g only
     * up to offsetof(runloom_g_t, id) -- i.e. everything BEFORE this block --
     * so reg_prev/reg_next survive recycling untouched.  That is what
     * keeps the global goroutine registry walkable without taking the
     * registry lock on the (hot) spawn path: the only writers of reg_prev/
     * reg_next are runloom_greg_link/unlink, both under runloom_greg_lock, on the
     * cold OS-alloc / slab-overflow-free paths.  See the reuse branch in
     * runloom_sched_core.c.inc and the field-ordering contract there. */

    /* Per-incarnation goroutine id (Go's goid analogue).  Assigned fresh
     * at each spawn from a block-batched global counter; unique + always
     * positive for the life of the process.  Read by the dump, written at
     * spawn with an atomic store.  0 until first spawned.  `long long` (not
     * uint64_t) so the MSVC _Generic atomic shim has a matching slot --
     * uint64_t is `unsigned __int64` there, which the shim doesn't name. */
    long long id;
    /* Monotonic-ns timestamp of the last state transition into a PARKED_*
     * state, stamped only when introspection timestamping is enabled
     * (runloom_introspect_set_timestamps / RUNLOOM_INTROSPECT_TIME).  Lets the
     * dump report "parked for 45.2s" to spot a wedged goroutine.  -1 when
     * never stamped / tracking off. */
    long long state_since_ns;
    /* "What am I blocked on" summary, kept as plain data ON THE G so the
     * dump never has to dereference the (teardown-freeable) parker object.
     * park_fd/park_events are written when the g commits to a netpoll wait
     * (runloom_netpoll_wait_fd); they are stale-but-harmless once the g moves
     * on (the dump only trusts them when state == RUNLOOM_GST_PARKED_NETPOLL).
     * park_fd defaults to -1. */
    int park_fd;
    int park_events;
    /* Set to 1 at spawn when this goroutine was admitted under an active
     * max-goroutines limit, so its final decref knows to release the slot.
     * Travels with the g so toggling the limit can't unbalance the counter. */
    unsigned char limit_counted;
    /* Intrusive doubly-linked global registry of every live g STRUCT
     * (live + slab-cached).  Linked once when the struct is first OS-
     * allocated, unlinked only when it is returned to the OS.  A cached
     * (RUNLOOM_GST_FREED) g stays linked; the dump skips FREED entries. */
    runloom_g_t *reg_prev;
    runloom_g_t *reg_next;
};

/* Park current g until runloom_sched_wake_g(g) is called.  Race-safe:
 * a wake that arrives BEFORE the park (because the future fires
 * synchronously, e.g. add_done_callback on an already-done future)
 * makes the park a no-op and the goroutine continues. */
void runloom_sched_park_safe(void);

/* Wake a goroutine previously parked via runloom_sched_park_safe.  Safe
 * to call before park (wake_pending counter records the arrival). */
void runloom_sched_wake_safe(runloom_g_t *g);

/* Lifetime helpers. */
void runloom_g_incref(runloom_g_t *g);
void runloom_g_decref(runloom_g_t *g);

/* go_n bulk-arena batch teardown: called by an arena g's final decref instead
 * of free()ing the g/coro/stack slices individually.  Decrements the batch's
 * live count; the LAST goroutine to finish frees the g + coro arenas and
 * MADV_DONTNEEDs the stack block.  Defined in mn_sched_init_fini.c.inc. */
struct runloom_gon_batch;
void runloom_gon_batch_finish_one(struct runloom_gon_batch *b);

/* Acquire a reference ONLY if the g is still live (refcount > 0).  Returns
 * 1 on success (caller now owns a ref, must decref), 0 if the g is already
 * being torn down (refcount reached 0).  CAS loop; used by the goroutine
 * dump to pin a g found via the registry without resurrecting one that a
 * concurrent final decref is mid-freeing. */
int runloom_g_try_incref(runloom_g_t *g);

/* Slab allocator for runloom_g_t -- per-thread LIFO free list with cap.
 * Exposed so mn_sched.c can share the same recycle pool as the
 * single-thread spawn path.  alloc returns a zeroed g (or NULL +
 * PyErr_NoMemory on OOM); free returns to the slab. */
runloom_g_t *runloom_g_slab_alloc(void);
void runloom_g_slab_free(runloom_g_t *g);

/* Per-OS-thread scheduler. */
struct runloom_sched {
    /* Ready FIFO -- ring buffer of g pointers.  Previously a linked
     * list threaded through g->next, which meant every pop dereffed
     * a different (cache-cold) g struct just to read the next
     * pointer.  At 100k gs in flight that was the bottleneck on
     * spawn-heavy workloads.  Ring buffer keeps the queue itself in
     * a contiguous array (hot in L1 if it fits) and saves one cache
     * miss per push/pop. */
    runloom_g_t **ready_ring;            /* power-of-2 sized array */
    size_t    ready_cap;              /* power of 2 */
    size_t    ready_mask;             /* ready_cap - 1 */
    size_t    ready_head;             /* dequeue index (monotonic counter, mask to index) */
    size_t    ready_tail;             /* enqueue index */
    /* Currently-running g (for yield). */
    runloom_g_t *current;
    /* Sleep heap -- min-heap by wake_at.  Stored as a growable array
     * indexed 1..size; index 0 unused. */
    runloom_g_t **sleep_heap;
    Py_ssize_t sleep_size;
    Py_ssize_t sleep_cap;
    uint64_t   sleep_seq_ctr;  /* monotonic counter for sleep_seq FIFO tiebreak */
    /* Default stack size for new gs. */
    Py_ssize_t stack_size;
    /* Goroutines completed since the last sched_drain. */
    Py_ssize_t completed;
    /* When set, sched_drain returns. */
    int stopping;
    /* A Python signal-handler exception (normalized, traceback attached) the
     * idle scheduler grab ran and is handing to a goroutine parked in wait_fd:
     * set just before runloom_netpoll_signal_wake re-queues that g, consumed (taken
     * + cleared) by runloom_netpoll_wait_fd when it resumes on the
     * RUNLOOM_NETPOLL_SIGNALED sentinel.  Owned ref while set; NULL otherwise. */
    PyObject *signal_exc;
    /* Count of THIS sched's goroutines currently parked in netpoll (non-hub
     * parkers whose g->owner == this sched).  Bumped in runloom_parker_link /
     * unlink.  The drain loop uses this -- NOT the global parked count -- so a
     * goroutine parked on another (or a dead) OS thread can't keep this
     * thread's runloom_c.run() alive forever.  Accessed atomically: a pump on
     * another thread may unlink (decrement) one of our parkers cross-thread. */
    int netpoll_parked;
    /* Cross-thread wake list -- MPSC linked through g->wake_next.
     * Foreign-thread wake_safe pushes here under wake_list_lock; the
     * drain owner consumes once per iteration via
     * runloom_sched_drain_wake_list and copies into the lock-free ready
     * ring.  Keeps wake_safe off the non-atomic ready_ring writes
     * that would race with the owner's pop. */
    runloom_mutex_t wake_list_lock;
    runloom_g_t *wake_list_head;
    runloom_g_t *wake_list_tail;
    /* Quiescence-barrier wait list (single-thread sched only) -- goroutines
     * parked by runloom_sched_run_ready().  FIFO, threaded through g->next (free
     * for a live g on this sched; the slab free-list and M:N hub queues are
     * the only other users of g->next and neither overlaps here).  The drain
     * loop flushes the WHOLE list back to ready at the next quiescence point
     * (ready empty, just before it would block on netpoll/timers), giving
     * "resume me once no other goroutine is immediately runnable" -- asyncio's
     * one-loop-iteration semantics, iterated to quiescence. */
    runloom_g_t *quiescence_head;
    runloom_g_t *quiescence_tail;
};

/* Is the ready queue empty?  Hot-path predicate; inline-friendly. */
RUNLOOM_INLINE int runloom_sched_ready_empty(const runloom_sched_t *s) {
    return s->ready_head == s->ready_tail;
}

/* Module-level: one sched per OS thread once Phase C lands.  For now
 * a single global. */
runloom_sched_t *runloom_sched_get(void);

/* Non-allocating: the g running on this thread's single-thread sched, or NULL. */
runloom_g_t *runloom_sched_peek_current(void);

/* Spawn a new goroutine.  Returns a NEW reference to a RunloomG Python
 * object (the wrapper around runloom_g_t).  Stealing the callable. */
PyObject *runloom_sched_spawn(runloom_sched_t *s, PyObject *callable);

/* Spawn a goroutine marked as "noyield" -- caller asserts the
 * callable will run to completion without calling sched_yield,
 * sched_sleep, wait_fd, or any monkey-patched I/O.  The drain path
 * skips the per-g datastack install / drain / sched_snap load+
 * resnap dance, cutting ~150-400 ns / g lifetime depending on
 * workload.  Useful for CPU-bound parallel fan-out where you know
 * the handler is pure compute. */
PyObject *runloom_sched_spawn_noyield(runloom_sched_t *s, PyObject *callable);

/* Spawn with an explicit per-g stack size override (bypasses calibration
 * and the scheduler default).  Used for the rare g whose call depth
 * exceeds the calibrated bound (deep recursion, heavy C extension). */
PyObject *runloom_sched_spawn_sized(runloom_sched_t *s, PyObject *callable,
                                 size_t stack_size);

/* ---- Stack calibration ----
 *
 * During the warmup window, every g is painted with a sentinel and
 * scanned on completion.  Once N completions have been observed (or T
 * seconds have elapsed) we lock the scheduler-wide default to
 * next_pow2(observed_max_hwm * SAFETY).  Painting is then disabled to
 * remove the per-spawn overhead, and pool entries at the old size
 * naturally drain.
 *
 * Override-on-set: runloom_sched_set_default_stack_size also freezes
 * calibration; subsequent goroutines spawn at the requested size. */
void   runloom_sched_set_default_stack_size(size_t bytes);
size_t runloom_sched_get_default_stack_size(void);

/* Snapshot of calibration state.  All fields are best-effort reads
 * (no lock).  Used by runloom_c.stats(). */
typedef struct runloom_stack_stats {
    size_t  default_size;    /* current per-spawn default in bytes */
    size_t  max_hwm;         /* highest HWM observed since start */
    long long completed;     /* number of gs that have been scanned */
    int     calibrated;      /* 0 = still calibrating, 1 = frozen */
    int     painting;        /* current paint-on flag */
} runloom_stack_stats_t;
void runloom_sched_stack_stats(runloom_stack_stats_t *out);

/* Yield the current g.  Re-queues on the ready FIFO, swaps back to
 * the scheduler stack.  Must be called from inside a g. */
void runloom_sched_yield(runloom_sched_t *s);

/* Park the current g until wake_at (monotonic seconds).  Swap back. */
void runloom_sched_sleep_until(runloom_sched_t *s, double wake_at);
/* As above, but a WALL-clock deadline the logical clock won't advance for
 * (the aio keepalive heartbeat -- must not ride RUNLOOM_LOGICAL_CLOCK). */
void runloom_sched_sleep_until_real(runloom_sched_t *s, double wake_at);

/* Park the current g on the quiescence-barrier list; the drain loop resumes
 * it once no other goroutine is immediately runnable (ready empty), before
 * blocking on netpoll/timers.  asyncio "one loop iteration" iterated to
 * quiescence.  Single-thread sched only; a no-op if not inside a g. */
void runloom_sched_run_ready(runloom_sched_t *s);

/* Mark current g as parked (no ready_push); netpoll/sleep saves snap.
 * Caller must then yield via runloom_coro_yield. */
void runloom_sched_park_current(void);

/* Re-queue a previously-parked g onto the ready list. */
void runloom_sched_wake(runloom_g_t *g);

/* Drive the scheduler until ready+sleep queues are empty.  Returns
 * the number of completed goroutines. */
Py_ssize_t runloom_sched_drain(runloom_sched_t *s);

/* Free all allocated state in the scheduler (does not destroy gs
 * still referenced by Python). */
void runloom_sched_init(runloom_sched_t *s);

/* Internal FIFO ops, exposed for reuse from mn_sched.c (hub-local
 * yielded-g queue piggybacks on the same singly-linked list). */
void runloom_sched_ready_push(runloom_sched_t *s, runloom_g_t *g);
runloom_g_t *runloom_sched_ready_pop(runloom_sched_t *s);

/* Snap/load primitives, exposed for mn_sched.c so hub_main can do the
 * same Phase B per-g state dance as the single-thread drain. */
void runloom_pystate_snap(runloom_pystate_snap_t *snap);
void runloom_pystate_load(runloom_pystate_snap_t *snap);
void runloom_pystate_snap_clear(runloom_pystate_snap_t *snap);

/* Per-goroutine-tstate mode (RUNLOOM_PER_G_TSTATE).  When on, runloom_pystate_snap
 * no-ops so each g's own tstate is never swapped out; mn_sched runs the
 * tstate-attach/detach path instead.  Set by mn_init, cleared by mn_fini. */
void runloom_set_per_g_tstate_mode(int on);
int  runloom_get_per_g_tstate_mode(void);

/* The user's callable trampoline for a goroutine; installs an initial
 * root cframe / current_frame on g's own stack, then runs g->callable.
 * Exposed so mn_sched.c can reuse the same entry (Phase B correct). */
void runloom_g_entry(void *user);

/* Free the datastack-chunk chain owned by the just-completed goroutine.
 * Call AFTER runloom_coro_resume returns done=true and BEFORE loading any
 * other snapshot back into tstate (which would overwrite the chunk
 * pointers and leak the g's allocation).  Matches greenlet's did_finish.
 *
 * Returned chunks go to a per-thread pool (capped) so the next first-run
 * g can pick one up via runloom_first_run_install_datastack instead of
 * paying for an arena alloc. */
void runloom_drain_g_datastack(void);

/* Set up tstate->datastack_{chunk,top,limit} for a first-run g.  Pulls
 * a chunk off the per-thread pool if available; otherwise leaves the
 * fields NULL so PyEval will arena-allocate.  Either is correct. */
void runloom_first_run_install_datastack(void);

/* Reclaim the idle tail of a parked Python goroutine's datastack chunk.
 * The companion of runloom_coro_madvise_idle (which drops the C stack below
 * SP): here we MADV_DONTNEED the free pages of g's CURRENT _PyStackChunk
 * above the live frontier (snap->datastack_top) up to the chunk end
 * (snap->datastack_limit).  Frames live in [chunk, top); everything above
 * is unpushed free space that refaults zero on the next frame push.
 *
 * SAFE under the same M:N contract as the C-stack sweep: the caller must
 * be g's OWNING hub (so nothing resumes g while we madvise) and g must be
 * suspended with a stable snap.  No-op for C-only gs (datastack_chunk
 * NULL), gs that never went deep enough to have a reclaimable tail, and
 * on pre-3.11 Pythons / platforms without MADV_DONTNEED.
 *
 * Default-ON (RUNLOOM_DATASTACK_SWEEP=0 opts out), mirroring the master
 * RUNLOOM_STACK_PARK_SWEEP switch that gates the dwell sweep this rides in;
 * the sweep calls this per batched parker right after the C-stack madvise. */
void runloom_sched_madvise_datastack_idle(runloom_g_t *g);

/* Decompose instrumentation for the datastack sweep (RUNLOOM_DATASTACK_DEBUG).
 * Accumulated only when the debug env is set: total reclaimable tail bytes
 * seen, how many of those were RESIDENT at madvise time (mincore), and the
 * number of chunks swept.  Lets the bench read off "is there resident RSS
 * to reclaim" before trusting the RSS A/B.  Counters are process-global. */
void runloom_sched_datastack_sweep_stats(unsigned long long *tail_bytes,
                                      unsigned long long *resident_bytes,
                                      unsigned long long *chunks);

/* Sleep-heap helpers exposed for mn_sched.c's per-hub timer processing.
 * Single-thread drain still uses them via #define aliases. */
runloom_g_t *runloom_sched_sleep_peek(runloom_sched_t *s);
runloom_g_t *runloom_sched_sleep_pop(runloom_sched_t *s);

/* Monotonic clock used by the sleep heap.  Public so hub_main can
 * decide when sleepers are due. */
double runloom_sched_monotonic_seconds(void);

/* Single-thread logical clock (RUNLOOM_LOGICAL_CLOCK, deterministic asyncio-timer
 * replay).  Returns the logical time that sched_sleep deadlines + the aio loop
 * clock are measured against when enabled, else `fallback` (a wall-clock value).
 * See runloom_sched_drain.c.inc. */
double runloom_sched_logical_now_or(double fallback);

/* Time-sliced cooperative preemption (3.13t only).
 *
 * Start a timer thread that posts a Py_AddPendingCall every quantum_us
 * microseconds.  CPython's eval loop checks the pending queue at
 * bytecode back-edges and function calls; when our pending call fires,
 * it invokes runloom_sched_yield() on whichever goroutine is currently
 * running.  Lets goroutines without explicit sched_yield() calls still
 * cooperate -- the Go 1.14 model translated to CPython terms.
 *
 * Returns 0 on success, -1 on error (with a Python exception set).
 * Calling init while already running just updates the quantum.
 * Calling fini stops the timer and joins the thread.
 * Idempotent. */
int runloom_preempt_init(long quantum_us);
void runloom_preempt_fini(void);

#endif /* RUNLOOM_SCHED_H */
