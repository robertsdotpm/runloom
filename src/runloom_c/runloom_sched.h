/* runloom_sched.h -- C-level cooperative scheduler.
 *
 * The Python-side `runloom.fiber(fn)` ultimately creates a fiber here.
 * yield, sleep, run -- all do their bookkeeping in C, calling into
 * Python only to invoke the user's entry function.
 *
 * Single OS thread per scheduler in v0.  Multi-thread is Phase C
 * (free-threaded Python with one scheduler per OS thread, work-stealing).
 *
 * Phase B (this file): per-fiber snapshot of the CPython thread
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

/* Per-fiber stack bounds for an EXPLICIT size override (go/mn_fiber(fn, n)): the
 * caller's size wins over the autosizer but is still clamped to [MIN, MAX].  A
 * wild size would otherwise fail the mmap with MemoryError on the M:N spawn
 * path instead of clamping the way the single-thread path does.  Shared here so
 * both the runloom_sched.c and mn_sched.c translation units agree on one value. */
#define RUNLOOM_MIN_STACK_SIZE       (16  * 1024)         /* 3.13t hard floor */
#define RUNLOOM_MAX_STACK_SIZE       (8   * 1024 * 1024)  /* 8 MiB ceiling */

/* Free-threaded 3.14+ minimum PHYSICAL fiber C-stack.
 *
 * 3.14 replaced the integer c_recursion_remaining counter with an SP-vs-soft_limit
 * guard (see runloom_arm_fiber_stackprot in runloom_iframe.c).  runloom arms it
 * RESERVE bytes earlier than the raw geometry so the datastack-chunk-alloc burst
 * can't punch the guard page -- but RESERVE is clamped to size/2 so a tiny fiber
 * doesn't invert the window, which on a <=128KB fiber collapses the usable window
 * to size - 2*MARGIN (MARGIN = _PyOS_STACK_MARGIN_BYTES = 16KB).  A 64KB fiber then
 * has only 32KB usable -- too small for a legit shallow recursion plus the harness
 * base frames -> a FALSE RecursionError (p226).  grow-on-demand can't rescue it: it
 * grows at headroom < size/4 (16KB on a 64KB fiber) but the guard trips at 32KB, so
 * the guard always wins first.
 *
 * Fix at fiber CREATION (a fresh empty stack -- NOT a live mid-recursion copy-grow,
 * which is the p212 SEGV hazard): floor the requested size at 256KB, where both the
 * chunk-alloc RESERVE (RUNLOOM_STACKPROT_RESERVE_MIN = 96KB) and a usable recursion
 * window fit (eff = 256 - 96 = 160KB usable).  Inert on 3.13 / non-free-threaded
 * (those keep the integer counter and the 16KB floor). */
#if defined(Py_GIL_DISABLED) && PY_VERSION_HEX >= 0x030E0000
#  define RUNLOOM_FT314_MIN_STACK_SIZE  ((size_t)256 * 1024)
#endif

/* Clamp a requested per-fiber C-stack size up to the free-threaded-3.14 floor.
 * A no-op (returns the size unchanged) on every other build. */
static inline size_t runloom_fiber_stack_floor(size_t bytes)
{
#if defined(RUNLOOM_FT314_MIN_STACK_SIZE)
    if (bytes < RUNLOOM_FT314_MIN_STACK_SIZE) bytes = RUNLOOM_FT314_MIN_STACK_SIZE;
#endif
    return bytes;
}

/* Per-fiber CPython thread state snapshot.
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
     * fibers yield while their current_exception is non-NULL and
     * other fibers overwrite it, causing tstate to read a freed/
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
    /* p69 residual UAF: each _PyErr_StackItem in the saved exc_info chain that is
     * NOT the tstate-embedded &exc_state is embedded inside a generator/coroutine
     * object's gi_exc_state.  The mac fix borrowed those items, ASSUMING the g's
     * frames keep the owning gen alive across the suspension -- but the internal
     * traceback.extract_tb generator (_extract_from_extended_frame_gen) is
     * TRANSIENT: it can be exhausted and FREED while the fiber is parked
     * mid-extract_tb (only under preemption), after which snap->exc_info (its
     * gi_exc_state) and the previous_item links into it DANGLE -> the borrowed
     * pointer load re-installs into ts->exc_info is wild -> AV on the next raise.
     * Fix: own a STRONG ref to every gen/coro that owns a chain item across the
     * suspension, so none can be freed.  count==0 in the common case (no in-flight
     * exception, or the only item is &exc_state).  Capacity bounds coroutine
     * nesting depth captured; deeper chains fall back to leaving the head item's
     * owner unpinned only past the cap (vanishingly rare, see snap). */
    PyObject *exc_owners[8];
    int exc_owner_count;
#endif
#if PY_VERSION_HEX >= 0x030B0000 && PY_VERSION_HEX < 0x030C0000
    /* 3.11: single recursion counter, named recursion_remaining. */
    int recursion_remaining;
#endif
#if PY_VERSION_HEX >= 0x030E0000
    /* 3.14: unified counter (c_recursion_remaining removed; C-stack overflow is
     * an SP-based check, set per-fiber in runloom_coro_resume). */
    int py_recursion_remaining;
#elif PY_VERSION_HEX >= 0x030C0000
    /* 3.12-3.13: split into Python-level and C-level counters. */
    int py_recursion_remaining;
    int c_recursion_remaining;
#endif
#if PY_VERSION_HEX >= 0x030B0000
    /* Per-fiber sys.setprofile / sys.settrace hooks (BUG #11).  These are
     * tstate-global, so without snap/restore a hook one fiber installs
     * leaks onto every other fiber sharing the hub (and is cleared from
     * under it on resume).  Saved/restored so each fiber carries its own. */
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
#if PY_VERSION_HEX >= 0x030E0000
    /* 3.14 free-threaded: head of this fiber's _PyThreadStateImpl.c_stack_refs
     * list (the per-thread-state chain of _PyCStackRef nodes the FT GC walks in
     * gc_visit_thread_stacks).  Those nodes live on the fiber's OWN C stack, so
     * they MUST be privatised across a swap -- left linked into the shared hub
     * tstate, a sibling fiber's stack reuse corrupts the list and the GC SIGSEGVs
     * (the p77_weakref_storm crash).  Taken (and the shared tstate cleared) at
     * snap, restored at load.  Borrowed: the nodes belong to the parked fiber's
     * preserved stack; no refcounting (the _PyStackRef inside each node is the
     * eval loop's borrowed temporary, not an owned ref this snap manages).
     * void* because runloom_sched.c is non-core (the type is internal-only;
     * runloom_iframe.c does the typed access). */
    void *c_stack_refs;
#endif
    /* DIAG (RUNLOOM_DIAG_MIGRATE): the PyThreadState bound when this snap was
     * saved -- i.e. the tstate pointer baked into the g's suspended CPython
     * eval-loop C frame.  A cross-hub resume loads the snap onto a DIFFERENT
     * bound tstate; if current_frame != NULL the eval loop still threads this
     * origin_tstate while the bound tstate differs -> the H>=2 corruption.
     * Borrowed pointer; compared, never dereferenced through here. */
    PyThreadState *origin_tstate;
};

/* One fiber (the "G" in Go's M:P:G nomenclature).
 *
 * Lifetime: refcounted.  Two parties hold refs:
 *   - the scheduler, while g is in the ready queue or sleep heap
 *   - the RunloomG Python wrapper, while the user holds it
 * Both decrement on release; the g is freed when both are gone.
 */
/* C-only entry point.  Set on a g spawned via runloom_mn_fiber_c (no Python
 * callable).  When set, runloom_g_entry calls c_entry(c_arg) instead of
 * PyObject_CallNoArgs(callable).  Used by the C test harness in
 * tests_c/ to exercise the M:N + netpoll core without the Python
 * interpreter, so sanitizers / valgrind have a clean view. */
typedef void (*runloom_c_entry_fn)(void *);

struct runloom_g {
    runloom_coro_t *coro;
#ifdef RUNLOOM_DBG_BADCORO
    /* Source-hunt canary (offset 8, in the corrupted region).  Set 0x600DC0DE
     * by slab_alloc, 0xDEAD by slab_free.  At the drain a `sub` reading
     * 0x600DC0DE is a live g; 0xDEAD is a freed-but-not-reused g (UAF);
     * anything else is reused memory (e.g. a coro stack overwrote it). */
    unsigned int magic;
#endif
    /* Stack-advice kind key (FNV hash of the entry callable's identity), or 0.
     * Set at spawn only while stack-advice profiling is enabled; read at
     * completion to fold this g's C-stack HWM into its kind.  See
     * runloom_stackadvice.c. */
    size_t advice_key;
    /* Resolved C-stack size requested at spawn, in bytes.  In the default
     * scheduler the coro + guarded stack are NOT created at spawn but lazily at
     * first resume (on the hub that runs the fiber), so this carries the size
     * across.  Set at spawn; read once by the first-run install in hub_main. */
    size_t req_stack_size;
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
    /* Caller-asserted "this fiber will never yield".  Spawned via
     * runloom_sched_spawn_noyield (Python: runloom_c.fiber_noyield(fn)).
     * When set, drain skips the per-g datastack install + drain +
     * sched_snap load/resnap dance, because g runs to completion
     * within one resume + uses the scheduler's existing Python state
     * without leaving anything behind.  Saves ~150-400 ns per g
     * lifetime depending on workload.
     *
     * If a noyield-marked g actually yields (calls sched_yield,
     * sched_sleep, wait_fd, or any monkey-patched I/O), the result
     * is undefined -- frames will alias across fibers.  Use
     * only for pure-compute callables. */
    int noyield;
    /* PCT (Probabilistic Concurrency Testing) priority -- 0 = unassigned;
     * only read/written on the single-hub ready path when RUNLOOM_PCT_SEED is
     * set (testing only, zero cost otherwise).  See runloom_sched.c. */
    int pct_prio;
    /* PCT FIFO marker: when set, PCT must NOT reorder this g relative to other
     * FIFO-marked gs -- it preserves their spawn (ready-ring) order.  The aio
     * bridge marks every _fiber_io fiber (call_soon callbacks, task steps,
     * io/timer drivers) so PCT respects asyncio's call_soon-FIFO contract
     * instead of permuting it (which was a false positive, not a bug -- asyncio
     * scheduling has no legal reordering freedom).  PCT still freely interleaves
     * un-marked raw fibers/channels in a mixed program.  Slab-zeroed to 0
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
    /* Set to the in-flight single io_uring op (runloom_iouring_op_t *, void* to
     * avoid include cycle) while this fiber is parked on it under M:N, so a
     * task.cancel can submit an ASYNC_CANCEL targeting it.  Cleared on resume.
     * In the slab-cleared pre-state range. */
    void *iouring_op;
    /* park_safe / wake_safe lost-wake guard.  Set to 1 by park_safe
     * just before runloom_coro_yield; CAS'd back to 0 by the first
     * wake_safe to observe it.  Replaces the original s->current
     * check, which read tstate state across threads -- a cross-thread
     * wake_safe (e.g., from a hub thread processing an iouring CQE
     * for a fiber parked on the single-thread sched) could
     * observe s->current==g (still set by drain) and skip the push,
     * losing the wake.  The CAS-based handoff is independent of any
     * tstate observation and gives wake_safe a deterministic "did we
     * own the wake?" answer regardless of caller thread. */
    int parked_safe;
    /* Hub the fiber was parked on by runloom_park_generic (the generic
     * in-memory park), or NULL if parked on the single-thread sched.  Read
     * (ACQUIRE) by RunloomG.wake to route the wake: a non-NULL hub -> the M:N
     * runloom_mn_wake_g re-queue; NULL -> the single-thread runloom_sched_wake_safe.
     * Set only by runloom_park_generic; a g never park_generic'd leaves it NULL
     * (it is woken by its own parker -- netpoll/chan -- not via this field). */
    void *park_hub;
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

    /* ---- bulk-arena ownership (fiber_n) ----
     * When `arena` is set, this g, its coro, and its stack are SLICES of a bulk
     * arena (one calloc / one mmap for the whole batch), NOT individually
     * malloc'd.  Its final decref must therefore NOT runloom_coro_destroy the coro
     * nor runloom_g_slab_free the g (either would free()/pool a slice -> heap
     * corruption).  Instead it decrements the owning batch's live count; the
     * LAST fiber to finish tears the whole batch down (free the g/coro
     * arenas, MADV_DONTNEED the stack block).  0 for every normal fiber.
     * Both live BEFORE the id introspection block so slab reuse clears them. */
    unsigned char arena;
    struct runloom_fibern_batch *batch;

    /* fiber_n(indexed=True): call the entry as fn(index) rather than fn().  The
     * index is stashed in c_arg (a void*, unused on the Python-callable path
     * since c_entry is NULL there); g_entry builds the PyLong lazily on the hub.
     * 0 = fn() (slab-cleared default). */
    unsigned char pass_index;

    /* Wait-reason taxonomy (see runloom_wait_reason in runloom_gstate.h).  Both
     * live in the slab-cleared range so a recycled fiber starts at WR_NONE.
     * `wait_reason` is the active reason read by the dump while parked;
     * `wait_reason_hint` is the pending reason a higher-level primitive sets
     * before parking, consumed (and cleared) at park_safe.  Diagnostic only. */
    unsigned char wait_reason;
    unsigned char wait_reason_hint;

    /* Deep-surface migration oracle (RUNLOOM_DBG_MIGRATE; per-g-tstate only).  The
     * OS-thread id whose mimalloc heap / brc / QSBR the per-g PyThreadState is
     * currently bound to (0 = not yet bound).  Each per-g attach checks it against
     * the running hub's thread id: a mismatch means the tstate migrated hub->hub
     * WITHOUT the mimalloc abandon/adopt re-bind handshake -- the precise, early
     * signature of the deferred _mi_page_retire corruption (RunloomTstateMigration.tla
     * proves the handshake necessary; this is its runtime fidelity oracle).  In the
     * slab-cleared [arena,id) range so a recycled g starts unbound. */
    unsigned long tstate_owner_tid;

    /* ---- introspection block (runloom_introspect.c) ----
     * These fields are deliberately the LAST members of runloom_g_t.  The
     * per-thread slab reuse path (runloom_g_slab_alloc) clears a g only up to
     * offsetof(runloom_g_t, id) -- i.e. everything BEFORE this block -- in two
     * memsets straddling the atomic `state` byte (see the reuse branch), so
     * reg_prev/reg_next survive recycling untouched.  That is what
     * keeps the global fiber registry walkable without taking the
     * registry lock on the (hot) spawn path: the only writers of reg_prev/
     * reg_next are runloom_greg_link/unlink, both under runloom_greg_lock, on the
     * cold OS-alloc / slab-overflow-free paths.  See the reuse branch in
     * runloom_sched_core.c.inc and the field-ordering contract there. */

    /* Per-incarnation fiber id (Go's goid analogue).  Assigned fresh
     * at each spawn from a block-batched global counter; unique + always
     * positive for the life of the process.  Read by the dump, written at
     * spawn with an atomic store.  0 until first spawned.  `long long` (not
     * uint64_t) so the MSVC _Generic atomic shim has a matching slot --
     * uint64_t is `unsigned __int64` there, which the shim doesn't name. */
    long long id;
    /* Monotonic-ns timestamp of the last state transition into a PARKED_*
     * state, stamped only when introspection timestamping is enabled
     * (runloom_introspect_set_timestamps / RUNLOOM_INTROSPECT_TIME).  Lets the
     * dump report "parked for 45.2s" to spot a wedged fiber.  -1 when
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
    /* Set to 1 at spawn when this fiber was admitted under an active
     * max-fibers limit, so its final decref knows to release the slot.
     * Travels with the g so toggling the limit can't unbalance the counter. */
    unsigned char limit_counted;
    /* Intrusive doubly-linked global registry of every live g STRUCT
     * (live + slab-cached).  Linked once when the struct is first OS-
     * allocated, unlinked only when it is returned to the OS.  A cached
     * (RUNLOOM_GST_FREED) g stays linked; the dump skips FREED entries. */
    runloom_g_t *reg_prev;
    runloom_g_t *reg_next;
#ifdef RUNLOOM_DBG_BADCORO
    /* Source-hunt: track the last writer of ->next so a corrupt chain link can
     * be attributed to a code site (1=hub_submit, 2=drain clear, 3=slab_free,
     * 4=init drain) and a post-write WILD overwrite distinguished (recorded
     * next_wval != the value actually observed in ->next). */
    int                next_wsite;
    void              *next_wval;
    unsigned long long next_wseq;
#endif
};

#ifdef RUNLOOM_DBG_BADCORO
extern unsigned long long runloom_next_wseq_ctr;
#define RUNLOOM_NEXT_SET(gp, val, site) do {                                  \
        runloom_g_t *runloom_ns_g = (gp);                                     \
        void        *runloom_ns_v = (void *)(val);                            \
        runloom_ns_g->next       = (runloom_g_t *)runloom_ns_v;               \
        runloom_ns_g->next_wsite = (site);                                    \
        runloom_ns_g->next_wval  = runloom_ns_v;                              \
        runloom_ns_g->next_wseq  =                                            \
            __atomic_add_fetch(&runloom_next_wseq_ctr, 1, __ATOMIC_RELAXED);  \
    } while (0)
#else
#define RUNLOOM_NEXT_SET(gp, val, site) ((gp)->next = (runloom_g_t *)(val))
#endif

/* Park current g until runloom_sched_wake_g(g) is called.  Race-safe:
 * a wake that arrives BEFORE the park (because the future fires
 * synchronously, e.g. add_done_callback on an already-done future)
 * makes the park a no-op and the fiber continues. */
void runloom_sched_park_safe(void);

/* Wake a fiber previously parked via runloom_sched_park_safe.  Safe
 * to call before park (wake_pending counter records the arrival). */
void runloom_sched_wake_safe(runloom_g_t *g);

/* Lifetime helpers. */
void runloom_g_incref(runloom_g_t *g);
void runloom_g_decref(runloom_g_t *g);

/* fiber_n bulk-arena batch teardown: called by an arena g's final decref instead
 * of free()ing the g/coro/stack slices individually.  Decrements the batch's
 * live count; the LAST fiber to finish frees the g + coro arenas and
 * MADV_DONTNEEDs the stack block.  Defined in mn_sched_init_fini.c.inc. */
struct runloom_fibern_batch;
void runloom_fibern_batch_finish_one(struct runloom_fibern_batch *b);

/* Acquire a reference ONLY if the g is still live (refcount > 0).  Returns
 * 1 on success (caller now owns a ref, must decref), 0 if the g is already
 * being torn down (refcount reached 0).  CAS loop; used by the fiber
 * dump to pin a g found via the registry without resurrecting one that a
 * concurrent final decref is mid-freeing. */
int runloom_g_try_incref(runloom_g_t *g);

/* Slab allocator for runloom_g_t -- per-thread LIFO free list with cap.
 * Exposed so mn_sched.c can share the same recycle pool as the
 * single-thread spawn path.  alloc returns a zeroed g (or NULL +
 * PyErr_NoMemory on OOM); free returns to the slab. */
runloom_g_t *runloom_g_slab_alloc(void);
void runloom_g_slab_free(runloom_g_t *g);

/* M:N g-slab reclamation (defined in runloom_sched_core.c.inc; called from
 * mn_sched.c).  An exiting hub thread splices its OWN TLS slab into the shared
 * global pool via thread_flush() before it dies; the main thread, after joining
 * every hub, calls reclaim() to fold in its own slab and free the entire global
 * pool back to the OS.  Without these the per-fiber g-structs cached in each
 * hub's TLS slab leak across every mn_init/mn_fini cycle. */
void runloom_g_slab_thread_flush(void);
void runloom_g_slab_reclaim(void);

/* Per-OS-thread scheduler. */
/* One entry in a sched's TIMER heap: an in-memory timed park (runloom_c.park
 * with a timeout) that must be woken at `deadline` (monotonic seconds) if a real
 * wake_safe has not already done so.  Self-contained by VALUE (no g->wake_at
 * reuse, unlike the sleep heap), so a g may have several STALE entries in flight
 * across re-parks -- a stale entry that pops just causes at most ONE spurious
 * wake (the parker re-checks the clock and re-parks), never a premature timeout:
 * the parker decides timed-out from the clock on resume, NOT from the timer. */
typedef struct {
    double       deadline;   /* wake deadline (monotonic seconds -- or census-
                              * LOGICAL seconds when `logical` is set, I4) */
    runloom_g_t *g;          /* the timed-parked fiber */
    int          logical;    /* I4: deadline is on the census logical plane.
                              * The census folds ONLY logical entries into its
                              * advance target -- folding a monotonic (wall-
                              * uptime-scale) value would poison the logical
                              * clock (review-caught).  Mixed heaps stay sound:
                              * monotonic values sort after logical ones and,
                              * under sim, never fire (wall timeouts do not
                              * exist there -- documented). */
} runloom_timer_entry_t;

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
    /* Timer heap -- min-heap by deadline for in-memory TIMED parks
     * (runloom_park_generic_timed).  Separate from the sleep heap so a g can hold
     * multiple stale entries safely (by-value entries, no g->wake_at reuse).
     * 1-indexed; index 0 unused.  Drained alongside the sleep heap. */
    runloom_timer_entry_t *timer_heap;
    Py_ssize_t timer_size;
    Py_ssize_t timer_cap;
    /* Default stack size for new gs. */
    Py_ssize_t stack_size;
    /* Goroutines completed since the last sched_drain. */
    Py_ssize_t completed;
    /* When set, sched_drain returns. */
    int stopping;
    /* A Python signal-handler exception (normalized, traceback attached) the
     * idle scheduler grab ran and is handing to a fiber parked in wait_fd:
     * set just before runloom_netpoll_signal_wake re-queues that g, consumed (taken
     * + cleared) by runloom_netpoll_wait_fd when it resumes on the
     * RUNLOOM_NETPOLL_SIGNALED sentinel.  Owned ref while set; NULL otherwise. */
    PyObject *signal_exc;
    /* Count of THIS sched's fibers currently parked in netpoll (non-hub
     * parkers whose g->owner == this sched).  Bumped in runloom_parker_link /
     * unlink.  The drain loop uses this -- NOT the global parked count -- so a
     * fiber parked on another (or a dead) OS thread can't keep this
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
    /* Quiescence-barrier wait list (single-thread sched only) -- fibers
     * parked by runloom_sched_run_ready().  FIFO, threaded through g->next (free
     * for a live g on this sched; the slab free-list and M:N hub queues are
     * the only other users of g->next and neither overlaps here).  The drain
     * loop flushes the WHOLE list back to ready at the next quiescence point
     * (ready empty, just before it would block on netpoll/timers), giving
     * "resume me once no other fiber is immediately runnable" -- asyncio's
     * one-loop-iteration semantics, iterated to quiescence. */
    runloom_g_t *quiescence_head;
    runloom_g_t *quiescence_tail;
};

/* Is the ready queue empty?  Hot-path predicate; inline-friendly.
 * ready_head/ready_tail are written non-atomically by the OWNING hub
 * (ready_push/ready_pop), but this predicate is also read CROSS-THREAD by the
 * deadlock-quiescence census (runloom_mn_has_wakeable_work, on the main thread),
 * so the indices are relaxed-atomic -- the lone plain access among that census's
 * atomic siblings (resume_start_ns / sub_head / global_runq_len), matching the
 * missing-atomic-qualifier fixes in tools/README Finding C.  Found by
 * tools/lifefuzz under the gold-standard TSan ext (datastack:267/403 vs sched.h
 * here).  RELAXED: single-writer per ring; the census only needs a non-torn
 * value (its deadlock streak + re-kick absorb staleness).  Zero-cost on x86. */
RUNLOOM_INLINE int runloom_sched_ready_empty(const runloom_sched_t *s) {
    return __atomic_load_n(&s->ready_head, __ATOMIC_RELAXED)
        == __atomic_load_n(&s->ready_tail, __ATOMIC_RELAXED);
}

/* Module-level: one sched per OS thread once Phase C lands.  For now
 * a single global. */
runloom_sched_t *runloom_sched_get(void);

/* Non-allocating: the g running on this thread's single-thread sched, or NULL. */
runloom_g_t *runloom_sched_peek_current(void);

/* Spawn a new fiber.  Returns a NEW reference to a RunloomG Python
 * object (the wrapper around runloom_g_t).  Stealing the callable. */
PyObject *runloom_sched_spawn(runloom_sched_t *s, PyObject *callable);

/* Spawn a fiber marked as "noyield" -- caller asserts the
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
 * calibration; subsequent fibers spawn at the requested size. */
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

/* After fork(): re-init the calibration lock in the child.  The child inherits
 * runloom_cal_lock in whatever state it had at fork -- possibly LOCKED by a
 * thread that did not survive -- so any later calibration acquire (cal_record /
 * get/set_default_stack_size / stack_stats) would deadlock.  Mirrors
 * runloom_global_runq_lock in runloom_mn_reset_after_fork.  Call from the
 * after-fork-in-child handler. */
void runloom_cal_reset_after_fork(void);

/* After fork(): re-init the cross-hub g-slab balance lock in the child (it may
 * be inherited held by a dead hub mid-splice) and abandon the bounded inherited
 * batch.  Wired into runloom_after_fork_child.  Non-hot-path. */
void runloom_g_global_reset_after_fork(void);

/* After fork(): re-init the FCONTEXT coro cross-hub balance lock in the child
 * (no-op on non-FCONTEXT backends).  Wired into runloom_after_fork_child. */
void runloom_coro_reset_after_fork(void);

/* Test-only hooks (runloom_c._test_*) for the fork-deadlock regression test. */
void runloom_g_global_test_hold_ns(long long ns);
void runloom_g_global_test_acquire(void);

/* Yield the current g.  Re-queues on the ready FIFO, swaps back to
 * the scheduler stack.  Must be called from inside a g. */
void runloom_sched_yield(runloom_sched_t *s);

/* Park the current g until wake_at (monotonic seconds).  Swap back. */
void runloom_sched_sleep_until(runloom_sched_t *s, double wake_at);
/* As above, but a WALL-clock deadline the logical clock won't advance for
 * (the aio keepalive heartbeat -- must not ride RUNLOOM_LOGICAL_CLOCK). */
void runloom_sched_sleep_until_real(runloom_sched_t *s, double wake_at);

/* Park the current g on the quiescence-barrier list; the drain loop resumes
 * it once no other fiber is immediately runnable (ready empty), before
 * blocking on netpoll/timers.  asyncio "one loop iteration" iterated to
 * quiescence.  Single-thread sched only; a no-op if not inside a g. */
void runloom_sched_run_ready(runloom_sched_t *s);

/* Mark current g as parked (no ready_push); netpoll/sleep saves snap.
 * Caller must then yield via runloom_coro_yield. */
void runloom_sched_park_current(void);

/* Generic in-memory park: park the current fiber with NO fd, routing to the
 * M:N hub park (park_current + coro_yield) or the single-thread park_safe by hub
 * presence, and recording g->park_hub so RunloomG.wake routes the wake.  Returns
 * 0 on success, -1 if not in a fiber.  foreign_wakeable arms the shared
 * run-alive anchor so a foreign-OS-thread waker cannot race a single-thread
 * run()'s exit.  This is the M:N-correct replacement for park_self (which busy-
 * loops on a hub). */
int runloom_park_generic(int foreign_wakeable);

/* Timed variant of runloom_park_generic: park the current fiber IN MEMORY (0
 * fds) until a wake_safe OR the monotonic-seconds `deadline`, whichever first.
 * Same Dekker handshake as runloom_park_generic; the deadline is a TIMER-heap
 * entry on the current sched/hub that the drain fires by CASing parked_safe (the
 * SAME exactly-once arbiter as wake_safe).  The parker decides the result from
 * the CLOCK on resume -- returns 1 if monotonic() >= deadline (timed out), else 0
 * (woken); -1 if not in a fiber.  A spurious early return is possible (a
 * real wake, or a stale timer entry from a prior re-park); the caller must
 * re-check its own deadline, exactly as the fd-backed wait_fd(timeout) path did. */
int runloom_park_generic_timed(int foreign_wakeable, double deadline);

/* Sim-plane twin (I4): entry expiry + resume verdict on the census logical
 * clock (a logical deadline vs monotonic uptime is expired-at-birth).  m_park
 * routes here under an armed sim census. */
int runloom_park_generic_timed_logical(int foreign_wakeable, double deadline);

/* --- runloom_park_until: the unified predicate-park kernel (item 1) ----------
 * The single register->recheck->park loop every cooperative primitive
 * (Future/WaitGroup/CoEvent/CoCondition/chan) hand-rolls today.  arm(ctx)
 * registers the current fiber on the wake source; pred(ctx) returns 1 when the
 * wait is satisfied; the source's signal path must wake this g (mn_wake_g /
 * wake_safe) when it makes pred true -- park_generic's Dekker then catches a
 * wake that lands between arm and commit.  disarm(ctx) unregisters on exit.
 * `deadline` < 0 means untimed.  See docs/dev/PARKWAKE_KERNEL_DESIGN.md.
 *
 * Increment 1 supports the LOCKLESS-Dekker form only (the sync primitives); the
 * lock-based form (chan, which serialises register/wake under a channel lock) is
 * a later increment and is documented in the design.  Returns: */
#define RUNLOOM_PARK_READY      0   /* pred satisfied */
#define RUNLOOM_PARK_TIMEOUT    1   /* deadline passed with pred still false */
#define RUNLOOM_PARK_CANCELLED (-1) /* not in a fiber / OOM arming the timer */

typedef int  (*runloom_pred_fn)(void *ctx);
typedef void (*runloom_arm_fn)(void *ctx);

int runloom_park_until(runloom_pred_fn pred, runloom_arm_fn arm,
                       runloom_arm_fn disarm, void *ctx,
                       int foreign_wakeable, double deadline);

/* The LOCK-BASED form of the park kernel (item 1): for primitives (chan) whose
 * wake side serialises register/deliver under a lock rather than the Dekker.
 * The caller holds `lock` on entry and has ALREADY armed (linked its waiter);
 * this releases `lock` across each coro_yield and re-takes it to re-check pred
 * (pred is read under the lock), looping until pred holds -- absorbing spurious
 * wakes (a stale dup wake_g resuming a still-queued waiter).  `gstate` is the
 * parked-state label for diagnostics.  Returns with `lock` HELD (the caller
 * unlocks).  Behaviour-identical to the hand-rolled chan park loop. */
int runloom_park_until_locked(runloom_pred_fn pred, void *ctx,
                              runloom_mutex_t *lock, int rank, int gstate);

/* Fire all due TIMER-heap entries on s (deadline <= now), claiming each via the
 * parked_safe CAS.  Called by the single-thread drain + the M:N hub_main on s's
 * own thread, alongside the sleep-heap drain. */
void runloom_sched_drain_timers(runloom_sched_t *s, double now);
/* Release the g-ref held by every still-pending timer entry + empty the heap.
 * Call at teardown so future-deadline entries don't leak their pinned gs. */
void runloom_sched_release_timers(runloom_sched_t *s);

/* Earliest pending timer deadline on s (monotonic seconds), or -1.0 if none --
 * lets the drain / hub idle-wait bound its block so a due timer fires on time. */
double runloom_sched_timer_next_deadline(runloom_sched_t *s);

/* Shared (process-wide) run-alive anchor for foreign-wakeable in-memory parkers:
 * an in-memory park leaves no fd/sleep/inflight footprint, so without this a
 * single-thread run() would exit and abandon a fiber a foreign thread is
 * about to wake.  acquire (before park) / release (after resume) bracket the
 * park; inflight() is read by the single-thread drain loop's exit condition.
 * One counter + the shared wake-pump eventfd, never per-waiter. */
void runloom_foreign_park_acquire(void);
void runloom_foreign_park_release(void);
long runloom_foreign_park_inflight(void);

/* Re-queue a previously-parked g onto the ready list. */
void runloom_sched_wake(runloom_g_t *g);

/* Drive the scheduler until ready+sleep queues are empty.  Returns
 * the number of completed fibers. */
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

/* Per-fiber-tstate mode (RUNLOOM_PER_G_TSTATE).  When on, runloom_pystate_snap
 * no-ops so each g's own tstate is never swapped out; mn_sched runs the
 * tstate-attach/detach path instead.  Set by mn_init, cleared by mn_fini. */
void runloom_set_per_g_tstate_mode(int on);
int  runloom_get_per_g_tstate_mode(void);

/* The user's callable trampoline for a fiber; installs an initial
 * root cframe / current_frame on g's own stack, then runs g->callable.
 * Exposed so mn_sched.c can reuse the same entry (Phase B correct). */
void runloom_g_entry(void *user);

/* Free the datastack-chunk chain owned by the just-completed fiber.
 * Call AFTER runloom_coro_resume returns done=true and BEFORE loading any
 * other snapshot back into tstate (which would overwrite the chunk
 * pointers and leak the g's allocation).  Matches greenlet's did_finish.
 *
 * Returned chunks go to a per-thread pool (capped) so the next first-run
 * g can pick one up via runloom_first_run_install_datastack instead of
 * paying for an arena alloc. */
void runloom_drain_g_datastack(void);

/* Datastack-chunk graveyard reclamation (defined in runloom_sched_pystate.c.inc).
 * thread_flush: an exiting hub splices its TLS chunk reuse-pool + grace-ring into
 * a shared graveyard; reclaim: the main thread frees the graveyard at mn_fini
 * after all hubs join.  Without these, each hub's pooled 16 KB datastack chunks
 * leak with the thread -- the dominant M:N per-fiber RSS growth across run cycles. */
void runloom_chunk_pool_thread_flush(void);
void runloom_chunk_pool_reclaim(void);

/* Set up tstate->datastack_{chunk,top,limit} for a first-run g.  Pulls
 * a chunk off the per-thread pool if available; otherwise leaves the
 * fields NULL so PyEval will arena-allocate.  Either is correct. */
void runloom_first_run_install_datastack(void);

/* Reclaim the idle tail of a parked Python fiber's datastack chunk.
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

/* Deterministic simulated-I/O mode (RUNLOOM_SIM, Slice 2).  Cached read-once;
 * when set, netpoll routes its deadline clock through the logical clock and its
 * pump runs the sim model loop.  RUNLOOM_SIM also implies the logical clock. */
int runloom_sim_enabled(void);

/* The single-thread logical clock in NANOSECONDS (netpoll deadline baseline). */
long long runloom_sched_logical_ns(void);

/* Reset the logical clock to 0 (runloom_sim_reset only; between run()s). */
void runloom_sched_logical_reset(void);

/* Slave the logical clock to an externally-advanced ns instant -- the mn census
 * advance mirrors through here so every plane reads ONE clock (MN_SIM_DST_PLAN
 * I1).  Never called on the H=1 legacy path. */
void runloom_sched_logical_set_ns(long long ns);

/* Native mn-sim foreign-wake tripwire (MN_SIM_DST_PLAN.md I3, wake contract
 * #13/#15): total foreign-thread wakes observed during seeded runs (green run
 * == 0); note() counts + aborts under RUNLOOM_SIM_STRICT=1 (default when
 * sim+mn armed); ctx() is the predicate (armed sim census + no hub TLS). */
long long runloom_sim_foreign_wake_total(void);
void runloom_sim_foreign_wake_note(const char *what);
void runloom_sim_foreign_wake_reset(void);
int runloom_sim_foreign_wake_ctx(void);

/* Sim-only: advance the logical clock to the earliest pending deadline across
 * the scheduler's logical sleep heap and the netpoll deadline heaps
 * (netpoll_min_ns, -1 if none).  Returns the ns advanced to (for the netpoll
 * expiry compare) or -1 if nothing is pending.  See runloom_sched_drain.c.inc. */
long long runloom_sched_sim_advance_clock(runloom_sched_t *s, long long netpoll_min_ns);

/* Time-sliced cooperative preemption (3.13t only).
 *
 * Start a timer thread that posts a Py_AddPendingCall every quantum_us
 * microseconds.  CPython's eval loop checks the pending queue at
 * bytecode back-edges and function calls; when our pending call fires,
 * it invokes runloom_sched_yield() on whichever fiber is currently
 * running.  Lets fibers without explicit sched_yield() calls still
 * cooperate -- the Go 1.14 model translated to CPython terms.
 *
 * Returns 0 on success, -1 on error (with a Python exception set).
 * Calling init while already running just updates the quantum.
 * Calling fini stops the timer and joins the thread.
 * Idempotent. */
int runloom_preempt_init(long quantum_us);
void runloom_preempt_fini(void);

#endif /* RUNLOOM_SCHED_H */
