/* mn_sched.c -- M:N scheduler.
 *
 * N OS threads, each one a "hub" with its own pygo_sched_t and a
 * Chase-Lev work-stealing deque.  Goroutines spawned by pygo_mn_go
 * are round-robined onto hubs at spawn time; once running, a g is
 * pinned to its hub (its C stack is absolute address).
 *
 * Two queues per hub:
 *   - Chase-Lev deque   (h->deque)        -- FRESH gs only.  Stealable.
 *   - Local FIFO        (h->sched.ready)  -- YIELDED gs.  Hub-pinned.
 *
 * A g moves between the two: it lives in the deque until first
 * resume, may be stolen by another hub.  Once it yields, it's pinned
 * to its hub (Phase B snap holds pointers into the g's own stack +
 * datastack chunks; cross-thread migration would require careful
 * tstate-field-by-tstate-field migration that we don't attempt).
 *
 * Work stealing: when a hub's local queues are both empty, it tries
 * to steal a g from a neighbour's deque.  Stolen gs are by
 * construction fresh (never run), so no migration concerns.
 *
 * Phase C v2 (this file): yield support inside hubs.  A goroutine
 * running on hub H can call sched_yield(); the call routes through
 * pygo_mn_yield_current() which pushes the g back to H's local FIFO,
 * snapshots the per-g PythonState, and asm-yields back to hub_main
 * which then loads its own hub_snap and loops to the next g.
 *
 * Free-threaded Python (3.13t) is required to get real parallelism
 * out of this: each hub thread has its own PyThreadState and runs
 * Python code without contending on a global lock.  On a GIL build
 * this still works correctly but serialises through the GIL.
 *
 * What's NOT in v2:
 *   - cross-hub netpoll: each hub has its own epoll fd; a g that
 *     parks on I/O stays on its hub.  A future version could share
 *     a single epoll across hubs and wake whichever hub is idle.
 *   - sleep-in-hub: pygo_sched_sleep_until still uses the global
 *     scheduler's sleep heap.  Hubs don't process timers.
 *   - park-on-eventfd: today hubs busy-loop trying to steal when
 *     local is empty.  A real impl uses futex / eventfd to sleep.
 */
#if !defined(_WIN32)
#  define _POSIX_C_SOURCE 200809L
#  ifndef _GNU_SOURCE
#    define _GNU_SOURCE
#  endif
#endif
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "plat.h"
#include "plat_compat.h"
#include "mn_sched.h"
#include "pygo_sched.h"
#include "netpoll.h"
#include "io_uring.h"
#include "coro.h"
#include "cldeque.h"
#include "pygo_diag.h"
#include "pygo_gstate.h"
#include "pygo_blockpool.h"

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#if !defined(PYGO_OS_WINDOWS)
#  include <unistd.h>
#endif

typedef struct pygo_hub {
    int id;
    pygo_thread_t thread;
    pygo_sched_t sched;
    pygo_cldeque_t deque;
    PyThreadState *tstate;        /* per-hub tstate */
    volatile int stopping;
    volatile long pending;        /* gs ever-pushed minus gs-completed */
    /* MPSC submission list.  Chase-Lev's `push` is owner-only (single
     * producer); when mn_go runs on a non-owner thread, pushing
     * directly to the deque races with the owner's pop and corrupts
     * the deque's `bottom` counter (a missed RMW), causing
     * non-deterministic segfaults under load.  Instead, producers
     * push to this list under a lock; the hub (the single consumer)
     * drains the list into its own deque each iteration, so all
     * deque pushes are done by the deque's owner thread. */
    pygo_mutex_t sub_lock;
    pygo_g_t *sub_head;
    pygo_g_t *sub_tail;
    /* Per-hub io_uring ring.  Created at hub_main entry with
     * IORING_SETUP_SINGLE_ISSUER (and DEFER_TASKRUN if the kernel
     * supports it).  Eventfd registered with the shared netpoll pump.
     * Used by hub-bound recv/send to bypass the global ring's
     * submission mutex and the legacy spin-drain.  NULL if the
     * kernel doesn't have io_uring (5.0 or older) or ring create
     * failed -- callers fall back to the global ring path. */
    pygo_iouring_ring_t *iouring_ring;
    int                  iouring_eventfd;  /* cached for unregister at fini */
    /* Last time this hub ran the idle stack-reclaim sweep (seconds, 0 at
     * init -> first idle sweep fires immediately).  Rate-limits the
     * O(parked) walk under PYGO_STACK_PARK_SWEEP. */
    double               last_sweep_s;
    /* ---- sysmon (Group B) progress instrumentation, PYGO_SYSMON only ----
     * resume_start_ns: monotonic-ns when this hub entered its current
     * pygo_coro_resume; 0 between resumes (idle / looping).  The sysmon
     * watchdog reads it to spot a hub stuck inside a non-yielding blocking
     * call (an UNANTICIPATED block -- the thing Group A's offload doesn't
     * wrap).  resume_g is the g being resumed, for the wedge log line.
     * resume_seq bumps every resume start so the watchdog can tell "same
     * stuck resume" from "made progress" without racing on the ns value.
     * Written by the hub only when pygo_sysmon_enabled (predicted-not-taken
     * off the hot path); read RELAXED by the watchdog (a stale read just
     * delays/!duplicates a report -- harmless for a watchdog). */
    volatile long long   resume_start_ns;
    volatile long        resume_seq;
    pygo_g_t            *resume_g;
} pygo_hub_t;

static pygo_hub_t *pygo_hubs = NULL;
static int pygo_hub_count = 0;
static volatile long pygo_mn_spawn_counter = 0;

/* Global pending-g counter, replacing the per-hub `pending` field
 * for purposes of "is there any work left in the M:N scheduler".
 * Incremented in pygo_mn_go (spawn), decremented in hub_main when a
 * g completes.  Steals do NOT touch this counter (the per-hub field
 * is still updated for diagnostics / future scheduler heuristics,
 * but the steal-time inc-then-dec across hubs created a
 * sum-observed-as-N-1 window where pygo_mn_run could see total=0
 * and exit while a stolen g was still running on the destination
 * hub).  ACQ_REL on both inc and dec; ACQUIRE on the mn_run read
 * pairs with the completion release. */
static volatile long pygo_mn_pending_global = 0;

/* ---- co-located pending counters ----
 *
 * Every change to a hub's pending count either changes the global
 * counter too (spawn, complete) or rebalances between two hubs (steal).
 * Forwarding through these helpers ensures the per-hub and global
 * counters always move atomically together and a future caller cannot
 * accidentally update one without the other.  See the broader
 * counter-co-location sweep in the diag/gstate session for context. */
PYGO_INLINE void pygo_mn_pending_inc(pygo_hub_t *h)
{
    /* spawn: per-hub up, global up.  RELAXED on the per-hub side is
     * fine (only the owning hub reads it for scheduling decisions);
     * ACQ_REL on the global so the mn_run reader sees the spawn
     * before any subsequent completion's release. */
    __atomic_add_fetch(&h->pending, 1, __ATOMIC_RELAXED);
    __atomic_add_fetch(&pygo_mn_pending_global, 1, __ATOMIC_ACQ_REL);
}

PYGO_INLINE void pygo_mn_pending_complete(pygo_hub_t *h)
{
    /* complete: per-hub completed up, per-hub pending down, global down.
     * Order matters: bump completed BEFORE decrementing pending so a
     * mn_run reader that observes pending == 0 (via acquire) is also
     * guaranteed to see the matching completed++ already published. */
    __atomic_add_fetch(&h->sched.completed, 1, __ATOMIC_RELEASE);
    __atomic_sub_fetch(&h->pending, 1, __ATOMIC_RELEASE);
    __atomic_sub_fetch(&pygo_mn_pending_global, 1, __ATOMIC_ACQ_REL);
}

PYGO_INLINE void pygo_mn_pending_steal(pygo_hub_t *victim,
                                              pygo_hub_t *thief)
{
    /* steal: work moves between hubs, total work unchanged.  ACQ_REL
     * on both so the stolen-g's data writes are visible to the
     * destination hub before its later resume. */
    __atomic_sub_fetch(&victim->pending, 1, __ATOMIC_ACQ_REL);
    __atomic_add_fetch(&thief->pending,  1, __ATOMIC_ACQ_REL);
}

/* ---- Global stealable run-queue (PYGO_PER_G_TSTATE only) ----
 *
 * Default mode routes a woken g back to its ORIGIN hub's owner-drained
 * submission list (pygo_mn_hub_submit).  That strands the g whenever the
 * origin hub is stuck in a non-yielding blocking C call (the classic one:
 * libc getaddrinfo): the hub never loops, never drains its sub list, so the
 * woken g never reaches a stealable deque and no idle hub can rescue it.
 * This is asyncio's signature failure -- one blocking task stalling a whole
 * fan-out -- reproduced in pygo's narrow blocking-C-call case.
 *
 * Under PYGO_PER_G_TSTATE a woken g carries its own migratable
 * PyThreadState, so it can resume on ANY hub.  We exploit that: wake_g
 * pushes the woken g onto this process-global queue instead of the origin
 * hub's sub list, and every idle hub drains it in hub_main's empty-local/
 * empty-deque path (alongside neighbour-steal).  An idle hub thus recovers a
 * stalled hub's woken work.  This is Go's per-P-local + global-runq model.
 *
 * Accounting: the g stays counted in its origin hub's `pending` and the
 * global counter while it sits here (we do NOT rebalance per-hub pending on
 * pull).  That is correct because the idle path only ever reads the SUM of
 * per-hub pending, and the sum is conserved regardless of which hub holds the
 * count: spawn does +1 on one hub, completion does -1 on one hub, so the sum
 * always equals the global in-flight count.  pygo_mn_run reads the global
 * counter, which a queued g keeps non-zero until it actually completes.
 *
 * Single-owner safety (the load-bearing part).  ONE per-g state machine
 * (g->wake_state, see the field comment in pygo_sched.h) makes the MPMC queue
 * safe without per-entry refcounting and without the duplicate/double/lost-wake
 * hazards.  An earlier two-flag design (an exactly-once-wake dedup + a separate
 * exclusive-resume claim, with a re-push on claim contention) raced into a
 * livelock: the re-pushed entry survived the owner's next park (which re-armed
 * the wake flag), so a fresh wake made a SECOND entry, and the two entries
 * double-claimed and stranded the claim -> the re-push spun forever.  The state
 * machine removes BOTH the re-push and the possibility of two entries:
 *
 *   - wake_g CASes PARKED -> QUEUED and only the winner enqueues; a wake while
 *     the g is QUEUED, RUNNING, or RUNNING_WOKEN does NOT enqueue.  So a g holds
 *     AT MOST ONE entry, ever -- no stale duplicates, no spurious/double resume.
 *   - a hub CASes QUEUED -> RUNNING to claim the g it pulled.  Because the entry
 *     it pulled is the queue's SOLE reference to that g (at-most-one-entry) and
 *     only the entry's holder runs this CAS, the CAS cannot lose -> no re-push.
 *   - while RUNNING the g is owned by exactly one hub; a wake only flips RUNNING
 *     -> RUNNING_WOKEN (remembered, not enqueued), so no other hub can pull or
 *     attach the g's live tstate during the commit->detach window.  The owner,
 *     after detaching the tstate, ends the resume by CASing RUNNING -> PARKED,
 *     or, if a wake landed, RUNNING_WOKEN -> QUEUED and enqueues exactly once.
 *
 * "One entry per park" and "one resumer" are now the SAME invariant (QUEUED
 * holds the entry; RUNNING holds the owner; they are distinct states), which is
 * why no re-push is needed and no duplicate can form.  Each entry takes one
 * queue ref (incref at enqueue, decref at consume/drain); at most one entry
 * references a g, so the proven sub_list/deque single-ref lifetime model holds.
 *
 * MPMC: many producers (any thread's wake_g) and many consumers (idle hubs).
 * Mutex-protected singly-linked list through g->next -- the same field the
 * per-hub sub list uses, safe because a given g is in exactly one of {sub
 * list, global runq} at a time (a fresh g goes to its hub's sub list; a woken
 * g under per-g-tstate comes here, long after it left the sub list).
 *
 * Process-lifetime lock: POSIX gets a static initialiser (usable with no
 * init/destroy, survives mn_init/mn_fini cycles); Windows can't statically
 * init a CRITICAL_SECTION, so mn_init does a one-time init there. */
static pygo_mutex_t  pygo_global_runq_lock = PYGO_MUTEX_STATIC_INIT;
static pygo_g_t     *pygo_global_runq_head = NULL;
static pygo_g_t     *pygo_global_runq_tail = NULL;
static volatile long pygo_global_runq_len  = 0;

/* Link g onto the tail of the global run-queue.  Each entry holds one queue
 * ref (incref'd by the caller -- wake_g, or the RUNNING_WOKEN release) so it
 * keeps the g alive until a hub consumes it.  Enqueue is exactly-once: only the
 * winner of the PARKED->QUEUED (or RUNNING_WOKEN->QUEUED) CAS pushes, so a g is
 * on this queue at most once -- no re-push, no duplicate. */
static void pygo_mn_global_runq_push(pygo_g_t *g)
{
    pygo_mutex_lock(&pygo_global_runq_lock);
    g->next = NULL;
    if (pygo_global_runq_tail != NULL) {
        pygo_global_runq_tail->next = g;
    } else {
        pygo_global_runq_head = g;
    }
    pygo_global_runq_tail = g;
    __atomic_add_fetch(&pygo_global_runq_len, 1, __ATOMIC_RELAXED);
    pygo_mutex_unlock(&pygo_global_runq_lock);
    PYGO_EVT(PYGO_EVT_G_SUBMIT, g, NULL, 0);
    pygo_g_state_set(g, PYGO_GST_SUBMITTED);
}

/* Pop one g from the global run-queue head (NULL if empty).  No per-hub
 * pending rebalance (see header: only the sum matters, and the mutex
 * release/acquire already orders the g's writes for the puller). */
static pygo_g_t *pygo_mn_global_runq_pull(void)
{
    pygo_g_t *g;
    /* Lock-free fast-out: the queue is empty in the common case (default
     * mode never touches it; per-g-tstate only when a wake is in flight),
     * so skip the mutex when there is clearly nothing to take.  A racing
     * push between this load and a lock would just be picked up next loop. */
    if (__atomic_load_n(&pygo_global_runq_len, __ATOMIC_ACQUIRE) == 0) {
        return NULL;
    }
    pygo_mutex_lock(&pygo_global_runq_lock);
    g = pygo_global_runq_head;
    if (g != NULL) {
        pygo_global_runq_head = g->next;
        if (pygo_global_runq_head == NULL) {
            pygo_global_runq_tail = NULL;
        }
        g->next = NULL;
        __atomic_sub_fetch(&pygo_global_runq_len, 1, __ATOMIC_RELAXED);
    }
    pygo_mutex_unlock(&pygo_global_runq_lock);
    return g;
}

/* PYGO_PER_G_TSTATE (default OFF, experimental): give each goroutine its
 * own PyThreadState so its Python execution state is migratable across
 * hubs.  A stolen woken g then resumes on ANY hub without the cross-hub
 * corruption that per-hub-tstate + snap migration hits (the snap bakes the
 * owning hub's tstate into the suspended frames).  Read once; mn_init
 * copies it into pygo_set_per_g_tstate_mode (which stands the snap down).
 * Trades memory (a tstate per g) for a closeable p99.9 tail. */
static int pygo_per_g_tstate_flag(void)
{
    static int v = -1;
    int cur = __atomic_load_n(&v, __ATOMIC_RELAXED);
    if (cur < 0) {
        const char *e = getenv("PYGO_PER_G_TSTATE");
        cur = (e != NULL && e[0] != '0') ? 1 : 0;
        __atomic_store_n(&v, cur, __ATOMIC_RELAXED);
    }
    return cur;
}

/* PYGO_STEAL_WOKEN (default OFF, experimental; Fix B): in the DEFAULT
 * per-hub-tstate scheduler, route a woken g through the global stealable
 * run-queue + the same wake-state machine per-g-tstate uses, so ANY idle hub
 * can resume it -- migrating the g's interpreter state via its `snap` onto the
 * stealing hub's single bound tstate.  Unlike PYGO_PER_G_TSTATE this needs NO
 * extra PyThreadState per g (so it does NOT violate free-threaded CPython's
 * one-tstate-per-OS-thread stop-the-world protocol -- see memory
 * project_pygo_per_g_python_crash); it relies on snap portability (the exc
 * chain is re-rooted onto the target hub's tstate in pygo_pystate_load).  When
 * OFF the default scheduler is byte-unchanged. */
static int pygo_steal_woken_flag(void)
{
    static int v = -1;
    int cur = __atomic_load_n(&v, __ATOMIC_RELAXED);
    if (cur < 0) {
        const char *e = getenv("PYGO_STEAL_WOKEN");
        cur = (e != NULL && e[0] != '0') ? 1 : 0;
        __atomic_store_n(&v, cur, __ATOMIC_RELAXED);
    }
    return cur;
}

/* True when woken gs route to the global stealable run-queue + wake-state
 * machine: either per-g-tstate (migrates via a per-g tstate) or steal-woken
 * (migrates via the per-g snap onto the hub's tstate). */
static int pygo_use_global_runq(void)
{
    return pygo_get_per_g_tstate_mode() || pygo_steal_woken_flag();
}

/* ---- sysmon watchdog (Group B: stalled-hub detection) ----
 *
 * PYGO_SYSMON (default OFF): spawn one extra OS thread that periodically
 * scans every hub's resume_start_ns and logs a hub stuck inside a single
 * pygo_coro_resume longer than PYGO_SYSMON_MS (the unanticipated-block
 * signature -- a goroutine occupying its hub's OS thread with a
 * non-yielding blocking C call that Group A's offload never wrapped).
 *
 * This first cut is DETECT-ONLY (log the wedge + how many gs it strands);
 * the tstate-handoff recovery builds on top once the detector is measured.
 * Gated so the per-resume instrumentation (two stores + a clock read) is a
 * predicted-not-taken branch -- zero cost when the watchdog is off. */
/* PyThreadState->state values (cpython/pystate.h documents the field;
 * the numeric values live in the internal pycore_pystate.h).  Mirrored here
 * so the watchdog can classify a wedge WITHOUT the Python C-API: it reads
 * h->tstate->state to tell a handoff-RECOVERABLE stall (the running g
 * released the tstate via Py_BEGIN_ALLOW_THREADS -> DETACHED, a well-behaved
 * blocking-IO call a standby thread can take over) from an un-recoverable one
 * (ATTACHED = CPU-bound bytecode or a raw tstate-holding syscall; SUSPENDED =
 * a stop-the-world is in progress -- never adopt). */
#define PYGO_TS_DETACHED  0
#define PYGO_TS_ATTACHED  1
#define PYGO_TS_SUSPENDED 2

static int       pygo_sysmon_enabled = 0;
static long long pygo_sysmon_wedge_ns = 50LL * 1000000LL;   /* 50 ms default */
static long long pygo_sysmon_tick_ns  = 10LL * 1000000LL;   /* scan every 10 ms */
static pygo_thread_t pygo_sysmon_thread;
static int       pygo_sysmon_running = 0;   /* 1 once the thread is spawned */
static volatile int pygo_sysmon_stop = 0;

/* Mark the start of a coro resume on this hub (sysmon progress beat).
 * resume_seq is bumped FIRST so a watchdog that samples mid-update sees a
 * fresh seq with a possibly-stale ns and just waits one more tick.  Inlined
 * and gated: when the watchdog is off this is a single predictable branch. */
PYGO_INLINE void pygo_hub_resume_begin(pygo_hub_t *h, pygo_g_t *g)
{
    if (__builtin_expect(pygo_sysmon_enabled, 0)) {
        h->resume_g = g;
        __atomic_add_fetch(&h->resume_seq, 1, __ATOMIC_RELAXED);
        __atomic_store_n(&h->resume_start_ns, pygo_monotonic_ns(),
                         __ATOMIC_RELAXED);
    }
}

/* Mark the end of a coro resume: clear resume_start_ns so the watchdog
 * stops counting this hub as "in a resume". */
PYGO_INLINE void pygo_hub_resume_end(pygo_hub_t *h)
{
    if (__builtin_expect(pygo_sysmon_enabled, 0)) {
        __atomic_store_n(&h->resume_start_ns, 0, __ATOMIC_RELAXED);
    }
}

/* Interpreter for per-g PyThreadState_New, captured at mn_init. */
static PyInterpreterState *pygo_mn_interp = NULL;

/* TLS pointers set at hub_main entry.  pygo_mn_yield_current() and
 * pygo_mn_current_hub() read these to route per-g operations to the
 * right hub without each call site needing to look it up. */
static PYGO_TLS pygo_hub_t *pygo_tls_hub = NULL;
static PYGO_TLS pygo_g_t   *pygo_tls_current_g = NULL;
/* Set by pygo_mn_yield_current() before pygo_coro_yield(); read by
 * hub_main after pygo_coro_resume returns.  Tells hub_main "the g
 * has already put itself on a queue, you don't need to requeue".
 * Distinguishes scheduler-aware yield (sched_yield) from raw yield
 * (pygo_core.yield_() -> pygo_coro_yield directly). */
static PYGO_TLS int pygo_tls_self_queued = 0;
/* Set ONLY by pygo_mn_tls_mark_parked (the off-queue park path: netpoll /
 * chan / sleep all route through it).  Distinguishes a g that parked off-queue
 * -- so a wake_g will route it back, and it must NOT be re-pushed -- from a
 * cooperative yield_current (self_queued too, but it already pushed itself to
 * the local FIFO).  Read by hub_main's per-g-tstate release to decide PARKED
 * (wakeable) vs RUNNING (hub-pinned) for the wake state machine.  Unused by the
 * default scheduler, which keys re-push purely on self_queued. */
static PYGO_TLS int pygo_tls_parked_offqueue = 0;

/* Hub thread main loop.  Phase C v2: runs the same snap/load dance
 * as pygo_sched_drain.  Each iteration:
 *   1. Pop a g (local FIFO of yielded gs first, then own deque of
 *      fresh gs, then steal from a neighbour's deque).
 *   2. Save hub's tstate into hub_snap (local var on hub_main's stack).
 *   3. If g has a saved snap, load it; else NULL datastack so g's
 *      first run gets its own root chunk (Phase B initial-run dance).
 *   4. Resume g.  g either runs to completion or yields.
 *   5. Restore hub_snap.  If g is still alive AND did not self-queue
 *      (raw pygo_coro_yield, no scheduler call), push it back to the
 *      local FIFO so it keeps making progress.
 *
 * Idle policy: when all hubs report pending=0 we still poll (the
 * caller may pygo_mn_go more work at any time).  Stop signal comes
 * from pygo_mn_fini setting h->stopping. */
static PYGO_THREAD_RET pygo_hub_main(void *arg)
{
    pygo_hub_t *h = (pygo_hub_t *)arg;
    pygo_pystate_snap_t hub_snap;

    PyEval_RestoreThread(h->tstate);
    /* Per-OS-thread coro-backend setup.  On Windows Fibers this calls
     * ConvertThreadToFiber so SwitchToFiber works on this thread; on
     * POSIX it's a no-op.  Must run BEFORE the first pygo_coro_resume
     * (otherwise SwitchToFiber faults with "not a fiber"). */
    pygo_coro_thread_init();
    pygo_tls_hub = h;

    /* Create the hub's per-thread io_uring ring.  Try
     * SINGLE_ISSUER (Linux 5.18+) + DEFER_TASKRUN (6.1+); the create
     * call downgrades gracefully on older kernels.  With
     * DEFER_TASKRUN the kernel only posts CQEs + fires the eventfd
     * when this thread next calls io_uring_enter(GETEVENTS); the
     * idle path below issues that call before pumping so the
     * deferred work flushes in time. */
    h->iouring_ring = pygo_iouring_ring_create(1 /*defer_taskrun*/);
    if (h->iouring_ring != NULL) {
        h->iouring_eventfd = pygo_iouring_ring_eventfd(h->iouring_ring);
        if (pygo_netpoll_add_iouring_ring(h->iouring_eventfd,
                                          h->iouring_ring) != 0) {
            /* Registration failed (most likely no epoll backend, but
             * this build path is Linux-only so it'd be unusual).
             * Discard the ring; hub-bound iouring calls fall through. */
            pygo_iouring_ring_destroy(h->iouring_ring);
            h->iouring_ring     = NULL;
            h->iouring_eventfd  = -1;
        }
    } else {
        h->iouring_eventfd = -1;
    }

    /* hub_snap is loop-invariant for the same reason sched_snap is in
     * pygo_sched_drain: hub_main runs no Python work between
     * iterations except via pygo_g_decref's tp_dealloc, where we
     * explicitly restore + re-snap.  Hoisting the per-iter snap+load
     * out of the loop is ~10 ns/yield on the M:N hot path. */
    pygo_pystate_snap(&hub_snap);

    while (!__atomic_load_n(&h->stopping, __ATOMIC_ACQUIRE)) {
        pygo_g_t *g;
        /* Set when g came from the global run-queue: it then carries a queue
         * ref this iteration must release, and its resume must claim it with the
         * QUEUED->RUNNING CAS.  Gs from the local FIFO / own deque / a neighbour
         * steal are single-owner by construction (already RUNNING) and skip
         * both. */
        int from_runq = 0;
        /* Drain the submission list into the deque first.  Pushing to
         * the deque is owner-only, so we (the hub) move fresh gs from
         * external producers onto our deque before anyone else looks. */
        pygo_mutex_lock(&h->sub_lock);
        {
            pygo_g_t *sub = h->sub_head;
            h->sub_head = h->sub_tail = NULL;
            pygo_mutex_unlock(&h->sub_lock);
            while (sub != NULL) {
                pygo_g_t *next = sub->next;
                sub->next = NULL;
                /* Route by state: fresh (no snap) -> Chase-Lev deque
                 * (stealable by other hubs); woken (snap.valid) ->
                 * local FIFO (hub-pinned, the netpoll-wake path).
                 *
                 * Note: in_sub_queue stays 1 throughout drain ->
                 * ready/deque -> pop -> resume.  hub_main clears it
                 * just before the actual coro resume, gating duplicate
                 * wake_gs against ANY queued state, not just the
                 * sub list. */
                if (sub->snap.valid) {
                    pygo_sched_ready_push(&h->sched, sub);
                } else if (pygo_cldeque_push(&h->deque, sub) != 0) {
                    /* Deque full (>PYGO_CLDEQUE_CAP=4096 fresh gs on this
                     * hub -- e.g. a big spawn burst at low hub count, the
                     * H=2/N>=8K case).  Fall back to the local ready FIFO,
                     * which grows on demand.  These gs run hub-pinned
                     * rather than stealable, but are NEVER dropped: the
                     * previous code ignored the push return and silently
                     * dropped the g, orphaning one whose pending-inc was
                     * already counted -> mn_run hung forever.  The resume
                     * prologue handles a snap-invalid g from either queue
                     * via the first-run install path. */
                    pygo_sched_ready_push(&h->sched, sub);
                }
                sub = next;
            }
        }

        /* Wake any sleepers whose timers have expired and move them
         * onto the local FIFO so the pop below picks them up. */
        if (h->sched.sleep_size > 0) {
            double now = pygo_sched_monotonic_seconds();
            while (h->sched.sleep_size > 0 &&
                   pygo_sched_sleep_peek(&h->sched)->wake_at <= now) {
                pygo_g_t *woke = pygo_sched_sleep_pop(&h->sched);
                if (pygo_use_global_runq()) {
                    /* The timer is a wake, and under a global-runq mode the
                     * sleep release left this g in wake_state PARKED.  Claim it
                     * back to RUNNING through the state machine (it has no other
                     * waker, so the CAS normally wins) and resume it hub-pinned
                     * on the local FIFO.  If the CAS loses, a concurrent waker
                     * (a g sleeping AND wakeable, e.g. a select-with-timeout)
                     * already drove PARKED->QUEUED and enqueued it on the global
                     * runq -- do NOT also push it here, or it would be scheduled
                     * twice. */
                    int pexp = PYGO_WS_PARKED;
                    if (__atomic_compare_exchange_n(&woke->wake_state, &pexp,
                                                    PYGO_WS_RUNNING, 0,
                                                    __ATOMIC_ACQ_REL,
                                                    __ATOMIC_ACQUIRE)) {
                        pygo_sched_ready_push(&h->sched, woke);
                    }
                } else {
                    pygo_sched_ready_push(&h->sched, woke);
                }
            }
        }

        g = pygo_sched_ready_pop(&h->sched);     /* local yielded */
        if (g == NULL) {
            g = (pygo_g_t *)pygo_cldeque_pop(&h->deque);  /* own fresh */
        }
        if (g == NULL) {
            /* Global run-queue: woken migratable gs (per-g-tstate) that any
             * idle hub may run, so a hub stuck in a blocking C call can't
             * strand them.  Checked before neighbour-steal below so a
             * stalled hub's woken work is recovered promptly.  from_runq tells
             * the resume block to claim the g (QUEUED->RUNNING) and to drop the
             * entry's queue ref when the resume ends. */
            g = pygo_mn_global_runq_pull();
            if (g != NULL) from_runq = 1;
        }
        if (g == NULL) {
            int i;
            for (i = 0; i < pygo_hub_count; i++) {
                if (i == h->id) continue;
                g = (pygo_g_t *)pygo_cldeque_steal(&pygo_hubs[i].deque);
                if (g != NULL) {
                    /* ACQ_REL pair: mn_run's polling loop reads
                     * hubs[*].pending with ACQUIRE; under
                     * free-threaded Python the RELAXED pair we used
                     * to have let a transient "victim already
                     * decremented, stealer not yet incremented"
                     * state become observable -- total could appear
                     * 0 momentarily even with work in flight, which
                     * is harmless on its own but ALSO meant the
                     * stealer's later `h->pending` increment lacked
                     * a release pairing with reads in other hubs
                     * that might inspect this hub's queue. */
                    pygo_mn_pending_steal(&pygo_hubs[i], h);
                    break;
                }
            }
            if (g == NULL) {
                long total = 0;
                int j;
                PyThreadState *saved;
                int parked;
                long long idle_ns;
                for (j = 0; j < pygo_hub_count; j++) {
                    total += __atomic_load_n(&pygo_hubs[j].pending,
                                             __ATOMIC_RELAXED);
                }
                /* Default idle wait (no work anywhere -> longer; some
                 * work elsewhere -> shorter so we re-poll for steal). */
                idle_ns = (total == 0) ? 500000LL : 100000LL;
                /* Cap by the next-due sleeper on THIS hub so we don't
                 * oversleep past a local timer.  (Other hubs' timers
                 * are handled by those hubs.) */
                if (h->sched.sleep_size > 0) {
                    double now = pygo_sched_monotonic_seconds();
                    double gap = pygo_sched_sleep_peek(&h->sched)->wake_at - now;
                    long long gap_ns = (long long)(gap * 1e9);
                    if (gap_ns < 0) gap_ns = 0;
                    if (gap_ns < idle_ns) idle_ns = gap_ns;
                }
                parked = pygo_netpoll_parked_count();
                /* Hub-idle dwell-based stack reclaim (opt-in,
                 * PYGO_STACK_PARK_SWEEP=1; threshold ms via
                 * PYGO_STACK_PARK_SWEEP_MS, default 5).  This hub is idle
                 * and is the sole resumer of its own parkers, so madvising
                 * their long-idle stacks here is race-free (see
                 * pygo_netpoll_sweep_idle).  Rate-limited to ~half the
                 * threshold so the O(parked) walk stays cheap. */
                {
                    static int sweep_on = -1;
                    /* 100 ms default: high enough that active round-trip
                     * parks (ms-scale, even with some queuing) are left
                     * alone, low enough to reclaim genuinely idle
                     * keepalive parks (seconds).  A saturated all-active
                     * bench inflates round-trip dwell, so it still pays
                     * some cost there; the N=1M target (5% active) does
                     * not.  Tunable via PYGO_STACK_PARK_SWEEP_MS. */
                    static long long sweep_thresh_ns = 100000000LL;
                    int on = __atomic_load_n(&sweep_on, __ATOMIC_RELAXED);
                    if (on < 0) {
                        const char *e = getenv("PYGO_STACK_PARK_SWEEP");
                        const char *ms = getenv("PYGO_STACK_PARK_SWEEP_MS");
                        if (ms != NULL) {
                            long long v = atoll(ms);
                            if (v > 0) sweep_thresh_ns = v * 1000000LL;
                        }
                        /* Default ON (2026-05-29): the dwell sweep delivers
                         * -32% idle RSS at no robustly-measurable p99 cost
                         * (the churn cost proved within run-to-run noise),
                         * and the churn throttle (PYGO_SWEEP_MAX_CHURN, see
                         * netpoll.c) degrades it to a no-op on active-churn
                         * workloads as insurance.  PYGO_STACK_PARK_SWEEP=0
                         * disables; =1 (or any non-"0") forces on. */
                        on = (e != NULL && *e == '0') ? 0 : 1;
                        __atomic_store_n(&sweep_on, on, __ATOMIC_RELAXED);
                    }
                    /* Runs in BOTH modes now: under per-g-tstate the sweep's
                     * sole-resumer safety is supplied by the per-g claim
                     * handshake (PARKED->SWEEPING) inside pygo_netpoll_sweep_idle,
                     * so a stolen wake can't resume a g into a stack mid-madvise. */
                    if (on && parked > 0) {
                        double now_s = pygo_sched_monotonic_seconds();
                        double interval_s = (double)sweep_thresh_ns / 2e9;
                        if (now_s - h->last_sweep_s >= interval_s) {
                            pygo_netpoll_sweep_idle(
                                pygo_mn_current_hub_opaque(), sweep_thresh_ns);
                            h->last_sweep_s = now_s;
                        }
                    }
                }
                {
                    /* Also drive the pump when ANY iouring ring (this
                     * hub's, the global, or another hub's) has inflight
                     * ops: the shared netpoll's epoll set holds every
                     * registered iouring eventfd, so any hub's pump call
                     * drains everyone's CQEs.  Without this, a hub that
                     * parks all its gs on iouring would idle-sleep
                     * while completions sit unread.
                     *
                     * GIL handling: pump internally does
                     * Py_BEGIN_ALLOW_THREADS for the epoll_wait syscall,
                     * so it must be called with the GIL HELD (matching
                     * how single-thread pygo_sched_drain calls it).
                     * Wrapping pump in our own PyEval_SaveThread would
                     * drop the GIL twice and crash with
                     * "must be called with GIL held".  pygo_sleep_ns is
                     * a plain nanosleep that doesn't need the GIL, so
                     * release the GIL around THAT alone. */
                    int iouring_total = pygo_iouring_inflight();
                    if (parked > 0 || iouring_total > 0) {
                        long long pump_ns = 1000000LL;
                        if (h->sched.sleep_size > 0 && idle_ns < pump_ns) {
                            pump_ns = idle_ns;
                        }
                        /* DEFER_TASKRUN heartbeat: if this hub's ring
                         * was created with that flag, completions on
                         * its own SQEs are queued in the kernel but
                         * NOT posted to the CQ (or the eventfd) until
                         * THIS thread next calls
                         * io_uring_enter(GETEVENTS).  Trigger that
                         * flush now so the upcoming epoll_wait sees a
                         * fresh eventfd hit if there's work pending.
                         * No-op for non-DEFER rings.  We only do this
                         * on OUR own ring -- under SINGLE_ISSUER each
                         * ring's GETEVENTS must come from its owner. */
                        pygo_iouring_ring_get_events(h->iouring_ring);
                        pygo_netpoll_pump(pump_ns);
                    } else {
                        if (idle_ns <= 0) idle_ns = 1;
                        saved = PyEval_SaveThread();
                        pygo_sleep_ns(idle_ns);
                        PyEval_RestoreThread(saved);
                    }
                }
                (void)saved;
                continue;
            }
        }

        /* Phase B snap dance: load g's tstate slice; resume; if g
         * completed, restore hub's tstate before any Python that
         * might allocate frames.  hub_snap is hoisted out of the loop
         * (see entry comment). */
        {
            int self_queued;

            if (pygo_get_per_g_tstate_mode()) {
                /* ---- per-g-tstate path (PYGO_PER_G_TSTATE) ----
                 * Swap the hub's tstate out, the g's own tstate in, resume,
                 * swap back.  The g's suspended eval frames reference
                 * g->tstate (not this hub's), so resuming on ANY hub is
                 * consistent -- which is exactly what makes a stolen woken g
                 * safe here.  No snap dance (snap no-ops under this mode). */
                PyThreadState *hub_ts;
                int parked_offqueue;
                /* CLAIM the g for this hub.  A from_runq g arrived via an entry
                 * that holds a queue ref and put it in wake_state QUEUED; CAS
                 * QUEUED->RUNNING to take ownership.  This CAS CANNOT lose: the
                 * entry we pulled is the queue's sole reference to this g
                 * (at-most-one-entry) and only its holder runs this CAS -- so
                 * there is no contention and no re-push (the old design's
                 * livelock).  Gs from the deque / local FIFO / a neighbour steal
                 * are already RUNNING by the state-machine invariant (fresh gs
                 * spawn RUNNING; a yield leaves the g RUNNING), so they need no
                 * claim. */
                if (from_runq) {
                    int qexp = PYGO_WS_QUEUED;
                    if (!__atomic_compare_exchange_n(&g->wake_state, &qexp,
                                                     PYGO_WS_RUNNING, 0,
                                                     __ATOMIC_ACQ_REL,
                                                     __ATOMIC_ACQUIRE)) {
                        /* Not QUEUED: a queued g is parked-waiting and never
                         * done, so this should not happen.  Stay leak-safe by
                         * dropping the queue ref and skipping. */
                        pygo_g_decref(g);   /* queue ref */
                        continue;
                    }
                }
                __atomic_store_n(&g->in_sub_queue, 0, __ATOMIC_RELEASE);
                if (g->coro == NULL || g->tstate == NULL ||
                    __atomic_load_n(&g->done, __ATOMIC_ACQUIRE)) {
                    /* Dead under our claim (shouldn't happen: a QUEUED g is
                     * parked, not done; a from_runq=0 g is freshly owned).  Stay
                     * leak-safe: a claimed runq g goes back to PARKED and drops
                     * its queue ref. */
                    if (from_runq) {
                        __atomic_store_n(&g->wake_state, PYGO_WS_PARKED,
                                         __ATOMIC_RELEASE);
                        pygo_g_decref(g);   /* queue ref */
                    }
                    continue;
                }
                hub_ts = PyEval_SaveThread();        /* detach hub tstate */
                PyEval_RestoreThread(g->tstate);      /* attach g's own tstate */
                h->sched.current = g;
                pygo_tls_current_g = g;
                pygo_tls_self_queued = 0;
                pygo_tls_parked_offqueue = 0;
                pygo_hub_resume_begin(h, g);
                pygo_coro_resume(g->coro);
                pygo_hub_resume_end(h);
                self_queued = pygo_tls_self_queued;
                parked_offqueue = pygo_tls_parked_offqueue;
                pygo_tls_self_queued = 0;
                pygo_tls_parked_offqueue = 0;
                pygo_tls_current_g = NULL;
                h->sched.current = NULL;
                PyEval_SaveThread();                  /* detach g's tstate */
                PyEval_RestoreThread(hub_ts);          /* reattach hub tstate */
                if (pygo_coro_done(g->coro)) {
                    /* Done: the g did not park this resume (a resume ends in a
                     * park OR completion, never both), so no parker is live and
                     * wake_state is still RUNNING with no other entry.  Force-
                     * unlink any stale parker, then drop both refs -- the slab
                     * re-zeroes wake_state when it reallocs the g. */
                    pygo_netpoll_force_unlink_g_parker(g);
                    pygo_mn_pending_complete(h);
                    pygo_g_decref(g);                  /* scheduler ref */
                    if (from_runq) pygo_g_decref(g);   /* queue ref */
                } else if (parked_offqueue) {
                    /* Parked off-queue (netpoll/chan/sleep): a wake_g will route
                     * the g back here.  End ownership now -- AFTER the tstate
                     * detach above, which is what closes the commit->detach
                     * window: while RUNNING, a wake only set RUNNING_WOKEN and
                     * never enqueued, so no other hub could attach this live
                     * tstate.  CAS RUNNING->PARKED so a later wake enqueues it.
                     * If a wake landed during the window (RUNNING_WOKEN), enqueue
                     * exactly once now instead of parking -- the wake is not
                     * lost and stays a single entry. */
                    int rexp = PYGO_WS_RUNNING;
                    if (__atomic_compare_exchange_n(&g->wake_state, &rexp,
                                                    PYGO_WS_PARKED, 0,
                                                    __ATOMIC_ACQ_REL,
                                                    __ATOMIC_ACQUIRE)) {
                        if (from_runq) pygo_g_decref(g);   /* old queue ref */
                    } else {
                        /* rexp == RUNNING_WOKEN.  Re-arm a single entry: incref
                         * the new one, drop the old (net zero for from_runq;
                         * first entry for from_runq=0). */
                        __atomic_store_n(&g->wake_state, PYGO_WS_QUEUED,
                                         __ATOMIC_RELEASE);
                        pygo_g_incref(g);                  /* new queue ref */
                        pygo_mn_global_runq_push(g);
                        if (from_runq) pygo_g_decref(g);   /* old queue ref */
                    }
                } else {
                    /* Cooperative yield_current (already on the local FIFO) or a
                     * raw pygo_coro_yield: the g stays runnable and hub-pinned,
                     * still owned by this hub, so it ends at RUNNING.  A store is
                     * safe -- a yielding g holds no live parker, so no wake_g can
                     * fire to race it.  yield_current already pushed itself to
                     * the FIFO; a raw yield (self_queued==0) did not, so push it
                     * here. */
                    __atomic_store_n(&g->wake_state, PYGO_WS_RUNNING,
                                     __ATOMIC_RELEASE);
                    if (from_runq) pygo_g_decref(g);       /* old queue ref */
                    if (!self_queued) pygo_sched_ready_push(&h->sched, g);
                }
                continue;
            }

            /* Steal-woken (or any non-per-g global-runq mode): a from_runq g
             * arrived via a queue entry holding a queue ref, in wake_state
             * QUEUED.  Claim it QUEUED->RUNNING (sole entry holder -> cannot
             * lose).  Fresh / deque / local-FIFO / stolen gs are already
             * RUNNING.  from_runq is only ever set under pygo_use_global_runq(),
             * so this block is inert in the byte-unchanged default. */
            int parked_offqueue = 0;
            if (from_runq) {
                int qexp = PYGO_WS_QUEUED;
                if (!__atomic_compare_exchange_n(&g->wake_state, &qexp,
                                                 PYGO_WS_RUNNING, 0,
                                                 __ATOMIC_ACQ_REL,
                                                 __ATOMIC_ACQUIRE)) {
                    /* Not QUEUED: a queued g is parked-waiting, never done, so
                     * this shouldn't happen.  Stay leak-safe: drop the queue ref
                     * and skip. */
                    pygo_g_decref(g);   /* queue ref */
                    continue;
                }
            }

            if (g->snap.valid) {
                pygo_pystate_load(&g->snap);
            } else {
                pygo_first_run_install_datastack();
#if PY_VERSION_HEX >= 0x030D0000
                {
                    PyThreadState *ts = PyThreadState_GET();
                    ts->current_frame = NULL;
                }
#endif
            }

            h->sched.current = g;
            pygo_tls_current_g = g;
            pygo_tls_self_queued = 0;
            /* Clear queued flag right before resume so a wake_g that
             * fires during this g's execution (e.g., a netpoll pump
             * processing an event for the parker g is about to link
             * inside wait_fd) can enqueue g via submit. */
            __atomic_store_n(&g->in_sub_queue, 0, __ATOMIC_RELEASE);
            /* Defensive: pop a stale queue entry?  Two failure modes
             * for the queue under M:N + free-threaded:
             *
             *   coro == NULL: g was already decref'd to 0; pygo_g_decref
             *      already ran the pending_global decrement; just skip.
             *
             *   done == 1, coro != NULL: g_entry set done before the
             *      asm trampoline returned to its original caller hub,
             *      but THIS hub popped a duplicate queue entry for
             *      the same g (stale wake_g raced with completion).
             *      The original hub still owes the decrement -- it
             *      will run completion when its coro_resume returns.
             *      We must NOT decrement here (would double-count) and
             *      must NOT re-resume (would re-run the asm trampoline
             *      against a coro whose user frames already unwound).
             *      Skip without decrementing.
             *
             * In both cases skipping does not lose work: the original
             * processing path owns the decrement. */
            PYGO_G_ASSERT_NOT(g, PYGO_GST_BIT(PYGO_GST_FREED));
            PYGO_EVT(PYGO_EVT_G_POP, g, h, 0);
            if (g->coro == NULL ||
                __atomic_load_n(&g->done, __ATOMIC_ACQUIRE)) {
                h->sched.current = NULL;
                pygo_tls_current_g = NULL;
                if (from_runq) pygo_g_decref(g);   /* queue ref */
                continue;
            }
            pygo_hub_resume_begin(h, g);
            pygo_coro_resume(g->coro);
            pygo_hub_resume_end(h);
            self_queued = pygo_tls_self_queued;
            parked_offqueue = pygo_tls_parked_offqueue;
            pygo_tls_self_queued = 0;
            pygo_tls_parked_offqueue = 0;
            pygo_tls_current_g = NULL;
            h->sched.current = NULL;

            if (pygo_coro_done(g->coro)) {
                /* Drain g's chunks, restore hub's tstate so the decref
                 * (potentially calling Python tp_dealloc) allocates on
                 * the hub's chunk -- not on a NULL datastack that
                 * would otherwise leak when the next iter overwrites.
                 * Re-snap so the next completion path / hub exit can
                 * load again. */
                pygo_drain_g_datastack();
                pygo_pystate_load(&hub_snap);
                /* Force-unlink any netpoll parker still referencing g.
                 * If wait_fd's safety unlink missed (because a wake
                 * path bypassed the netpoll dispatcher AND the safety
                 * check's structural test couldn't see the leak), the
                 * parker would survive into stack-pool reuse and
                 * resurrect this just-freed g via a future pump
                 * dispatch.  This call covers that gap. */
                pygo_netpoll_force_unlink_g_parker(g);
                pygo_mn_pending_complete(h);
                pygo_g_decref(g);                  /* scheduler ref */
                if (from_runq) pygo_g_decref(g);   /* queue ref */
                pygo_pystate_snap(&hub_snap);
            } else if (pygo_use_global_runq()) {
                /* Steal-woken release via the wake-state machine -- a near-exact
                 * mirror of the per-g-tstate release above; the difference is
                 * only that the migrated state rides in g->snap (saved by the
                 * parker BEFORE it yielded) instead of a per-g tstate.
                 *
                 * parked_offqueue => the g parked off-queue (netpoll/chan/sleep)
                 * and a wake_g will route it back through the global runq.  End
                 * ownership now: this happens AFTER the snap was saved, and while
                 * RUNNING a wake only set RUNNING_WOKEN (never enqueued), so no
                 * other hub could pull the g and load a half-saved snap.  CAS
                 * RUNNING->PARKED so a later wake enqueues it; if a wake already
                 * landed (RUNNING_WOKEN), enqueue exactly once now. */
                if (parked_offqueue) {
                    int rexp = PYGO_WS_RUNNING;
                    /* Detach the g's ref-holding interpreter state from THIS
                     * hub's tstate now -- still RUNNING (sole owner), BEFORE the
                     * g becomes migratable.  The parker already saved g->snap,
                     * which owns its OWN refs to context / current_exception /
                     * exc_value; loading the hub's clean base here drops this
                     * hub's duplicate refs (rebalancing them).  Without it the
                     * origin hub and the stealing hub (which loads the snap) both
                     * own the g's exception and race its refcount -> the freed
                     * object surfaces as "error return without exception set" /
                     * SEGV deep in CPython's exception machinery.  This is the
                     * snap-mode analogue of per-g-tstate's PyEval_SaveThread
                     * (detach) before the RUNNING->PARKED transition.  Re-snap to
                     * keep hub_snap valid for the next g. */
                    pygo_pystate_load(&hub_snap);
                    pygo_pystate_snap(&hub_snap);
                    if (pygo_g_state_in(g, PYGO_GST_MASK_PARKED)) {
                        /* madvise idle stack below sp while still RUNNING (sole
                         * owner -- a wake only flips RUNNING->RUNNING_WOKEN), so
                         * no hub can resume into pages mid-drop.  Cross-hub-safe
                         * for the same reason the sweep handshake is.  No-op
                         * unless PYGO_STACK_PARK_DONTNEED=1. */
                        pygo_coro_park(g->coro);
                    }
                    if (__atomic_compare_exchange_n(&g->wake_state, &rexp,
                                                    PYGO_WS_PARKED, 0,
                                                    __ATOMIC_ACQ_REL,
                                                    __ATOMIC_ACQUIRE)) {
                        if (from_runq) pygo_g_decref(g);   /* old queue ref */
                    } else {
                        /* rexp == RUNNING_WOKEN: a wake landed during the resume.
                         * Re-arm a single entry and enqueue now (snap is saved),
                         * exactly once -- no lost wake, no duplicate. */
                        __atomic_store_n(&g->wake_state, PYGO_WS_QUEUED,
                                         __ATOMIC_RELEASE);
                        pygo_g_incref(g);                  /* new queue ref */
                        pygo_mn_global_runq_push(g);
                        if (from_runq) pygo_g_decref(g);   /* old queue ref */
                    }
                } else {
                    /* Cooperative yield (sched_yield: already on local FIFO, or
                     * raw pygo_coro_yield: push it here).  Holds no live parker,
                     * so no wake_g can race -- stays RUNNING, hub-pinned. */
                    __atomic_store_n(&g->wake_state, PYGO_WS_RUNNING,
                                     __ATOMIC_RELEASE);
                    if (from_runq) pygo_g_decref(g);       /* old queue ref */
                    if (!self_queued) pygo_sched_ready_push(&h->sched, g);
                }
            } else if (!self_queued) {
                /* Raw pygo_coro_yield() -- g didn't go through
                 * sched_yield to push itself.  Keep it alive on the
                 * local FIFO.  tstate still has g's state from after
                 * resume; that's fine -- next iter's g_next->snap
                 * load overwrites. */
                pygo_sched_ready_push(&h->sched, g);
            } else if (pygo_g_state_in(g, PYGO_GST_MASK_PARKED)) {
                /* g parked on a waiter (netpoll/chan/sleep/park_safe)
                 * and won't run again until woken: drop its now-idle
                 * stack pages.  No-op unless PYGO_STACK_PARK_DONTNEED=1.
                 * Gated on the PARKED states so a cooperative sched_yield
                 * (also self_queued, but re-queued + resuming shortly)
                 * doesn't pay an immediate re-fault.
                 *
                 * Safe under M:N: a woken g routes back to THIS (owning)
                 * hub's non-stealable local FIFO, so this hub is the sole
                 * resumer -- the madvise happens-before the next resume on
                 * one thread.  Default-OFF is a throughput choice, not a
                 * safety one (see pygo_coro_park doc in coro.h). */
                pygo_coro_park(g->coro);
            }
            /* remaining sched_yield path: g pushed itself, tstate has
             * g's state, no work needed here. */
        }
    }
    /* Restore hub's tstate before the thread exits. */
    pygo_pystate_load(&hub_snap);
    pygo_tls_hub = NULL;
    /* Tear down the hub's iouring ring.  Unregister from netpoll FIRST
     * so the pump can't dispatch a CQE to a freed ring; THEN destroy.
     * Hubs only exit when the scheduler is shutting down + all gs have
     * completed, so inflight==0 here is the expected case; if a ring
     * still had inflight ops we'd leak the underlying CQEs (kernel
     * frees them on close). */
    if (h->iouring_ring != NULL) {
        pygo_netpoll_remove_iouring_ring(h->iouring_eventfd);
        pygo_iouring_ring_destroy(h->iouring_ring);
        h->iouring_ring    = NULL;
        h->iouring_eventfd = -1;
    }
    /* Reverse pygo_coro_thread_init for clean exit on Windows
     * (ConvertFiberToThread); no-op elsewhere. */
    pygo_coro_thread_fini();
    PyEval_SaveThread();
    PYGO_THREAD_RETURN(NULL);
}

int pygo_mn_hub_count(void)
{
    return pygo_hub_count;
}

void *pygo_mn_current_hub_opaque(void)
{
    return (void *)pygo_tls_hub;
}

int pygo_mn_hub_id_of(void *hub_opaque)
{
    if (hub_opaque == NULL) return -1;
    return ((pygo_hub_t *)hub_opaque)->id;
}

pygo_g_t *pygo_mn_tls_current_g(void)
{
    return pygo_tls_current_g;
}

void pygo_mn_tls_mark_parked(void)
{
    pygo_tls_self_queued = 1;
    pygo_tls_parked_offqueue = 1;
}

pygo_sched_t *pygo_mn_current_sched(void)
{
    pygo_hub_t *h = pygo_tls_hub;
    return h ? &h->sched : NULL;
}

struct pygo_iouring_ring *pygo_mn_current_iouring_ring(void)
{
    pygo_hub_t *h = pygo_tls_hub;
    return h ? h->iouring_ring : NULL;
}

/* Push g onto a hub's submission list.  Called by netpoll pump to
 * route an I/O-woken g back to whichever hub it was running on.
 * (Also used internally by mn_go.)  Hub_main drains submissions every
 * iteration and routes each entry to either the deque (if g is fresh)
 * or the local FIFO (if g has saved state -- the netpoll-wake case).
 *
 * Idempotency: a CAS on g->in_sub_queue makes duplicate submissions
 * no-ops.  Under M:N + free-threaded 3.13t a parker can legitimately
 * be wake_g'd more than once (e.g., wake_g from a netpoll pump that
 * unlinked it normally, followed by a stale wake from the safety
 * unlink at the next wait_fd's yield-return).  Without the CAS the
 * sub queue ends up with g twice, hub_main pops it twice, and the
 * second resume hits a g whose coro was already destroyed by the
 * decref that ran after the first run-to-completion -- segfault in
 * pygo_asm_swap on *(NULL coro). */
static void pygo_mn_hub_submit(pygo_hub_t *h, pygo_g_t *g)
{
    int expected = 0;
    /* Defence-in-depth: a submitted g must not already be DONE/FREED.
     * If this fires, the in_sub_queue CAS below would still no-op but
     * the diag ring + abort give a precise place to look. */
    PYGO_G_ASSERT_NOT(g, PYGO_GST_MASK_DEAD);
    if (!__atomic_compare_exchange_n(&g->in_sub_queue, &expected, 1,
                                     0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        /* Already queued; no need to enqueue again.  The pending resume
         * will pick up whatever state the g has when hub_main gets to it. */
        return;
    }
    pygo_mutex_lock(&h->sub_lock);
    g->next = NULL;
    if (h->sub_tail != NULL) {
        h->sub_tail->next = g;
    } else {
        h->sub_head = g;
    }
    h->sub_tail = g;
    pygo_mutex_unlock(&h->sub_lock);
    PYGO_EVT(PYGO_EVT_G_SUBMIT, g, h, 0);
    pygo_g_state_set(g, PYGO_GST_SUBMITTED);
}

void pygo_mn_wake_g(void *hub_opaque, pygo_g_t *g)
{
    if (hub_opaque == NULL) {
        /* g belongs to the single-thread global scheduler (or netpoll
         * was used outside any hub context). */
        pygo_sched_ready_push(pygo_sched_get(), g);
        return;
    }
    if (pygo_use_global_runq()) {
        /* per-g-tstate OR steal-woken: g is migratable, so route it to the
         * global run-queue any idle hub can drain instead of the origin hub's
         * owner-drained sub list -- recovers it even if the origin hub is wedged
         * in a blocking C call.  Drive the per-g wake state machine (see
         * pygo_sched.h); it is tstate-agnostic -- the snap (steal-woken) or the
         * per-g tstate (per-g-tstate) carries the migrated state:
         *
         *   PARKED  -> QUEUED          : we won the wake; enqueue exactly once
         *                                (+ a queue ref so the entry keeps g
         *                                alive until a hub consumes it).
         *   RUNNING -> RUNNING_WOKEN    : g is owned by a hub mid-resume; just
         *                                remember the wake -- the owner enqueues
         *                                it at release.  Enqueuing now would let
         *                                a second hub attach g's live tstate
         *                                during the owner's commit->detach gap.
         *   QUEUED / RUNNING_WOKEN      : a wake is already pending -> drop, so a
         *                                g is never enqueued twice (the old
         *                                two-flag design's livelock).
         *
         * CAS-on-failure reloads `st`, so the loop only spins while it keeps
         * losing the PARKED->QUEUED or RUNNING->RUNNING_WOKEN race (a couple of
         * instructions); every other state returns immediately. */
        int st = __atomic_load_n(&g->wake_state, __ATOMIC_ACQUIRE);
        (void)hub_opaque;   /* origin no longer needed: no per-hub rebalance */
        PYGO_G_ASSERT_NOT(g, PYGO_GST_MASK_DEAD);
        for (;;) {
            if (st == PYGO_WS_PARKED) {
                if (__atomic_compare_exchange_n(&g->wake_state, &st,
                                                PYGO_WS_QUEUED, 0,
                                                __ATOMIC_ACQ_REL,
                                                __ATOMIC_ACQUIRE)) {
                    pygo_g_incref(g);   /* queue ref, dropped when consumed */
                    pygo_mn_global_runq_push(g);
                    return;
                }
                /* lost: st reloaded to the current state; re-evaluate */
            } else if (st == PYGO_WS_RUNNING) {
                if (__atomic_compare_exchange_n(&g->wake_state, &st,
                                                PYGO_WS_RUNNING_WOKEN, 0,
                                                __ATOMIC_ACQ_REL,
                                                __ATOMIC_ACQUIRE)) {
                    return;   /* owner will enqueue at release */
                }
            } else if (st == PYGO_WS_SWEEPING) {
                if (__atomic_compare_exchange_n(&g->wake_state, &st,
                                                PYGO_WS_SWEEPING_WOKEN, 0,
                                                __ATOMIC_ACQ_REL,
                                                __ATOMIC_ACQUIRE)) {
                    return;   /* sweeper will enqueue at release */
                }
            } else {
                return;   /* QUEUED / RUNNING_WOKEN / SWEEPING_WOKEN: pending */
            }
        }
    }
    pygo_mn_hub_submit((pygo_hub_t *)hub_opaque, g);
}

/* Idle-stack-sweep handshake (PYGO_PER_G_TSTATE).  An idle hub about to
 * MADV_DONTNEED a long-parked g's below-SP stack pages must hold the g
 * un-resumable for the madvise's duration, or another hub could pull a wake and
 * resume the g into pages the kernel is concurrently zeroing.  try_claim CASes
 * PARKED -> SWEEPING (the same exclusivity QUEUED->RUNNING gives a resumer); it
 * loses to any non-PARKED state (woken/owned) and the sweeper skips that g.
 * The held g cannot run, complete, or be freed, so its pointer stays valid
 * across the unlocked madvise -- restoring the exact liveness the default
 * (sole-resumer) mode relies on. */
int pygo_mn_sweep_try_claim(pygo_g_t *g)
{
    int st = PYGO_WS_PARKED;
    return __atomic_compare_exchange_n(&g->wake_state, &st, PYGO_WS_SWEEPING,
                                       0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
}

/* End a sweep claim after the madvise.  SWEEPING -> PARKED if no wake landed.
 * If a wake landed during the madvise the state is SWEEPING_WOKEN (wake_g
 * remembered it without enqueueing); convert it to QUEUED and push exactly once
 * -- the sweeper is the sole "winner" of that deferred wake, mirroring the
 * RUNNING_WOKEN -> QUEUED release, so the wake is never lost and never
 * duplicated. */
void pygo_mn_sweep_claim_release(pygo_g_t *g)
{
    int st = PYGO_WS_SWEEPING;
    if (__atomic_compare_exchange_n(&g->wake_state, &st, PYGO_WS_PARKED, 0,
                                    __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        return;   /* clean release; no wake during the madvise */
    }
    /* The only state try_claim could leave that isn't SWEEPING is
     * SWEEPING_WOKEN (no other actor touches a SWEEPING g except wake_g, which
     * only flips it to SWEEPING_WOKEN).  Re-enqueue the deferred wake. */
    __atomic_store_n(&g->wake_state, PYGO_WS_QUEUED, __ATOMIC_RELEASE);
    pygo_g_incref(g);   /* queue ref, dropped when consumed */
    pygo_mn_global_runq_push(g);
}

int pygo_mn_yield_current(void)
{
    pygo_hub_t *h = pygo_tls_hub;
    pygo_g_t *g = pygo_tls_current_g;
    if (h == NULL || g == NULL) {
        return 0;
    }
    /* Trivial-switch fast path: if there's no other work for this hub
     * -- no yielded g in local FIFO, no fresh g in our deque, no
     * sleeper due, no parked I/O -- yielding would just snap, swap to
     * hub_main, find an empty queue, swap back.  Skip it.
     *
     * We do NOT peek neighbours' deques here: a steal happens only when
     * a hub goes idle (hub_main's main path), and idle implies the
     * neighbour itself has nothing local to run.  Letting a g monopolise
     * a hub while neighbours have work is fine -- the work-stealing
     * scheduler is allowed to leave stealable items on a busy hub.
     * This matches single-thread's pygo_sched_yield fast path. */
    if (__builtin_expect(pygo_sched_ready_empty(&h->sched)
                         && pygo_cldeque_size(&h->deque) == 0
                         && h->sched.sleep_size == 0
                         && pygo_netpoll_parked_count() == 0
                         && pygo_blockpool_inflight() == 0, 1)) {
        return 1;
    }
    pygo_sched_ready_push(&h->sched, g);
    pygo_pystate_snap(&g->snap);
    pygo_tls_self_queued = 1;
    pygo_coro_yield();
    /* On return: hub_main has loaded g->snap, so we're back in our
     * own tstate slice and can keep running user code. */
    return 1;
}

/* sysmon watchdog thread.  Holds NO GIL and NO tstate -- it only reads
 * per-hub atomics and writes to stderr, never touching the Python C-API.
 * Scans every pygo_sysmon_tick_ns; logs a hub whose current pygo_coro_resume
 * has run longer than pygo_sysmon_wedge_ns (an unanticipated non-yielding
 * block), once per wedge episode, and a matching "recovered" line so a run's
 * wedge count + dwell are measurable.  DETECT-ONLY for now (no handoff). */
static PYGO_THREAD_RET pygo_sysmon_main(void *arg)
{
    int n = pygo_hub_count;
    /* w_seq[i] = the resume_seq currently logged as wedged on hub i (0 = none);
     * w_start[i] = its resume_start_ns, for the recovery-dwell report. */
    long      *w_seq   = (long *)calloc((size_t)(n > 0 ? n : 1), sizeof(long));
    long long *w_start = (long long *)calloc((size_t)(n > 0 ? n : 1),
                                             sizeof(long long));
    (void)arg;
    if (w_seq == NULL || w_start == NULL) {
        free(w_seq); free(w_start);
        PYGO_THREAD_RETURN(NULL);
    }
    while (!__atomic_load_n(&pygo_sysmon_stop, __ATOMIC_ACQUIRE)) {
        long long now = pygo_monotonic_ns();
        int i;
        for (i = 0; i < n; i++) {
            pygo_hub_t *h = &pygo_hubs[i];
            long long start = __atomic_load_n(&h->resume_start_ns,
                                              __ATOMIC_RELAXED);
            long seq = __atomic_load_n(&h->resume_seq, __ATOMIC_RELAXED);
            /* End of a previously-reported episode: the hub moved to a new
             * resume (seq changed) or finished resuming (start cleared). */
            if (w_seq[i] != 0 && (seq != w_seq[i] || start == 0)) {
                long pend = __atomic_load_n(&h->pending, __ATOMIC_RELAXED);
                fprintf(stderr,
                    "[PYGO_SYSMON] hub %d RECOVERED after ~%.1f ms "
                    "(pending=%ld)\n",
                    i, (double)(now - w_start[i]) / 1e6, pend);
                w_seq[i] = 0;
            }
            /* Start of a wedge episode: this resume has overrun the budget and
             * we have not logged THIS seq yet. */
            if (start != 0 && (now - start) > pygo_sysmon_wedge_ns &&
                w_seq[i] != seq) {
                long pend = __atomic_load_n(&h->pending, __ATOMIC_RELAXED);
                /* Classify by the hub tstate's attach state (racy read, fine
                 * for a diagnostic): DETACHED => the wedged g released the
                 * GIL-era tstate (blocking IO) and a standby thread could
                 * adopt the hub -> handoff-RECOVERABLE.  ATTACHED => CPU-bound
                 * or a raw tstate-holding syscall -> needs preemption, not a
                 * handoff.  SUSPENDED => stop-the-world. */
                int tss = h->tstate != NULL
                    ? __atomic_load_n(&h->tstate->state, __ATOMIC_RELAXED) : -1;
                const char *cls =
                    tss == PYGO_TS_DETACHED  ? "DETACHED (blocking-IO: handoff-recoverable)" :
                    tss == PYGO_TS_ATTACHED  ? "ATTACHED (CPU/raw-syscall: needs preemption)" :
                    tss == PYGO_TS_SUSPENDED ? "SUSPENDED (stop-the-world)" :
                                               "?";
                fprintf(stderr,
                    "[PYGO_SYSMON] hub %d WEDGED %.1f ms in g=%p tstate=%s -- "
                    "%ld g(s) stranded on this hub\n",
                    i, (double)(now - start) / 1e6, (void *)h->resume_g, cls,
                    pend > 0 ? pend - 1 : 0);
                w_seq[i]   = seq;
                w_start[i] = start;
            }
        }
        pygo_sleep_ns(pygo_sysmon_tick_ns);
    }
    free(w_seq);
    free(w_start);
    PYGO_THREAD_RETURN(NULL);
}

/* Read PYGO_SYSMON / PYGO_SYSMON_MS once and set the enable flag + threshold.
 * Must run in mn_init BEFORE the hub threads start so the per-resume
 * instrumentation is live from the first resume.  No-op (enabled stays 0)
 * unless PYGO_SYSMON is set to a non-"0" value. */
static void pygo_sysmon_config(void)
{
    const char *e = getenv("PYGO_SYSMON");
    const char *ms;
    pygo_sysmon_enabled = (e != NULL && e[0] != '0') ? 1 : 0;
    if (!pygo_sysmon_enabled) return;
    ms = getenv("PYGO_SYSMON_MS");
    if (ms != NULL) {
        long long v = atoll(ms);
        if (v > 0) pygo_sysmon_wedge_ns = v * 1000000LL;
    }
    /* Scan ~5x faster than the wedge budget (cap 10 ms) so detection latency
     * is a small fraction of the threshold without busy-spinning. */
    pygo_sysmon_tick_ns = pygo_sysmon_wedge_ns / 5;
    if (pygo_sysmon_tick_ns > 10000000LL) pygo_sysmon_tick_ns = 10000000LL;
    if (pygo_sysmon_tick_ns < 1000000LL)  pygo_sysmon_tick_ns = 1000000LL;
}

/* Spawn the watchdog thread (after hubs exist).  Enabled-gated; a spawn
 * failure is non-fatal -- the scheduler runs fine without the watchdog. */
static void pygo_sysmon_spawn(void)
{
    if (!pygo_sysmon_enabled) return;
    pygo_sysmon_stop = 0;
    if (pygo_thread_create(&pygo_sysmon_thread, pygo_sysmon_main, NULL) == 0) {
        pygo_sysmon_running = 1;
    } else {
        fprintf(stderr, "[PYGO_SYSMON] watchdog thread spawn failed; "
                        "stall detection disabled\n");
    }
}

/* Stop + join the watchdog.  Called at the top of mn_fini, BEFORE the hubs
 * are torn down, so the scan never reads freed hub state. */
static void pygo_sysmon_stop_join(void)
{
    if (!pygo_sysmon_running) return;
    __atomic_store_n(&pygo_sysmon_stop, 1, __ATOMIC_RELEASE);
    pygo_thread_join(pygo_sysmon_thread);
    pygo_sysmon_running = 0;
}

int pygo_mn_init(int n_threads)
{
    int i;
    PyInterpreterState *interp;
    PyThreadState *main_ts;
    PyThreadState *saved;

    if (pygo_hubs != NULL) return 0;  /* already inited */
#if defined(PYGO_OS_WINDOWS)
    /* CRITICAL_SECTION can't be statically initialised; do it exactly once
     * for the process (never destroyed -- a process-lifetime lock).  mn_init
     * runs single-threaded before any hub spawns, so the flag needs no
     * atomics beyond surviving repeated mn_init/mn_fini cycles. */
    {
        static int runq_lock_inited = 0;
        if (!runq_lock_inited) {
            pygo_mutex_init(&pygo_global_runq_lock);
            runq_lock_inited = 1;
        }
    }
#endif
    if (n_threads <= 0) {
        /* Auto: min(cores, 16).  Agent 6 measured pygo on 3.13t scaling
         * linearly to H=16 (~1.17 M ops/sec) then REGRESSING past H=32
         * for Python workloads -- atomic-refcount cache-line contention,
         * the same ceiling threads/asyncio hit.  16 is the right default
         * for Python; an explicit pygo_mn_init(H>16) is still honored
         * (worthwhile only for pure-C goroutine workloads). */
        n_threads = pygo_cpu_count();
        if (n_threads <= 0) n_threads = 4;
        if (n_threads > 16) n_threads = 16;
    }
    pygo_hubs = (pygo_hub_t *)PyMem_Calloc((size_t)n_threads, sizeof(pygo_hub_t));
    if (pygo_hubs == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    pygo_hub_count = n_threads;
    main_ts = PyThreadState_Get();
    interp = main_ts->interp;
    /* Per-g-tstate mode: capture the interp for PyThreadState_New and stand
     * the snap down BEFORE any hub thread starts running goroutines. */
    pygo_mn_interp = interp;
    pygo_set_per_g_tstate_mode(pygo_per_g_tstate_flag());
    /* Configure the sysmon watchdog (enable + threshold) BEFORE the hubs
     * spawn so the per-resume progress instrumentation is live immediately. */
    pygo_sysmon_config();

    for (i = 0; i < n_threads; i++) {
        pygo_hub_t *h = &pygo_hubs[i];
        h->id = i;
        pygo_sched_init(&h->sched);
        pygo_cldeque_init(&h->deque);
        pygo_mutex_init(&h->sub_lock);
        h->sub_head = h->sub_tail = NULL;
        h->tstate = PyThreadState_New(interp);
        if (h->tstate == NULL) {
            /* Clean up everything we've partially initialised so far
             * (mutexes + earlier tstates) and reset module state.
             * Without this, a later mn_fini would join an
             * uninitialised thread handle = undefined behaviour. */
            int j;
            pygo_mutex_destroy(&h->sub_lock);   /* this hub's mutex */
            for (j = 0; j < i; j++) {
                PyThreadState_Clear(pygo_hubs[j].tstate);
                PyThreadState_Delete(pygo_hubs[j].tstate);
                pygo_mutex_destroy(&pygo_hubs[j].sub_lock);
            }
            PyMem_Free(pygo_hubs);
            pygo_hubs = NULL;
            pygo_hub_count = 0;
            PyErr_NoMemory();
            return -1;
        }
    }
    saved = PyEval_SaveThread();
    for (i = 0; i < n_threads; i++) {
        if (pygo_thread_create(&pygo_hubs[i].thread,
                               pygo_hub_main, &pygo_hubs[i]) != 0) {
            /* Mark the unspawned hubs as already-stopping + join the
             * ones we did spawn before returning -1. */
            int j;
            for (j = i; j < n_threads; j++) {
                __atomic_store_n(&pygo_hubs[j].stopping, 1, __ATOMIC_RELEASE);
            }
            for (j = 0; j < i; j++) {
                __atomic_store_n(&pygo_hubs[j].stopping, 1, __ATOMIC_RELEASE);
                pygo_thread_join(pygo_hubs[j].thread);
            }
            PyEval_RestoreThread(saved);
            for (j = 0; j < n_threads; j++) {
                PyThreadState_Clear(pygo_hubs[j].tstate);
                PyThreadState_Delete(pygo_hubs[j].tstate);
                pygo_mutex_destroy(&pygo_hubs[j].sub_lock);
            }
            PyMem_Free(pygo_hubs);
            pygo_hubs = NULL;
            pygo_hub_count = 0;
            PyErr_SetString(PyExc_OSError, "thread spawn failed");
            return -1;
        }
    }
    PyEval_RestoreThread(saved);
    /* Hubs are up; start the watchdog (no-op unless PYGO_SYSMON is set). */
    pygo_sysmon_spawn();
    return n_threads;
}

/* Release any gs still parked in this hub's queues after the hub
 * thread has exited.  Hub thread is already joined by the caller, so
 * sub_head / deque / sched.ready / sched.sleep_heap are quiescent and
 * safe to drain from a single thread regardless of which primitive
 * they normally use for concurrent access.
 *
 * Each leaked g still holds: its Python callable ref, an fcontext
 * stack, and a slab entry.  pygo_g_decref unwinds all three when its
 * last reference drops.  The hub holds exactly one ref per g it
 * accepted (taken at spawn in pygo_mn_go_core, dropped at completion
 * in hub_main), so a single decref here mirrors the completion path. */
static void pygo_mn_hub_drain_leftovers(pygo_hub_t *h)
{
    pygo_g_t *sub;
    pygo_g_t *g;
    sub = h->sub_head;
    h->sub_head = h->sub_tail = NULL;
    while (sub != NULL) {
        pygo_g_t *next = sub->next;
        sub->next = NULL;
        pygo_g_decref(sub);
        sub = next;
    }
    while ((g = pygo_sched_ready_pop(&h->sched)) != NULL) {
        pygo_g_decref(g);
    }
    while ((g = (pygo_g_t *)pygo_cldeque_pop(&h->deque)) != NULL) {
        pygo_g_decref(g);
    }
    while (h->sched.sleep_size > 0) {
        g = pygo_sched_sleep_pop(&h->sched);
        if (g != NULL) pygo_g_decref(g);
    }
}

void pygo_mn_fini(void)
{
    int i;
    if (pygo_hubs == NULL) return;
    /* Stop the watchdog before any hub teardown so its scan never touches
     * freed hub state. */
    pygo_sysmon_stop_join();
    for (i = 0; i < pygo_hub_count; i++) {
        __atomic_store_n(&pygo_hubs[i].stopping, 1, __ATOMIC_RELEASE);
    }
    {
        PyThreadState *saved = PyEval_SaveThread();
        for (i = 0; i < pygo_hub_count; i++) {
            pygo_thread_join(pygo_hubs[i].thread);
        }
        PyEval_RestoreThread(saved);
    }
    /* Drain leftover gs BEFORE tearing down per-hub tstates: g->snap
     * may reference per-tstate state (exc_info, datastack chunks) that
     * pystate_snap_clear needs to release while the tstate is still
     * walkable.  pygo_g_decref also runs Py_XDECREF on callable /
     * result / error, which needs the main thread's GIL -- already
     * held here (we restored above). */
    for (i = 0; i < pygo_hub_count; i++) {
        pygo_mn_hub_drain_leftovers(&pygo_hubs[i]);
    }
    /* Drain the global run-queue too.  Exactly-once-wake means an entry here
     * is a single, live, yet-to-run woken g holding its one scheduler ref (no
     * per-entry ref; no stale duplicates).  A single decref mirrors the
     * completion path and frees it -- exactly like pygo_mn_hub_drain_leftovers
     * does for the sub_list / deque / FIFO.  Hubs are joined, so this is
     * single-threaded -- no lock needed. */
    {
        pygo_g_t *g = pygo_global_runq_head;
        pygo_global_runq_head = pygo_global_runq_tail = NULL;
        __atomic_store_n(&pygo_global_runq_len, 0, __ATOMIC_RELEASE);
        while (g != NULL) {
            pygo_g_t *next = g->next;
            g->next = NULL;
            pygo_g_decref(g);
            g = next;
        }
    }
    for (i = 0; i < pygo_hub_count; i++) {
        PyThreadState_Clear(pygo_hubs[i].tstate);
        PyThreadState_Delete(pygo_hubs[i].tstate);
        pygo_mutex_destroy(&pygo_hubs[i].sub_lock);
    }
    PyMem_Free(pygo_hubs);
    pygo_hubs = NULL;
    pygo_hub_count = 0;
    /* Reset the global pending counter so the next mn_init starts at
     * 0.  Any leaked g (incomplete at fini) leaves a non-zero residue
     * otherwise. */
    __atomic_store_n(&pygo_mn_pending_global, 0, __ATOMIC_RELEASE);
    /* Stand the snap back up for any later single-thread scheduler use. */
    pygo_set_per_g_tstate_mode(0);
    pygo_mn_interp = NULL;
}

/* Internal core: pick hub, alloc g, set up coro, submit, bump counters.
 * Either callable (Python path) or c_fn+c_arg (C-only path) must be set,
 * not both.  Returns 0 on success, -1 on failure (errno set).  Python
 * error is also set on failure when callable != NULL. */
static int pygo_mn_go_core(PyObject *callable, pygo_c_entry_fn c_fn,
                           void *c_arg)
{
    long n;
    int hub_idx;
    pygo_g_t *g;
    pygo_hub_t *h;
    if (pygo_hubs == NULL) {
        if (callable != NULL) {
            PyErr_SetString(PyExc_RuntimeError,
                            "pygo_mn_init() must be called first");
        }
        errno = EINVAL;
        return -1;
    }
    n = __atomic_fetch_add(&pygo_mn_spawn_counter, 1, __ATOMIC_RELAXED);
    hub_idx = (int)(n % pygo_hub_count);
    h = &pygo_hubs[hub_idx];
    g = pygo_g_slab_alloc();
    if (g == NULL) {
        if (callable != NULL) PyErr_NoMemory();
        errno = ENOMEM;
        return -1;
    }
    if (callable != NULL) {
        Py_INCREF(callable);
        g->callable = callable;
    } else {
        g->c_entry = c_fn;
        g->c_arg   = c_arg;
    }
    g->refcount = 1;
    g->coro = pygo_coro_new((size_t)h->sched.stack_size,
                            pygo_g_entry, g);
    if (g->coro == NULL) {
        if (callable != NULL) {
            Py_DECREF(callable);
            PyErr_NoMemory();
        }
        PyMem_Free(g);
        errno = ENOMEM;
        return -1;
    }
    /* Wake-state machine init.  Any global-runq mode (per-g-tstate OR
     * steal-woken) reads wake_state: a fresh g has never parked, so it is
     * conceptually owned/running from the moment a hub first resumes it
     * (slab-zeroed wake_state would be PARKED, which the QUEUED->RUNNING claim
     * and the park CAS both mis-handle).  Lift it to RUNNING up front. */
    if (pygo_use_global_runq()) {
        __atomic_store_n(&g->wake_state, PYGO_WS_RUNNING, __ATOMIC_RELEASE);
    }
    /* PYGO_PER_G_TSTATE only: give the g its own migratable PyThreadState.
     * g->tstate was zeroed by the slab alloc, so every other path leaves it
     * NULL (and pygo_g_decref's teardown is a no-op).  Steal-woken migrates via
     * the snap instead, so it allocates no tstate here. */
    if (pygo_get_per_g_tstate_mode() && pygo_mn_interp != NULL) {
        g->tstate = PyThreadState_New(pygo_mn_interp);
        if (g->tstate == NULL) {
            pygo_coro_destroy(g->coro);
            if (callable != NULL) {
                Py_DECREF(callable);
                PyErr_NoMemory();
            }
            PyMem_Free(g);
            errno = ENOMEM;
            return -1;
        }
    }
    pygo_mn_hub_submit(h, g);
    pygo_mn_pending_inc(h);
    return 0;
}

int pygo_mn_go_c(pygo_c_entry_fn fn, void *arg)
{
    if (fn == NULL) { errno = EINVAL; return -1; }
    return pygo_mn_go_core(NULL, fn, arg);
}

PyObject *pygo_mn_go(PyObject *callable)
{
    if (pygo_mn_go_core(callable, NULL, NULL) < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

Py_ssize_t pygo_mn_run(void)
{
    int i;
    /* Py_ssize_t to match the per-hub completed counter (which is
     * Py_ssize_t -- long long on 64-bit Windows MSVC, long on
     * 64-bit POSIX).  Using plain long would truncate on Windows. */
    Py_ssize_t total_completed = 0;
    PyThreadState *saved = PyEval_SaveThread();
    for (;;) {
        /* Read the GLOBAL pending counter, not the per-hub fields.
         * The per-hub fields race during work-stealing (source dec
         * before dest inc); the global counter is only touched on
         * spawn + completion, so it's monotonic across the lifecycle
         * of each g and reliably reaches 0 iff all gs are done. */
        long total = __atomic_load_n(&pygo_mn_pending_global,
                                     __ATOMIC_ACQUIRE);
        if (total == 0) break;
        pygo_sleep_ns(1000000LL);   /* 1 ms poll */
    }
    /* Silence the unused-variable warning that the per-hub loop used
     * to consume. */
    (void)i;
    PyEval_RestoreThread(saved);
    for (i = 0; i < pygo_hub_count; i++) {
        total_completed += __atomic_load_n(&pygo_hubs[i].sched.completed,
                                           __ATOMIC_ACQUIRE);
    }
    return total_completed;
}
