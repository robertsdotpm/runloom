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
#include "coro.h"
#include "cldeque.h"

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
} pygo_hub_t;

static pygo_hub_t *pygo_hubs = NULL;
static int pygo_hub_count = 0;
static volatile long pygo_mn_spawn_counter = 0;

/* TLS pointers set at hub_main entry.  pygo_mn_yield_current() and
 * pygo_mn_current_hub() read these to route per-g operations to the
 * right hub without each call site needing to look it up. */
static __thread pygo_hub_t *pygo_tls_hub = NULL;
static __thread pygo_g_t   *pygo_tls_current_g = NULL;
/* Set by pygo_mn_yield_current() before pygo_coro_yield(); read by
 * hub_main after pygo_coro_resume returns.  Tells hub_main "the g
 * has already put itself on a queue, you don't need to requeue".
 * Distinguishes scheduler-aware yield (sched_yield) from raw yield
 * (pygo_core.yield_() -> pygo_coro_yield directly). */
static __thread int pygo_tls_self_queued = 0;

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

    /* hub_snap is loop-invariant for the same reason sched_snap is in
     * pygo_sched_drain: hub_main runs no Python work between
     * iterations except via pygo_g_decref's tp_dealloc, where we
     * explicitly restore + re-snap.  Hoisting the per-iter snap+load
     * out of the loop is ~10 ns/yield on the M:N hot path. */
    pygo_pystate_snap(&hub_snap);

    while (!__atomic_load_n(&h->stopping, __ATOMIC_ACQUIRE)) {
        pygo_g_t *g;
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
                 * local FIFO (hub-pinned, the netpoll-wake path). */
                if (sub->snap.valid) {
                    pygo_sched_ready_push(&h->sched, sub);
                } else {
                    pygo_cldeque_push(&h->deque, sub);
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
                pygo_sched_ready_push(&h->sched, woke);
            }
        }

        g = pygo_sched_ready_pop(&h->sched);     /* local yielded */
        if (g == NULL) {
            g = (pygo_g_t *)pygo_cldeque_pop(&h->deque);  /* own fresh */
        }
        if (g == NULL) {
            int i;
            for (i = 0; i < pygo_hub_count; i++) {
                if (i == h->id) continue;
                g = (pygo_g_t *)pygo_cldeque_steal(&pygo_hubs[i].deque);
                if (g != NULL) {
                    __atomic_sub_fetch(&pygo_hubs[i].pending, 1,
                                       __ATOMIC_RELAXED);
                    __atomic_add_fetch(&h->pending, 1, __ATOMIC_RELAXED);
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
                saved = PyEval_SaveThread();
                if (parked > 0) {
                    /* Cap pump timeout the same way -- if a local timer
                     * is due sooner than the next I/O event, we want to
                     * wake to handle it. */
                    long long pump_ns = 1000000LL;
                    if (h->sched.sleep_size > 0 && idle_ns < pump_ns) {
                        pump_ns = idle_ns;
                    }
                    pygo_netpoll_pump(pump_ns);
                } else {
                    if (idle_ns <= 0) idle_ns = 1;
                    pygo_sleep_ns(idle_ns);
                }
                PyEval_RestoreThread(saved);
                continue;
            }
        }

        /* Phase B snap dance: load g's tstate slice; resume; if g
         * completed, restore hub's tstate before any Python that
         * might allocate frames.  hub_snap is hoisted out of the loop
         * (see entry comment). */
        {
            int self_queued;

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
            pygo_coro_resume(g->coro);
            self_queued = pygo_tls_self_queued;
            pygo_tls_self_queued = 0;
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
                /* Bump completed BEFORE decrementing pending, both
                 * with release ordering, so a mn_run reader that
                 * observes pending == 0 (via acquire) is also
                 * guaranteed to see the matching completed++. */
                __atomic_add_fetch(&h->sched.completed, 1, __ATOMIC_RELEASE);
                __atomic_sub_fetch(&h->pending, 1, __ATOMIC_RELEASE);
                pygo_g_decref(g);
                pygo_pystate_snap(&hub_snap);
            } else if (!self_queued) {
                /* Raw pygo_coro_yield() -- g didn't go through
                 * sched_yield to push itself.  Keep it alive on the
                 * local FIFO.  tstate still has g's state from after
                 * resume; that's fine -- next iter's g_next->snap
                 * load overwrites. */
                pygo_sched_ready_push(&h->sched, g);
            }
            /* sched_yield path: g pushed itself, tstate has g's state,
             * no work needed here. */
        }
    }
    /* Restore hub's tstate before the thread exits. */
    pygo_pystate_load(&hub_snap);
    pygo_tls_hub = NULL;
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

pygo_g_t *pygo_mn_tls_current_g(void)
{
    return pygo_tls_current_g;
}

void pygo_mn_tls_mark_parked(void)
{
    pygo_tls_self_queued = 1;
}

pygo_sched_t *pygo_mn_current_sched(void)
{
    pygo_hub_t *h = pygo_tls_hub;
    return h ? &h->sched : NULL;
}

/* Push g onto a hub's submission list.  Called by netpoll pump to
 * route an I/O-woken g back to whichever hub it was running on.
 * (Also used internally by mn_go.)  Hub_main drains submissions every
 * iteration and routes each entry to either the deque (if g is fresh)
 * or the local FIFO (if g has saved state -- the netpoll-wake case). */
static void pygo_mn_hub_submit(pygo_hub_t *h, pygo_g_t *g)
{
    pygo_mutex_lock(&h->sub_lock);
    g->next = NULL;
    if (h->sub_tail != NULL) {
        h->sub_tail->next = g;
    } else {
        h->sub_head = g;
    }
    h->sub_tail = g;
    pygo_mutex_unlock(&h->sub_lock);
}

void pygo_mn_wake_g(void *hub_opaque, pygo_g_t *g)
{
    if (hub_opaque == NULL) {
        /* g belongs to the single-thread global scheduler (or netpoll
         * was used outside any hub context). */
        pygo_sched_ready_push(pygo_sched_get(), g);
        return;
    }
    pygo_mn_hub_submit((pygo_hub_t *)hub_opaque, g);
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
    if (__builtin_expect(h->sched.ready_head == NULL
                         && pygo_cldeque_size(&h->deque) == 0
                         && h->sched.sleep_size == 0
                         && pygo_netpoll_parked_count() == 0, 1)) {
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

int pygo_mn_init(int n_threads)
{
    int i;
    PyInterpreterState *interp;
    PyThreadState *main_ts;
    PyThreadState *saved;

    if (pygo_hubs != NULL) return 0;  /* already inited */
    if (n_threads <= 0) {
        n_threads = pygo_cpu_count();
        if (n_threads <= 0) n_threads = 4;
    }
    pygo_hubs = (pygo_hub_t *)PyMem_Calloc((size_t)n_threads, sizeof(pygo_hub_t));
    if (pygo_hubs == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    pygo_hub_count = n_threads;
    main_ts = PyThreadState_Get();
    interp = main_ts->interp;

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
    return n_threads;
}

void pygo_mn_fini(void)
{
    int i;
    if (pygo_hubs == NULL) return;
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
    for (i = 0; i < pygo_hub_count; i++) {
        PyThreadState_Clear(pygo_hubs[i].tstate);
        PyThreadState_Delete(pygo_hubs[i].tstate);
        pygo_mutex_destroy(&pygo_hubs[i].sub_lock);
    }
    PyMem_Free(pygo_hubs);
    pygo_hubs = NULL;
    pygo_hub_count = 0;
}

PyObject *pygo_mn_go(PyObject *callable)
{
    long n;
    int hub_idx;
    pygo_g_t *g;
    pygo_hub_t *h;
    if (pygo_hubs == NULL) {
        PyErr_SetString(PyExc_RuntimeError,
                        "pygo_mn_init() must be called first");
        return NULL;
    }
    n = __atomic_fetch_add(&pygo_mn_spawn_counter, 1, __ATOMIC_RELAXED);
    hub_idx = (int)(n % pygo_hub_count);
    h = &pygo_hubs[hub_idx];
    g = (pygo_g_t *)PyMem_Calloc(1, sizeof(*g));
    if (g == NULL) {
        return PyErr_NoMemory();
    }
    Py_INCREF(callable);
    g->callable = callable;
    g->refcount = 1;
    g->coro = pygo_coro_new((size_t)h->sched.stack_size,
                            pygo_g_entry, g);
    if (g->coro == NULL) {
        Py_DECREF(callable);
        PyMem_Free(g);
        PyErr_NoMemory();
        return NULL;
    }
    /* Submit via the shared MPSC helper.  Hub_main drains submissions
     * each iteration -- fresh gs (snap.valid==0) get pushed to the
     * Chase-Lev deque (stealable); yielded-then-woken gs (snap.valid==1,
     * the netpoll path) get pushed to the local FIFO (hub-pinned). */
    pygo_mn_hub_submit(h, g);
    __atomic_add_fetch(&h->pending, 1, __ATOMIC_RELAXED);
    Py_RETURN_NONE;
}

Py_ssize_t pygo_mn_run(void)
{
    int i;
    long total_completed = 0;
    PyThreadState *saved = PyEval_SaveThread();
    for (;;) {
        long total = 0;
        for (i = 0; i < pygo_hub_count; i++) {
            /* ACQUIRE pairs with the RELEASE stores hub_main does on
             * pending dec and completed inc; once we see pending == 0
             * we're guaranteed to see all corresponding completed++. */
            total += __atomic_load_n(&pygo_hubs[i].pending,
                                     __ATOMIC_ACQUIRE);
        }
        if (total == 0) break;
        pygo_sleep_ns(1000000LL);   /* 1 ms poll */
    }
    PyEval_RestoreThread(saved);
    for (i = 0; i < pygo_hub_count; i++) {
        total_completed += __atomic_load_n(&pygo_hubs[i].sched.completed,
                                           __ATOMIC_ACQUIRE);
    }
    return total_completed;
}
