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
#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "mn_sched.h"
#include "pygo_sched.h"
#include "coro.h"
#include "cldeque.h"

#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

typedef struct pygo_hub {
    int id;
    pthread_t thread;
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
    pthread_mutex_t sub_lock;
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
static void *pygo_hub_main(void *arg)
{
    pygo_hub_t *h = (pygo_hub_t *)arg;
    PyEval_RestoreThread(h->tstate);
    pygo_tls_hub = h;

    while (!h->stopping) {
        pygo_g_t *g;
        /* Drain the submission list into the deque first.  Pushing to
         * the deque is owner-only, so we (the hub) move fresh gs from
         * external producers onto our deque before anyone else looks. */
        pthread_mutex_lock(&h->sub_lock);
        {
            pygo_g_t *sub = h->sub_head;
            h->sub_head = h->sub_tail = NULL;
            pthread_mutex_unlock(&h->sub_lock);
            while (sub != NULL) {
                pygo_g_t *next = sub->next;
                sub->next = NULL;
                pygo_cldeque_push(&h->deque, sub);
                sub = next;
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
                struct timespec ts;
                for (j = 0; j < pygo_hub_count; j++) {
                    total += __atomic_load_n(&pygo_hubs[j].pending,
                                             __ATOMIC_RELAXED);
                }
                ts.tv_sec = 0;
                ts.tv_nsec = (total == 0) ? 500000 : 100000;
                /* Release the GIL across the nanosleep so the main
                 * thread (waiting in mn_run / mn_fini to re-acquire)
                 * isn't blocked.  On free-threaded 3.13t this is a
                 * no-op but harmless; on GIL builds it's essential. */
                saved = PyEval_SaveThread();
                nanosleep(&ts, NULL);
                PyEval_RestoreThread(saved);
                continue;
            }
        }

        /* Phase B snap dance.  Save hub's tstate; load g's snap (or
         * NULL the datastack on first run); resume; restore hub.
         * pygo_pystate_snap_t is a few-dozen-byte stack-allocated bag. */
        {
            pygo_pystate_snap_t hub_snap;
            int self_queued;

            memset(&hub_snap, 0, sizeof(hub_snap));
            pygo_pystate_snap(&hub_snap);

            if (g->snap.valid) {
                pygo_pystate_load(&g->snap);
            } else {
                PyThreadState *ts = PyThreadState_GET();
                ts->datastack_chunk = NULL;
                ts->datastack_top = NULL;
                ts->datastack_limit = NULL;
#if PY_VERSION_HEX >= 0x030D0000
                ts->current_frame = NULL;
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

            pygo_pystate_load(&hub_snap);

            if (pygo_coro_done(g->coro)) {
                __atomic_sub_fetch(&h->pending, 1, __ATOMIC_RELAXED);
                __atomic_add_fetch(&h->sched.completed, 1, __ATOMIC_RELAXED);
                pygo_g_decref(g);
            } else if (!self_queued) {
                /* Raw pygo_coro_yield() -- g didn't go through
                 * sched_yield to push itself.  Keep it alive on the
                 * local FIFO. */
                pygo_sched_ready_push(&h->sched, g);
            }
        }
    }
    pygo_tls_hub = NULL;
    PyEval_SaveThread();
    return NULL;
}

int pygo_mn_yield_current(void)
{
    pygo_hub_t *h = pygo_tls_hub;
    pygo_g_t *g = pygo_tls_current_g;
    if (h == NULL || g == NULL) {
        return 0;
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
        n_threads = (int)sysconf(_SC_NPROCESSORS_ONLN);
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
        pthread_mutex_init(&h->sub_lock, NULL);
        h->sub_head = h->sub_tail = NULL;
        h->tstate = PyThreadState_New(interp);
        if (h->tstate == NULL) {
            pygo_hub_count = i;   /* only init'd this many */
            return -1;
        }
    }
    saved = PyEval_SaveThread();
    for (i = 0; i < n_threads; i++) {
        pthread_create(&pygo_hubs[i].thread, NULL,
                       pygo_hub_main, &pygo_hubs[i]);
    }
    PyEval_RestoreThread(saved);
    return n_threads;
}

void pygo_mn_fini(void)
{
    int i;
    if (pygo_hubs == NULL) return;
    for (i = 0; i < pygo_hub_count; i++) {
        pygo_hubs[i].stopping = 1;
    }
    {
        PyThreadState *saved = PyEval_SaveThread();
        for (i = 0; i < pygo_hub_count; i++) {
            pthread_join(pygo_hubs[i].thread, NULL);
        }
        PyEval_RestoreThread(saved);
    }
    for (i = 0; i < pygo_hub_count; i++) {
        PyThreadState_Clear(pygo_hubs[i].tstate);
        PyThreadState_Delete(pygo_hubs[i].tstate);
        pthread_mutex_destroy(&pygo_hubs[i].sub_lock);
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
    /* Push to the hub's MPSC submission list; the hub thread (the
     * deque's owner) will drain this onto its own deque next iter.
     * Pushing the deque directly here would race with the hub's pop
     * and corrupt the deque's bottom counter. */
    pthread_mutex_lock(&h->sub_lock);
    g->next = NULL;
    if (h->sub_tail != NULL) {
        h->sub_tail->next = g;
    } else {
        h->sub_head = g;
    }
    h->sub_tail = g;
    pthread_mutex_unlock(&h->sub_lock);
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
            total += __atomic_load_n(&pygo_hubs[i].pending,
                                     __ATOMIC_RELAXED);
        }
        if (total == 0) break;
        {
            struct timespec ts = {0, 1000000};  /* 1ms poll */
            nanosleep(&ts, NULL);
        }
    }
    PyEval_RestoreThread(saved);
    for (i = 0; i < pygo_hub_count; i++) {
        total_completed += pygo_hubs[i].sched.completed;
    }
    return total_completed;
}
