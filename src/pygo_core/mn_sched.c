/* mn_sched.c -- M:N scheduler.
 *
 * N OS threads, each one a "hub" with its own pygo_sched_t and a
 * Chase-Lev work-stealing deque.  Goroutines spawned by pygo_mn_go
 * are round-robined onto hubs at spawn time; once running, a g is
 * pinned to its hub (its C stack is absolute address).
 *
 * Work stealing: when a hub's local deque is empty, it tries to
 * steal from a neighbour's deque.  The stolen g hasn't run yet so
 * it's a fresh pygo_g_t with no live stack -- safe to migrate.  This
 * keeps load balanced when spawns are concentrated on one thread.
 *
 * Free-threaded Python (3.13t) is required to get real parallelism
 * out of this: each hub thread has its own PyThreadState and runs
 * Python code without contending on a global lock.  On a GIL build
 * this still works correctly but serialises through the GIL.
 *
 * What's NOT in v1:
 *   - cross-hub netpoll: each hub has its own epoll fd; a g that
 *     parks on I/O stays on its hub.  A future version could share
 *     a single epoll across hubs and wake whichever hub is idle.
 *   - hub-local stack pool: per-thread TLS already, no change needed.
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
} pygo_hub_t;

static pygo_hub_t *pygo_hubs = NULL;
static int pygo_hub_count = 0;
static volatile long pygo_mn_spawn_counter = 0;

/* M:N entry shim -- same as sched.c's pygo_g_entry but reachable
 * from outside the translation unit.  Runs the user's callable,
 * captures result/exception, sets done. */
static void pygo_mn_g_entry(void *user)
{
    pygo_g_t *g = (pygo_g_t *)user;
    PyObject *res = PyObject_CallNoArgs(g->callable);
    if (res == NULL) {
        PyObject *type, *value, *tb;
        PyErr_Fetch(&type, &value, &tb);
        PyErr_NormalizeException(&type, &value, &tb);
        if (value == NULL) {
            value = Py_None;
            Py_INCREF(value);
        }
        if (tb != NULL) {
            PyException_SetTraceback(value, tb);
            Py_DECREF(tb);
        }
        Py_XDECREF(type);
        g->error = value;
    } else {
        g->result = res;
    }
    g->done = 1;
}

/* Hub thread main loop.  v1: only handles fire-and-forget gs
 * (callable runs to completion without yielding via sched_yield).
 * Yield support requires a thread-local "current hub" so
 * pygo_sched_yield knows which deque to push back to; planned for v2. */
static void *pygo_hub_main(void *arg)
{
    pygo_hub_t *h = (pygo_hub_t *)arg;
    PyEval_RestoreThread(h->tstate);

    while (!h->stopping) {
        pygo_g_t *g = (pygo_g_t *)pygo_cldeque_pop(&h->deque);
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
                for (j = 0; j < pygo_hub_count; j++) {
                    total += __atomic_load_n(&pygo_hubs[j].pending,
                                             __ATOMIC_RELAXED);
                }
                if (total == 0) {
                    /* Idle drain.  If user is done, we'll be told via
                     * stopping; until then poll. */
                    struct timespec ts = {0, 500000};   /* 500us */
                    nanosleep(&ts, NULL);
                    continue;
                }
                struct timespec ts = {0, 100000};   /* 100us */
                nanosleep(&ts, NULL);
                continue;
            }
        }
        /* Run g.  v1 assumes g runs to completion (no yield). */
        h->sched.current = g;
        pygo_coro_resume(g->coro);
        h->sched.current = NULL;
        if (pygo_coro_done(g->coro)) {
            __atomic_sub_fetch(&h->pending, 1, __ATOMIC_RELAXED);
            __atomic_add_fetch(&h->sched.completed, 1, __ATOMIC_RELAXED);
            pygo_g_decref(g);
        } else {
            /* g yielded but v1 doesn't support yield in M:N -- push
             * it back onto the deque so it keeps running.  This will
             * spin in busy-yield gs, which is the documented v1 limit. */
            pygo_cldeque_push(&h->deque, g);
        }
    }
    PyEval_SaveThread();
    return NULL;
}

int pygo_mn_init(int n_threads)
{
    int i;
    PyInterpreterState *interp;
    PyThreadState *main_ts;

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
        h->tstate = PyThreadState_New(interp);
        if (h->tstate == NULL) {
            pygo_hub_count = i;   /* only init'd this many */
            return -1;
        }
    }
    /* Spawn pthreads.  Must release the GIL on the main thread first
     * so the hubs can acquire their tstates. */
    PyThreadState *saved = PyEval_SaveThread();
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
    /* Signal stop and wait. */
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
    /* Round-robin to a hub. */
    n = __atomic_fetch_add(&pygo_mn_spawn_counter, 1, __ATOMIC_RELAXED);
    hub_idx = (int)(n % pygo_hub_count);
    h = &pygo_hubs[hub_idx];
    /* Build the g (mostly identical to pygo_sched_spawn but pushes
     * to the hub's deque). */
    g = (pygo_g_t *)PyMem_Calloc(1, sizeof(*g));
    if (g == NULL) {
        return PyErr_NoMemory();
    }
    Py_INCREF(callable);
    g->callable = callable;
    g->refcount = 1;
    g->coro = pygo_coro_new((size_t)h->sched.stack_size,
                            pygo_mn_g_entry, g);
    if (g->coro == NULL) {
        Py_DECREF(callable);
        PyMem_Free(g);
        PyErr_NoMemory();
        return NULL;
    }
    pygo_cldeque_push(&h->deque, g);
    __atomic_add_fetch(&h->pending, 1, __ATOMIC_RELAXED);
    Py_RETURN_NONE;
}

Py_ssize_t pygo_mn_run(void)
{
    int i;
    long total_completed = 0;
    /* Wait for all hubs to drain.  We can't pthread_join here because
     * hubs may keep running for new gs; instead we poll until all
     * pending counts are 0. */
    PyThreadState *saved = PyEval_SaveThread();
    for (;;) {
        long total = 0;
        for (i = 0; i < pygo_hub_count; i++) {
            total += __atomic_load_n(&pygo_hubs[i].pending,
                                     __ATOMIC_RELAXED);
        }
        if (total == 0) break;
        struct timespec ts = {0, 1000000};  /* 1ms poll */
        nanosleep(&ts, NULL);
    }
    PyEval_RestoreThread(saved);
    for (i = 0; i < pygo_hub_count; i++) {
        total_completed += pygo_hubs[i].sched.completed;
    }
    return total_completed;
}
