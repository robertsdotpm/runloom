/* sched.c -- C-level cooperative scheduler.
 *
 * Cost model (target 50-100 ns per yield once everything compiles):
 *   - yield: 2 list ops + ptr swap + asm switch + tstate snap/restore.
 *   - resume: same in reverse.
 *
 * What's _not_ here (yet):
 *   - netpoll integration (parking on fds)
 *   - work-stealing across threads
 *   - free-threaded-Python coexistence
 *
 * The Python side talks to us through a tiny Python type defined in
 * module.c (PygoG).  The user-visible API is `pygo.go / yield_ /
 * sleep / run`.
 */

#define _POSIX_C_SOURCE 200809L

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "pygo_sched.h"

#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ---- monotonic seconds ---- */
static double pygo_monotonic(void)
{
    struct timespec ts;
#if defined(CLOCK_MONOTONIC)
    if (clock_gettime(CLOCK_MONOTONIC, &ts) == 0) {
        return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
    }
#endif
    return (double)time(NULL);
}

/* ---- tstate snapshot ---- */
static void pygo_g_tstate_save(pygo_g_t *g)
{
    PyThreadState *ts = PyThreadState_GET();
#if PY_VERSION_HEX >= 0x030C0000
    g->py_recursion_remaining = ts->py_recursion_remaining;
    g->c_recursion_remaining = ts->c_recursion_remaining;
#else
    g->recursion_depth = ts->recursion_depth;
#endif
    g->snapshot_valid = 1;
}

static void pygo_g_tstate_restore(const pygo_g_t *g)
{
    PyThreadState *ts;
    if (!g->snapshot_valid) return;
    ts = PyThreadState_GET();
#if PY_VERSION_HEX >= 0x030C0000
    ts->py_recursion_remaining = g->py_recursion_remaining;
    ts->c_recursion_remaining = g->c_recursion_remaining;
#else
    ts->recursion_depth = g->recursion_depth;
#endif
}

/* ---- Ready FIFO ops (singly-linked, head=pop, tail=push) ---- */
static void pygo_ready_push(pygo_sched_t *s, pygo_g_t *g)
{
    g->next = NULL;
    if (s->ready_tail == NULL) {
        s->ready_head = s->ready_tail = g;
    } else {
        s->ready_tail->next = g;
        s->ready_tail = g;
    }
}

static pygo_g_t *pygo_ready_pop(pygo_sched_t *s)
{
    pygo_g_t *g = s->ready_head;
    if (g == NULL) return NULL;
    s->ready_head = g->next;
    if (s->ready_head == NULL) {
        s->ready_tail = NULL;
    }
    g->next = NULL;
    return g;
}

/* ---- Sleep heap (min-heap by wake_at) ---- */
static int pygo_sleep_grow(pygo_sched_t *s)
{
    Py_ssize_t new_cap = s->sleep_cap ? s->sleep_cap * 2 : 16;
    pygo_g_t **new_heap = (pygo_g_t **)PyMem_Realloc(
        s->sleep_heap, sizeof(pygo_g_t *) * (size_t)(new_cap + 1));
    if (new_heap == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    s->sleep_heap = new_heap;
    s->sleep_cap = new_cap;
    return 0;
}

static int pygo_sleep_push(pygo_sched_t *s, pygo_g_t *g)
{
    Py_ssize_t i;
    if (s->sleep_size + 1 > s->sleep_cap) {
        if (pygo_sleep_grow(s) < 0) return -1;
    }
    s->sleep_size++;
    i = s->sleep_size;
    s->sleep_heap[i] = g;
    /* sift up */
    while (i > 1 && s->sleep_heap[i / 2]->wake_at > g->wake_at) {
        s->sleep_heap[i] = s->sleep_heap[i / 2];
        i /= 2;
    }
    s->sleep_heap[i] = g;
    return 0;
}

static pygo_g_t *pygo_sleep_peek(pygo_sched_t *s)
{
    if (s->sleep_size == 0) return NULL;
    return s->sleep_heap[1];
}

static pygo_g_t *pygo_sleep_pop(pygo_sched_t *s)
{
    pygo_g_t *top;
    pygo_g_t *last;
    Py_ssize_t i, child;
    if (s->sleep_size == 0) return NULL;
    top = s->sleep_heap[1];
    last = s->sleep_heap[s->sleep_size];
    s->sleep_size--;
    if (s->sleep_size == 0) return top;
    i = 1;
    while (1) {
        child = i * 2;
        if (child > s->sleep_size) break;
        if (child + 1 <= s->sleep_size &&
            s->sleep_heap[child + 1]->wake_at < s->sleep_heap[child]->wake_at) {
            child++;
        }
        if (last->wake_at <= s->sleep_heap[child]->wake_at) break;
        s->sleep_heap[i] = s->sleep_heap[child];
        i = child;
    }
    s->sleep_heap[i] = last;
    return top;
}

/* ---- Scheduler lifecycle ---- */
static pygo_sched_t pygo_global_sched;
static int pygo_global_sched_init_done = 0;

void pygo_sched_init(pygo_sched_t *s)
{
    s->ready_head = NULL;
    s->ready_tail = NULL;
    s->current = NULL;
    s->sleep_heap = NULL;
    s->sleep_size = 0;
    s->sleep_cap = 0;
    s->stack_size = 131072;
    s->completed = 0;
    s->stopping = 0;
}

pygo_sched_t *pygo_sched_get(void)
{
    if (!pygo_global_sched_init_done) {
        pygo_sched_init(&pygo_global_sched);
        pygo_global_sched_init_done = 1;
    }
    return &pygo_global_sched;
}

/* ---- Coro entry shim ---- */
static void pygo_g_entry(void *user)
{
    pygo_g_t *g = (pygo_g_t *)user;
    PyObject *res;
    res = PyObject_CallNoArgs(g->callable);
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
    /* Falls back through asm trampoline -> infinite swap to caller. */
}

/* ---- Refcount ---- */
void pygo_g_incref(pygo_g_t *g)
{
    if (g) g->refcount++;
}

void pygo_g_decref(pygo_g_t *g)
{
    if (g == NULL) return;
    g->refcount--;
    if (g->refcount <= 0) {
        if (g->coro != NULL) {
            pygo_coro_destroy(g->coro);
            g->coro = NULL;
        }
        Py_XDECREF(g->callable);
        Py_XDECREF(g->result);
        Py_XDECREF(g->error);
        PyMem_Free(g);
    }
}

/* ---- Spawn ---- */
PyObject *pygo_sched_spawn(pygo_sched_t *s, PyObject *callable)
{
    pygo_g_t *g = (pygo_g_t *)PyMem_Calloc(1, sizeof(*g));
    if (g == NULL) {
        PyErr_NoMemory();
        return NULL;
    }
    Py_INCREF(callable);
    g->callable = callable;
    g->refcount = 1;   /* one ref for the scheduler queue */
    g->coro = pygo_coro_new((size_t)s->stack_size,
                            pygo_g_entry, g);
    if (g->coro == NULL) {
        Py_DECREF(g->callable);
        PyMem_Free(g);
        PyErr_SetString(PyExc_MemoryError, "pygo_coro_new failed");
        return NULL;
    }
    pygo_ready_push(s, g);
    return PyCapsule_New(g, "pygo_g", NULL);
}

/* ---- Yield ---- */
void pygo_sched_yield(pygo_sched_t *s)
{
    pygo_g_t *g = s->current;
    if (g == NULL) return;
    pygo_ready_push(s, g);
    /* Snapshot tstate before leaving the coroutine, so when the
     * scheduler resumes a different g, that g's snapshot is restored
     * cleanly. */
    pygo_g_tstate_save(g);
    pygo_coro_yield();
    /* On resume, our snapshot has been restored by the resumer. */
}

/* ---- Sleep ---- */
void pygo_sched_sleep_until(pygo_sched_t *s, double wake_at)
{
    pygo_g_t *g = s->current;
    if (g == NULL) return;
    g->wake_at = wake_at;
    if (pygo_sleep_push(s, g) < 0) {
        return; /* leave g in current; caller will see exception */
    }
    pygo_g_tstate_save(g);
    pygo_coro_yield();
}

/* ---- Drain (main loop) ---- */
Py_ssize_t pygo_sched_drain(pygo_sched_t *s)
{
    Py_ssize_t completed_before = s->completed;
    s->stopping = 0;

    while (!s->stopping && (s->ready_head != NULL || s->sleep_size > 0)) {
        double now = pygo_monotonic();
        /* Wake up any sleepers whose time has come. */
        while (s->sleep_size > 0 && pygo_sleep_peek(s)->wake_at <= now) {
            pygo_g_t *woke = pygo_sleep_pop(s);
            pygo_ready_push(s, woke);
        }
        if (s->ready_head == NULL && s->sleep_size > 0) {
            double gap = pygo_sleep_peek(s)->wake_at - now;
            if (gap > 0) {
                /* No netpoll yet; block the OS thread.  Sleep in small
                 * chunks so a future cancellation could break out. */
                struct timespec req, rem;
                if (gap > 0.05) gap = 0.05;
                req.tv_sec = (time_t)gap;
                req.tv_nsec = (long)((gap - (double)req.tv_sec) * 1e9);
                nanosleep(&req, &rem);
            }
            continue;
        }
        /* Pop a ready g and resume it.
         *
         * Snapshot dance for tstate (CPython 3.12 recursion counters):
         *   1. Save the SCHEDULER's tstate before we touch anything.
         *   2. If g has a valid snapshot, restore it.  Otherwise the g
         *      inherits the scheduler's tstate (first-run case).
         *   3. Resume into g.  G runs Python code.  When it yields it
         *      calls pygo_sched_yield which saves into g's snapshot.
         *   4. After the swap returns, restore the SCHEDULER's tstate
         *      so subsequent gs start from a clean baseline. */
        {
            pygo_g_t *g = pygo_ready_pop(s);
            pygo_g_t *prev = s->current;
            pygo_g_t sched_snap;   /* tiny stack-allocated bag for save */
            sched_snap.snapshot_valid = 0;
            pygo_g_tstate_save(&sched_snap);

            s->current = g;
            if (g->snapshot_valid) {
                pygo_g_tstate_restore(g);
            }
            pygo_coro_resume(g->coro);
            if (!pygo_coro_done(g->coro)) {
                /* g yielded.  Capture its state for next time.  Then
                 * restore the scheduler's. */
                pygo_g_tstate_save(g);
            }
            pygo_g_tstate_restore(&sched_snap);
            s->current = prev;

            if (pygo_coro_done(g->coro)) {
                s->completed++;
                pygo_g_decref(g);   /* scheduler releases its ref */
            }
        }
    }
    return s->completed - completed_before;
}
