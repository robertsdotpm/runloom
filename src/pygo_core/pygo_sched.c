/* pygo_sched.c -- C-level cooperative scheduler.
 *
 * Cost model (target 50-100 ns per yield once everything compiles):
 *   - yield: 2 list ops + ptr swap + asm switch + tstate snap/restore.
 *   - resume: same in reverse.
 *
 * What's _not_ here (yet):
 *   - work-stealing across threads (Phase C v1 is in mn_sched.c)
 *
 * Phase B: per-goroutine snapshot of CPython tstate.  Algorithm copied
 * from greenlet (MIT) -- src/greenlet/TPythonState.cpp.  Each goroutine
 * gets its own slice of cframe / current_frame / datastack_chunk / etc,
 * so frames from different gs do not link into one shared C-stack chain.
 * Lifts the ~200 concurrent yielded goroutine cliff.
 *
 * The Python side talks to us through a tiny Python type defined in
 * module.c (PygoG).  The user-visible API is `pygo.go / yield_ /
 * sleep / run`.
 */

#define _POSIX_C_SOURCE 200809L

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "pygo_sched.h"
#include "mn_sched.h"
#include "netpoll.h"

#include <math.h>
#include <stdio.h>
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

/* ---- tstate snapshot ----
 *
 * Save copies fields from tstate INTO snap and takes ownership of the
 * owned-pointer fields (context, top_frame, delete_later).  Load copies
 * fields from snap BACK INTO tstate and transfers ownership the other
 * way.  After a load, the snap is empty.  Save/load must be balanced.
 *
 * Greenlet uses operator<< / operator>> for this; we use snap/load.
 * The field set is the same as greenlet's PythonState + ExceptionState
 * combined.  Each PY_VERSION_HEX gate matches greenlet's GREENLET_PY*
 * branches.
 */
void pygo_pystate_snap(pygo_pystate_snap_t *snap)
{
    PyThreadState *ts = PyThreadState_GET();

    /* No memset: every field is assigned below.  Saves ~80B of
     * unnecessary writes on the hot per-yield path. */

#if PY_VERSION_HEX >= 0x030C0000
    /* contextvars: steal a strong ref.  Py_XINCREF is null-safe and
     * compiles to a predicted-not-taken branch over the atomic. */
    Py_XINCREF(ts->context);
    snap->context = ts->context;

    snap->py_recursion_remaining = ts->py_recursion_remaining;
    snap->c_recursion_remaining = ts->c_recursion_remaining;

    snap->datastack_chunk = ts->datastack_chunk;
    snap->datastack_top = ts->datastack_top;
    snap->datastack_limit = ts->datastack_limit;

    /* No top_frame snap: pygo does not expose a g.frame introspection
     * API, and the underlying _PyInterpreterFrame stays alive via the
     * datastack_chunk chain that we already restore.  Greenlet keeps
     * a strong PyFrameObject ref for `gr_frame`; we don't need it.
     * Skipping this avoids a PyFrameObject allocation per snap. */

    snap->exc_info = ts->exc_info;
    snap->exc_state = ts->exc_state;
#endif

#if PY_VERSION_HEX >= 0x030C0000 && PY_VERSION_HEX < 0x030D0000
    /* 3.12: cframe lives on the C stack, threaded through the linked
     * list.  We save the pointer; it remains valid because g's C stack
     * is preserved across the swap. */
    snap->cframe = ts->cframe;
    snap->trash_delete_nesting = ts->trash.delete_nesting;
#endif

#if PY_VERSION_HEX >= 0x030D0000
    /* 3.13: cframe is gone; current_frame is directly on tstate. */
    snap->current_frame = ts->current_frame;
    Py_XINCREF(ts->delete_later);
    snap->delete_later = ts->delete_later;
#endif

#if PY_VERSION_HEX < 0x030C0000
    snap->recursion_depth = ts->recursion_depth;
#endif

    snap->valid = 1;
}

void pygo_pystate_load(pygo_pystate_snap_t *snap)
{
    PyThreadState *ts;

    if (!snap->valid) {
        return;
    }
    ts = PyThreadState_GET();

#if PY_VERSION_HEX >= 0x030C0000
    {
        PyObject *old = ts->context;
        ts->context = snap->context;
        snap->context = NULL;
        Py_XDECREF(old);
        /* Bump the cache version: contextvars caches are keyed by
         * context_ver and must be invalidated on swap. */
        ts->context_ver++;
    }

    ts->py_recursion_remaining = snap->py_recursion_remaining;
    ts->c_recursion_remaining = snap->c_recursion_remaining;

    ts->datastack_chunk = snap->datastack_chunk;
    ts->datastack_top = snap->datastack_top;
    ts->datastack_limit = snap->datastack_limit;

    /* (No top_frame snap to drop -- see comment in pygo_pystate_snap.) */

    ts->exc_state = snap->exc_state;
    ts->exc_info = snap->exc_info ? snap->exc_info : &ts->exc_state;
    snap->exc_info = NULL;
    snap->exc_state.exc_value = NULL;
    snap->exc_state.previous_item = NULL;
#endif

#if PY_VERSION_HEX >= 0x030C0000 && PY_VERSION_HEX < 0x030D0000
    ts->cframe = snap->cframe;
    ts->trash.delete_nesting = snap->trash_delete_nesting;
#endif

#if PY_VERSION_HEX >= 0x030D0000
    ts->current_frame = snap->current_frame;
    {
        PyObject *old = ts->delete_later;
        ts->delete_later = snap->delete_later;
        snap->delete_later = NULL;
        Py_XDECREF(old);
    }
#endif

#if PY_VERSION_HEX < 0x030C0000
    ts->recursion_depth = snap->recursion_depth;
#endif

    snap->valid = 0;
}

/* Clear a snap that we own but won't load (e.g., g died while suspended).
 * Drops all owned refs. */
void pygo_pystate_snap_clear(pygo_pystate_snap_t *snap)
{
    if (!snap->valid) {
        return;
    }
#if PY_VERSION_HEX >= 0x030C0000
    Py_CLEAR(snap->context);
    Py_CLEAR(snap->exc_state.exc_value);
    snap->exc_info = NULL;
#endif
#if PY_VERSION_HEX >= 0x030D0000
    Py_CLEAR(snap->delete_later);
#endif
    snap->valid = 0;
}

/* Install an initial root for the goroutine's Python frame chain.  Run
 * inside pygo_g_entry, on the goroutine's own stack, BEFORE we call any
 * user Python code.
 *
 * The point: when user code calls PyEval_EvalFrameDefault, the new
 * interpreter frame's `previous` field is linked to whatever was at
 * tstate's "top of chain" pointer.  If we don't sever the chain, that
 * "top" is whoever ran most recently on this OS thread -- the
 * scheduler, or worse, another goroutine.  Then traceback walks and
 * recursion checks pull in every frame across every goroutine.
 *
 * On 3.12, we put a fresh _PyCFrame at the bottom of g's stack and
 * point its previous to tstate->root_cframe (the per-thread sentinel).
 * On 3.13, the cframe is gone; we just NULL out tstate->current_frame.
 * In both cases the chain starts here and walks back to a terminator,
 * not to the previous coroutine. */
#if PY_VERSION_HEX >= 0x030C0000 && PY_VERSION_HEX < 0x030D0000
static void pygo_install_initial_root_frame(_PyCFrame *frame_storage)
{
    PyThreadState *ts = PyThreadState_GET();
    *frame_storage = *ts->cframe;            /* inherit current_frame, etc. */
    frame_storage->previous = &ts->root_cframe;
    ts->cframe = frame_storage;
}
#endif

#if PY_VERSION_HEX >= 0x030D0000
static void pygo_install_initial_root_frame(void)
{
    PyThreadState *ts = PyThreadState_GET();
    ts->current_frame = NULL;
}
#endif

/* ---- Ready FIFO ops (singly-linked, head=pop, tail=push) ----
 * Exposed publicly so mn_sched.c can reuse the same list ops for its
 * per-hub local FIFO of yielded gs. */
void pygo_sched_ready_push(pygo_sched_t *s, pygo_g_t *g)
{
    g->next = NULL;
    if (s->ready_tail == NULL) {
        s->ready_head = s->ready_tail = g;
    } else {
        s->ready_tail->next = g;
        s->ready_tail = g;
    }
}

pygo_g_t *pygo_sched_ready_pop(pygo_sched_t *s)
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

/* Short aliases for use inside this file (keeps the existing code
 * readable; same functions). */
#define pygo_ready_push pygo_sched_ready_push
#define pygo_ready_pop  pygo_sched_ready_pop

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
    s->stack_size = 131072;   /* 128 KB per goroutine */
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

/* ---- Coro entry shim ----
 *
 * Runs ON THE GOROUTINE'S STACK.  Local variables here live for the
 * lifetime of g.  We exploit that by allocating the initial _PyCFrame
 * here (3.12 only) so its address remains valid for as long as we
 * might switch back to g. */
void pygo_g_entry(void *user)
{
    pygo_g_t *g = (pygo_g_t *)user;
    PyObject *res;

#if PY_VERSION_HEX >= 0x030C0000 && PY_VERSION_HEX < 0x030D0000
    _PyCFrame root_cframe_storage;
    pygo_install_initial_root_frame(&root_cframe_storage);
#elif PY_VERSION_HEX >= 0x030D0000
    pygo_install_initial_root_frame();
#endif

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
        pygo_pystate_snap_clear(&g->snap);
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
    pygo_g_t *g;
    /* M:N first.  If a hub claims this thread, it handles the requeue
     * + snap + asm-yield internally and we return through hub_main's
     * resume cycle. */
    if (pygo_mn_yield_current()) {
        return;
    }
    g = s->current;
    if (g == NULL) return;
    /* Fast path (Go's runtime.Gosched shortcut): if there's nobody
     * else to run -- no other ready gs, no sleepers due, no parked
     * I/O -- yielding is just expensive bookkeeping that hands
     * control right back to us.  Skip the whole snap + asm-yield +
     * resume cycle and return.  This cuts the single-coro tight-yield
     * baseline from ~230 ns to <10 ns. */
    if (s->ready_head == NULL
        && s->sleep_size == 0
        && pygo_netpoll_parked_count() == 0) {
        return;
    }
    pygo_ready_push(s, g);
    /* Save tstate INTO g's snap.  The scheduler's snap (in drain's
     * local frame) is untouched; drain will load it after the swap
     * returns. */
    pygo_pystate_snap(&g->snap);
    pygo_coro_yield();
    /* On resume, drain has loaded g's snap back into tstate; we resume
     * exactly where we left off. */
}

/* Park current g without re-queueing (caller takes ownership and
 * arranges to wake it later via pygo_sched_wake). */
void pygo_sched_park_current(void)
{
    pygo_sched_t *s = pygo_sched_get();
    pygo_g_t *g = s->current;
    if (g == NULL) return;
    pygo_pystate_snap(&g->snap);
    /* DO NOT push to ready; the parker (netpoll, channel, etc) owns
     * the g until it calls pygo_sched_wake. */
}

void pygo_sched_wake(pygo_g_t *g)
{
    if (g == NULL) return;
    pygo_ready_push(pygo_sched_get(), g);
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
    pygo_pystate_snap(&g->snap);
    pygo_coro_yield();
}

/* ---- Drain (main loop) ---- */
Py_ssize_t pygo_sched_drain(pygo_sched_t *s)
{
    Py_ssize_t completed_before = s->completed;
    s->stopping = 0;

    while (!s->stopping && (s->ready_head != NULL ||
                            s->sleep_size > 0 ||
                            pygo_netpoll_parked_count() > 0)) {
        double now = pygo_monotonic();
        /* Wake up any sleepers whose time has come. */
        while (s->sleep_size > 0 && pygo_sleep_peek(s)->wake_at <= now) {
            pygo_g_t *woke = pygo_sleep_pop(s);
            pygo_ready_push(s, woke);
        }
        /* Pump netpoll: if any goroutines are parked, wait for I/O up
         * to the next sleep deadline (or forever if none).  Drives
         * pygo_sched_wake which moves ready I/O goroutines back to
         * the ready queue. */
        if (s->ready_head == NULL &&
            (pygo_netpoll_parked_count() > 0 || s->sleep_size > 0)) {
            long long timeout_ns = -1;
            if (s->sleep_size > 0) {
                double gap = pygo_sleep_peek(s)->wake_at - now;
                if (gap < 0) gap = 0;
                if (gap > 60.0) gap = 60.0;
                timeout_ns = (long long)(gap * 1e9);
            }
            if (pygo_netpoll_parked_count() > 0) {
                pygo_netpoll_pump(timeout_ns);
            } else if (timeout_ns > 0) {
                /* No fds parked, just a sleep heap timer. */
                struct timespec req, rem;
                if (timeout_ns > 50000000LL) timeout_ns = 50000000LL;
                req.tv_sec = (time_t)(timeout_ns / 1000000000LL);
                req.tv_nsec = (long)(timeout_ns % 1000000000LL);
                nanosleep(&req, &rem);
            }
            continue;
        }
        /* Pop a ready g and resume it.
         *
         * Snap dance (Phase B):
         *   1. Save the SCHEDULER's tstate into a local snap on drain's
         *      own stack.  This captures the scheduler's frame chain,
         *      contextvars, recursion budget, etc.
         *   2. If g has a valid saved snap, load it into tstate -- this
         *      restores g's frame chain, contextvars, etc.  Otherwise
         *      g is on its first run; the initial root cframe is
         *      installed inside pygo_g_entry, on g's stack.
         *   3. Resume into g.  G runs Python code.  When it yields it
         *      calls pygo_sched_yield/park/sleep, all of which call
         *      pygo_pystate_snap to capture g's tstate into g->snap.
         *   4. After the swap returns, load the scheduler's snap back
         *      so the next loop iteration starts from a clean baseline.
         */
        {
            pygo_g_t *g = pygo_ready_pop(s);
            pygo_g_t *prev = s->current;
            pygo_pystate_snap_t sched_snap;

            memset(&sched_snap, 0, sizeof(sched_snap));
            pygo_pystate_snap(&sched_snap);

            s->current = g;
            if (g->snap.valid) {
                pygo_pystate_load(&g->snap);
            } else {
                /* First run for this g.  We must give it its own slice
                 * of the per-thread interpreter state, otherwise:
                 *   - g would allocate Python frame storage into the
                 *     scheduler's datastack_chunk, then g2 would do the
                 *     same starting from where the scheduler left off
                 *     (the snap restored that position), overwriting
                 *     g1's live frames -> segfault on g1 resume.
                 *   - g would inherit current_frame from the scheduler,
                 *     linking g's frame chain back into shared frames
                 *     across all goroutines (the original cliff).
                 *
                 * NULL the datastack pointers so PyEval starts a fresh
                 * root chunk owned by g.  For 3.13 also NULL
                 * current_frame so g's first frame chains to nothing.
                 * For 3.12, g_entry will install a root cframe on g's
                 * own stack before any Python code runs. */
                PyThreadState *ts = PyThreadState_GET();
                ts->datastack_chunk = NULL;
                ts->datastack_top = NULL;
                ts->datastack_limit = NULL;
#if PY_VERSION_HEX >= 0x030D0000
                ts->current_frame = NULL;
#endif
            }

            pygo_coro_resume(g->coro);

            /* Back on the scheduler's C stack.  tstate now reflects
             * whatever g left it as just before yielding.  Restore
             * the scheduler's snap so the next iteration is clean. */
            pygo_pystate_load(&sched_snap);
            s->current = prev;

            if (pygo_coro_done(g->coro)) {
                s->completed++;
                pygo_g_decref(g);   /* scheduler releases its ref */
            }
        }
    }
    return s->completed - completed_before;
}
