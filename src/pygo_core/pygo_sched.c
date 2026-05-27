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

#if !defined(_WIN32)
#  define _POSIX_C_SOURCE 200809L
#endif

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "plat.h"
#include "plat_compat.h"
#include "pygo_sched.h"
#include "mn_sched.h"
#include "netpoll.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ---- monotonic seconds ----
 * Shim-backed: plat_compat's pygo_monotonic_ns() picks
 * QueryPerformanceCounter on Windows and clock_gettime(CLOCK_MONOTONIC)
 * on POSIX (macOS/Linux/BSD).  Both have sub-microsecond resolution. */
double pygo_sched_monotonic_seconds(void)
{
    return pygo_monotonic_seconds_compat();
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
__attribute__((hot))
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

    /* Exception state: the common case is "no exception in flight"
     * which means ts->exc_info points to ts->exc_state (the per-tstate
     * sentinel) and ts->exc_state.exc_value is NULL.  Signal that with
     * snap->exc_info=NULL and skip the 24-byte exc_state copy. */
    if (__builtin_expect(ts->exc_info == &ts->exc_state &&
                         ts->exc_state.exc_value == NULL, 1)) {
        snap->exc_info = NULL;
    } else {
        snap->exc_info = ts->exc_info;
        snap->exc_state = ts->exc_state;
        /* The struct copy borrowed a reference; take a real one so the
         * exc_value can't be freed while g is suspended.  Matched by
         * Py_XDECREF in pygo_pystate_load's old-value cleanup, and by
         * Py_CLEAR in pygo_pystate_snap_clear. */
        Py_XINCREF(snap->exc_state.exc_value);
    }
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

__attribute__((hot))
void pygo_pystate_load(pygo_pystate_snap_t *snap)
{
    PyThreadState *ts;

    if (__builtin_expect(!snap->valid, 0)) {
        return;
    }
    ts = PyThreadState_GET();

#if PY_VERSION_HEX >= 0x030C0000
    /* contextvars fast path: if g didn't touch ts->context, the snap's
     * pointer matches ts's.  Drop our extra ref and skip the swap +
     * context_ver bump.  The version stays stable because the content
     * didn't change -- caches keyed by ver remain valid. */
    if (__builtin_expect(ts->context == snap->context, 1)) {
        Py_XDECREF(snap->context);
        snap->context = NULL;
    } else {
        PyObject *old = ts->context;
        ts->context = snap->context;
        snap->context = NULL;
        Py_XDECREF(old);
        ts->context_ver++;
    }

    ts->py_recursion_remaining = snap->py_recursion_remaining;
    ts->c_recursion_remaining = snap->c_recursion_remaining;

    /* Datastack fast path: in tight loops that yield without pushing
     * Python frames, the chunk pointers are unchanged across the
     * yield.  Skip 3 stores when chunk matches -- top and limit are
     * derived from chunk so they implicitly match too. */
    if (__builtin_expect(ts->datastack_chunk != snap->datastack_chunk, 0)) {
        ts->datastack_chunk = snap->datastack_chunk;
        ts->datastack_top = snap->datastack_top;
        ts->datastack_limit = snap->datastack_limit;
    } else if (ts->datastack_top != snap->datastack_top) {
        /* Same chunk but top moved (e.g., frames pushed within chunk). */
        ts->datastack_top = snap->datastack_top;
    }

    /* (No top_frame snap to drop -- see comment in pygo_pystate_snap.) */

    /* Exception state restore.  snap->exc_info==NULL is the
     * default-state sentinel (see snap path); reset ts to default only
     * if it has drifted.  Otherwise copy the saved chain back. */
    if (__builtin_expect(snap->exc_info == NULL, 1)) {
        if (ts->exc_info != &ts->exc_state || ts->exc_state.exc_value != NULL) {
            Py_CLEAR(ts->exc_state.exc_value);
            ts->exc_state.previous_item = NULL;
            ts->exc_info = &ts->exc_state;
        }
    } else {
        /* Drop ts's old exc_value before overwriting it with snap's
         * (which we own a strong ref to from the matching snap call). */
        Py_XDECREF(ts->exc_state.exc_value);
        ts->exc_state = snap->exc_state;
        ts->exc_info = snap->exc_info;
        snap->exc_info = NULL;
        snap->exc_state.exc_value = NULL;       /* ownership transferred */
        snap->exc_state.previous_item = NULL;
    }
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

/* Per-OS-thread datastack chunk pool.
 *
 * Each goroutine starts with tstate->datastack_chunk = NULL so PyEval
 * allocates a fresh chunk for it from the arena.  When the goroutine
 * completes we used to arena-free that chunk; profiling on spawn-heavy
 * workloads showed ~200 ns alloc + ~200 ns free per g lifetime spent
 * round-tripping through the arena.
 *
 * The pool keeps recently-freed chunks on a per-thread (TLS) LIFO so
 * the next g picks one up instead.  Cache-warm.  Capped to avoid
 * hoarding memory after a burst of short-lived gs.
 *
 * Chunks are linked through their existing `previous` pointer, so the
 * pool is zero-allocation -- we reuse the field for the free-list
 * link.  Pool entries have top=0 (empty) which is the same state a
 * freshly arena-allocated chunk would be in.
 *
 * 3.13t has tstate->datastack_cached_chunk, an in-tstate one-slot
 * cache.  That helps when one thread runs many gs serially, but
 * doesn't compose with our M:N hubs (each hub already has its own
 * tstate, so the slot would be cold across hub-to-hub g migration --
 * which we don't actually do, but the pool is also useful for the
 * single-thread sched on both 3.12 and 3.13). */
#if PY_VERSION_HEX >= 0x030C0000
#define PYGO_CHUNK_POOL_CAP 32
/* PYGO_TLS expands to __thread on GCC/Clang, __declspec(thread) on MSVC. */
static PYGO_TLS _PyStackChunk *pygo_chunk_pool = NULL;
static PYGO_TLS int pygo_chunk_pool_size = 0;

/* Pop one chunk off the pool.  Returns NULL when empty; caller falls
 * back to letting PyEval arena-allocate. */
static _PyStackChunk *pygo_chunk_pool_get(void)
{
    _PyStackChunk *c = pygo_chunk_pool;
    if (c == NULL) return NULL;
    pygo_chunk_pool = c->previous;
    pygo_chunk_pool_size--;
    c->previous = NULL;
    c->top = 0;
    return c;
}

/* Push one chunk onto the pool.  At cap, arena-free instead. */
static void pygo_chunk_pool_put(_PyStackChunk *c, PyObjectArenaAllocator *alloc)
{
    if (pygo_chunk_pool_size >= PYGO_CHUNK_POOL_CAP) {
        if (alloc->free != NULL) {
            alloc->free(alloc->ctx, c, c->size);
        }
        return;
    }
    c->previous = pygo_chunk_pool;
    c->top = 0;
    pygo_chunk_pool = c;
    pygo_chunk_pool_size++;
}

/* Install a pooled chunk on tstate for a fresh g.  Returns 1 if the
 * pool had a chunk and tstate is now wired to it; 0 if pool empty and
 * caller should NULL the datastack so PyEval allocates fresh.
 *
 * datastack_top starts at &c->data[1] (NOT data[0]) -- this mirrors
 * CPython's push_chunk root-chunk handling.  The data[0] slot is
 * intentionally wasted so _PyThreadState_PopFrame's check
 * `base == &chunk->data[0]` is never true for a frame in this chunk,
 * keeping pop from arena-freeing a chunk we own. */
static int pygo_chunk_pool_install(PyThreadState *ts)
{
    _PyStackChunk *c = pygo_chunk_pool_get();
    if (c == NULL) return 0;
    ts->datastack_chunk = c;
    ts->datastack_top = &c->data[1];
    ts->datastack_limit = (PyObject **)((char *)c + c->size);
    return 1;
}
#endif

void pygo_first_run_install_datastack(void)
{
#if PY_VERSION_HEX >= 0x030C0000
    PyThreadState *ts = PyThreadState_GET();
    if (!pygo_chunk_pool_install(ts)) {
        ts->datastack_chunk = NULL;
        ts->datastack_top = NULL;
        ts->datastack_limit = NULL;
    }
#else
    (void)0;
#endif
}

/* Drain the datastack-chunk chain currently attached to tstate,
 * returning tstate's datastack pointers to NULL.
 *
 * Called after a goroutine completes, BEFORE we restore the scheduler
 * or hub snapshot (which would overwrite tstate->datastack_chunk with
 * the scheduler's saved value and leak g's chain).
 *
 * Reused chunks go to the per-thread pool (up to PYGO_CHUNK_POOL_CAP).
 * Overflow goes back to the arena allocator that CPython's frame
 * allocator pulls from.
 *
 * Algorithm matches greenlet's PythonState::did_finish, plus pool reuse. */
void pygo_drain_g_datastack(void)
{
#if PY_VERSION_HEX >= 0x030C0000
    PyThreadState *ts = PyThreadState_GET();
    _PyStackChunk *chunk = ts->datastack_chunk;
    PyObjectArenaAllocator alloc;

    if (chunk == NULL) return;

    ts->datastack_chunk = NULL;
    ts->datastack_top = NULL;
    ts->datastack_limit = NULL;

    PyObject_GetArenaAllocator(&alloc);
    while (chunk != NULL) {
        _PyStackChunk *prev = chunk->previous;
        pygo_chunk_pool_put(chunk, &alloc);
        chunk = prev;
    }
#endif
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

pygo_g_t *pygo_sched_sleep_peek(pygo_sched_t *s)
{
    if (s->sleep_size == 0) return NULL;
    return s->sleep_heap[1];
}

pygo_g_t *pygo_sched_sleep_pop(pygo_sched_t *s)
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

#define pygo_sleep_peek pygo_sched_sleep_peek
#define pygo_sleep_pop  pygo_sched_sleep_pop

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
    /* RELEASE store on g->done publishes the prior g->result/g->error
     * writes; PygoG_done_get / PygoG_result_get load g->done with
     * ACQUIRE and only read result/error if done. */
    __atomic_store_n(&g->done, 1, __ATOMIC_RELEASE);
    /* Falls back through asm trampoline -> infinite swap to caller. */
}

/* ---- Refcount ---- */
void pygo_g_incref(pygo_g_t *g)
{
    if (g) __atomic_add_fetch(&g->refcount, 1, __ATOMIC_RELAXED);
}

void pygo_g_decref(pygo_g_t *g)
{
    int new_count;
    if (g == NULL) return;
    /* ACQ_REL: pairs with other threads' decrefs so all prior writes
     * (including g->result / g->error / g->done done-flag updates)
     * are observable on the last reference's owner before free. */
    new_count = __atomic_sub_fetch(&g->refcount, 1, __ATOMIC_ACQ_REL);
    if (new_count <= 0) {
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
__attribute__((hot))
void pygo_sched_yield(pygo_sched_t *s)
{
    pygo_g_t *g;
    /* M:N first.  If a hub claims this thread, it handles the requeue
     * + snap + asm-yield internally and we return through hub_main's
     * resume cycle. */
    if (__builtin_expect(pygo_mn_yield_current(), 0)) {
        return;
    }
    g = s->current;
    if (__builtin_expect(g == NULL, 0)) return;
    /* Fast path (Go's runtime.Gosched shortcut): if there's nobody
     * else to run -- no other ready gs, no sleepers due, no parked
     * I/O -- yielding is just expensive bookkeeping that hands
     * control right back to us.  Skip the whole snap + asm-yield +
     * resume cycle and return.  This cuts the single-coro tight-yield
     * baseline from ~230 ns to <10 ns. */
    if (__builtin_expect(s->ready_head == NULL
                         && s->sleep_size == 0
                         && pygo_netpoll_parked_count() == 0, 1)) {
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
 * arranges to wake it later via pygo_sched_wake / pygo_mn_wake_g).
 * Hub-aware: in an M:N hub the current g is in TLS, not in the
 * global sched->current slot. */
void pygo_sched_park_current(void)
{
    pygo_g_t *g;
    if (pygo_mn_current_hub_opaque() != NULL) {
        g = pygo_mn_tls_current_g();
        /* Tell hub_main that this g has been taken off-queue by an
         * external parker; don't re-push it on the local FIFO when
         * pygo_coro_yield returns control to hub_main. */
        pygo_mn_tls_mark_parked();
    } else {
        pygo_sched_t *s = pygo_sched_get();
        g = s->current;
    }
    if (g == NULL) return;
    pygo_pystate_snap(&g->snap);
    /* DO NOT push to ready; the parker (netpoll, channel, etc) owns
     * the g until it calls pygo_sched_wake / pygo_mn_wake_g. */
}

void pygo_sched_wake(pygo_g_t *g)
{
    if (g == NULL) return;
    pygo_ready_push(pygo_sched_get(), g);
}

/* ---- Sleep ----
 *
 * Hub-aware: in an M:N hub the sleep heap belongs to the hub's
 * pygo_sched_t (h->sched), not the global single-thread sched.  We
 * also mark self_queued so hub_main doesn't re-push the g onto the
 * local FIFO on return from pygo_coro_yield (same rationale as
 * pygo_sched_park_current). */
void pygo_sched_sleep_until(pygo_sched_t *s, double wake_at)
{
    pygo_g_t *g;
    pygo_sched_t *target = pygo_mn_current_sched();
    if (target != NULL) {
        g = pygo_mn_tls_current_g();
        if (g == NULL) return;
        g->wake_at = wake_at;
        if (pygo_sleep_push(target, g) < 0) return;
        pygo_pystate_snap(&g->snap);
        pygo_mn_tls_mark_parked();
        pygo_coro_yield();
        return;
    }
    /* Single-thread path */
    g = s->current;
    if (g == NULL) return;
    g->wake_at = wake_at;
    if (pygo_sleep_push(s, g) < 0) {
        return; /* leave g in current; caller will see exception */
    }
    pygo_pystate_snap(&g->snap);
    pygo_coro_yield();
}

/* ---- Drain (main loop) ----
 *
 * sched_snap optimisation: the scheduler's tstate (the Python frame
 * chain anchored at pygo_core.run()'s caller, plus context / exc state
 * / recursion budgets at drain entry) is INVARIANT for the duration of
 * drain.  Drain itself does no Python work between iterations -- the
 * only places where it would are pygo_g_decref (may run tp_dealloc) and
 * the loop's final return to Python.
 *
 * So instead of snap+load per iteration (which was costing ~10 ns of
 * write traffic on the slow path), we snap once at entry and load only
 * where Python may run -- before pygo_g_decref and at drain exit.  Re-
 * snap after decref so the next completion-path load is still valid. */
Py_ssize_t pygo_sched_drain(pygo_sched_t *s)
{
    Py_ssize_t completed_before = s->completed;
    pygo_pystate_snap_t sched_snap;

    s->stopping = 0;
    pygo_pystate_snap(&sched_snap);

    while (!s->stopping && (s->ready_head != NULL ||
                            s->sleep_size > 0 ||
                            pygo_netpoll_parked_count() > 0)) {
        double now = pygo_sched_monotonic_seconds();
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
                /* No fds parked, just a sleep heap timer.  Cap at 50 ms
                 * so an external caller (signal handler, debugger) can
                 * unstick us quickly. */
                if (timeout_ns > 50000000LL) timeout_ns = 50000000LL;
                pygo_sleep_ns(timeout_ns);
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

            /* sched_snap is loop-invariant (taken at drain entry).  We
             * deliberately do NOT snap or load it per-iter: drain runs
             * no Python between iterations, so tstate can stay in g's
             * state across the brief window between coro_resume return
             * and the next iter's g->snap load. */

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
                 * Install a chunk from the per-thread pool (or NULL the
                 * datastack pointers so PyEval allocates fresh).  For
                 * 3.13 also NULL current_frame so g's first frame
                 * chains to nothing.  For 3.12, g_entry will install a
                 * root cframe on g's own stack before any Python code
                 * runs. */
                pygo_first_run_install_datastack();
#if PY_VERSION_HEX >= 0x030D0000
                {
                    PyThreadState *ts = PyThreadState_GET();
                    ts->current_frame = NULL;
                }
#endif
            }

            pygo_coro_resume(g->coro);

            s->current = prev;

            if (pygo_coro_done(g->coro)) {
                /* g done: pygo_g_decref below may run tp_dealloc, which
                 * needs drain's tstate (frame chain + datastack chunk)
                 * to be installed before allocating frames -- otherwise
                 * tp_dealloc allocs a chunk on the wrong root and we
                 * leak it on the next iter's g->snap load.  So free g's
                 * chunks, restore drain's tstate, decref, then re-snap
                 * so a subsequent completion (or drain exit) has a
                 * valid sched_snap to load. */
                pygo_drain_g_datastack();
                pygo_pystate_load(&sched_snap);
                s->completed++;
                pygo_g_decref(g);
                pygo_pystate_snap(&sched_snap);
            }
            /* Yielded gs: tstate stays in g's state.  Next iter's
             * g_next->snap load (or first-run install) overwrites. */
        }
    }
    /* Restore drain's tstate before returning to Python. */
    pygo_pystate_load(&sched_snap);
    return s->completed - completed_before;
}

/* ---- Time-sliced preemption (3.13t only) ----
 *
 * A separate pthread sleeps for quantum_us microseconds, then schedules
 * a pending call via Py_AddPendingCall.  CPython's eval loop checks
 * its pending queue at bytecode back-edges and call instructions; when
 * the call fires, pygo_preempt_yield_cb runs on whatever tstate the
 * eval loop is in, sees a current goroutine (via the M:N TLS or the
 * single-thread sched), and yields cooperatively through the existing
 * snap + asm-yield path.
 *
 * Net effect: a goroutine that never calls sched_yield() still gets
 * preempted every quantum_us, so it can't starve other gs.  This is
 * Go's runtime preemption model (since 1.14) ported to CPython.  Zero
 * hot-path overhead -- the eval_breaker bit is already checked by
 * CPython on every back-edge.
 *
 * For M:N hubs we post `pygo_hub_count` pending calls per quantum so
 * each hub eventually picks one up; whichever hub's eval loop dequeues
 * a call runs the yield on its currently-running g. */

static pygo_thread_t pygo_preempt_thread;
static volatile int pygo_preempt_running = 0;
static volatile long pygo_preempt_quantum_us = 10000;

extern int pygo_mn_hub_count(void);   /* defined in mn_sched.c */

static int pygo_preempt_yield_cb(void *user)
{
    (void)user;
    /* If we're in a hub, yield via M:N path.  Otherwise check the
     * single-thread global scheduler.  Either way, this is a no-op
     * when no goroutine is currently running on this tstate. */
    if (pygo_mn_yield_current()) {
        return 0;
    }
    {
        pygo_sched_t *s = pygo_sched_get();
        if (s->current != NULL) {
            pygo_sched_yield(s);
        }
    }
    return 0;
}

static PYGO_THREAD_RET pygo_preempt_main(void *arg)
{
    (void)arg;
    while (pygo_preempt_running) {
        long us = pygo_preempt_quantum_us;
        int posts, i;

        if (us < 100) us = 100;        /* clamp lower bound */
        pygo_sleep_ns((long long)us * 1000LL);
        if (!pygo_preempt_running) break;

        /* Post one pending call per hub (or just one for single-thread)
         * so each hub's eval loop has something to pick up.  The
         * pending queue is shared per-interp; whichever hub drains
         * fastest gets the next one. */
        posts = pygo_mn_hub_count();
        if (posts < 1) posts = 1;
        for (i = 0; i < posts; i++) {
            /* Py_AddPendingCall is documented to be callable from any
             * thread without holding the GIL. */
            Py_AddPendingCall(pygo_preempt_yield_cb, NULL);
        }
    }
    PYGO_THREAD_RETURN(NULL);
}

int pygo_preempt_init(long quantum_us)
{
    if (quantum_us <= 0) {
        PyErr_SetString(PyExc_ValueError, "quantum_us must be > 0");
        return -1;
    }
    pygo_preempt_quantum_us = quantum_us;
    if (pygo_preempt_running) {
        /* Already running -- the timer loop reloads quantum on its
         * next iteration. */
        return 0;
    }
    pygo_preempt_running = 1;
    if (pygo_thread_create(&pygo_preempt_thread,
                           pygo_preempt_main, NULL) != 0) {
        pygo_preempt_running = 0;
        PyErr_SetString(PyExc_OSError, "pygo preempt thread create failed");
        return -1;
    }
    return 0;
}

void pygo_preempt_fini(void)
{
    if (!pygo_preempt_running) return;
    pygo_preempt_running = 0;
    /* Release the GIL so the timer thread's final pending-call post
     * (if any) doesn't deadlock with our join.  The timer's sleep
     * doesn't need the GIL but Py_AddPendingCall briefly touches
     * shared state. */
    {
        PyThreadState *saved = PyEval_SaveThread();
        pygo_thread_join(pygo_preempt_thread);
        PyEval_RestoreThread(saved);
    }
}
