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
#include "io_uring.h"
#include "pygo_blockpool.h"
#include "pygo_diag.h"
#include "pygo_gstate.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if !defined(_WIN32)
#  include <sys/mman.h>          /* madvise / MADV_DONTNEED, mincore */
#  include <unistd.h>            /* sysconf(_SC_PAGESIZE) */
#endif

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
/* Per-goroutine PyThreadState mode (PYGO_PER_G_TSTATE).  When the M:N
 * scheduler runs in this mode, each goroutine owns its own PyThreadState,
 * so the per-g "snap" (which swaps execution-state fields in/out of a
 * shared per-hub tstate) is redundant and MUST NOT run -- snapping would
 * clear context/top_frame/exc out of the g's own live tstate.  Making
 * pygo_pystate_snap a no-op here (leaving snap->valid = 0) transparently
 * disables the snap at every park primitive that calls it; load and
 * snap_clear already no-op on !valid, so the whole snap machinery stands
 * down.  Set by pygo_mn_init when the flag is on, cleared at pygo_mn_fini.
 * Stays 0 for the single-thread scheduler path. */
static int pygo_per_g_tstate_mode = 0;
void pygo_set_per_g_tstate_mode(int on) { pygo_per_g_tstate_mode = on ? 1 : 0; }
int  pygo_get_per_g_tstate_mode(void)   { return pygo_per_g_tstate_mode; }

__attribute__((hot))
void pygo_pystate_snap(pygo_pystate_snap_t *snap)
{
    PyThreadState *ts;

    if (__builtin_expect(pygo_per_g_tstate_mode, 0)) {
        snap->valid = 0;   /* per-g tstate owns the state; nothing to swap out */
        return;
    }
    ts = PyThreadState_GET();

    /* DIAG: remember which tstate is bound right now.  This is the pointer
     * the suspended eval loop bakes into its C frame; load checks it for a
     * cross-hub mismatch.  Cheap unconditional store (one word). */
    snap->origin_tstate = ts;

    /* No memset: every field is assigned below.  Saves ~80B of
     * unnecessary writes on the hot per-yield path. */

#if PY_VERSION_HEX >= 0x030B0000
    /* contextvars: hold a ref while suspended so g's saved context
     * can't be freed.  Optimisation: skip the atomic INCREF when the
     * context is immortal -- the empty default context is immortal
     * on 3.12+, and that's what 99% of programs that never touch
     * contextvars see.  Free-threaded 3.13t pays an extra atomic op
     * per Py_INCREF on non-immortals; this shaves it off the most
     * common case. */
#  if defined(_Py_IsImmortal)
    if (ts->context != NULL && _Py_IsImmortal(ts->context)) {
        snap->context = ts->context;        /* no INCREF needed */
    } else {
        Py_XINCREF(ts->context);
        snap->context = ts->context;
    }
#  else
    Py_XINCREF(ts->context);
    snap->context = ts->context;
#  endif

#  if PY_VERSION_HEX >= 0x030C0000
    snap->py_recursion_remaining = ts->py_recursion_remaining;
    snap->c_recursion_remaining = ts->c_recursion_remaining;
#  else
    /* 3.11: single counter named recursion_remaining. */
    snap->recursion_remaining = ts->recursion_remaining;
#  endif

    snap->datastack_chunk = ts->datastack_chunk;
    snap->datastack_top = ts->datastack_top;
    snap->datastack_limit = ts->datastack_limit;

    /* No top_frame snap: pygo does not expose a g.frame introspection
     * API, and the underlying _PyInterpreterFrame stays alive via the
     * datastack_chunk chain that we already restore.  Greenlet keeps
     * a strong PyFrameObject ref for `gr_frame`; we don't need it.
     * Skipping this avoids a PyFrameObject allocation per snap. */

    /* Exception state: common case is "no exception in flight" --
     * ts->exc_info points to the sentinel and exc_value is NULL.
     * Encode that as snap->exc_info=NULL and skip the 24-byte copy. */
    if (__builtin_expect(ts->exc_info == &ts->exc_state &&
                         ts->exc_state.exc_value == NULL, 1)) {
        snap->exc_info = NULL;
        snap->exc_chain_bottom = NULL;
    } else {
        snap->exc_info = ts->exc_info;
        snap->exc_state = ts->exc_state;
        Py_XINCREF(snap->exc_state.exc_value);
        /* Record the bottom per-g exc item -- the one whose previous_item roots
         * at THIS tstate's &exc_state -- so a cross-hub load (steal-woken) can
         * re-root it onto the target hub's tstate.  Bounded walk (chain depth =
         * coroutine nesting).  NULL if the chain doesn't root here (defensive;
         * then load skips the re-root). */
        {
            _PyErr_StackItem *it = ts->exc_info;
            int guard = 0;
            snap->exc_chain_bottom = NULL;
            while (it != NULL && it != &ts->exc_state && guard++ < 256) {
                if (it->previous_item == &ts->exc_state) {
                    snap->exc_chain_bottom = it;
                    break;
                }
                it = it->previous_item;
            }
        }
    }
    /* current_exception is the in-flight raised-but-not-yet-caught
     * exception.  Save with our own ref so another goroutine can't
     * free it during our suspension. */
    snap->current_exception = ts->current_exception;
    Py_XINCREF(snap->current_exception);
#endif

#if PY_VERSION_HEX >= 0x030B0000 && PY_VERSION_HEX < 0x030D0000
    /* 3.11 and 3.12: cframe lives on the C stack, threaded through
     * the linked list.  We save the pointer; it remains valid
     * because g's C stack is preserved across the swap.  The
     * trashcan nesting depth lived directly on tstate in 3.11
     * (trash_delete_nesting) and inside a nested `trash` struct in
     * 3.12 (trash.delete_nesting). */
    snap->cframe = ts->cframe;
#  if PY_VERSION_HEX >= 0x030C0000
    snap->trash_delete_nesting = ts->trash.delete_nesting;
#  else
    snap->trash_delete_nesting = ts->trash_delete_nesting;
#  endif
#endif

#if PY_VERSION_HEX >= 0x030D0000
    /* 3.13: cframe is gone; current_frame is directly on tstate. */
    snap->current_frame = ts->current_frame;
    Py_XINCREF(ts->delete_later);
    snap->delete_later = ts->delete_later;
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

    /* DIAG (PYGO_DIAG_MIGRATE=1): detect the H>=2 cross-hub corruption at its
     * SOURCE.  When we load a snap onto a tstate that differs from the one the
     * snap was saved on (origin_tstate) AND the g has live Python frames
     * (current_frame != NULL on 3.13), the g's suspended eval loop will keep
     * threading origin_tstate while the bound tstate is `ts` -> divergent
     * exception/datastack reads -> "error return without exception set" / SEGV.
     * This fires immediately BEFORE the crash, naming both tstates. */
    {
        static int diag = -1;
        int d = __atomic_load_n(&diag, __ATOMIC_RELAXED);
        if (d < 0) {
            const char *e = getenv("PYGO_DIAG_MIGRATE");
            d = (e != NULL && e[0] != '0') ? 1 : 0;
            __atomic_store_n(&diag, d, __ATOMIC_RELAXED);
        }
        if (d && snap->origin_tstate != ts) {
            void *frame = NULL;
#if PY_VERSION_HEX >= 0x030D0000
            frame = (void *)snap->current_frame;
#endif
            fprintf(stderr,
                "[PYGO_DIAG_MIGRATE] cross-hub snap load: origin_ts=%p "
                "bound_ts=%p live_frame=%p exc_info=%p cur_exc=%p%s\n",
                (void *)snap->origin_tstate, (void *)ts, frame,
                (void *)snap->exc_info, (void *)snap->current_exception,
                frame != NULL ? "  <-- LIVE FRAME: eval loop will use stale "
                                "origin_ts (CORRUPTION)" : "  (no live frame)");
        }
    }

#if PY_VERSION_HEX >= 0x030B0000
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

#  if PY_VERSION_HEX >= 0x030C0000
    ts->py_recursion_remaining = snap->py_recursion_remaining;
    ts->c_recursion_remaining = snap->c_recursion_remaining;
#  else
    /* 3.11: single counter. */
    ts->recursion_remaining = snap->recursion_remaining;
#  endif

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
        /* Re-root the bottom per-g exc item onto THIS hub's &exc_state.  Same-hub
         * load: &ts->exc_state is the address snap recorded -- a no-op store.
         * Cross-hub (steal-woken): fixes the previous_item that still pointed at
         * the ORIGIN hub's tstate, which would otherwise dangle when the g's
         * generator/coroutine unwinds its exception context on the new hub. */
        if (snap->exc_chain_bottom != NULL) {
            snap->exc_chain_bottom->previous_item = &ts->exc_state;
            snap->exc_chain_bottom = NULL;
        }
        snap->exc_info = NULL;
        snap->exc_state.exc_value = NULL;       /* ownership transferred */
        snap->exc_state.previous_item = NULL;
    }
    /* Restore current_exception: drop whatever the previous g left
     * and install ours.  snap's ref transfers to ts. */
    {
        PyObject *old = ts->current_exception;
        ts->current_exception = snap->current_exception;
        snap->current_exception = NULL;
        Py_XDECREF(old);
    }
#endif

#if PY_VERSION_HEX >= 0x030B0000 && PY_VERSION_HEX < 0x030D0000
    ts->cframe = snap->cframe;
#  if PY_VERSION_HEX >= 0x030C0000
    ts->trash.delete_nesting = snap->trash_delete_nesting;
#  else
    ts->trash_delete_nesting = snap->trash_delete_nesting;
#  endif
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

    snap->valid = 0;
}

/* Clear a snap that we own but won't load (e.g., g died while suspended).
 * Drops all owned refs. */
void pygo_pystate_snap_clear(pygo_pystate_snap_t *snap)
{
    if (!snap->valid) {
        return;
    }
#if PY_VERSION_HEX >= 0x030B0000
    Py_CLEAR(snap->context);
    Py_CLEAR(snap->exc_state.exc_value);
    snap->exc_info = NULL;
    Py_CLEAR(snap->current_exception);
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
#if PY_VERSION_HEX >= 0x030B0000
/* 4096 chunks = ~16 MB / thread reserved when full, but matches
 * spawn-heavy peak working sets without falling back to the arena. */
#define PYGO_CHUNK_POOL_CAP 4096
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
    /* Clear `previous` so the popped chunk is standalone.  We use
     * `previous` as the pool's linked-list link AND CPython uses
     * `previous` to chain the data stack -- without this NULL, when
     * a frame pops to chunk-empty CPython's
     * _PyThreadState_PopFrame_ChunkEmpty would walk into the next
     * pool chunk via this pointer, arena-free THAT chunk, and corrupt
     * the pool.  Manifests as random use-after-free in exception
     * state once ~25+ goroutines are alive at once. */
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
#if PY_VERSION_HEX >= 0x030B0000
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

/* ---- Datastack-tail idle reclaim (companion to the C-stack sweep) ----
 *
 * A Python goroutine parked deep in CPython (e.g. in recv inside the eval
 * loop) holds a _PyStackChunk with its live frames in [chunk, datastack_top)
 * and unpushed free space in [datastack_top, datastack_limit).  The dwell
 * stack-sweep madvises the C stack below SP but NEVER this chunk, so the
 * chunk's free tail stays resident on every parked Python g.  This drops it.
 *
 * Only the CURRENT (top) chunk has a reclaimable tail: chunk->previous
 * chunks are full of live outer frames, so we touch only datastack_chunk.
 * The chunk is an arena allocation (mmap, page-aligned base, size a multiple
 * of the page), so datastack_limit is page-aligned and the geometry is clean.
 * We keep the partial page holding the live frontier and drop whole pages
 * strictly above it.  Refault on the next frame push is the (cheap, zero-
 * filled) cost, identical to the C-stack sweep's contract. */
#if PY_VERSION_HEX >= 0x030B0000 && !defined(_WIN32) && defined(MADV_DONTNEED)
/* Decompose counters (PYGO_DATASTACK_DEBUG only). */
static unsigned long long pygo_ds_sweep_tail_bytes = 0;
static unsigned long long pygo_ds_sweep_resident_bytes = 0;
static unsigned long long pygo_ds_sweep_chunks = 0;

/* mincore the [lo,hi) range and return resident bytes.  Best-effort: on any
 * error (e.g. mincore unsupported) returns 0 so accounting just under-counts
 * rather than misleads. */
static unsigned long long pygo_ds_resident_bytes(uintptr_t lo, uintptr_t hi,
                                                 size_t page)
{
    unsigned char vec[64];           /* covers up to 64 pages == 256 KiB tail */
    size_t npages = (size_t)((hi - lo) / page);
    unsigned long long resident = 0;
    size_t i;
    if (npages == 0 || npages > sizeof(vec)) return 0;
    if (mincore((void *)lo, (size_t)(hi - lo), vec) != 0) return 0;
    for (i = 0; i < npages; i++) {
        if (vec[i] & 1) resident += (unsigned long long)page;
    }
    return resident;
}
#endif

void pygo_sched_madvise_datastack_idle(pygo_g_t *g)
{
#if PY_VERSION_HEX >= 0x030B0000 && !defined(_WIN32) && defined(MADV_DONTNEED)
    static int on = -1;              /* read PYGO_DATASTACK_SWEEP once */
    static int dbg = -1;             /* read PYGO_DATASTACK_DEBUG once */
    pygo_pystate_snap_t *snap;
    uintptr_t top, limit, lo, hi;
    long ps;
    size_t page;

    {
        int v = __atomic_load_n(&on, __ATOMIC_RELAXED);
        if (v < 0) {
            /* Default-ON, mirroring the master PYGO_STACK_PARK_SWEEP flip:
             * unset -> on, "0" -> off.  The master sweep already gates
             * whether this runs at all (it's called only from the sweep's
             * batch loop) and supplies the churn throttle, so on a flat
             * handler -- where the tail is unfaulted and we reclaim nothing
             * -- this is just a cheap no-op madvise (measured: no throughput
             * cost); on the common deep-then-park shape it reclaims real
             * resident RAM.  Opt out with PYGO_DATASTACK_SWEEP=0. */
            const char *e = getenv("PYGO_DATASTACK_SWEEP");
            v = (e != NULL && e[0] == '0') ? 0 : 1;
            __atomic_store_n(&on, v, __ATOMIC_RELAXED);
        }
        if (!v) return;
    }

    if (g == NULL) return;
    snap = &g->snap;
    if (!snap->valid || snap->datastack_chunk == NULL ||
        snap->datastack_top == NULL || snap->datastack_limit == NULL) {
        return;                      /* C-only g, or no chunk installed */
    }

    ps = sysconf(_SC_PAGESIZE);
    page = (ps > 0) ? (size_t)ps : (size_t)4096;
    top   = (uintptr_t)snap->datastack_top;
    limit = (uintptr_t)snap->datastack_limit;
    if (limit <= top) return;
    lo = (top + page - 1) & ~(uintptr_t)(page - 1);   /* align UP past frontier */
    hi = limit & ~(uintptr_t)(page - 1);              /* align DOWN to chunk end */
    if (hi <= lo) return;            /* no whole free page to drop */

    {
        int d = __atomic_load_n(&dbg, __ATOMIC_RELAXED);
        if (d < 0) {
            const char *e = getenv("PYGO_DATASTACK_DEBUG");
            d = (e != NULL && e[0] != '0') ? 1 : 0;
            __atomic_store_n(&dbg, d, __ATOMIC_RELAXED);
        }
        if (d) {
            unsigned long long tail = (unsigned long long)(hi - lo);
            unsigned long long res = pygo_ds_resident_bytes(lo, hi, page);
            __atomic_add_fetch(&pygo_ds_sweep_tail_bytes, tail,
                               __ATOMIC_RELAXED);
            __atomic_add_fetch(&pygo_ds_sweep_resident_bytes, res,
                               __ATOMIC_RELAXED);
            __atomic_add_fetch(&pygo_ds_sweep_chunks, 1ULL, __ATOMIC_RELAXED);
        }
    }

    (void)madvise((void *)lo, (size_t)(hi - lo), MADV_DONTNEED);
#else
    (void)g;
#endif
}

void pygo_sched_datastack_sweep_stats(unsigned long long *tail_bytes,
                                      unsigned long long *resident_bytes,
                                      unsigned long long *chunks)
{
#if PY_VERSION_HEX >= 0x030B0000 && !defined(_WIN32) && defined(MADV_DONTNEED)
    if (tail_bytes)
        *tail_bytes = __atomic_load_n(&pygo_ds_sweep_tail_bytes, __ATOMIC_RELAXED);
    if (resident_bytes)
        *resident_bytes = __atomic_load_n(&pygo_ds_sweep_resident_bytes,
                                          __ATOMIC_RELAXED);
    if (chunks)
        *chunks = __atomic_load_n(&pygo_ds_sweep_chunks, __ATOMIC_RELAXED);
#else
    if (tail_bytes) *tail_bytes = 0;
    if (resident_bytes) *resident_bytes = 0;
    if (chunks) *chunks = 0;
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
#if PY_VERSION_HEX >= 0x030B0000
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
#if PY_VERSION_HEX >= 0x030B0000 && PY_VERSION_HEX < 0x030D0000
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

/* ---- Ready FIFO ops (ring buffer of g pointers) ----
 *
 * head/tail are MONOTONIC counters, masked into the ring on access.
 * As long as we don't wrap a size_t (which would take >5000 years at
 * 100M push/s on 64-bit), the (tail - head) arithmetic gives the
 * exact count.  Wraparound-safe for 32-bit too because we mask
 * differences, not absolute values.
 *
 * Grows on overflow by doubling; the ring is per-thread so no lock.
 */
static int pygo_ready_grow(pygo_sched_t *s)
{
    size_t old_cap = s->ready_cap;
    size_t new_cap = old_cap ? old_cap * 2 : 64;
    pygo_g_t **new_ring = (pygo_g_t **)PyMem_Calloc(new_cap, sizeof(pygo_g_t *));
    size_t i, head = s->ready_head, tail = s->ready_tail;
    if (new_ring == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    /* Copy from the old ring, preserving order.  Old has tail-head
     * entries starting at (head & old_mask). */
    for (i = 0; i < (tail - head); i++) {
        new_ring[i] = s->ready_ring[(head + i) & s->ready_mask];
    }
    PyMem_Free(s->ready_ring);
    s->ready_ring = new_ring;
    s->ready_cap = new_cap;
    s->ready_mask = new_cap - 1;
    s->ready_head = 0;
    s->ready_tail = tail - head;
    return 0;
}

void pygo_sched_ready_push(pygo_sched_t *s, pygo_g_t *g)
{
    if (__builtin_expect(s->ready_tail - s->ready_head >= s->ready_cap, 0)) {
        if (pygo_ready_grow(s) < 0) return;     /* OOM: drop g on the floor;
                                                 * caller checks PyErr */
    }
    s->ready_ring[s->ready_tail & s->ready_mask] = g;
    s->ready_tail++;
}

pygo_g_t *pygo_sched_ready_pop(pygo_sched_t *s)
{
    pygo_g_t *g;
    if (s->ready_head == s->ready_tail) return NULL;
    g = s->ready_ring[s->ready_head & s->ready_mask];
    s->ready_head++;
    /* Note: we deliberately do NOT clear g->next here -- the linked-
     * list `next` field is now repurposed by the g_slab as the free-
     * list link and by the M:N submission list.  Whichever consumer
     * uses it next overwrites the slot. */
    return g;
}

#define pygo_ready_push pygo_sched_ready_push
#define pygo_ready_pop  pygo_sched_ready_pop

/* ---- Sleep heap (min-heap by (wake_at, sleep_seq)) ----
 * The sleep_seq tiebreak makes equal-deadline sleepers wake in FIFO insertion
 * order, matching asyncio's (when, seq) TimerHandle ordering.  Without it,
 * two coros sleeping the same duration but started a tick apart could wake in
 * the wrong relative order (observed breaking equal-timeout races, e.g.
 * hypercorn's lifespan startup-timeout test). */
static inline int pygo_sleep_before(const pygo_g_t *a, const pygo_g_t *b)
{
    if (a->wake_at < b->wake_at) return 1;
    if (a->wake_at > b->wake_at) return 0;
    return a->sleep_seq < b->sleep_seq;
}

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
    /* Assign the FIFO tiebreak sequence at push time. */
    g->sleep_seq = s->sleep_seq_ctr++;
    s->sleep_heap[i] = g;
    /* sift up: move g toward the root while it is earlier than its parent */
    while (i > 1 && pygo_sleep_before(g, s->sleep_heap[i / 2])) {
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
            pygo_sleep_before(s->sleep_heap[child + 1], s->sleep_heap[child])) {
            child++;
        }
        /* stop once `last` is no later than the smaller child */
        if (!pygo_sleep_before(s->sleep_heap[child], last)) break;
        s->sleep_heap[i] = s->sleep_heap[child];
        i = child;
    }
    s->sleep_heap[i] = last;
    return top;
}

#define pygo_sleep_peek pygo_sched_sleep_peek
#define pygo_sleep_pop  pygo_sched_sleep_pop

/* ---- Scheduler lifecycle ---- */
/* Phase C: per-thread schedulers (see pygo_sched_get below).  pygo_cal_default
 * holds the calibrated default stack size that each new thread's sched is
 * initialised with. */

/* ---- Stack calibration ----
 *
 * Default: 32 KB.  Empirically the test suite passes at 16 KB on
 * CPython 3.13t and 8 KB on 3.12; the deepest path is
 * socket.getaddrinfo, which lazy-loads the encodings codec on first
 * call.  32 KB doubles the worst measured floor and keeps 1M gs at
 * 32 GB VM (256 KB would be 256 GB).  Demand-paging means the unused
 * tail consumes no physical RAM until touched.  Calibration adapts
 * UP for stack-hungry programs: after PYGO_CAL_TARGET completions we
 * freeze to next_pow2(max_hwm * PYGO_CAL_SAFETY).  Programs with deep
 * frames in the first 1000 gs (before freeze) should call
 * pygo_sched_set_default_stack_size() -- stacks have no guard page.
 *
 * Reads/writes are plain (single-threaded scheduler in v0); when
 * Phase C arrives these will move under the scheduler lock. */
#define PYGO_DEFAULT_STACK_SIZE   (32  * 1024)
#define PYGO_MIN_STACK_SIZE       (16  * 1024)   /* 3.13t hard floor */
#define PYGO_MAX_STACK_SIZE       (8   * 1024 * 1024)
#define PYGO_CAL_TARGET           1000     /* gs before freeze */
#define PYGO_CAL_SAFETY           4        /* multiply HWM by this */

static size_t    pygo_cal_default = PYGO_DEFAULT_STACK_SIZE;
static size_t    pygo_cal_max_hwm = 0;
static long long pygo_cal_completed = 0;
static int       pygo_cal_frozen = 0;

static size_t pygo_next_pow2(size_t v)
{
    size_t p = 1;
    while (p < v && p < PYGO_MAX_STACK_SIZE) p <<= 1;
    return p;
}

/* Called from drain after a g completes.  Scans the g's stack for
 * HWM, updates the running max, and freezes the calibration window
 * if we've collected enough samples. */
static void pygo_cal_record(pygo_g_t *g)
{
    size_t hwm;
    if (pygo_cal_frozen || !pygo_coro_paint_enabled()) return;
    if (g == NULL || g->coro == NULL) return;
    hwm = pygo_coro_scan_hwm(g->coro);
    if (hwm > pygo_cal_max_hwm) pygo_cal_max_hwm = hwm;
    pygo_cal_completed++;
    if (pygo_cal_completed >= PYGO_CAL_TARGET) {
        size_t bound = pygo_cal_max_hwm * PYGO_CAL_SAFETY;
        size_t chosen = pygo_next_pow2(bound);
        if (chosen < PYGO_MIN_STACK_SIZE) chosen = PYGO_MIN_STACK_SIZE;
        if (chosen > PYGO_MAX_STACK_SIZE) chosen = PYGO_MAX_STACK_SIZE;
        pygo_cal_default = chosen;
        pygo_cal_frozen = 1;
        pygo_coro_paint_set(0);
        /* cal_record runs inside drain on this thread's own sched; bump it so
         * the freeze takes effect immediately here.  Other threads' scheds pick
         * up pygo_cal_default when they spawn their next g. */
        pygo_sched_get()->stack_size = (Py_ssize_t)chosen;
    }
}

void pygo_sched_set_default_stack_size(size_t bytes)
{
    if (bytes < PYGO_MIN_STACK_SIZE) bytes = PYGO_MIN_STACK_SIZE;
    if (bytes > PYGO_MAX_STACK_SIZE) bytes = PYGO_MAX_STACK_SIZE;
    pygo_cal_default = bytes;
    pygo_cal_frozen = 1;
    pygo_coro_paint_set(0);
    /* Update the calling thread's sched immediately; other threads pick up
     * pygo_cal_default on their next spawn. */
    pygo_sched_get()->stack_size = (Py_ssize_t)bytes;
}

size_t pygo_sched_get_default_stack_size(void)
{
    return pygo_cal_default;
}

void pygo_sched_stack_stats(pygo_stack_stats_t *out)
{
    if (out == NULL) return;
    out->default_size = pygo_cal_default;
    out->max_hwm      = pygo_cal_max_hwm;
    out->completed    = pygo_cal_completed;
    out->calibrated   = pygo_cal_frozen;
    out->painting     = pygo_coro_paint_enabled();
}

void pygo_sched_init(pygo_sched_t *s)
{
    /* Initial ring of 64 entries; grows on demand.  Empty when
     * ready_head == ready_tail == 0. */
    s->ready_ring = (pygo_g_t **)PyMem_Calloc(64, sizeof(pygo_g_t *));
    s->ready_cap = 64;
    s->ready_mask = 63;
    s->ready_head = 0;
    s->ready_tail = 0;
    s->current = NULL;
    s->sleep_heap = NULL;
    s->sleep_size = 0;
    s->sleep_cap = 0;
    s->stack_size = (Py_ssize_t)pygo_cal_default;
    s->completed = 0;
    s->stopping = 0;
    s->netpoll_parked = 0;
    pygo_mutex_init(&s->wake_list_lock);
    s->wake_list_head = NULL;
    s->wake_list_tail = NULL;
}

/* Pop the cross-thread wake list and push each entry onto the
 * scheduler's lock-free ready ring.  Called by the sched owner thread
 * once per drain iteration.  Holding the lock only for the swap (not
 * the per-g ready_push) keeps wake_safe latency bounded even when the
 * owner is doing a large fan-in. */
static void pygo_sched_drain_wake_list(pygo_sched_t *s)
{
    pygo_g_t *head;
    pygo_mutex_lock(&s->wake_list_lock);
    head = s->wake_list_head;
    s->wake_list_head = NULL;
    s->wake_list_tail = NULL;
    pygo_mutex_unlock(&s->wake_list_lock);
    while (head != NULL) {
        pygo_g_t *next = head->wake_next;
        head->wake_next = NULL;
        pygo_ready_push(s, head);
        head = next;
    }
}

/* Phase C: ONE scheduler per OS thread.  pygo.aio runs each event loop on the
 * thread that drives it (pygo_core.run -> pygo_sched_drain on this thread's
 * sched), so two loops on two threads are fully independent -- one thread
 * blocking synchronously inside a coroutine (concurrent.futures.Future.result,
 * thread.join, queue.get -- anyio BlockingPortal, run_coroutine_threadsafe,
 * threaded server controllers) only freezes ITS OWN sched, never the other's.
 *
 * The sched is thread-local and lazily created on first use.  No cross-thread
 * init race (each thread builds its own), so the old 0->1->2 election is gone.
 * M:N hubs keep their own per-hub scheds via pygo_mn_current_sched and never
 * funnel through here for their run loop.
 *
 * Lifetime: the per-thread sched is intentionally leaked at thread exit (no
 * portable pre-C11 TLS destructor across GCC/MSVC; a sched is small and the
 * thread is gone).  A thread churning many short loops (e.g. aiosmtpd's
 * per-test Controller thread) leaks one sched per thread, not per loop. */
static PYGO_TLS pygo_sched_t *pygo_tls_sched = NULL;
/* Count of per-thread scheds ever created.  When a SECOND one appears,
 * cross-thread wakes become possible (a g owned by one thread's sched woken by
 * another -- a foreign-thread future resolution, or the shared netpoll pump on
 * a different loop's thread delivering an fd event for this thread's parker).
 * That is when we arm the pump-interrupt eventfd (Phase 2); a single-loop app
 * stays exactly as before, with no eventfd in the shared epoll. */
static volatile int pygo_sched_count = 0;

pygo_sched_t *pygo_sched_get(void)
{
    pygo_sched_t *s = pygo_tls_sched;
    if (__builtin_expect(s != NULL, 1)) return s;
    s = (pygo_sched_t *)PyMem_RawMalloc(sizeof(*s));
    if (s == NULL) {
        Py_FatalError("pygo: per-thread scheduler allocation failed");
    }
    pygo_sched_init(s);                 /* sets stack_size = pygo_cal_default */
    pygo_tls_sched = s;
    /* First call on THIS thread -- never under pool->lock (it runs before this
     * thread touches netpoll), so arming (which takes pool->lock via
     * pygo_netpoll_init) cannot self-deadlock against the wake path.  Arm only
     * once a second thread's sched exists; idempotent thereafter. */
    if (__atomic_add_fetch(&pygo_sched_count, 1, __ATOMIC_ACQ_REL) >= 2) {
        pygo_netpoll_wake_pump_arm();
    }
    return s;
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

    /* C-only entry: skip all Python-frame setup and just call the
     * registered C function.  Used by the pure-C bench harness; no
     * Python state to manage. */
    if (g->c_entry != NULL) {
        g->c_entry(g->c_arg);
        __atomic_store_n(&g->done, 1, __ATOMIC_RELEASE);
        pygo_g_state_set(g, PYGO_GST_DONE);
        PYGO_EVT(PYGO_EVT_G_COMPLETE, g, NULL, 0);
        return;
    }

#if PY_VERSION_HEX >= 0x030B0000 && PY_VERSION_HEX < 0x030D0000
    _PyCFrame root_cframe_storage;
    pygo_install_initial_root_frame(&root_cframe_storage);
#elif PY_VERSION_HEX >= 0x030D0000
    pygo_install_initial_root_frame();
#endif

    /* Reset recursion counters at g entry -- each g has its own
     * physical C stack, so the per-tstate counters don't reflect
     * actual stack usage when shared across multiple gs. */
    {
        PyThreadState *ts = PyThreadState_GET();
        int py_limit = Py_GetRecursionLimit();
#if PY_VERSION_HEX >= 0x030C0000
        ts->py_recursion_remaining = py_limit;
        /* 200 frames matches what a 128KB stack can safely hold. */
        ts->c_recursion_remaining  = 200;
#else
        ts->recursion_remaining = py_limit;
#endif
    }

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
    /* The goroutine has run to completion; g->callable is never invoked
     * again.  Release it NOW rather than waiting for the g's last decref --
     * otherwise a pygo.aio task, whose callable is the task's own bound
     * _driver method, is kept alive forever by an unbreakable cycle:
     *
     *     task -> task._g (PygoG) -> pygo_g_t -> g->callable (_driver) -> task
     *
     * pygo_g_t is a C struct, invisible to cyclic GC, and the PygoG wrapper
     * has no tp_traverse, so the collector cannot see the g->callable edge and
     * never reclaims the completed task (its 'exception never retrieved'
     * warning never fires either).  Clearing callable here cuts that edge at
     * the source, so the remaining task graph collects by plain refcounting.
     *
     * Running the resulting finalizer here is safe: we are on g's own stack in
     * g's tstate (any finalizer frames land on g's datastack chunk, drained
     * immediately after this returns), and the scheduler still holds its
     * ref(s) to g, so g itself cannot be freed under us.  pygo_g_decref's later
     * Py_XDECREF(g->callable) becomes a NULL no-op.  Done last, so g->done
     * publishes a fully torn-down g. */
    Py_CLEAR(g->callable);
    /* RELEASE store on g->done publishes the prior g->result/g->error
     * writes; PygoG_done_get / PygoG_result_get load g->done with
     * ACQUIRE and only read result/error if done. */
    __atomic_store_n(&g->done, 1, __ATOMIC_RELEASE);
    pygo_g_state_set(g, PYGO_GST_DONE);
    PYGO_EVT(PYGO_EVT_G_COMPLETE, g, NULL, 0);
    /* Falls back through asm trampoline -> infinite swap to caller. */
}

/* ---- pygo_g_t slab allocator ----
 *
 * spawn/decref-last-ref on the hot path is dominated by
 * PyMem_Calloc(144) / PyMem_Free.  At 100k spawns/sec that's 200k
 * calls/sec through CPython's small-block allocator, which is
 * thread-contended on free-threaded 3.13t.
 *
 * Per-thread LIFO free list of recycled pygo_g_t.  Push on last
 * decref; pop on next spawn.  Reuses the `next` field for the
 * free-list link (it's NULL'd by ready_pop after each scheduler
 * cycle, so it's free real estate while g is unallocated).  Cap of
 * 256 entries / thread bounds memory footprint after a burst. */
/* Cap chosen so a steady-state spawn-heavy workload (100k+ gs in
 * flight) doesn't continuously overflow back to PyMem_Free, while
 * still bounding memory to ~150 KB / thread when not in use. */
#define PYGO_G_SLAB_CAP 4096
static PYGO_TLS pygo_g_t *pygo_g_slab = NULL;
static PYGO_TLS int pygo_g_slab_size = 0;

pygo_g_t *pygo_g_slab_alloc(void)
{
    pygo_g_t *g = pygo_g_slab;
    if (g != NULL) {
        pygo_g_slab = g->next;
        pygo_g_slab_size--;
        /* memset the fields that callers expect zeroed.  Slightly
         * smaller than the original PyMem_Calloc which zeroes the
         * whole struct -- but spawn always overwrites callable,
         * coro, refcount; we only need to clear the rest. */
        memset(g, 0, sizeof(*g));
        return g;
    }
    return (pygo_g_t *)PyMem_Calloc(1, sizeof(*g));
}

void pygo_g_slab_free(pygo_g_t *g)
{
    pygo_g_state_set(g, PYGO_GST_FREED);
    if (pygo_g_slab_size >= PYGO_G_SLAB_CAP) {
        PyMem_Free(g);
        return;
    }
    g->next = pygo_g_slab;
    pygo_g_slab = g;
    pygo_g_slab_size++;
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
    PYGO_EVT(PYGO_EVT_G_DECREF, g, NULL, (long long)g->refcount);
    /* ACQ_REL: pairs with other threads' decrefs so all prior writes
     * (including g->result / g->error / g->done done-flag updates)
     * are observable on the last reference's owner before free. */
    new_count = __atomic_sub_fetch(&g->refcount, 1, __ATOMIC_ACQ_REL);
    if (new_count <= 0) {
        pygo_pystate_snap_clear(&g->snap);
        if (g->tstate != NULL) {
            /* per-g tstate (PYGO_PER_G_TSTATE): g is done + detached, so its
             * tstate is current on no thread.  Clear+Delete frees its
             * datastack/frames/exc.  Mirrors mn_fini's hub-tstate teardown,
             * which also clears non-current tstates with the caller's tstate
             * held -- pygo_g_decref's contexts (hub completion, drain, PygoG
             * dealloc) all run with some other tstate current. */
            PyThreadState_Clear(g->tstate);
            PyThreadState_Delete(g->tstate);
            g->tstate = NULL;
        }
        if (g->coro != NULL) {
            pygo_coro_destroy(g->coro);
            g->coro = NULL;
        }
        Py_XDECREF(g->callable);
        Py_XDECREF(g->result);
        Py_XDECREF(g->error);
        pygo_g_slab_free(g);
    }
}

/* ---- Spawn ---- */
static PyObject *spawn_common(pygo_sched_t *s, PyObject *callable,
                              int noyield, size_t stack_size)
{
    pygo_g_t *g = pygo_g_slab_alloc();
    if (g == NULL) {
        PyErr_NoMemory();
        return NULL;
    }
    Py_INCREF(callable);
    g->callable = callable;
    g->refcount = 1;   /* one ref for the scheduler queue */
    g->owner = s;      /* per-thread sched that owns this g (Phase C) */
    g->noyield = noyield;
    g->coro = pygo_coro_new(stack_size, pygo_g_entry, g);
    if (g->coro == NULL) {
        Py_DECREF(g->callable);
        pygo_g_slab_free(g);
        PyErr_SetString(PyExc_MemoryError, "pygo_coro_new failed");
        return NULL;
    }
    pygo_g_state_set(g, PYGO_GST_RUNNABLE);
    pygo_ready_push(s, g);
    return PyCapsule_New(g, "pygo_g", NULL);
}

PyObject *pygo_sched_spawn(pygo_sched_t *s, PyObject *callable)
{
    return spawn_common(s, callable, /*noyield*/0, (size_t)s->stack_size);
}

PyObject *pygo_sched_spawn_noyield(pygo_sched_t *s, PyObject *callable)
{
    return spawn_common(s, callable, /*noyield*/1, (size_t)s->stack_size);
}

PyObject *pygo_sched_spawn_sized(pygo_sched_t *s, PyObject *callable,
                                 size_t stack_size)
{
    if (stack_size < PYGO_MIN_STACK_SIZE) stack_size = PYGO_MIN_STACK_SIZE;
    if (stack_size > PYGO_MAX_STACK_SIZE) stack_size = PYGO_MAX_STACK_SIZE;
    return spawn_common(s, callable, /*noyield*/0, stack_size);
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
    /* Fold any pending wakes into the ready ring BEFORE the fast-path
     * check and BEFORE we re-queue ourselves.  G.wake() (used by
     * pygo.aio for Task.cancel() / future.set_result()/set_exception())
     * routes through wake_safe, which appends to the cross-thread
     * wake_list rather than the ready ring.  The wake_list is normally
     * drained only inside the drain loop, so a wake() issued by THIS
     * goroutine immediately before `await asyncio.sleep(0)` was invisible
     * to this yield: the woken g sat in the wake_list (so the ready-empty
     * fast path returned without ever entering the drain loop), and even
     * when the drain loop ran it, we had already pushed ourselves first
     * and resumed before it -- so the woken task only ran on the SECOND
     * sleep(0).  Stock asyncio's sleep(0) is one loop iteration that runs
     * the callbacks scheduled before it; draining here restores that.
     * Draining before our own ready_push means the woken g sits ahead of
     * us in FIFO order and runs first.  The cheap NULL hint keeps the
     * tight-yield fast path intact (empty in the common case; a same-
     * thread setter's store is already visible to us here, and a missed
     * cross-thread store is harmless -- the drain loop still catches it). */
    if (__builtin_expect(s->wake_list_head != NULL, 0)) {
        pygo_sched_drain_wake_list(s);
    }
    /* Fast path (Go's runtime.Gosched shortcut): if there's nobody
     * else to run -- no other ready gs, no sleepers due, no parked
     * I/O -- yielding is just expensive bookkeeping that hands
     * control right back to us.  Skip the whole snap + asm-yield +
     * resume cycle and return.  This cuts the single-coro tight-yield
     * baseline from ~230 ns to <10 ns. */
    if (__builtin_expect(pygo_sched_ready_empty(s)
                         && s->sleep_size == 0
                         && pygo_netpoll_parked_count() == 0
                         && pygo_blockpool_inflight() == 0, 1)) {
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
    pygo_sched_t *self, *owner;
    if (g == NULL) return;
    pygo_g_state_set(g, PYGO_GST_RUNNABLE);
    self  = pygo_sched_get();
    owner = g->owner ? g->owner : self;
    if (owner == self) {
        /* Same thread (the common single-loop / same-loop case): push onto our
         * own cooperative ready ring. */
        pygo_ready_push(self, g);
        return;
    }
    /* Phase 2 -- cross-thread wake.  g's owner sched runs on ANOTHER OS thread
     * (e.g. the shared netpoll pump that delivered this fd event is draining on
     * a different loop's thread than the one that parked the g).  Pushing onto
     * OUR ready ring would resume the g on the wrong thread (and our ready ring
     * is single-consumer, not cross-thread-safe).  Instead enqueue onto the
     * OWNER's thread-safe wake_list (its drain consumes it), then kick its pump
     * so an idle epoll_wait wakes to drain the list.  Same mechanism as
     * pygo_sched_wake_safe; the kick eventfd is level-triggered + non-exclusive
     * so every blocked pumper wakes and drains its own wake_list. */
    pygo_mutex_lock(&owner->wake_list_lock);
    g->wake_next = NULL;
    if (owner->wake_list_tail != NULL) {
        owner->wake_list_tail->wake_next = g;
    } else {
        owner->wake_list_head = g;
    }
    owner->wake_list_tail = g;
    pygo_mutex_unlock(&owner->wake_list_lock);
    pygo_netpoll_wake_pump();
}

/* Race-safe park/wake.  Used by pygo.aio.PygoTask to replace its
 * per-task Chan(1) wake mechanism.  Saves the Chan alloc + try_send/
 * recv path -- about 5 us per parked goroutine at fan-out time.
 *
 * Cross-thread correctness: wake_safe may be called from a thread
 * other than the sched owner (e.g., an iouring CQE callback running
 * on a hub thread invokes a done-callback that resolves a future a
 * main-thread goroutine is awaiting via park_safe).  The handoff is
 * coordinated by an atomic parked_safe flag on g:
 *
 *   park_safe (parker):                wake_safe (waker):
 *     wake_pending == 0? early-out      atomic_add wake_pending (was 0)
 *     parked_safe = 1 (release)         CAS parked_safe 1->0 (acquire)
 *     wake_pending == 0?                 on success: enqueue to wake_list
 *       no -> CAS parked_safe 1->0       on failure: nothing (parker
 *             on success, abort yield                    will see the
 *             on failure, yield (waker                   wake_pending
 *             already queued g)                          and abort)
 *
 * The previous implementation used `s->current != g` as the "is g
 * parked?" predicate, but s->current is updated by the sched owner's
 * drain and reading it from a foreign thread races: the cross-thread
 * waker could see s->current==g (drain hadn't restored prev yet) and
 * skip the push, losing the wake.  The parked_safe CAS handoff is
 * deterministic regardless of caller thread. */
void pygo_sched_wake_safe(pygo_g_t *g)
{
    if (g == NULL) return;

    /* Bump wake_pending FIRST so the parker's recheck after its
     * parked_safe store observes our arrival.  ACQ_REL pairs with
     * park_safe's load-acquire on wake_pending. */
    __atomic_add_fetch(&g->wake_pending, 1, __ATOMIC_ACQ_REL);

    /* Try to transition parked_safe 1->0.  On success, we own the
     * wake and route g back to its home sched via the thread-safe
     * wake_list (pygo_global_sched here -- park_safe is single-thread
     * sched only; M:N hubs use pygo_mn_wake_g for an analogous race
     * pattern). */
    {
        int expected = 1;
        if (__atomic_compare_exchange_n(&g->parked_safe, &expected, 0,
                                        0, __ATOMIC_ACQ_REL,
                                        __ATOMIC_ACQUIRE)) {
            /* Route to the g's OWNER sched (the thread that spawned it),
             * not the waker's thread -- the waker may be a foreign thread
             * (run_in_executor pool worker, iouring CQE) whose own sched is
             * never drained.  g->owner's drain owner consumes the wake_list. */
            pygo_sched_t *s = g->owner ? g->owner : pygo_sched_get();
            pygo_mutex_lock(&s->wake_list_lock);
            g->wake_next = NULL;
            if (s->wake_list_tail != NULL) {
                s->wake_list_tail->wake_next = g;
            } else {
                s->wake_list_head = g;
            }
            s->wake_list_tail = g;
            pygo_mutex_unlock(&s->wake_list_lock);
        }
        /* CAS failed: g was either running (parked_safe==0 already)
         * or another wake_safe already claimed it.  Our wake_pending
         * bump is still observable; the parker (or the prior claimer's
         * subsequent park_safe) will consume it. */
    }
}

void pygo_sched_park_safe(void)
{
    pygo_sched_t *s = pygo_sched_get();
    pygo_g_t *g = s->current;
    if (g == NULL) return;

    /* Was wake_safe already called?  If so, eat one count and return
     * without yielding -- the future fired synchronously. */
    if (__atomic_load_n(&g->wake_pending, __ATOMIC_ACQUIRE) > 0) {
        __atomic_sub_fetch(&g->wake_pending, 1, __ATOMIC_ACQ_REL);
        return;
    }

    /* Commit to parking.  Release order so wake_safe's acquire CAS
     * on parked_safe sees a fully consistent g (in particular, any
     * wake_next reset). */
    g->wake_next = NULL;
    __atomic_store_n(&g->parked_safe, 1, __ATOMIC_RELEASE);

    /* Recheck wake_pending after the store.  Two outcomes pair with
     * wake_safe's "bump pending, CAS parked_safe":
     *   - wake_safe's bump happened before our store: we observe
     *     wake_pending>0 here; its CAS failed (parked_safe was 0 then);
     *     we CAS parked_safe back to 0 and return without yielding.
     *   - wake_safe's bump happened after our store: its CAS sees
     *     parked_safe==1 and succeeds, queueing g on wake_list; we
     *     either observe wake_pending>0 and CAS-race lose (drain
     *     will pick g up via wake_list) or observe wake_pending==0
     *     (the bump hadn't landed) and proceed to yield (drain still
     *     picks g up via wake_list -- the queueing happens-before the
     *     wake_pending bump is irrelevant). */
    if (__atomic_load_n(&g->wake_pending, __ATOMIC_ACQUIRE) > 0) {
        int expected = 1;
        if (__atomic_compare_exchange_n(&g->parked_safe, &expected, 0,
                                        0, __ATOMIC_ACQ_REL,
                                        __ATOMIC_ACQUIRE)) {
            __atomic_sub_fetch(&g->wake_pending, 1, __ATOMIC_ACQ_REL);
            return;
        }
        /* Lost the CAS -- wake_safe already claimed us and pushed g
         * onto the wake_list.  Fall through to yield; drain will
         * dequeue g on its next iteration. */
    }

    pygo_pystate_snap(&g->snap);
    pygo_g_state_set(g, PYGO_GST_PARKED_SAFE);
    pygo_coro_yield();
    /* On resume, parked_safe has already been cleared by wake_safe's
     * CAS.  Eat one wake_pending count -- there is at least one (the
     * wake that delivered us back to ready). */
    __atomic_sub_fetch(&g->wake_pending, 1, __ATOMIC_ACQ_REL);
    pygo_g_state_set(g, PYGO_GST_RUNNING);
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

    while (!s->stopping && (!pygo_sched_ready_empty(s) ||
                            s->sleep_size > 0 ||
                            __atomic_load_n(&s->netpoll_parked, __ATOMIC_ACQUIRE) > 0 ||
                            pygo_iouring_inflight() > 0 ||
                            pygo_blockpool_inflight() > 0 ||
                            __atomic_load_n(&s->wake_list_head,
                                            __ATOMIC_ACQUIRE) != NULL)) {
        double now = pygo_sched_monotonic_seconds();
        /* Drain cross-thread wakes into the ready ring before any
         * other work this iteration.  Empty in the common (same-
         * thread wake_safe) case -- one atomic load on the head.
         * Otherwise: a single lock acquire on wake_list_lock, then
         * lock-free per-g pushes to ready. */
        if (__atomic_load_n(&s->wake_list_head, __ATOMIC_ACQUIRE) != NULL) {
            pygo_sched_drain_wake_list(s);
        }
        /* Wake up any sleepers whose time has come. */
        while (s->sleep_size > 0 && pygo_sleep_peek(s)->wake_at <= now) {
            pygo_g_t *woke = pygo_sleep_pop(s);
            pygo_ready_push(s, woke);
        }
        /* Pump netpoll: if any goroutines are parked, wait for I/O up
         * to the next sleep deadline (or forever if none).  Drives
         * pygo_sched_wake which moves ready I/O goroutines back to
         * the ready queue. */
        if (pygo_sched_ready_empty(s) &&
            (__atomic_load_n(&s->netpoll_parked, __ATOMIC_ACQUIRE) > 0 || s->sleep_size > 0 ||
             pygo_iouring_inflight() > 0 || pygo_blockpool_inflight() > 0)) {
            long long timeout_ns = -1;
            if (s->sleep_size > 0) {
                double gap = pygo_sleep_peek(s)->wake_at - now;
                if (gap < 0) gap = 0;
                if (gap > 60.0) gap = 60.0;
                timeout_ns = (long long)(gap * 1e9);
            }
            /* iouring goroutines wake when the pump observes a CQE on
             * the registered eventfd; blocking-pool goroutines wake when
             * a worker pokes the pump-interrupt eventfd -- both ride the
             * netpoll pump, so the pump call covers netpoll parkers,
             * iouring waiters AND blocking-pool waiters in one syscall. */
            if (__atomic_load_n(&s->netpoll_parked, __ATOMIC_ACQUIRE) > 0 ||
                pygo_iouring_inflight() > 0 ||
                pygo_blockpool_inflight() > 0) {
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

            /* noyield short-circuit -- the caller has asserted g
             * runs to completion without yielding.  We can share the
             * scheduler's datastack chunk and current_frame across
             * the resume: g's Python frames push onto the same chunk,
             * run, pop, and tstate ends up exactly where it was.  No
             * snap, no install, no drain, no load+resnap on
             * completion. */
            if (g->noyield) {
                pygo_coro_resume(g->coro);
                s->current = prev;
                if (pygo_coro_done(g->coro)) {
                    s->completed++;
                    pygo_cal_record(g);
                    pygo_g_decref(g);
                }
                /* If a noyield g ACTUALLY yielded (caller broke its
                 * promise) we'd land here with !done.  Carry on; the
                 * snap dance never happened, so tstate is now in g's
                 * uncommitted state and the next iter will see garbage.
                 * Undefined behaviour per the noyield contract. */
                continue;
            }

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
#if PY_VERSION_HEX >= 0x030B0000
                /* Also reset exception state for first-run g.  Without
                 * this, the new g inherits whatever ts->exc_info /
                 * exc_state / current_exception was left by the
                 * scheduler's last load, which under heavy concurrent
                 * cascades may be a pointer into a freed chunk or a
                 * stale exception object.  Each fresh g starts with
                 * default-state exception. */
                {
                    PyThreadState *ts = PyThreadState_GET();
                    Py_CLEAR(ts->exc_state.exc_value);
                    ts->exc_state.previous_item = NULL;
                    ts->exc_info = &ts->exc_state;
#  if PY_VERSION_HEX >= 0x030C0000
                    Py_CLEAR(ts->current_exception);
#  endif
                }
#endif
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
                pygo_cal_record(g);
                pygo_g_decref(g);
                pygo_pystate_snap(&sched_snap);
            } else if (pygo_g_state_in(g, PYGO_GST_MASK_PARKED)) {
                /* Parked on a waiter (netpoll/chan/sleep/park_safe):
                 * drop the g's now-idle stack pages until it's woken.
                 * No-op unless PYGO_STACK_PARK_DONTNEED=1.  Cooperative
                 * sched_yield gs are RUNNABLE (already re-queued), not
                 * PARKED, so they skip this and avoid a re-fault. */
                pygo_coro_park(g->coro);
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
