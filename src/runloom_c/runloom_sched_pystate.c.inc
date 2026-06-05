/* Part of runloom_sched.c -- #included into that translation unit, not compiled
 * standalone.  See runloom_sched.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

double runloom_sched_monotonic_seconds(void)
{
    return runloom_monotonic_seconds_compat();
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
/* Per-goroutine PyThreadState mode (RUNLOOM_PER_G_TSTATE).  When the M:N
 * scheduler runs in this mode, each goroutine owns its own PyThreadState,
 * so the per-g "snap" (which swaps execution-state fields in/out of a
 * shared per-hub tstate) is redundant and MUST NOT run -- snapping would
 * clear context/top_frame/exc out of the g's own live tstate.  Making
 * runloom_pystate_snap a no-op here (leaving snap->valid = 0) transparently
 * disables the snap at every park primitive that calls it; load and
 * snap_clear already no-op on !valid, so the whole snap machinery stands
 * down.  Set by runloom_mn_init when the flag is on, cleared at runloom_mn_fini.
 * Stays 0 for the single-thread scheduler path. */
static int runloom_per_g_tstate_mode = 0;
void runloom_set_per_g_tstate_mode(int on) { runloom_per_g_tstate_mode = on ? 1 : 0; }
int  runloom_get_per_g_tstate_mode(void)   { return runloom_per_g_tstate_mode; }

__attribute__((hot))
void runloom_pystate_snap(runloom_pystate_snap_t *snap)
{
    PyThreadState *ts;

    if (__builtin_expect(runloom_per_g_tstate_mode, 0)) {
        snap->valid = 0;   /* per-g tstate owns the state; nothing to swap out */
        return;
    }
    ts = PyThreadState_GET();

    /* DIAG: remember which tstate is bound right now.  This is the pointer
     * the suspended eval loop bakes into its C frame; load checks it for a
     * cross-hub mismatch.  Cheap unconditional store (one word). */
    snap->origin_tstate = ts;

    /* Suspend any CPython critical section this g holds (free-threaded 3.13t):
     * release the held per-object mutexes and save the chain so load can
     * re-lock them on resume.  Common case (no CS held at a park point) is one
     * predicted-not-taken branch inside the helper.  Without this, a g that
     * parks mid-critical-section strands that mutex locked across the swap and
     * every other hub deadlocks on it (the routes-dict hang). */
    snap->critical_section = runloom_critsec_suspend(ts);

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

    /* Per-goroutine sys.setprofile/settrace hooks (BUG #11): tstate-global, so
     * without snap/restore one goroutine's hook leaks onto siblings sharing the
     * hub and is cleared from under it on resume.  Hold a ref to the hook
     * objects while suspended; all NULL (free) in the common no-hook case. */
    snap->c_profilefunc = ts->c_profilefunc;
    snap->c_tracefunc = ts->c_tracefunc;
    snap->c_profileobj = ts->c_profileobj;
    snap->c_traceobj = ts->c_traceobj;
    snap->tracing = ts->tracing;
    Py_XINCREF(snap->c_profileobj);
    Py_XINCREF(snap->c_traceobj);

    snap->datastack_chunk = ts->datastack_chunk;
    snap->datastack_top = ts->datastack_top;
    snap->datastack_limit = ts->datastack_limit;

    /* No top_frame snap: runloom does not expose a g.frame introspection
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
    /* delete_later is the trashcan's deferred-dealloc chain: objects that are
     * DYING (refcount 0) and linked through op->ob_tid (repurposed as the
     * next-pointer, see CPython _PyTrash_begin).  It MUST be carried
     * per-goroutine as a RAW pointer and NEVER Py_INCREF'd: on free-threaded
     * builds Py_INCREF reads op->ob_tid to pick the local-vs-shared refcount
     * path, but here ob_tid is a heap next-pointer, so an INCREF mutates the
     * dying object's shared refcount and scrambles the chain -> use-after-free
     * when the trashcan later destroys it.  (Py_XINCREF(NULL) is a no-op, which
     * is why this stayed latent: delete_later is almost always NULL at a yield;
     * only DEEP recursion -- BUG #6 -- leaves it non-NULL across a yield.)
     * Transfer ownership of the chain to the snap and clear ts so the chain is
     * private to this goroutine and the shared hub tstate never carries another
     * goroutine's half-built destroy list. */
    snap->delete_later = ts->delete_later;
    ts->delete_later = NULL;
#endif

    snap->valid = 1;
}

__attribute__((hot))
void runloom_pystate_load(runloom_pystate_snap_t *snap)
{
    PyThreadState *ts;

    if (__builtin_expect(!snap->valid, 0)) {
        return;
    }
    ts = PyThreadState_GET();

    /* Re-acquire any critical section this g held when it parked (see snap).
     * Must run on the tstate this g is being resumed onto -- ts -- which is
     * exactly the chain the held PyCriticalSection structs (on g's preserved
     * coro stack) point through.  No-op when none were held. */
    runloom_critsec_restore(ts, snap->critical_section);
    snap->critical_section = 0;

    /* DIAG (RUNLOOM_DIAG_MIGRATE=1): detect the H>=2 cross-hub corruption at its
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
            const char *e = getenv("RUNLOOM_DIAG_MIGRATE");
            d = (e != NULL && e[0] != '0') ? 1 : 0;
            __atomic_store_n(&diag, d, __ATOMIC_RELAXED);
        }
        if (d && snap->origin_tstate != ts) {
            void *frame = NULL;
#if PY_VERSION_HEX >= 0x030D0000
            frame = (void *)snap->current_frame;
#endif
            fprintf(stderr,
                "[RUNLOOM_DIAG_MIGRATE] cross-hub snap load: origin_ts=%p "
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

    /* Restore this goroutine's own profile/trace hooks (BUG #11).  Transfer the
     * refs the snap holds back onto ts and drop ts's previous ones.  All NULL
     * in the common no-hook case, so the DECREFs are no-ops. */
    ts->c_profilefunc = snap->c_profilefunc;
    ts->c_tracefunc = snap->c_tracefunc;
    ts->tracing = snap->tracing;
    {
        PyObject *oldp = ts->c_profileobj;
        PyObject *oldt = ts->c_traceobj;
        ts->c_profileobj = snap->c_profileobj;
        ts->c_traceobj = snap->c_traceobj;
        snap->c_profileobj = NULL;
        snap->c_traceobj = NULL;
        Py_XDECREF(oldp);
        Py_XDECREF(oldt);
    }

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

    /* (No top_frame snap to drop -- see comment in runloom_pystate_snap.) */

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
    /* Restore this goroutine's deferred-dealloc chain as a RAW pointer (see the
     * snap side -- never refcount a delete_later chain).  ts->delete_later is
     * NULL here: the goroutine that ran before us cleared it on its own snap,
     * and hub_main runs no Python (so creates no deferred deallocs) between
     * swaps -- so there is nothing to drop. */
    ts->delete_later = snap->delete_later;
    snap->delete_later = NULL;
#endif

    snap->valid = 0;
}

/* Clear a snap that we own but won't load (e.g., g died while suspended).
 * Drops all owned refs. */
void runloom_pystate_snap_clear(runloom_pystate_snap_t *snap)
{
    if (!snap->valid) {
        return;
    }
    /* A g destroyed while parked: its held CS mutexes were already released on
     * snap (suspend), and the PyCriticalSection structs live on the g's own
     * coro stack (freed with it), so there is nothing to unlock -- just drop
     * the saved chain. */
    snap->critical_section = 0;
#if PY_VERSION_HEX >= 0x030B0000
    Py_CLEAR(snap->context);
    Py_CLEAR(snap->exc_state.exc_value);
    snap->exc_info = NULL;
    Py_CLEAR(snap->current_exception);
    Py_CLEAR(snap->c_profileobj);            /* BUG #11: drop held hook refs */
    Py_CLEAR(snap->c_traceobj);
#endif
#if PY_VERSION_HEX >= 0x030D0000
    /* delete_later is a raw chain of DYING objects linked via ob_tid, never
     * refcounted (see runloom_pystate_snap).  A goroutine destroyed while parked
     * mid-trashcan-deferral is extremely rare; rather than risk corrupting those
     * dying objects with a DECREF, just drop the reference -- a small bounded
     * leak of a few already-dead objects beats a use-after-free. */
    snap->delete_later = NULL;
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
#define RUNLOOM_CHUNK_POOL_CAP 4096
/* RUNLOOM_TLS expands to __thread on GCC/Clang, __declspec(thread) on MSVC. */
static RUNLOOM_TLS _PyStackChunk *runloom_chunk_pool = NULL;
static RUNLOOM_TLS int runloom_chunk_pool_size = 0;

/* Pop one chunk off the pool.  Returns NULL when empty; caller falls
 * back to letting PyEval arena-allocate. */
static _PyStackChunk *runloom_chunk_pool_get(void)
{
    _PyStackChunk *c = runloom_chunk_pool;
    if (c == NULL) return NULL;
    runloom_chunk_pool = c->previous;
    runloom_chunk_pool_size--;
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
static void runloom_chunk_pool_put(_PyStackChunk *c, PyObjectArenaAllocator *alloc)
{
    if (runloom_chunk_pool_size >= RUNLOOM_CHUNK_POOL_CAP) {
        if (alloc->free != NULL) {
            alloc->free(alloc->ctx, c, c->size);
        }
        return;
    }
    c->previous = runloom_chunk_pool;
    c->top = 0;
    runloom_chunk_pool = c;
    runloom_chunk_pool_size++;
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
static int runloom_chunk_pool_install(PyThreadState *ts)
{
    _PyStackChunk *c = runloom_chunk_pool_get();
    if (c == NULL) return 0;
    ts->datastack_chunk = c;
    ts->datastack_top = &c->data[1];
    ts->datastack_limit = (PyObject **)((char *)c + c->size);
    return 1;
}
#endif
