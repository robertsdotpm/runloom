/* Part of runloom_sched.c -- #included into that translation unit, not compiled
 * standalone.  See runloom_sched.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


PyObject *runloom_sched_spawn_sized(runloom_sched_t *s, PyObject *callable,
                                 size_t stack_size)
{
    /* Explicit stack_size= path: the user's size wins -- do NOT autosize.
     * Still record the kind so stack_advice() can report on it. */
    size_t key = runloom_advice_note_spawn(callable);
    if (stack_size < RUNLOOM_MIN_STACK_SIZE) stack_size = RUNLOOM_MIN_STACK_SIZE;
    if (stack_size > RUNLOOM_MAX_STACK_SIZE) stack_size = RUNLOOM_MAX_STACK_SIZE;
    return spawn_common(s, callable, /*noyield*/0, stack_size, key);
}

/* ---- Yield ---- */
__attribute__((hot))
void runloom_sched_yield(runloom_sched_t *s)
{
    runloom_g_t *g;
    /* M:N first.  If a hub claims this thread, it handles the requeue
     * + snap + asm-yield internally and we return through hub_main's
     * resume cycle. */
    if (__builtin_expect(runloom_mn_yield_current(), 0)) {
        return;
    }
    g = s->current;
    if (__builtin_expect(g == NULL, 0)) return;
    /* Fold any pending wakes into the ready ring BEFORE the fast-path
     * check and BEFORE we re-queue ourselves.  G.wake() (used by
     * runloom.aio for Task.cancel() / future.set_result()/set_exception())
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
    if (__builtin_expect(
            __atomic_load_n(&s->wake_list_head, __ATOMIC_ACQUIRE) != NULL, 0)) {
        runloom_sched_drain_wake_list(s);
    }
    /* Fast path (Go's runtime.Gosched shortcut): if there's nobody
     * else to run -- no other ready gs, no sleepers due, no parked
     * I/O -- yielding is just expensive bookkeeping that hands
     * control right back to us.  Skip the whole snap + asm-yield +
     * resume cycle and return.  This cuts the single-coro tight-yield
     * baseline from ~230 ns to <10 ns. */
    if (__builtin_expect(runloom_sched_ready_empty(s)
                         && s->sleep_size == 0
                         && runloom_netpoll_parked_count() == 0
                         && runloom_blockpool_inflight() == 0, 1)) {
        return;
    }
    runloom_ready_push(s, g);
    /* Save tstate INTO g's snap.  The scheduler's snap (in drain's
     * local frame) is untouched; drain will load it after the swap
     * returns. */
    runloom_pystate_snap(&g->snap);
    runloom_coro_yield();
    /* On resume, drain has loaded g's snap back into tstate; we resume
     * exactly where we left off. */
}

/* Park current g without re-queueing (caller takes ownership and
 * arranges to wake it later via runloom_sched_wake / runloom_mn_wake_g).
 * Hub-aware: in an M:N hub the current g is in TLS, not in the
 * global sched->current slot. */
void runloom_sched_park_current(void)
{
    runloom_g_t *g;
    if (runloom_mn_current_hub_opaque() != NULL) {
        g = runloom_mn_tls_current_g();
        /* Tell hub_main that this g has been taken off-queue by an
         * external parker; don't re-push it on the local FIFO when
         * runloom_coro_yield returns control to hub_main. */
        runloom_mn_tls_mark_parked();
    } else {
        runloom_sched_t *s = runloom_sched_get();
        g = s->current;
    }
    if (g == NULL) return;
    runloom_pystate_snap(&g->snap);
    /* DO NOT push to ready; the parker (netpoll, channel, etc) owns
     * the g until it calls runloom_sched_wake / runloom_mn_wake_g. */
}

void runloom_sched_wake(runloom_g_t *g)
{
    runloom_sched_t *self, *owner;
    if (g == NULL) return;
    runloom_g_state_set(g, RUNLOOM_GST_RUNNABLE);
    self  = runloom_sched_get();
    owner = g->owner ? g->owner : self;
    if (owner == self) {
        /* Same thread (the common single-loop / same-loop case): push onto our
         * own cooperative ready ring. */
        runloom_ready_push(self, g);
        return;
    }
    /* Phase 2 -- cross-thread wake.  g's owner sched runs on ANOTHER OS thread
     * (e.g. the shared netpoll pump that delivered this fd event is draining on
     * a different loop's thread than the one that parked the g).  Pushing onto
     * OUR ready ring would resume the g on the wrong thread (and our ready ring
     * is single-consumer, not cross-thread-safe).  Instead enqueue onto the
     * OWNER's thread-safe wake_list (its drain consumes it), then kick its pump
     * so an idle epoll_wait wakes to drain the list.  Same mechanism as
     * runloom_sched_wake_safe; the kick eventfd is level-triggered + non-exclusive
     * so every blocked pumper wakes and drains its own wake_list. */
    runloom_mutex_lock(&owner->wake_list_lock);
    g->wake_next = NULL;
    if (owner->wake_list_tail != NULL) {
        owner->wake_list_tail->wake_next = g;
    } else {
        /* RELEASE: see the matching note in runloom_sched_wake_safe -- pairs with
         * the owner drain's lock-free ACQUIRE peek of wake_list_head. */
        __atomic_store_n(&owner->wake_list_head, g, __ATOMIC_RELEASE);
    }
    owner->wake_list_tail = g;
    runloom_mutex_unlock(&owner->wake_list_lock);
    runloom_netpoll_wake_pump();
}

/* Race-safe park/wake.  Used by runloom.aio.RunloomTask to replace its
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
void runloom_sched_wake_safe(runloom_g_t *g)
{
    if (g == NULL) return;

    /* Bump wake_pending FIRST so the parker's recheck after its
     * parked_safe store observes our arrival.  ACQ_REL pairs with
     * park_safe's load-acquire on wake_pending. */
    __atomic_add_fetch(&g->wake_pending, 1, __ATOMIC_ACQ_REL);

    /* StoreLoad barrier: the bump (store to wake_pending) and the CAS-load of
     * parked_safe below are on DIFFERENT locations, and release/acquire alone
     * does NOT order a store-then-load -- so without this fence the waker's CAS
     * could read a stale parked_safe==0 while, symmetrically, the parker's
     * recheck reads a stale wake_pending==0, and BOTH miss each other: the g
     * parks but is never enqueued (lost wakeup -> permanent hang).  This is the
     * Dekker/StoreLoad pattern; the matching fence is in park_safe.  Verified
     * necessary + sufficient by verify/genmc/sched_parkwake.c under RC11
     * (x86-TSO is also vulnerable via the store buffer). */
    __atomic_thread_fence(__ATOMIC_SEQ_CST);

    /* Try to transition parked_safe 1->0.  On success, we own the
     * wake and route g back to its home sched via the thread-safe
     * wake_list (runloom_global_sched here -- park_safe is single-thread
     * sched only; M:N hubs use runloom_mn_wake_g for an analogous race
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
            /* Peek THIS thread's sched WITHOUT creating one.  A foreign waker
             * (run_in_executor blockpool worker, iouring CQE) has no per-thread
             * sched and must NOT lazily allocate one here -- runloom_sched_get()
             * runs mimalloc, and on a thread with no usable Python heap that
             * crashes.  runloom_tls_sched is NULL on exactly those threads, which
             * is precisely the cross-thread case.  Keep the `s` (wake target)
             * computation byte-identical to the original so owner-less and
             * foreign paths are unchanged: runloom_sched_get() is still reached
             * only when g->owner == NULL (never on a foreign waker, whose woken
             * g always has an owner). */
            runloom_sched_t *cur = runloom_tls_sched;
            runloom_sched_t *s = g->owner ? g->owner : runloom_sched_get();
            if (cur != NULL && s == cur) {
                /* Same-thread wake -- the single-hub asyncio bridge's common
                 * case: a future completed by a goroutine on the loop's own
                 * thread wakes a RunloomTask parked on that future.  Push straight
                 * onto our cooperative ready ring (exactly what runloom_sched_wake
                 * does for owner==self) so the wake is FIFO-ordered with the
                 * go()/call_soon spawns issued AFTER it.  Routing a same-thread
                 * wake through the batch-drained wake_list instead made a woken
                 * task resume AFTER a call_soon scheduled later in the same turn,
                 * violating asyncio's guarantee that a future's completion
                 * callbacks run in call_soon (FIFO) order -- the asyncssh
                 * channel-open-vs-close ordering crash, and
                 * call_soon_fifo.py::test_future_then_callback.  The parked_safe
                 * CAS above already serialized the claim, so this is race-safe;
                 * the ready ring is single-consumer and we are its consumer. */
                runloom_ready_push(s, g);
            } else {
                /* Cross-thread wake -- foreign-thread waker (run_in_executor
                 * pool worker, iouring CQE).  Our ready ring is single-consumer
                 * and not cross-thread-safe; route to the owner's thread-safe
                 * wake_list, which its own drain consumes. */
                runloom_mutex_lock(&s->wake_list_lock);
                g->wake_next = NULL;
                if (s->wake_list_tail != NULL) {
                    s->wake_list_tail->wake_next = g;
                } else {
                    /* RELEASE: publishes g (and its wake_next/fields) to the
                     * owner's LOCK-FREE peek of wake_list_head, which loads it
                     * ACQUIRE. tail/wake_next are only read under the lock, so
                     * only head needs an atomic on the lock-free edge. */
                    __atomic_store_n(&s->wake_list_head, g, __ATOMIC_RELEASE);
                }
                s->wake_list_tail = g;
                runloom_mutex_unlock(&s->wake_list_lock);
            }
        }
        /* CAS failed: g was either running (parked_safe==0 already)
         * or another wake_safe already claimed it.  Our wake_pending
         * bump is still observable; the parker (or the prior claimer's
         * subsequent park_safe) will consume it. */
    }
}

void runloom_sched_park_safe(void)
{
    runloom_sched_t *s = runloom_sched_get();
    runloom_g_t *g = s->current;
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

    /* StoreLoad barrier between the parked_safe store (above) and the
     * wake_pending recheck (below).  They are on DIFFERENT locations, so
     * release/acquire does NOT prevent the store-load reorder -- without this
     * fence the recheck could read a stale wake_pending==0 while wake_safe's
     * CAS reads a stale parked_safe==0, and the wake is lost (g parks, never
     * enqueued).  Pairs with the fence in wake_safe; see that comment and
     * verify/genmc/sched_parkwake.c (the -DBUG_NO_SC_FENCE control reproduces
     * the lost wake; the SC fence makes GenMC clean under RC11). */
    __atomic_thread_fence(__ATOMIC_SEQ_CST);

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

    runloom_pystate_snap(&g->snap);
    runloom_g_state_set(g, RUNLOOM_GST_PARKED_SAFE);
    runloom_coro_yield();
    /* On resume, parked_safe has already been cleared by wake_safe's
     * CAS.  Eat one wake_pending count -- there is at least one (the
     * wake that delivered us back to ready). */
    __atomic_sub_fetch(&g->wake_pending, 1, __ATOMIC_ACQ_REL);
    runloom_g_state_set(g, RUNLOOM_GST_RUNNING);
}

/* ---- Sleep ----
 *
 * Hub-aware: in an M:N hub the sleep heap belongs to the hub's
 * runloom_sched_t (h->sched), not the global single-thread sched.  We
 * also mark self_queued so hub_main doesn't re-push the g onto the
 * local FIFO on return from runloom_coro_yield (same rationale as
 * runloom_sched_park_current). */
/* real != 0: this sleeper uses the WALL clock even under the logical clock
 * (RUNLOOM_LOGICAL_CLOCK) -- see g->sleep_real.  Public wrappers below. */
static void runloom_sched_sleep_until_ex(runloom_sched_t *s, double wake_at, int real)
{
    runloom_g_t *g;
    runloom_sched_t *target = runloom_mn_current_sched();
    if (target != NULL) {
        g = runloom_mn_tls_current_g();
        if (g == NULL) return;
        g->wake_at = wake_at;
        g->sleep_real = real;
        if (runloom_sleep_push(target, g) < 0) return;
        runloom_g_state_set(g, RUNLOOM_GST_PARKED_SLEEP);
        runloom_pystate_snap(&g->snap);
        runloom_mn_tls_mark_parked();
        runloom_coro_yield();
        runloom_g_state_set(g, RUNLOOM_GST_RUNNING);
        return;
    }
    /* Single-thread path */
    g = s->current;
    if (g == NULL) return;
    g->wake_at = wake_at;
    g->sleep_real = real;
    if (runloom_sleep_push(s, g) < 0) {
        return; /* leave g in current; caller will see exception */
    }
    runloom_g_state_set(g, RUNLOOM_GST_PARKED_SLEEP);
    runloom_pystate_snap(&g->snap);
    runloom_coro_yield();
    runloom_g_state_set(g, RUNLOOM_GST_RUNNING);
}

void runloom_sched_sleep_until(runloom_sched_t *s, double wake_at)
{
    runloom_sched_sleep_until_ex(s, wake_at, 0);
}

/* Sleep until a WALL-clock deadline that the logical clock won't advance for
 * (the aio keepalive heartbeat). */
void runloom_sched_sleep_until_real(runloom_sched_t *s, double wake_at)
{
    runloom_sched_sleep_until_ex(s, wake_at, 1);
}

/* ---- run_ready: quiescence-barrier yield ----
 *
 * Park the current g on the quiescence list; the drain loop resumes it at the
 * next quiescence point -- when the ready ring is empty and it would otherwise
 * block on netpoll/timers (runloom_sched_drain).  Net effect: every goroutine
 * that is runnable *now* (including ones just woken) runs to its next park or
 * to completion before run_ready() returns, matching asyncio's "drain the
 * ready callbacks for this loop iteration" boundary.  Single-thread sched
 * only -- under an M:N hub this falls back to a single classic yield (no
 * hub-local quiescence list), which is the closest safe behaviour. */
void runloom_sched_run_ready(runloom_sched_t *s)
{
    runloom_g_t *g;
    if (runloom_mn_current_sched() != NULL) {
        /* M:N hub: no quiescence list on the hub sched.  Degrade to one pass. */
        runloom_sched_yield(s);
        return;
    }
    g = s->current;
    if (g == NULL) return;            /* not inside a goroutine: no-op */
    /* FIFO append onto the quiescence list (threaded through g->next). */
    g->next = NULL;
    if (s->quiescence_tail != NULL) {
        s->quiescence_tail->next = g;
    } else {
        s->quiescence_head = g;
    }
    s->quiescence_tail = g;
    runloom_pystate_snap(&g->snap);
    runloom_coro_yield();
}
