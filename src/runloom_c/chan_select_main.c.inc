/* Part of chan.c -- #included into that translation unit, not compiled
 * standalone.  See chan.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


int runloom_chan_select(runloom_select_case_t *cases, int n, int default_ready)
{
    int rc;
    int select_retries = 0;

    if (n <= 0) {
        PyErr_SetString(PyExc_ValueError, "select needs at least 1 case");
        return -2;
    }

select_retry:
    /* Phase 1: try each case (no parking).  This handles the
     * common case (some channel is ready) and matches Go's select-
     * scan-then-park semantics.  Also the re-entry point after a
     * spurious wake in Phase 2 (see the fired < 0 path below). */
    rc = select_try_each(cases, n);
    if (rc != -1) return rc;          /* fired, errored, or PyErr set */

    if (default_ready) return -1;     /* Go's default: branch */

    /* Phase 2: park on every channel.  Waiter records are heap-
     * allocated; their lifetime is bounded by this function call.
     * We could stack-allocate with a cap fallback, but most selects
     * have small N and the malloc cost is dwarfed by the asm
     * context switch.  Important: do NOT return without freeing
     * `waiters`. */
    {
        struct runloom_select_park park;
        runloom_chan_waiter_t *waiters;
        int i, fired;

        waiters = (runloom_chan_waiter_t *)PyMem_Calloc(
            (size_t)n, sizeof(runloom_chan_waiter_t));
        if (waiters == NULL) {
            PyErr_NoMemory();
            return -2;
        }
        park.fired_case = -1;
        park.n_cases = n;
        park.fired_value = NULL;
        park.fired_ok = 0;

        /* PyHandle pin: every channel in cases[] must stay alive across
         * the park + eviction phase.  Without this, a concurrent decref
         * of the last Python reference would free the channel mid-park,
         * and the eviction walk after wake would dereference freed
         * memory.  Balance with a decref-loop before returning. */
        for (i = 0; i < n; i++) {
            if (cases[i].ch != NULL) runloom_chan_incref(cases[i].ch);
        }
        #define RUNLOOM_SELECT_UNPIN()                                          \
            do {                                                             \
                int _i;                                                      \
                for (_i = 0; _i < n; _i++) {                                 \
                    if (cases[_i].ch != NULL) runloom_chan_decref(cases[_i].ch);\
                }                                                            \
            } while (0)

        /* Determine our hub + g once for all entries. */
        {
            void *hub = runloom_mn_current_hub_opaque();
            runloom_g_t *cur_g = (hub != NULL) ?
                runloom_mn_tls_current_g() : runloom_sched_get()->current;
            for (i = 0; i < n; i++) {
                waiters[i].g = cur_g;
                waiters[i].hub = hub;
                waiters[i].select = &park;
                waiters[i].case_index = i;
                if (cases[i].op == RUNLOOM_SELECT_SEND) {
                    Py_INCREF(cases[i].send_value);
                    waiters[i].value = cases[i].send_value;
                    waiters[i].send_result = -1;
                } else {
                    waiters[i].ok = -1;
                }
            }
        }

        /* Push each waiter under that channel's lock.  Order doesn't
         * matter for correctness because the claim-CAS is global to
         * the park, not the channel locks. */
        for (i = 0; i < n; i++) {
            runloom_chan_t *ch = cases[i].ch;
            runloom_mutex_lock(&ch->lock);
            /* It's possible the channel went ready/closed since
             * phase-1.  Re-check before parking. */
            if (cases[i].op == RUNLOOM_SELECT_SEND) {
                if (ch->closed || (ch->cap > 0 && ch->len < ch->cap) ||
                    waiter_has_foreign(ch->receivers, &park)) {
                    runloom_mutex_unlock(&ch->lock);
                    /* Race: someone freed space / opened a slot.
                     * Tear down what we've installed and retry try-each. */
                    /* Mark park as fired-by-us-on-this-case so other
                     * channels skip our tombstones.  But if the CAS
                     * LOSES, a concurrent sender/closer already claimed
                     * us on an earlier-installed case and delivered its
                     * value into that waiter.  Aborting + evicting now
                     * would DROP that delivered value (the eviction frees
                     * the waiter holding it).  Instead stop installing and
                     * fall through to the park: the delivery already
                     * woke us, so the park resumes immediately, clears the
                     * stale hub-submission, and we return the delivered
                     * value via the fired case below. */
                    {
                        int expected = -1;
                        if (!__atomic_compare_exchange_n(&park.fired_case,
                                                    &expected, i, 0,
                                                    __ATOMIC_ACQ_REL,
                                                    __ATOMIC_ACQUIRE)) {
                            break;   /* already fired (expected = winner); go park */
                        }
                    }
                    /* Already-installed waiters are tombstones; the
                     * channels they're on will skip them.  We need
                     * to evict them so freed stack memory isn't
                     * dangling. */
                    select_evict_self(cases, n, waiters, /*fired*/-1);
                    /* Drop SEND refs we incref'd. */
                    {
                        int j;
                        for (j = 0; j < n; j++) {
                            if (cases[j].op == RUNLOOM_SELECT_SEND && waiters[j].value != NULL) {
                                Py_DECREF(waiters[j].value);
                            }
                        }
                    }
                    PyMem_Free(waiters);
                    RUNLOOM_SELECT_UNPIN();
                    /* Retry the whole scan-then-park.  Do NOT return
                     * select_try_each() directly: it can come back -1
                     * (the channel that looked ready raced away to
                     * another goroutine), and a bare -1 for a BLOCKING
                     * select becomes PyLong(-1) in m_select -> the
                     * caller's `v, ok = select(...)` unpack raises
                     * TypeError and the goroutine dies, dropping work.
                     * select_retry re-scans and, if still nothing is
                     * ready, re-parks (or returns -1 only when a default
                     * case actually exists). */
                    if (++select_retries > 10000000) {
                        PyErr_SetString(PyExc_RuntimeError,
                                        "select: too many retries");
                        return -2;
                    }
                    goto select_retry;
                }
                waiter_push(&ch->senders, &ch->senders_tail, &waiters[i]);
                runloom_mutex_unlock(&ch->lock);
                continue;
            }
            if (cases[i].op == RUNLOOM_SELECT_RECV) {
                if (ch->closed || ch->len > 0 ||
                    waiter_has_foreign(ch->senders, &park)) {
                    runloom_mutex_unlock(&ch->lock);
                    {
                        int expected = -1;
                        if (!__atomic_compare_exchange_n(&park.fired_case,
                                                    &expected, i, 0,
                                                    __ATOMIC_ACQ_REL,
                                                    __ATOMIC_ACQUIRE)) {
                            break;   /* already fired by a delivery: go park
                                      * to drain the wake + return its value */
                        }
                    }
                    select_evict_self(cases, n, waiters, /*fired*/-1);
                    {
                        int j;
                        for (j = 0; j < n; j++) {
                            if (cases[j].op == RUNLOOM_SELECT_SEND && waiters[j].value != NULL) {
                                Py_DECREF(waiters[j].value);
                            }
                        }
                    }
                    PyMem_Free(waiters);
                    RUNLOOM_SELECT_UNPIN();
                    /* Retry rather than returning select_try_each() (which
                     * can be -1 if the ready channel raced away) -- see the
                     * SEND abort path above for why a bare -1 here would
                     * crash a blocking select's caller. */
                    if (++select_retries > 10000000) {
                        PyErr_SetString(PyExc_RuntimeError,
                                        "select: too many retries");
                        return -2;
                    }
                    goto select_retry;
                }
                waiter_push(&ch->receivers, &ch->receivers_tail, &waiters[i]);
            }
            runloom_mutex_unlock(&ch->lock);
        }

        /* All installed.  Park self (snap tstate) and yield. */
        if (runloom_mn_current_hub_opaque() != NULL) {
            runloom_sched_park_current();
        } else if (runloom_sched_get()->current != NULL) {
            runloom_sched_park_current();
        }
        runloom_coro_yield();

        /* Woken.  park.fired_case is the winning index, OR still -1 on a
         * spurious wake: the scheduler resumed us without any channel
         * having claimed our park (e.g. a stale hub-submission entry left
         * by a wake that raced our park).  Returning -2 here made m_select
         * hand back NULL with no exception -> CPython SystemError -> the
         * goroutine died mid-loop, dropping every value it had received.
         *
         * On an apparent spurious wake we cannot just free + retry: a
         * delivery could be claiming a still-installed waiter right now,
         * and freeing the waiters[] would drop the value it hands us.  So
         * EVICT first -- once our waiters are out of every channel's queue
         * no further claim can occur and fired_case is frozen -- THEN
         * re-read it.  A value claimed before/during the eviction is now
         * safely captured in waiters[fired]; only a still-unset fired_case
         * is genuinely spurious and retries (Go's spurious-wakeup path).
         *
         * Acquire-load: fired_case is claimed by waiter_claim's ACQ_REL CAS on
         * another thread; reading it plain was a data race (TSan) and, on weak
         * HW, could observe fired_case >= 0 while waiters[fired].value (written
         * before the release-CAS) was still stale.  The acquire pairs with that
         * release so the captured value is visible. */
        fired = __atomic_load_n(&park.fired_case, __ATOMIC_ACQUIRE);
        if (fired < 0) {
            select_evict_self(cases, n, waiters, /*fired*/-1);
            fired = __atomic_load_n(&park.fired_case, __ATOMIC_ACQUIRE);  /* re-read: now stable */
            if (fired < 0) {
                int j;
                for (j = 0; j < n; j++) {
                    if (cases[j].op == RUNLOOM_SELECT_SEND &&
                        waiters[j].value != NULL) {
                        Py_DECREF(waiters[j].value);
                    }
                }
                PyMem_Free(waiters);
                RUNLOOM_SELECT_UNPIN();
                if (++select_retries > 10000000) {
                    PyErr_SetString(PyExc_RuntimeError,
                                    "select: too many spurious wakeups");
                    return -2;
                }
                goto select_retry;
            }
            /* else: a delivery claimed waiters[fired] before we evicted;
             * fall through to handle it.  Our waiters are already out of
             * their queues, so the eviction below is a harmless no-op. */
        }

        /* Evict our tombstone waiters from all losing channels. */
        select_evict_self(cases, n, waiters, fired);

        /* Drop the value refs from non-firing SEND cases. */
        {
            int j;
            for (j = 0; j < n; j++) {
                if (j == fired) continue;
                if (cases[j].op == RUNLOOM_SELECT_SEND && waiters[j].value != NULL) {
                    Py_DECREF(waiters[j].value);
                }
            }
        }

        /* If the fired case is a SEND, the channel took our value via
         * waiters[fired].send_result.  Nothing to do on our side
         * other than check for closed-while-parked. */
        if (cases[fired].op == RUNLOOM_SELECT_SEND) {
            if (waiters[fired].send_result != 0) {
                Py_DECREF(waiters[fired].value);  /* still ours */
                PyMem_Free(waiters);
                RUNLOOM_SELECT_UNPIN();
                PyErr_SetString(PyExc_ValueError, "select send on closed channel");
                return -2;
            }
            /* Delivered.  The channel/receiver took our ref. */
            PyMem_Free(waiters);
            RUNLOOM_SELECT_UNPIN();
            return fired;
        }
        /* Fired RECV.  A real delivery has value != NULL -- return it. */
        if (waiters[fired].value != NULL) {
            cases[fired].recv_value = waiters[fired].value;  /* delivered ref */
            cases[fired].recv_ok = 1;
            PyMem_Free(waiters);
            RUNLOOM_SELECT_UNPIN();
            return fired;
        }
        /* Otherwise this was a close-wake (runloom_chan_close left value ==
         * NULL, ok == 0).  Do NOT report closed yet: re-scan from the top.
         * A value may have been buffered on this channel in the window
         * between our Phase-1 and our install (the sender buffered before
         * our waiter existed, then close claimed our waiter before our
         * abort-check could) -- and buffered values must drain before
         * closed (Go / chan_recv_locked semantics).  Likewise a sibling
         * case may now be ready and should win over a bare close.  The
         * re-scan's Phase-1 returns the buffered value / ready case if any,
         * else the closed case with ok=0.  This is bounded: a closed (or
         * ready) channel makes Phase-1 return without re-parking. */
        PyMem_Free(waiters);
        RUNLOOM_SELECT_UNPIN();
        if (++select_retries > 10000000) {
            PyErr_SetString(PyExc_RuntimeError,
                            "select: too many spurious wakeups");
            return -2;
        }
        goto select_retry;
        #undef RUNLOOM_SELECT_UNPIN
    }
}
