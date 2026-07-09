/* Part of netpoll.c -- #included into that translation unit, not compiled
 * standalone.  See netpoll.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* ---- Deterministic simulated-I/O netpoll pump (Slice 2, RUNLOOM_SIM) --------
 *
 * The third DST pillar: with deterministic scheduling (the baton) and
 * deterministic time (the logical clock) already in place, this closes the
 * open-system limit -- "real-I/O arrival timing" -- for wait_fd-based workloads,
 * so a hang/crash reduces to one integer.  See docs/dev/soak/SIM_IO_DST.md.
 *
 * There is NO vtable and NO new poller: the sim backend is one early-return at
 * the top of runloom_netpoll_pump (netpoll_pump.c.inc) that replaces the real
 * epoll_wait/kevent/WSAPoll block with this function.  EVERYTHING else on the
 * wait_fd path -- the park/commit FSM, the parker pools, the deadline heap, the
 * per-fd buckets, cancel, and M:N wake routing -- is inherited verbatim; only
 * the CLOCK the deadlines are measured against changes (netpoll_deadline_now_ns
 * -> the logical clock under sim), and this pump advances that clock instead of
 * sleeping the OS thread.
 *
 * SCOPE of this increment -- the TIMEOUT path.  A sim wait_fd with a finite
 * timeout parks with a logical-clock deadline; the drain calls this pump (the
 * parker keeps netpoll_parked > 0); the pump advances the logical clock to the
 * earliest pending deadline (no wall sleep -> logical-time compression) and
 * fires every now-due timeout through the existing runloom_pump_drain_expired,
 * which unlinks the parker (decrementing netpoll_parked) and re-queues its g.
 * The workload thus terminates deterministically and instantly.
 *
 * NOT yet modelled here (documented follow-ups): the fd-READINESS plane (bytes
 * flowing over sim sockets -- Slice 3, prefer socketpair-backed real fds, whose
 * readiness a later revision of this loop dispatches in seed-drawn order BEFORE
 * the clock advance); and the instant deadlock oracle for a forever-park on the
 * netpoll path (the settle census counts only PARKED_CHAN/PARKED_SAFE today, and
 * a wait_fd(timeout<0) with no modelled readiness would spin here -- so sim
 * scenarios use finite timeouts / modelled readiness until that lands).
 *
 * SINGLE-THREAD target: the sim runs under runloom_c.run() (the single-thread
 * drain), which is what the Slice-0/1 simnet already uses.  Multi-hub sim is a
 * research item (deterministic only at H=1); this pump advances the single-thread
 * logical clock via the scheduler this thread owns. */
static int runloom_netpoll_sim_pump(long long timeout_ns)
{
    int woke = 0;
    long long netpoll_min, advanced;
    runloom_sched_t *s = runloom_sched_get();

    /* The sim never blocks on wall time; timeout_ns (a real-poller wait bound) is
     * meaningless here.  It is IGNORED -- the logical clock is advanced instead. */
    (void)timeout_ns;

    /* Slice 3: dispatch modelled fd readiness due at the current logical instant
     * FIRST -- through the existing runloom_pump_dispatch_event (netpoll_sim_ready.c.inc),
     * in the stable (deliver_at, conn_id, dir) order.  If anything woke, HOLD the
     * clock (return) so ready I/O at `now` beats a same-instant wait_fd timeout
     * (which fires only on a later pass, once the ledger has nothing due). */
    woke += runloom_sim_dispatch_ready();
    if (woke > 0) return woke;

    /* Nothing ready now: advance the logical clock to the earliest pending
     * deadline across the netpoll heaps (netpoll_min), the scheduler's earliest
     * logical sleeper (folded in by advance_clock), AND a FUTURE ledger delivery
     * (ledger_next) -- so a delivery scheduled for a later logical instant
     * compresses time to it, like a logical sleeper.  advance_clock returns the
     * exact ns so the expiry compare is long-long-vs-long-long, no double round-trip. */
    netpoll_min = runloom_dh_peek_deadline_global();
    {
        long long ledger_next = runloom_sim_ready_next_ns();
        if (ledger_next >= 0 && (netpoll_min < 0 || ledger_next < netpoll_min))
            netpoll_min = ledger_next;
    }
    advanced = runloom_sched_sim_advance_clock(s, netpoll_min);
    if (advanced >= 0) {
        /* Fire netpoll timeouts due at the advanced clock.  If the clock advanced
         * to a sleeper earlier than netpoll_min, `advanced` < every netpoll
         * deadline so none fire (the drain wakes the sleeper next iteration); if
         * it advanced to a netpoll deadline, that deadline == `advanced` so the
         * expiry `deadline_ns <= advanced` holds and it fires. */
        woke += runloom_pump_drain_expired(advanced);
        return woke;
    }

    /* advanced < 0: NOTHING is pending -- no netpoll deadline, no logical
     * sleeper.  The pump is only entered when the ready ring is empty, and under
     * single-thread sim there is no iouring / blockpool / foreign work, so if any
     * netpoll parker remains it is UNTIMED (timeout < 0) and the system is fully
     * SETTLED: no runnable fiber exists to ever generate the readiness it waits
     * on, so it can never wake.  That is a genuine deadlock (the sim's whole
     * point -- surface it, don't hang).  Correct regardless of a future readiness
     * plane: readiness is produced only by runnable fibers, and there are none.
     * Wake this thread's untimed parkers as unsatisfiable (ready_mask -1 ->
     * wait_fd raises OSError), so run() terminates with a loud error instead of a
     * hot-spin.  COUNT the reaped parkers into runloom_sim_reap_total so a
     * workload can assert its expected settle-reap total (infra parkers -- e.g.
     * MITM shuttlers still parked at end) and flag any EXCESS as a stranded
     * fiber == a netpoll-plane lost wake (count_deadlocked censuses only
     * chan/safe, so this is the netpoll-plane analogue).  The count is taken HERE,
     * at the sim pump's settle site -- NOT inside drain_parked, whose other caller
     * (m_sched_reset) must not pollute the tally. */
    /* Gate on THIS scheduler's parked count, not the global one: drain_parked (and
     * so the reap tally) is OWNER-scoped -- it reaps only parkers whose g->owner is
     * this sched, deliberately leaving NULL-owner hub gs linked.  At H=1 (sim's
     * scope) the two counts coincide, but matching the scope keeps guard, reap, and
     * the drain-re-entry decision consistent (review finding). */
    if (s != NULL &&
        __atomic_load_n(&s->netpoll_parked, __ATOMIC_ACQUIRE) > 0) {
        int reaped = runloom_netpoll_drain_parked();
        woke += reaped;
        runloom_sim_reap_total += reaped;
    }
    return woke;
}
