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

    /* [Slice 3] dispatch modelled fd readiness in seed-drawn order here, via
     * runloom_pump_dispatch_event, BEFORE advancing the clock -- so ready I/O
     * wins over a same-instant timeout, matching a real poller. */

    /* Earliest netpoll deadline across all pools (ns on the logical clock; -1 if
     * no timed parker).  The advance takes the min with the scheduler's earliest
     * logical sleeper and returns that exact ns, so the expiry compare below is
     * long-long-vs-long-long with no double round-trip. */
    netpoll_min = runloom_dh_peek_deadline_global();
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
     * hot-spin.  (Feeding this into the count_deadlocked oracle -- which counts
     * only PARKED_CHAN/PARKED_SAFE today -- by teaching the census PARKED_NETPOLL
     * is a documented follow-up; termination is the correctness floor.) */
    if (runloom_netpoll_parked_count() > 0)
        woke += runloom_netpoll_drain_parked();
    return woke;
}
