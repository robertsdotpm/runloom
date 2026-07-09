/* netpoll.h -- portable I/O multiplexing for the scheduler.
 *
 *   runloom_netpoll_wait_fd(fd, READ | WRITE, timeout_ns)
 *     park the current fiber until the fd is ready or timeout expires.
 *     Returns READY mask, 0 on timeout, -1 on error (errno set).
 *
 * Backend per OS:
 *   Linux           -> epoll_create1 + epoll_wait
 *   BSD/macOS       -> kqueue + kevent
 *   Solaris         -> event ports (fallback to select for v0)
 *   Windows         -> select for v0 (IOCP is async-completion, harder
 *                      to slot under a "wait until ready" API)
 *   everything else -> select
 *
 * v0 caveat: this is single-threaded.  Phase C M:N scheduler will need
 * to extend with thread-safe registration and per-hub poll sets.
 */
#ifndef RUNLOOM_NETPOLL_H
#define RUNLOOM_NETPOLL_H

#include "compat.h"
#include "plat.h"

#define RUNLOOM_NETPOLL_READ  0x1
#define RUNLOOM_NETPOLL_WRITE 0x2

/* Sentinel returned by runloom_netpoll_wait_fd when the parked fiber was
 * cancelled out-of-band via runloom_netpoll_cancel_g (a task.cancel() targeting a
 * g parked in a C wait_fd, where there is no coro await-point to throw into).
 * A high positive bit that can never be a real event mask (0x1/0x2) nor the
 * 0/-1 timeout/error returns, so callers distinguish it without an errno or a
 * sign check.  runloom.aio's wait_fd wrapper turns this into CancelledError. */
#define RUNLOOM_NETPOLL_CANCELLED 0x40000000

/* Sentinel stored in a parker's ready_out by runloom_netpoll_signal_wake when the
 * scheduler hands a raised Python signal-handler exception to a fiber
 * parked in wait_fd (so it propagates out of the cooperative blocking call
 * through that fiber's own stack, not out of run()).  On resume wait_fd
 * restores the exception the scheduler stashed on this g's owner scheduler
 * (->signal_exc) and returns -1 with it set.  A distinct high bit, never a real
 * event mask nor the 0/-1/CANCELLED returns. */
#define RUNLOOM_NETPOLL_SIGNALED 0x20000000

/* Sentinel stored in a parker's ready_out by runloom_netpoll_unpark_many when a
 * cooperative primitive (Event.set / Condition.notify_all / Semaphore.release)
 * wakes a batch of fiber waiters DIRECTLY -- claiming each parker and
 * re-queuing its g, bypassing the per-waiter pipe-write -> epoll -> drain
 * round-trip.  Like CANCELLED/SIGNALED it is a distinct high bit that can never
 * be a real event mask (0x1/0x2) nor the 0/-1 timeout/error returns; the
 * _Parker/events.py side treats ANY wait_fd return as "woke, re-check the
 * predicate", so the distinct value is informational (FV/debug, and lets a
 * caller tell an explicit unpark from a deadline-0). */
#define RUNLOOM_NETPOLL_UNPARKED 0x10000000

/* Directly wake up to `n` fibers parked in runloom_netpoll_wait_fd (each via
 * its g->netpoll_parker), as a batch -- the wake side of a fan-in primitive.
 * Claims each parker through the SAME commit CAS the pump uses (so the
 * {pump, timeout, cancel, unpark} race resolves to exactly one winner), stores
 * RUNLOOM_NETPOLL_UNPARKED into its ready_out, unlinks it, and re-queues a
 * committed (PARKED) g.  A g whose parker is still NULL (it appended itself to
 * the primitive's wait list but has not yet committed the wait_fd park -- the
 * edge-before-park window) cannot be direct-woken; its index is written into
 * `missed_out` (caller-provided, capacity >= n) so the caller can fall back to
 * the pipe-write backstop for exactly those.  Returns the number of missed
 * indices (written to missed_out[0..ret)).  gs[i] may be NULL (skipped, not
 * counted as missed). */
struct runloom_g;   /* forward decl (full type lives in runloom_sched.h) */
int runloom_netpoll_unpark_many(struct runloom_g **gs, int n, int *missed_out);

/* Park the current fiber until fd is ready for any of `events`,
 * or timeout_ns nanoseconds have passed (-1 = wait forever).
 * Returns the ready events mask (subset of `events`), 0 on timeout,
 * -1 on error.  Must be called from inside a fiber. */
int runloom_netpoll_wait_fd(int fd, int events, long long timeout_ns);

/* Cooperative-socket variant of wait_fd: maps the POSITIVE
 * RUNLOOM_NETPOLL_CANCELLED sentinel to errno=ECANCELED + return -1, so a
 * fast-path caller testing "(wait_fd < 0) -> raise OSError" unwinds a cancelled
 * park instead of re-parking on a still-open fd (audit finding B3).  Used by the
 * C socket fast paths (module_tcp.c.inc + the TCPConn methods); the aio bridge
 * keeps the raw wait_fd so it maps the sentinel to its own CancelledError. */
int runloom_netpoll_wait_fd_coop(int fd, int events, long long timeout_ns);

/* Drive netpoll once.  Returns the number of fibers woken.
 * Called by the scheduler when its ready queue is empty but at
 * least one fiber is parked. */
int runloom_netpoll_pump(long long timeout_ns);

/* How many fibers are currently parked.  Scheduler uses this
 * to decide whether to call pump or exit. */
int runloom_netpoll_parked_count(void);

/* R0 gauges (lock-free): timed parkers across all deadline heaps; stale-arm
 * heals since start; count of fds holding a nonzero epoll LEVEL arm mask. */
int runloom_netpoll_deadline_heap_total(void);
unsigned long long runloom_netpoll_stale_arm_heals(void);
int runloom_netpoll_fd_armed_count(void);

/* DIAG: dump every parked parker (fd/g/hub/commit) to stderr. */
void runloom_netpoll_dump_parkers(void);

/* Hub-idle dwell-based stack reclaim (RUNLOOM_STACK_PARK_SWEEP).  The
 * calling hub madvises the idle stack pages of its OWN parkers whose
 * park has exceeded threshold_ns.  Safe only when called by the owning
 * hub while idle (see the netpoll.c definition).  Returns # reclaimed. */
int runloom_netpoll_sweep_idle(void *hub_opaque, long long threshold_ns);

/* Forcibly wake every parked fiber with ready_mask=-1.  Used by
 * sched_reset() on paio.run cleanup so leftover accept loops /
 * tickers don't block the next runloom_c.run(). */
int runloom_netpoll_drain_parked(void);

/* Wake ONE wait_fd parker owned by the calling thread's scheduler with a
 * benign (ready_mask=0) result, so it resumes and runs PyErr_CheckSignals in
 * its own tstate.  Called by the idle pump when its blocking wait returned
 * EINTR (a signal interrupted it): the woken fiber delivers the pending
 * Python signal handler in-context, so a handler that raises propagates out of
 * the cooperative blocking call (recv/accept/select/...) through that
 * fiber's stack -- where its own try/except sees it -- instead of the
 * scheduler swallowing it or carrying it out of run().  Returns 1 if a parker
 * was woken, 0 if none were eligible (the scheduler then handles the signal
 * itself and carries a raised exception out of run_forever()). */
int runloom_netpoll_signal_wake(void);

/* Force-unlink a g's pending parker, if any.  Called by the hub
 * completion path before runloom_g_decref so a leaked parker (M:N race
 * where some wake path bypassed runloom_parker_unlink) cannot survive
 * into stack-pool reuse and resurrect the freed g via pump dispatch. */
struct runloom_g;
void runloom_netpoll_force_unlink_g_parker(struct runloom_g *g);

/* Cancel a fiber parked in runloom_netpoll_wait_fd: claim its parker (the
 * same commit-CAS the pump uses, so exactly one of {pump, timeout, cancel}
 * wins), make its wait_fd return RUNLOOM_NETPOLL_CANCELLED, and re-queue it to its
 * owner scheduler.  Returns 1 if a parked g was woken, 0 if g had no live
 * parker (not parked in wait_fd, or already woken by the pump/timeout).  This
 * is the per-g cancel primitive that lets task.cancel() interrupt a g blocked
 * in a socket recv/accept/connect with no coro await-point. */
int runloom_netpoll_cancel_g(struct runloom_g *g);

/* Clear the "fd is registered in netpoll" cache bit.  Call from the
 * socket-close hook so a future fd reuse re-registers cleanly.  No
 * syscall; the kernel auto-clears its epoll/kqueue entry when the
 * last fd reference closes, so this just keeps our bitmap honest.
 * Safe to call on unknown fds (no-op). */
void runloom_netpoll_unregister(int fd);

/* Drop an OPEN fd's epoll registration IFF no fiber is parked on it (any
 * pool).  Unlike unregister, issues EPOLL_CTL_DEL (the fd is still open, so the
 * kernel won't auto-remove it).  The aio bridge calls this after each low-level
 * loop.sock_* op on a user socket -- those close via a plain socket.close() that
 * never reaches the unregister hook, so without this a reused fd inherits a stale
 * arm and hangs.  epoll-only (kqueue re-arms per park; select needs no reg). */
void runloom_netpoll_release_if_idle(int fd);

/* Wake every fiber parked in wait_fd on `fd` (returning
 * RUNLOOM_NETPOLL_CANCELLED) -- the socket close hook calls this AFTER closing
 * the fd so a cross-fiber close unblocks a parked accept()/recv()/connect()
 * instead of stranding it forever (BUG #5).  Safe on unknown fds (no-op). */
void runloom_netpoll_cancel_fd(int fd);

/* Cancel every fiber parked on ANY fd across all pools (returning
 * RUNLOOM_NETPOLL_CANCELLED) -- a teardown backstop so a fiber left parked on an
 * idle-but-open socket cannot wedge mn_run's join-on-pending (audit B3).
 * Returns the count cancelled; cheap no-op when nothing is parked. */
int runloom_netpoll_cancel_all_parked(void);

/* One-time init / cleanup. */
int runloom_netpoll_init(void);
void runloom_netpoll_fini(void);

/* Reset netpoll in a forked child: close the inherited (shared-with-parent)
 * poll fd, re-init the per-pool locks, and drop inherited parker bookkeeping
 * so the child re-creates its own poller cleanly.  Single-thread child only. */
void runloom_netpoll_reset_after_fork(void);

/* Backend name for diagnostics: "epoll" / "kqueue" / "select". */
const char *runloom_netpoll_backend(void);

/* The shared epoll fd (Linux), for the io_uring-as-loop backend to poll-add
 * into a hub ring.  -1 on non-epoll backends.  Forces netpoll init. */
int runloom_netpoll_epoll_fd(void);

/* The epoll fd the CURRENT hub waits on: per-hub (pool->epoll_fd) under
 * RUNLOOM_PERHUB_EPOLL, else the shared one.  The io_uring-as-loop F_EPOLL bridge
 * must poll THIS so a hub's own socket fds are observed in per-hub mode. */
int runloom_netpoll_hub_epoll_fd(void);

/* Test-only Windows netpoll fault-injection introspection (see netpoll.c).
 * runloom_fault_count returns how many times the named site ("WSAPOLL"/"SELECT"/
 * "IOCP_WAIT"/"IOCP_SUBMIT") injected an error (-1 for an unknown name / a
 * non-Windows build); runloom_fault_reset clears all counters + once-flags.
 * No-ops on non-Windows. */
long runloom_fault_count(const char *name);
void runloom_fault_reset(void);

/* Fault-injection site indices, shared with runloom_tcp.c so the socket-surface
 * syscalls can be faulted on the kqueue/Windows backends (which have no
 * syscall-injecting tracer; Linux uses strace).  Keep in sync with the name/
 * env tables in netpoll.c. */
enum {
    RUNLOOM_FAULT_WSAPOLL = 0, RUNLOOM_FAULT_SELECT, RUNLOOM_FAULT_IOCP_WAIT,
    RUNLOOM_FAULT_IOCP_SUBMIT, RUNLOOM_FAULT_KQUEUE_WAIT,
    RUNLOOM_FAULT_KQUEUE_CREATE, RUNLOOM_FAULT_KQUEUE_CTL,
    RUNLOOM_FAULT_KQUEUE_PERHUB,   /* force a NON-default pool's kqueue create to fail */
    RUNLOOM_FAULT_TCP_SOCKET, RUNLOOM_FAULT_TCP_CONNECT, RUNLOOM_FAULT_TCP_ACCEPT,
    RUNLOOM_FAULT_TCP_RECV, RUNLOOM_FAULT_TCP_SEND,
    RUNLOOM_FAULT_FD_READ, RUNLOOM_FAULT_FD_WRITE,
    /* Goroutine-spawn allocation OOM injection (every platform). */
    RUNLOOM_FAULT_SPAWN_G, RUNLOOM_FAULT_SPAWN_STACK, RUNLOOM_FAULT_SPAWN_TSTATE,
    RUNLOOM_FAULT_NSITES
};
/* Returns the errno/WSA code to inject at this site now (nonzero), or 0.
 * Defined only on the kqueue/Windows backends (Linux uses strace); runloom_tcp.c
 * calls it only there, so this prototype is harmless + unreferenced on Linux.
 * The SPAWN_* sites are injected on every platform (in-process alloc faults). */
int runloom_fault_inject(int site);

/* Cached "is any RUNLOOM_FAULT_SPAWN_* env set" check, so the fiber-spawn
 * alloc sites pay only a branch when OOM injection is not armed. */
int runloom_spawn_fault_armed(void);
#define RUNLOOM_SPAWN_FINJ(site) \
    (runloom_spawn_fault_armed() ? runloom_fault_inject(site) : 0)

/* Register an external eventfd so the pump treats EPOLLIN on that fd
 * as a "drain io_uring CQEs" signal.  Only meaningful on the epoll
 * backend (Linux); a no-op elsewhere.  Used by io_uring.c to hook
 * its completion eventfd into the global pump.  Caller retains
 * ownership of the fd. */
int runloom_netpoll_add_iouring_eventfd(int fd);

/* Like above but registers a per-hub ring instead of the global ring.
 * The pump dispatches the eventfd hit to runloom_iouring_ring_drain(ring).
 * Up to RUNLOOM_NETPOLL_MAX_IOURING_RINGS hub rings may be registered at
 * once (sized for typical CPU counts).  Returns 0 on success, -1 on
 * "too many registered" or non-epoll backend. */
struct runloom_iouring_ring;
int runloom_netpoll_add_iouring_ring(int eventfd_fd,
                                  struct runloom_iouring_ring *ring);
void runloom_netpoll_remove_iouring_ring(int eventfd_fd);

/* Generic cross-thread pump interrupt.  Arm once (idempotent); returns 0
 * if the backend supports it (epoll today), -1 otherwise.  Any thread may
 * then call runloom_netpoll_wake_pump() to break an idle epoll_wait so a
 * scheduler blocked in the pump re-checks its ready/wake lists.  Used by
 * the blocking-offload pool to wake the single-thread scheduler. */
int  runloom_netpoll_wake_pump_arm(void);
/* hub_opaque names the hub whose pump to wake (its own kqueue, per-hub kqueue
 * backend).  NULL = default/single-thread pool.  Ignored on the shared-handle
 * backends (epoll eventfd / Windows IOCP / select self-pipe). */
void runloom_netpoll_wake_pump(void *hub_opaque);

/* Deterministic sim readiness plane (RUNLOOM_SIM, Slice 3; see
 * netpoll_sim_ready.c.inc).  Register a socketpair-backed sim connection and get
 * its stable conn_id -- the ready-ledger ordering key (never the raw fd).  Append
 * a readiness delivery: at logical time deliver_at, wake a fiber parked on `fd`
 * for `dir` (RUNLOOM_NETPOLL_READ/WRITE) via the sim pump's dispatch.  deliver_ready
 * is a no-op when RUNLOOM_SIM is off (the real epoll pump owns readiness then). */
long long runloom_sim_conn_register(int fd_a, int fd_b);
void runloom_sim_deliver_ready(long long conn_id, int fd, int dir, long long deliver_at);
/* Reset the sim readiness plane (ledger + conn registry + logical clock + reap
 * tally) between run()s -- for multi-scenario-per-process replay/tests. */
void runloom_sim_reset(void);

/* Total parkers reaped at settled deadlocks by the sim pump this run (increment
 * O): a workload asserts its expected infra-reap total and flags excess as a
 * stranded fiber (a netpoll-plane lost wake). */
long long runloom_sim_reap_count(void);

/* MN_SIM_DST_PLAN.md I1 hooks -- the mn census's view of the ready ledger.
 * peek: earliest deliver_at OVERALL (due or future), -1 if empty; takes NO
 * clock reading (the census compares against ITS ns clock, never the possibly-
 * unmirrored global one).  dispatch_due: fire every entry with deliver_at <=
 * now_ns in the strict total order via runloom_pump_dispatch_event, three-phase
 * locked (the ledger lock is never held across a wake); returns parkers woken. */
long long runloom_sim_ready_peek_ns(void);
int runloom_sim_dispatch_due(long long now_ns);

/* Is fd one end of a registered sim connection?  The wait_fd gate under
 * native mn-sim (I2 gate [11]): unregistered fds have no wake source there. */
int runloom_sim_conn_has_fd(int fd);

#endif /* RUNLOOM_NETPOLL_H */
