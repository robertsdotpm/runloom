/* mn_sched.c -- M:N scheduler.
 *
 * N OS threads, each one a "hub" with its own runloom_sched_t and a
 * Chase-Lev work-stealing deque.  Goroutines spawned by runloom_mn_go
 * are round-robined onto hubs at spawn time; once running, a g is
 * pinned to its hub (its C stack is absolute address).
 *
 * Two queues per hub:
 *   - Chase-Lev deque   (h->deque)        -- FRESH gs only.  Stealable.
 *   - Local FIFO        (h->sched.ready)  -- YIELDED gs.  Hub-pinned.
 *
 * A g moves between the two: it lives in the deque until first
 * resume, may be stolen by another hub.  Once it yields, it's pinned
 * to its hub (Phase B snap holds pointers into the g's own stack +
 * datastack chunks; cross-thread migration would require careful
 * tstate-field-by-tstate-field migration that we don't attempt).
 *
 * Work stealing: when a hub's local queues are both empty, it tries
 * to steal a g from a neighbour's deque.  Stolen gs are by
 * construction fresh (never run), so no migration concerns.
 *
 * Phase C v2 (this file): yield support inside hubs.  A fiber
 * running on hub H can call sched_yield(); the call routes through
 * runloom_mn_yield_current() which pushes the g back to H's local FIFO,
 * snapshots the per-g PythonState, and asm-yields back to hub_main
 * which then loads its own hub_snap and loops to the next g.
 *
 * Free-threaded Python (3.13t) is required to get real parallelism
 * out of this: each hub thread has its own PyThreadState and runs
 * Python code without contending on a global lock.  On a GIL build
 * this still works correctly but serialises through the GIL.
 *
 * What's NOT in v2:
 *   - cross-hub netpoll: each hub has its own epoll fd; a g that
 *     parks on I/O stays on its hub.  A future version could share
 *     a single epoll across hubs and wake whichever hub is idle.
 *   - sleep-in-hub: runloom_sched_sleep_until still uses the global
 *     scheduler's sleep heap.  Hubs don't process timers.
 *   - park-on-eventfd: today hubs busy-loop trying to steal when
 *     local is empty.  A real impl uses futex / eventfd to sleep.
 */
#if !defined(_WIN32)
#  define _POSIX_C_SOURCE 200809L
#  ifndef _GNU_SOURCE
#    define _GNU_SOURCE
#  endif
#endif
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "plat.h"
#include "plat_compat.h"
#include "runloom_lockrank.h"
#include "mn_sched.h"
#include "runloom_sched.h"
#include "netpoll.h"
#include "io_uring.h"
#include "coro.h"
#include "cldeque.h"
#include "runloom_diag.h"
#include "runloom_gstate.h"
#include "runloom_introspect.h"
#include "runloom_iframe.h"
#include "runloom_stackadvice.h"
#include "runloom_blockpool.h"

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdalign.h>   /* alignas for cache-line padding (B4 / R6) */

#if !defined(RUNLOOM_OS_WINDOWS)
#  include <unistd.h>
#endif

/* Cache-line size for false-sharing avoidance (B4 / R6).  x86-64 and most ARM
 * use 64B lines; Apple M-series and some ARM64 use 128B.  Pad to the larger on
 * arm64 so one layout is false-sharing-free on every target we build for. */
#ifndef RUNLOOM_CACHELINE
#  if defined(__aarch64__) || defined(__arm64__) || defined(_M_ARM64)
#    define RUNLOOM_CACHELINE 128
#  else
#    define RUNLOOM_CACHELINE 64
#  endif
#endif

/* The hub array is PyMem_Calloc'd contiguously, so without per-element
 * alignment hub k's hot writes (deque bottom, the foreign-written submission
 * mailbox) and hub k+1's reads share a cache line -> inter-hub false sharing
 * that worsens with hub count (R6).  alignas on the first member aligns every
 * array element to its own line, killing the inter-hub bounce; alignas on
 * sub_lock and resume_start_ns then splits the two foreign-touched regions
 * within each hub -- the cross-hub submission mailbox (producers write it on
 * every cross-hub submit) and the sysmon/cancel signals -- off the
 * owner-private deque/sched line. */
typedef struct runloom_hub {
    alignas(RUNLOOM_CACHELINE) int id;
    runloom_thread_t thread;
    runloom_sched_t sched;
    runloom_cldeque_t deque;
    PyThreadState *tstate;        /* per-hub tstate */
    volatile int stopping;
    volatile long pending;        /* gs ever-pushed minus gs-completed */
    /* MPSC submission list.  Chase-Lev's `push` is owner-only (single
     * producer); when mn_go runs on a non-owner thread, pushing
     * directly to the deque races with the owner's pop and corrupts
     * the deque's `bottom` counter (a missed RMW), causing
     * non-deterministic segfaults under load.  Instead, producers
     * push to this list under a lock; the hub (the single consumer)
     * drains the list into its own deque each iteration, so all
     * deque pushes are done by the deque's owner thread. */
    alignas(RUNLOOM_CACHELINE) runloom_mutex_t sub_lock;
    runloom_g_t *sub_head;
    runloom_g_t *sub_tail;
    /* BUG #10 throughput: monotonic submission generation, bumped (RELEASE) by
     * runloom_mn_hub_submit right after a push.  An idle hub that owns parked gs
     * (cooperative-lock / channel waiters a cross-hub unlock will hub_submit to
     * it) waits on its per-hub idle condvar (below) before sleeping, so a
     * hand-off signals it awake in ~us instead of after idle_ns (~100us) -- the
     * cap that held the contended cooperative Lock at ~10K ops/s.  Targeted:
     * each hub has its OWN condvar, so there is no shared wake / thundering
     * herd.  sub_gen is the monotonic submit counter (RELEASE-bumped by submit);
     * idle_waiting (set only while parked in the timed wait) is the lock-free
     * hint that lets a submit skip the signal lock when no one is waiting. */
    volatile unsigned sub_gen;
    runloom_mutex_t   idle_lock;
    runloom_cond_t    idle_cond;
    volatile int      idle_waiting;
    /* Controlled-replay deferred publish (barrier mode only).  A cross-hub
     * submit made DURING this hub's execution segment (mn_go / channel-wake from
     * a running fiber) is staged here per TARGET hub, then spliced onto the
     * target's sub_list ATOMICALLY at this segment's release -- so a target's
     * loop-top drain sees the COMPLETE set a segment sent it, never a partial
     * mid-segment snapshot.  This makes the implementation match the documented
     * model (README sec.2 / RunloomMNControl.tla): "work created later by a
     * running segment is published at that segment's release."  Without it the
     * partial snapshot left a target's runq front a function of sub_lock race
     * timing, not the schedule (the deterministic-replay residual).  Indexed by
     * target hub id, touched ONLY by this hub's own thread (no lock).  Allocated
     * to hub_count entries iff the controlled barrier is active; NULL otherwise
     * (the default scheduler never stages -- zero cost). */
    runloom_g_t **stage_head;
    runloom_g_t **stage_tail;
    long          stage_pending;   /* staged gs awaiting release-flush; 0 = skip */
    /* Per-hub io_uring ring.  Created at hub_main entry with
     * IORING_SETUP_SINGLE_ISSUER (and DEFER_TASKRUN if the kernel
     * supports it).  Eventfd registered with the shared netpoll pump.
     * Used by hub-bound recv/send to bypass the global ring's
     * submission mutex and the legacy spin-drain.  NULL if the
     * kernel doesn't have io_uring (5.0 or older) or ring create
     * failed -- callers fall back to the global ring path. */
    runloom_iouring_ring_t *iouring_ring;
    int                  iouring_eventfd;  /* cached for unregister at fini */
    /* io_uring-as-loop backend (RUNLOOM_IOURING_LOOP=1, default off).  When
     * the loop backend is active the hub blocks DIRECTLY in its ring via
     * io_uring_submit_and_wait_timeout instead of epoll_wait, so cross-hub
     * submits (and foreign wakes) must interrupt the ring wait rather than the
     * idle condvar.  loop_wake_fd is a per-hub eventfd poll-added (multishot)
     * into the ring; ring_waiting is the lock-free hint (set only while blocked
     * in the ring wait) that lets a submit skip the eventfd write when the hub
     * is busy.  Both are -1/0 and untouched unless the loop backend is on. */
    int                  loop_wake_fd;
    volatile int         ring_waiting;
    /* Last time this hub ran the idle stack-reclaim sweep (seconds, 0 at
     * init -> first idle sweep fires immediately).  Rate-limits the
     * O(parked) walk under RUNLOOM_STACK_PARK_SWEEP. */
    double               last_sweep_s;
    /* ---- sysmon (Group B) progress instrumentation, RUNLOOM_SYSMON only ----
     * resume_start_ns: monotonic-ns when this hub entered its current
     * runloom_coro_resume; 0 between resumes (idle / looping).  The sysmon
     * watchdog reads it to spot a hub stuck inside a non-yielding blocking
     * call (an UNANTICIPATED block -- the thing Group A's offload doesn't
     * wrap).  resume_g is the g being resumed, for the wedge log line.
     * resume_seq bumps every resume start so the watchdog can tell "same
     * stuck resume" from "made progress" without racing on the ns value.
     * Written by the hub only when runloom_sysmon_enabled (predicted-not-taken
     * off the hot path); read RELAXED by the watchdog (a stale read just
     * delays/!duplicates a report -- harmless for a watchdog). */
    alignas(RUNLOOM_CACHELINE) volatile long long   resume_start_ns;
    volatile long        resume_seq;
    runloom_g_t            *resume_g;
    /* RUNLOOM_PREEMPT: set by the sysmon watchdog when this hub is ATTACHED-wedged
     * (a CPU-bound / non-yielding fiber the DETACHED handoff can't take).
     * runloom's installed eval-frame wrapper reads it at the next Python frame
     * boundary on THIS hub's owner thread and yields the running g back to the
     * scheduler -- Go pre-1.14 cooperative preemption.  Written rarely (only
     * while wedged); read every frame only when RUNLOOM_PREEMPT installed the
     * wrapper (opt-in, so default mode never touches it). */
    volatile int         preempt_requested;
    /* Cross-thread io_uring single-op cancel mailbox.  A hub ring is
     * SINGLE_ISSUER, so a foreign task.cancel cannot submit the ASYNC_CANCEL
     * itself -- it deposits the target op here (CAS NULL->op) and signals
     * idle_cond; THIS hub (the ring's sole issuer) drains it at its loop top
     * and submits the cancel on its own ring.  Single slot: a second concurrent
     * cancel for the same hub is dropped (best-effort -- that fiber still
     * unblocks when its op completes).  See runloom_iouring_cancel_g. */
    void                *iouring_cancel_op;
} runloom_hub_t;

/* B4/R6: enforce that every hub array element starts on its own cache line, so
 * no two hubs ever false-share.  If a future field reorder drops the leading
 * alignas this fails the build rather than silently regressing scaling. */
_Static_assert(alignof(runloom_hub_t) >= RUNLOOM_CACHELINE,
               "runloom_hub_t must be cache-line aligned (B4/R6 false-sharing)");

static runloom_hub_t *runloom_hubs = NULL;
static int runloom_hub_count = 0;
static volatile long runloom_mn_spawn_counter = 0;

/* BUG #10 throughput: is the per-hub idle condvar wake enabled?  Default ON; set
 * RUNLOOM_HUB_IDLE_WAKE=0 to fall back to the plain idle nanosleep (A/B / escape
 * hatch).  An idle hub that owns parked gs waits on its per-hub condvar instead
 * of a plain nanosleep, so a cross-hub hub_submit signals it awake in ~us
 * instead of stranding the woken g for idle_ns (the cap that held the contended
 * cooperative Lock at ~10K ops/s).  The wait is TIMED (idle_ns), so a missed
 * signal degrades to the old latency for one hand-off -- never a hang. */
static int runloom_hub_idle_wake_enabled(void)
{
    static int v = -1;
    int cur = __atomic_load_n(&v, __ATOMIC_RELAXED);
    if (cur < 0) {
        const char *e = getenv("RUNLOOM_HUB_IDLE_WAKE");
        cur = (e != NULL && e[0] == '0') ? 0 : 1;
        __atomic_store_n(&v, cur, __ATOMIC_RELAXED);
    }
    return cur;
}

/* Serializes the hubs' one-time PyThreadState_New at startup.  Each hub creates
 * its OWN tstate on its OWN thread (so biased-refcount + mimalloc bind to the
 * thread that will run it -- see runloom_mn_init), but CPython's new_threadstate()
 * does a check-then-act on interp->gc.immortalize OUTSIDE its HEAD_LOCK, so N
 * hubs calling PyThreadState_New at once race there.  Stock CPython never hits
 * it (threads are spawned serially); this lock restores that serialization.
 * Process-lifetime, init-once (CRITICAL_SECTION can't be statically inited on
 * Windows), never destroyed -- mn_init runs single-threaded so the flag needs
 * no atomics. */
static runloom_mutex_t runloom_hub_tstate_lock;
static int runloom_hub_tstate_lock_inited = 0;

/* Global pending-g counter, replacing the per-hub `pending` field
 * for purposes of "is there any work left in the M:N scheduler".
 * Incremented in runloom_mn_go (spawn), decremented in hub_main when a
 * g completes.  Steals do NOT touch this counter (the per-hub field
 * is still updated for diagnostics / future scheduler heuristics,
 * but the steal-time inc-then-dec across hubs created a
 * sum-observed-as-N-1 window where runloom_mn_run could see total=0
 * and exit while a stolen g was still running on the destination
 * hub).  ACQ_REL on both inc and dec; ACQUIRE on the mn_run read
 * pairs with the completion release. */
static volatile long runloom_mn_pending_global = 0;

/* ---- co-located pending counters ----
 *
 * Every change to a hub's pending count either changes the global
 * counter too (spawn, complete) or rebalances between two hubs (steal).
 * Forwarding through these helpers ensures the per-hub and global
 * counters always move atomically together and a future caller cannot
 * accidentally update one without the other.  See the broader
 * counter-co-location sweep in the diag/gstate session for context. */

/* ---------------------------------------------------------------------------
 * mn_sched.c is split across the mn_sched_*.c.inc fragments below for readability.
 * They are #included here (one translation unit): the fragments share this
 * file's includes, typedefs and file-scope statics and are NOT compiled
 * standalone.  setup.py compiles only mn_sched.c.
 * --------------------------------------------------------------------------- */
#include "mn_sched_runq.c.inc"
#include "mn_sched_hub_resume_preempt.c.inc"
#include "mn_sched_hub_main.c.inc"
#include "mn_sched_mn_api.c.inc"
#include "mn_sched_sysmon.c.inc"
#include "mn_sched_handoff.c.inc"
#include "mn_sched_hubinfo.c.inc"
#include "mn_sched_init_fini.c.inc"
