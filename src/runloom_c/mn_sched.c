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
 * Phase C v2 (this file): yield support inside hubs.  A goroutine
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
#include "mn_sched.h"
#include "runloom_sched.h"
#include "netpoll.h"
#include "io_uring.h"
#include "coro.h"
#include "cldeque.h"
#include "runloom_diag.h"
#include "runloom_gstate.h"
#include "runloom_introspect.h"
#include "runloom_blockpool.h"

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#if !defined(RUNLOOM_OS_WINDOWS)
#  include <unistd.h>
#endif

typedef struct runloom_hub {
    int id;
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
    runloom_mutex_t sub_lock;
    runloom_g_t *sub_head;
    runloom_g_t *sub_tail;
    /* Per-hub io_uring ring.  Created at hub_main entry with
     * IORING_SETUP_SINGLE_ISSUER (and DEFER_TASKRUN if the kernel
     * supports it).  Eventfd registered with the shared netpoll pump.
     * Used by hub-bound recv/send to bypass the global ring's
     * submission mutex and the legacy spin-drain.  NULL if the
     * kernel doesn't have io_uring (5.0 or older) or ring create
     * failed -- callers fall back to the global ring path. */
    runloom_iouring_ring_t *iouring_ring;
    int                  iouring_eventfd;  /* cached for unregister at fini */
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
    volatile long long   resume_start_ns;
    volatile long        resume_seq;
    runloom_g_t            *resume_g;
    /* RUNLOOM_PREEMPT: set by the sysmon watchdog when this hub is ATTACHED-wedged
     * (a CPU-bound / non-yielding goroutine the DETACHED handoff can't take).
     * runloom's installed eval-frame wrapper reads it at the next Python frame
     * boundary on THIS hub's owner thread and yields the running g back to the
     * scheduler -- Go pre-1.14 cooperative preemption.  Written rarely (only
     * while wedged); read every frame only when RUNLOOM_PREEMPT installed the
     * wrapper (opt-in, so default mode never touches it). */
    volatile int         preempt_requested;
} runloom_hub_t;

static runloom_hub_t *runloom_hubs = NULL;
static int runloom_hub_count = 0;
static volatile long runloom_mn_spawn_counter = 0;

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
#include "mn_sched_init_fini.c.inc"
