/* runloom_crash.c -- fatal-signal crash reporter.  See runloom_crash.h. */

#if !defined(_WIN32)
#  define _POSIX_C_SOURCE 200809L
#endif
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "runloom_crash.h"
#include "runloom_diag.h"   /* runloom_evt_crash_dump (flight recorder) */
#include "runloom_introspect.h"
#include "mn_sched.h"
#include "coro.h"
#include "netpoll.h"           /* R5 stats snapshot: parked / fd-armed / heals */
#include "runloom_blockpool.h" /* R5 stats snapshot: blockpool inflight */
#include "io_uring.h"          /* R5 stats snapshot: iouring inflight */
#include "plat.h"
#include "plat_compat.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if !defined(_WIN32)
#  include <signal.h>
#  include <unistd.h>
#  include <fcntl.h>
#  include <errno.h>
#  include <time.h>
#  include <pthread.h>
#  include <sys/types.h>
#  include <sys/mman.h>
#  if defined(__has_include)
#    if __has_include(<execinfo.h>)
#      include <execinfo.h>
#      define RUNLOOM_HAVE_EXECINFO 1
#    endif
#    if __has_include(<sys/prctl.h>)
#      include <sys/prctl.h>
#      define RUNLOOM_HAVE_PRCTL 1
#    endif
#    if __has_include(<sys/wait.h>)
#      include <sys/wait.h>
#    endif
#  else
#    include <sys/wait.h>
#  endif
#endif

/* R5 crash-telemetry build identity.  RUNLOOM_CRASH_VERSION can be overridden
 * by setup.py (-DRUNLOOM_CRASH_VERSION=\"x.y.z\"); defaults to "dev".  The
 * build-flags string records the toolchain conditions that most change runtime
 * behaviour (sanitizers, NDEBUG) so a field report is unambiguous. */
#ifndef RUNLOOM_CRASH_VERSION
#  define RUNLOOM_CRASH_VERSION "dev"
#endif
#if defined(__SANITIZE_ADDRESS__)
#  define RUNLOOM_CRASH_BF_SAN "  [ASan]"
#elif defined(__SANITIZE_THREAD__)
#  define RUNLOOM_CRASH_BF_SAN "  [TSan]"
#else
#  define RUNLOOM_CRASH_BF_SAN ""
#endif
#if defined(NDEBUG)
#  define RUNLOOM_CRASH_BF_NDEBUG ""
#else
#  define RUNLOOM_CRASH_BF_NDEBUG "  [assert]"
#endif
#define RUNLOOM_CRASH_BUILDFLAGS RUNLOOM_CRASH_BF_SAN RUNLOOM_CRASH_BF_NDEBUG

/* ---------------------------------------------------------------- *
 *  Shared state                                                    *
 * ---------------------------------------------------------------- */
static int runloom_crash_flags_v   = 0;
static int runloom_crash_on        = 0;    /* installed? (atomic) */
static int runloom_crash_report_fd = -1;   /* extra report file, or -1 */

#if !defined(_WIN32)

/* The fatal signals we install on.  SIGSEGV / SIGBUS are the headline (a
 * fiber stack overflow lands in a guard page -> SIGSEGV); the rest are
 * crashes worth a dump.  SIGABRT is special on the chain-out (re-raise, the
 * faulting-instruction re-execution trick does not apply to a raised signal). */
static const int runloom_crash_signals[] = {
    SIGSEGV, SIGBUS, SIGILL, SIGFPE, SIGABRT
};
#define RUNLOOM_CRASH_NSIG \
    ((int)(sizeof runloom_crash_signals / sizeof runloom_crash_signals[0]))

static struct sigaction runloom_crash_prev[RUNLOOM_CRASH_NSIG];
static struct sigaction runloom_crash_prev_cont;   /* SIGCONT (wait-release) */
static int  runloom_crash_cont_saved = 0;
static long runloom_crash_wait_secs = 0;   /* RUNLOOM_CRASH_WAIT_SECS snapshot */

static volatile sig_atomic_t runloom_crash_in_progress = 0;
static volatile sig_atomic_t runloom_crash_wait_release = 0;

/* Per-thread altstack bookkeeping (so disarm can unmap on thread exit). */
static RUNLOOM_TLS int    runloom_crash_armed     = 0;
static RUNLOOM_TLS void  *runloom_crash_alt_base  = NULL;
static RUNLOOM_TLS size_t runloom_crash_alt_total = 0;

/* ---------------------------------------------------------------- *
 *  Async-signal-safe-ish emit (write + snprintf only, no malloc)   *
 * ---------------------------------------------------------------- */
static void crash_write(int fd, const char *s, size_t n)
{
    ssize_t off = 0;
    while ((size_t)off < n) {
        ssize_t w = write(fd, s + off, n - off);
        if (w <= 0) break;
        off += w;
    }
}

static void crash_emit(const char *s)
{
    size_t n = strlen(s);
    crash_write(2, s, n);
    if (runloom_crash_report_fd >= 0) crash_write(runloom_crash_report_fd, s, n);
}

static void crash_emitf(const char *fmt, ...)
{
    char buf[512];
    int m;
    va_list ap;
    va_start(ap, fmt);
    m = vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);
    if (m > 0) {
        if ((size_t)m >= sizeof buf) m = (int)sizeof buf - 1;
        crash_write(2, buf, (size_t)m);
        if (runloom_crash_report_fd >= 0)
            crash_write(runloom_crash_report_fd, buf, (size_t)m);
    }
}

static const char *crash_sig_name(int sig)
{
    switch (sig) {
    case SIGSEGV: return "SIGSEGV";
    case SIGBUS:  return "SIGBUS";
    case SIGILL:  return "SIGILL";
    case SIGFPE:  return "SIGFPE";
    case SIGABRT: return "SIGABRT";
    default:      return "signal";
    }
}

static int crash_sig_index(int sig)
{
    int i;
    for (i = 0; i < RUNLOOM_CRASH_NSIG; i++)
        if (runloom_crash_signals[i] == sig) return i;
    return -1;
}

/* ---------------------------------------------------------------- *
 *  Per-thread sigaltstack                                          *
 * ---------------------------------------------------------------- */
static size_t crash_altstack_size(void)
{
    size_t want = (size_t)64 * 1024;
    long ps;
    size_t page;
#if defined(_SC_SIGSTKSZ)
    {
        long v = sysconf(_SC_SIGSTKSZ);
        if (v > 0 && (size_t)v > want) want = (size_t)v;
    }
#elif defined(SIGSTKSZ)
    if ((size_t)SIGSTKSZ > want) want = (size_t)SIGSTKSZ;
#endif
    ps = sysconf(_SC_PAGESIZE);
    page = (ps > 0) ? (size_t)ps : (size_t)4096;
    return (want + page - 1) & ~(page - 1);
}

void runloom_crash_thread_arm(void)
{
    long ps;
    size_t page, total;
    void *base;
    stack_t ss;

    if (!__atomic_load_n(&runloom_crash_on, __ATOMIC_ACQUIRE)) return;
    if (runloom_crash_armed) return;

    ps = sysconf(_SC_PAGESIZE);
    page = (ps > 0) ? (size_t)ps : (size_t)4096;
    total = crash_altstack_size() + page;   /* +1 guard page below the altstack */

    base = mmap(NULL, total, PROT_READ | PROT_WRITE,
                MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (base == MAP_FAILED) return;
    (void)mprotect(base, page, PROT_NONE);   /* trap an altstack overflow too */

    ss.ss_sp    = (char *)base + page;
    ss.ss_size  = total - page;
    ss.ss_flags = 0;
    if (sigaltstack(&ss, NULL) != 0) {
        munmap(base, total);
        return;
    }
    runloom_crash_alt_base  = base;
    runloom_crash_alt_total = total;
    runloom_crash_armed     = 1;
}

void runloom_crash_thread_disarm(void)
{
    stack_t ss;
    if (!runloom_crash_armed) return;
    ss.ss_sp    = NULL;
    ss.ss_size  = 0;
    ss.ss_flags = SS_DISABLE;
    (void)sigaltstack(&ss, NULL);
    if (runloom_crash_alt_base != NULL)
        munmap(runloom_crash_alt_base, runloom_crash_alt_total);
    runloom_crash_alt_base  = NULL;
    runloom_crash_alt_total = 0;
    runloom_crash_armed     = 0;
}

void runloom_crash_reset_after_fork(void)
{
    /* Single-threaded child: the surviving thread keeps its inherited altstack
     * and the process-wide handler.  Just clear the latch in case the fork
     * raced a crash on another (now-dead) thread. */
    runloom_crash_in_progress = 0;
    runloom_crash_wait_release = 0;
}

/* ---------------------------------------------------------------- *
 *  Debugger integration                                            *
 * ---------------------------------------------------------------- */
static void crash_cont_handler(int sig)
{
    (void)sig;
    runloom_crash_wait_release = 1;   /* `kill -CONT <pid>` releases the wait */
}

static void crash_wait_for_debugger(void)
{
    long pid = (long)getpid();
    long waited_ms = 0;
    crash_emitf(
        "\n[runloom] paused for a debugger. Attach to live state with:\n"
        "    gdb -p %ld        (or)        lldb -p %ld\n"
        "  then `continue`, or run `kill -CONT %ld` from another shell to resume.\n",
        pid, pid, pid);
    runloom_crash_wait_release = 0;
    while (!runloom_crash_wait_release) {
        struct timespec ts;
        ts.tv_sec  = 0;
        ts.tv_nsec = 200L * 1000L * 1000L;   /* 200 ms */
        nanosleep(&ts, NULL);
        waited_ms += 200;
        if (runloom_crash_wait_secs > 0 &&
            waited_ms >= runloom_crash_wait_secs * 1000) {
            crash_emit("[runloom] debugger wait timed out; continuing.\n");
            break;
        }
    }
}

static void crash_spawn_gdb(void)
{
    char pidbuf[24];
    pid_t child;
    snprintf(pidbuf, sizeof pidbuf, "%ld", (long)getpid());
    crash_emit("\n[runloom] launching gdb (thread apply all bt full) ...\n");
    child = fork();
    if (child == 0) {
        char *const argv[] = {
            (char *)"gdb", (char *)"-p", pidbuf, (char *)"-batch", (char *)"-nx",
            (char *)"-ex", (char *)"set pagination off",
            (char *)"-ex", (char *)"thread apply all bt full",
            (char *)"-ex", (char *)"detach",
            (char *)"-ex", (char *)"quit",
            (char *)NULL
        };
        execvp("gdb", argv);
        _exit(127);
    } else if (child > 0) {
        int st;
        while (waitpid(child, &st, 0) < 0 && errno == EINTR) { /* retry */ }
    }
}

/* ---------------------------------------------------------------- *
 *  The handler                                                     *
 * ---------------------------------------------------------------- */
/* R5 build + runtime snapshot (docs/dev/RELIABILITY_PROGRAM.md).  Emit the
 * build identity + backends + the R0 gauge snapshot.  Every value is an
 * async-signal-safe read: a compile-time string, a backend id string, or a
 * lock-free atomic-load gauge accessor (the SAME R0 accessors stats() uses) --
 * NO Python, NO malloc, NO lock a pump holds.  This is what makes a pasted
 * crash/hang artifact diagnosable without a reproduction.  Shared by the crash
 * handler and the self-hang watchdog. */
static void crash_emit_snapshot(void)
{
    crash_emit("[runloom] --- build + runtime snapshot ---\n");
    crash_emitf("[runloom]   version %s  built %s %s\n",
                RUNLOOM_CRASH_VERSION, __DATE__, __TIME__);
    crash_emitf("[runloom]   backends: coro=%s netpoll=%s%s\n",
                runloom_coro_backend(), runloom_netpoll_backend(),
                RUNLOOM_CRASH_BUILDFLAGS);
    crash_emitf("[runloom]   gs: total=%ld pending=%ld completed=%lld hubs=%d\n",
                runloom_greg_total_count(), runloom_mn_pending_total(),
                runloom_mn_completed_total(), runloom_mn_hub_count());
    crash_emitf("[runloom]   stacks: live=%ld depot=%ld\n",
                runloom_coro_stack_live(), runloom_coro_depot_pooled());
    crash_emitf("[runloom]   netpoll: parked=%d heap=%d fd_armed=%d heals=%llu\n",
                runloom_netpoll_parked_count(),
                runloom_netpoll_deadline_heap_total(),
                runloom_netpoll_fd_armed_count(),
                runloom_netpoll_stale_arm_heals());
    crash_emitf("[runloom]   inflight: blockpool=%ld iouring=%d\n",
                runloom_blockpool_inflight(), runloom_iouring_inflight());
}

/* ---------------------------------------------------------------- *
 *  R5 self-hang watchdog (RUNLOOM_WATCHDOG=secs)                    *
 * ---------------------------------------------------------------- *
 * A detached native thread that snapshots the SAME artifact -- WITHOUT aborting
 * -- when the runtime stops making progress for N seconds while work is still
 * outstanding.  The field equivalent of hang_hunter's gdb capture: a wedge that
 * used to be a silent "the server just stopped responding" becomes a
 * pinpointed, pasteable hang report.
 *
 * Progress signal: runloom_mn_completed_total() -- fibers RETIRING (R0, already
 * maintained; zero new hot-path cost).  Guarded by "work outstanding" (pending
 * or parked > 0) so a legitimately IDLE process (nothing to do) never trips it.
 * So it fires exactly on: work is outstanding, yet none has retired for N
 * seconds -> a wedge (deadlock / lost wake / a hub frozen off the scheduler).
 *
 * SCOPE: tuned for continuously-active workloads (the soak/canary that R5-R6
 * run).  A workload whose fibers are long-lived by design (a pure keepalive
 * server that rarely completes a fiber) can look stalled while healthy -- set
 * RUNLOOM_WATCHDOG high, or leave it off, for such a service.  One snapshot per
 * hang episode (re-arms only after progress resumes), so a persistent wedge
 * does not spam. */
static volatile int  runloom_watchdog_secs = 0;
static volatile int  runloom_watchdog_on   = 0;

static int runloom_watchdog_work_outstanding(void)
{
    return runloom_mn_pending_total() > 0 ||
           runloom_netpoll_parked_count() > 0;
}

static void *runloom_watchdog_main(void *arg)
{
    int secs = runloom_watchdog_secs;
    long long last_completed = -1;
    double stalled_since = 0.0;      /* monotonic seconds; 0 = not stalling */
    int reported = 0;
    (void)arg;
    for (;;) {
        struct timespec nap = { 1, 0 };   /* poll cadence: 1 s */
        nanosleep(&nap, NULL);
        if (!runloom_watchdog_on) return NULL;
        {
            long long now_completed = runloom_mn_completed_total();
            double now = (double)runloom_monotonic_ns() / 1e9;
            if (now_completed != last_completed) {
                /* progress happened -> reset the stall clock + re-arm. */
                last_completed = now_completed;
                stalled_since = 0.0;
                reported = 0;
                continue;
            }
            if (!runloom_watchdog_work_outstanding()) {
                /* no progress but also no outstanding work -> genuinely idle. */
                stalled_since = 0.0;
                continue;
            }
            if (stalled_since == 0.0) {
                stalled_since = now;
                continue;
            }
            if (!reported && (now - stalled_since) >= (double)secs) {
                /* WEDGE: outstanding work, no completion for `secs`.  Emit the
                 * hang artifact -- snapshot + fiber dump + flight recorder --
                 * WITHOUT aborting (the process may still recover; we only
                 * observe).  Serialise against the crash handler so a real
                 * crash mid-report does not interleave. */
                if (__atomic_exchange_n(&runloom_crash_in_progress, 1,
                                        __ATOMIC_ACQ_REL) == 0) {
                    crash_emit("\n===================== runloom HANG (watchdog) "
                               "=====================\n");
                    crash_emitf("[runloom] no fiber has completed in %ds while "
                                "work is outstanding (pid %ld) -- likely a "
                                "deadlock / lost wake / frozen hub.\n",
                                secs, (long)getpid());
                    crash_emit_snapshot();
                    if (runloom_crash_flags_v & RUNLOOM_CRASH_GOROUTINES) {
                        runloom_dump_fibers_fd(2);
                        if (runloom_crash_report_fd >= 0)
                            runloom_dump_fibers_fd(runloom_crash_report_fd);
                    }
                    runloom_evt_crash_dump(2, 24);
                    if (runloom_crash_report_fd >= 0)
                        runloom_evt_crash_dump(runloom_crash_report_fd, 24);
                    crash_emit("[runloom] (watchdog observed only; not aborting) "
                               "==================\n");
                    __atomic_store_n(&runloom_crash_in_progress, 0,
                                     __ATOMIC_RELEASE);
                }
                reported = 1;   /* one report per episode */
            }
        }
    }
}

/* Start the watchdog (idempotent).  secs<=0 disables.  Called from
 * install_crash_handler when RUNLOOM_WATCHDOG is set, or explicitly. */
void runloom_watchdog_start(int secs)
{
    pthread_t th;
    if (secs <= 0) { runloom_watchdog_on = 0; return; }
    if (__atomic_exchange_n(&runloom_watchdog_on, 1, __ATOMIC_ACQ_REL) != 0)
        return;   /* already running */
    runloom_watchdog_secs = secs;
    if (pthread_create(&th, NULL, runloom_watchdog_main, NULL) != 0) {
        runloom_watchdog_on = 0;
        return;
    }
    pthread_detach(th);
}

static void crash_handler(int sig, siginfo_t *si, void *uctx)
{
    int   idx  = crash_sig_index(sig);
    void *addr = (si != NULL) ? si->si_addr : NULL;
    (void)uctx;

    /* Serialise: only the first faulting thread drives the dump.  A second
     * concurrent fault (or a re-fault while we dump) parks until the owner
     * re-raises the default disposition and terminates the process -- this
     * keeps the report from interleaving and prevents infinite recursion. */
    if (__atomic_exchange_n(&runloom_crash_in_progress, 1, __ATOMIC_ACQ_REL) != 0) {
        for (;;) pause();
    }

    /* We are the crash owner.  Freeze the M:N watchdog FIRST -- before the
     * (potentially slow) dump dwells long enough for sysmon to flag this hub
     * wedged and preempt the faulting fiber, which would stop the chain-out
     * from re-faulting and coring (the process would limp on with a stranded
     * hub instead of dying cleanly). */
    runloom_sched_freeze_for_crash();

    crash_emit("\n======================== runloom crash ========================\n");
    if (sig == SIGSEGV || sig == SIGBUS)
        crash_emitf("[runloom] fatal %s at address %p", crash_sig_name(sig), addr);
    else
        crash_emitf("[runloom] fatal %s", crash_sig_name(sig));
    crash_emitf("  (pid %ld, thread 0x%lx)\n",
                (long)getpid(), (unsigned long)pthread_self());

    /* R5 field telemetry: build identity + backends + the R0 gauge snapshot,
     * emitted RIGHT AFTER the header (so it lands even if a later step hangs).
     * Shared with the self-hang watchdog. */
    crash_emit_snapshot();

    /* Classify the fault against the per-fiber guard pages. */
    {
        int kind = 0;
        unsigned skib = 0;
        long long gid = (addr != NULL)
                      ? runloom_fiber_for_addr(addr, &kind, &skib) : 0;
        if (kind == 1) {
            /* The long lines go through crash_emit (strlen + write, no fixed
             * buffer); only the goid line needs the formatting buffer. */
            crash_emit("[runloom] >>> GOROUTINE STACK OVERFLOW <<<\n");
            crash_emitf(
                "[runloom]     fiber g%lld ran off the low end of its %u KiB C stack\n",
                gid, skib);
            crash_emit(
                "[runloom]     -- the fault hit the guard page just below it: a CLEAN trap,\n"
                "[runloom]     not memory corruption.\n"
                "[runloom]     Fix: pin a bigger stack with runloom.fiber(fn, stack_size=N)\n"
                "[runloom]     (an explicit size ALWAYS wins over the auto-sizer), or flatten\n"
                "[runloom]     the deep native recursion to a heap stack / offload it.  If this\n"
                "[runloom]     fiber's depth varies with INPUT, pin it -- the auto-sizer\n"
                "[runloom]     sizes from the runs it has seen and can under-size a deeper path\n"
                "[runloom]     a later input reaches.  See docs/stack-sizing.md.\n");
        } else if (kind == 2) {
            crash_emitf(
                "[runloom] fault is INSIDE fiber g%lld's %u KiB stack (not the guard\n",
                gid, skib);
            crash_emit(
                "[runloom]     page) -- likely a wild pointer / use-after-free in code\n"
                "[runloom]     running on that fiber.\n");
        } else {
            crash_emit(
                "[runloom] fault is not in any fiber stack (main/hub stack, the heap,\n"
                "[runloom]     or a wild pointer).\n");
        }
    }

    /* What this thread was running (may differ from the address-mapped g if the
     * fault address is a wild pointer rather than this g's own stack). */
    {
        runloom_g_t *cur = runloom_mn_tls_current_g();
        if (cur != NULL)
            crash_emitf("[runloom] this thread was executing fiber g%lld.\n",
                        (long long)runloom_g_id(cur));
    }

    /* Full fiber registry dump (async-signal-safe; try-locked). */
    if (runloom_crash_flags_v & RUNLOOM_CRASH_GOROUTINES) {
        runloom_dump_fibers_fd(2);
        if (runloom_crash_report_fd >= 0)
            runloom_dump_fibers_fd(runloom_crash_report_fd);
    }

#if defined(RUNLOOM_HAVE_EXECINFO)
    if (runloom_crash_flags_v & RUNLOOM_CRASH_BACKTRACE) {
        void *bt[64];
        int n = backtrace(bt, 64);
        crash_emit("\n[runloom] native backtrace (faulting thread):\n");
        backtrace_symbols_fd(bt, n, 2);
        if (runloom_crash_report_fd >= 0)
            backtrace_symbols_fd(bt, n, runloom_crash_report_fd);
    }
#endif

    /* Flight recorder (determinism tooling #1): the recent scheduler-transition
     * timeline that led here.  No-op unless RUNLOOM_DEBUG=ring was enabled. */
    runloom_evt_crash_dump(2, 24);
    if (runloom_crash_report_fd >= 0)
        runloom_evt_crash_dump(runloom_crash_report_fd, 24);

    if (runloom_crash_flags_v & RUNLOOM_CRASH_GDB)  crash_spawn_gdb();
    if (runloom_crash_flags_v & RUNLOOM_CRASH_WAIT) crash_wait_for_debugger();

    crash_emit("[runloom] chaining to the default handler"
               " (a Python traceback may follow) ...\n");
    crash_emit("===============================================================\n");

    /* Chain out: restore the previously-installed disposition (faulthandler ->
     * prints the Python traceback then cores, or SIG_DFL -> cores) and let the
     * fault re-trigger.  For a synchronous fault, returning re-executes the
     * faulting instruction under the restored disposition; SIGABRT came from a
     * raise()/abort() that we'd return PAST, so re-raise it explicitly. */
    if (idx >= 0) sigaction(sig, &runloom_crash_prev[idx], NULL);
    if (sig == SIGABRT) {
        sigset_t s;
        sigemptyset(&s);
        sigaddset(&s, sig);
        sigprocmask(SIG_UNBLOCK, &s, NULL);
        raise(sig);
    }
}

/* ---------------------------------------------------------------- *
 *  Install / uninstall (POSIX)                                     *
 * ---------------------------------------------------------------- */
int runloom_crash_install(int flags, const char *report_path)
{
    int i;
    /* Idempotent re-install: a second runloom_crash_install (e.g. runloom's
     * package __init__ auto-installs from $RUNLOOM_CRASH, then the app calls
     * install_crash_handler() to set a level/file) must NOT re-capture the
     * "previous" dispositions -- doing so saves OUR OWN crash_handler as the
     * chain-out target, so on a real fault the handler restores itself and
     * re-faults straight back into its re-entrancy pause() guard: the process
     * wedges (a stranded hub, no core) instead of coring + dying.  Keep the
     * dispositions captured by the FIRST install (the true originals, normally
     * SIG_DFL / faulthandler). */
    int already = __atomic_load_n(&runloom_crash_on, __ATOMIC_ACQUIRE);
    if (flags == 0) flags = RUNLOOM_CRASH_DEFAULT;

    if (report_path != NULL && report_path[0] != '\0') {
        int fd = open(report_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            /* The caller explicitly asked for a crash report file and we could
             * not open it (e.g. the parent dir doesn't exist).  Surface the
             * failure (errno is set by open) rather than silently dropping it
             * and installing without the file -- m_install_crash_handler turns
             * the -1 into OSError, matching install_traceback_signal's contract
             * for a bad argument.  Returned BEFORE touching any signal state. */
            return -1;
        }
        if (runloom_crash_report_fd >= 0 && runloom_crash_report_fd != 2)
            close(runloom_crash_report_fd);
        runloom_crash_report_fd = fd;
    }

    {
        const char *s = getenv("RUNLOOM_CRASH_WAIT_SECS");
        runloom_crash_wait_secs = (s != NULL) ? atol(s) : 0;
    }

    /* Option D: enable faulthandler FIRST so our handler -- installed after,
     * hence outermost -- runs the fiber dump and then chains out to
     * faulthandler for the Python traceback. */
    if (flags & RUNLOOM_CRASH_PYSTACK) {
        PyObject *fh = PyImport_ImportModule("faulthandler");
        if (fh != NULL) {
            PyObject *r = PyObject_CallMethod(fh, "enable", NULL);
            Py_XDECREF(r);
            Py_DECREF(fh);
        }
        PyErr_Clear();
    }

#if defined(RUNLOOM_HAVE_PRCTL) && defined(PR_SET_PTRACER)
    /* Let a forked child (auto-gdb) or an external debugger attach under a
     * restrictive Yama ptrace_scope. */
    if (flags & (RUNLOOM_CRASH_GDB | RUNLOOM_CRASH_WAIT)) {
#  if defined(PR_SET_PTRACER_ANY)
        prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, 0, 0, 0);
#  else
        prctl(PR_SET_PTRACER, (unsigned long)-1, 0, 0, 0);
#  endif
    }
#endif

    if ((flags & RUNLOOM_CRASH_WAIT) && !runloom_crash_cont_saved) {
        struct sigaction sc;
        memset(&sc, 0, sizeof sc);
        sc.sa_handler = crash_cont_handler;
        sigemptyset(&sc.sa_mask);
        sc.sa_flags = SA_RESTART;
        if (sigaction(SIGCONT, &sc, &runloom_crash_prev_cont) == 0)
            runloom_crash_cont_saved = 1;
    }

    runloom_crash_flags_v = flags;

    for (i = 0; i < RUNLOOM_CRASH_NSIG; i++) {
        struct sigaction sa;
        int j;
        memset(&sa, 0, sizeof sa);
        sa.sa_sigaction = crash_handler;
        sigemptyset(&sa.sa_mask);
        /* Block the other fatal signals while handling one (belt-and-braces
         * with the in_progress latch). */
        for (j = 0; j < RUNLOOM_CRASH_NSIG; j++)
            sigaddset(&sa.sa_mask, runloom_crash_signals[j]);
        sa.sa_flags = SA_SIGINFO | SA_ONSTACK | SA_RESTART;
        /* Only capture the previous disposition on the FIRST install (see the
         * `already` note above); a re-install must preserve it. */
        (void)sigaction(runloom_crash_signals[i], &sa,
                        already ? NULL : &runloom_crash_prev[i]);
    }

    __atomic_store_n(&runloom_crash_on, 1, __ATOMIC_RELEASE);
    runloom_crash_thread_arm();   /* arm the installing (main) thread now */
    /* R5: auto-start the self-hang watchdog if RUNLOOM_WATCHDOG=<secs> is set.
     * It reuses the crash report fd + goroutine-dump flag installed here. */
    {
        const char *wd = getenv("RUNLOOM_WATCHDOG");
        if (wd != NULL && wd[0] != '\0') {
            int secs = atoi(wd);
            if (secs > 0) runloom_watchdog_start(secs);
        }
    }
    return 0;
}

void runloom_crash_uninstall(void)
{
    int i;
    if (!__atomic_load_n(&runloom_crash_on, __ATOMIC_ACQUIRE)) return;
    for (i = 0; i < RUNLOOM_CRASH_NSIG; i++)
        (void)sigaction(runloom_crash_signals[i], &runloom_crash_prev[i], NULL);
    if (runloom_crash_cont_saved) {
        (void)sigaction(SIGCONT, &runloom_crash_prev_cont, NULL);
        runloom_crash_cont_saved = 0;
    }
    __atomic_store_n(&runloom_crash_on, 0, __ATOMIC_RELEASE);
    if (runloom_crash_report_fd >= 0 && runloom_crash_report_fd != 2) {
        close(runloom_crash_report_fd);
        runloom_crash_report_fd = -1;
    }
}

#else /* _WIN32 ----------------------------------------------------- */

/* Minimal Windows path: a Vectored Exception Handler that dumps the fiber
 * registry on an access violation / stack overflow, then continues the search
 * so the OS still produces the crash.  (No sigaltstack equivalent yet, so a
 * true stack overflow may be unable to run this; the rich path is POSIX.) */
static void *runloom_crash_veh_handle = NULL;

static LONG WINAPI runloom_crash_veh(EXCEPTION_POINTERS *ep)
{
    DWORD code = (ep && ep->ExceptionRecord) ? ep->ExceptionRecord->ExceptionCode : 0;
    if (code == EXCEPTION_ACCESS_VIOLATION || code == EXCEPTION_STACK_OVERFLOW) {
        if (runloom_crash_flags_v & RUNLOOM_CRASH_GOROUTINES)
            runloom_dump_fibers_fd(2);
    }
    return EXCEPTION_CONTINUE_SEARCH;
}

int runloom_crash_install(int flags, const char *report_path)
{
    (void)report_path;
    if (flags == 0) flags = RUNLOOM_CRASH_DEFAULT;
    if (flags & RUNLOOM_CRASH_PYSTACK) {
        PyObject *fh = PyImport_ImportModule("faulthandler");
        if (fh != NULL) {
            PyObject *r = PyObject_CallMethod(fh, "enable", NULL);
            Py_XDECREF(r);
            Py_DECREF(fh);
        }
        PyErr_Clear();
    }
    runloom_crash_flags_v = flags;
    if (runloom_crash_veh_handle == NULL)
        runloom_crash_veh_handle = AddVectoredExceptionHandler(1, runloom_crash_veh);
    __atomic_store_n(&runloom_crash_on, 1, __ATOMIC_RELEASE);
    return 0;
}

void runloom_crash_uninstall(void)
{
    if (runloom_crash_veh_handle != NULL) {
        RemoveVectoredExceptionHandler(runloom_crash_veh_handle);
        runloom_crash_veh_handle = NULL;
    }
    __atomic_store_n(&runloom_crash_on, 0, __ATOMIC_RELEASE);
}

void runloom_crash_thread_arm(void)      { /* no altstack on Windows yet */ }
void runloom_crash_thread_disarm(void)   { }
void runloom_crash_reset_after_fork(void){ }

#endif /* _WIN32 */

/* ---------------------------------------------------------------- *
 *  Shared helpers                                                  *
 * ---------------------------------------------------------------- */
int runloom_crash_installed(void)
{
    return __atomic_load_n(&runloom_crash_on, __ATOMIC_ACQUIRE);
}

int runloom_crash_parse_flags(const char *s)
{
    char buf[160];
    size_t i;
    int f = 0;
    if (s == NULL || s[0] == '\0') return RUNLOOM_CRASH_DEFAULT;
    for (i = 0; s[i] != '\0' && i < sizeof buf - 1; i++) {
        char c = s[i];
        if (c >= 'A' && c <= 'Z') c = (char)(c - 'A' + 'a');
        buf[i] = c;
    }
    buf[i] = '\0';
    if (strstr(buf, "off") != NULL || strcmp(buf, "0") == 0) return -1;
    if (strstr(buf, "all") != NULL)        f |= RUNLOOM_CRASH_ALL;
    if (strstr(buf, "fiber") != NULL)  f |= RUNLOOM_CRASH_GOROUTINES;
    if (strstr(buf, "backtrace") != NULL ||
        strstr(buf, "native") != NULL)     f |= RUNLOOM_CRASH_BACKTRACE;
    if (strstr(buf, "py") != NULL)         f |= RUNLOOM_CRASH_PYSTACK;
    if (strstr(buf, "wait") != NULL)       f |= RUNLOOM_CRASH_WAIT;
    if (strstr(buf, "gdb") != NULL)        f |= RUNLOOM_CRASH_GDB;
    if (f == 0) f = RUNLOOM_CRASH_DEFAULT;   /* "on"/"1"/unknown -> default */
    else        f |= RUNLOOM_CRASH_GOROUTINES;  /* always include the dump */
    return f;
}

/* ---------------------------------------------------------------- *
 *  Test-only fault injection                                       *
 *                                                                  *
 *  Deterministically overflow the CURRENT C stack via unbounded    *
 *  real-C recursion -- unlike Python recursion (which CPython's own *
 *  C-recursion guard catches with a RecursionError), this runs off  *
 *  the low end of the stack and into the guard page.  Run from      *
 *  inside a fiber to exercise the per-fiber-overflow path.  *
 *  Used only by tests/test_crash_handler.py.                       *
 * ---------------------------------------------------------------- */
volatile char runloom_crash_selftest_sink = 0;

/* The unbounded recursion is the whole point -- silence the (correct) warning. */
#if defined(__GNUC__) && !defined(__clang__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Winfinite-recursion"
#endif
#if defined(__GNUC__)
__attribute__((noinline))
#endif
static void runloom_crash_recurse(int depth)
{
    volatile char pad[512];
    pad[0] = (char)depth;
    runloom_crash_selftest_sink = pad[0];
    runloom_crash_recurse(depth + 1);
    /* Use pad AFTER the call so the frame can't be tail-call-eliminated. */
    runloom_crash_selftest_sink = pad[sizeof pad - 1];
}
#if defined(__GNUC__) && !defined(__clang__)
#  pragma GCC diagnostic pop
#endif

void runloom_crash_selftest_overflow(void)
{
    runloom_crash_recurse(0);
}
