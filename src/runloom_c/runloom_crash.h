/* runloom_crash.h -- fatal-signal crash reporter.
 *
 * Turns a SIGSEGV / SIGBUS (and optionally SIGILL / SIGFPE / SIGABRT) into a
 * structured dump instead of a silent core:
 *
 *   - the faulting address is CLASSIFIED by mapping it onto the per-goroutine
 *     PROT_NONE guard pages installed in coro.c -- a fault in a guard page is
 *     reported as a GOROUTINE STACK OVERFLOW naming the goroutine and its stack
 *     size; a fault inside a usable goroutine stack as a wild pointer / UAF on
 *     that goroutine; anything else as a non-goroutine fault;
 *   - the full live-goroutine registry is dumped (the same async-signal-safe
 *     dump as the SIGQUIT handler);
 *   - an optional native C backtrace (execinfo) and Python traceback (by
 *     enabling faulthandler and chaining out to it) are emitted;
 *   - the process can optionally WAIT for a debugger to attach, or fork+exec
 *     gdb on itself, before chaining to the default disposition so a core is
 *     still produced and the exit code stays correct.
 *
 * Survives a blown goroutine stack: every runloom thread installs its own
 * sigaltstack (runloom_crash_thread_arm, wired into runloom_coro_thread_init
 * and the blockpool workers), so the handler runs even when the fault IS the
 * stack overflow.
 *
 * POSIX has the rich path.  On Windows a Vectored Exception Handler does the
 * goroutine dump and continues the search (the OS still produces the crash).
 */
#ifndef RUNLOOM_CRASH_H
#define RUNLOOM_CRASH_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Behaviour / verbosity flags (bitmask). */
enum {
    RUNLOOM_CRASH_GOROUTINES = 1 << 0,  /* dump the live-goroutine registry */
    RUNLOOM_CRASH_BACKTRACE  = 1 << 1,  /* native C backtrace (execinfo) */
    RUNLOOM_CRASH_PYSTACK    = 1 << 2,  /* Python traceback (enables faulthandler) */
    RUNLOOM_CRASH_WAIT       = 1 << 3,  /* block for a debugger to attach */
    RUNLOOM_CRASH_GDB        = 1 << 4   /* fork+exec gdb -batch on self */
};
#define RUNLOOM_CRASH_DEFAULT (RUNLOOM_CRASH_GOROUTINES)
#define RUNLOOM_CRASH_ALL     (RUNLOOM_CRASH_GOROUTINES | RUNLOOM_CRASH_BACKTRACE | \
                               RUNLOOM_CRASH_PYSTACK)

/* Install the process-wide fatal-signal handler.  flags = bitmask above (0 ->
 * RUNLOOM_CRASH_DEFAULT).  report_path: a file to ALSO append the report to
 * besides stderr, or NULL.  Idempotent; chains to any previously installed
 * handler (incl. faulthandler).  Arms the calling thread's sigaltstack.
 * Returns 0, or -1 with errno set.  Call with the GIL held (it may enable
 * faulthandler). */
int  runloom_crash_install(int flags, const char *report_path);

/* Restore the previous dispositions and stop reporting. */
void runloom_crash_uninstall(void);

/* 1 once installed, else 0. */
int  runloom_crash_installed(void);

/* Per-thread sigaltstack arm / disarm.  Idempotent; both no-op unless the
 * handler is installed.  Wired into runloom_coro_thread_init / _fini and the
 * blockpool worker loop so every runloom OS thread is covered. */
void runloom_crash_thread_arm(void);
void runloom_crash_thread_disarm(void);

/* After fork(): clear the in-progress latch (the surviving thread keeps its
 * inherited altstack and the process-wide handler).  Chained from
 * runloom_after_fork_child. */
void runloom_crash_reset_after_fork(void);

/* Parse a RUNLOOM_CRASH token string ("on"/"all"/"backtrace"/"pystack"/"wait"/
 * "gdb"/"off"/...; comma- or space-separated) into a flags bitmask.  Returns
 * the bitmask, or -1 for "off"/"0". */
int  runloom_crash_parse_flags(const char *s);

/* Test-only: overflow the current C stack via unbounded real-C recursion
 * (does not return).  Run inside a goroutine to fault into its guard page. */
void runloom_crash_selftest_overflow(void);

#ifdef __cplusplus
}
#endif

#endif /* RUNLOOM_CRASH_H */
