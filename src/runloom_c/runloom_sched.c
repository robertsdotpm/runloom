/* runloom_sched.c -- C-level cooperative scheduler.
 *
 * Cost model (target 50-100 ns per yield once everything compiles):
 *   - yield: 2 list ops + ptr swap + asm switch + tstate snap/restore.
 *   - resume: same in reverse.
 *
 * What's _not_ here (yet):
 *   - work-stealing across threads (Phase C v1 is in mn_sched.c)
 *
 * Phase B: per-goroutine snapshot of CPython tstate.  Algorithm copied
 * from greenlet (MIT) -- src/greenlet/TPythonState.cpp.  Each goroutine
 * gets its own slice of cframe / current_frame / datastack_chunk / etc,
 * so frames from different gs do not link into one shared C-stack chain.
 * Lifts the ~200 concurrent yielded goroutine cliff.
 *
 * The Python side talks to us through a tiny Python type defined in
 * module.c (RunloomG).  The user-visible API is `runloom.go / yield_ /
 * sleep / run`.
 */

#if !defined(_WIN32)
#  define _POSIX_C_SOURCE 200809L
#endif

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "plat.h"
#include "plat_compat.h"
#include "runloom_sched.h"
#include "mn_sched.h"
#include "netpoll.h"
#include "io_uring.h"
#include "runloom_blockpool.h"
#include "runloom_diag.h"
#include "runloom_gstate.h"
#include "runloom_introspect.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if !defined(_WIN32)
#  include <sys/mman.h>          /* madvise / MADV_DONTNEED, mincore */
#  include <unistd.h>            /* sysconf(_SC_PAGESIZE) */
#endif

/* ---- monotonic seconds ----
 * Shim-backed: plat_compat's runloom_monotonic_ns() picks
 * QueryPerformanceCounter on Windows and clock_gettime(CLOCK_MONOTONIC)
 * on POSIX (macOS/Linux/BSD).  Both have sub-microsecond resolution. */

/* ---------------------------------------------------------------------------
 * runloom_sched.c is split across the runloom_sched_*.c.inc fragments below for readability.
 * They are #included here (one translation unit): the fragments share this
 * file's includes, typedefs and file-scope statics and are NOT compiled
 * standalone.  setup.py compiles only runloom_sched.c.
 * --------------------------------------------------------------------------- */
#include "runloom_sched_pystate.c.inc"
#include "runloom_sched_datastack.c.inc"
#include "runloom_sched_core.c.inc"
#include "runloom_sched_parkwake.c.inc"
#include "runloom_sched_drain.c.inc"
#include "runloom_sched_preempt.c.inc"
