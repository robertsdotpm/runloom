/* runloom_stackadvice.h -- per-goroutine-kind stack-usage profiler (advisory).
 *
 * An OPT-IN diagnostic.  When enabled it measures each goroutine kind's actual
 * C-stack high-water mark (keyed by the entry callable's identity) and reports,
 * per kind, "used X of the Y you reserved -- consider stack_size=Z".  It does
 * NOT change any stack size itself: the runtime never auto-redefines or
 * persists sizes (a remembered-small size is only ever a lower bound on what a
 * future input needs -- see docs).  You read the advice and apply it yourself
 * via runloom_c.go(fn, stack_size=...), with the guard page + crash reporter
 * still backstopping every guess.
 *
 * Off by default (zero cost).  Enabling it turns stack painting back on so the
 * HWM scan works, so it carries the paint/scan cost only while profiling.
 */
#ifndef RUNLOOM_STACKADVICE_H
#define RUNLOOM_STACKADVICE_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Turn measurement on/off.  Enabling also forces stack painting on. */
void   runloom_advice_set_enabled(int on);
int    runloom_advice_enabled(void);

/* At spawn (GIL held): return a non-zero key identifying this callable's kind,
 * recording the kind's display name if first-seen.  Returns 0 if advice is off,
 * the callable is NULL, or the table is full. */
size_t runloom_advice_note_spawn(PyObject *callable);

/* At completion (no Python objects touched): fold one goroutine's stack HWM
 * (and the size it ran with) into its kind. */
void   runloom_advice_record(size_t key, size_t hwm, size_t reserved);

/* Convenience: fold a completed goroutine's HWM into its kind, scanning its
 * coro stack.  No-op unless profiling is on and the g carries a kind key.  Call
 * at any completion point while g->coro is still valid (the single-sched drain
 * and the M:N hub both use it). */
struct runloom_g;
void   runloom_advice_record_g(struct runloom_g *g);

/* GIL held: a list[dict] of per-kind records
 * {kind, samples, max_hwm, reserved, suggested}. */
PyObject *runloom_advice_report(void);

/* Clear all accumulated samples (keeps the enabled flag). */
void   runloom_advice_reset(void);

/* Re-init the lock in a forked child. */
void   runloom_advice_reset_after_fork(void);

#ifdef __cplusplus
}
#endif

#endif /* RUNLOOM_STACKADVICE_H */
