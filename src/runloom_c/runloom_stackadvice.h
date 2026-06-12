/* runloom_stackadvice.h -- per-fiber-kind stack-usage profiler (advisory).
 *
 * An OPT-IN diagnostic.  When enabled it measures each fiber kind's actual
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

/* Auto-sizer: in addition to measuring, actually APPLY the learned per-kind
 * size to the next fiber of that kind.  "Start large, learn down": an
 * unseen kind's first fibers start at a generous size, and once measured
 * the kind's later fibers start at next_pow2(observed_max * 4) (clamped).
 * In-memory only -- never persisted (a remembered-small size is only a lower
 * bound on what a future input needs; the guard page + grow + crash reporter
 * stay the safety net).  Enabling autosize implies measurement, forces painting
 * on, and turns on park-time idle-page reclaim (so the large starts are
 * RSS-free).  Off by default. */
/* `prescan`: also run the cold-start optimizer -- loosely scan an unseen kind's
 * bytecode for symbols whose C implementation has a fat single stack frame
 * (e.g. Decimal arithmetic at 256 KiB) and start its first fibers big
 * enough to hold that frame, so they don't overflow before being measured.
 * Off within autosize unless requested. */
void   runloom_advice_set_autosize(int on, int prescan);
int    runloom_advice_autosize_enabled(void);

/* At spawn (default-size path only; an explicit stack_size= bypasses this):
 * given the kind key (from runloom_advice_note_spawn) and the entry callable,
 * return the stack size to use -- the learned size if the kind has samples; for
 * an unseen kind the start-large default, raised by the cold-start optimizer if
 * `prescan` is on and the code references a fat-frame symbol; or `fallback`
 * unchanged if autosize is off.  GIL must be held (it may read the callable). */
size_t runloom_advice_size_for(size_t key, PyObject *callable, size_t fallback);

/* At spawn (GIL held): return a non-zero key identifying this callable's kind,
 * recording the kind's display name if first-seen.  Returns 0 if advice is off,
 * the callable is NULL, or the table is full. */
size_t runloom_advice_note_spawn(PyObject *callable);

/* At completion (no Python objects touched): fold one fiber's stack HWM
 * (and the size it ran with) into its kind. */
void   runloom_advice_record(size_t key, size_t hwm, size_t reserved);

/* Convenience: fold a completed fiber's HWM into its kind, scanning its
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
