/* runloom_fiber_san.h -- fiber-aware sanitizer annotations for stackful swaps.
 *
 * WHY THIS EXISTS
 * ---------------
 * runloom multiplexes N goroutines onto M OS threads (hubs) by swapping the
 * machine stack in arch/swap_*.S.  Both ThreadSanitizer and AddressSanitizer
 * assume one call stack per OS thread; a raw asm stack-swap violates that:
 *
 *   * TSan sees ONE thread per hub and MERGES the happens-before history of
 *     every goroutine multiplexed on it, so it is structurally blind to
 *     intra-hub cross-goroutine races -- exactly runloom's core product
 *     surface.  Its fiber API (__tsan_create_fiber / __tsan_switch_to_fiber /
 *     __tsan_destroy_fiber) gives each goroutine its own history and models a
 *     cooperative switch as a synchronization edge.
 *
 *   * ASan tracks a "fake stack" (use-after-return redzones) and the live
 *     stack bounds per thread; across a swap it either false-positives or
 *     hangs ("switching stacks") unless bracketed with
 *     __sanitizer_start_switch_fiber / __sanitizer_finish_switch_fiber.
 *
 * This header is the ONE place those APIs live.  Every helper compiles to
 * nothing unless the TU is built with -fsanitize=thread/address (uniform
 * across all ~19 TUs -- setup.py appends the flag to every compile), so
 * release pays zero: no fields, no calls.  -DRUNLOOM_NO_FIBER_SAN force-
 * disables it (bisect escape hatch).
 *
 * MODEL
 * -----
 * There are exactly THREE swap sites (coro.c resume/yield, fcontext.c exit)
 * and NO goroutine->goroutine switch: every switch is hub<->goroutine.  So all
 * annotation state lives on the coro, and the hub's stack bounds are LEARNED
 * from ASan's finish_switch_fiber out-params (which also makes cross-hub
 * migration correct for free -- a stolen goroutine relearns the new hub's
 * bounds and captures the new hub's fiber on the next resume).
 *
 * Pairing (each side stores its own token, start before swapping out, finish
 * after swapping back in -- the Boost.Context idiom):
 *   RESUME  (hub->g):  enter()  [start_switch save hub tok; tsan switch to g]
 *                       ... swap ...
 *                      left()   [finish_switch restore hub]         (post-swap)
 *   YIELD   (g->hub):  suspend() [start_switch save g tok; tsan switch to hub]
 *                       ... swap ...
 *                      resumed() [finish_switch relearn hub bounds] (post-swap)
 *   FIRST   (g entry): first_entry() [finish_switch(NULL) -> learn hub bounds]
 *   EXIT    (g done):  abandon() [tsan switch to hub; start_switch(NULL)=discard]
 */
#ifndef RUNLOOM_FIBER_SAN_H
#define RUNLOOM_FIBER_SAN_H

#include <stddef.h>

/* ---- detection: RUNLOOM_FIBERSAN_TSAN / _ASAN / RUNLOOM_FIBERSAN ---- */
#if !defined(RUNLOOM_NO_FIBER_SAN)
#  if defined(__SANITIZE_THREAD__)
#    define RUNLOOM_FIBERSAN_TSAN 1
#  elif defined(__has_feature)
#    if __has_feature(thread_sanitizer)
#      define RUNLOOM_FIBERSAN_TSAN 1
#    endif
#  endif
#  if defined(__SANITIZE_ADDRESS__)
#    define RUNLOOM_FIBERSAN_ASAN 1
#  elif defined(__has_feature)
#    if __has_feature(address_sanitizer)
#      define RUNLOOM_FIBERSAN_ASAN 1
#    endif
#  endif
#endif
#if defined(RUNLOOM_FIBERSAN_TSAN) || defined(RUNLOOM_FIBERSAN_ASAN)
#  define RUNLOOM_FIBERSAN 1
#endif

#if defined(RUNLOOM_FIBERSAN)

/* Pull the interface prototypes; fall back to hand-declaring the stable C ABI
 * if the toolchain ships the runtime but not the headers. */
#if defined(RUNLOOM_FIBERSAN_ASAN)
#  if defined(__has_include)
#    if __has_include(<sanitizer/common_interface_defs.h>)
#      include <sanitizer/common_interface_defs.h>
#      define RUNLOOM_HAVE_ASAN_FIBER_HDR 1
#    endif
#  endif
#  if !defined(RUNLOOM_HAVE_ASAN_FIBER_HDR)
void __sanitizer_start_switch_fiber(void **fake_stack_save,
                                    const void *bottom, size_t size);
void __sanitizer_finish_switch_fiber(void *fake_stack_save,
                                     const void **bottom_old, size_t *size_old);
#  endif
#endif

#if defined(RUNLOOM_FIBERSAN_TSAN)
#  if defined(__has_include)
#    if __has_include(<sanitizer/tsan_interface.h>)
#      include <sanitizer/tsan_interface.h>
#      define RUNLOOM_HAVE_TSAN_FIBER_HDR 1
#    endif
#  endif
#  if !defined(RUNLOOM_HAVE_TSAN_FIBER_HDR)
void *__tsan_get_current_fiber(void);
void *__tsan_create_fiber(unsigned flags);
void __tsan_destroy_fiber(void *fiber);
void __tsan_switch_to_fiber(void *fiber, unsigned flags);
void __tsan_set_fiber_name(void *fiber, const char *name);
#  endif
#endif

/* Per-coro annotation state, embedded in runloom_asm_coro (fcontext.h) so both
 * coro.c and fcontext.c reach it uniformly.  Zeroed at coro creation. */
typedef struct runloom_fiber_san {
#if defined(RUNLOOM_FIBERSAN_TSAN)
    void       *tsan_fiber;        /* this goroutine's TSan fiber (lazy) */
    void       *tsan_caller;       /* hub fiber captured at resume       */
#endif
#if defined(RUNLOOM_FIBERSAN_ASAN)
    void       *asan_self_fake;    /* ASan fake-stack token, goroutine side */
    void       *asan_caller_fake;  /* ASan fake-stack token, hub side       */
    const void *asan_hub_bottom;   /* hub stack bounds, learned via finish  */
    size_t      asan_hub_size;
#endif
    int         initialized;
} runloom_fiber_san_t;

/* forward decl -- the real struct is in fcontext.h */
struct runloom_asm_coro;

/* Zero the state (fresh coro).  MUST run before the first enter() on any coro
 * whose backing memory is not calloc'd (placement/arena coros). */
static inline void runloom_fibersan_zero(struct runloom_asm_coro *a);

/* Free the TSan fiber (true coro free, NOT pool recycle). */
static inline void runloom_fibersan_destroy(struct runloom_asm_coro *a);

/* hub -> goroutine */
static inline void runloom_fibersan_enter(struct runloom_asm_coro *a,
                                          const void *self_lo, size_t self_size);
static inline void runloom_fibersan_left(struct runloom_asm_coro *a);

/* goroutine -> hub */
static inline void runloom_fibersan_suspend(struct runloom_asm_coro *a);
static inline void runloom_fibersan_resumed(struct runloom_asm_coro *a);

/* goroutine first entry / exit (fcontext.c) */
static inline void runloom_fibersan_first_entry(struct runloom_asm_coro *a);
static inline void runloom_fibersan_abandon(struct runloom_asm_coro *a);
static inline void runloom_fibersan_reentered(struct runloom_asm_coro *a);

/* Definitions live after runloom_asm_coro is complete (end of fcontext.h). */

#else  /* !RUNLOOM_FIBERSAN -- every helper is a no-op; no fields, no cost. */

struct runloom_asm_coro;
static inline void runloom_fibersan_zero(struct runloom_asm_coro *a)    { (void)a; }
static inline void runloom_fibersan_destroy(struct runloom_asm_coro *a) { (void)a; }
static inline void runloom_fibersan_enter(struct runloom_asm_coro *a,
                                          const void *lo, size_t n)
{ (void)a; (void)lo; (void)n; }
static inline void runloom_fibersan_left(struct runloom_asm_coro *a)    { (void)a; }
static inline void runloom_fibersan_suspend(struct runloom_asm_coro *a) { (void)a; }
static inline void runloom_fibersan_resumed(struct runloom_asm_coro *a) { (void)a; }
static inline void runloom_fibersan_first_entry(struct runloom_asm_coro *a) { (void)a; }
static inline void runloom_fibersan_abandon(struct runloom_asm_coro *a) { (void)a; }
static inline void runloom_fibersan_reentered(struct runloom_asm_coro *a) { (void)a; }

#endif /* RUNLOOM_FIBERSAN */

#endif /* RUNLOOM_FIBER_SAN_H */
