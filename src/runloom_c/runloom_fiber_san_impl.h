/* runloom_fiber_san_impl.h -- helper bodies for the fiber sanitizer brackets.
 *
 * Included at the END of fcontext.h, AFTER struct runloom_asm_coro is complete
 * (the bodies dereference a->fibersan).  Split from runloom_fiber_san.h so that
 * header can define the runloom_fiber_san_t TYPE that the struct embeds without
 * a circular dependency.  Everything here compiles away unless RUNLOOM_FIBERSAN.
 */
#ifndef RUNLOOM_FIBER_SAN_IMPL_H
#define RUNLOOM_FIBER_SAN_IMPL_H

#if defined(RUNLOOM_FIBERSAN)

#include <string.h>   /* memset for _zero */

static inline void runloom_fibersan_zero(struct runloom_asm_coro *a)
{
    memset(&a->fibersan, 0, sizeof(a->fibersan));
}

static inline void runloom_fibersan_destroy(struct runloom_asm_coro *a)
{
#if defined(RUNLOOM_FIBERSAN_TSAN)
    if (a->fibersan.tsan_fiber != NULL) {
        __tsan_destroy_fiber(a->fibersan.tsan_fiber);
        a->fibersan.tsan_fiber = NULL;
    }
#else
    (void)a;
#endif
}

/* RESUME, hub -> goroutine, pre-swap.  self_lo/self_size = the goroutine's own
 * stack (lowest usable byte + usable length). */
static inline void runloom_fibersan_enter(struct runloom_asm_coro *a,
                                          const void *self_lo, size_t self_size)
{
#if defined(RUNLOOM_FIBERSAN_TSAN)
    if (a->fibersan.tsan_fiber == NULL) {
        a->fibersan.tsan_fiber = __tsan_create_fiber(0);
        __tsan_set_fiber_name(a->fibersan.tsan_fiber, "runloom_g");
    }
    a->fibersan.tsan_caller = __tsan_get_current_fiber();
    __tsan_switch_to_fiber(a->fibersan.tsan_fiber, 0);
#endif
#if defined(RUNLOOM_FIBERSAN_ASAN)
    __sanitizer_start_switch_fiber(&a->fibersan.asan_caller_fake,
                                   self_lo, self_size);
#else
    (void)self_lo; (void)self_size;
#endif
}

/* RESUME, post-swap: hub is running again (goroutine yielded back). */
static inline void runloom_fibersan_left(struct runloom_asm_coro *a)
{
#if defined(RUNLOOM_FIBERSAN_ASAN)
    __sanitizer_finish_switch_fiber(a->fibersan.asan_caller_fake, NULL, NULL);
#else
    (void)a;
#endif
}

/* YIELD, goroutine -> hub, pre-swap. */
static inline void runloom_fibersan_suspend(struct runloom_asm_coro *a)
{
#if defined(RUNLOOM_FIBERSAN_TSAN)
    if (a->fibersan.tsan_caller != NULL)
        __tsan_switch_to_fiber(a->fibersan.tsan_caller, 0);
#endif
#if defined(RUNLOOM_FIBERSAN_ASAN)
    __sanitizer_start_switch_fiber(&a->fibersan.asan_self_fake,
                                   a->fibersan.asan_hub_bottom,
                                   a->fibersan.asan_hub_size);
#endif
}

/* YIELD, post-swap: goroutine is running again; relearn hub bounds (they change
 * if a different hub resumed us -- cross-hub steal/migration). */
static inline void runloom_fibersan_resumed(struct runloom_asm_coro *a)
{
#if defined(RUNLOOM_FIBERSAN_ASAN)
    __sanitizer_finish_switch_fiber(a->fibersan.asan_self_fake,
                                    &a->fibersan.asan_hub_bottom,
                                    &a->fibersan.asan_hub_size);
#else
    (void)a;
#endif
}

/* FIRST ENTRY (trampoline): brand-new fiber, no prior self token -> NULL.
 * Learn the hub's stack bounds from the finish out-params. */
static inline void runloom_fibersan_first_entry(struct runloom_asm_coro *a)
{
#if defined(RUNLOOM_FIBERSAN_ASAN)
    __sanitizer_finish_switch_fiber(NULL,
                                    &a->fibersan.asan_hub_bottom,
                                    &a->fibersan.asan_hub_size);
#else
    (void)a;
#endif
}

/* EXIT: goroutine done, swapping back to the hub for the last time.  Discard
 * this fiber's fake stack (NULL save slot). */
static inline void runloom_fibersan_abandon(struct runloom_asm_coro *a)
{
#if defined(RUNLOOM_FIBERSAN_TSAN)
    if (a->fibersan.tsan_caller != NULL)
        __tsan_switch_to_fiber(a->fibersan.tsan_caller, 0);
#endif
#if defined(RUNLOOM_FIBERSAN_ASAN)
    __sanitizer_start_switch_fiber(NULL,
                                   a->fibersan.asan_hub_bottom,
                                   a->fibersan.asan_hub_size);
#endif
}

/* Spurious resume of a done coro (defensive; the exit for(;;) loop).  We
 * discarded on abandon, so land as a fresh fiber (NULL) and relearn. */
static inline void runloom_fibersan_reentered(struct runloom_asm_coro *a)
{
#if defined(RUNLOOM_FIBERSAN_ASAN)
    __sanitizer_finish_switch_fiber(NULL,
                                    &a->fibersan.asan_hub_bottom,
                                    &a->fibersan.asan_hub_size);
#else
    (void)a;
#endif
}

#endif /* RUNLOOM_FIBERSAN */

#endif /* RUNLOOM_FIBER_SAN_IMPL_H */
