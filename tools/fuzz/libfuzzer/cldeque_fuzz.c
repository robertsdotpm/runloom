/* cldeque_fuzz.c -- coverage-guided (libFuzzer) fuzz of the REAL Chase-Lev deque.
 *
 * The deque's CONCURRENCY is already proven (Spin/CBMC/GenMC on the real source,
 * TSan on real threads).  What no tool does is COVERAGE-GUIDED exploration of its
 * SEQUENTIAL state machine + boundary arithmetic on the actually-compiled
 * cldeque.c (wrap-around at CAP, the full check, the 1-element pop/steal CAS
 * window) with the operation sequence driven by feedback rather than a fixed
 * stress loop.  This harness decodes the fuzz input into a push/pop/steal stream,
 * mirrors the exact (top,bottom) index semantics in a shadow model, and asserts
 * the real deque agrees on EVERY op -- under AddressSanitizer + UBSan, so an OOB
 * on buf[] or a wrap/overflow UB is caught too.
 *
 * Single-threaded by design: with no concurrent thief, pop and steal each have a
 * deterministic result (bottom for pop, top for steal), so an exact-identity
 * oracle is sound and tight.  The race paths stay covered by CBMC/GenMC/TSan;
 * this is the complementary sequential-coverage angle (the fuzzing-SOTA gap:
 * runloom had zero coverage-guided fuzzing).
 *
 * Build + run: tools/fuzz/libfuzzer/build.sh   (uses a small CAP to hit the
 * full/wrap boundaries fast; the logic is capacity-independent, as CBMC's CAP=4
 * proof shows).  Bounded by -max_total_time so it can't run away.
 */
#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#include "cldeque.h"

#ifndef RUNLOOM_CLDEQUE_CAP
#error "build with -DRUNLOOM_CLDEQUE_CAP=<power-of-two> (e.g. 8)"
#endif
#define CAP  RUNLOOM_CLDEQUE_CAP
#define MASK (CAP - 1)

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    runloom_cldeque_t d;
    runloom_cldeque_init(&d);

    /* shadow model mirroring the deque's own (t=top, b=bottom) indices */
    void *model[CAP];
    long mt = 0, mb = 0;           /* top, bottom */
    uintptr_t next_tag = 1;        /* unique, non-NULL item tags */

    for (size_t i = 0; i < size; i++) {
        uint8_t op = data[i] & 0x3u;            /* 0,1 -> push (weighted) */
        long msz = mb - mt;

        if (op <= 1u) {                          /* PUSH (owner) */
            void *tag = (void *)next_tag;
            int r = runloom_cldeque_push(&d, tag);
            if (msz >= CAP) {
                assert(r == -1);                 /* full must be signalled */
            } else {
                assert(r == 0);
                model[mb & MASK] = tag;
                mb++;
                next_tag++;
            }
        } else if (op == 2u) {                    /* POP (owner, bottom) */
            void *got = runloom_cldeque_pop(&d);
            if (msz == 0) {
                assert(got == NULL);
            } else {
                mb--;
                assert(got == model[mb & MASK]);  /* LIFO from the owner side */
            }
        } else {                                  /* STEAL (thief, top) */
            void *got = runloom_cldeque_steal(&d);
            if (msz == 0) {
                assert(got == NULL);
            } else {
                assert(got == model[mt & MASK]);  /* FIFO from the thief side */
                mt++;
            }
        }
        assert(runloom_cldeque_size(&d) == (mb - mt));   /* size conservation */
    }

    /* drain whatever is left: must come out top-first, exactly, then empty */
    while (mt < mb) {
        void *got = runloom_cldeque_steal(&d);
        assert(got == model[mt & MASK]);
        mt++;
    }
    assert(runloom_cldeque_steal(&d) == NULL);
    assert(runloom_cldeque_pop(&d) == NULL);
    assert(runloom_cldeque_size(&d) == 0);
    return 0;
}
