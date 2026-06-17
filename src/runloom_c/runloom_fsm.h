#ifndef RUNLOOM_FSM_H
#define RUNLOOM_FSM_H
/* runloom_fsm.h -- provably-total finite-state-machine scaffolding.
 *
 * A lost wakeup / dropped event is fundamentally an UNDEFINED state transition:
 * an event arrives in a state with no handler and is silently dropped.  This
 * header makes that unrepresentable for the state machines that adopt it (see
 * docs/dev/wake_protocol/FSM_ADOPTION.md for the scoped list):
 *
 *   (1) States + events are explicit, densely-numbered enums ending in a _COUNT
 *       sentinel.  The legal transitions live in ONE const table
 *       `signed char T[STATE_COUNT][EVENT_COUNT]`, with every illegal cell set to
 *       RUNLOOM_FSM_INVALID.  RUNLOOM_FSM_ASSERT_TABLE(_Static_assert) ties the
 *       table dimensions to the enum _COUNTs, so adding a state or event without
 *       resizing the table FAILS THE BUILD.
 *   (2) RUNLOOM_FSM_STEP() looks the next state up in the table and, on an
 *       INVALID cell, calls runloom_fsm_violation() -- a loud, immediate abort.
 *       An undefined pathway is a diagnosed crash, never silent state drift.
 *   (3) The tiny FSM is then model-checked in verify/ (CBMC/GenMC) for the
 *       protocol properties (no lost event, progress); that gates check_all.
 *
 * Two adoption modes (FSM_ADOPTION.md tier 1 vs the hot-path sites):
 *   - FULL conversion: the table DRIVES the state via RUNLOOM_FSM_STEP().  Use
 *     where the dispatch is not on the hottest path (e.g. the channel waiter).
 *   - OBSERVATIONAL layer: keep the existing proven atomic/CAS logic, but ALSO
 *     drive an explicit state field and call RUNLOOM_FSM_NOTE() at each
 *     transition.  When built -DRUNLOOM_FSM_VALIDATE it asserts the (from->to)
 *     edge exists in the table (illegal transition -> abort); it compiles to
 *     NOTHING otherwise, so the hot path pays zero cost in release.  Mirrors the
 *     runloom_lockrank.h "free unless -D" philosophy.
 *
 * C99/C11, header-only, no C++.  runloom_fsm_violation is always compiled in
 * (it is the runtime backstop the design depends on); its branch is
 * predicted-not-taken off the hot path.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>

#include "plat.h"   /* RUNLOOM_INLINE, RUNLOOM_NORETURN */

/* Sentinel for an illegal (state,event) cell.  States/events are small
 * non-negative enums, so -1 is a safe out-of-band value in a signed char. */
#define RUNLOOM_FSM_INVALID ((signed char)-1)

/* Loud, immediate abort on an undefined transition. */
RUNLOOM_NORETURN RUNLOOM_INLINE void
runloom_fsm_violation(const char *fsm, int state, int event,
                      const char *file, int line)
{
    fprintf(stderr,
            "\nRUNLOOM FSM VIOLATION [%s]: no transition for "
            "(state=%d, event=%d) at %s:%d\n",
            fsm, state, event, file, line);
    fflush(stderr);
    abort();
}

/* Table-driven checked transition: returns T[state][event], or aborts if that
 * cell is RUNLOOM_FSM_INVALID.  `table` is a 2-D `const signed char[NS][NE]`;
 * NE is its event-dimension (row width).  Side-effect-free args only (the macro
 * evaluates them once via the helper). */
RUNLOOM_INLINE int
runloom_fsm_step_(const char *fsm, const signed char *table, int ne,
                  int state, int event, const char *file, int line)
{
    signed char next = table[state * ne + event];
    if (next == RUNLOOM_FSM_INVALID)
        runloom_fsm_violation(fsm, state, event, file, line);
    return (int)next;
}
#define RUNLOOM_FSM_STEP(fsm, table, NE, state, event) \
    runloom_fsm_step_((fsm), &(table)[0][0], (NE), \
                      (int)(state), (int)(event), __FILE__, __LINE__)

/* Compile-time: the transition table must cover exactly NS x NE cells.  Place
 * after the table with the enum _COUNT sentinels; a state/event added without
 * resizing the table then fails to compile. */
#define RUNLOOM_FSM_ASSERT_TABLE(table, NS, NE, fsm)                      \
    _Static_assert(sizeof(table) ==                                      \
                       (size_t)(NS) * (size_t)(NE) * sizeof((table)[0][0]), \
                   fsm ": transition table must be [" #NS "][" #NE "] -- "  \
                   "a state or event enum changed without updating the table")

/* Observational validation for hot-path sites that keep their own atomic/CAS
 * dispatch: assert that the (from -> to) edge exists in the table (some event
 * maps from -> to).  Zero cost unless -DRUNLOOM_FSM_VALIDATE. */
#if defined(RUNLOOM_FSM_VALIDATE)
RUNLOOM_INLINE void
runloom_fsm_note_(const char *fsm, const signed char *table, int ns, int ne,
                  int from, int to, const char *file, int line)
{
    int e;
    if (from < 0 || from >= ns || to < 0 || to >= ns) {
        fprintf(stderr, "\nRUNLOOM FSM VIOLATION [%s]: out-of-range "
                "(from=%d, to=%d) at %s:%d\n", fsm, from, to, file, line);
        fflush(stderr); abort();
    }
    for (e = 0; e < ne; e++)
        if ((int)table[from * ne + e] == to)
            return;                       /* a legal edge exists */
    fprintf(stderr, "\nRUNLOOM FSM VIOLATION [%s]: illegal transition "
            "%d -> %d (no event maps it) at %s:%d\n", fsm, from, to, file, line);
    fflush(stderr); abort();
}
#  define RUNLOOM_FSM_NOTE(fsm, table, NS, NE, from, to) \
       runloom_fsm_note_((fsm), &(table)[0][0], (NS), (NE), \
                         (int)(from), (int)(to), __FILE__, __LINE__)
#else
#  define RUNLOOM_FSM_NOTE(fsm, table, NS, NE, from, to) ((void)0)
#endif

#endif /* RUNLOOM_FSM_H */
