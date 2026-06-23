#ifndef RUNLOOM_IO_FSM_H
#define RUNLOOM_IO_FSM_H
/* runloom_io_fsm.h -- the I/O-return classifier FSM.
 *
 * The OTHER half of the two-stacked-FSM design (the Parker FSM -- RUNNING ->
 * ARMED -> {PARKED|READY} -- being the first; see
 * docs/dev/wake_protocol/FSM_ADOPTION.md).  Every cooperative I/O syscall
 * (recv/recvfrom/read, send/sendto/write, accept, connect, ...) returns into a
 * CLOSED event alphabet.  This header is the single, TOTAL (rc, errno) -> event
 * classifier, so "what does this return mean?" is decided in exactly ONE place:
 *
 *   - The classifier is exhaustive by construction: every rc sign is handled and
 *     the errno space ends in a default ERROR (no input escapes unclassified;
 *     an invalid CALL KIND is a loud runloom_fsm_violation abort, never UB).
 *   - The per-call ALLOWED-EVENT mask (runloom_io_call_events[]) + _Static_assert
 *     ties the table to the enum: adding a call kind / an event a kind can emit
 *     without updating the mask is catchable, and RUNLOOM_IO_NOTE() asserts
 *     (under -DRUNLOOM_FSM_VALIDATE) that a site only acts on events the call can
 *     actually emit -- the consumer-side "you didn't handle a real pathway" net.
 *   - tools/verify/cbmc/io_classify_cbmc.c proves TOTALITY (always an in-range event)
 *     and MASK-SOUNDNESS (the event is always one the call kind may emit) over
 *     all (call, rc, errno); a -DBUG_* teeth config must fail.
 *
 * This makes a dropped/mis-handled I/O return "unrepresentable" the same way the
 * wake-state FSM makes a lost wake unrepresentable: an unhandled return is a
 * diagnosed abort or a failed proof, never a silent wrong branch.
 *
 * C99/C11, header-only, no C++.  Zero runtime cost in release (the NOTE compiles
 * to ((void)0); the classifier is a small inline switch).
 */

#include <errno.h>

#include "plat.h"        /* RUNLOOM_INLINE */
#include "runloom_fsm.h" /* runloom_fsm_violation, stdio/stdlib */

/* The closed event alphabet every cooperative I/O syscall maps onto. */
typedef enum {
    RUNLOOM_IO_READY = 0,    /* recv/read got data; accept got fd; connect done  */
    RUNLOOM_IO_EOF,          /* recv/read returned 0 -- orderly peer shutdown     */
    RUNLOOM_IO_PROGRESS,     /* send/write accepted N>=0 bytes (maybe partial)    */
    RUNLOOM_IO_WOULDBLOCK_R, /* not ready now; park on READ and retry             */
    RUNLOOM_IO_WOULDBLOCK_W, /* not ready now; park on WRITE and retry            */
    RUNLOOM_IO_INTR,         /* EINTR -- interrupted; retry (or take a signal)    */
    RUNLOOM_IO_RETRY,        /* transient (ECONNABORTED on accept); retry quietly */
    RUNLOOM_IO_ERROR,        /* terminal errno -- raise OSError(errno)            */
    RUNLOOM_IO_EVENT_COUNT
} runloom_io_event_t;

/* The cooperative I/O call kinds, grouped by RETURN-SHAPE contract. */
typedef enum {
    RUNLOOM_IO_RECV = 0,  /* recv/recvfrom/read:  >0 READY, 0 EOF, <0 errno       */
    RUNLOOM_IO_SEND,      /* send/sendto/write:   >=0 PROGRESS, <0 errno          */
    RUNLOOM_IO_ACCEPT,    /* accept:              >=0 READY, <0 errno (+ABORTED)  */
    RUNLOOM_IO_CONNECT,   /* connect:             0 READY, <0 errno (EINPROGRESS) */
    RUNLOOM_IO_CALL_COUNT
} runloom_io_call_t;

#define RUNLOOM_IO_BIT(ev) (1u << (unsigned)(ev))

/* Which events each call kind may LEGITIMATELY emit.  A consumer switch only
 * needs to handle these per kind; RUNLOOM_IO_NOTE asserts a site stays inside
 * its kind's mask.  Keep in sync with runloom_io_classify below (the CBMC proof
 * enforces classify's output is always a subset of the acting call's mask). */
#if defined(__GNUC__) || defined(__clang__)
#  define RUNLOOM_IO_MAYBE_UNUSED __attribute__((unused))
#else
#  define RUNLOOM_IO_MAYBE_UNUSED
#endif
static const unsigned runloom_io_call_events[RUNLOOM_IO_CALL_COUNT]
        RUNLOOM_IO_MAYBE_UNUSED = {
    /* RECV    */ RUNLOOM_IO_BIT(RUNLOOM_IO_READY) | RUNLOOM_IO_BIT(RUNLOOM_IO_EOF)
                | RUNLOOM_IO_BIT(RUNLOOM_IO_WOULDBLOCK_R)
                | RUNLOOM_IO_BIT(RUNLOOM_IO_INTR) | RUNLOOM_IO_BIT(RUNLOOM_IO_ERROR),
    /* SEND    */ RUNLOOM_IO_BIT(RUNLOOM_IO_PROGRESS)
                | RUNLOOM_IO_BIT(RUNLOOM_IO_WOULDBLOCK_W)
                | RUNLOOM_IO_BIT(RUNLOOM_IO_INTR) | RUNLOOM_IO_BIT(RUNLOOM_IO_ERROR),
    /* ACCEPT  */ RUNLOOM_IO_BIT(RUNLOOM_IO_READY)
                | RUNLOOM_IO_BIT(RUNLOOM_IO_WOULDBLOCK_R)
                | RUNLOOM_IO_BIT(RUNLOOM_IO_INTR) | RUNLOOM_IO_BIT(RUNLOOM_IO_RETRY)
                | RUNLOOM_IO_BIT(RUNLOOM_IO_ERROR),
    /* CONNECT */ RUNLOOM_IO_BIT(RUNLOOM_IO_READY)
                | RUNLOOM_IO_BIT(RUNLOOM_IO_WOULDBLOCK_W)
                | RUNLOOM_IO_BIT(RUNLOOM_IO_INTR) | RUNLOOM_IO_BIT(RUNLOOM_IO_ERROR),
};
_Static_assert(sizeof(runloom_io_call_events) ==
                   (size_t)RUNLOOM_IO_CALL_COUNT * sizeof(unsigned),
               "runloom_io_fsm: call-events mask must have exactly "
               "RUNLOOM_IO_CALL_COUNT rows -- a call kind was added without "
               "declaring the events it can emit");

/* errno -> event for a syscall that signalled failure (rc < 0).  `wb` is the
 * park direction for a would-block (READ for recv/accept, WRITE for send/
 * connect).  TOTAL: any errno not explicitly transient is a terminal ERROR. */
RUNLOOM_INLINE runloom_io_event_t
runloom_io_classify_errno_(int err, runloom_io_event_t wb)
{
#if defined(EWOULDBLOCK) && EWOULDBLOCK != EAGAIN
    if (err == EAGAIN || err == EWOULDBLOCK) return wb;
#else
    if (err == EAGAIN) return wb;
#endif
    if (err == EINTR) return RUNLOOM_IO_INTR;
    return RUNLOOM_IO_ERROR;
}

/* The single, total (call, rc, errno) -> event classifier.  `rc` is the raw
 * syscall return widened to long long (ssize_t/int/SOCKET all fit); `err` is
 * errno captured immediately after the call. */
RUNLOOM_INLINE runloom_io_event_t
runloom_io_classify(runloom_io_call_t call, long long rc, int err)
{
    switch (call) {
    case RUNLOOM_IO_RECV:
        if (rc > 0)  return RUNLOOM_IO_READY;
        if (rc == 0) return RUNLOOM_IO_EOF;
        return runloom_io_classify_errno_(err, RUNLOOM_IO_WOULDBLOCK_R);
    case RUNLOOM_IO_SEND:
        if (rc >= 0) return RUNLOOM_IO_PROGRESS;
        return runloom_io_classify_errno_(err, RUNLOOM_IO_WOULDBLOCK_W);
    case RUNLOOM_IO_ACCEPT:
        if (rc >= 0) return RUNLOOM_IO_READY;
#ifdef ECONNABORTED
        if (err == ECONNABORTED) return RUNLOOM_IO_RETRY;
#endif
        return runloom_io_classify_errno_(err, RUNLOOM_IO_WOULDBLOCK_R);
    case RUNLOOM_IO_CONNECT:
        if (rc == 0) return RUNLOOM_IO_READY;
#ifdef EINPROGRESS
        if (err == EINPROGRESS) return RUNLOOM_IO_WOULDBLOCK_W;
#endif
        return runloom_io_classify_errno_(err, RUNLOOM_IO_WOULDBLOCK_W);
    case RUNLOOM_IO_CALL_COUNT:
        break;  /* sentinel, never a real call kind */
    }
    /* Unreachable for any valid call kind: a loud abort, never silent UB. */
    runloom_fsm_violation("runloom_io", (int)call, -1, __FILE__, __LINE__);
}

/* Consumer-side net: assert (under -DRUNLOOM_FSM_VALIDATE) that the event a site
 * is about to act on is one its call kind can actually emit.  Catches a site
 * that mis-routes (e.g. treats a SEND as if it could EOF).  Zero cost else. */
#if defined(RUNLOOM_FSM_VALIDATE)
RUNLOOM_INLINE void
runloom_io_note_(runloom_io_call_t call, runloom_io_event_t ev,
                 const char *file, int line)
{
    if ((unsigned)call >= (unsigned)RUNLOOM_IO_CALL_COUNT ||
        (unsigned)ev   >= (unsigned)RUNLOOM_IO_EVENT_COUNT ||
        !(runloom_io_call_events[call] & RUNLOOM_IO_BIT(ev))) {
        fprintf(stderr, "\nRUNLOOM FSM VIOLATION [runloom_io]: call %d cannot "
                "emit event %d at %s:%d\n", (int)call, (int)ev, file, line);
        fflush(stderr);
        abort();
    }
}
#  define RUNLOOM_IO_NOTE(call, ev) \
       runloom_io_note_((call), (ev), __FILE__, __LINE__)
#else
#  define RUNLOOM_IO_NOTE(call, ev) ((void)0)
#endif

/* Exhaustive consumer switch on a runloom_io_event_t.  OMITTING any event (a new
 * return pathway) is a BUILD ERROR -- "the program can't compile if a pathway
 * isn't handled" -- scoped via pragma so it adds zero global -Wswitch-enum
 * noise.  Every enumerator (including RUNLOOM_IO_EVENT_COUNT) must have a case.
 * Usage:
 *     RUNLOOM_IO_SWITCH(ev) {
 *     case RUNLOOM_IO_READY: ...; break;
 *     ...
 *     case RUNLOOM_IO_EVENT_COUNT:  // impossible sentinel
 *         runloom_fsm_violation(...);
 *     } RUNLOOM_IO_SWITCH_END */
#if defined(__GNUC__) || defined(__clang__)
#  define RUNLOOM_IO_SWITCH(ev)                              \
       _Pragma("GCC diagnostic push")                        \
       _Pragma("GCC diagnostic error \"-Wswitch-enum\"")     \
       switch (ev)
#  define RUNLOOM_IO_SWITCH_END _Pragma("GCC diagnostic pop")
#else
#  define RUNLOOM_IO_SWITCH(ev) switch (ev)
#  define RUNLOOM_IO_SWITCH_END /* no scoped diagnostic on this compiler */
#endif

#endif /* RUNLOOM_IO_FSM_H */
