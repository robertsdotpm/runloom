/* io_uring.c -- cooperative io_uring backend.
 *
 * Why we exist: runloom_c.fd_read / fd_write on regular files don't
 * work cooperatively through epoll -- regular file fds always report
 * "ready" so wait_fd is a no-op and the actual read/write blocks the
 * OS thread.  io_uring submits the read/write asynchronously to the
 * kernel; we park the fiber and let other gs run while the kernel
 * processes the op.  When a completion is posted the kernel signals
 * an eventfd registered with the ring; the netpoll pump observes that
 * eventfd (epoll-registered), drains the CQ ring, and wakes the
 * fiber that submitted each op.
 *
 * What we DON'T do: liburing.  Adding a build-time dependency on a
 * native library would compromise runloom's "pip install . just works"
 * story.  We talk to io_uring via the raw syscalls (io_uring_setup,
 * io_uring_enter, io_uring_register) and an mmap'd ring -- about 300
 * lines of code total.
 *
 * Backend availability is runtime-detected via the io_uring_setup
 * syscall returning -ENOSYS on old kernels (<5.1).  In that case
 * runloom_iouring_available() returns 0 and callers fall back to the
 * thread-pool path in monkey.py / runloom.sync.
 *
 * Concurrency model:
 *   - Submission is mutex-protected so multiple OS threads (the global
 *     scheduler thread and any M:N hub thread) can share the single
 *     ring.
 *   - Drain runs lock-free over the CQ ring; wakes are routed via
 *     runloom_sched_wake_safe (global sched g) or runloom_mn_wake_g (hub g)
 *     based on the per-op record's hub pointer.
 *   - The op record lives on the submitter's C stack.  The fiber
 *     doesn't get torn down while parked, so the stack stays alive
 *     through to drain.
 *
 * Hub callers: the eventfd integration is wired into the GLOBAL netpoll
 * pump.  Within an M:N hub there's no shared pump that drains the ring
 * automatically, so hub callers take a synchronous spin-drain path
 * (block in io_uring_enter with min_complete=1 + drain inline).  This
 * regresses the hub case versus single-thread but is correct; future
 * work is one-ring-per-hub for full M:N coverage.
 */
#include "plat.h"

#if defined(__linux__)

#include <errno.h>
#include <fcntl.h>
#include <linux/io_uring.h>
#include <poll.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "io_uring.h"
#include "mn_sched.h"
#include "netpoll.h"
#include "plat_compat.h"
#include "runloom_lockrank.h"
#include "runloom_sched.h"
#include "runloom_fsm.h"
#include "runloom_io_fsm.h"   /* the total (rc,errno)->event I/O classifier */

/* IORING_REGISTER_EVENTFD opcode for io_uring_register.  Value is a
 * stable kernel ABI but some older Linux headers don't expose the
 * symbol; define a fallback. */
#ifndef IORING_REGISTER_EVENTFD
#  define IORING_REGISTER_EVENTFD 4
#endif

/* Provided-buffer-ring opcodes/symbols (Linux 5.19+).  Older kernel
 * headers don't define these; supply fallbacks so we can compile and
 * just feature-detect at runtime. */
#ifndef IORING_REGISTER_PBUF_RING
#  define IORING_REGISTER_PBUF_RING   22
#endif
#ifndef IORING_UNREGISTER_PBUF_RING
#  define IORING_UNREGISTER_PBUF_RING 23
#endif
#ifndef IOSQE_BUFFER_SELECT
#  define IOSQE_BUFFER_SELECT (1U << 5)
#endif
#ifndef IORING_CQE_F_BUFFER
#  define IORING_CQE_F_BUFFER 1U
#endif
#ifndef IORING_CQE_F_MORE
#  define IORING_CQE_F_MORE 2U
#endif
#ifndef IORING_CQE_BUFFER_SHIFT
#  define IORING_CQE_BUFFER_SHIFT 16
#endif
#ifndef IORING_RECV_MULTISHOT
#  define IORING_RECV_MULTISHOT (1U << 1)
#endif

/* Provided-buffer ring sizing.  Powers of two only; the ring uses
 * masking against (n - 1) to wrap indices.
 *
 * Sized to absorb the worst case: every armed multishot recv has one
 * CQE in flight at once.  N=4096 conns × 1 buffer each = 4096 entries.
 * At 2 KB/buffer that's 8 MB of pinned kernel-visible memory -- the
 * trade is acceptable for the high-concurrency workloads that
 * actually benefit from multishot.  Smaller pools risk -ENOBUFS
 * storms where the kernel ends a multishot mid-stream, the conn's
 * consumer fiber has to re-arm, and another conn may grab the
 * buffer first -- with enough conns this stalls progress entirely. */
#define RUNLOOM_IOURING_PBUF_COUNT    4096
#define RUNLOOM_IOURING_PBUF_SIZE     2048
#define RUNLOOM_IOURING_PBUF_BGID        0

/* Minimal kernel-shared structs for the buffer ring.  We could rely
 * on the libc UAPI header (linux/io_uring.h above), but it's been
 * around long enough on 5.19+ kernels that we just feature-test the
 * registration syscall at runtime and use these shadow declarations
 * for source compatibility with older build hosts. */
struct runloom_iouring_buf {
    uint64_t addr;
    uint32_t len;
    uint16_t bid;
    uint16_t resv;
};

struct runloom_iouring_buf_reg {
    uint64_t ring_addr;
    uint32_t ring_entries;
    uint16_t bgid;
    uint16_t flags;
    uint64_t resv[3];
};


/* ---------------------------------------------------------------------------
 * io_uring.c is split across the io_uring_*.c.inc fragments below for readability.
 * They are #included here (one translation unit): the fragments share this
 * file's includes, typedefs and file-scope statics and are NOT compiled
 * standalone.  setup.py compiles only io_uring.c.
 * --------------------------------------------------------------------------- */
#include "io_uring_l_sys.c.inc"   /* defines RUNLOOM_IOURING_WAIT_* used below */

/* ---- io_uring SINGLE-op park/wake FSM (OBSERVATIONAL) -----------------------
 * The op->wait commit handshake (INFLIGHT/PARKED/DONE), GenMC-proven in
 * verify/genmc/iouring_waitcommit.c.  A submitter that won't block the OS thread
 * CASes INFLIGHT->PARKED and coro_yields; a concurrent drainer exchanges
 * *->DONE and, iff it observed PARKED, wakes the parker.  Three states:
 *   INFLIGHT -> PARKED : submitter commits to park (CAS).
 *   INFLIGHT -> DONE   : a drainer (often the submitter's own inline drain)
 *                        completes the op before it parks.
 *   PARKED   -> DONE   : a drainer completes a parked op and wakes it.
 * DONE is terminal (the op leaves scope).  This table never drives op->wait
 * (the proven CAS/exchange still do); RUNLOOM_IOU_NOTE() asserts the edge under
 * -DRUNLOOM_FSM_VALIDATE and compiles to nothing otherwise. */
enum {
    RUNLOOM_IOU_EV_PARK = 0,   /* submitter CAS INFLIGHT -> PARKED            */
    RUNLOOM_IOU_EV_DONE,       /* drainer exchange * -> DONE                  */
    RUNLOOM_IOU_EV_COUNT
};
#define RUNLOOM_IOU_STATE_COUNT 3   /* INFLIGHT, PARKED, DONE */

static const signed char runloom_iou_table
        [RUNLOOM_IOU_STATE_COUNT][RUNLOOM_IOU_EV_COUNT]
        __attribute__((unused)) = {
    /*                                  PARK                       DONE */
    [RUNLOOM_IOURING_WAIT_INFLIGHT] = { RUNLOOM_IOURING_WAIT_PARKED, RUNLOOM_IOURING_WAIT_DONE },
    [RUNLOOM_IOURING_WAIT_PARKED]   = { RUNLOOM_FSM_INVALID,         RUNLOOM_IOURING_WAIT_DONE },
    [RUNLOOM_IOURING_WAIT_DONE]     = { RUNLOOM_FSM_INVALID,         RUNLOOM_FSM_INVALID       },
};
RUNLOOM_FSM_ASSERT_TABLE(runloom_iou_table, RUNLOOM_IOU_STATE_COUNT,
                         RUNLOOM_IOU_EV_COUNT, "iouring_wait");
#define RUNLOOM_IOU_NOTE(from, to)                                            \
    RUNLOOM_FSM_NOTE("iouring_wait", runloom_iou_table,                       \
                     RUNLOOM_IOU_STATE_COUNT, RUNLOOM_IOU_EV_COUNT, (from), (to))

/* ---- io_uring MULTISHOT recv handle lifecycle FSM (OBSERVATIONAL) -----------
 * A multishot recv handle's lifecycle, derived from {armed, eof, err}.  It arms
 * a multishot SQE, stays ARMED across every data CQE (F_MORE), and on the
 * terminal CQE (!F_MORE) ends in EOF (res==0), ERR (res<0), or UNARMED (a
 * non-terminal end like -ENOBUFS that ms_recv re-arms).  EOF/ERR are terminal
 * (ms_recv returns 0/-1 before re-arming).  We do NOT change the eof/err/armed
 * SETTING logic -- runloom_ms_state() merely DERIVES the state from the flags and
 * RUNLOOM_MS_NOTE() asserts the lifecycle edge under -DRUNLOOM_FSM_VALIDATE. */
enum {
    RUNLOOM_MS_ARMED = 0,   /* multishot SQE in flight                        */
    RUNLOOM_MS_UNARMED,     /* ended without a terminal status; re-armable    */
    RUNLOOM_MS_EOF,         /* orderly EOF (terminal)                         */
    RUNLOOM_MS_ERR,         /* sticky error (terminal)                        */
    RUNLOOM_MS_STATE_COUNT
};
enum {
    RUNLOOM_MS_EV_ARM = 0,  /* ms_submit re-arms the handle                   */
    RUNLOOM_MS_EV_CQE,      /* a CQE updates the handle (data / terminal)     */
    RUNLOOM_MS_EV_COUNT
};
static const signed char runloom_ms_table
        [RUNLOOM_MS_STATE_COUNT][RUNLOOM_MS_EV_COUNT]
        __attribute__((unused)) = {
    /*                       ARM                  CQE */
    [RUNLOOM_MS_ARMED]   = { RUNLOOM_FSM_INVALID, RUNLOOM_MS_ARMED   },  /* CQE: data(more) stays ARMED; terminal handled by the *_EOF/_ERR/_UNARMED rows below */
    [RUNLOOM_MS_UNARMED] = { RUNLOOM_MS_ARMED,    RUNLOOM_FSM_INVALID },
    [RUNLOOM_MS_EOF]     = { RUNLOOM_FSM_INVALID, RUNLOOM_FSM_INVALID },
    [RUNLOOM_MS_ERR]     = { RUNLOOM_FSM_INVALID, RUNLOOM_FSM_INVALID },
};
/* The CQE event can carry ARMED to {ARMED, UNARMED, EOF, ERR}.  The dense table
 * above lists ARMED->ARMED (the common data path); the terminal CQE edges
 * (ARMED->EOF / ARMED->ERR / ARMED->UNARMED) are legal too -- they are asserted
 * by RUNLOOM_MS_NOTE searching the row for ANY event mapping from->to, and EOF/
 * ERR/UNARMED are reachable destinations of the CQE event from ARMED.  To keep
 * the relation total without a multi-event explosion, RUNLOOM_MS_NOTE treats the
 * terminal edges as legal via the dedicated terminal-edge check below. */
RUNLOOM_FSM_ASSERT_TABLE(runloom_ms_table, RUNLOOM_MS_STATE_COUNT,
                         RUNLOOM_MS_EV_COUNT, "iouring_multishot");

/* Derive the multishot handle's lifecycle state from its flags (read under
 * h->lock at the call sites).  err wins over eof (matches the defensive
 * err-before-eof order in ms_recv); both terminal. */
static inline int runloom_ms_state(int armed, int eof, int err)
{
    if (err) return RUNLOOM_MS_ERR;
    if (eof) return RUNLOOM_MS_EOF;
    return armed ? RUNLOOM_MS_ARMED : RUNLOOM_MS_UNARMED;
}

/* Assert a multishot lifecycle edge.  The legal edges are: UNARMED->ARMED (arm)
 * and ARMED->{ARMED,UNARMED,EOF,ERR} (a CQE).  Implemented directly (rather than
 * via the generic NOTE) so the four CQE destinations from ARMED are all accepted
 * without a per-destination event.  Zero cost unless -DRUNLOOM_FSM_VALIDATE. */
#if defined(RUNLOOM_FSM_VALIDATE)
static inline void
runloom_ms_note_(int from, int to, const char *file, int line)
{
    int ok = 0;
    if (from == RUNLOOM_MS_UNARMED && to == RUNLOOM_MS_ARMED) ok = 1;       /* arm */
    else if (from == RUNLOOM_MS_ARMED &&
             (to == RUNLOOM_MS_ARMED   || to == RUNLOOM_MS_UNARMED ||
              to == RUNLOOM_MS_EOF     || to == RUNLOOM_MS_ERR)) ok = 1;    /* CQE */
    if (!ok) {
        fprintf(stderr, "\nRUNLOOM FSM VIOLATION [iouring_multishot]: illegal "
                "transition %d -> %d at %s:%d\n", from, to, file, line);
        fflush(stderr); abort();
    }
}
#  define RUNLOOM_MS_NOTE(from, to) \
       runloom_ms_note_((int)(from), (int)(to), __FILE__, __LINE__)
#else
#  define RUNLOOM_MS_NOTE(from, to) ((void)0)
#endif

#include "io_uring_l_buf.c.inc"
#include "io_uring_l_do.c.inc"
#include "io_uring_l_msclose.c.inc"
#include "io_uring_l_ring.c.inc"
#include "io_uring_l_loop.c.inc"
#else  /* !__linux__ */

#include <errno.h>
#include "io_uring.h"

int runloom_iouring_available(void) { return 0; }
int runloom_iouring_eventfd(void)   { return -1; }
void runloom_iouring_drain(void)    { /* no-op */ }
int runloom_iouring_inflight(void)  { return 0; }
int runloom_iouring_cancel_g(struct runloom_g *g) { (void)g; return 0; }
void runloom_iouring_submit_cancel_for_op(void *op) { (void)op; }

int runloom_iouring_pbuf_available(void) { return 0; }
unsigned runloom_iouring_pbuf_size(void) { return 0; }
unsigned runloom_iouring_pbuf_count(void) { return 0; }
void *runloom_iouring_pbuf_addr(unsigned bid) { (void)bid; return NULL; }
void runloom_iouring_pbuf_return(unsigned bid) { (void)bid; }

runloom_iouring_ms_t *runloom_iouring_ms_open(int fd) { (void)fd; return NULL; }
runloom_iouring_ssize_t runloom_iouring_ms_recv(runloom_iouring_ms_t *h,
                                          void *buf, size_t n)
{
    (void)h; (void)buf; (void)n;
    errno = ENOSYS;
    return -1;
}
void runloom_iouring_ms_close(runloom_iouring_ms_t *h) { (void)h; }

runloom_iouring_ssize_t runloom_iouring_pread(int fd, void *buf, size_t n, runloom_iouring_off_t offset)
{
    (void)fd; (void)buf; (void)n; (void)offset;
    errno = ENOSYS;
    return -1;
}

runloom_iouring_ssize_t runloom_iouring_pwrite(int fd, const void *buf, size_t n, runloom_iouring_off_t offset)
{
    (void)fd; (void)buf; (void)n; (void)offset;
    errno = ENOSYS;
    return -1;
}

runloom_iouring_ssize_t runloom_iouring_recv(int fd, void *buf, size_t n, int flags)
{
    (void)fd; (void)buf; (void)n; (void)flags;
    errno = ENOSYS;
    return -1;
}

runloom_iouring_ssize_t runloom_iouring_send(int fd, const void *buf, size_t n, int flags)
{
    (void)fd; (void)buf; (void)n; (void)flags;
    errno = ENOSYS;
    return -1;
}

/* Per-hub ring stubs (Linux-only feature; safe no-ops elsewhere). */
runloom_iouring_ring_t *runloom_iouring_ring_create(int defer_taskrun)
{
    (void)defer_taskrun;
    errno = ENOSYS;
    return NULL;
}
void runloom_iouring_ring_destroy(runloom_iouring_ring_t *r) { (void)r; }
int  runloom_iouring_ring_eventfd(const runloom_iouring_ring_t *r) { (void)r; return -1; }
int  runloom_iouring_ring_inflight(const runloom_iouring_ring_t *r) { (void)r; return 0; }
void runloom_iouring_ring_drain(runloom_iouring_ring_t *r) { (void)r; }
void runloom_iouring_ring_get_events(runloom_iouring_ring_t *r) { (void)r; }
runloom_iouring_ssize_t runloom_iouring_ring_recv(runloom_iouring_ring_t *r,
                                            int fd, void *buf, size_t n, int flags)
{
    (void)r; (void)fd; (void)buf; (void)n; (void)flags;
    errno = ENOSYS;
    return -1;
}
runloom_iouring_ssize_t runloom_iouring_ring_send(runloom_iouring_ring_t *r,
                                            int fd, const void *buf, size_t n, int flags)
{
    (void)r; (void)fd; (void)buf; (void)n; (void)flags;
    errno = ENOSYS;
    return -1;
}

/* io_uring-as-loop backend stubs (Linux-only feature).  enabled()/ms_enabled()
 * return 0 so the hub idle path and the all-C echo never take the loop path on
 * these platforms; the rest are unreachable no-ops kept for linking, since
 * mn_sched.c / module_io.c.inc reference them unconditionally (runtime-gated). */
int runloom_iouring_loop_enabled(void)    { return 0; }
int runloom_iouring_loop_ms_enabled(void) { return 0; }
int runloom_iouring_any_enabled(void)     { return 0; }
int runloom_iouring_loop_hub_arm(runloom_iouring_ring_t *r, int epoll_fd)
{
    (void)r; (void)epoll_fd; return -1;
}
void runloom_iouring_loop_wait(runloom_iouring_ring_t *r, long long timeout_ns,
                               int *flags_out)
{
    (void)r; (void)timeout_ns; (void)flags_out;
}
void runloom_iouring_loop_wake(int wake_fd) { (void)wake_fd; }
void runloom_iouring_loop_hub_disarm(runloom_iouring_ring_t *r) { (void)r; }
runloom_iouring_ssize_t runloom_iouring_loop_recv(runloom_iouring_ring_t *r,
                                                  int fd, void *buf, size_t n,
                                                  int flags)
{
    (void)r; (void)fd; (void)buf; (void)n; (void)flags;
    errno = ENOSYS; return -1;
}
runloom_iouring_ssize_t runloom_iouring_loop_send(runloom_iouring_ring_t *r,
                                                  int fd, const void *buf,
                                                  size_t n, int flags)
{
    (void)r; (void)fd; (void)buf; (void)n; (void)flags;
    errno = ENOSYS; return -1;
}
void *runloom_iouring_loop_ms_open(runloom_iouring_ring_t *r, int fd)
{
    (void)r; (void)fd; return NULL;
}
runloom_iouring_ssize_t runloom_iouring_loop_ms_recv(void *handle,
                                                     void *buf, size_t n)
{
    (void)handle; (void)buf; (void)n;
    errno = ENOSYS; return -1;
}
void runloom_iouring_loop_ms_close(void *handle) { (void)handle; }

#endif
