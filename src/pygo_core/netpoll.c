/* netpoll.c -- portable I/O multiplexing.
 *
 * Backends, picked by plat.h:
 *   Linux           -> epoll
 *   macOS / *BSD    -> kqueue
 *   Windows         -> WSAPoll (sockets) -- POSIX poll()-shaped Winsock API,
 *                      Vista+, no FD_SETSIZE cap.  IOCP would be more
 *                      efficient for the file-handle case but only sockets
 *                      flow through wait_fd today (regular files go to the
 *                      thread-pool backend in monkey.py).
 *   else            -> select() POSIX fallback
 *
 * Park mechanics:
 *   - the current goroutine snapshot tstate, gets pushed onto an internal
 *     "parked" list with (fd, events, deadline) metadata.  yields via
 *     pygo_coro_yield.
 *   - the scheduler's drain loop, when ready queue is empty, calls
 *     pygo_netpoll_pump(timeout) instead of sleeping the OS thread.
 *   - pump waits for I/O / timeout, wakes parked goroutines, returns.
 */
#if !defined(_WIN32)
#  define _POSIX_C_SOURCE 200809L
#endif
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "plat.h"
#include "plat_compat.h"
#include "netpoll.h"
#include "coro.h"
#include "pygo_sched.h"
#include "mn_sched.h"
#include "io_uring.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#if defined(PYGO_HAVE_EPOLL)
#  include <sys/epoll.h>
#  include <unistd.h>
#elif defined(PYGO_HAVE_KQUEUE)
#  include <sys/event.h>
#  include <sys/time.h>
#  include <unistd.h>
#elif defined(PYGO_OS_WINDOWS)
   /* winsock2.h, ws2tcpip.h and windows.h are already pulled in via
    * plat_compat.h.  WSAPoll + WSAPOLLFD live in winsock2.h, FD_SET /
    * FD_ISSET likewise -- no extra header needed here. */
#else
#  include <sys/select.h>
#  include <unistd.h>
#endif

/* ---- internal park record ----
 * Allocated on the parked goroutine's C stack inside pygo_netpoll_wait_fd
 * (the stack stays alive across yield because the goroutine isn't
 * destroyed until it completes).
 *
 * Two intrusive links:
 *   * `next` / `slot` into the global parked list (used by drain_parked
 *     and the timeout sweep).  Uses the "slot pointer" trick for O(1)
 *     unlink; the head pointer lives in a static, so its address is
 *     stable.
 *   * `next_by_fd` / `prev_by_fd` into the per-fd bucket (the hot path:
 *     epoll-event → matching parker → wake).  Doubly-linked because
 *     the bucket-head pointer lives inside a realloc-able heap array,
 *     and the slot-pointer trick can't survive a realloc that moves
 *     the array. */
typedef struct pygo_parked {
    int fd;
    int events;
    long long deadline_ns;     /* -1 = forever */
    int *ready_out;            /* where to store the wakeup mask */
    pygo_g_t *g;
    void *hub;                  /* M:N hub opaque; NULL = global sched */
    struct pygo_parked  *next;
    struct pygo_parked **slot;
    struct pygo_parked  *next_by_fd;
    struct pygo_parked  *prev_by_fd;
} pygo_parked_t;

/* Forcibly wake all parked goroutines with a cancelled marker.
 * Returns count of waiters woken.  Used by sched_reset() so paio.run
 * cleanup doesn't leave the next pygo_core.run() blocking on parked
 * accept loops / tickers / etc. */
int pygo_netpoll_drain_parked(void);

/* Shared parked list + lock.  Under M:N multiple hubs concurrently
 * call wait_fd (park.add) and pump (park.remove); without the lock
 * the singly-linked list corrupts.  Single-thread sched takes the
 * lock too but no contention. */
static pygo_parked_t *pygo_parked_head = NULL;
static int pygo_parked_total = 0;       /* read with __atomic_load_n */
static int pygo_netpoll_inited = 0;
static pygo_mutex_t pygo_parked_lock;
static volatile long pygo_parked_lock_inited = 0;

/* ---- per-fd parker index ----
 * Sparse array indexed by fd; each slot holds the head of a singly-
 * linked list of parkers interested in events on that fd (usually 1,
 * occasionally 2 for read+write).  Replaces the prior O(N) walk of
 * pygo_parked_head on every epoll event with an O(1) bucket lookup
 * + O(parkers-on-this-fd) walk.  At N=1024 concurrent conns this
 * changes the pump from O(N*events) to O(events).
 *
 * Protected by pygo_parked_lock, same as the global list. */
static pygo_parked_t **pygo_parked_by_fd = NULL;
static size_t          pygo_parked_by_fd_cap = 0;

static int pygo_parker_fd_index_ensure(int fd)
{
    if (fd < 0) return -1;
    if ((size_t)fd < pygo_parked_by_fd_cap) return 0;
    {
        size_t newcap = pygo_parked_by_fd_cap ? pygo_parked_by_fd_cap * 2 : 256;
        pygo_parked_t **nb;
        while (newcap <= (size_t)fd) newcap *= 2;
        nb = (pygo_parked_t **)realloc(pygo_parked_by_fd,
                                       newcap * sizeof(*nb));
        if (nb == NULL) return -1;
        memset(nb + pygo_parked_by_fd_cap, 0,
               (newcap - pygo_parked_by_fd_cap) * sizeof(*nb));
        pygo_parked_by_fd     = nb;
        pygo_parked_by_fd_cap = newcap;
    }
    return 0;
}

/* Link p into both the global list and its per-fd bucket.  Caller
 * holds pygo_parked_lock. */
static void pygo_parker_link(pygo_parked_t *p)
{
    /* Global list: push at head, slot-pointer trick. */
    p->next = pygo_parked_head;
    if (p->next != NULL) p->next->slot = &p->next;
    pygo_parked_head = p;
    p->slot = &pygo_parked_head;

    /* Per-fd bucket: push at head, doubly-linked.  If the realloc
     * failed we still keep the parker on the global list (slow path
     * walks the global list to find it); subsequent epoll events on
     * this fd just won't find it through the fast path. */
    p->prev_by_fd = NULL;
    p->next_by_fd = NULL;
    if (p->fd >= 0 && pygo_parker_fd_index_ensure(p->fd) == 0) {
        pygo_parked_t *head = pygo_parked_by_fd[p->fd];
        p->next_by_fd = head;
        if (head != NULL) head->prev_by_fd = p;
        pygo_parked_by_fd[p->fd] = p;
    }
}

/* Unlink p from both lists.  Caller holds pygo_parked_lock. */
static void pygo_parker_unlink(pygo_parked_t *p)
{
    if (p->slot != NULL) {
        *p->slot = p->next;
        if (p->next != NULL) p->next->slot = p->slot;
        p->slot = NULL;
        p->next = NULL;
    }
    if (p->prev_by_fd != NULL) {
        p->prev_by_fd->next_by_fd = p->next_by_fd;
    } else if (p->fd >= 0 && (size_t)p->fd < pygo_parked_by_fd_cap &&
               pygo_parked_by_fd[p->fd] == p) {
        pygo_parked_by_fd[p->fd] = p->next_by_fd;
    }
    if (p->next_by_fd != NULL) p->next_by_fd->prev_by_fd = p->prev_by_fd;
    p->prev_by_fd = NULL;
    p->next_by_fd = NULL;
}

/* ---- per-fd registration cache ----
 * One bit per fd; set when we've already issued EPOLL_CTL_ADD (or
 * the kqueue equivalent) for this fd as edge-triggered for both
 * READ and WRITE.  Subsequent wait_fd calls then skip the
 * epoll_ctl syscall entirely -- the kernel keeps reporting edges
 * until the fd is closed (which auto-clears the registration).
 *
 * Protected by pygo_parked_lock for write; reads under the lock
 * to keep the check + ADD atomic against concurrent registers
 * for the same fd. */
static unsigned char *pygo_fd_registered_bm = NULL;
static size_t         pygo_fd_registered_cap_bytes = 0;

static int pygo_fd_bit_get(int fd)
{
    if (fd < 0) return 0;
    size_t byte = (size_t)fd >> 3;
    if (byte >= pygo_fd_registered_cap_bytes) return 0;
    return (pygo_fd_registered_bm[byte] >> (fd & 7)) & 1;
}

static int pygo_fd_bit_set(int fd)
{
    if (fd < 0) return -1;
    size_t byte = (size_t)fd >> 3;
    if (byte >= pygo_fd_registered_cap_bytes) {
        size_t newcap = pygo_fd_registered_cap_bytes ? pygo_fd_registered_cap_bytes * 2 : 256;
        while (newcap <= byte) newcap *= 2;
        unsigned char *nb = (unsigned char *)realloc(pygo_fd_registered_bm, newcap);
        if (nb == NULL) return -1;
        memset(nb + pygo_fd_registered_cap_bytes, 0,
               newcap - pygo_fd_registered_cap_bytes);
        pygo_fd_registered_bm        = nb;
        pygo_fd_registered_cap_bytes = newcap;
    }
    pygo_fd_registered_bm[byte] |= (unsigned char)(1u << (fd & 7));
    return 0;
}

static void pygo_fd_bit_clear(int fd)
{
    if (fd < 0) return;
    size_t byte = (size_t)fd >> 3;
    if (byte >= pygo_fd_registered_cap_bytes) return;
    pygo_fd_registered_bm[byte] &= (unsigned char)~(1u << (fd & 7));
}

#if defined(PYGO_HAVE_EPOLL)
static int pygo_epoll_fd = -1;
/* Eventfd registered by io_uring.c; events on this fd are dispatched
 * to pygo_iouring_drain() instead of the normal parked-list walk.
 * -1 = none registered. */
static int pygo_iouring_eventfd_in_epoll = -1;
#elif defined(PYGO_HAVE_KQUEUE)
static int pygo_kqueue_fd = -1;
#elif defined(PYGO_OS_WINDOWS)
#  include "netpoll_iocp.h"
/* Runtime-selected Windows backend.  Tier order:
 *   1. IOCP+AFD via \Device\Afd -- fastest at scale, O(1) per
 *      ready socket.  Requires NtDeviceIoControlFile (NT 4.0+).
 *   2. WSAPoll -- Vista+, no FD_SETSIZE cap, linear over fds.
 *   3. select() -- XP / Server 2003 fallback.
 *
 * Selection happens once in pygo_netpoll_init; backend_name is
 * exported via pygo_core.netpoll_backend() for introspection. */
typedef int (WSAAPI *pygo_wsapoll_fn)(LPWSAPOLLFD, ULONG, INT);
static pygo_wsapoll_fn pygo_win_wsapoll = NULL;
static int             pygo_win_use_iocp = 0;
static const char     *pygo_win_backend_name = "select";   /* updated by init */
#endif

/* Initialise the lock once, regardless of platform.  POSIX could use
 * PTHREAD_MUTEX_INITIALIZER and skip this, but Windows CRITICAL_SECTION
 * has no static-init form so the lazy-init pattern is uniform. */
static void pygo_parked_lock_ensure_inited(void)
{
#if defined(PYGO_OS_WINDOWS)
    /* InterlockedCompareExchange returns the prior value; only the
     * first caller transitions 0 -> 1 and runs the init. */
    if (InterlockedCompareExchange(&pygo_parked_lock_inited, 1, 0) == 0) {
        pygo_mutex_init(&pygo_parked_lock);
    } else {
        /* Spin briefly while another thread finishes init.  In practice
         * the init is one InitializeCriticalSection call (~100 ns), so
         * any starvation here is bounded. */
        while (pygo_parked_lock_inited != 2) { /* spin */ }
        return;
    }
    pygo_parked_lock_inited = 2;
#else
    if (__atomic_load_n(&pygo_parked_lock_inited, __ATOMIC_ACQUIRE) == 2) {
        return;
    }
    long expected = 0;
    if (__atomic_compare_exchange_n(&pygo_parked_lock_inited, &expected, 1,
                                    0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        pygo_mutex_init(&pygo_parked_lock);
        __atomic_store_n(&pygo_parked_lock_inited, 2, __ATOMIC_RELEASE);
    } else {
        while (__atomic_load_n(&pygo_parked_lock_inited, __ATOMIC_ACQUIRE) != 2)
            { /* spin */ }
    }
#endif
}

const char *pygo_netpoll_backend(void)
{
#if defined(PYGO_HAVE_EPOLL)
    return "epoll";
#elif defined(PYGO_HAVE_KQUEUE)
    return "kqueue";
#elif defined(PYGO_OS_WINDOWS)
    /* Force init so the IOCP/WSAPoll probe runs.  Without this the
     * default string ("select") is returned even when IOCP would
     * succeed -- the actual init only runs on first wait. */
    if (!pygo_netpoll_inited) pygo_netpoll_init();
    return pygo_win_backend_name;
#else
    return "select";
#endif
}

int pygo_netpoll_init(void)
{
    if (pygo_netpoll_inited) return 0;
    pygo_parked_lock_ensure_inited();
#if defined(PYGO_HAVE_EPOLL)
    pygo_epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (pygo_epoll_fd < 0) return -1;
#elif defined(PYGO_HAVE_KQUEUE)
    pygo_kqueue_fd = kqueue();
    if (pygo_kqueue_fd < 0) return -1;
#elif defined(PYGO_OS_WINDOWS)
    /* Bring up Winsock once.  Idempotent via plat_compat's
     * InterlockedCompareExchange guard. */
    pygo_winsock_init();
    /* Backend selection on Windows.  Default tier order:
     *   1. IOCP+AFD     - NT 5.1+; O(1) per ready socket, scales to 10k+
     *   2. WSAPoll      - Vista+ fallback (linear walk per call)
     *   3. select()     - XP / restricted-sandbox fallback (FD_SETSIZE cap)
     *
     *   PYGO_NETPOLL=wsapoll  -> force WSAPoll
     *   PYGO_NETPOLL=select   -> force select() */
    {
        const char *env = getenv("PYGO_NETPOLL");
        int want_wsapoll = (env != NULL && strcmp(env, "wsapoll") == 0);
        int want_select  = (env != NULL && strcmp(env, "select")  == 0);

        if (!want_wsapoll && !want_select && pygo_iocp_init() == 0) {
            pygo_win_use_iocp = 1;
            pygo_win_backend_name = "iocp-afd";
        } else if (want_select) {
            pygo_win_backend_name = "select";
        } else {
            HMODULE ws2 = GetModuleHandleA("ws2_32.dll");
            if (ws2 == NULL) ws2 = LoadLibraryA("ws2_32.dll");
            if (ws2 != NULL) {
                pygo_win_wsapoll = (pygo_wsapoll_fn)
                    (void *)GetProcAddress(ws2, "WSAPoll");
            }
            pygo_win_backend_name = (pygo_win_wsapoll != NULL) ? "wsapoll" : "select";
        }
    }
#endif
    pygo_netpoll_inited = 1;
    return 0;
}

void pygo_netpoll_fini(void)
{
#if defined(PYGO_HAVE_EPOLL)
    if (pygo_epoll_fd >= 0) { close(pygo_epoll_fd); pygo_epoll_fd = -1; }
#elif defined(PYGO_HAVE_KQUEUE)
    if (pygo_kqueue_fd >= 0) { close(pygo_kqueue_fd); pygo_kqueue_fd = -1; }
#elif defined(PYGO_OS_WINDOWS)
    if (pygo_win_use_iocp) {
        pygo_iocp_fini();
        pygo_win_use_iocp = 0;
    }
    /* WSAPoll / select are stateless; nothing else to close.
     * Winsock itself is left up by design (see pygo_winsock_init). */
#endif
    pygo_netpoll_inited = 0;
}

static long long monotonic_ns(void)
{
    return pygo_monotonic_ns();
}

/* ---- registration ----
 * Edge-triggered, register-once.  On epoll/kqueue the fd is ADDed
 * exactly once with both READ and WRITE arms in ET mode.  All
 * subsequent wait_fd calls just consult the bitmap and skip the
 * epoll_ctl/kevent syscall -- the kernel keeps reporting edges
 * until the fd closes.
 *
 * Safety: the caller MUST try the operation first and only call
 * wait_fd after EAGAIN.  That serialises the "kernel observed not-
 * ready" state with our parking, so the next not-ready->ready
 * transition is guaranteed to deliver an edge.  This is the same
 * pattern Go's netpoll uses.
 *
 * Stale-fd recovery: socket close auto-clears the kernel
 * registration when the last fd reference goes away.  monkey.py's
 * close hook calls pygo_netpoll_unregister so the bitmap stays in
 * sync with the kernel for fd reuse. */
static int pygo_netpoll_register(int fd, int events)
{
    (void)events;   /* both arms always registered; events filtered at wake */
#if defined(PYGO_HAVE_EPOLL)
    int need_register;

    pygo_mutex_lock(&pygo_parked_lock);
    if (pygo_fd_bit_get(fd)) {
        pygo_mutex_unlock(&pygo_parked_lock);
        return 0;
    }
    if (pygo_fd_bit_set(fd) != 0) {
        pygo_mutex_unlock(&pygo_parked_lock);
        errno = ENOMEM;
        return -1;
    }
    need_register = 1;
    pygo_mutex_unlock(&pygo_parked_lock);
    (void)need_register;

    {
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP;
        ev.data.fd = fd;
        if (epoll_ctl(pygo_epoll_fd, EPOLL_CTL_ADD, fd, &ev) == 0) return 0;
        /* Stale registration from before the bit was cleared (e.g.
         * dup'd fd, or close-hook missed).  MOD into ET both-arms. */
        if (errno == EEXIST) {
            if (epoll_ctl(pygo_epoll_fd, EPOLL_CTL_MOD, fd, &ev) == 0) return 0;
        }
    }
    /* Failed: drop the bit so a future caller can retry. */
    pygo_mutex_lock(&pygo_parked_lock);
    pygo_fd_bit_clear(fd);
    pygo_mutex_unlock(&pygo_parked_lock);
    return -1;
#elif defined(PYGO_HAVE_KQUEUE)
    pygo_mutex_lock(&pygo_parked_lock);
    if (pygo_fd_bit_get(fd)) {
        pygo_mutex_unlock(&pygo_parked_lock);
        return 0;
    }
    if (pygo_fd_bit_set(fd) != 0) {
        pygo_mutex_unlock(&pygo_parked_lock);
        errno = ENOMEM;
        return -1;
    }
    pygo_mutex_unlock(&pygo_parked_lock);
    {
        struct kevent kev[2];
        EV_SET(&kev[0], fd, EVFILT_READ,  EV_ADD | EV_CLEAR, 0, 0, NULL);
        EV_SET(&kev[1], fd, EVFILT_WRITE, EV_ADD | EV_CLEAR, 0, 0, NULL);
        if (kevent(pygo_kqueue_fd, kev, 2, NULL, 0, NULL) == 0) return 0;
    }
    pygo_mutex_lock(&pygo_parked_lock);
    pygo_fd_bit_clear(fd);
    pygo_mutex_unlock(&pygo_parked_lock);
    return -1;
#else
    (void)fd;
    return 0;  /* select doesn't need pre-registration */
#endif
}

void pygo_netpoll_unregister(int fd)
{
#if defined(PYGO_HAVE_EPOLL) || defined(PYGO_HAVE_KQUEUE)
    pygo_mutex_lock(&pygo_parked_lock);
    pygo_fd_bit_clear(fd);
    pygo_mutex_unlock(&pygo_parked_lock);
    /* No kernel syscall: epoll/kqueue auto-remove the fd when the
     * last reference closes.  Calling EPOLL_CTL_DEL after close
     * would race with fd reuse anyway. */
#else
    (void)fd;
#endif
}

int pygo_netpoll_parked_count(void)
{
    return __atomic_load_n(&pygo_parked_total, __ATOMIC_ACQUIRE);
}

int pygo_netpoll_add_iouring_eventfd(int fd)
{
#if defined(PYGO_HAVE_EPOLL)
    struct epoll_event ev;
    if (fd < 0) return -1;
    if (pygo_netpoll_init() != 0) return -1;
    /* Idempotent: if the same fd is already registered, skip the
     * epoll_ctl call.  io_uring.c only ever creates one eventfd per
     * process, so this matters only if init runs twice. */
    if (pygo_iouring_eventfd_in_epoll == fd) return 0;
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = fd;
    if (epoll_ctl(pygo_epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        if (errno != EEXIST) return -1;
    }
    pygo_iouring_eventfd_in_epoll = fd;
    return 0;
#else
    (void)fd;
    return 0;     /* iouring is Linux-only; non-epoll backends never hit this */
#endif
}

/* Forcibly wake every parked goroutine with ready_mask=-1 (cancelled).
 * Each waiter's pygo_netpoll_wait_fd call returns -1; callers (server
 * accept loops, etc.) see that and exit their loops.  Returns count
 * woken. */
int pygo_netpoll_drain_parked(void)
{
    int n = 0;
    pygo_parked_t *p, *next;
    pygo_mutex_lock(&pygo_parked_lock);
    p = pygo_parked_head;
    pygo_parked_head = NULL;
    /* Clear per-fd buckets too; everything is leaving the lists. */
    if (pygo_parked_by_fd != NULL && pygo_parked_by_fd_cap > 0) {
        memset(pygo_parked_by_fd, 0,
               pygo_parked_by_fd_cap * sizeof(*pygo_parked_by_fd));
    }
    while (p != NULL) {
        next = p->next;
        if (p->ready_out != NULL) {
            *p->ready_out = -1;   /* signal cancellation */
        }
        p->next = NULL;
        p->slot = NULL;
        p->next_by_fd = NULL;
        p->prev_by_fd = NULL;
        if (p->hub != NULL) {
            pygo_mn_wake_g(p->hub, p->g);
        } else {
            pygo_sched_wake(p->g);
        }
        __atomic_sub_fetch(&pygo_parked_total, 1, __ATOMIC_RELEASE);
        p = next;
        n++;
    }
    pygo_mutex_unlock(&pygo_parked_lock);
    return n;
}

/* ---- park / wake ---- */
int pygo_netpoll_wait_fd(int fd, int events, long long timeout_ns)
{
    pygo_parked_t park;
    pygo_sched_t *s;
    int ready_mask = 0;
    void *hub_opaque;
    pygo_g_t *current_g;

    if (pygo_netpoll_init() != 0) return -1;
#if defined(PYGO_OS_WINDOWS)
    /* IOCP-AFD: submit the poll request now; pump just drains
     * completions.  Falls through to the no-op register for the
     * WSAPoll / select paths. */
    if (pygo_win_use_iocp) {
        if (pygo_iocp_submit(fd, events, timeout_ns) != 0) return -1;
    } else {
        if (pygo_netpoll_register(fd, events) != 0) return -1;
    }
#else
    if (pygo_netpoll_register(fd, events) != 0) return -1;
#endif

    /* Determine where this g lives so pump can route the wake.
     * If we're inside an M:N hub, the hub TLS gives us the target;
     * otherwise it's the global single-thread scheduler. */
    hub_opaque = pygo_mn_current_hub_opaque();
    s = pygo_sched_get();
    if (hub_opaque != NULL) {
        /* Hub context: current g is in TLS (set by hub_main).  We
         * don't read s->current because that's the single-thread
         * sched's slot, not the hub's. */
        current_g = pygo_mn_tls_current_g();
    } else {
        current_g = s->current;
    }

    park.fd = fd;
    park.events = events;
    park.deadline_ns = timeout_ns < 0 ? -1 : monotonic_ns() + timeout_ns;
    park.ready_out = &ready_mask;
    park.g = current_g;
    park.hub = hub_opaque;
    park.next = NULL;
    park.slot = NULL;
    park.next_by_fd = NULL;
    park.prev_by_fd = NULL;

    pygo_mutex_lock(&pygo_parked_lock);
    pygo_parker_link(&park);
    __atomic_add_fetch(&pygo_parked_total, 1, __ATOMIC_RELEASE);
    pygo_mutex_unlock(&pygo_parked_lock);

    /* Snapshot tstate (same as pygo_sched_yield does) so the next
     * resume restores it.  Then yield WITHOUT re-queueing -- pump
     * pushes us back when the fd becomes ready. */
    if (current_g != NULL) {
        pygo_sched_park_current();
    }
    pygo_coro_yield();
    /* On wake: pump set ready_mask, removed us from parked list. */
    return ready_mask;
}

int pygo_netpoll_pump(long long timeout_ns)
{
    long long now;
    long long min_deadline = -1;
    int woke = 0;

    if (!pygo_netpoll_inited) {
        if (pygo_netpoll_init() != 0) return -1;
    }

    /* Find earliest deadline among parked goroutines.  Held briefly
     * to walk the list. */
    pygo_mutex_lock(&pygo_parked_lock);
    {
        pygo_parked_t *p;
        for (p = pygo_parked_head; p != NULL; p = p->next) {
            if (p->deadline_ns < 0) continue;
            if (min_deadline < 0 || p->deadline_ns < min_deadline) {
                min_deadline = p->deadline_ns;
            }
        }
    }
    pygo_mutex_unlock(&pygo_parked_lock);

    now = monotonic_ns();
    if (min_deadline >= 0) {
        long long until = min_deadline - now;
        if (until < 0) until = 0;
        if (timeout_ns < 0 || until < timeout_ns) timeout_ns = until;
    }

#if defined(PYGO_HAVE_EPOLL)
    {
        struct epoll_event evs[64];
        int n;
        int ms = timeout_ns < 0 ? -1 :
                 (timeout_ns > 1000000000LL ? 1000 : (int)(timeout_ns / 1000000LL));
        Py_BEGIN_ALLOW_THREADS
        n = epoll_wait(pygo_epoll_fd, evs, 64, ms);
        Py_END_ALLOW_THREADS
        if (n > 0) {
            int i;
            /* Lock parked list for walk + remove.  Order: parked_lock
             * then hub->sub_lock (inside pygo_mn_wake_g).  Don't take
             * locks in the reverse order anywhere. */
            pygo_mutex_lock(&pygo_parked_lock);
            for (i = 0; i < n; i++) {
                int fd = evs[i].data.fd;
                int mask = 0;
                pygo_parked_t *bucket, *p;
                /* io_uring eventfd: drain its counter and walk the CQ
                 * ring to wake parked goroutines via their per-op
                 * record.  Not a normal fd-park entry; skip the
                 * parked-list walk. */
                if (pygo_iouring_eventfd_in_epoll >= 0 &&
                    fd == pygo_iouring_eventfd_in_epoll) {
                    pygo_mutex_unlock(&pygo_parked_lock);
                    pygo_iouring_drain();
                    pygo_mutex_lock(&pygo_parked_lock);
                    continue;
                }
                if (evs[i].events & EPOLLIN)  mask |= PYGO_NETPOLL_READ;
                if (evs[i].events & EPOLLOUT) mask |= PYGO_NETPOLL_WRITE;
                /* O(1) bucket lookup; walk only the parkers on this fd
                 * (usually 1 -- at most a read+write pair). */
                if ((size_t)fd >= pygo_parked_by_fd_cap) continue;
                bucket = pygo_parked_by_fd[fd];
                p = bucket;
                while (p != NULL) {
                    pygo_parked_t *next_p = p->next_by_fd;
                    if (p->events & mask) {
                        *(p->ready_out) = mask & p->events;
                        pygo_parker_unlink(p);
                        __atomic_sub_fetch(&pygo_parked_total, 1, __ATOMIC_RELEASE);
                        pygo_mn_wake_g(p->hub, p->g);
                        woke++;
                        break;
                    }
                    p = next_p;
                }
            }
            pygo_mutex_unlock(&pygo_parked_lock);
        }
    }
#elif defined(PYGO_HAVE_KQUEUE)
    {
        struct kevent evs[64];
        struct timespec ts;
        struct timespec *tsp = NULL;
        int n;
        if (timeout_ns >= 0) {
            ts.tv_sec = (time_t)(timeout_ns / 1000000000LL);
            ts.tv_nsec = (long)(timeout_ns % 1000000000LL);
            tsp = &ts;
        }
        Py_BEGIN_ALLOW_THREADS
        n = kevent(pygo_kqueue_fd, NULL, 0, evs, 64, tsp);
        Py_END_ALLOW_THREADS
        if (n > 0) {
            int i;
            pygo_mutex_lock(&pygo_parked_lock);
            for (i = 0; i < n; i++) {
                int fd = (int)evs[i].ident;
                int mask = (evs[i].filter == EVFILT_READ) ?
                           PYGO_NETPOLL_READ : PYGO_NETPOLL_WRITE;
                pygo_parked_t *p;
                /* Per-fd bucket lookup: O(parkers-on-this-fd). */
                if ((size_t)fd >= pygo_parked_by_fd_cap) continue;
                p = pygo_parked_by_fd[fd];
                while (p != NULL) {
                    pygo_parked_t *next_p = p->next_by_fd;
                    if (p->events & mask) {
                        *(p->ready_out) = mask & p->events;
                        pygo_parker_unlink(p);
                        __atomic_sub_fetch(&pygo_parked_total, 1, __ATOMIC_RELEASE);
                        pygo_mn_wake_g(p->hub, p->g);
                        woke++;
                        break;
                    }
                    p = next_p;
                }
            }
            pygo_mutex_unlock(&pygo_parked_lock);
        }
    }
#elif defined(PYGO_OS_WINDOWS)
    /* Windows backend.  Runtime-chosen between IOCP+AFD (NT 4.0+,
     * O(1) per ready socket), WSAPoll (Vista+, no FD_SETSIZE cap)
     * and select() (XP / older fallback).
     *
     * Important: Windows fds passed in here MUST be SOCKET handles
     * (returned by socket.socket.fileno()).  Pipe/file fds are NOT
     * pollable through Winsock -- the monkey-patch layer routes those
     * to the thread-pool backend in monkey.py. */
    if (pygo_win_use_iocp) {
        /* --- IOCP+AFD drain ---
         *
         * Every wait_fd call submitted its own AFD_POLL IRP at park
         * time (see pygo_netpoll_wait_fd's Windows branch).  Pump
         * just drains completions and wakes the matching gs.  No
         * fd_set assembly, no linear walk over parked entries to
         * test readiness -- the kernel only signals what's ready.
         *
         * Loop: pull as many completions as are immediately
         * available, then return.  The hub_main outer loop calls
         * pump again with a fresh timeout if it needs to wait. */
        long long deadline = timeout_ns;
        int local_woke = 0;
        while (1) {
            int fd, evs;
            int rc;
            long long step = (local_woke == 0) ? deadline : 0;
            Py_BEGIN_ALLOW_THREADS
            rc = pygo_iocp_wait(step, &fd, &evs);
            Py_END_ALLOW_THREADS
            if (rc <= 0) break;             /* 0 = timeout, -1 = error */
            local_woke++;
            /* Locate the matching parked entry and wake its g. */
            pygo_mutex_lock(&pygo_parked_lock);
            if ((size_t)fd < pygo_parked_by_fd_cap) {
                pygo_parked_t *p = pygo_parked_by_fd[fd];
                while (p != NULL) {
                    pygo_parked_t *next_p = p->next_by_fd;
                    if (p->events & evs) {
                        *(p->ready_out) = evs & p->events;
                        pygo_parker_unlink(p);
                        __atomic_sub_fetch(&pygo_parked_total, 1,
                                           __ATOMIC_RELEASE);
                        pygo_mn_wake_g(p->hub, p->g);
                        woke++;
                        break;
                    }
                    p = next_p;
                }
            }
            pygo_mutex_unlock(&pygo_parked_lock);
        }
    } else if (pygo_win_wsapoll != NULL) {
        /* --- WSAPoll path --- */
        WSAPOLLFD fds_stack[128];
        WSAPOLLFD *fds = fds_stack;
        ULONG fds_cap = 128;
        ULONG n_fds = 0;
        int ms = timeout_ns < 0 ? -1 :
                 (timeout_ns > 1000000000LL ? 1000 :
                  (int)(timeout_ns / 1000000LL));
        int rc;
        pygo_parked_t *p;

        pygo_mutex_lock(&pygo_parked_lock);
        /* Two passes: count, then fill (grows the heap buffer if the
         * stack one isn't big enough).  Worst case we re-malloc once. */
        {
            ULONG need = 0;
            for (p = pygo_parked_head; p != NULL; p = p->next) need++;
            if (need > fds_cap) {
                fds = (WSAPOLLFD *)malloc(sizeof(WSAPOLLFD) * need);
                if (fds == NULL) {
                    pygo_mutex_unlock(&pygo_parked_lock);
                    goto post_wait;   /* skip wait; deadlines still get checked */
                }
                fds_cap = need;
            }
        }
        for (p = pygo_parked_head; p != NULL; p = p->next) {
            fds[n_fds].fd = (SOCKET)p->fd;
            fds[n_fds].events = 0;
            if (p->events & PYGO_NETPOLL_READ)  fds[n_fds].events |= POLLRDNORM;
            if (p->events & PYGO_NETPOLL_WRITE) fds[n_fds].events |= POLLWRNORM;
            fds[n_fds].revents = 0;
            n_fds++;
        }
        if (n_fds > 0) {
            Py_BEGIN_ALLOW_THREADS
            rc = pygo_win_wsapoll(fds, n_fds, ms);
            Py_END_ALLOW_THREADS
            if (rc > 0) {
                ULONG i;
                for (i = 0; i < n_fds; i++) {
                    int mask = 0;
                    SHORT re = fds[i].revents;
                    int fdi = (int)fds[i].fd;
                    pygo_parked_t *p;
                    if (re & (POLLRDNORM | POLLIN | POLLHUP | POLLERR))
                        mask |= PYGO_NETPOLL_READ;
                    if (re & (POLLWRNORM | POLLOUT | POLLERR))
                        mask |= PYGO_NETPOLL_WRITE;
                    if (mask == 0) continue;
                    if ((size_t)fdi >= pygo_parked_by_fd_cap) continue;
                    p = pygo_parked_by_fd[fdi];
                    while (p != NULL) {
                        pygo_parked_t *next_p = p->next_by_fd;
                        if (p->events & mask) {
                            *(p->ready_out) = mask & p->events;
                            pygo_parker_unlink(p);
                            __atomic_sub_fetch(&pygo_parked_total, 1,
                                               __ATOMIC_RELEASE);
                            pygo_mn_wake_g(p->hub, p->g);
                            woke++;
                            break;
                        }
                        p = next_p;
                    }
                }
            }
        }
        if (fds != fds_stack) free(fds);
        pygo_mutex_unlock(&pygo_parked_lock);
    } else {
        /* --- select() fallback (XP / Server 2003 / no WSAPoll) ---
         * Windows select() uses SOCKET handles directly; the first arg
         * is ignored.  FD_SETSIZE is 64 by default but can be raised
         * via build define (see setup.py).  This path is best-effort
         * for legacy hosts; production usage assumes WSAPoll. */
        fd_set rfds, wfds, efds;
        pygo_parked_t *p;
        int rc;
        struct timeval tv, *tvp = NULL;

        FD_ZERO(&rfds); FD_ZERO(&wfds); FD_ZERO(&efds);
        pygo_mutex_lock(&pygo_parked_lock);
        for (p = pygo_parked_head; p != NULL; p = p->next) {
            if (p->events & PYGO_NETPOLL_READ)  FD_SET((SOCKET)p->fd, &rfds);
            if (p->events & PYGO_NETPOLL_WRITE) FD_SET((SOCKET)p->fd, &wfds);
            FD_SET((SOCKET)p->fd, &efds);
        }
        if (timeout_ns >= 0) {
            tv.tv_sec  = (long)(timeout_ns / 1000000000LL);
            tv.tv_usec = (long)((timeout_ns % 1000000000LL) / 1000LL);
            tvp = &tv;
        }
        Py_BEGIN_ALLOW_THREADS
        rc = select(0, &rfds, &wfds, &efds, tvp);
        Py_END_ALLOW_THREADS
        if (rc > 0) {
            pygo_parked_t *p = pygo_parked_head;
            while (p != NULL) {
                pygo_parked_t *next_p = p->next;
                int mask = 0;
                if (FD_ISSET((SOCKET)p->fd, &rfds)) mask |= PYGO_NETPOLL_READ;
                if (FD_ISSET((SOCKET)p->fd, &wfds)) mask |= PYGO_NETPOLL_WRITE;
                if (FD_ISSET((SOCKET)p->fd, &efds))
                    mask |= PYGO_NETPOLL_READ | PYGO_NETPOLL_WRITE;
                if (mask & p->events) {
                    *(p->ready_out) = mask & p->events;
                    pygo_parker_unlink(p);
                    __atomic_sub_fetch(&pygo_parked_total, 1, __ATOMIC_RELEASE);
                    pygo_mn_wake_g(p->hub, p->g);
                    woke++;
                }
                p = next_p;
            }
        }
        pygo_mutex_unlock(&pygo_parked_lock);
    }
post_wait:
    ;
#else
    /* POSIX select() backend.  Same as kqueue/epoll absent platforms. */
    {
        fd_set rfds, wfds;
        int max_fd = -1;
        pygo_parked_t *p;
        FD_ZERO(&rfds); FD_ZERO(&wfds);
        pygo_mutex_lock(&pygo_parked_lock);
        for (p = pygo_parked_head; p != NULL; p = p->next) {
            if (p->fd > max_fd) max_fd = p->fd;
            if (p->events & PYGO_NETPOLL_READ)  FD_SET(p->fd, &rfds);
            if (p->events & PYGO_NETPOLL_WRITE) FD_SET(p->fd, &wfds);
        }
        if (max_fd >= 0) {
            struct timeval tv, *tvp = NULL;
            if (timeout_ns >= 0) {
                tv.tv_sec = (long)(timeout_ns / 1000000000LL);
                tv.tv_usec = (long)((timeout_ns % 1000000000LL) / 1000LL);
                tvp = &tv;
            }
            if (select(max_fd + 1, &rfds, &wfds, NULL, tvp) > 0) {
                pygo_parked_t *p = pygo_parked_head;
                while (p != NULL) {
                    pygo_parked_t *next_p = p->next;
                    int mask = 0;
                    if (FD_ISSET(p->fd, &rfds)) mask |= PYGO_NETPOLL_READ;
                    if (FD_ISSET(p->fd, &wfds)) mask |= PYGO_NETPOLL_WRITE;
                    if (mask & p->events) {
                        *(p->ready_out) = mask & p->events;
                        pygo_parker_unlink(p);
                        __atomic_sub_fetch(&pygo_parked_total, 1, __ATOMIC_RELEASE);
                        pygo_mn_wake_g(p->hub, p->g);
                        woke++;
                    }
                    p = next_p;
                }
            }
        }
        pygo_mutex_unlock(&pygo_parked_lock);
    }
#endif

    /* Handle timeouts: any park whose deadline has passed gets ready=0. */
    now = monotonic_ns();
    pygo_mutex_lock(&pygo_parked_lock);
    {
        pygo_parked_t *p = pygo_parked_head;
        while (p != NULL) {
            pygo_parked_t *next_p = p->next;
            if (p->deadline_ns >= 0 && p->deadline_ns <= now) {
                *(p->ready_out) = 0;
                pygo_parker_unlink(p);
                __atomic_sub_fetch(&pygo_parked_total, 1, __ATOMIC_RELEASE);
                pygo_mn_wake_g(p->hub, p->g);
                woke++;
            }
            p = next_p;
        }
    }
    pygo_mutex_unlock(&pygo_parked_lock);
    return woke;
}
