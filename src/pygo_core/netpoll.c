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
 * destroyed until it completes). */
typedef struct pygo_parked {
    int fd;
    int events;
    long long deadline_ns;     /* -1 = forever */
    int *ready_out;            /* where to store the wakeup mask */
    pygo_g_t *g;
    void *hub;                  /* M:N hub opaque; NULL = global sched */
    struct pygo_parked *next;
} pygo_parked_t;

/* Shared parked list + lock.  Under M:N multiple hubs concurrently
 * call wait_fd (park.add) and pump (park.remove); without the lock
 * the singly-linked list corrupts.  Single-thread sched takes the
 * lock too but no contention. */
static pygo_parked_t *pygo_parked_head = NULL;
static int pygo_parked_total = 0;       /* read with __atomic_load_n */
static int pygo_netpoll_inited = 0;
static pygo_mutex_t pygo_parked_lock;
static volatile long pygo_parked_lock_inited = 0;

#if defined(PYGO_HAVE_EPOLL)
static int pygo_epoll_fd = -1;
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
    /* Backend selection on Windows.  IOCP+AFD is wired end-to-end and
     * works for TCP listener/connect/recv flows (validated on Win 11),
     * but the Parker-socketpair pattern that monkey.py uses for
     * cooperative threading primitives surfaces an AFD edge case where
     * an IRP submitted after the read end's data has already been
     * deposited returns STATUS_PENDING and never fires.  Until that
     * lifecycle quirk is characterised (probably wepoll-style re-arm
     * after each completion), IOCP stays opt-in.
     *
     *   PYGO_NETPOLL=iocp     -> opt-in IOCP+AFD
     *   PYGO_NETPOLL=wsapoll  -> force WSAPoll (default on Vista+)
     *   PYGO_NETPOLL=select   -> force select() (XP/2003 default) */
    {
        const char *env = getenv("PYGO_NETPOLL");
        if (env != NULL && strcmp(env, "iocp") == 0 &&
            pygo_iocp_init() == 0) {
            pygo_win_use_iocp = 1;
            pygo_win_backend_name = "iocp-afd";
        } else if (env != NULL && strcmp(env, "select") == 0) {
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

/* ---- registration ---- */
static int pygo_netpoll_register(int fd, int events)
{
#if defined(PYGO_HAVE_EPOLL)
    struct epoll_event ev;
    ev.events = 0;
    if (events & PYGO_NETPOLL_READ)  ev.events |= EPOLLIN;
    if (events & PYGO_NETPOLL_WRITE) ev.events |= EPOLLOUT;
    ev.events |= EPOLLONESHOT;
    ev.data.fd = fd;
    if (epoll_ctl(pygo_epoll_fd, EPOLL_CTL_ADD, fd, &ev) == 0) return 0;
    /* If already registered, modify. */
    if (errno == EEXIST) {
        return epoll_ctl(pygo_epoll_fd, EPOLL_CTL_MOD, fd, &ev);
    }
    return -1;
#elif defined(PYGO_HAVE_KQUEUE)
    struct kevent kev[2];
    int n = 0;
    if (events & PYGO_NETPOLL_READ) {
        EV_SET(&kev[n++], fd, EVFILT_READ,  EV_ADD | EV_ONESHOT, 0, 0, NULL);
    }
    if (events & PYGO_NETPOLL_WRITE) {
        EV_SET(&kev[n++], fd, EVFILT_WRITE, EV_ADD | EV_ONESHOT, 0, 0, NULL);
    }
    return kevent(pygo_kqueue_fd, kev, n, NULL, 0, NULL);
#else
    (void)fd; (void)events;
    return 0;  /* select doesn't need pre-registration */
#endif
}

int pygo_netpoll_parked_count(void)
{
    return __atomic_load_n(&pygo_parked_total, __ATOMIC_ACQUIRE);
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

    pygo_mutex_lock(&pygo_parked_lock);
    park.next = pygo_parked_head;
    pygo_parked_head = &park;
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
                pygo_parked_t **pp;
                if (evs[i].events & EPOLLIN)  mask |= PYGO_NETPOLL_READ;
                if (evs[i].events & EPOLLOUT) mask |= PYGO_NETPOLL_WRITE;
                /* Find parked entry for this fd, mark ready. */
                pp = &pygo_parked_head;
                while (*pp != NULL) {
                    if ((*pp)->fd == fd && ((*pp)->events & mask)) {
                        pygo_parked_t *hit = *pp;
                        *(hit->ready_out) = mask & hit->events;
                        *pp = hit->next;
                        __atomic_sub_fetch(&pygo_parked_total, 1, __ATOMIC_RELEASE);
                        pygo_mn_wake_g(hit->hub, hit->g);
                        woke++;
                        break;
                    }
                    pp = &(*pp)->next;
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
                pygo_parked_t **pp = &pygo_parked_head;
                while (*pp != NULL) {
                    if ((*pp)->fd == fd && ((*pp)->events & mask)) {
                        pygo_parked_t *hit = *pp;
                        *(hit->ready_out) = mask & hit->events;
                        *pp = hit->next;
                        __atomic_sub_fetch(&pygo_parked_total, 1, __ATOMIC_RELEASE);
                        pygo_mn_wake_g(hit->hub, hit->g);
                        woke++;
                        break;
                    }
                    pp = &(*pp)->next;
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
            {
                pygo_parked_t **pp = &pygo_parked_head;
                while (*pp != NULL) {
                    if ((*pp)->fd == fd && ((*pp)->events & evs)) {
                        pygo_parked_t *hit = *pp;
                        *(hit->ready_out) = evs & hit->events;
                        *pp = hit->next;
                        __atomic_sub_fetch(&pygo_parked_total, 1,
                                           __ATOMIC_RELEASE);
                        pygo_mn_wake_g(hit->hub, hit->g);
                        woke++;
                        break;
                    }
                    pp = &(*pp)->next;
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
                    pygo_parked_t **pp;
                    if (re & (POLLRDNORM | POLLIN | POLLHUP | POLLERR))
                        mask |= PYGO_NETPOLL_READ;
                    if (re & (POLLWRNORM | POLLOUT | POLLERR))
                        mask |= PYGO_NETPOLL_WRITE;
                    if (mask == 0) continue;
                    pp = &pygo_parked_head;
                    while (*pp != NULL) {
                        if ((SOCKET)(*pp)->fd == fds[i].fd &&
                            ((*pp)->events & mask)) {
                            pygo_parked_t *hit = *pp;
                            *(hit->ready_out) = mask & hit->events;
                            *pp = hit->next;
                            __atomic_sub_fetch(&pygo_parked_total, 1,
                                               __ATOMIC_RELEASE);
                            pygo_mn_wake_g(hit->hub, hit->g);
                            woke++;
                            break;
                        }
                        pp = &(*pp)->next;
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
            pygo_parked_t **pp = &pygo_parked_head;
            while (*pp != NULL) {
                int mask = 0;
                if (FD_ISSET((SOCKET)(*pp)->fd, &rfds)) mask |= PYGO_NETPOLL_READ;
                if (FD_ISSET((SOCKET)(*pp)->fd, &wfds)) mask |= PYGO_NETPOLL_WRITE;
                if (FD_ISSET((SOCKET)(*pp)->fd, &efds))
                    mask |= PYGO_NETPOLL_READ | PYGO_NETPOLL_WRITE;
                if (mask & (*pp)->events) {
                    pygo_parked_t *hit = *pp;
                    *(hit->ready_out) = mask & hit->events;
                    *pp = hit->next;
                    __atomic_sub_fetch(&pygo_parked_total, 1, __ATOMIC_RELEASE);
                    pygo_mn_wake_g(hit->hub, hit->g);
                    woke++;
                    continue;
                }
                pp = &(*pp)->next;
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
                pygo_parked_t **pp = &pygo_parked_head;
                while (*pp != NULL) {
                    int mask = 0;
                    if (FD_ISSET((*pp)->fd, &rfds)) mask |= PYGO_NETPOLL_READ;
                    if (FD_ISSET((*pp)->fd, &wfds)) mask |= PYGO_NETPOLL_WRITE;
                    if (mask & (*pp)->events) {
                        pygo_parked_t *hit = *pp;
                        *(hit->ready_out) = mask & hit->events;
                        *pp = hit->next;
                        __atomic_sub_fetch(&pygo_parked_total, 1, __ATOMIC_RELEASE);
                        pygo_mn_wake_g(hit->hub, hit->g);
                        woke++;
                        continue;
                    }
                    pp = &(*pp)->next;
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
        pygo_parked_t **pp = &pygo_parked_head;
        while (*pp != NULL) {
            if ((*pp)->deadline_ns >= 0 && (*pp)->deadline_ns <= now) {
                pygo_parked_t *hit = *pp;
                *(hit->ready_out) = 0;
                *pp = hit->next;
                __atomic_sub_fetch(&pygo_parked_total, 1, __ATOMIC_RELEASE);
                pygo_mn_wake_g(hit->hub, hit->g);
                woke++;
                continue;
            }
            pp = &(*pp)->next;
        }
    }
    pygo_mutex_unlock(&pygo_parked_lock);
    return woke;
}
