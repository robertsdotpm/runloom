/* netpoll.c -- portable I/O multiplexing.
 *
 * Two layers:
 *   1. Backend (epoll / kqueue / select) — adds + removes fd registrations.
 *   2. Wait routine: park current goroutine on a (fd, events) pair,
 *      run the scheduler until netpoll wakes the goroutine.
 *
 * Park mechanics:
 *   - the current goroutine snapshot tstate, gets pushed onto an internal
 *     "parked" list with (fd, events, deadline) metadata.  yields via
 *     pygo_coro_yield.
 *   - the scheduler's drain loop, when ready queue is empty, calls
 *     pygo_netpoll_pump(timeout) instead of sleeping the OS thread.
 *   - pump waits for I/O / timeout, wakes parked goroutines, returns.
 */
#define _POSIX_C_SOURCE 200809L
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "netpoll.h"
#include "coro.h"
#include "pygo_sched.h"
#include "mn_sched.h"

#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#if defined(PYGO_HAVE_EPOLL)
#  include <sys/epoll.h>
#  include <unistd.h>
#elif defined(PYGO_HAVE_KQUEUE)
#  include <sys/event.h>
#  include <sys/time.h>
#  include <unistd.h>
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
static pthread_mutex_t pygo_parked_lock = PTHREAD_MUTEX_INITIALIZER;

#if defined(PYGO_HAVE_EPOLL)
static int pygo_epoll_fd = -1;
#elif defined(PYGO_HAVE_KQUEUE)
static int pygo_kqueue_fd = -1;
#endif

const char *pygo_netpoll_backend(void)
{
#if defined(PYGO_HAVE_EPOLL)
    return "epoll";
#elif defined(PYGO_HAVE_KQUEUE)
    return "kqueue";
#else
    return "select";
#endif
}

int pygo_netpoll_init(void)
{
    if (pygo_netpoll_inited) return 0;
#if defined(PYGO_HAVE_EPOLL)
    pygo_epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (pygo_epoll_fd < 0) return -1;
#elif defined(PYGO_HAVE_KQUEUE)
    pygo_kqueue_fd = kqueue();
    if (pygo_kqueue_fd < 0) return -1;
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
#endif
    pygo_netpoll_inited = 0;
}

static long long monotonic_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
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
    if (pygo_netpoll_register(fd, events) != 0) return -1;

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

    pthread_mutex_lock(&pygo_parked_lock);
    park.next = pygo_parked_head;
    pygo_parked_head = &park;
    __atomic_add_fetch(&pygo_parked_total, 1, __ATOMIC_RELEASE);
    pthread_mutex_unlock(&pygo_parked_lock);

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
    pthread_mutex_lock(&pygo_parked_lock);
    {
        pygo_parked_t *p;
        for (p = pygo_parked_head; p != NULL; p = p->next) {
            if (p->deadline_ns < 0) continue;
            if (min_deadline < 0 || p->deadline_ns < min_deadline) {
                min_deadline = p->deadline_ns;
            }
        }
    }
    pthread_mutex_unlock(&pygo_parked_lock);

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
            pthread_mutex_lock(&pygo_parked_lock);
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
            pthread_mutex_unlock(&pygo_parked_lock);
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
            pthread_mutex_lock(&pygo_parked_lock);
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
            pthread_mutex_unlock(&pygo_parked_lock);
        }
    }
#else
    /* select() backend.  Build fd sets from parked entries.  We hold
     * the lock across select() here because the fd_set is built from
     * the list state -- holding through the blocking call serialises
     * all hubs' pump calls under select, which is the fallback path
     * and not a free-threaded target anyway. */
    {
        fd_set rfds, wfds;
        int max_fd = -1;
        pygo_parked_t *p;
        FD_ZERO(&rfds); FD_ZERO(&wfds);
        pthread_mutex_lock(&pygo_parked_lock);
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
        pthread_mutex_unlock(&pygo_parked_lock);
    }
#endif

    /* Handle timeouts: any park whose deadline has passed gets ready=0. */
    now = monotonic_ns();
    pthread_mutex_lock(&pygo_parked_lock);
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
    pthread_mutex_unlock(&pygo_parked_lock);
    return woke;
}
