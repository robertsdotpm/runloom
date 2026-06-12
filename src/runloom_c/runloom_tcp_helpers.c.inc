/* Part of runloom_tcp.c -- #included into that translation unit, not compiled
 * standalone.  See runloom_tcp.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* ============================================================
 * Helpers
 * ============================================================ */

/* Set O_NONBLOCK on a fd.  Idempotent. */
static int runloom_set_nonblock(int fd)
{
#if defined(RUNLOOM_OS_WINDOWS)
    u_long mode = 1;
    if (ioctlsocket((SOCKET)fd, FIONBIO, &mode) == SOCKET_ERROR) {
        return -1;
    }
    return 0;
#else
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    if (flags & O_NONBLOCK) return 0;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
#endif
}

/* Set TCP_NODELAY.  Best effort; ignore errors. */
static void runloom_set_nodelay(int fd, int family)
{
    int on = 1;
    if (family != AF_INET && family != AF_INET6) return;
#if defined(RUNLOOM_OS_WINDOWS)
    (void)setsockopt((SOCKET)fd, IPPROTO_TCP, TCP_NODELAY,
                     (const char *)&on, sizeof(on));
#else
    (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
#endif
}

/* Translate the last errno/WSAGetLastError to a Python exception. */
static PyObject *runloom_raise_errno(void)
{
#if defined(RUNLOOM_OS_WINDOWS)
    return PyErr_SetFromWindowsErr(WSAGetLastError());
#else
    return PyErr_SetFromErrno(PyExc_OSError);
#endif
}

/* ---- test-only socket-surface fault injection (kqueue/Windows) -------------
 * Linux faults the socket syscalls with strace; the kqueue/Windows backends
 * have no such tracer, so they gate in-process on RUNLOOM_FAULT_TCP_<CALL> (see
 * netpoll.c).  runloom_tcp_fault_armed() caches whether ANY such env is set, so
 * the hot recv/send path pays only a load+branch when disarmed -- the getenv
 * scan runs once.  cached is written idempotently (always the same 0/1), so
 * the benign first-call race needs no atomic.  Compiled out on Linux, where
 * RUNLOOM_TCP_FINJ() is a constant 0 (no overhead, no reference to the helper).
 *
 * Usage:  int injerr = RUNLOOM_TCP_FINJ(SITE);
 *         r = injerr ? (errno = injerr, -1) : <real syscall>; */
#if defined(RUNLOOM_OS_WINDOWS) || defined(RUNLOOM_HAVE_KQUEUE)
static int runloom_tcp_fault_armed(void)
{
    static int cached = -1;
    if (cached < 0)
        cached = (getenv("RUNLOOM_FAULT_TCP_SOCKET")  || getenv("RUNLOOM_FAULT_TCP_CONNECT") ||
                  getenv("RUNLOOM_FAULT_TCP_ACCEPT")  || getenv("RUNLOOM_FAULT_TCP_RECV")    ||
                  getenv("RUNLOOM_FAULT_TCP_SEND")) ? 1 : 0;
    return cached;
}
#  define RUNLOOM_TCP_FINJ(site) (runloom_tcp_fault_armed() ? runloom_fault_inject(site) : 0)
#else
#  define RUNLOOM_TCP_FINJ(site) 0
#endif

#if defined(RUNLOOM_OS_WINDOWS)
/* Used only on Windows branches below; the POSIX paths inline the
 * errno comparisons.  Tagged so non-Windows builds (which include
 * this header anyway for the prototype) don't warn. */
static int runloom_is_wouldblock(void)
{
    return WSAGetLastError() == WSAEWOULDBLOCK;
}

static int runloom_is_intr(void)
{
    return WSAGetLastError() == WSAEINTR;
}
#endif

/* ============================================================
 * Address parsing -- AF_INET / AF_INET6.
 *
 * Inputs accepted:
 *   ("1.2.3.4", 8080)         -> AF_INET
 *   ("::1", 8080)             -> AF_INET6
 *   ("example.com", 8080)     -> getaddrinfo, returns first usable
 *
 * Returns 0 on success and fills `*storage` + `*addrlen` + `*family`.
 * On error sets a Python exception and returns -1.
 * ============================================================ */
/* getaddrinfo offloaded to the blocking pool.  Job lives on the
 * resolving fiber's coroutine stack (alive across the park); the
 * worker runs getaddrinfo with no GIL. */
typedef struct {
    const char *host;
    const char *portbuf;
    const struct addrinfo *hints;
    struct addrinfo *res;
    int rc;
} runloom_resolve_job_t;

static void *runloom_resolve_worker(void *p)
{
    runloom_resolve_job_t *j = (runloom_resolve_job_t *)p;
    j->rc = getaddrinfo(j->host, j->portbuf, j->hints, &j->res);
    return NULL;
}

static int runloom_resolve(const char *host, int port, int want_passive,
                        struct sockaddr_storage *storage,
                        socklen_t *addrlen, int *family)
{
    struct addrinfo hints, *res = NULL, *p;
    char portbuf[16];
    int rc;
    runloom_sched_t *sched;
    int in_fiber;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    if (want_passive) hints.ai_flags |= AI_PASSIVE;
    /* AI_NUMERICSERV: we always pass a numeric port. */
    hints.ai_flags |= AI_NUMERICSERV;

    snprintf(portbuf, sizeof(portbuf), "%d", port);

    /* getaddrinfo is a non-preemptible blocking C call.  If we are
     * running inside a fiber, offload it to the blocking pool and
     * park -- otherwise it would wedge the fiber's hub (or the
     * single-thread scheduler), stranding everything queued behind it.
     * The worker runs getaddrinfo with no GIL, so we must NOT bracket
     * the offload in Py_BEGIN_ALLOW_THREADS (you cannot yield a
     * coroutine across that macro pair).  Outside a fiber there is
     * nothing to park, so fall back to the classic inline lookup with
     * the GIL released. */
    sched = runloom_sched_get();
    in_fiber = (runloom_mn_current_hub_opaque() != NULL) ||
                   (sched != NULL && sched->current != NULL);
    if (in_fiber) {
        runloom_resolve_job_t job;
        job.host = host; job.portbuf = portbuf; job.hints = &hints;
        job.res = NULL;  job.rc = 0;
        runloom_blocking_call(runloom_resolve_worker, &job);
        rc  = job.rc;
        res = job.res;
    } else {
        Py_BEGIN_ALLOW_THREADS
        rc = getaddrinfo(host, portbuf, &hints, &res);
        Py_END_ALLOW_THREADS
    }
    if (rc != 0 || res == NULL) {
        PyErr_Format(PyExc_OSError, "getaddrinfo: %s",
                     gai_strerror(rc));
        if (res) freeaddrinfo(res);
        return -1;
    }
    for (p = res; p != NULL; p = p->ai_next) {
        if (p->ai_addrlen <= sizeof(*storage)) {
            memcpy(storage, p->ai_addr, p->ai_addrlen);
            *addrlen = (socklen_t)p->ai_addrlen;
            *family  = p->ai_family;
            freeaddrinfo(res);
            return 0;
        }
    }
    freeaddrinfo(res);
    PyErr_SetString(PyExc_OSError, "getaddrinfo: no usable address");
    return -1;
}
