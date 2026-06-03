/* Part of netpoll.c -- #included into that translation unit, not compiled
 * standalone.  See netpoll.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* Initialise pool->lock once, regardless of platform.  POSIX could use
 * PTHREAD_MUTEX_INITIALIZER and skip this, but Windows CRITICAL_SECTION
 * has no static-init form so the lazy-init pattern is uniform. */
static void pygo_parker_pool_lock_ensure_inited(pygo_parker_pool_t *pool)
{
#if defined(PYGO_OS_WINDOWS)
    /* InterlockedCompareExchange returns the prior value; only the
     * first caller transitions 0 -> 1 and runs the init. */
    if (InterlockedCompareExchange(&pool->lock_inited, 1, 0) == 0) {
        pygo_mutex_init(&pool->lock);
    } else {
        /* Spin briefly while another thread finishes init.  In practice
         * the init is one InitializeCriticalSection call (~100 ns), so
         * any starvation here is bounded. */
        while (pool->lock_inited != 2) { /* spin */ }
        return;
    }
    pool->lock_inited = 2;
#else
    if (__atomic_load_n(&pool->lock_inited, __ATOMIC_ACQUIRE) == 2) {
        return;
    }
    long expected = 0;
    if (__atomic_compare_exchange_n(&pool->lock_inited, &expected, 1,
                                    0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        pygo_mutex_init(&pool->lock);
        __atomic_store_n(&pool->lock_inited, 2, __ATOMIC_RELEASE);
    } else {
        while (__atomic_load_n(&pool->lock_inited, __ATOMIC_ACQUIRE) != 2)
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
    if (!__atomic_load_n(&pygo_netpoll_inited, __ATOMIC_ACQUIRE)) pygo_netpoll_init();
    return pygo_win_backend_name;
#else
    return "select";
#endif
}

/* ---- netpoll syscall fault injection (test-only) ----------------------- *
 * Where strace's `-e inject=` drives the Linux epoll campaign, the platforms
 * WITHOUT a syscall-injecting tracer (Windows; FreeBSD/macOS kqueue) gate IN
 * PROCESS on env vars.  PYGO_FAULT_<SITE> = "<mode>:<code>", mode in
 * {once, always}, forces the named pump syscall to report a failure:
 *   once:CODE   -- inject CODE exactly the first time the site is reached
 *   always:CODE -- inject CODE every time (persistent error)
 * code is a WSA error (Windows) or an errno (kqueue).  Each site counts how
 * many times it fired (read via pygo_core._fault_count) so a test can prove a
 * PERSISTENT error BACKS OFF (bounded count) instead of busy-spinning.  Inert
 * unless the env var is set (one getenv per pump iteration when armed).  The
 * counters + count/reset are compiled on every platform so module.c exposes
 * the introspection uniformly (they stay zero on epoll, which uses strace). */
/* Fault-injection site enum lives in netpoll.h (shared with pygo_tcp.c). */
static const char *const pygo_fault_names[PYGO_FAULT_NSITES] = {
    "WSAPOLL", "SELECT", "IOCP_WAIT", "IOCP_SUBMIT", "KQUEUE_WAIT",
    "KQUEUE_CREATE", "KQUEUE_CTL",
    "TCP_SOCKET", "TCP_CONNECT", "TCP_ACCEPT", "TCP_RECV", "TCP_SEND",
    "FD_READ", "FD_WRITE",
    "SPAWN_G", "SPAWN_STACK", "SPAWN_TSTATE" };
static volatile long pygo_fault_fired[PYGO_FAULT_NSITES];
static volatile long pygo_fault_once_done[PYGO_FAULT_NSITES];

long pygo_fault_count(const char *name)
{
    int i;
    for (i = 0; i < PYGO_FAULT_NSITES; i++)
        if (strcmp(pygo_fault_names[i], name) == 0)
            return pygo_fault_fired[i];
    return -1;
}

void pygo_fault_reset(void)
{
    int i;
    for (i = 0; i < PYGO_FAULT_NSITES; i++) {
        pygo_fault_fired[i] = 0;
        pygo_fault_once_done[i] = 0;
    }
}

/* Compiled on every platform: the netpoll pump faults (KQUEUE/SELECT/IOCP/
 * WSAPOLL) only fire on their own backend, but the syscall-surface faults
 * (TCP_*, FD_*) are reachable on Linux too (e.g. module.c fd_read/fd_write). */
static const char *const pygo_fault_envs[PYGO_FAULT_NSITES] = {
    "PYGO_FAULT_WSAPOLL", "PYGO_FAULT_SELECT", "PYGO_FAULT_IOCP_WAIT",
    "PYGO_FAULT_IOCP_SUBMIT", "PYGO_FAULT_KQUEUE_WAIT",
    "PYGO_FAULT_KQUEUE_CREATE", "PYGO_FAULT_KQUEUE_CTL",
    "PYGO_FAULT_TCP_SOCKET", "PYGO_FAULT_TCP_CONNECT", "PYGO_FAULT_TCP_ACCEPT",
    "PYGO_FAULT_TCP_RECV", "PYGO_FAULT_TCP_SEND",
    "PYGO_FAULT_FD_READ", "PYGO_FAULT_FD_WRITE",
    "PYGO_FAULT_SPAWN_G", "PYGO_FAULT_SPAWN_STACK", "PYGO_FAULT_SPAWN_TSTATE" };

/* Cached "any spawn-OOM fault armed" -- keeps the hot spawn alloc sites at a
 * single branch when no PYGO_FAULT_SPAWN_* env is set.  Read once. */
int pygo_spawn_fault_armed(void)
{
    static int cached = -1;
    if (cached < 0) {
        cached = (getenv("PYGO_FAULT_SPAWN_G") || getenv("PYGO_FAULT_SPAWN_STACK")
                  || getenv("PYGO_FAULT_SPAWN_TSTATE")) ? 1 : 0;
    }
    return cached;
}

/* Returns the error code to inject (nonzero) if this site should fail now. */
int pygo_fault_inject(int site)
{
    const char *spec = getenv(pygo_fault_envs[site]);
    int code, once;
    if (spec == NULL || *spec == '\0') return 0;
    if (strncmp(spec, "once:", 5) == 0)        { once = 1; code = atoi(spec + 5); }
    else if (strncmp(spec, "always:", 7) == 0) { once = 0; code = atoi(spec + 7); }
    else return 0;
    if (code == 0) return 0;
    if (once) {
#if defined(PYGO_OS_WINDOWS)
        if (InterlockedCompareExchange(&pygo_fault_once_done[site], 1, 0) != 0)
            return 0;          /* once-mode already fired */
#else
        long expected = 0;
        if (!__atomic_compare_exchange_n(&pygo_fault_once_done[site], &expected,
                1, 0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
            return 0;
#endif
    }
#if defined(PYGO_OS_WINDOWS)
    InterlockedIncrement(&pygo_fault_fired[site]);
#else
    __atomic_add_fetch(&pygo_fault_fired[site], 1, __ATOMIC_RELAXED);
#endif
    return code;
}

#if defined(PYGO_OS_WINDOWS)
/* Windows analogue of pygo_netpoll_wait_failed (epoll/kqueue): a hard poll
 * error returns WITHOUT blocking, so a persistent one (a polled socket closed
 * under us -> WSAENOTSOCK, a teardown race) would peg a CPU as the idle loop
 * re-pumps the instant pump() returns.  WSAEINTR is transient (retry, no
 * backoff); anything else logs once and sleeps 1 ms, degrading a silent CPU
 * peg into an observable, throttled slow-loop.  Returns 1 (handled). */
static int pygo_netpoll_win_wait_failed(int wsa_err)
{
    static volatile LONG warned;
    if (wsa_err == WSAEINTR) return 1;
    if (InterlockedExchange(&warned, 1) == 0) {
        fprintf(stderr,
                "[pygo] netpoll pump: Windows poll failed (WSA=%d); backing off "
                "to avoid a busy-spin -- the polled socket is likely closed "
                "(teardown race) or invalid\n", wsa_err);
    }
    Py_BEGIN_ALLOW_THREADS
    Sleep(1);
    Py_END_ALLOW_THREADS
    return 1;
}
#endif /* PYGO_OS_WINDOWS */

int pygo_netpoll_init(void)
{
    /* ACQUIRE pairs with the RELEASE store at the end: any thread that
     * sees inited==1 here also sees the backend + array publication
     * below.  This is only the fast-path early-out; the authoritative
     * check is re-done under the lock (see below). */
    if (__atomic_load_n(&pygo_netpoll_inited, __ATOMIC_ACQUIRE)) return 0;
    pygo_parker_pool_lock_ensure_inited(&pygo_pool);
    /* EVERYTHING that brings the backend up runs under pygo_pool.lock:
     * the per-fd arrays AND the single shared epoll/kqueue/IOCP handle.
     *
     * The old code checked `inited` unlocked, then created the backend
     * OUTSIDE the lock.  Under M:N every hub calls this concurrently at
     * startup (each registers its io_uring eventfd via
     * pygo_netpoll_add_iouring_ring -> pygo_netpoll_init), so N racing
     * first-callers each ran epoll_create1 and stored pygo_epoll_fd.
     * pygo_epoll_fd ended at the last writer's value and the others
     * leaked -- but worse, while it churned, registers on other threads
     * read intermediate pygo_epoll_fd values and EPOLL_CTL_ADD'd client
     * sockets into the soon-orphaned epolls.  Those fds were armed
     * correctly but lived in an epoll instance no hub ever epoll_wait'd
     * on, so their readiness was never delivered: a parked g with data
     * waiting (Recv-Q>0), forever.  That was the ~0.1% startup-window
     * lost-wake residual.  Creating the handle exactly once, under the
     * lock, with a re-checked `inited`, closes it. */
    pygo_mutex_lock(&pygo_pool.lock);
    /* Re-check under the lock: a racing thread may have finished the
     * whole init while we waited to acquire it. */
    if (__atomic_load_n(&pygo_netpoll_inited, __ATOMIC_ACQUIRE)) {
        pygo_mutex_unlock(&pygo_pool.lock);
        return 0;
    }
    pygo_fd_arrays_init();
#if defined(PYGO_HAVE_EPOLL)
    pygo_epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (pygo_epoll_fd < 0) { pygo_mutex_unlock(&pygo_pool.lock); return -1; }
#elif defined(PYGO_HAVE_KQUEUE)
    pygo_kqueue_fd = kqueue();
    {   /* test-only: force kqueue() to fail so the init-error path is covered.
         * A hard init failure must surface as a clean OSError to the first
         * parker (via the -1 return below), never crash or hang. */
        int finj = pygo_fault_inject(PYGO_FAULT_KQUEUE_CREATE);
        if (finj) {
            if (pygo_kqueue_fd >= 0) close(pygo_kqueue_fd);
            pygo_kqueue_fd = -1;
            errno = finj;
        }
    }
    if (pygo_kqueue_fd < 0) { pygo_mutex_unlock(&pygo_pool.lock); return -1; }
    /* Cross-thread pump wake: a self-pipe registered in the kqueue.  Best
     * effort -- if pipe()/fcntl()/kevent() fail the pump still re-polls on
     * its bounded timeout, so wake latency degrades but nothing wedges. */
    {
        int pfd[2];
        if (pipe(pfd) == 0) {
            int k;
            struct kevent kev;
            for (k = 0; k < 2; k++) {
                int fl = fcntl(pfd[k], F_GETFL, 0);
                if (fl >= 0) (void)fcntl(pfd[k], F_SETFL, fl | O_NONBLOCK);
                (void)fcntl(pfd[k], F_SETFD, FD_CLOEXEC);
            }
            EV_SET(&kev, pfd[0], EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0, NULL);
            if (kevent(pygo_kqueue_fd, &kev, 1, NULL, 0, NULL) == 0) {
                pygo_selfpipe_r = pfd[0];
                pygo_selfpipe_w = pfd[1];
            } else {
                close(pfd[0]);
                close(pfd[1]);
            }
        }
    }
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
#else
    /* select() fallback: arm the self-pipe wake.  Best-effort -- if
     * pipe()/fcntl() fail the pump still re-polls on its bounded
     * timeout, so wake latency degrades but nothing wedges. */
    {
        int pfd[2];
        if (pipe(pfd) == 0) {
            int k;
            for (k = 0; k < 2; k++) {
                int fl = fcntl(pfd[k], F_GETFL, 0);
                if (fl >= 0) (void)fcntl(pfd[k], F_SETFL, fl | O_NONBLOCK);
                (void)fcntl(pfd[k], F_SETFD, FD_CLOEXEC);
            }
            pygo_selfpipe_r = pfd[0];
            pygo_selfpipe_w = pfd[1];
        }
    }
#endif
    /* RELEASE: publish the backend handle + array writes above to any
     * thread that later observes inited==1 via the ACQUIRE load (at the
     * top here, or in pygo_netpoll_pump).  Still inside pygo_pool.lock
     * so a racing init that lost the re-check above never observes a
     * half-built backend. */
    __atomic_store_n(&pygo_netpoll_inited, 1, __ATOMIC_RELEASE);
    pygo_mutex_unlock(&pygo_pool.lock);
    return 0;
}

void pygo_netpoll_fini(void)
{
#if defined(PYGO_HAVE_EPOLL)
    if (pygo_epoll_fd >= 0) { close(pygo_epoll_fd); pygo_epoll_fd = -1; }
#elif defined(PYGO_HAVE_KQUEUE)
    if (pygo_kqueue_fd >= 0) { close(pygo_kqueue_fd); pygo_kqueue_fd = -1; }
    /* Self-pipe wake teardown (see pygo_netpoll_init kqueue branch). */
    if (pygo_selfpipe_r >= 0) { close(pygo_selfpipe_r); pygo_selfpipe_r = -1; }
    if (pygo_selfpipe_w >= 0) { close(pygo_selfpipe_w); pygo_selfpipe_w = -1; }
#elif defined(PYGO_OS_WINDOWS)
    if (pygo_win_use_iocp) {
        pygo_iocp_fini();
        pygo_win_use_iocp = 0;
    }
    /* WSAPoll / select are stateless; nothing else to close.
     * Winsock itself is left up by design (see pygo_winsock_init). */
#else
    /* select() fallback: tear down the self-pipe wake. */
    if (pygo_selfpipe_r >= 0) { close(pygo_selfpipe_r); pygo_selfpipe_r = -1; }
    if (pygo_selfpipe_w >= 0) { close(pygo_selfpipe_w); pygo_selfpipe_w = -1; }
#endif
    __atomic_store_n(&pygo_netpoll_inited, 0, __ATOMIC_RELEASE);
}

/* Reset netpoll in a forked CHILD.  The inherited epoll/kqueue fd is the
 * SAME kernel object as the parent's (fork shares it), so a child that did
 * I/O would cross-contaminate the parent's readiness -- it must get its
 * own.  Single-threaded here, so no lock is taken; we only re-init the per-
 * pool locks (a dead hub may have held one at fork) and drop the inherited
 * parker bookkeeping (it references parent goroutines that don't exist in
 * the child).  pygo_netpoll_fini() closes the inherited fds + marks uninit
 * (it is itself lock-free); the next wait_fd re-creates a fresh poller. */
void pygo_netpoll_reset_after_fork(void)
{
    size_t i;
    pygo_netpoll_fini();        /* close inherited fds, inited = 0 (lock-free) */
    for (i = 0; i < (size_t)PYGO_PARKER_POOL_MAX; i++) {
        pygo_parker_pool_t *p = &pygo_pools[i];
        pygo_mutex_init(&p->lock);
        p->lock_inited = 2;     /* ready (re-inited above) */
        p->head = NULL;
        __atomic_store_n(&p->total, 0, __ATOMIC_RELAXED);
        if (p->by_fd != NULL)
            memset(p->by_fd, 0, p->by_fd_cap * sizeof(p->by_fd[0]));
        p->dh_size = 0;
        p->sweep_cursor = NULL;
        p->sweep_cursor_gen = 0;
        p->link_count = 0;
        p->churn_last_links = 0;
        p->churn_last_ns = 0;
    }
    /* The per-fd registration bitmap says "fd X is already armed in the
     * poller"; against the child's BRAND-NEW poller every such bit is a lie
     * that would make wait_fd skip EPOLL_CTL_ADD and lose the event.  Clear
     * it (and the pending-wake bits) so the child re-arms cleanly. */
    if (pygo_fd_registered_bm != NULL)
        memset(pygo_fd_registered_bm, 0, pygo_fd_registered_cap_bytes);
    {
        unsigned char *pw = __atomic_load_n(&pygo_fd_pending_wake, __ATOMIC_RELAXED);
        size_t cap = __atomic_load_n(&pygo_fd_pending_wake_cap, __ATOMIC_RELAXED);
        if (pw != NULL) memset(pw, 0, cap);
    }
#if defined(PYGO_HAVE_EPOLL)
    /* The per-hub io_uring ring registry names the PARENT hubs' rings (dead in
     * the child).  The child's fresh hubs re-register their own; if we keep the
     * stale entries the pump dispatches CQEs to freed rings -> hang/crash.  The
     * epoll/eventfd/pump-wake fds were closed with the epoll fd above. */
    pygo_iouring_ring_count = 0;
    pygo_iouring_eventfd_in_epoll = -1;
    pygo_pump_wake_fd = -1;
#endif
}

static long long monotonic_ns(void)
{
    return pygo_monotonic_ns();
}
