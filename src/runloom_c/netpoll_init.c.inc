/* Part of netpoll.c -- #included into that translation unit, not compiled
 * standalone.  See netpoll.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* Initialise pool->lock once, regardless of platform.  POSIX could use
 * PTHREAD_MUTEX_INITIALIZER and skip this, but Windows CRITICAL_SECTION
 * has no static-init form so the lazy-init pattern is uniform. */
/* Per-pool kqueue + pump-wake self-pipe (BSD/macOS).  Each hub's pool gets its
 * OWN kqueue so register/collect/wake never contend on the kernel's single
 * per-kqueue lock across hubs.  Best-effort: on any failure the field stays -1
 * and the caller (register/pump) reports an error or degrades to a re-poll, but
 * never crashes.  Called once per pool from lock_ensure_inited under the
 * once-init CAS, so no extra locking is needed here. */
static void runloom_pool_backend_create(runloom_parker_pool_t *pool)
{
    pool->kqueue_fd = -1;
    pool->selfpipe_r = -1;
    pool->selfpipe_w = -1;
    pool->epoll_fd = -1;
    pool->wake_efd = -1;
#if defined(RUNLOOM_HAVE_EPOLL)
    /* Per-hub epoll (RUNLOOM_PERHUB_EPOLL): this pool's own epoll + a level-
     * triggered wake eventfd registered in it.  Best-effort: on any failure the
     * fields stay -1 and the per-hub path's callers fall back / report, never
     * crash (mirrors the kqueue best-effort below).  See docs/dev/PERHUB_EPOLL.md. */
    if (runloom_perhub_epoll()) {
        pool->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
        if (pool->epoll_fd >= 0) {
            int efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
            if (efd >= 0) {
                struct epoll_event ev;
                ev.events  = EPOLLIN;        /* level-triggered; pump drains on fire */
                ev.data.fd = efd;
                if (epoll_ctl(pool->epoll_fd, EPOLL_CTL_ADD, efd, &ev) == 0) {
                    pool->wake_efd = efd;
                } else {
                    close(efd);
                }
            }
            /* Nest the shared runloom_epoll_fd (io_uring ring eventfds + the
             * legacy generic wake live there) so this hub's pump drains it too:
             * a ready fd in the shared set makes runloom_epoll_fd readable in THIS
             * pool's epoll, and the pump drains it non-blocking.  The shared set
             * is off the hot socket path (sockets register per-hub directly), so
             * the bounded herd there is fine.  runloom_netpoll_init nests into
             * already-created pools too, covering either init order. */
            if (runloom_epoll_fd >= 0) {
                struct epoll_event nev;
                nev.events  = EPOLLIN;
                nev.data.fd = runloom_epoll_fd;
                (void)epoll_ctl(pool->epoll_fd, EPOLL_CTL_ADD,
                                runloom_epoll_fd, &nev);
            }
        }
    }
#endif
#if defined(RUNLOOM_HAVE_KQUEUE)
    pool->kqueue_fd = kqueue();
    {   /* test-only (RUNLOOM_FAULT_KQUEUE_PERHUB): force a NON-default pool's
         * kqueue create to fail, so the register/pump "kqueue_fd < 0" defensive
         * arms are reachable while the run still starts -- the default pool
         * (validated by netpoll_init via RUNLOOM_FAULT_KQUEUE_CREATE) stays
         * intact, and only the per-hub pool a fiber later parks on is broken. */
        if (pool != &runloom_pool && pool->kqueue_fd >= 0) {
            int finj = runloom_fault_inject(RUNLOOM_FAULT_KQUEUE_PERHUB);
            if (finj) { close(pool->kqueue_fd); pool->kqueue_fd = -1; }
        }
    }
    if (pool->kqueue_fd < 0) return;
    /* Don't leak the kqueue fd into exec'd subprocesses (parity with the
     * self-pipe below and every other poller fd) -- audit finding B8.  A
     * kqueue fd is meaningless in a child anyway; CLOEXEC drops it at execve. */
    (void)fcntl(pool->kqueue_fd, F_SETFD, FD_CLOEXEC);
    {   /* self-pipe registered in THIS pool's kqueue (EV_CLEAR) so a cross-hub
         * wake_pump can break this hub's idle kevent().  Best effort. */
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
            if (kevent(pool->kqueue_fd, &kev, 1, NULL, 0, NULL) == 0) {
                pool->selfpipe_r = pfd[0];
                pool->selfpipe_w = pfd[1];
            } else {
                close(pfd[0]);
                close(pfd[1]);
            }
        }
    }
#endif
}

static void runloom_parker_pool_lock_ensure_inited(runloom_parker_pool_t *pool)
{
#if defined(RUNLOOM_OS_WINDOWS)
    /* InterlockedCompareExchange returns the prior value; only the
     * first caller transitions 0 -> 1 and runs the init. */
    if (InterlockedCompareExchange(&pool->lock_inited, 1, 0) == 0) {
        runloom_mutex_init(&pool->lock);
        runloom_pool_backend_create(pool);
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
        runloom_mutex_init(&pool->lock);
        runloom_pool_backend_create(pool);
        __atomic_store_n(&pool->lock_inited, 2, __ATOMIC_RELEASE);
    } else {
        while (__atomic_load_n(&pool->lock_inited, __ATOMIC_ACQUIRE) != 2)
            { /* spin */ }
    }
#endif
}

const char *runloom_netpoll_backend(void)
{
#if defined(RUNLOOM_HAVE_EPOLL)
    return "epoll";
#elif defined(RUNLOOM_HAVE_KQUEUE)
    return "kqueue";
#elif defined(RUNLOOM_OS_WINDOWS)
    /* Force init so the IOCP/WSAPoll probe runs.  Without this the
     * default string ("select") is returned even when IOCP would
     * succeed -- the actual init only runs on first wait. */
    if (!__atomic_load_n(&runloom_netpoll_inited, __ATOMIC_ACQUIRE)) runloom_netpoll_init();
    return runloom_win_backend_name;
#else
    return "select";
#endif
}

/* The shared epoll fd, for the io_uring-as-loop backend to poll-add into a hub
 * ring so existing wait_fd readiness still works while the hub blocks in the
 * ring.  -1 on non-epoll backends or before init.  Forces init so the fd is
 * live for the caller (the hub arms its loop at startup). */
int runloom_netpoll_epoll_fd(void)
{
#if defined(RUNLOOM_HAVE_EPOLL)
    if (runloom_netpoll_init() != 0) return -1;
    return runloom_epoll_fd;
#else
    return -1;
#endif
}

/* The epoll fd the CURRENT hub waits on for the io_uring-as-loop F_EPOLL bridge.
 * Under RUNLOOM_PERHUB_EPOLL the hub's socket fds live in its OWN pool->epoll_fd
 * (which also NESTS the shared epoll, so io_uring eventfds still reach it), so the
 * LOOP ring must poll THAT, not the shared runloom_epoll_fd -- polling the shared
 * one would never see this hub's socket readiness and stall I/O (the LOOP+MS echo
 * server only accepted/echoed its first connection).  Falls back to the shared
 * epoll when per-hub is off or the pool epoll didn't come up. */
int runloom_netpoll_hub_epoll_fd(void)
{
#if defined(RUNLOOM_HAVE_EPOLL)
    if (runloom_netpoll_init() != 0) return -1;
    if (runloom_perhub_epoll()) {
        runloom_parker_pool_t *pool =
            runloom_parker_pool_for_hub(runloom_mn_current_hub_opaque());
        runloom_parker_pool_lock_ensure_inited(pool);
        if (pool->epoll_fd >= 0) return pool->epoll_fd;
    }
    return runloom_epoll_fd;
#else
    return -1;
#endif
}

/* ---- netpoll syscall fault injection (test-only) ----------------------- *
 * Where strace's `-e inject=` drives the Linux epoll campaign, the platforms
 * WITHOUT a syscall-injecting tracer (Windows; FreeBSD/macOS kqueue) gate IN
 * PROCESS on env vars.  RUNLOOM_FAULT_<SITE> = "<mode>:<code>", mode in
 * {once, always, nth}, forces the named pump syscall to report a failure:
 *   once:CODE   -- inject CODE exactly the first time the site is reached
 *   always:CODE -- inject CODE every time (persistent error)
 *   nth:N:CODE  -- inject CODE exactly the Nth (1-based) time the site is
 *                  reached -- the counted-exhaustive sweep's primitive
 * code is a WSA error (Windows) or an errno (kqueue).  Each site counts how
 * many times it fired (read via runloom_c._fault_count) so a test can prove a
 * PERSISTENT error BACKS OFF (bounded count) instead of busy-spinning.  Inert
 * unless the env var is set (one getenv per pump iteration when armed).  The
 * counters + count/reset are compiled on every platform so module.c exposes
 * the introspection uniformly (they stay zero on epoll, which uses strace). */
/* Fault-injection site enum lives in netpoll.h (shared with runloom_tcp.c). */
static const char *const runloom_fault_names[RUNLOOM_FAULT_NSITES] = {
    "WSAPOLL", "SELECT", "IOCP_WAIT", "IOCP_SUBMIT", "KQUEUE_WAIT",
    "KQUEUE_CREATE", "KQUEUE_CTL", "KQUEUE_PERHUB",
    "TCP_SOCKET", "TCP_CONNECT", "TCP_ACCEPT", "TCP_RECV", "TCP_SEND",
    "FD_READ", "FD_WRITE",
    "SPAWN_G", "SPAWN_STACK", "SPAWN_TSTATE" };
static volatile long runloom_fault_fired[RUNLOOM_FAULT_NSITES];
static volatile long runloom_fault_once_done[RUNLOOM_FAULT_NSITES];
/* nth-mode per-site HIT counter (every armed reach, fired or not).  Separate
 * from fired so the counted-exhaustive sweep can detect "site reached fewer
 * than N times this run" as fired==0 -- its termination fixpoint. */
static volatile long runloom_fault_seen[RUNLOOM_FAULT_NSITES];

long runloom_fault_count(const char *name)
{
    int i;
    for (i = 0; i < RUNLOOM_FAULT_NSITES; i++)
        if (strcmp(runloom_fault_names[i], name) == 0)
            return runloom_fault_fired[i];
    return -1;
}

void runloom_fault_reset(void)
{
    int i;
    for (i = 0; i < RUNLOOM_FAULT_NSITES; i++) {
        runloom_fault_fired[i] = 0;
        runloom_fault_once_done[i] = 0;
        runloom_fault_seen[i] = 0;
    }
}

/* Compiled on every platform: the netpoll pump faults (KQUEUE/SELECT/IOCP/
 * WSAPOLL) only fire on their own backend, but the syscall-surface faults
 * (TCP_*, FD_*) are reachable on Linux too (e.g. module.c fd_read/fd_write). */
static const char *const runloom_fault_envs[RUNLOOM_FAULT_NSITES] = {
    "RUNLOOM_FAULT_WSAPOLL", "RUNLOOM_FAULT_SELECT", "RUNLOOM_FAULT_IOCP_WAIT",
    "RUNLOOM_FAULT_IOCP_SUBMIT", "RUNLOOM_FAULT_KQUEUE_WAIT",
    "RUNLOOM_FAULT_KQUEUE_CREATE", "RUNLOOM_FAULT_KQUEUE_CTL",
    "RUNLOOM_FAULT_KQUEUE_PERHUB",
    "RUNLOOM_FAULT_TCP_SOCKET", "RUNLOOM_FAULT_TCP_CONNECT", "RUNLOOM_FAULT_TCP_ACCEPT",
    "RUNLOOM_FAULT_TCP_RECV", "RUNLOOM_FAULT_TCP_SEND",
    "RUNLOOM_FAULT_FD_READ", "RUNLOOM_FAULT_FD_WRITE",
    "RUNLOOM_FAULT_SPAWN_G", "RUNLOOM_FAULT_SPAWN_STACK", "RUNLOOM_FAULT_SPAWN_TSTATE" };

/* Cached "any spawn-OOM fault armed" -- keeps the hot spawn alloc sites at a
 * single branch when no RUNLOOM_FAULT_SPAWN_* env is set.  Read once. */
int runloom_spawn_fault_armed(void)
{
    static int cached = -1;
    if (cached < 0) {
        cached = (getenv("RUNLOOM_FAULT_SPAWN_G") || getenv("RUNLOOM_FAULT_SPAWN_STACK")
                  || getenv("RUNLOOM_FAULT_SPAWN_TSTATE")) ? 1 : 0;
    }
    return cached;
}

/* Returns the error code to inject (nonzero) if this site should fail now. */
int runloom_fault_inject(int site)
{
    const char *spec = getenv(runloom_fault_envs[site]);
    int code, once;
    if (spec == NULL || *spec == '\0') return 0;
    if (strncmp(spec, "once:", 5) == 0)        { once = 1; code = atoi(spec + 5); }
    else if (strncmp(spec, "always:", 7) == 0) { once = 0; code = atoi(spec + 7); }
    else if (strncmp(spec, "nth:", 4) == 0) {
        /* nth:N:CODE -- fail EXACTLY the Nth reach of this site (1-based), then
         * never again this process.  Powers the counted-exhaustive anomaly sweep
         * (tools/fault_sweep_counted.py, SQLite-style): iterate N=1,2,3,... and
         * stop when a clean run ends with _fault_count(site)==0 -- the site was
         * reached fewer than N times, so every reachable failure point has been
         * exercised.  Counting is scoped to runloom's OWN sites (unlike an
         * Nth-libc-malloc shim, which counts CPython's churn too). */
        char *end = NULL;
        long n = strtol(spec + 4, &end, 10);
        long hit;
        if (n <= 0 || end == NULL || *end != ':') return 0;
        code = atoi(end + 1);
        if (code == 0) return 0;
#if defined(RUNLOOM_OS_WINDOWS)
        hit = InterlockedIncrement(&runloom_fault_seen[site]);
#else
        hit = __atomic_add_fetch(&runloom_fault_seen[site], 1, __ATOMIC_ACQ_REL);
#endif
        if (hit != n) return 0;
        goto fire;
    }
    else return 0;
    if (code == 0) return 0;
    if (once) {
#if defined(RUNLOOM_OS_WINDOWS)
        if (InterlockedCompareExchange(&runloom_fault_once_done[site], 1, 0) != 0)
            return 0;          /* once-mode already fired */
#else
        long expected = 0;
        if (!__atomic_compare_exchange_n(&runloom_fault_once_done[site], &expected,
                1, 0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
            return 0;
#endif
    }
fire:
#if defined(RUNLOOM_OS_WINDOWS)
    InterlockedIncrement(&runloom_fault_fired[site]);
#else
    __atomic_add_fetch(&runloom_fault_fired[site], 1, __ATOMIC_RELAXED);
#endif
    return code;
}

#if defined(RUNLOOM_OS_WINDOWS)
/* Windows analogue of runloom_netpoll_wait_failed (epoll/kqueue): a hard poll
 * error returns WITHOUT blocking, so a persistent one (a polled socket closed
 * under us -> WSAENOTSOCK, a teardown race) would peg a CPU as the idle loop
 * re-pumps the instant pump() returns.  WSAEINTR is transient (retry, no
 * backoff); anything else logs once and sleeps 1 ms, degrading a silent CPU
 * peg into an observable, throttled slow-loop.  Returns 1 (handled). */
static int runloom_netpoll_win_wait_failed(int wsa_err)
{
    static volatile LONG warned;
    if (wsa_err == WSAEINTR) return 1;
    if (InterlockedExchange(&warned, 1) == 0) {
        fprintf(stderr,
                "[runloom] netpoll pump: Windows poll failed (WSA=%d); backing off "
                "to avoid a busy-spin -- the polled socket is likely closed "
                "(teardown race) or invalid\n", wsa_err);
    }
    Py_BEGIN_ALLOW_THREADS
    Sleep(1);
    Py_END_ALLOW_THREADS
    return 1;
}
#endif /* RUNLOOM_OS_WINDOWS */

int runloom_netpoll_init(void)
{
    /* ACQUIRE pairs with the RELEASE store at the end: any thread that
     * sees inited==1 here also sees the backend + array publication
     * below.  This is only the fast-path early-out; the authoritative
     * check is re-done under the lock (see below). */
    if (__atomic_load_n(&runloom_netpoll_inited, __ATOMIC_ACQUIRE)) return 0;
    runloom_parker_pool_lock_ensure_inited(&runloom_pool);
    /* EVERYTHING that brings the backend up runs under runloom_pool.lock:
     * the per-fd arrays AND the single shared epoll/kqueue/IOCP handle.
     *
     * The old code checked `inited` unlocked, then created the backend
     * OUTSIDE the lock.  Under M:N every hub calls this concurrently at
     * startup (each registers its io_uring eventfd via
     * runloom_netpoll_add_iouring_ring -> runloom_netpoll_init), so N racing
     * first-callers each ran epoll_create1 and stored runloom_epoll_fd.
     * runloom_epoll_fd ended at the last writer's value and the others
     * leaked -- but worse, while it churned, registers on other threads
     * read intermediate runloom_epoll_fd values and EPOLL_CTL_ADD'd client
     * sockets into the soon-orphaned epolls.  Those fds were armed
     * correctly but lived in an epoll instance no hub ever epoll_wait'd
     * on, so their readiness was never delivered: a parked g with data
     * waiting (Recv-Q>0), forever.  That was the ~0.1% startup-window
     * lost-wake residual.  Creating the handle exactly once, under the
     * lock, with a re-checked `inited`, closes it. */
    RUNLOOM_RLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
    /* Re-check under the lock: a racing thread may have finished the
     * whole init while we waited to acquire it. */
    if (__atomic_load_n(&runloom_netpoll_inited, __ATOMIC_ACQUIRE)) {
        RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
        return 0;
    }
    runloom_fd_arrays_init();
#if defined(RUNLOOM_HAVE_EPOLL)
    runloom_epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (runloom_epoll_fd < 0) { RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL); return -1; }
    /* Per-hub epoll (RUNLOOM_PERHUB_EPOLL): nest the freshly-created shared epoll
     * into any pool whose own epoll was already built (backend_create can run
     * lazily before this init), so every hub's pump drains the shared io_uring
     * set.  Pools created AFTER this nest it themselves in backend_create. */
    if (runloom_perhub_epoll()) {
        size_t pi;
        for (pi = 0; pi < (size_t)RUNLOOM_PARKER_POOL_MAX; pi++) {
            runloom_parker_pool_t *p = &runloom_pools[pi];
            /* > 0, not >= 0: a never-created pool slot is BSS-zero, so its
             * epoll_fd is 0 -- which is fd 0 (stdin), NOT a real epoll.  The
             * >= 0 test matched it and spewed epoll_ctl(0, ADD, ...) -> EINVAL
             * once per uninited slot (POOL_MAX-1 wasted failing syscalls every
             * netpoll_init).  A real per-hub epoll is always > 2; an inited-but-
             * unused pool is -1 (backend_create).  Single-pool reads elsewhere
             * keep >= 0 -- only these all-slots loops can see the BSS-zero slot. */
            if (p->epoll_fd > 0) {
                struct epoll_event nev;
                nev.events  = EPOLLIN;
                nev.data.fd = runloom_epoll_fd;
                (void)epoll_ctl(p->epoll_fd, EPOLL_CTL_ADD,
                                runloom_epoll_fd, &nev);
            }
        }
    }
#elif defined(RUNLOOM_HAVE_KQUEUE)
    /* Per-hub kqueue: the default pool's kqueue + self-pipe were already created
     * by the lock_ensure_inited(&runloom_pool) call above; each per-hub pool
     * creates its OWN lazily on first use (runloom_pool_backend_create).  No
     * single shared kqueue exists anymore -- that was the kq_lock contention
     * source.  Validate the default pool's handle here so a hard kqueue()
     * failure surfaces as a clean init error, exactly like the epoll branch. */
    {   /* test-only: simulate the kqueue() init-failure path on the default pool. */
        int finj = runloom_fault_inject(RUNLOOM_FAULT_KQUEUE_CREATE);
        if (finj) {
            if (runloom_pool.kqueue_fd  >= 0) { close(runloom_pool.kqueue_fd);  runloom_pool.kqueue_fd  = -1; }
            if (runloom_pool.selfpipe_r >= 0) { close(runloom_pool.selfpipe_r); runloom_pool.selfpipe_r = -1; }
            if (runloom_pool.selfpipe_w >= 0) { close(runloom_pool.selfpipe_w); runloom_pool.selfpipe_w = -1; }
            errno = finj;
        }
    }
    /* Per-hub kqueue: each pool's kqueue_fd + self-pipe are created in
     * runloom_pool_backend_create (called from lock_ensure_inited), so the
     * inline single-kqueue self-pipe setup that lived here on main is gone --
     * just validate the default pool's kqueue came up. */
    if (runloom_pool.kqueue_fd < 0) { RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL); return -1; }
#elif defined(RUNLOOM_OS_WINDOWS)
    /* Bring up Winsock once.  Idempotent via plat_compat's
     * InterlockedCompareExchange guard. */
    runloom_winsock_init();
    /* Backend selection on Windows.  Default tier order:
     *   1. IOCP+AFD     - NT 5.1+; O(1) per ready socket, scales to 10k+
     *   2. WSAPoll      - Vista+ fallback (linear walk per call)
     *   3. select()     - XP / restricted-sandbox fallback (FD_SETSIZE cap)
     *
     *   RUNLOOM_NETPOLL=wsapoll  -> force WSAPoll
     *   RUNLOOM_NETPOLL=select   -> force select() */
    {
        const char *env = getenv("RUNLOOM_NETPOLL");
        int want_wsapoll = (env != NULL && strcmp(env, "wsapoll") == 0);
        int want_select  = (env != NULL && strcmp(env, "select")  == 0);

        if (!want_wsapoll && !want_select && runloom_iocp_init() == 0) {
            runloom_win_use_iocp = 1;
            runloom_win_backend_name = "iocp-afd";
        } else if (want_select) {
            runloom_win_backend_name = "select";
        } else {
            HMODULE ws2 = GetModuleHandleA("ws2_32.dll");
            if (ws2 == NULL) ws2 = LoadLibraryA("ws2_32.dll");
            if (ws2 != NULL) {
                runloom_win_wsapoll = (runloom_wsapoll_fn)
                    (void *)GetProcAddress(ws2, "WSAPoll");
            }
            runloom_win_backend_name = (runloom_win_wsapoll != NULL) ? "wsapoll" : "select";
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
            runloom_selfpipe_r = pfd[0];
            runloom_selfpipe_w = pfd[1];
        }
    }
#endif
    /* RELEASE: publish the backend handle + array writes above to any
     * thread that later observes inited==1 via the ACQUIRE load (at the
     * top here, or in runloom_netpoll_pump).  Still inside runloom_pool.lock
     * so a racing init that lost the re-check above never observes a
     * half-built backend. */
    __atomic_store_n(&runloom_netpoll_inited, 1, __ATOMIC_RELEASE);
    RUNLOOM_RUNLOCK(&runloom_pool.lock, RUNLOOM_RANK_PARKER_POOL);
    return 0;
}

void runloom_netpoll_fini(void)
{
#if defined(RUNLOOM_HAVE_EPOLL)
    if (runloom_epoll_fd >= 0) { close(runloom_epoll_fd); runloom_epoll_fd = -1; }
    /* Per-hub epoll teardown (RUNLOOM_PERHUB_EPOLL): close every pool's own epoll
     * + wake eventfd.  Fields are -1 when the per-hub path was never created, so
     * this is a no-op under the default (shared) path. */
    {
        size_t i;
        for (i = 0; i < (size_t)RUNLOOM_PARKER_POOL_MAX; i++) {
            runloom_parker_pool_t *p = &runloom_pools[i];
            /* > 0, not >= 0: a never-created slot is BSS-zero, so these fields
             * are 0 -- closing them would close fd 0 (stdin).  fini() runs in the
             * forked child (reset_after_fork), so the >= 0 test silently closed
             * the child's stdin for every uninited pool.  Real fds are > 2; -1 is
             * not-created.  (backend_create normalizes BSS-zero slots to -1 right
             * after, line ~510, so a skipped slot is harmless.) */
            if (p->wake_efd > 0) { close(p->wake_efd); p->wake_efd = -1; }
            if (p->epoll_fd > 0) { close(p->epoll_fd); p->epoll_fd = -1; }
        }
    }
#elif defined(RUNLOOM_HAVE_KQUEUE)
    /* Per-hub teardown: close every pool's own kqueue + self-pipe. */
    {
        size_t i;
        for (i = 0; i < (size_t)RUNLOOM_PARKER_POOL_MAX; i++) {
            runloom_parker_pool_t *p = &runloom_pools[i];
            /* > 0, not >= 0: BSS-zero (never-created) slots have these at 0 --
             * close(0) would close the forked child's stdin.  See the epoll fini
             * loop above for the full rationale. */
            if (p->kqueue_fd  > 0) { close(p->kqueue_fd);  p->kqueue_fd  = -1; }
            if (p->selfpipe_r > 0) { close(p->selfpipe_r); p->selfpipe_r = -1; }
            if (p->selfpipe_w > 0) { close(p->selfpipe_w); p->selfpipe_w = -1; }
        }
    }
#elif defined(RUNLOOM_OS_WINDOWS)
    if (runloom_win_use_iocp) {
        runloom_iocp_fini();
        runloom_win_use_iocp = 0;
    }
    /* WSAPoll / select are stateless; nothing else to close.
     * Winsock itself is left up by design (see runloom_winsock_init). */
#else
    /* select() fallback: tear down the self-pipe wake. */
    if (runloom_selfpipe_r >= 0) { close(runloom_selfpipe_r); runloom_selfpipe_r = -1; }
    if (runloom_selfpipe_w >= 0) { close(runloom_selfpipe_w); runloom_selfpipe_w = -1; }
#endif
    __atomic_store_n(&runloom_netpoll_inited, 0, __ATOMIC_RELEASE);
}

/* Reset netpoll in a forked CHILD.  The inherited epoll/kqueue fd is the
 * SAME kernel object as the parent's (fork shares it), so a child that did
 * I/O would cross-contaminate the parent's readiness -- it must get its
 * own.  Single-threaded here, so no lock is taken; we only re-init the per-
 * pool locks (a dead hub may have held one at fork) and drop the inherited
 * parker bookkeeping (it references parent fibers that don't exist in
 * the child).  runloom_netpoll_fini() closes the inherited fds + marks uninit
 * (it is itself lock-free); the next wait_fd re-creates a fresh poller. */
void runloom_netpoll_reset_after_fork(void)
{
    size_t i;
    runloom_netpoll_fini();        /* close inherited fds, inited = 0 (lock-free) */
    for (i = 0; i < (size_t)RUNLOOM_PARKER_POOL_MAX; i++) {
        runloom_parker_pool_t *p = &runloom_pools[i];
        runloom_mutex_init(&p->lock);
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
        /* Fresh per-hub kqueue + self-pipe for the child: fork shares the
         * parent's kernel kqueue, so the inherited handles (closed by fini above,
         * set -1) must not be reused.  lock_inited is set to 2 directly here, so
         * the lazy backend path is skipped -- create it explicitly.  No-op on
         * epoll/Windows/select. */
        runloom_pool_backend_create(p);
    }
    /* The per-fd registration bitmap says "fd X is already armed in the
     * poller"; against the child's BRAND-NEW poller every such bit is a lie
     * that would make wait_fd skip EPOLL_CTL_ADD and lose the event.  Clear
     * it (and the pending-wake bits) so the child re-arms cleanly. */
    if (runloom_fd_registered_bm != NULL)
        memset(runloom_fd_registered_bm, 0, runloom_fd_registered_cap_bytes);
    {
        unsigned char *pw = __atomic_load_n(&runloom_fd_pending_wake, __ATOMIC_RELAXED);
        size_t cap = __atomic_load_n(&runloom_fd_pending_wake_cap, __ATOMIC_RELAXED);
        if (pw != NULL) memset(pw, 0, cap);
        /* CRITICAL: registered_bm is NOT actually what gates the zero-syscall
         * register skip -- runloom_fd_armed is (netpoll_register.c.inc:118,166).
         * Against the child's brand-new epoll, a stale armed byte makes wait_fd
         * take the "already armed -> 0 syscalls" skip, so an inherited fd is
         * NEVER EPOLL_CTL_ADD'd into the child's poller and a cooperative recv on
         * it parks forever (silent hang in the pre-fork-server / multiprocessing
         * pattern).  The non-fork close paths (unregister:300, release_if_idle:361)
         * clear fd_armed + fd_epoll_pool together; the fork path must too.  Both
         * are cap_fds-sized (same cap as pending_wake) and same-TU statics.
         * epoll_pool resets to 0xFF ("no owner"), matching arrays_init. */
        if (runloom_fd_armed != NULL) memset(runloom_fd_armed, 0, cap);
        if (runloom_fd_epoll_pool != NULL) memset(runloom_fd_epoll_pool, 0xFF, cap);
    }
#if defined(RUNLOOM_HAVE_EPOLL)
    /* The per-hub io_uring ring registry names the PARENT hubs' rings (dead in
     * the child).  The child's fresh hubs re-register their own; if we keep the
     * stale entries the pump dispatches CQEs to freed rings -> hang/crash.  The
     * epoll/eventfd/pump-wake fds were closed with the epoll fd above. */
    runloom_iouring_ring_count = 0;
    runloom_iouring_eventfd_in_epoll = -1;
    runloom_pump_wake_fd = -1;
#endif
}

static long long monotonic_ns(void)
{
    return runloom_monotonic_ns();
}
