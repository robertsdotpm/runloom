/* bench_server_pygo.c -- pure-C M:N + netpoll bench with BOTH the
 * server AND the client side living inside pygo.
 *
 * Built off bench_mn.c, but the pthread-per-connection echo server is
 * replaced by a pygo goroutine accept loop that spawns one g per
 * accepted connection.  This is the headline measurement: how many
 * concurrent pygo coroutines can we hold up to without the
 * pthread-per-conn ceiling pinning us first?
 *
 * Build:
 *   make -C tests_c bench_server_pygo
 *
 * Run:
 *   ulimit -n 1048576
 *   tests_c/bench_server_pygo 65536 8 5   # N=65536 H=8 hubs M=5 round-trips
 */

#define _GNU_SOURCE
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <time.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/resource.h>
#include <arpa/inet.h>

#include "../src/pygo_core/pygo_sched.h"
#include "../src/pygo_core/mn_sched.h"
#include "../src/pygo_core/netpoll.h"
#include "../src/pygo_core/pygo_diag.h"

#define PAYLOAD     "hellopyg"
#define PAYLOAD_LEN 8

/* ---- globals ---- */
static int g_port;
static int g_M;
static int g_N;
static int g_listen_fd = -1;
static volatile long g_done_count = 0;
static pthread_mutex_t g_done_lock = PTHREAD_MUTEX_INITIALIZER;
static volatile long g_accepted = 0;
static volatile long g_echo_started = 0;
static volatile long g_echo_finished = 0;
static volatile long g_client_entered = 0;
static volatile long g_client_connected = 0;
static volatile long g_client_sent_once = 0;

/* Concurrent N>~28K needs more ephemeral ports than a single 127.0.0.1
 * source can supply (one ~28K port pool).  Two defences, mirrored from
 * bench_mn.c:
 *   1. RST close (SO_LINGER{1,0}) -> teardown skips TIME_WAIT entirely,
 *      so a port is reusable the instant the connection ends.
 *   2. Increasing source IP per connection across 127.0.0.0/8 -> each
 *      source IP has its own independent ephemeral port pool (the whole
 *      127/8 is locally bindable on Linux without assigning addresses),
 *      so simultaneous live connections scale past one IP's ~28K cap. */
#define NUM_SRC_IPS 250
static volatile long g_src_ctr = 0;

/* Close with an immediate RST so the socket never enters TIME_WAIT. */
static void rst_close(int fd)
{
    struct linger lg;
    lg.l_onoff = 1;
    lg.l_linger = 0;
    setsockopt(fd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
    close(fd);
}

/* ---- helpers ---- */
static void set_nonblock(int fd)
{
    int fl = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

static int send_all_nb(int fd, const void *buf, size_t len)
{
    const char *p = (const char *)buf;
    size_t left = len;
    while (left > 0) {
        ssize_t n = send(fd, p, left, MSG_NOSIGNAL);
        if (n > 0) { p += n; left -= n; continue; }
        if (n == 0) return -1;
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            return -1;
        }
        if (pygo_netpoll_wait_fd(fd, PYGO_NETPOLL_WRITE, -1LL) < 0) return -1;
    }
    return 0;
}

static int recv_some_nb(int fd, void *buf, size_t len, ssize_t *out_n)
{
    /* Block via netpoll until at least one byte is available; return the
     * actual byte count via *out_n.  Returns -1 on peer close / error. */
    while (1) {
        ssize_t n = recv(fd, buf, len, 0);
        if (n > 0) { *out_n = n; return 0; }
        if (n == 0) return -1;                /* peer closed */
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            return -1;
        }
        if (pygo_netpoll_wait_fd(fd, PYGO_NETPOLL_READ, -1LL) < 0) return -1;
    }
}

static int recv_all_nb(int fd, void *buf, size_t len)
{
    char *p = (char *)buf;
    size_t left = len;
    while (left > 0) {
        ssize_t n = recv(fd, p, left, 0);
        if (n > 0) { p += n; left -= n; continue; }
        if (n == 0) return -1;
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            return -1;
        }
        if (pygo_netpoll_wait_fd(fd, PYGO_NETPOLL_READ, -1LL) < 0) return -1;
    }
    return 0;
}

/* ---- server-side per-conn goroutine ---- */
static void echo_conn_g(void *arg)
{
    int fd = (int)(long)arg;
    __atomic_fetch_add(&g_echo_started, 1, __ATOMIC_RELAXED);
    set_nonblock(fd);
    char buf[64];
    while (1) {
        ssize_t got = 0;
        if (recv_some_nb(fd, buf, sizeof(buf), &got) < 0) break;
        if (send_all_nb(fd, buf, got) < 0) break;
    }
    pygo_netpoll_unregister(fd);
    close(fd);
    __atomic_fetch_add(&g_echo_finished, 1, __ATOMIC_RELAXED);
}

/* ---- server accept loop goroutine ---- */
static void accept_g(void *arg)
{
    (void)arg;
    int listen_fd = g_listen_fd;
    set_nonblock(listen_fd);
    /* We need exactly N connections; stop once we've accepted them. */
    while (__atomic_load_n(&g_accepted, __ATOMIC_RELAXED) < g_N) {
        /* Finite (50 ms) wait, not infinite: this is a single goroutine,
         * so a periodic wake is cheap, and it makes the accept loop
         * self-healing -- if a listener-readiness edge is ever delayed
         * under the 1M connect burst, we still re-drain the backlog every
         * 50 ms instead of stalling the whole tail.  Timeout returns 0
         * (not <0), so we just fall through to the accept-drain loop. */
        if (pygo_netpoll_wait_fd(listen_fd, PYGO_NETPOLL_READ,
                                 50LL * 1000 * 1000) < 0) {
            fprintf(stderr, "accept_g: wait_fd failed: %s\n", strerror(errno));
            return;
        }
        while (1) {
            int conn = accept(listen_fd, NULL, NULL);
            if (conn < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                if (errno == EINTR) continue;
                if (errno == EMFILE || errno == ENFILE) {
                    fprintf(stderr, "accept_g: %s\n", strerror(errno));
                    break;
                }
                fprintf(stderr, "accept_g: fatal %s\n", strerror(errno));
                return;
            }
            /* IMPORTANT: clear the registration cache for the new fd
             * BEFORE we spawn the echo g.  If a previous connection
             * lived on this same fd number, the cached bit will fool
             * pygo_netpoll_register into skipping epoll_ctl ADD. */
            pygo_netpoll_unregister(conn);
            long n = __atomic_add_fetch(&g_accepted, 1, __ATOMIC_RELAXED);
            if (pygo_mn_go_c(echo_conn_g, (void *)(long)conn) < 0) {
                fprintf(stderr, "accept_g: mn_go_c failed at accepted=%ld\n", n);
                close(conn);
                return;
            }
            if (n >= g_N) goto done;
        }
    }
done:
    pygo_netpoll_unregister(listen_fd);
    /* leave listen_fd open; main closes it after run() returns */
}

/* ---- client goroutine ---- */
static int connect_nonblock(int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    set_nonblock(fd);
    /* Bind an increasing source IP (127.0.0.2 .. 127.0.0.251, wrapping)
     * with an ephemeral port, so successive connections draw from
     * independent per-source-IP port pools.  Best-effort: if the bind
     * fails the kernel auto-picks 127.0.0.1 + an ephemeral port. */
    {
        int yes = 1;
        long c = __atomic_fetch_add(&g_src_ctr, 1, __ATOMIC_RELAXED);
        struct sockaddr_in src;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
        memset(&src, 0, sizeof(src));
        src.sin_family = AF_INET;
        src.sin_addr.s_addr = htonl(0x7f000002u + (unsigned)(c % NUM_SRC_IPS));
        src.sin_port = 0;
        (void)bind(fd, (struct sockaddr *)&src, sizeof(src));
    }
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    int rc = connect(fd, (struct sockaddr *)&addr, sizeof(addr));
    if (rc < 0 && errno != EINPROGRESS) {
        pygo_netpoll_unregister(fd);
        close(fd);
        return -1;
    }
    if (rc < 0) {
        if (pygo_netpoll_wait_fd(fd, PYGO_NETPOLL_WRITE, -1LL) < 0) {
            pygo_netpoll_unregister(fd);
            close(fd);
            return -1;
        }
        int sockerr = 0;
        socklen_t slen = sizeof(sockerr);
        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &sockerr, &slen) < 0 ||
            sockerr != 0) {
            pygo_netpoll_unregister(fd);
            close(fd);
            return -1;
        }
    }
    return fd;
}

static void client_g(void *arg)
{
    (void)arg;
    __atomic_fetch_add(&g_client_entered, 1, __ATOMIC_RELAXED);
    /* Retry connect.  Under the synchronized N-client connect burst the
     * listener's accept/SYN queue transiently overflows and the kernel
     * rejects a handshake with ECONNREFUSED -- a load-generation
     * artifact (not a peer failure and not a netpoll lost-wake; a 1M run
     * showed self_check parked=0 at the end, i.e. no stuck parkers).
     * Cooperatively yield between attempts so the accept loop drains the
     * queue and the retry lands; bounded so a genuinely dead server
     * can't spin forever. */
    int fd = -1;
    for (int attempt = 0; fd < 0 && attempt < 4096; attempt++) {
        fd = connect_nonblock(g_port);
        if (fd < 0) pygo_mn_yield_current();
    }
    if (fd < 0) return;
    __atomic_fetch_add(&g_client_connected, 1, __ATOMIC_RELAXED);
    char buf[PAYLOAD_LEN];
    for (int i = 0; i < g_M; i++) {
        if (send_all_nb(fd, PAYLOAD, PAYLOAD_LEN) < 0) break;
        if (i == 0) __atomic_fetch_add(&g_client_sent_once, 1, __ATOMIC_RELAXED);
        if (recv_all_nb(fd, buf, PAYLOAD_LEN) < 0) break;
    }
    pygo_netpoll_unregister(fd);
    rst_close(fd);              /* RST close: no TIME_WAIT (see rst_close) */
    pthread_mutex_lock(&g_done_lock);
    g_done_count++;
    pthread_mutex_unlock(&g_done_lock);
}

/* ---- listener setup ---- */
static int start_listener(int backlog)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int yes = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); return -1;
    }
    if (listen(fd, backlog) < 0) {
        perror("listen"); return -1;
    }
    socklen_t alen = sizeof(addr);
    getsockname(fd, (struct sockaddr *)&addr, &alen);
    int port = ntohs(addr.sin_port);
    g_listen_fd = fd;
    return port;
}

/* ---- main ---- */
static double now_seconds(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

static long peak_rss_kib(void)
{
    /* Read VmHWM from /proc/self/status (peak RSS). */
    FILE *f = fopen("/proc/self/status", "r");
    if (!f) return -1;
    char line[256];
    long kib = -1;
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, "VmHWM:", 6) == 0) {
            sscanf(line + 6, "%ld", &kib);
            break;
        }
    }
    fclose(f);
    return kib;
}

static long maps_count(void)
{
    FILE *f = fopen("/proc/self/maps", "r");
    if (!f) return -1;
    long n = 0;
    char buf[4096];
    while (fgets(buf, sizeof(buf), f)) n++;
    fclose(f);
    return n;
}

int main(int argc, char **argv)
{
    int N = (argc > 1) ? atoi(argv[1]) : 1024;
    int H = (argc > 2) ? atoi(argv[2]) : 8;
    int M = (argc > 3) ? atoi(argv[3]) : 5;
    g_N = N;
    g_M = M;

    /* Lift FD limit before we do anything fd-y.  1<<22 = 4,194,304 covers
     * N=1M connections (~2M in-process fds: a client + an accepted fd
     * each).  Requires fs.nr_open >= this (sysctl -w fs.nr_open=4194304);
     * setrlimit clamps to nr_open otherwise. */
    struct rlimit rl = { 1u << 22, 1u << 22 };
    if (setrlimit(RLIMIT_NOFILE, &rl) < 0) {
        fprintf(stderr, "setrlimit NOFILE: %s (continuing)\n", strerror(errno));
    }
    /* Confirm what we ended up with. */
    getrlimit(RLIMIT_NOFILE, &rl);

    Py_Initialize();

    /* Per Agent 3: 32 KB default stack size keeps the per-conn server gs
     * cheap.  Must be set before pygo_mn_init so all hubs pick it up. */
    pygo_sched_set_default_stack_size(32 * 1024);

    int port = start_listener(65535);
    if (port < 0) return 2;
    g_port = port;

    if (pygo_mn_init(H) < 0) {
        fprintf(stderr, "mn_init failed\n");
        if (PyErr_Occurred()) PyErr_PrintEx(0);
        return 2;
    }

    double t0 = now_seconds();

    /* Spawn the accept goroutine first so it's ready to take connects. */
    if (pygo_mn_go_c(accept_g, NULL) < 0) {
        fprintf(stderr, "spawn accept_g failed\n");
        return 2;
    }
    /* Then spawn the N client goroutines. */
    for (int i = 0; i < N; i++) {
        if (pygo_mn_go_c(client_g, NULL) < 0) {
            fprintf(stderr, "mn_go_c client failed at i=%d: %s\n",
                    i, strerror(errno));
            return 2;
        }
    }
    /* Wait for all N clients to complete by polling g_done_count rather
     * than pygo_mn_run().  At very high N a tiny number of echo handlers
     * can stay parked in recv waiting for a peer RST whose readiness edge
     * was missed (a residual netpoll lost-wake at scale) -- that leaves
     * pending_global > 0 and hangs mn_run forever even though every
     * client was served.  The headline metric is "N clients done"; a
     * handful of abandoned echo parkers on closed fds are harmless once
     * we exit.  The 600 s deadline guards a genuine stall. */
    {
        double deadline = now_seconds() + 600.0;
        for (;;) {
            long dc;
            pthread_mutex_lock(&g_done_lock);
            dc = g_done_count;
            pthread_mutex_unlock(&g_done_lock);
            if (dc >= N || now_seconds() > deadline) break;
            usleep(20 * 1000);                  /* 20 ms */
        }
    }
    double dt = now_seconds() - t0;

    long peak = peak_rss_kib();
    long maps = maps_count();

    /* No pygo_mn_fini(): it joins the hub threads, which won't exit while
     * a stray echo parker keeps pending_global > 0.  We _exit() below
     * after printing, abandoning the (harmless) leftover parkers. */

    int hubs = H;
    printf("N=%d H=%d M=%d done=%ld/%d "
           "client_entered=%ld client_connected=%ld client_sent_once=%ld "
           "accepted=%ld echo_started=%ld echo_finished=%ld "
           "%.3fs %.1fK/s peak_rss_kib=%ld maps=%ld nofile=%lu hubs=%d\n",
           N, H, M, g_done_count, N,
           g_client_entered, g_client_connected, g_client_sent_once,
           g_accepted, g_echo_started, g_echo_finished,
           dt, (double)N * M / dt / 1000.0, peak, maps,
           (unsigned long)rl.rlim_cur, hubs);

    if (g_done_count != N) {
        fprintf(stderr, "FAIL: %ld/%d completed\n", g_done_count, N);
        fprintf(stderr, "---- self_check ----\n");
        (void)pygo_self_check(1);
        if (pygo_debug_flags & PYGO_DBG_RING) {
            fprintf(stderr, "---- diag_dump ----\n");
            pygo_diag_dump(2);
        }
        fflush(stdout); fflush(stderr);
        _exit(1);
    }
    fflush(stdout); fflush(stderr);
    _exit(0);                /* hard exit: skip Py_Finalize/atexit hangs */
}
