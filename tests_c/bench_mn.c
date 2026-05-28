/* bench_mn.c -- pure-C M:N + netpoll bench / repro for the 3.13t
 * residual timeout at N>=512.
 *
 * Mirrors /tmp/probe_mn.py: spawns N TCP clients on H hubs.  Each
 * client connects to a local Python-style threading echo server,
 * does M send/recv round-trips on an 8-byte payload, closes.
 *
 * Differences from the Python harness:
 *   - No CPython interpreter -- everything runs through the C-only
 *     pygo_mn_go_c spawn path added in this commit.  Python is only
 *     used to call Py_Initialize once at startup so PyMem_*, etc., are
 *     available (pygo_core internals still call into CPython for the
 *     slab allocator and a few other places even when no Python work
 *     is actually performed -- removing that dependency is a separate
 *     project).
 *   - Server is a pthread-based echo loop; the client side is the
 *     M:N path we want to exercise.
 *   - One process per N invocation: no orchestration loop.
 *
 * Build:
 *   make -C tests_c bench_mn
 *
 * Run:
 *   tests_c/bench_mn 256 4 20   # N=256 H=4 M=20
 *
 * Sanitizer builds (slower; useful for catching the residual race):
 *   make -C tests_c bench_mn-asan
 *   make -C tests_c bench_mn-tsan
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
#include <arpa/inet.h>

#include "../src/pygo_core/pygo_sched.h"
#include "../src/pygo_core/mn_sched.h"
#include "../src/pygo_core/netpoll.h"

#define PAYLOAD     "hellopyg"
#define PAYLOAD_LEN 8

/* ---- echo server (pure pthread, no pygo) ---- */
typedef struct {
    int fd;
    int payload_len;
} echo_conn_t;

static void *echo_conn_thread(void *arg)
{
    echo_conn_t *c = (echo_conn_t *)arg;
    char buf[64];
    while (1) {
        ssize_t n = recv(c->fd, buf, c->payload_len, 0);
        if (n <= 0) break;
        ssize_t off = 0;
        while (off < n) {
            ssize_t w = send(c->fd, buf + off, n - off, MSG_NOSIGNAL);
            if (w < 0) { off = -1; break; }
            off += w;
        }
        if (off < 0) break;
    }
    close(c->fd);
    free(c);
    return NULL;
}

typedef struct {
    int listen_fd;
    int payload_len;
} echo_server_t;

static void *echo_accept_thread(void *arg)
{
    echo_server_t *s = (echo_server_t *)arg;
    while (1) {
        int fd = accept(s->listen_fd, NULL, NULL);
        if (fd < 0) {
            if (errno == EINTR) continue;
            return NULL;       /* listener closed */
        }
        echo_conn_t *c = malloc(sizeof(*c));
        c->fd = fd;
        c->payload_len = s->payload_len;
        pthread_t tid;
        pthread_create(&tid, NULL, echo_conn_thread, c);
        pthread_detach(tid);
    }
}

static int start_echo_server(int backlog)
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
        perror("bind");
        return -1;
    }
    if (listen(fd, backlog) < 0) {
        perror("listen");
        return -1;
    }
    socklen_t alen = sizeof(addr);
    getsockname(fd, (struct sockaddr *)&addr, &alen);
    int port = ntohs(addr.sin_port);
    echo_server_t *s = malloc(sizeof(*s));
    s->listen_fd = fd;
    s->payload_len = PAYLOAD_LEN;
    pthread_t tid;
    pthread_create(&tid, NULL, echo_accept_thread, s);
    pthread_detach(tid);
    return port;
}

/* ---- client goroutine ---- */
static int g_port;
static int g_M;
static volatile long g_done_count = 0;
static pthread_mutex_t g_done_lock = PTHREAD_MUTEX_INITIALIZER;

static void set_nonblock(int fd)
{
    int fl = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

static int connect_nonblock(int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    set_nonblock(fd);
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
        /* EINPROGRESS: wait for writability. */
        if (pygo_netpoll_wait_fd(fd, 2 /* PYGO_NETPOLL_WRITE */, -1LL) < 0) {
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

static int send_all_nb(int fd, const void *buf, size_t len)
{
    const char *p = (const char *)buf;
    size_t left = len;
    while (left > 0) {
        ssize_t n = send(fd, p, left, MSG_NOSIGNAL);
        if (n > 0) { p += n; left -= n; continue; }
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            return -1;
        }
        if (pygo_netpoll_wait_fd(fd, 2 /* WRITE */, -1LL) < 0) return -1;
    }
    return 0;
}

static int recv_all_nb(int fd, void *buf, size_t len)
{
    char *p = (char *)buf;
    size_t left = len;
    while (left > 0) {
        ssize_t n = recv(fd, p, left, 0);
        if (n > 0) { p += n; left -= n; continue; }
        if (n == 0) return -1;     /* peer closed */
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            return -1;
        }
        if (pygo_netpoll_wait_fd(fd, 1 /* READ */, -1LL) < 0) return -1;
    }
    return 0;
}

static volatile long g_entered = 0;
static volatile long g_connected = 0;
static volatile long g_sent_once = 0;

static void client_g(void *arg)
{
    (void)arg;
    __atomic_fetch_add(&g_entered, 1, __ATOMIC_RELAXED);
    int fd = connect_nonblock(g_port);
    if (fd < 0) return;
    __atomic_fetch_add(&g_connected, 1, __ATOMIC_RELAXED);
    char buf[PAYLOAD_LEN];
    for (int i = 0; i < g_M; i++) {
        if (send_all_nb(fd, PAYLOAD, PAYLOAD_LEN) < 0) break;
        if (i == 0) __atomic_fetch_add(&g_sent_once, 1, __ATOMIC_RELAXED);
        if (recv_all_nb(fd, buf, PAYLOAD_LEN) < 0) break;
    }
    pygo_netpoll_unregister(fd);   /* clear registration bitmap so fd reuse re-registers */
    close(fd);
    pthread_mutex_lock(&g_done_lock);
    g_done_count++;
    pthread_mutex_unlock(&g_done_lock);
}

/* ---- main ---- */
static double now_seconds(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

int main(int argc, char **argv)
{
    int N = (argc > 1) ? atoi(argv[1]) : 256;
    int H = (argc > 2) ? atoi(argv[2]) : 4;
    int M = (argc > 3) ? atoi(argv[3]) : 20;
    g_M = M;

    /* Initialise CPython enough to satisfy pygo_core's internal calls
     * (slab allocator uses PyMem_*, tstate setup, etc.).  This is a
     * minimal embed -- no Python script runs. */
    Py_Initialize();

    int port = start_echo_server(N + 16);
    if (port < 0) {
        fprintf(stderr, "server start failed\n");
        return 2;
    }
    g_port = port;
    /* Tiny grace period for the accept thread to wire up. */
    usleep(50000);

    /* pygo_mn_init returns n_threads on success (not 0), -1 on failure. */
    if (pygo_mn_init(H) < 0) {
        fprintf(stderr, "mn_init failed: ");
        if (PyErr_Occurred()) PyErr_PrintEx(0);
        else fprintf(stderr, "(no Python error set)\n");
        return 2;
    }

    double t0 = now_seconds();
    for (int i = 0; i < N; i++) {
        if (pygo_mn_go_c(client_g, NULL) < 0) {
            fprintf(stderr, "mn_go_c failed at i=%d: %s\n",
                    i, strerror(errno));
            return 2;
        }
    }
    pygo_mn_run();
    double dt = now_seconds() - t0;
    pygo_mn_fini();

    printf("N=%d H=%d M=%d done=%ld/%d entered=%ld connected=%ld sent_once=%ld %.3fs %.1fK/s\n",
           N, H, M, g_done_count, N, g_entered, g_connected, g_sent_once, dt,
           (double)N * M / dt / 1000.0);
    if (g_done_count != N) {
        fprintf(stderr, "FAIL: %ld/%d completed\n", g_done_count, N);
        return 1;
    }
    return 0;
}
