/* Part of module.c -- #included into that translation unit, not compiled
 * standalone.  See module.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* ---- Module-level functions ---- */

static PyObject *m_yield(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    /* yield is just "switch back to caller".  We hold the GIL across the
     * switch on GIL-builds because the caller will resume us on the same
     * OS thread; under free-threaded Python the thread state stays
     * attached.  This is safe because we never migrate a coroutine
     * between OS threads (each thread has its own scheduler). */
    runloom_coro_yield();
    Py_RETURN_NONE;
}

static PyObject *m_backend(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    return PyUnicode_FromString(runloom_coro_backend());
}

static PyObject *m_thread_init(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    if (runloom_coro_thread_init() != 0) {
        PyErr_SetString(PyExc_OSError, "runloom_coro_thread_init failed");
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *m_thread_fini(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    runloom_coro_thread_fini();
    Py_RETURN_NONE;
}

static PyObject *m_warmup(PyObject *self, PyObject *args)
{
    int n;
    Py_ssize_t stack_size = 131072;
    int actual;
    (void)self;
    if (!PyArg_ParseTuple(args, "i|n", &n, &stack_size)) return NULL;
    if (stack_size < 0) {
        /* A negative size casts to a giant size_t; the coro allocator's
         * guard-page math then underflows -> SIGSEGV.  Reject it the same
         * way Coro()/go() do. */
        PyErr_SetString(PyExc_ValueError, "stack_size must be non-negative");
        return NULL;
    }
    actual = runloom_coro_warmup((size_t)stack_size, n);
    return PyLong_FromLong(actual);
}

static PyObject *m_prewarm(PyObject *self, PyObject *args, PyObject *kwds)
{
    int n;
    Py_ssize_t stack_size = 512 * 1024;     /* match the default fiber stack */
    int background = 1;
    int r;
    static char *kw[] = {"n", "stack_size", "background", NULL};
    (void)self;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "i|np", kw,
                                     &n, &stack_size, &background))
        return NULL;
    if (stack_size < 0) {
        PyErr_SetString(PyExc_ValueError, "stack_size must be non-negative");
        return NULL;
    }
    Py_BEGIN_ALLOW_THREADS
    r = runloom_coro_prewarm((size_t)stack_size, n, background);
    Py_END_ALLOW_THREADS
    return PyLong_FromLong(r);
}

static PyObject *m_prewarm_keep(PyObject *self, PyObject *args, PyObject *kwds)
{
    int target;
    Py_ssize_t stack_size = 512 * 1024;
    int r;
    static char *kw[] = {"target", "stack_size", NULL};
    (void)self;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "i|n", kw, &target, &stack_size))
        return NULL;
    if (stack_size < 0) {
        PyErr_SetString(PyExc_ValueError, "stack_size must be non-negative");
        return NULL;
    }
    r = runloom_coro_prewarm_keep((size_t)stack_size, target);
    return PyLong_FromLong(r);
}

static PyObject *m_prewarm_stop(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    Py_BEGIN_ALLOW_THREADS
    runloom_coro_prewarm_stop();
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

/* Native C TCP recv into a writable buffer.  Equivalent to
 *   sock.recv_into(buf)  with cooperative blocking, but bypasses
 *   the Python socket.recv_into method dispatch (saves ~3-5 us per
 *   call on tight echo loops).
 *
 * Signature: runloom_c.tcp_recv(fd: int, buf: bytearray-like, n: int) -> int
 *   Returns the number of bytes received; 0 = orderly shutdown.
 *
 * Implementation: loop recv(fd, buf, n, 0); on EAGAIN/EWOULDBLOCK
 * park on the netpoll, retry.  The `buf` argument must be a
 * writable buffer (bytearray, memoryview, ...) and runloom will fill
 * its first n bytes. */
#if defined(RUNLOOM_OS_WINDOWS)
#else
#  include <sys/socket.h>
#  include <unistd.h>
#  include <errno.h>
#endif

static PyObject *m_tcp_recv(PyObject *self, PyObject *args)
{
    int fd;
    Py_buffer buf;
    Py_ssize_t n_bytes;
    int flags = 0;
    Py_ssize_t got = 0;
    (void)self;

    if (!PyArg_ParseTuple(args, "iw*n|i", &fd, &buf, &n_bytes, &flags)) return NULL;
    if (n_bytes > buf.len) n_bytes = buf.len;
    if (n_bytes <= 0) {
        PyBuffer_Release(&buf);
        return PyLong_FromLong(0);
    }

    while (1) {
#if defined(RUNLOOM_OS_WINDOWS)
        int r = recv((SOCKET)fd, (char *)buf.buf, (int)n_bytes, flags);
        if (r > 0) { got = r; break; }
        if (r == 0) { got = 0; break; }         /* orderly shutdown */
        {
            int err = WSAGetLastError();
            if (err != WSAEWOULDBLOCK) {
                PyBuffer_Release(&buf);
                PyErr_SetFromWindowsErr(err);
                return NULL;
            }
        }
#else
        ssize_t r = recv(fd, (char *)buf.buf, (size_t)n_bytes, flags);
        if (r > 0) { got = (Py_ssize_t)r; break; }
        if (r == 0) { got = 0; break; }
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
#endif
        /* Park on read. */
        if (runloom_netpoll_wait_fd_coop(fd, /*RUNLOOM_NETPOLL_READ*/ 1, -1LL) < 0) {
            PyBuffer_Release(&buf);
            /* wait_fd may have run a Python signal handler that raised (e.g. an
             * alarm handler / Ctrl-C) -> propagate it; don't overwrite with OSError. */
            return PyErr_Occurred() ? NULL : PyErr_SetFromErrno(PyExc_OSError);
        }
    }

    PyBuffer_Release(&buf);
    return PyLong_FromSsize_t(got);
}

/* Native C TCP recv that allocates and returns a bytes object.
 * Equivalent to sock.recv(n[, flags]) with cooperative blocking,
 * but bypasses Python frame dispatch and exception-on-EAGAIN cost
 * of the monkey-patched socket.recv path.
 *
 * Signature: runloom_c.tcp_recv_alloc(fd, n, flags=0) -> bytes
 *   Returns a bytes object of length <= n; b"" on orderly shutdown.
 */
static PyObject *m_tcp_recv_alloc(PyObject *self, PyObject *args)
{
    int fd;
    Py_ssize_t n_bytes;
    int flags = 0;
    Py_ssize_t got = 0;
    PyObject *result;
    char *out;
    (void)self;

    if (!PyArg_ParseTuple(args, "in|i", &fd, &n_bytes, &flags)) return NULL;
    if (n_bytes < 0) {
        PyErr_SetString(PyExc_ValueError, "negative bufsize");
        return NULL;
    }
    if (n_bytes == 0) {
        return PyBytes_FromStringAndSize(NULL, 0);
    }
    result = PyBytes_FromStringAndSize(NULL, n_bytes);
    if (result == NULL) return NULL;
    out = PyBytes_AS_STRING(result);

    while (1) {
#if defined(RUNLOOM_OS_WINDOWS)
        int r = recv((SOCKET)fd, out, (int)n_bytes, flags);
        if (r >= 0) { got = r; break; }
        {
            int err = WSAGetLastError();
            if (err != WSAEWOULDBLOCK) {
                Py_DECREF(result);
                PyErr_SetFromWindowsErr(err);
                return NULL;
            }
        }
#else
        ssize_t r = recv(fd, out, (size_t)n_bytes, flags);
        if (r >= 0) { got = (Py_ssize_t)r; break; }
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            Py_DECREF(result);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
#endif
        if (runloom_netpoll_wait_fd_coop(fd, /*RUNLOOM_NETPOLL_READ*/ 1, -1LL) < 0) {
            Py_DECREF(result);
            /* wait_fd may have run a Python signal handler that raised (e.g. an
             * alarm handler / Ctrl-C) -> propagate it; don't overwrite with OSError. */
            return PyErr_Occurred() ? NULL : PyErr_SetFromErrno(PyExc_OSError);
        }
    }

    if (got < n_bytes) {
        if (_PyBytes_Resize(&result, got) < 0) return NULL;
    }
    return result;
}

/* Native C TCP send.  Equivalent to sock.sendall(buf) with
 * cooperative blocking.  Loops until all bytes sent or error. */
static PyObject *m_tcp_send(PyObject *self, PyObject *args)
{
    int fd;
    Py_buffer buf;
    int flags = 0;
    Py_ssize_t sent = 0;
    (void)self;

    if (!PyArg_ParseTuple(args, "iy*|i", &fd, &buf, &flags)) return NULL;

    while (sent < buf.len) {
#if defined(RUNLOOM_OS_WINDOWS)
        int r = send((SOCKET)fd, (const char *)buf.buf + sent,
                     (int)(buf.len - sent), flags);
        if (r >= 0) { sent += r; continue; }
        {
            int err = WSAGetLastError();
            if (err != WSAEWOULDBLOCK) {
                PyBuffer_Release(&buf);
                PyErr_SetFromWindowsErr(err);
                return NULL;
            }
        }
#else
        ssize_t r = send(fd, (const char *)buf.buf + sent,
                         (size_t)(buf.len - sent), flags);
        if (r >= 0) { sent += r; continue; }
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
#endif
        if (runloom_netpoll_wait_fd_coop(fd, /*RUNLOOM_NETPOLL_WRITE*/ 2, -1LL) < 0) {
            PyBuffer_Release(&buf);
            /* wait_fd may have run a Python signal handler that raised (e.g. an
             * alarm handler / Ctrl-C) -> propagate it; don't overwrite with OSError. */
            return PyErr_Occurred() ? NULL : PyErr_SetFromErrno(PyExc_OSError);
        }
    }

    PyBuffer_Release(&buf);
    return PyLong_FromSsize_t(sent);
}

/* Single send.  Equivalent to sock.send(buf, flags) with cooperative
 * blocking on EAGAIN.  Returns bytes sent in one syscall; caller may
 * call again with the unsent tail.
 *
 * Signature: runloom_c.tcp_send_once(fd, bytes_like, flags=0) -> int
 */
static PyObject *m_tcp_send_once(PyObject *self, PyObject *args)
{
    int fd;
    Py_buffer buf;
    int flags = 0;
    Py_ssize_t sent = 0;
    (void)self;

    if (!PyArg_ParseTuple(args, "iy*|i", &fd, &buf, &flags)) return NULL;

    while (1) {
#if defined(RUNLOOM_OS_WINDOWS)
        int r = send((SOCKET)fd, (const char *)buf.buf,
                     (int)buf.len, flags);
        if (r >= 0) { sent = r; break; }
        {
            int err = WSAGetLastError();
            if (err != WSAEWOULDBLOCK) {
                PyBuffer_Release(&buf);
                PyErr_SetFromWindowsErr(err);
                return NULL;
            }
        }
#else
        ssize_t r = send(fd, (const char *)buf.buf, (size_t)buf.len, flags);
        if (r >= 0) { sent = (Py_ssize_t)r; break; }
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
#endif
        if (runloom_netpoll_wait_fd_coop(fd, /*RUNLOOM_NETPOLL_WRITE*/ 2, -1LL) < 0) {
            PyBuffer_Release(&buf);
            /* wait_fd may have run a Python signal handler that raised (e.g. an
             * alarm handler / Ctrl-C) -> propagate it; don't overwrite with OSError. */
            return PyErr_Occurred() ? NULL : PyErr_SetFromErrno(PyExc_OSError);
        }
    }

    PyBuffer_Release(&buf);
    return PyLong_FromSsize_t(sent);
}

/* Cooperative POSIX read(2) / write(2) for non-socket fds (pipes,
 * tty, etc.).  Windows: file fds aren't pollable through Winsock so
 * fd_read/write fall back to a synchronous _read/_write that blocks
 * the OS thread.  This is the same trade-off monkey.py already takes
 * via _blocking_call; exposing it here lets the C scheduler shortcut
 * around the Python frame overhead. */
/* test-only fd_read/fd_write fault injection.  kqueue/Windows have no syscall
 * tracer; Linux could use strace but the compiled-in path is uniform.  Cached
 * so the cooperative read/write loop pays only a load+branch when disarmed. */
