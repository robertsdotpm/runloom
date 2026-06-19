/* Part of module.c -- #included into that translation unit, not compiled
 * standalone.  See module.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

static int runloom_fdio_fault_armed(void)
{
    static int cached = -1;
    if (cached < 0)
        cached = (getenv("RUNLOOM_FAULT_FD_READ") || getenv("RUNLOOM_FAULT_FD_WRITE")) ? 1 : 0;
    return cached;
}
#define RUNLOOM_FDIO_FINJ(site) (runloom_fdio_fault_armed() ? runloom_fault_inject(site) : 0)

#if !defined(RUNLOOM_OS_WINDOWS)
#include <fcntl.h>
/* fd_read/fd_write cooperatively PARK on EAGAIN, which REQUIRES a non-blocking
 * fd: on a BLOCKING fd, read()/write() would block the whole scheduler OS thread
 * instead of returning EAGAIN to park -- a silent scheduler wedge.  The monkey
 * os.read/os.write patches set non-blocking first; for the RAW primitive we
 * ensure it here so a blocking fd can never wedge the scheduler.  (This mutates
 * the fd's O_NONBLOCK flag, exactly as cooperative socket I/O requires.) */
static void runloom_fd_set_nonblocking(int fd)
{
    int fl = fcntl(fd, F_GETFL, 0);
    if (fl >= 0 && !(fl & O_NONBLOCK)) {
        (void)fcntl(fd, F_SETFL, fl | O_NONBLOCK);
    }
}
#endif

static PyObject *m_fd_read(PyObject *self, PyObject *args)
{
    int fd;
    Py_buffer buf;
    Py_ssize_t n_bytes;
    Py_ssize_t got = 0;
    (void)self;

    if (!PyArg_ParseTuple(args, "iw*n", &fd, &buf, &n_bytes)) return NULL;
    if (n_bytes < 0 || n_bytes > buf.len) {
        PyBuffer_Release(&buf);
        PyErr_SetString(PyExc_ValueError, "n out of range for buffer");
        return NULL;
    }

#if defined(RUNLOOM_OS_WINDOWS)
    /* No pollable file/pipe model on Win32 -- block the OS thread.
     * Callers expecting cooperation here should route to monkey.py's
     * thread-pool _blocking_call instead. */
    {
        int r = _read(fd, buf.buf, (unsigned)n_bytes);
        PyBuffer_Release(&buf);
        if (r < 0) return PyErr_SetFromErrno(PyExc_OSError);
        return PyLong_FromLong(r);
    }
#else
    runloom_fd_set_nonblocking(fd);   /* cooperative park needs EAGAIN, not a blocked thread */
    while (1) {
        int injerr = RUNLOOM_FDIO_FINJ(RUNLOOM_FAULT_FD_READ);
        ssize_t r = injerr ? (errno = injerr, (ssize_t)-1)
                           : read(fd, buf.buf, (size_t)n_bytes);
        /* fd read classifies like recv: READY (>0) / EOF (0) / WOULDBLOCK_R /
         * INTR / ERROR.  Observational route through the total I/O FSM. */
        RUNLOOM_IO_NOTE(RUNLOOM_IO_RECV,
                        runloom_io_classify(RUNLOOM_IO_RECV, (long long)r, errno));
        if (r > 0)  { got = (Py_ssize_t)r; break; }
        if (r == 0) { got = 0; break; }   /* EOF */
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
        if (errno == EINTR) continue;
        if (runloom_netpoll_wait_fd(fd, 1 /*READ*/, -1LL) < 0) {
            PyBuffer_Release(&buf);
            /* wait_fd may have run a Python signal handler that raised (e.g. an
             * alarm handler / Ctrl-C) -> propagate it; don't overwrite with OSError. */
            return PyErr_Occurred() ? NULL : PyErr_SetFromErrno(PyExc_OSError);
        }
    }
    PyBuffer_Release(&buf);
    return PyLong_FromSsize_t(got);
#endif
}

static PyObject *m_fd_write(PyObject *self, PyObject *args)
{
    int fd;
    Py_buffer buf;
    Py_ssize_t written = 0;
    (void)self;

    if (!PyArg_ParseTuple(args, "iy*", &fd, &buf)) return NULL;

#if defined(RUNLOOM_OS_WINDOWS)
    {
        int r = _write(fd, buf.buf, (unsigned)buf.len);
        PyBuffer_Release(&buf);
        if (r < 0) return PyErr_SetFromErrno(PyExc_OSError);
        return PyLong_FromLong(r);
    }
#else
    runloom_fd_set_nonblocking(fd);   /* cooperative park needs EAGAIN, not a blocked thread */
    while (written < buf.len) {
        int injerr = RUNLOOM_FDIO_FINJ(RUNLOOM_FAULT_FD_WRITE);
        ssize_t r = injerr ? (errno = injerr, (ssize_t)-1)
                           : write(fd, (const char *)buf.buf + written,
                                   (size_t)(buf.len - written));
        /* fd write classifies like send: PROGRESS (>=0) / WOULDBLOCK_W / INTR /
         * ERROR.  Observational route through the total I/O FSM. */
        RUNLOOM_IO_NOTE(RUNLOOM_IO_SEND,
                        runloom_io_classify(RUNLOOM_IO_SEND, (long long)r, errno));
        if (r >= 0) { written += r; continue; }
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
        if (errno == EINTR) continue;
        if (runloom_netpoll_wait_fd(fd, 2 /*WRITE*/, -1LL) < 0) {
            PyBuffer_Release(&buf);
            /* wait_fd may have run a Python signal handler that raised (e.g. an
             * alarm handler / Ctrl-C) -> propagate it; don't overwrite with OSError. */
            return PyErr_Occurred() ? NULL : PyErr_SetFromErrno(PyExc_OSError);
        }
    }
    PyBuffer_Release(&buf);
    return PyLong_FromSsize_t(written);
#endif
}

/* io_uring file I/O.  On Linux >= 5.1 these submit the read/write
 * to the kernel via io_uring and block (in the kernel) until done.
 * Cheaper than the thread-pool path because no GIL release/reacquire
 * or thread handoff -- one syscall per op.
 *
 * Falls back to a plain pread/pwrite on systems without io_uring. */
#if defined(__linux__)
#  include "io_uring.h"
#endif

static PyObject *m_iouring_available(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
#if defined(__linux__)
    return PyBool_FromLong(runloom_iouring_available());
#else
    Py_RETURN_FALSE;
#endif
}

static PyObject *m_file_read(PyObject *self, PyObject *args)
{
    int fd;
    Py_buffer buf;
    Py_ssize_t n_bytes;
    long long offset = -1;   /* -1 = use current fd offset via pread */
    Py_ssize_t r;
    (void)self;

    if (!PyArg_ParseTuple(args, "iw*n|L", &fd, &buf, &n_bytes, &offset)) {
        return NULL;
    }
    if (n_bytes < 0 || n_bytes > buf.len) {
        PyBuffer_Release(&buf);
        PyErr_SetString(PyExc_ValueError, "n out of range for buffer");
        return NULL;
    }
#if defined(__linux__)
    if (runloom_iouring_available()) {
        off_t off = (offset < 0) ? 0 : (off_t)offset;
        r = runloom_iouring_pread(fd, buf.buf, (size_t)n_bytes, off);
        if (r < 0) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
        PyBuffer_Release(&buf);
        return PyLong_FromSsize_t((Py_ssize_t)r);
    }
#endif
    /* Fallback: plain blocking read.  Caller routed cooperatively
     * via monkey.py's thread-pool _blocking_call if needed. */
#if defined(_WIN32)
    {
        int rr = _read(fd, buf.buf, (unsigned)n_bytes);
        PyBuffer_Release(&buf);
        if (rr < 0) return PyErr_SetFromErrno(PyExc_OSError);
        return PyLong_FromLong(rr);
    }
#else
    r = (offset < 0) ? read(fd, buf.buf, (size_t)n_bytes)
                     : pread(fd, buf.buf, (size_t)n_bytes, (off_t)offset);
    PyBuffer_Release(&buf);
    if (r < 0) return PyErr_SetFromErrno(PyExc_OSError);
    return PyLong_FromSsize_t((Py_ssize_t)r);
#endif
}

static PyObject *m_file_write(PyObject *self, PyObject *args)
{
    int fd;
    Py_buffer buf;
    long long offset = -1;
    Py_ssize_t r;
    (void)self;

    if (!PyArg_ParseTuple(args, "iy*|L", &fd, &buf, &offset)) return NULL;
#if defined(__linux__)
    if (runloom_iouring_available()) {
        off_t off = (offset < 0) ? 0 : (off_t)offset;
        r = runloom_iouring_pwrite(fd, buf.buf, (size_t)buf.len, off);
        if (r < 0) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
        PyBuffer_Release(&buf);
        return PyLong_FromSsize_t((Py_ssize_t)r);
    }
#endif
#if defined(_WIN32)
    {
        int rr = _write(fd, buf.buf, (unsigned)buf.len);
        PyBuffer_Release(&buf);
        if (rr < 0) return PyErr_SetFromErrno(PyExc_OSError);
        return PyLong_FromLong(rr);
    }
#else
    r = (offset < 0) ? write(fd, buf.buf, (size_t)buf.len)
                     : pwrite(fd, buf.buf, (size_t)buf.len, (off_t)offset);
    PyBuffer_Release(&buf);
    if (r < 0) return PyErr_SetFromErrno(PyExc_OSError);
    return PyLong_FromSsize_t((Py_ssize_t)r);
#endif
}

/* ---- Fast-path: C scheduler ----
 *
 * runloom_c.fiber(fn)       -> RunloomG handle.  Schedules fn to run.
 * runloom_c.sched_yield() -> None.  Yields current g via the C scheduler.
 * runloom_c.sched_sleep(s) -> None.  Parks current g on the sleep heap.
 * runloom_c.run()        -> int.  Drives the scheduler until idle.
 *
 * These call directly into sched.c -- no Python-level scheduler.
 * Compared to the original Python runloom.runtime.Scheduler this is a
 * single C call per yield/spawn rather than 3-5 Python frames.
 */

typedef struct {
    PyObject_HEAD
    runloom_g_t *g;
    /* M:N session generation at handle creation (runloom_mn_generation_get()).
     * RunloomG_wake compares it to the live generation: if the hub pool this g
     * parked on has since been torn down, the recorded g->park_hub dangles into
     * freed memory, so the hub-routed wake is skipped.  See module_g.c.inc. */
    uint64_t born_gen;
} RunloomG;

