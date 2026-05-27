/* module.c -- Python bindings for pygo_core.
 *
 * Exposes:
 *   pygo_core.Coro(callable, stack_size=131072) -> coro object
 *      .resume()         switch into the coroutine
 *      .done             True if entry returned
 *   pygo_core.yield_()   yield from inside a coroutine
 *   pygo_core.backend()  "fibers" | "ucontext"
 *
 * Free-threaded friendly: each OS thread runs its own coroutines.
 * We do NOT release the GIL during resume() because the Python callable
 * we run inside the coro will reacquire/release as it pleases.  Under
 * free-threaded Python (3.13t) there is no global lock to release.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#if PY_VERSION_HEX < 0x030B0000
#  error "pygo requires CPython 3.11 or later -- the Phase B per-g \
PyThreadState snapshot uses 3.11+ tstate fields (cframe, \
datastack_chunk).  3.10 and earlier had a fundamentally different \
frame model (PyFrameObject linked list) and would need separate \
snap/load paths; not built today."
#endif

#include "plat.h"
#include "plat_compat.h"
#include "coro.h"
#include "pygo_sched.h"
#include "netpoll.h"
#include "mn_sched.h"
#include "chan.h"

/* ---- Per-coro Python object ---- */

/* CPython thread-state snapshot.  These fields are not preserved by a
 * raw C-stack swap, but Python frame chain + recursion counters live
 * on the thread state and need to follow the coroutine.  We snapshot
 * what we can portably:
 *  - py_recursion_remaining / c_recursion_remaining (3.12+)
 *  - recursion_depth (older 3.x)
 * Other fields like the topmost frame chain are still UB territory and
 * are why we don't run unittest harness frames over yields. */
typedef struct {
#if PY_VERSION_HEX >= 0x030C0000
    int py_recursion_remaining;
    int c_recursion_remaining;
#elif PY_VERSION_HEX >= 0x030B0000
    int recursion_remaining;
#else
    int recursion_depth;
#endif
    int initialised;
} PygoTstateSnapshot;

typedef struct {
    PyObject_HEAD
    pygo_coro_t *coro;
    PyObject *callable;   /* invoked once when the coro first resumes */
    PyObject *result;     /* return value of callable, or NULL */
    PyObject *error;      /* unhandled exception caught, or NULL */
    int has_run;
    PygoTstateSnapshot tstate_snap;  /* captured at yield, restored at resume */
} PygoCoro;

PYGO_INLINE void pygo_tstate_save(PygoTstateSnapshot *s)
{
    PyThreadState *ts = PyThreadState_GET();
#if PY_VERSION_HEX >= 0x030C0000
    s->py_recursion_remaining = ts->py_recursion_remaining;
    s->c_recursion_remaining = ts->c_recursion_remaining;
#elif PY_VERSION_HEX >= 0x030B0000
    s->recursion_remaining = ts->recursion_remaining;
#else
    s->recursion_depth = ts->recursion_depth;
#endif
    s->initialised = 1;
}

PYGO_INLINE void pygo_tstate_restore(const PygoTstateSnapshot *s)
{
    PyThreadState *ts;
    if (!s->initialised) {
        return;
    }
    ts = PyThreadState_GET();
#if PY_VERSION_HEX >= 0x030C0000
    ts->py_recursion_remaining = s->py_recursion_remaining;
    ts->c_recursion_remaining = s->c_recursion_remaining;
#elif PY_VERSION_HEX >= 0x030B0000
    ts->recursion_remaining = s->recursion_remaining;
#else
    ts->recursion_depth = s->recursion_depth;
#endif
}

/* Entry function: runs inside the coroutine stack.  Note: we are
 * already executing Python C code on the OS thread that owns the GIL
 * (on GIL-builds), so we can call Py_* freely.  Under free-threaded
 * builds, the calling thread state is still valid here. */
static void pygo_coro_python_entry(void *user)
{
    PygoCoro *self = (PygoCoro *)user;
    PyObject *res;
    res = PyObject_CallNoArgs(self->callable);
    if (res == NULL) {
        /* The exception is set; capture and clear so the resumer sees it. */
        PyObject *type, *value, *tb;
        PyErr_Fetch(&type, &value, &tb);
        PyErr_NormalizeException(&type, &value, &tb);
        if (value == NULL) {
            value = Py_None; Py_INCREF(value);
        }
        if (tb != NULL) {
            PyException_SetTraceback(value, tb);
            Py_DECREF(tb);
        }
        Py_XDECREF(type);
        self->error = value;
    } else {
        self->result = res;
    }
}

static int PygoCoro_init(PygoCoro *self, PyObject *args, PyObject *kw)
{
    static char *kwlist[] = {"callable", "stack_size", NULL};
    PyObject *callable;
    Py_ssize_t stack_size = 131072; /* 128 KB default */
    if (!PyArg_ParseTupleAndKeywords(args, kw, "O|n", kwlist,
                                     &callable, &stack_size)) {
        return -1;
    }
    if (!PyCallable_Check(callable)) {
        PyErr_SetString(PyExc_TypeError, "callable must be callable");
        return -1;
    }
    Py_INCREF(callable);
    self->callable = callable;
    self->result = NULL;
    self->error = NULL;
    self->has_run = 0;
    self->coro = pygo_coro_new((size_t)stack_size,
                               pygo_coro_python_entry,
                               self);
    if (self->coro == NULL) {
        PyErr_SetString(PyExc_MemoryError, "pygo_coro_new failed");
        return -1;
    }
    return 0;
}

static void PygoCoro_dealloc(PygoCoro *self)
{
    if (self->coro != NULL) {
        pygo_coro_destroy(self->coro);
        self->coro = NULL;
    }
    Py_XDECREF(self->callable);
    Py_XDECREF(self->result);
    Py_XDECREF(self->error);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *PygoCoro_resume(PygoCoro *self, PyObject *unused)
{
    PygoTstateSnapshot caller_snap;
    (void)unused;
    if (self->coro == NULL || pygo_coro_done(self->coro)) {
        if (self->error != NULL) {
            PyObject *err = self->error;
            self->error = NULL;
            PyErr_SetObject((PyObject *)Py_TYPE(err), err);
            Py_DECREF(err);
            return NULL;
        }
        Py_INCREF(Py_None);
        return Py_None;
    }
    self->has_run = 1;

    /* Save the caller's (scheduler's) thread-state recursion counters,
     * then restore the coro's snapshot if it has one.  After
     * swapcontext returns (the coro yielded or finished), save its
     * counters into our snapshot and restore the caller's.  Without
     * this, each yield permanently decrements py_recursion_remaining
     * on the OS thread, so a sufficiently-long pygo.run() hits
     * RecursionError. */
    pygo_tstate_save(&caller_snap);
    if (self->tstate_snap.initialised) {
        pygo_tstate_restore(&self->tstate_snap);
    }

    pygo_coro_resume(self->coro);

    /* Snapshot the coro's tstate at the yield/return point, then
     * restore the caller's counters. */
    pygo_tstate_save(&self->tstate_snap);
    pygo_tstate_restore(&caller_snap);

    if (pygo_coro_done(self->coro) && self->error != NULL) {
        PyObject *err = self->error;
        self->error = NULL;
        PyErr_SetObject((PyObject *)Py_TYPE(err), err);
        Py_DECREF(err);
        return NULL;
    }
    if (pygo_coro_done(self->coro) && self->result != NULL) {
        Py_INCREF(self->result);
        return self->result;
    }
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *PygoCoro_done_get(PygoCoro *self, void *closure)
{
    (void)closure;
    if (self->coro == NULL || pygo_coro_done(self->coro)) {
        Py_RETURN_TRUE;
    }
    Py_RETURN_FALSE;
}

static PyObject *PygoCoro_result_get(PygoCoro *self, void *closure)
{
    (void)closure;
    if (self->result == NULL) {
        Py_RETURN_NONE;
    }
    Py_INCREF(self->result);
    return self->result;
}

static PyMethodDef PygoCoro_methods[] = {
    {"resume", (PyCFunction)PygoCoro_resume, METH_NOARGS,
     "Resume the coroutine.  Returns when it yields or returns."},
    {NULL, NULL, 0, NULL}
};

static PyGetSetDef PygoCoro_getset[] = {
    {"done",   (getter)PygoCoro_done_get,   NULL, "True after entry returns", NULL},
    {"result", (getter)PygoCoro_result_get, NULL, "Return value, if any",     NULL},
    {NULL, NULL, NULL, NULL, NULL}
};

static PyTypeObject PygoCoroType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "pygo_core.Coro",          /* tp_name */
    sizeof(PygoCoro),          /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)PygoCoro_dealloc, /* tp_dealloc */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    Py_TPFLAGS_DEFAULT,        /* tp_flags */
    "Stackful coroutine.",     /* tp_doc */
    0, 0, 0, 0, 0, 0,
    PygoCoro_methods,          /* tp_methods */
    0,                         /* tp_members */
    PygoCoro_getset,           /* tp_getset */
    0, 0, 0, 0, 0,
    (initproc)PygoCoro_init,   /* tp_init */
    0,
    PyType_GenericNew,         /* tp_new */
};

/* ---- Module-level functions ---- */

static PyObject *m_yield(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    /* yield is just "switch back to caller".  We hold the GIL across the
     * switch on GIL-builds because the caller will resume us on the same
     * OS thread; under free-threaded Python the thread state stays
     * attached.  This is safe because we never migrate a coroutine
     * between OS threads (each thread has its own scheduler). */
    pygo_coro_yield();
    Py_RETURN_NONE;
}

static PyObject *m_backend(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    return PyUnicode_FromString(pygo_coro_backend());
}

static PyObject *m_thread_init(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    if (pygo_coro_thread_init() != 0) {
        PyErr_SetString(PyExc_OSError, "pygo_coro_thread_init failed");
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *m_thread_fini(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    pygo_coro_thread_fini();
    Py_RETURN_NONE;
}

static PyObject *m_warmup(PyObject *self, PyObject *args)
{
    int n;
    Py_ssize_t stack_size = 131072;
    int actual;
    (void)self;
    if (!PyArg_ParseTuple(args, "i|n", &n, &stack_size)) return NULL;
    actual = pygo_coro_warmup((size_t)stack_size, n);
    return PyLong_FromLong(actual);
}

/* Native C TCP recv into a writable buffer.  Equivalent to
 *   sock.recv_into(buf)  with cooperative blocking, but bypasses
 *   the Python socket.recv_into method dispatch (saves ~3-5 us per
 *   call on tight echo loops).
 *
 * Signature: pygo_core.tcp_recv(fd: int, buf: bytearray-like, n: int) -> int
 *   Returns the number of bytes received; 0 = orderly shutdown.
 *
 * Implementation: loop recv(fd, buf, n, 0); on EAGAIN/EWOULDBLOCK
 * park on the netpoll, retry.  The `buf` argument must be a
 * writable buffer (bytearray, memoryview, ...) and pygo will fill
 * its first n bytes. */
#if defined(PYGO_OS_WINDOWS)
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
    Py_ssize_t got = 0;
    (void)self;

    if (!PyArg_ParseTuple(args, "iw*n", &fd, &buf, &n_bytes)) return NULL;
    if (n_bytes > buf.len) n_bytes = buf.len;
    if (n_bytes <= 0) {
        PyBuffer_Release(&buf);
        return PyLong_FromLong(0);
    }

    while (1) {
#if defined(PYGO_OS_WINDOWS)
        int r = recv((SOCKET)fd, (char *)buf.buf, (int)n_bytes, 0);
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
        ssize_t r = recv(fd, (char *)buf.buf, (size_t)n_bytes, 0);
        if (r > 0) { got = (Py_ssize_t)r; break; }
        if (r == 0) { got = 0; break; }
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
#endif
        /* Park on read. */
        if (pygo_netpoll_wait_fd(fd, /*PYGO_NETPOLL_READ*/ 1, -1LL) < 0) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
    }

    PyBuffer_Release(&buf);
    return PyLong_FromSsize_t(got);
}

/* Native C TCP send.  Equivalent to sock.sendall(buf) with
 * cooperative blocking.  Loops until all bytes sent or error. */
static PyObject *m_tcp_send(PyObject *self, PyObject *args)
{
    int fd;
    Py_buffer buf;
    Py_ssize_t sent = 0;
    (void)self;

    if (!PyArg_ParseTuple(args, "iy*", &fd, &buf)) return NULL;

    while (sent < buf.len) {
#if defined(PYGO_OS_WINDOWS)
        int r = send((SOCKET)fd, (const char *)buf.buf + sent,
                     (int)(buf.len - sent), 0);
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
                         (size_t)(buf.len - sent), 0);
        if (r >= 0) { sent += r; continue; }
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
#endif
        if (pygo_netpoll_wait_fd(fd, /*PYGO_NETPOLL_WRITE*/ 2, -1LL) < 0) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
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

#if defined(PYGO_OS_WINDOWS)
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
    while (1) {
        ssize_t r = read(fd, buf.buf, (size_t)n_bytes);
        if (r > 0)  { got = (Py_ssize_t)r; break; }
        if (r == 0) { got = 0; break; }   /* EOF */
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
        if (errno == EINTR) continue;
        if (pygo_netpoll_wait_fd(fd, 1 /*READ*/, -1LL) < 0) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
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

#if defined(PYGO_OS_WINDOWS)
    {
        int r = _write(fd, buf.buf, (unsigned)buf.len);
        PyBuffer_Release(&buf);
        if (r < 0) return PyErr_SetFromErrno(PyExc_OSError);
        return PyLong_FromLong(r);
    }
#else
    while (written < buf.len) {
        ssize_t r = write(fd, (const char *)buf.buf + written,
                          (size_t)(buf.len - written));
        if (r >= 0) { written += r; continue; }
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
        }
        if (errno == EINTR) continue;
        if (pygo_netpoll_wait_fd(fd, 2 /*WRITE*/, -1LL) < 0) {
            PyBuffer_Release(&buf);
            return PyErr_SetFromErrno(PyExc_OSError);
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
    return PyBool_FromLong(pygo_iouring_available());
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
    if (pygo_iouring_available()) {
        off_t off = (offset < 0) ? 0 : (off_t)offset;
        r = pygo_iouring_pread(fd, buf.buf, (size_t)n_bytes, off);
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
    if (pygo_iouring_available()) {
        off_t off = (offset < 0) ? 0 : (off_t)offset;
        r = pygo_iouring_pwrite(fd, buf.buf, (size_t)buf.len, off);
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
 * pygo_core.go(fn)       -> PygoG handle.  Schedules fn to run.
 * pygo_core.sched_yield() -> None.  Yields current g via the C scheduler.
 * pygo_core.sched_sleep(s) -> None.  Parks current g on the sleep heap.
 * pygo_core.run()        -> int.  Drives the scheduler until idle.
 *
 * These call directly into sched.c -- no Python-level scheduler.
 * Compared to the original Python pygo.runtime.Scheduler this is a
 * single C call per yield/spawn rather than 3-5 Python frames.
 */

typedef struct {
    PyObject_HEAD
    pygo_g_t *g;
} PygoG;

static void PygoG_dealloc(PygoG *self)
{
    if (self->g != NULL) {
        pygo_g_decref(self->g);
        self->g = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *PygoG_done_get(PygoG *self, void *closure)
{
    (void)closure;
    if (self->g == NULL) Py_RETURN_TRUE;
    /* ACQUIRE pairs with the RELEASE store in pygo_g_entry; ensures
     * that if done is true, we also see the matching result/error
     * stores even on free-threaded 3.13t reading from another thread. */
    if (__atomic_load_n(&self->g->done, __ATOMIC_ACQUIRE)) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static PyObject *PygoG_result_get(PygoG *self, void *closure)
{
    PyObject *r;
    (void)closure;
    if (self->g == NULL) Py_RETURN_NONE;
    /* Same acquire pattern as PygoG_done_get -- gate the result read
     * behind the done flag's release ordering. */
    if (!__atomic_load_n(&self->g->done, __ATOMIC_ACQUIRE)) Py_RETURN_NONE;
    r = self->g->result;
    if (r == NULL) Py_RETURN_NONE;
    Py_INCREF(r);
    return r;
}

/* Wake a goroutine that's parked via pygo_core.park_self().  Safe to
 * call before the park (race-handled inside pygo_sched_wake_safe). */
static PyObject *PygoG_wake(PygoG *self, PyObject *unused)
{
    (void)unused;
    if (self->g != NULL) {
        pygo_sched_wake_safe(self->g);
    }
    Py_RETURN_NONE;
}

/* Return a small introspection dict for a parked goroutine:
 *   {"state": "done"|"running"|"parked"|"fresh",
 *    "has_snap": bool}
 *
 * Originally this returned a full Python frame walk via the saved
 * snap.  The internal _PyInterpreterFrame layout changes across
 * patch releases (3.11/3.12/3.13 all differ), so walking it from a
 * stable C extension would require pinning to the internal-API build
 * flag -- something we deliberately avoid for portability.  This
 * minimal version still lets a watchdog goroutine answer "what state
 * is task X in?" without paying the internal-header dependency. */
static PyObject *PygoG_stack(PygoG *self, PyObject *unused)
{
    PyObject *d;
    const char *state;
    int has_snap;
    (void)unused;

    if (self->g == NULL) {
        state = "freed";
        has_snap = 0;
    } else if (self->g->done) {
        state = "done";
        has_snap = 0;
    } else if (self->g->snap.valid) {
        state = (pygo_sched_get()->current == self->g) ? "running" : "parked";
        has_snap = 1;
    } else {
        state = "fresh";
        has_snap = 0;
    }

    d = PyDict_New();
    if (d == NULL) return NULL;
    if (PyDict_SetItemString(d, "state", PyUnicode_FromString(state)) < 0 ||
        PyDict_SetItemString(d, "has_snap", PyBool_FromLong(has_snap)) < 0) {
        Py_DECREF(d);
        return NULL;
    }
    return d;
}

static PyMethodDef PygoG_methods[] = {
    {"wake", (PyCFunction)PygoG_wake, METH_NOARGS,
     "Wake this goroutine if parked via park_self().  Safe to call "
     "before park_self (race-handled)."},
    {"stack", (PyCFunction)PygoG_stack, METH_NOARGS,
     "Return a small introspection dict: "
     "{'state': 'done'|'running'|'parked'|'fresh', 'has_snap': bool}.  "
     "Watchdog-safe; cheap to poll for 'where is task X stuck?'."},
    {NULL, NULL, 0, NULL},
};

static PyGetSetDef PygoG_getset[] = {
    {"done",   (getter)PygoG_done_get,   NULL, "True after entry returns", NULL},
    {"result", (getter)PygoG_result_get, NULL, "Return value or None",     NULL},
    {NULL, NULL, NULL, NULL, NULL}
};

static PyTypeObject PygoGType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "pygo_core.G",
    sizeof(PygoG),
    0,
    (destructor)PygoG_dealloc,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    Py_TPFLAGS_DEFAULT,
    "Goroutine handle.",
    0, 0, 0, 0, 0, 0,
    PygoG_methods,
    0,
    PygoG_getset,
    0, 0, 0, 0, 0,
    0, 0,
    PyType_GenericNew,
};

/* ============================================================ */
/* PygoChan -- Go-style channel                                 */
/* ============================================================ */
typedef struct {
    PyObject_HEAD
    pygo_chan_t *ch;
} PygoChan;

static int PygoChan_init(PygoChan *self, PyObject *args, PyObject *kw)
{
    static char *kwlist[] = {"capacity", NULL};
    Py_ssize_t cap = 0;
    if (!PyArg_ParseTupleAndKeywords(args, kw, "|n", kwlist, &cap)) {
        return -1;
    }
    self->ch = pygo_chan_new(cap);
    if (self->ch == NULL) return -1;
    return 0;
}

static void PygoChan_dealloc(PygoChan *self)
{
    if (self->ch != NULL) {
        pygo_chan_decref(self->ch);
        self->ch = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *PygoChan_send(PygoChan *self, PyObject *value)
{
    if (pygo_chan_send(self->ch, value) < 0) return NULL;
    Py_RETURN_NONE;
}

static PyObject *PygoChan_try_send(PygoChan *self, PyObject *value)
{
    int r = pygo_chan_try_send(self->ch, value);
    if (r < 0) return NULL;
    if (r == 0) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static PyObject *PygoChan_recv(PygoChan *self, PyObject *unused)
{
    int ok;
    PyObject *v;
    (void)unused;
    v = pygo_chan_recv(self->ch, &ok);
    if (v == NULL) return NULL;
    /* Return (value, ok) matching Go's `v, ok := <-ch` idiom. */
    {
        PyObject *tup = PyTuple_New(2);
        if (tup == NULL) { Py_DECREF(v); return NULL; }
        PyTuple_SET_ITEM(tup, 0, v);                     /* steals ref */
        PyTuple_SET_ITEM(tup, 1, PyBool_FromLong(ok));
        return tup;
    }
}

static PyObject *PygoChan_try_recv(PygoChan *self, PyObject *unused)
{
    int ok;
    PyObject *v;
    (void)unused;
    if (pygo_chan_try_recv(self->ch, &v, &ok) < 0) return NULL;
    if (v == NULL) {
        /* Would-block. */
        Py_RETURN_NONE;
    }
    {
        PyObject *tup = PyTuple_New(2);
        if (tup == NULL) { Py_DECREF(v); return NULL; }
        PyTuple_SET_ITEM(tup, 0, v);
        PyTuple_SET_ITEM(tup, 1, PyBool_FromLong(ok));
        return tup;
    }
}

static PyObject *PygoChan_close(PygoChan *self, PyObject *unused)
{
    (void)unused;
    if (pygo_chan_close(self->ch) < 0) return NULL;
    Py_RETURN_NONE;
}

/* Iterator protocol: `for v in ch:` calls recv() repeatedly and stops
 * on close.  Matches Go's `for v := range ch { ... }`. */
static PyObject *PygoChan_iter(PygoChan *self)
{
    Py_INCREF(self);
    return (PyObject *)self;
}

static PyObject *PygoChan_iternext(PygoChan *self)
{
    int ok;
    PyObject *v = pygo_chan_recv(self->ch, &ok);
    if (v == NULL) return NULL;            /* error */
    if (!ok) {
        /* Channel closed and empty -> end iteration. */
        Py_DECREF(v);                       /* v was Py_None */
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }
    return v;                               /* new ref to caller */
}

static PyObject *PygoChan_closed_get(PygoChan *self, void *closure)
{
    (void)closure;
    if (pygo_chan_is_closed(self->ch)) Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static PyObject *PygoChan_cap_get(PygoChan *self, void *closure)
{
    (void)closure;
    return PyLong_FromSsize_t(pygo_chan_cap(self->ch));
}

static Py_ssize_t PygoChan_len(PygoChan *self)
{
    return pygo_chan_len(self->ch);
}

static PyMethodDef PygoChan_methods[] = {
    {"send", (PyCFunction)PygoChan_send, METH_O,
     "Send a value, blocking until delivered."},
    {"try_send", (PyCFunction)PygoChan_try_send, METH_O,
     "Try to send.  Returns True if delivered, False if would-block."},
    {"recv", (PyCFunction)PygoChan_recv, METH_NOARGS,
     "Receive (value, ok); blocks until a value or close.  ok=False "
     "means the channel was closed and empty (Go's `v, ok := <-ch`)."},
    {"try_recv", (PyCFunction)PygoChan_try_recv, METH_NOARGS,
     "Try to receive.  Returns (value, ok) on success, None if "
     "would-block."},
    {"close", (PyCFunction)PygoChan_close, METH_NOARGS,
     "Mark the channel closed.  Wakes all parked senders + receivers."},
    {NULL, NULL, 0, NULL}
};

static PyGetSetDef PygoChan_getset[] = {
    {"closed",   (getter)PygoChan_closed_get,   NULL, "True after close()", NULL},
    {"capacity", (getter)PygoChan_cap_get,      NULL, "buffered capacity", NULL},
    {NULL, NULL, NULL, NULL, NULL}
};

static PySequenceMethods PygoChan_seq = {
    (lenfunc)PygoChan_len,            /* sq_length */
    0, 0, 0, 0, 0, 0, 0, 0, 0,
};

static PyTypeObject PygoChanType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "pygo_core.Chan",
    .tp_basicsize = sizeof(PygoChan),
    .tp_dealloc = (destructor)PygoChan_dealloc,
    .tp_as_sequence = &PygoChan_seq,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Go-style channel.  Chan(capacity=0).",
    .tp_iter = (getiterfunc)PygoChan_iter,
    .tp_iternext = (iternextfunc)PygoChan_iternext,
    .tp_methods = PygoChan_methods,
    .tp_getset = PygoChan_getset,
    .tp_init = (initproc)PygoChan_init,
    .tp_new = PyType_GenericNew,
};

static PyObject *m_go(PyObject *self, PyObject *callable)
{
    pygo_sched_t *s;
    PygoG *handle;
    pygo_g_t *g;
    PyObject *cap;
    (void)self;
    if (!PyCallable_Check(callable)) {
        PyErr_SetString(PyExc_TypeError, "go(): callable required");
        return NULL;
    }
    s = pygo_sched_get();
    cap = pygo_sched_spawn(s, callable);
    if (cap == NULL) return NULL;
    g = (pygo_g_t *)PyCapsule_GetPointer(cap, "pygo_g");
    Py_DECREF(cap);
    if (g == NULL) return NULL;

    handle = PyObject_New(PygoG, &PygoGType);
    if (handle == NULL) {
        return NULL;
    }
    pygo_g_incref(g);   /* second ref for the Python wrapper */
    handle->g = g;
    return (PyObject *)handle;
}

static PyObject *m_go_noyield(PyObject *self, PyObject *callable)
{
    pygo_sched_t *s;
    PygoG *handle;
    pygo_g_t *g;
    PyObject *cap;
    (void)self;
    if (!PyCallable_Check(callable)) {
        PyErr_SetString(PyExc_TypeError, "go_noyield(): callable required");
        return NULL;
    }
    s = pygo_sched_get();
    cap = pygo_sched_spawn_noyield(s, callable);
    if (cap == NULL) return NULL;
    g = (pygo_g_t *)PyCapsule_GetPointer(cap, "pygo_g");
    Py_DECREF(cap);
    if (g == NULL) return NULL;

    handle = PyObject_New(PygoG, &PygoGType);
    if (handle == NULL) return NULL;
    pygo_g_incref(g);
    handle->g = g;
    return (PyObject *)handle;
}

static PyObject *m_sched_yield(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    pygo_sched_yield(pygo_sched_get());
    Py_RETURN_NONE;
}

/* Signal the C scheduler to exit its drain loop at the next safe
 * point.  Used by pygo.aio.PygoEventLoop.run_until_complete to bail
 * out when the user-visible future is done, even if there are still
 * background goroutines parked on I/O (accept loops, etc). */
static PyObject *m_sched_stop(PyObject *self, PyObject *unused)
{
    pygo_sched_t *s = pygo_sched_get();
    (void)self; (void)unused;
    s->stopping = 1;
    Py_RETURN_NONE;
}

/* Forcibly drop everything from the scheduler: clears the ready queue,
 * sleep heap, and (best-effort) any netpoll-parked goroutines.  Used
 * by paio.run's cleanup after the main future completes, so leftover
 * call_later runners / accept loops / ticker goroutines don't block
 * the next pygo_core.run() with a sleep heap that won't drain for
 * minutes.  Goroutines being dropped have their coros marked done +
 * freed; any user-visible Python references (G handles) will report
 * done=True. */
static PyObject *m_sched_reset(PyObject *self, PyObject *unused)
{
    pygo_sched_t *s = pygo_sched_get();
    int n_ready = 0, n_sleep = 0, n_parked;
    (void)self; (void)unused;

    /* Wake netpoll-parked goroutines with ready_mask=-1 first.  This
     * pushes them back to ready; we then drain ready below. */
    n_parked = pygo_netpoll_drain_parked();

    /* Drain ready queue. */
    while (!pygo_sched_ready_empty(s)) {
        pygo_g_t *g = pygo_sched_ready_pop(s);
        if (g != NULL) {
            __atomic_store_n(&g->done, 1, __ATOMIC_RELEASE);
            pygo_g_decref(g);
            n_ready++;
        }
    }
    /* Drain sleep heap. */
    while (s->sleep_size > 0) {
        pygo_g_t *g = pygo_sched_sleep_pop(s);
        if (g != NULL) {
            __atomic_store_n(&g->done, 1, __ATOMIC_RELEASE);
            pygo_g_decref(g);
            n_sleep++;
        }
    }
    return Py_BuildValue("(iii)", n_ready, n_sleep, n_parked);
}

/* Park the current goroutine until G.wake() is called on it.
 * Race-safe: a wake that arrives before the park (sync callback firing
 * from add_done_callback) makes this a no-op.  Used by pygo.aio's
 * PygoTask to replace the per-task Chan(1) wake mechanism. */
static PyObject *m_park_self(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    pygo_sched_park_safe();
    Py_RETURN_NONE;
}

/* Return a G handle to the currently-running goroutine, or None if
 * called outside any goroutine.  Used by pygo.aio's PygoTask driver
 * to capture its own handle for wake-from-callback. */
static PyObject *m_current_g(PyObject *self, PyObject *unused)
{
    pygo_sched_t *s = pygo_sched_get();
    pygo_g_t *g;
    PygoG *handle;
    (void)self; (void)unused;

    g = s->current;
    if (g == NULL) {
        Py_RETURN_NONE;
    }
    handle = PyObject_New(PygoG, &PygoGType);
    if (handle == NULL) return NULL;
    pygo_g_incref(g);
    handle->g = g;
    return (PyObject *)handle;
}

/* Vectorcall fast-dispatch version of sched_yield.
 *
 * METH_NOARGS for a module-level function in CPython 3.12+ specializes
 * the bytecode CALL to CALL_BUILTIN_O which is fast, but goes through
 * PyCFunction_NewEx / etc. layers.  A type with tp_vectorcall_offset
 * exposes its instances' vectorcallfunc directly, so the interpreter
 * can branch straight to the C function pointer with zero argument-
 * unpacking overhead.
 *
 * We expose a singleton instance as `pygo_core.sched_yield_fast` (and
 * also assign it to `pygo_core.sched_yield` so existing call sites
 * pick it up transparently).  The METH_NOARGS PyCFunction stays
 * available under `sched_yield_classic` for benchmarking comparisons. */
typedef struct {
    PyObject_HEAD
    vectorcallfunc vectorcall;
} PygoYielder;

static PyObject *yielder_vectorcall(PyObject *self,
                                    PyObject *const *args,
                                    size_t nargsf,
                                    PyObject *kwnames)
{
    (void)self;
    (void)args;
    if (PyVectorcall_NARGS(nargsf) != 0 || kwnames != NULL) {
        PyErr_SetString(PyExc_TypeError, "yielder takes no arguments");
        return NULL;
    }
    pygo_sched_yield(pygo_sched_get());
    Py_RETURN_NONE;
}

static PyTypeObject PygoYielderType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "pygo_core._Yielder",
    .tp_basicsize = sizeof(PygoYielder),
    .tp_vectorcall_offset = offsetof(PygoYielder, vectorcall),
    .tp_call = PyVectorcall_Call,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_VECTORCALL,
    .tp_doc = "Singleton wrapper exposing sched_yield via vectorcall.",
};


static PyObject *m_sched_sleep(PyObject *self, PyObject *arg)
{
    double secs;
    pygo_sched_t *s;
    (void)self;
    secs = PyFloat_AsDouble(arg);
    if (secs == -1.0 && PyErr_Occurred()) {
        return NULL;
    }
    s = pygo_sched_get();
    {
        double now = pygo_monotonic_seconds_compat();
        pygo_sched_sleep_until(s, now + secs);
    }
    Py_RETURN_NONE;
}

static PyObject *m_run(PyObject *self, PyObject *unused)
{
    Py_ssize_t completed;
    (void)self; (void)unused;
    completed = pygo_sched_drain(pygo_sched_get());
    return PyLong_FromSsize_t(completed);
}

static PyObject *m_wait_fd(PyObject *self, PyObject *args)
{
    int fd, events;
    long timeout_ms = -1;
    int result;
    (void)self;
    if (!PyArg_ParseTuple(args, "ii|l", &fd, &events, &timeout_ms)) {
        return NULL;
    }
    {
        long long timeout_ns = timeout_ms < 0 ? -1LL :
                               (long long)timeout_ms * 1000000LL;
        result = pygo_netpoll_wait_fd(fd, events, timeout_ns);
    }
    if (result < 0) {
        return PyErr_SetFromErrno(PyExc_OSError);
    }
    return PyLong_FromLong((long)result);
}

static PyObject *m_netpoll_backend(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    return PyUnicode_FromString(pygo_netpoll_backend());
}

/* Production introspection: returns a dict of scheduler counters so a
 * stuck deployment can be debugged without attaching a C debugger.
 * Counts cover the single-thread scheduler; M:N hub stats arrive when
 * Phase C grows its own introspection hooks. */
static PyObject *m_stats(PyObject *self, PyObject *unused)
{
    pygo_sched_t *s = pygo_sched_get();
    Py_ssize_t ready;
    PyObject *d;
    (void)self; (void)unused;

    ready = (Py_ssize_t)((s->ready_tail - s->ready_head) & s->ready_mask);
    d = PyDict_New();
    if (d == NULL) return NULL;

#define PYGO_STATS_SET(k, v) do {                                      \
        PyObject *pv = PyLong_FromSsize_t((Py_ssize_t)(v));            \
        if (pv == NULL || PyDict_SetItemString(d, (k), pv) < 0) {      \
            Py_XDECREF(pv); Py_DECREF(d); return NULL;                 \
        }                                                              \
        Py_DECREF(pv);                                                 \
    } while (0)

    PYGO_STATS_SET("ready",     ready);
    PYGO_STATS_SET("sleeping",  s->sleep_size);
    PYGO_STATS_SET("netpoll_parked", pygo_netpoll_parked_count());
    PYGO_STATS_SET("completed", s->completed);
    PYGO_STATS_SET("running",   (s->current != NULL) ? 1 : 0);
    PYGO_STATS_SET("stack_size_default", s->stack_size);
    PYGO_STATS_SET("ready_capacity", (Py_ssize_t)s->ready_cap);

#undef PYGO_STATS_SET
    /* Strings: backends are useful in the same payload. */
    {
        PyObject *coro    = PyUnicode_FromString(pygo_coro_backend());
        PyObject *netpoll = PyUnicode_FromString(pygo_netpoll_backend());
        if (!coro || !netpoll ||
            PyDict_SetItemString(d, "backend", coro) < 0 ||
            PyDict_SetItemString(d, "netpoll", netpoll) < 0) {
            Py_XDECREF(coro); Py_XDECREF(netpoll); Py_DECREF(d);
            return NULL;
        }
        Py_DECREF(coro); Py_DECREF(netpoll);
    }
    return d;
}

/* ---- M:N scheduler bindings (Phase C) ---- */
static PyObject *m_mn_init(PyObject *self, PyObject *args)
{
    int n = 0;
    (void)self;
    if (!PyArg_ParseTuple(args, "|i", &n)) return NULL;
    {
        int got = pygo_mn_init(n);
        if (got < 0) return NULL;
        return PyLong_FromLong(got);
    }
}

static PyObject *m_mn_go(PyObject *self, PyObject *callable)
{
    (void)self;
    if (!PyCallable_Check(callable)) {
        PyErr_SetString(PyExc_TypeError, "mn_go(): callable required");
        return NULL;
    }
    return pygo_mn_go(callable);
}

static PyObject *m_mn_run(PyObject *self, PyObject *unused)
{
    Py_ssize_t completed;
    (void)self; (void)unused;
    completed = pygo_mn_run();
    return PyLong_FromSsize_t(completed);
}

static PyObject *m_mn_fini(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    pygo_mn_fini();
    Py_RETURN_NONE;
}

/* Preemption: 3.13t only.  Refuse on other versions with a clear error
 * rather than silently failing -- the timer + Py_AddPendingCall path
 * has only been validated on free-threaded 3.13, and the M:N hub model
 * that needs preemption most only makes sense there. */
static PyObject *m_preempt_init(PyObject *self, PyObject *args)
{
    long quantum_us = 10000;
    (void)self;
    if (!PyArg_ParseTuple(args, "|l", &quantum_us)) return NULL;
#if !(defined(Py_GIL_DISABLED) && PY_VERSION_HEX >= 0x030D0000)
    PyErr_SetString(PyExc_RuntimeError,
                    "preempt_init: free-threaded Python 3.13t only "
                    "(GIL builds and pre-3.13 not supported yet)");
    return NULL;
#else
    if (pygo_preempt_init(quantum_us) < 0) return NULL;
    Py_RETURN_NONE;
#endif
}

static PyObject *m_preempt_fini(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    pygo_preempt_fini();
    Py_RETURN_NONE;
}

/* ============================================================ */
/* select() -- multi-channel wait                               */
/* ============================================================ */

/* Python API:
 *
 *   r = pygo_core.select([
 *       ("recv", ch1),
 *       ("send", ch2, value),
 *   ], default=False)
 *
 * Returns:
 *   - If default=True and no case is ready: -1
 *   - Otherwise: (index, recv_value_or_None) tuple
 *     - For SEND cases, recv_value_or_None is None
 *     - For RECV cases, it's (value, ok) like ch.recv()
 */
static PyObject *m_select(PyObject *self, PyObject *args, PyObject *kw)
{
    static char *kwlist[] = {"cases", "default", NULL};
    PyObject *cases_list = NULL;
    int default_flag = 0;
    Py_ssize_t n_cases, i;
    pygo_select_case_t *cs = NULL;
    int fired;
    PyObject *result = NULL;
    (void)self;

    if (!PyArg_ParseTupleAndKeywords(args, kw, "O|p", kwlist,
                                     &cases_list, &default_flag)) {
        return NULL;
    }
    if (!PyList_Check(cases_list) && !PyTuple_Check(cases_list)) {
        PyErr_SetString(PyExc_TypeError, "select cases must be a list/tuple");
        return NULL;
    }
    n_cases = PySequence_Size(cases_list);
    if (n_cases <= 0) {
        PyErr_SetString(PyExc_ValueError, "select needs at least 1 case");
        return NULL;
    }

    cs = (pygo_select_case_t *)PyMem_Calloc((size_t)n_cases, sizeof(*cs));
    if (cs == NULL) return PyErr_NoMemory();

    for (i = 0; i < n_cases; i++) {
        PyObject *item = PySequence_GetItem(cases_list, i);
        const char *op_str;
        PyObject *chan_obj;
        if (item == NULL) goto err;
        if (!PyTuple_Check(item) || PyTuple_GET_SIZE(item) < 2) {
            Py_DECREF(item);
            PyErr_SetString(PyExc_TypeError,
                "each case must be ('recv', ch) or ('send', ch, value)");
            goto err;
        }
        op_str = PyUnicode_AsUTF8(PyTuple_GET_ITEM(item, 0));
        chan_obj = PyTuple_GET_ITEM(item, 1);
        if (!PyObject_TypeCheck(chan_obj, &PygoChanType)) {
            Py_DECREF(item);
            PyErr_SetString(PyExc_TypeError, "case[1] must be a Chan");
            goto err;
        }
        cs[i].ch = ((PygoChan *)chan_obj)->ch;
        if (op_str && strcmp(op_str, "recv") == 0) {
            cs[i].op = PYGO_SELECT_RECV;
        } else if (op_str && strcmp(op_str, "send") == 0) {
            if (PyTuple_GET_SIZE(item) != 3) {
                Py_DECREF(item);
                PyErr_SetString(PyExc_TypeError, "send case needs (op, ch, value)");
                goto err;
            }
            cs[i].op = PYGO_SELECT_SEND;
            cs[i].send_value = PyTuple_GET_ITEM(item, 2);   /* borrowed */
        } else {
            Py_DECREF(item);
            PyErr_SetString(PyExc_ValueError, "op must be 'recv' or 'send'");
            goto err;
        }
        Py_DECREF(item);
    }

    fired = pygo_chan_select(cs, (int)n_cases, default_flag);
    if (fired == -2) goto err;        /* PyErr already set */
    if (fired == -1) {
        /* default-fired */
        PyMem_Free(cs);
        return PyLong_FromLong(-1);
    }

    /* Build result.  For RECV: (index, (value, ok)).
     *               For SEND: (index, None). */
    if (cs[fired].op == PYGO_SELECT_RECV) {
        PyObject *vok = PyTuple_New(2);
        if (vok == NULL) goto err;
        /* recv_value is a new ref already. */
        PyTuple_SET_ITEM(vok, 0, cs[fired].recv_value);
        PyTuple_SET_ITEM(vok, 1, PyBool_FromLong(cs[fired].recv_ok));
        result = Py_BuildValue("(iO)", fired, vok);
        Py_DECREF(vok);
    } else {
        result = Py_BuildValue("(iO)", fired, Py_None);
    }
    PyMem_Free(cs);
    return result;

err:
    /* Drop any RECV values we materialised (shouldn't be many). */
    if (cs != NULL) {
        for (i = 0; i < n_cases; i++) {
            if (cs[i].op == PYGO_SELECT_RECV && cs[i].recv_value != NULL) {
                Py_DECREF(cs[i].recv_value);
            }
        }
        PyMem_Free(cs);
    }
    return NULL;
}

static PyMethodDef module_methods[] = {
    {"select", (PyCFunction)m_select, METH_VARARGS | METH_KEYWORDS,
     "select(cases, default=False): wait on multiple channels.  Each "
     "case is ('recv', ch) or ('send', ch, value).  Returns "
     "(index, (value, ok)) for recv or (index, None) for send.  With "
     "default=True returns -1 if no case is immediately ready."},
    {"yield_",      m_yield,       METH_NOARGS,
     "Yield from inside a raw Coro (a no-op outside one)."},
    {"backend",     m_backend,     METH_NOARGS,
     "Return the active stack-switch backend name."},
    {"thread_init", m_thread_init, METH_NOARGS,
     "Per-OS-thread setup; idempotent."},
    {"thread_fini", m_thread_fini, METH_NOARGS,
     "Per-OS-thread teardown."},
    {"warmup", m_warmup, METH_VARARGS,
     "warmup(n, stack_size=131072) -> int: pre-mmap n stacks of "
     "stack_size bytes into the per-thread stack pool, eliminating "
     "first-spawn mmap latency for servers that know they're about "
     "to spawn N goroutines.  Returns the number actually allocated."},
    {"tcp_recv", m_tcp_recv, METH_VARARGS,
     "tcp_recv(fd, writable_buffer, n) -> bytes_received.  C-level "
     "recv with cooperative blocking; bypasses socket.recv_into's "
     "Python frame dispatch.  Buffer is filled in place."},
    {"tcp_send", m_tcp_send, METH_VARARGS,
     "tcp_send(fd, bytes_like) -> bytes_sent.  C-level sendall with "
     "cooperative blocking; loops until all bytes sent or error."},
    {"fd_read", m_fd_read, METH_VARARGS,
     "fd_read(fd, writable_buffer, n) -> bytes_read.  POSIX read(2) "
     "with cooperative blocking via netpoll.  Works on pipes, ttys, "
     "any pollable fd.  On Windows file fds aren't pollable: blocks "
     "the OS thread (use the monkey.py thread-pool path instead)."},
    {"fd_write", m_fd_write, METH_VARARGS,
     "fd_write(fd, bytes_like) -> bytes_written.  POSIX write(2) "
     "loop with cooperative blocking.  Same Windows caveat as fd_read."},
    {"file_read", m_file_read, METH_VARARGS,
     "file_read(fd, writable_buffer, n, offset=-1) -> bytes_read.  "
     "Uses io_uring on Linux >=5.1, falls back to pread/read.  "
     "Cooperative-ish: avoids the thread-pool roundtrip the monkey-"
     "patch path takes for regular files."},
    {"file_write", m_file_write, METH_VARARGS,
     "file_write(fd, bytes_like, offset=-1) -> bytes_written.  "
     "io_uring on Linux, plain write/pwrite elsewhere."},
    {"iouring_available", m_iouring_available, METH_NOARGS,
     "True if the io_uring kernel interface is usable (Linux 5.1+)."},
    /* C-scheduler fast path. */
    {"go",          m_go,          METH_O,
     "Spawn a goroutine via the C scheduler.  Returns a G handle."},
    {"go_noyield",  m_go_noyield,  METH_O,
     "Spawn a goroutine that the caller promises will run to "
     "completion without yielding.  Skips the per-g datastack/snap/"
     "load-sched dance -- ~150-400 ns/g faster than go() for pure-"
     "compute callables.  If the callable does yield (sched_yield, "
     "sched_sleep, wait_fd, monkey-patched I/O), behaviour is "
     "undefined.  Use only when you know the work is CPU-bound."},
    {"sched_yield_classic", m_sched_yield, METH_NOARGS,
     "Yield the current goroutine (METH_NOARGS PyCFunction form, kept "
     "for benchmarking against the vectorcall singleton)."},
    {"sched_sleep", m_sched_sleep, METH_O,
     "Sleep the current goroutine for N seconds (C scheduler aware)."},
    {"sched_stop",  m_sched_stop,  METH_NOARGS,
     "Signal the C scheduler to exit its drain loop at the next safe "
     "point.  Background goroutines parked on netpoll/sleep/wake will "
     "be left in their parked state; cleanup happens when the wrapping "
     "Python objects are gc'd."},
    {"sched_reset", m_sched_reset, METH_NOARGS,
     "Drop everything from the scheduler (ready + sleep heap).  Used "
     "by paio.run's cleanup so leftover background goroutines don't "
     "block the next pygo_core.run() with a sleep heap that won't "
     "drain for minutes.  Returns (n_ready_dropped, n_sleep_dropped)."},
    {"park_self",   m_park_self,   METH_NOARGS,
     "Park the current goroutine until G.wake() is called on its "
     "handle.  Race-safe: a wake that arrives before the park is "
     "consumed and the park returns immediately."},
    {"current_g",   m_current_g,   METH_NOARGS,
     "Return a G handle to the currently-running goroutine, or None "
     "if called outside one.  Used together with park_self/G.wake to "
     "implement lightweight per-task wake primitives without the "
     "overhead of a Chan."},
    {"run",         m_run,         METH_NOARGS,
     "Drive the C scheduler until all goroutines finish.  Returns count."},
    {"wait_fd",     m_wait_fd,     METH_VARARGS,
     "wait_fd(fd, events, timeout_ms=-1): park the current goroutine "
     "until fd is ready.  events is a bitmask: 1=read, 2=write."},
    {"netpoll_backend", m_netpoll_backend, METH_NOARGS,
     "Return active netpoll backend name (\"epoll\", \"kqueue\", \"select\")."},
    {"stats",       m_stats,       METH_NOARGS,
     "Return a dict of scheduler counters: ready, sleeping, "
     "netpoll_parked, completed, running, plus backend names.  "
     "Cheap; safe to poll from a watchdog goroutine in production."},
    {"mn_init",     m_mn_init,     METH_VARARGS,
     "mn_init(n=cpus): start N hub threads.  Returns count."},
    {"mn_go",       m_mn_go,       METH_O,
     "mn_go(callable): spawn a goroutine on a round-robin hub.  "
     "v1 only supports run-to-completion gs (no yield)."},
    {"mn_run",      m_mn_run,      METH_NOARGS,
     "mn_run(): wait for all gs to complete.  Returns total completed."},
    {"mn_fini",     m_mn_fini,     METH_NOARGS,
     "mn_fini(): tear down the hub pool."},
    {"preempt_init", m_preempt_init, METH_VARARGS,
     "preempt_init(quantum_us=10000): start the time-sliced preemption "
     "timer.  3.13t only.  Goroutines without explicit sched_yield "
     "calls will be preempted every quantum_us microseconds via a "
     "Py_AddPendingCall hook into CPython's eval_breaker."},
    {"preempt_fini", m_preempt_fini, METH_NOARGS,
     "preempt_fini(): stop the preemption timer (if running)."},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef pygo_core_module = {
    PyModuleDef_HEAD_INIT,
    "pygo_core",
    "Portable stackful coroutines + scheduler primitives.",
    -1,
    module_methods,
    NULL, NULL, NULL, NULL
};

/* Declare the extension as safe under free-threaded Python (3.13t).
 * Our single-OS-thread scheduler doesn't have multi-thread races
 * today; the M:N work-stealing in Phase C will need actual atomic
 * work-queue ops to keep this declaration honest. */
#ifdef Py_GIL_DISABLED
#  define PYGO_FT_OK 1
#endif

PyMODINIT_FUNC PyInit_pygo_core(void)
{
    PyObject *m;
    if (PyType_Ready(&PygoCoroType) < 0) return NULL;
    if (PyType_Ready(&PygoGType) < 0) return NULL;
    m = PyModule_Create(&pygo_core_module);
    if (m == NULL) {
        return NULL;
    }
    Py_INCREF(&PygoCoroType);
    Py_INCREF(&PygoGType);
    if (PyModule_AddObject(m, "G", (PyObject *)&PygoGType) < 0) {
        Py_DECREF(&PygoGType);
        Py_DECREF(m);
        return NULL;
    }
    if (PyModule_AddObject(m, "Coro", (PyObject *)&PygoCoroType) < 0) {
        Py_DECREF(&PygoCoroType);
        Py_DECREF(m);
        return NULL;
    }
    if (PyType_Ready(&PygoChanType) < 0) {
        Py_DECREF(m);
        return NULL;
    }
    Py_INCREF(&PygoChanType);
    if (PyModule_AddObject(m, "Chan", (PyObject *)&PygoChanType) < 0) {
        Py_DECREF(&PygoChanType);
        Py_DECREF(m);
        return NULL;
    }
    /* Set up the vectorcall singleton and expose it as `sched_yield`.
     * The interpreter's CALL bytecode will specialize this to the
     * vectorcall fast path, shaving Python-call-dispatch overhead off
     * each yield site. */
    if (PyType_Ready(&PygoYielderType) < 0) {
        Py_DECREF(m);
        return NULL;
    }
    {
        PygoYielder *y = PyObject_New(PygoYielder, &PygoYielderType);
        if (y == NULL) {
            Py_DECREF(m);
            return NULL;
        }
        y->vectorcall = yielder_vectorcall;
        if (PyModule_AddObject(m, "sched_yield", (PyObject *)y) < 0) {
            Py_DECREF(y);
            Py_DECREF(m);
            return NULL;
        }
    }
#ifdef Py_GIL_DISABLED
    /* Declare free-thread safety.  v0 scheduler is still single-OS-
     * thread, so this is trivially safe.  Phase C M:N needs to maintain
     * this when adding the work-stealing ring queue. */
    PyUnstable_Module_SetGIL(m, Py_MOD_GIL_NOT_USED);
#endif
    return m;
}
