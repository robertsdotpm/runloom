/* Part of module.c -- #included into that translation unit, not compiled
 * standalone.  See module.c for the includes/typedefs/file-scope state.
 *
 * RunloomMachineCode -- execute runtime-provided NATIVE MACHINE CODE.
 *
 * !!! DANGER: runs arbitrary native instructions with NO sandbox, no memory
 * safety, nothing between your bytes and the CPU.  This is an experimental /
 * research primitive (hand-rolled SIMD kernels, tiny JITs, teaching).  NEVER
 * pass bytes from an untrusted source.
 *
 * MachineCode(code) maps a page W^X -- writable while we copy the bytes in,
 * then flipped to read+execute (never simultaneously writable AND executable).
 * Calling the object does a REAL native call into the page: the CPU's
 * instruction pointer jumps straight to your bytes and the silicon executes
 * them -- there is no interpreter, no VM, no dispatch loop (unlike CPython
 * bytecode).  It runs on the CALLER's stack, so inside a fiber it uses the
 * fiber's small, guard-paged C stack: the same fat-frame rule applies, and
 * it cannot yield/park or be preempted mid-blob (keep blobs short, or run a
 * heavy kernel via runloom.monkey.offload() so it lands on a pool thread).
 *
 * ABI: the blob is the host platform's calling convention for
 *     intptr_t f(intptr_t a0, ..., a5)         (0..6 integer/pointer args)
 *   x86-64 SysV : args rdi,rsi,rdx,rcx,r8,r9 ; return rax
 *   AArch64     : args x0..x5                 ; return x0
 * Pass an address (int) to hand the blob a buffer for more args / results.
 * The trampoline is portable C -- the compiler emits the host ABI; only the
 * blob itself is architecture-specific.
 */
#include <stdint.h>
#include <string.h>

#if defined(_WIN32)
#include <windows.h>
#else
#include <sys/mman.h>
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif
#endif

typedef struct {
    PyObject_HEAD
    void  *page;
    size_t len;
} RunloomMachineCode;

/* Map `n` bytes from `src` as executable (W^X).  Returns the mapping or NULL. */
static void *runloom_mc_map(const void *src, size_t n)
{
#if defined(_WIN32)
    DWORD old;
    void *p = VirtualAlloc(NULL, n, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
    if (p == NULL) return NULL;
    memcpy(p, src, n);
    if (!VirtualProtect(p, n, PAGE_EXECUTE_READ, &old)) {
        VirtualFree(p, 0, MEM_RELEASE);
        return NULL;
    }
    FlushInstructionCache(GetCurrentProcess(), p, n);
    return p;
#else
    void *p = mmap(NULL, n, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (p == MAP_FAILED) return NULL;
    memcpy(p, src, n);
    /* Make the just-written bytes coherent with the instruction cache
     * (a no-op on x86, required on ARM/POWER). */
    __builtin___clear_cache((char *)p, (char *)p + n);
    if (mprotect(p, n, PROT_READ | PROT_EXEC) != 0) {
        munmap(p, n);
        return NULL;
    }
    return p;
#endif
}

static void runloom_mc_unmap(void *p, size_t n)
{
    if (p == NULL) return;
#if defined(_WIN32)
    (void)n;
    VirtualFree(p, 0, MEM_RELEASE);
#else
    munmap(p, n);
#endif
}

typedef intptr_t (*runloom_mc_fn0)(void);
typedef intptr_t (*runloom_mc_fn1)(intptr_t);
typedef intptr_t (*runloom_mc_fn2)(intptr_t, intptr_t);
typedef intptr_t (*runloom_mc_fn3)(intptr_t, intptr_t, intptr_t);
typedef intptr_t (*runloom_mc_fn4)(intptr_t, intptr_t, intptr_t, intptr_t);
typedef intptr_t (*runloom_mc_fn5)(intptr_t, intptr_t, intptr_t, intptr_t,
                                   intptr_t);
typedef intptr_t (*runloom_mc_fn6)(intptr_t, intptr_t, intptr_t, intptr_t,
                                   intptr_t, intptr_t);

static PyObject *runloom_mc_new(PyTypeObject *type, PyObject *args,
                                PyObject *kwds)
{
    static char *kw[] = {"code", NULL};
    Py_buffer view;
    void *page;
    size_t n;
    RunloomMachineCode *self;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "y*:MachineCode", kw, &view))
        return NULL;
    if (view.len <= 0) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "MachineCode: code must be non-empty");
        return NULL;
    }
    n = (size_t)view.len;
    page = runloom_mc_map(view.buf, n);
    PyBuffer_Release(&view);
    if (page == NULL) {
        PyErr_SetString(PyExc_OSError,
                        "MachineCode: could not map executable memory "
                        "(W^X / execmem policy?)");
        return NULL;
    }
    self = (RunloomMachineCode *)type->tp_alloc(type, 0);
    if (self == NULL) {
        runloom_mc_unmap(page, n);
        return NULL;
    }
    self->page = page;
    self->len = n;
    return (PyObject *)self;
}

static void runloom_mc_dealloc(RunloomMachineCode *self)
{
    runloom_mc_unmap(self->page, self->len);
    self->page = NULL;
    Py_TYPE(self)->tp_free((PyObject *)self);
}

/* MachineCode(args...) -- call the blob.  Up to 6 integer/pointer args; the
 * return value is the result register reinterpreted as a signed word. */
static PyObject *runloom_mc_call(RunloomMachineCode *self, PyObject *args,
                                 PyObject *kwds)
{
    Py_ssize_t i, nargs;
    intptr_t a[6] = {0, 0, 0, 0, 0, 0};
    intptr_t r;
    void *p = self->page;

    if (p == NULL) {
        PyErr_SetString(PyExc_ValueError, "MachineCode: closed");
        return NULL;
    }
    if (kwds != NULL && PyDict_GET_SIZE(kwds) != 0) {
        PyErr_SetString(PyExc_TypeError,
                        "MachineCode call takes no keyword arguments");
        return NULL;
    }
    nargs = PyTuple_GET_SIZE(args);
    if (nargs > 6) {
        PyErr_SetString(PyExc_TypeError,
                        "MachineCode: at most 6 integer/pointer args");
        return NULL;
    }
    for (i = 0; i < nargs; i++) {
        /* Mask to the low pointer-width bits so any Python int -- including a
         * raw address with the high bit set, or a negative -- becomes the
         * machine word the blob expects. */
        unsigned long long bits =
            PyLong_AsUnsignedLongLongMask(PyTuple_GET_ITEM(args, i));
        if (bits == (unsigned long long)-1 && PyErr_Occurred())
            return NULL;
        a[i] = (intptr_t)(uintptr_t)bits;
    }

    switch (nargs) {
    case 0:  r = ((runloom_mc_fn0)p)(); break;
    case 1:  r = ((runloom_mc_fn1)p)(a[0]); break;
    case 2:  r = ((runloom_mc_fn2)p)(a[0], a[1]); break;
    case 3:  r = ((runloom_mc_fn3)p)(a[0], a[1], a[2]); break;
    case 4:  r = ((runloom_mc_fn4)p)(a[0], a[1], a[2], a[3]); break;
    case 5:  r = ((runloom_mc_fn5)p)(a[0], a[1], a[2], a[3], a[4]); break;
    default: r = ((runloom_mc_fn6)p)(a[0], a[1], a[2], a[3], a[4], a[5]); break;
    }
    return PyLong_FromSsize_t((Py_ssize_t)r);
}

static PyObject *runloom_mc_close(RunloomMachineCode *self, PyObject *unused)
{
    (void)unused;
    runloom_mc_unmap(self->page, self->len);
    self->page = NULL;
    Py_RETURN_NONE;
}

static PyObject *runloom_mc_enter(RunloomMachineCode *self, PyObject *unused)
{
    (void)unused;
    Py_INCREF(self);
    return (PyObject *)self;
}

static PyObject *runloom_mc_exit(RunloomMachineCode *self, PyObject *unused)
{
    (void)unused;
    runloom_mc_unmap(self->page, self->len);
    self->page = NULL;
    Py_RETURN_FALSE;
}

static PyObject *runloom_mc_get_address(RunloomMachineCode *self, void *closure)
{
    (void)closure;
    return PyLong_FromVoidPtr(self->page);
}

static PyObject *runloom_mc_get_size(RunloomMachineCode *self, void *closure)
{
    (void)closure;
    return PyLong_FromSize_t(self->len);
}

static PyMethodDef runloom_mc_methods[] = {
    {"close",    (PyCFunction)runloom_mc_close, METH_NOARGS,
     "Unmap the executable page now (idempotent).  Calling after close raises."},
    {"__enter__", (PyCFunction)runloom_mc_enter, METH_NOARGS, "Context manager."},
    {"__exit__",  (PyCFunction)runloom_mc_exit,  METH_VARARGS, "Close on exit."},
    {NULL, NULL, 0, NULL},
};

static PyGetSetDef runloom_mc_getset[] = {
    {"address", (getter)runloom_mc_get_address, NULL,
     "Address of the mapped executable page (int).", NULL},
    {"size", (getter)runloom_mc_get_size, NULL,
     "Length of the machine code in bytes.", NULL},
    {NULL, NULL, NULL, NULL, NULL},
};

static PyTypeObject RunloomMachineCodeType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "runloom_c.MachineCode",
    .tp_basicsize = sizeof(RunloomMachineCode),
    .tp_dealloc = (destructor)runloom_mc_dealloc,
    .tp_call = (ternaryfunc)runloom_mc_call,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc =
        "MachineCode(code) -> a callable wrapping runtime NATIVE MACHINE CODE.\n\n"
        "DANGER: executes arbitrary native instructions with no sandbox -- an\n"
        "experimental primitive; never pass untrusted bytes.  `code` is a\n"
        "bytes-like blob of host machine code implementing\n"
        "  intptr_t f(intptr_t a0, ..., a5)   (0..6 int/pointer args; x86-64\n"
        "  SysV args rdi,rsi,...; arm64 x0..x5; return in rax/x0).\n"
        "Calling jumps the CPU straight into the bytes (no interpreter).  It\n"
        "runs on the caller's stack -- inside a fiber that's the small,\n"
        "guard-paged g-stack, and it can't yield or be preempted mid-blob.\n"
        "Mapped W^X; .address/.size expose the page; close()/with free it.",
    .tp_methods = runloom_mc_methods,
    .tp_getset = runloom_mc_getset,
    .tp_new = runloom_mc_new,
};
