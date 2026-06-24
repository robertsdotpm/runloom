/*
 * stacktest.c -- tiny CPython C extension used to exercise the static
 * stack-overflow rewriter.
 *
 * Pure C11, NO C++. Builds against free-threaded CPython 3.13t.
 *
 * It exposes functions whose compiled bodies contain a LARGE
 * `sub rsp, imm32` frame -- the 7-byte encoding `48 81 EC <imm32>` that
 * is the rewriter's PRIMARY instrumentation target. We force a large
 * frame by declaring a big local array and touching it (so the compiler
 * can't optimise the allocation away).
 *
 * `recurse_c` lets the harness drive arbitrary stack depth so we can blow
 * a small software limit deliberately.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <string.h>
#include <stdint.h>

/* Touch the whole buffer so the frame allocation survives -O2.
 * 'volatile' + a return value derived from the buffer defeats DCE. */
static long big_frame_work(long seed)
{
    volatile char buf[8192];          /* forces sub rsp, ~0x2000 (imm32) */
    long acc = 0;
    for (size_t i = 0; i < sizeof(buf); i++) {
        buf[i] = (char)((seed + (long)i) & 0xff);
    }
    for (size_t i = 0; i < sizeof(buf); i++) {
        acc += buf[i];
    }
    return acc;
}

/* Recursion that consumes a meaningful chunk of stack per frame so a
 * small limit is hit quickly. Each frame allocates a large array ->
 * one big single-shot `sub rsp,imm32` per call.
 *
 * We mark it noinline + use a noinline opaque sink so the compiler can't
 * unroll/flatten the recursion (which would merge the per-frame subs).
 * Built with -fno-stack-clash-protection so the frame is allocated in a
 * single `sub rsp,imm32` rather than a probing loop -- giving the rewriter
 * a clean per-call instrumentation target. */
static __attribute__((noinline)) long sink(volatile char *p, long n)
{
    return (long)p[0] + (long)p[n - 1];
}

static __attribute__((noinline)) long recurse_c(long depth)
{
    volatile char pad[4096];          /* per-frame stack burn; sub rsp,imm32 */
    long s = sink(pad, sizeof(pad));
    pad[0] = (char)depth;
    pad[sizeof(pad) - 1] = (char)(depth >> 8);
    if (depth <= 0) {
        return s + (long)pad[0] + (long)pad[sizeof(pad) - 1];
    }
    long r = recurse_c(depth - 1);
    return r + (long)pad[0] + s;
}

/* Return the current stack pointer as seen from inside the extension, so the
 * test harness can set a software limit RELATIVE to where the C code actually
 * runs (the C stack may differ from the Python interpreter's measured rsp,
 * especially on free-threaded builds). */
static PyObject *py_current_sp(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    void *sp;
    __asm__ volatile("mov %%rsp, %0" : "=r"(sp));
    return PyLong_FromUnsignedLongLong((unsigned long long)(uintptr_t)sp);
}

static PyObject *py_big_frame(PyObject *self, PyObject *args)
{
    long seed = 0;
    if (!PyArg_ParseTuple(args, "|l", &seed)) {
        return NULL;
    }
    long r = big_frame_work(seed);
    return PyLong_FromLong(r);
}

static PyObject *py_recurse(PyObject *self, PyObject *args)
{
    long depth = 0;
    if (!PyArg_ParseTuple(args, "l", &depth)) {
        return NULL;
    }
    long r = recurse_c(depth);
    return PyLong_FromLong(r);
}

static PyMethodDef stacktest_methods[] = {
    {"big_frame", py_big_frame, METH_VARARGS,
     "Run a function with a large sub rsp,imm32 frame; returns a checksum."},
    {"recurse", py_recurse, METH_VARARGS,
     "Recurse C-side to the given depth (each frame burns ~4KB of stack)."},
    {"current_sp", py_current_sp, METH_NOARGS,
     "Return the current rsp as an int (for the test harness limit calc)."},
    {NULL, NULL, 0, NULL},
};

static struct PyModuleDef stacktest_module = {
    PyModuleDef_HEAD_INIT,
    "stacktest",
    "Stack-overflow rewriter test extension (large sub rsp,imm32 frames).",
    -1,
    stacktest_methods,
    NULL, NULL, NULL, NULL,
};

PyMODINIT_FUNC PyInit_stacktest(void)
{
    return PyModule_Create(&stacktest_module);
}
