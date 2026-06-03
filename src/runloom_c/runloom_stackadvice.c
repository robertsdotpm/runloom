/* runloom_stackadvice.c -- per-goroutine-kind stack-usage profiler.
 * See runloom_stackadvice.h. */

#if !defined(_WIN32)
#  define _POSIX_C_SOURCE 200809L
#endif
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "runloom_stackadvice.h"
#include "runloom_sched.h"   /* runloom_g_t fields (advice_key, coro) */
#include "coro.h"
#include "plat.h"
#include "plat_compat.h"

#include <string.h>

/* Mirror the calibration's sizing policy (runloom_sched_core.c.inc) so the
 * suggested size lines up with what set_default_stack_size would pick. */
#define RUNLOOM_ADVICE_SAFETY 4
#define RUNLOOM_ADVICE_MIN    ((size_t)16  * 1024)
#define RUNLOOM_ADVICE_MAX    ((size_t)8   * 1024 * 1024)
#define RUNLOOM_ADVICE_CAP    2048

typedef struct {
    size_t key;        /* 0 = empty slot */
    size_t max_hwm;    /* deepest stack use seen for this kind */
    size_t reserved;   /* stack size the most recent sample ran with */
    long   samples;
    char   name[112];  /* "module.qualname (file:line)" */
} runloom_advice_entry_t;

static runloom_advice_entry_t runloom_advice_tbl[RUNLOOM_ADVICE_CAP];
static runloom_mutex_t        runloom_advice_lock;
static int                    runloom_advice_lock_inited = 0;
static int                    runloom_advice_on = 0;   /* atomic */

static size_t runloom_advice_pow2(size_t v)
{
    size_t p = 1;
    while (p < v && p < RUNLOOM_ADVICE_MAX) p <<= 1;
    return p;
}

static void runloom_advice_ensure_lock(void)
{
    /* One-time lock init; the 0/1/2 CAS+spin guard the rest of runloom_c uses. */
    int st = __atomic_load_n(&runloom_advice_lock_inited, __ATOMIC_ACQUIRE);
    if (st == 2) return;
    if (st == 0 &&
        __atomic_compare_exchange_n(&runloom_advice_lock_inited, &st, 1, 0,
                                    __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        runloom_mutex_init(&runloom_advice_lock);
        __atomic_store_n(&runloom_advice_lock_inited, 2, __ATOMIC_RELEASE);
        return;
    }
    while (__atomic_load_n(&runloom_advice_lock_inited, __ATOMIC_ACQUIRE) != 2) { /* spin */ }
}

void runloom_advice_set_enabled(int on)
{
    runloom_advice_ensure_lock();
    __atomic_store_n(&runloom_advice_on, on ? 1 : 0, __ATOMIC_RELEASE);
    if (on) {
        /* The HWM scan needs the stacks painted; calibration turns painting off
         * once it freezes, so turn it back on for the profiling session. */
        runloom_coro_paint_set(1);
    }
}

int runloom_advice_enabled(void)
{
    return __atomic_load_n(&runloom_advice_on, __ATOMIC_ACQUIRE);
}

static size_t runloom_advice_hash(const char *s)
{
    size_t h = (size_t)1469598103934665603ULL;
    while (*s) {
        h ^= (unsigned char)*s++;
        h *= (size_t)1099511628211ULL;
    }
    return h ? h : 1;   /* never 0 (0 means "no kind") */
}

/* Build "module.qualname (basename:line)" for a callable.  GIL held. */
static void runloom_advice_name_of(PyObject *callable, char *out, size_t outsz)
{
    PyObject *qn  = PyObject_GetAttrString(callable, "__qualname__");
    PyObject *mod = PyObject_GetAttrString(callable, "__module__");
    PyObject *code = PyObject_GetAttrString(callable, "__code__");
    PyObject *fnobj = NULL, *lnobj = NULL;
    const char *q = (qn  && PyUnicode_Check(qn))  ? PyUnicode_AsUTF8(qn)  : NULL;
    const char *m = (mod && PyUnicode_Check(mod)) ? PyUnicode_AsUTF8(mod) : NULL;
    const char *fn = NULL;
    long line = 0;

    if (code != NULL) {
        fnobj = PyObject_GetAttrString(code, "co_filename");
        lnobj = PyObject_GetAttrString(code, "co_firstlineno");
        if (fnobj && PyUnicode_Check(fnobj)) fn = PyUnicode_AsUTF8(fnobj);
        if (lnobj && PyLong_Check(lnobj))    line = PyLong_AsLong(lnobj);
    }
    if (fn != NULL) {
        const char *slash = strrchr(fn, '/');
        if (slash != NULL) fn = slash + 1;
    }
    if (fn != NULL)
        snprintf(out, outsz, "%s.%s (%s:%ld)", m ? m : "?", q ? q : "<callable>", fn, line);
    else
        snprintf(out, outsz, "%s.%s", m ? m : "?", q ? q : "<callable>");

    Py_XDECREF(qn);
    Py_XDECREF(mod);
    Py_XDECREF(code);
    Py_XDECREF(fnobj);
    Py_XDECREF(lnobj);
    PyErr_Clear();   /* any attribute miss is non-fatal */
}

/* Caller holds the lock. */
static runloom_advice_entry_t *runloom_advice_find(size_t key)
{
    size_t i, base = key % RUNLOOM_ADVICE_CAP;
    for (i = 0; i < RUNLOOM_ADVICE_CAP; i++) {
        runloom_advice_entry_t *e = &runloom_advice_tbl[(base + i) % RUNLOOM_ADVICE_CAP];
        if (e->key == key) return e;
        if (e->key == 0)   return NULL;
    }
    return NULL;
}

/* Caller holds the lock.  Returns the entry, or NULL if the table is full. */
static runloom_advice_entry_t *runloom_advice_insert(size_t key, const char *name)
{
    size_t i, base = key % RUNLOOM_ADVICE_CAP;
    for (i = 0; i < RUNLOOM_ADVICE_CAP; i++) {
        runloom_advice_entry_t *e = &runloom_advice_tbl[(base + i) % RUNLOOM_ADVICE_CAP];
        if (e->key == key) return e;
        if (e->key == 0) {
            e->key = key;
            e->max_hwm = 0;
            e->reserved = 0;
            e->samples = 0;
            snprintf(e->name, sizeof e->name, "%s", name);
            return e;
        }
    }
    return NULL;
}

size_t runloom_advice_note_spawn(PyObject *callable)
{
    char name[112];
    size_t key;
    runloom_advice_entry_t *e;
    if (!__atomic_load_n(&runloom_advice_on, __ATOMIC_RELAXED)) return 0;
    if (callable == NULL) return 0;
    runloom_advice_name_of(callable, name, sizeof name);
    key = runloom_advice_hash(name);
    runloom_advice_ensure_lock();
    runloom_mutex_lock(&runloom_advice_lock);
    e = runloom_advice_insert(key, name);
    runloom_mutex_unlock(&runloom_advice_lock);
    return e ? key : 0;
}

void runloom_advice_record_g(struct runloom_g *g)
{
    if (g == NULL || g->coro == NULL) return;
    if (g->advice_key == 0) return;   /* 0 unless spawned while profiling on */
    if (!__atomic_load_n(&runloom_advice_on, __ATOMIC_RELAXED)) return;
    runloom_advice_record(g->advice_key,
                          runloom_coro_scan_hwm(g->coro),
                          runloom_coro_stack_size(g->coro));
}

void runloom_advice_record(size_t key, size_t hwm, size_t reserved)
{
    runloom_advice_entry_t *e;
    if (key == 0) return;
    if (!__atomic_load_n(&runloom_advice_on, __ATOMIC_RELAXED)) return;
    runloom_advice_ensure_lock();
    runloom_mutex_lock(&runloom_advice_lock);
    e = runloom_advice_find(key);
    if (e != NULL) {
        if (hwm > e->max_hwm) e->max_hwm = hwm;
        e->reserved = reserved;
        e->samples++;
    }
    runloom_mutex_unlock(&runloom_advice_lock);
}

void runloom_advice_reset(void)
{
    runloom_advice_ensure_lock();
    runloom_mutex_lock(&runloom_advice_lock);
    memset(runloom_advice_tbl, 0, sizeof runloom_advice_tbl);
    runloom_mutex_unlock(&runloom_advice_lock);
}

void runloom_advice_reset_after_fork(void)
{
    runloom_mutex_init(&runloom_advice_lock);
    __atomic_store_n(&runloom_advice_lock_inited, 2, __ATOMIC_RELEASE);
}

PyObject *runloom_advice_report(void)
{
    PyObject *list;
    size_t i;
    runloom_advice_ensure_lock();
    list = PyList_New(0);
    if (list == NULL) return NULL;
    runloom_mutex_lock(&runloom_advice_lock);
    for (i = 0; i < RUNLOOM_ADVICE_CAP; i++) {
        runloom_advice_entry_t *e = &runloom_advice_tbl[i];
        size_t sug;
        PyObject *d;
        if (e->key == 0) continue;
        sug = runloom_advice_pow2(e->max_hwm * RUNLOOM_ADVICE_SAFETY);
        if (sug < RUNLOOM_ADVICE_MIN) sug = RUNLOOM_ADVICE_MIN;
        if (sug > RUNLOOM_ADVICE_MAX) sug = RUNLOOM_ADVICE_MAX;
        d = Py_BuildValue("{s:s,s:l,s:n,s:n,s:n}",
                          "kind", e->name,
                          "samples", e->samples,
                          "max_hwm", (Py_ssize_t)e->max_hwm,
                          "reserved", (Py_ssize_t)e->reserved,
                          "suggested", (Py_ssize_t)sug);
        if (d == NULL) {
            runloom_mutex_unlock(&runloom_advice_lock);
            Py_DECREF(list);
            return NULL;
        }
        if (PyList_Append(list, d) != 0) {
            Py_DECREF(d);
            runloom_mutex_unlock(&runloom_advice_lock);
            Py_DECREF(list);
            return NULL;
        }
        Py_DECREF(d);
    }
    runloom_mutex_unlock(&runloom_advice_lock);
    return list;
}
