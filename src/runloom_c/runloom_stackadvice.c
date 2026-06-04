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
#include "runloom_heavy_frames.h"   /* generated fat-frame symbol table */
#include "plat.h"
#include "plat_compat.h"

#include <string.h>

/* Mirror the calibration's sizing policy (runloom_sched_core.c.inc) so the
 * suggested size lines up with what set_default_stack_size would pick. */
#define RUNLOOM_ADVICE_SAFETY 4
#define RUNLOOM_ADVICE_MIN    ((size_t)16  * 1024)
#define RUNLOOM_ADVICE_MAX    ((size_t)8   * 1024 * 1024)
#define RUNLOOM_ADVICE_CAP    2048
#define RUNLOOM_AUTOSIZE_START_DEFAULT  ((size_t)256 * 1024)

typedef struct {
    size_t key;        /* 0 = empty slot */
    size_t max_hwm;    /* deepest stack use seen for this kind */
    size_t reserved;   /* stack size the most recent sample ran with */
    size_t cold_floor; /* prescan cold-start size if a heuristic/fat-frame
                        * symbol matched (0 = none); learn-down never shrinks
                        * this kind below it */
    long   samples;
    char   name[112];  /* "module.qualname (file:line)" */
} runloom_advice_entry_t;

static runloom_advice_entry_t runloom_advice_tbl[RUNLOOM_ADVICE_CAP];
static runloom_mutex_t        runloom_advice_lock;
static int                    runloom_advice_lock_inited = 0;
static int                    runloom_advice_on = 0;        /* measurement (atomic) */
static int                    runloom_autosize_on = 0;      /* apply learned sizes (atomic) */
static int                    runloom_prescan_on = 0;       /* cold-start fat-frame scan (atomic) */
static size_t                 runloom_autosize_start = RUNLOOM_AUTOSIZE_START_DEFAULT;

/* Forward decls (find/insert are defined lower, after the lock helpers). */
static runloom_advice_entry_t *runloom_advice_find(size_t key);

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

void runloom_advice_set_autosize(int on, int prescan)
{
    runloom_advice_ensure_lock();
    /* Establish the start size deterministically each enable: the default,
     * overridden by the env if set -- never a leftover from a previous call. */
    runloom_autosize_start = RUNLOOM_AUTOSIZE_START_DEFAULT;
    {
        const char *e = getenv("RUNLOOM_STACK_AUTOSIZE_START");
        if (e != NULL && e[0]) {
            long v = atol(e);
            if (v > 0) runloom_autosize_start = (size_t)v;
        }
    }
    if (on) {
        /* Autosize implies measurement (to learn sizes), painting (for the HWM
         * scan), and park-time reclaim (so the large starts stay RSS-free). */
        __atomic_store_n(&runloom_advice_on, 1, __ATOMIC_RELEASE);
        runloom_coro_paint_set(1);
        runloom_coro_park_reclaim_set(1);
    }
    __atomic_store_n(&runloom_prescan_on, (on && prescan) ? 1 : 0, __ATOMIC_RELEASE);
    __atomic_store_n(&runloom_autosize_on, on ? 1 : 0, __ATOMIC_RELEASE);
}

int runloom_advice_autosize_enabled(void)
{
    return __atomic_load_n(&runloom_autosize_on, __ATOMIC_ACQUIRE);
}

/* Follow __wrapped__ to the REAL underlying callable so a goroutine's kind (and
 * its prescan scan) is keyed on the user's function, not a wrapper.  runloom.go's
 * arg-binding lambda sets __wrapped__ to the target, and functools.wraps sets it
 * on decorated functions -- both should attribute to the wrapped function, not
 * the wrapper's code location.  Returns a NEW reference (the input incref'd when
 * there is no wrapper); bounded so a self-referential __wrapped__ can't spin. */
static PyObject *runloom_advice_unwrap(PyObject *callable)
{
    PyObject *cur = callable;
    int i;
    Py_INCREF(cur);
    for (i = 0; i < 8; i++) {
        PyObject *w = PyObject_GetAttrString(cur, "__wrapped__");
        if (w == NULL) { PyErr_Clear(); break; }
        Py_DECREF(cur);
        cur = w;            /* new ref */
    }
    return cur;
}

/* Cold-start optimizer: loosely scan the entry callable's bytecode names for a
 * fat-frame symbol (Decimal arithmetic, ...) and, if present, return a stack big
 * enough to hold that single frame plus headroom for the call chain reaching it.
 * MAX over matches, never a sum -- only the deepest single frame constrains the
 * stack (sequential C calls reuse the same stack).  GIL held.  Returns `generic`
 * unchanged when nothing heavy is referenced or the callable has no code. */
static size_t runloom_advice_cold_start(PyObject *callable, size_t generic)
{
    PyObject *real, *code, *names;
    Py_ssize_t i, n;
    size_t worst = 0, want;

    if (callable == NULL) return generic;
    real = runloom_advice_unwrap(callable);
    code = PyObject_GetAttrString(real, "__code__");
    Py_DECREF(real);
    if (code == NULL) { PyErr_Clear(); return generic; }
    names = PyObject_GetAttrString(code, "co_names");
    Py_DECREF(code);
    if (names == NULL || !PyTuple_Check(names)) {
        Py_XDECREF(names);
        PyErr_Clear();
        return generic;
    }
    n = PyTuple_GET_SIZE(names);
    for (i = 0; i < n; i++) {
        PyObject *nm = PyTuple_GET_ITEM(names, i);
        const char *s;
        int j;
        if (!PyUnicode_Check(nm)) continue;
        s = PyUnicode_AsUTF8(nm);
        if (s == NULL) { PyErr_Clear(); continue; }
        for (j = 0; j < RUNLOOM_HEAVY_FRAME_COUNT; j++) {
            if (strcmp(s, runloom_heavy_frames[j].sym) == 0 &&
                runloom_heavy_frames[j].frame_bytes > worst) {
                worst = runloom_heavy_frames[j].frame_bytes;   /* MAX, not sum */
            }
        }
    }
    Py_DECREF(names);
    if (worst == 0) return generic;
    /* Frame + ~50% headroom for the Python/C chain that reaches it. */
    want = runloom_advice_pow2(worst + worst / 2);
    if (want < generic) want = generic;
    if (want > RUNLOOM_ADVICE_MAX) want = RUNLOOM_ADVICE_MAX;
    return want;
}

size_t runloom_advice_size_for(size_t key, PyObject *callable, size_t fallback)
{
    int sampled = 0;
    size_t learned = 0;
    if (!__atomic_load_n(&runloom_autosize_on, __ATOMIC_RELAXED)) return fallback;
    if (key == 0) return fallback;    /* untracked / table-full: normal default */
    runloom_advice_ensure_lock();
    runloom_mutex_lock(&runloom_advice_lock);
    {
        runloom_advice_entry_t *e = runloom_advice_find(key);
        if (e != NULL && e->samples > 0) {
            /* Learned: size to the observed peak with margin (== suggested). */
            sampled = 1;
            learned = runloom_advice_pow2(e->max_hwm * RUNLOOM_ADVICE_SAFETY);
            if (learned < RUNLOOM_ADVICE_MIN) learned = RUNLOOM_ADVICE_MIN;
            if (learned > RUNLOOM_ADVICE_MAX) learned = RUNLOOM_ADVICE_MAX;
            /* Heuristic / fat-frame floor: a kind that matched a prescan symbol
             * (crypto, Decimal) never learns DOWN below its cold-start size --
             * matching is itself a signal the kind CAN go deep, so a run of
             * shallow inputs must not shrink it under the size that protects the
             * deep path it didn't happen to exercise. */
            if (learned < e->cold_floor) learned = e->cold_floor;
        }
    }
    runloom_mutex_unlock(&runloom_advice_lock);
    if (sampled) return learned;
    /* Unseen kind: start large; the cold-start optimizer (prescan) can raise it
     * to cover a fat single frame / heuristic class the code is about to call. */
    if (__atomic_load_n(&runloom_prescan_on, __ATOMIC_RELAXED)) {
        size_t cs = runloom_advice_cold_start(callable, runloom_autosize_start);
        if (cs > runloom_autosize_start) {
            /* A prescan symbol matched -> remember the floor so learn-down keeps
             * this kind roomy even if its measured samples turn out shallow. */
            runloom_mutex_lock(&runloom_advice_lock);
            {
                runloom_advice_entry_t *e = runloom_advice_find(key);
                if (e != NULL && cs > e->cold_floor) e->cold_floor = cs;
            }
            runloom_mutex_unlock(&runloom_advice_lock);
        }
        return cs;
    }
    return runloom_autosize_start;
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
    PyObject *real = runloom_advice_unwrap(callable);
    PyObject *qn  = PyObject_GetAttrString(real, "__qualname__");
    PyObject *mod = PyObject_GetAttrString(real, "__module__");
    PyObject *code = PyObject_GetAttrString(real, "__code__");
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

    Py_DECREF(real);
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
