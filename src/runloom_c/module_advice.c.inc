/* Part of module.c -- #included into that translation unit, not compiled
 * standalone.  Python bindings for the stack-usage advisory profiler
 * (runloom_stackadvice.c). */

static PyObject *m_enable_stack_advice(PyObject *self, PyObject *args)
{
    int on = 1;
    (void)self;
    if (!PyArg_ParseTuple(args, "|p", &on)) return NULL;
    runloom_advice_set_enabled(on);
    Py_RETURN_NONE;
}

static PyObject *m_stack_advice_enabled(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    return PyBool_FromLong(runloom_advice_enabled());
}

static PyObject *m_stack_advice(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    return runloom_advice_report();
}

static PyObject *m_reset_stack_advice(PyObject *self, PyObject *unused)
{
    (void)self; (void)unused;
    runloom_advice_reset();
    Py_RETURN_NONE;
}
