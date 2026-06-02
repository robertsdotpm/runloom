/* Part of module.c -- #included into that translation unit, not compiled
 * standalone.  See module.c for the includes/typedefs/file-scope state
 * these definitions depend on. */

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


/* ---- diagnostic wrappers ----
 *
 * Thin Python<->C bridges to pygo_diag.c.  Used by the C bench, the
 * Python bench, and gdb scripts to observe invariants without having
 * to hand-walk the C data structures. */
static PyObject *m_self_check(PyObject *self, PyObject *args)
{
    int verbose = 0;
    (void)self;
    if (!PyArg_ParseTuple(args, "|i", &verbose)) return NULL;
    return PyLong_FromLong((long)pygo_self_check(verbose));
}

static PyObject *m_diag_dump(PyObject *self, PyObject *args)
{
    int fd = 2;
    (void)self;
    if (!PyArg_ParseTuple(args, "|i", &fd)) return NULL;
    pygo_diag_dump(fd);
    Py_RETURN_NONE;
}

static PyObject *m_dump_parkers(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pygo_netpoll_dump_parkers();
    Py_RETURN_NONE;
}

static PyObject *m_diag_flags(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    return PyLong_FromUnsignedLong((unsigned long)pygo_debug_flags);
}

/* (tail_bytes, resident_bytes, chunks) for the datastack-tail sweep.
 * Only nonzero when PYGO_DATASTACK_DEBUG is set; the decompose readout. */
static PyObject *m_datastack_sweep_stats(PyObject *self, PyObject *args)
{
    unsigned long long tail = 0, resident = 0, chunks = 0;
    (void)self; (void)args;
    pygo_sched_datastack_sweep_stats(&tail, &resident, &chunks);
    return Py_BuildValue("(KKK)", tail, resident, chunks);
}


