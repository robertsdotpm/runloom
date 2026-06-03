/* Part of runloom_tcp.c -- #included into that translation unit, not compiled
 * standalone.  See runloom_tcp.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* ============================================================
 * Method / type table
 * ============================================================ */
static PyMethodDef RunloomTCPConn_methods[] = {
    {"recv",       (PyCFunction)RunloomTCPConn_recv,       METH_VARARGS,
     "recv(n, flags=0) -> bytes.  Cooperative recv that allocates a "
     "bytes object of length <= n.  b'' on orderly shutdown."},
    {"recv_into",  (PyCFunction)RunloomTCPConn_recv_into,  METH_VARARGS,
     "recv_into(buf, n=0, flags=0) -> int.  Recv directly into a "
     "writable buffer; no bytes-object allocation per call."},
    {"send",       (PyCFunction)RunloomTCPConn_send,       METH_VARARGS,
     "send(data, flags=0) -> int.  Single send syscall; may return "
     "fewer bytes than len(data).  Parks on EAGAIN."},
    {"send_all",   (PyCFunction)RunloomTCPConn_send_all,   METH_VARARGS,
     "send_all(data, flags=0) -> int.  Loop until all bytes sent."},
    {"accept",     (PyCFunction)RunloomTCPConn_accept,     METH_NOARGS,
     "accept() -> TCPConn.  Park on EAGAIN until a new connection "
     "arrives; return a new TCPConn wrapping the accepted fd."},
    {"close",      (PyCFunction)RunloomTCPConn_close,      METH_NOARGS,
     "close().  Idempotent."},
    {"fileno",     (PyCFunction)RunloomTCPConn_fileno,     METH_NOARGS,
     "fileno() -> int.  Underlying socket fd, or -1 if closed."},
    {"setsockopt", (PyCFunction)RunloomTCPConn_setsockopt, METH_VARARGS,
     "setsockopt(level, optname, value).  Same args as socket."},
    {"connect",    (PyCFunction)RunloomTCPConn_connect_cls, METH_VARARGS | METH_KEYWORDS | METH_CLASS,
     "TCPConn.connect(host, port) -> TCPConn.  TCP/IPv4-or-v6 "
     "non-blocking connect; cooperatively waits for completion."},
    {"listen",     (PyCFunction)RunloomTCPConn_listen_cls,  METH_VARARGS | METH_KEYWORDS | METH_CLASS,
     "TCPConn.listen(host, port, backlog=128) -> TCPConn.  Bind + "
     "listen on the given address; returns a listener that accept() "
     "can be called on."},
    {NULL, NULL, 0, NULL}
};

static PyGetSetDef RunloomTCPConn_getset[] = {
    {"closed", (getter)RunloomTCPConn_is_closed, NULL,
     "True after close().", NULL},
    {"family", (getter)RunloomTCPConn_family_get, NULL,
     "AF_INET / AF_INET6 / etc.", NULL},
    {NULL, NULL, NULL, NULL, NULL}
};

static PyTypeObject RunloomTCPConnType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "runloom_c.TCPConn",
    .tp_basicsize = sizeof(RunloomTCPConn),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_doc = "Thin C-side TCP connection that bypasses socket.socket.",
    .tp_new = RunloomTCPConn_new,
    .tp_init = (initproc)RunloomTCPConn_init,
    .tp_dealloc = (destructor)RunloomTCPConn_dealloc,
    .tp_methods = RunloomTCPConn_methods,
    .tp_getset  = RunloomTCPConn_getset,
};

int runloom_tcpconn_register(PyObject *module)
{
    if (PyType_Ready(&RunloomTCPConnType) < 0) return -1;
    Py_INCREF(&RunloomTCPConnType);
    if (PyModule_AddObject(module, "TCPConn",
                           (PyObject *)&RunloomTCPConnType) < 0) {
        Py_DECREF(&RunloomTCPConnType);
        return -1;
    }
    return 0;
}
