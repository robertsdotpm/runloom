/* Part of pygo_tcp.c -- #included into that translation unit, not compiled
 * standalone.  See pygo_tcp.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* ============================================================
 * Method / type table
 * ============================================================ */
static PyMethodDef PygoTCPConn_methods[] = {
    {"recv",       (PyCFunction)PygoTCPConn_recv,       METH_VARARGS,
     "recv(n, flags=0) -> bytes.  Cooperative recv that allocates a "
     "bytes object of length <= n.  b'' on orderly shutdown."},
    {"recv_into",  (PyCFunction)PygoTCPConn_recv_into,  METH_VARARGS,
     "recv_into(buf, n=0, flags=0) -> int.  Recv directly into a "
     "writable buffer; no bytes-object allocation per call."},
    {"send",       (PyCFunction)PygoTCPConn_send,       METH_VARARGS,
     "send(data, flags=0) -> int.  Single send syscall; may return "
     "fewer bytes than len(data).  Parks on EAGAIN."},
    {"send_all",   (PyCFunction)PygoTCPConn_send_all,   METH_VARARGS,
     "send_all(data, flags=0) -> int.  Loop until all bytes sent."},
    {"accept",     (PyCFunction)PygoTCPConn_accept,     METH_NOARGS,
     "accept() -> TCPConn.  Park on EAGAIN until a new connection "
     "arrives; return a new TCPConn wrapping the accepted fd."},
    {"close",      (PyCFunction)PygoTCPConn_close,      METH_NOARGS,
     "close().  Idempotent."},
    {"fileno",     (PyCFunction)PygoTCPConn_fileno,     METH_NOARGS,
     "fileno() -> int.  Underlying socket fd, or -1 if closed."},
    {"setsockopt", (PyCFunction)PygoTCPConn_setsockopt, METH_VARARGS,
     "setsockopt(level, optname, value).  Same args as socket."},
    {"connect",    (PyCFunction)PygoTCPConn_connect_cls, METH_VARARGS | METH_KEYWORDS | METH_CLASS,
     "TCPConn.connect(host, port) -> TCPConn.  TCP/IPv4-or-v6 "
     "non-blocking connect; cooperatively waits for completion."},
    {"listen",     (PyCFunction)PygoTCPConn_listen_cls,  METH_VARARGS | METH_KEYWORDS | METH_CLASS,
     "TCPConn.listen(host, port, backlog=128) -> TCPConn.  Bind + "
     "listen on the given address; returns a listener that accept() "
     "can be called on."},
    {NULL, NULL, 0, NULL}
};

static PyGetSetDef PygoTCPConn_getset[] = {
    {"closed", (getter)PygoTCPConn_is_closed, NULL,
     "True after close().", NULL},
    {"family", (getter)PygoTCPConn_family_get, NULL,
     "AF_INET / AF_INET6 / etc.", NULL},
    {NULL, NULL, NULL, NULL, NULL}
};

static PyTypeObject PygoTCPConnType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "pygo_core.TCPConn",
    .tp_basicsize = sizeof(PygoTCPConn),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_doc = "Thin C-side TCP connection that bypasses socket.socket.",
    .tp_new = PygoTCPConn_new,
    .tp_init = (initproc)PygoTCPConn_init,
    .tp_dealloc = (destructor)PygoTCPConn_dealloc,
    .tp_methods = PygoTCPConn_methods,
    .tp_getset  = PygoTCPConn_getset,
};

int pygo_tcpconn_register(PyObject *module)
{
    if (PyType_Ready(&PygoTCPConnType) < 0) return -1;
    Py_INCREF(&PygoTCPConnType);
    if (PyModule_AddObject(module, "TCPConn",
                           (PyObject *)&PygoTCPConnType) < 0) {
        Py_DECREF(&PygoTCPConnType);
        return -1;
    }
    return 0;
}
