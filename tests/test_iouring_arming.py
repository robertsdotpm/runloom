"""Behavioral torture test for the io_uring file-I/O backend.

Same philosophy as test_netpoll_arming.py, adapted to io_uring's completion
model.  For netpoll the unit of delivery is an fd-readiness edge; for io_uring
it is a CQE whose result must be routed back to the EXACT fiber that
submitted it -- including ERROR completions, where the kernel reports the op's
errno as cqe->res = -errno (not via the errno of any syscall) and the runtime
must turn that into a clean OSError.

This pins down the completion contract: correct result and byte payload on
success, the correct errno on every failure mode, EOF as a zero-length read,
and -- the concurrency-critical part -- that N fibers submitting at once
each get their OWN completion back (the multi-drainer cq_head CAS must not
cross-deliver or double-consume a CQE).

Skipped unless io_uring is actually available (Linux >= 5.1).
"""
import errno
import os
import subprocess
import sys
import tempfile

import pytest

import runloom_c

pytestmark = pytest.mark.skipif(
    not runloom_c.iouring_available(),
    reason="io_uring not available (need Linux >= 5.1)")

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read_via_fiber(fd, n, offset=0):
    """file_read `n` bytes from `fd` inside a fiber; return (data|None, errno|None)."""
    out = {}

    def worker():
        buf = bytearray(n)
        try:
            got = runloom_c.file_read(fd, buf, n, offset)
            out["data"] = bytes(buf[:got])
            out["n"] = got
        except OSError as e:
            out["errno"] = e.errno

    runloom_c.go(worker)
    runloom_c.run()
    return out


def _tmpfile(content):
    path = tempfile.mktemp()
    with open(path, "wb") as f:
        f.write(content)
    return path


# ---------------------------------------------------------------------------
# Success completions.
# ---------------------------------------------------------------------------

def test_roundtrip_write_then_read():
    path = _tmpfile(b"")
    fd = os.open(path, os.O_RDWR)
    try:
        data = b"io_uring round trip " * 50

        out = {}

        def w():
            out["written"] = runloom_c.file_write(fd, data, 0)

        runloom_c.go(w)
        runloom_c.run()
        assert out["written"] == len(data)

        got = _read_via_fiber(fd, len(data), 0)
        assert got.get("data") == data, got
    finally:
        os.close(fd)
        os.unlink(path)


def test_eof_reads_zero():
    path = _tmpfile(b"abc")
    fd = os.open(path, os.O_RDONLY)
    try:
        got = _read_via_fiber(fd, 16, 100)   # offset past EOF
        assert got.get("n") == 0, got
    finally:
        os.close(fd)
        os.unlink(path)


def test_partial_read():
    path = _tmpfile(b"abcdefghij")
    fd = os.open(path, os.O_RDONLY)
    try:
        got = _read_via_fiber(fd, 5, 0)
        assert got.get("data") == b"abcde", got
    finally:
        os.close(fd)
        os.unlink(path)


# ---------------------------------------------------------------------------
# Error completions: cqe->res = -errno must surface as OSError(errno).
# ---------------------------------------------------------------------------

def test_error_completion_closed_fd():
    """A read submitted on a stale (closed) fd completes with res=-EBADF."""
    fd = os.open(os.devnull, os.O_RDONLY)
    os.close(fd)
    got = _read_via_fiber(fd, 16, 0)
    assert got.get("errno") == errno.EBADF, got


def test_error_completion_read_on_writeonly():
    """Reading a write-only fd completes with res=-EBADF."""
    path = _tmpfile(b"x")
    fd = os.open(path, os.O_WRONLY)
    try:
        got = _read_via_fiber(fd, 4, 0)
        assert got.get("errno") == errno.EBADF, got
    finally:
        os.close(fd)
        os.unlink(path)


def test_error_completion_isdir():
    """Reading a directory fd completes with res=-EISDIR."""
    d = tempfile.mkdtemp()
    fd = os.open(d, os.O_RDONLY)
    try:
        got = _read_via_fiber(fd, 16, 0)
        assert got.get("errno") == errno.EISDIR, got
    finally:
        os.close(fd)
        os.rmdir(d)


# ---------------------------------------------------------------------------
# Concurrent submitters: every fiber gets ITS OWN completion back.
# ---------------------------------------------------------------------------

def test_concurrent_distinct_files_single_thread():
    """N fibers each read a file with distinct content; the per-op result
    routing (user_data) must give each fiber exactly its own bytes."""
    N = 24
    paths = [_tmpfile(("file-%03d-" % i).encode() * 40) for i in range(N)]
    fds = [os.open(p, os.O_RDONLY) for p in paths]
    results = [None] * N
    try:
        def make_worker(i):
            def worker():
                size = os.path.getsize(paths[i])
                buf = bytearray(size)
                got = runloom_c.file_read(fds[i], buf, size, 0)
                results[i] = bytes(buf[:got])
            return worker

        for i in range(N):
            runloom_c.go(make_worker(i))
        runloom_c.run()

        for i in range(N):
            expected = ("file-%03d-" % i).encode() * 40
            assert results[i] == expected, (
                "cross-delivered completion at %d: %r" % (i, results[i]))
    finally:
        for fd in fds:
            os.close(fd)
        for p in paths:
            os.unlink(p)


def _mn_fileread_snippet(hubs, n):
    """A self-contained snippet: spawn `n` fibers across `hubs` M:N hubs,
    each file_read'ing its own file, and PASS iff every byte payload is right."""
    code = r'''
import sys; sys.path.insert(0, __SRCPATH__)
import os, tempfile
import runloom_c

if not runloom_c.iouring_available():
    print("PASS")   # nothing to stress
    sys.exit(0)

N = __N__; H = __H__
paths, fds, expected, results = [], [], [], [None] * N
for i in range(N):
    content = ("mn-%04d-" % i).encode() * 32
    p = tempfile.mktemp()
    with open(p, "wb") as f:
        f.write(content)
    paths.append(p); expected.append(content)
    fds.append(os.open(p, os.O_RDONLY))

def mk(i):
    def w():
        size = len(expected[i])
        buf = bytearray(size)
        m = runloom_c.file_read(fds[i], buf, size, 0)
        results[i] = bytes(buf[:m])
    return w

runloom_c.mn_init(H)
for i in range(N):
    runloom_c.mn_go(mk(i))
runloom_c.mn_run()
runloom_c.mn_fini()

for fd in fds: os.close(fd)
for p in paths: os.unlink(p)

bad = [i for i in range(N) if results[i] != expected[i]]
print("PASS" if not bad else ("FAIL cross/missed: %r" % bad[:10]))
'''
    return (code.replace("__SRCPATH__", repr(os.path.join(REPO, "src")))
                .replace("__N__", str(n)).replace("__H__", str(hubs)))


def _run_snippet(code, timeout=60):
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    return subprocess.run([sys.executable, "-c", code], cwd=REPO, env=env,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          text=True, timeout=timeout)


def test_mn_iouring_fileread_single_hub():
    """Regression for the M:N io_uring completion-wake corruption.  Under the
    M:N scheduler a file_read's own CQE drain called runloom_mn_wake_g on the
    RUNNING, spin-draining submitter; in the default (non-global-runq) mode that
    re-submits the fiber to its hub while it is running and about to
    complete, corrupting the hub run-queue/pending accounting and stranding
    other queued fibers -- an intermittent hang reproducible even with ONE
    hub.  Fixed by not waking hub SINGLE ops (the spinner observes op->result
    directly).  Heavy single-hub stress, repeated, in fresh subprocesses
    (mn_init installs process-global hubs)."""
    for _ in range(5):
        p = _run_snippet(_mn_fileread_snippet(hubs=1, n=200))
        assert p.returncode == 0 and "PASS" in p.stdout, (
            "rc=%d\n--- stdout ---\n%s\n--- stderr ---\n%s" % (
                p.returncode, p.stdout, p.stderr))


def _mn_concurrent_init_snippet(hubs, n):
    """Like _mn_fileread_snippet but WITHOUT a prior single-threaded
    iouring_available() call, so the fibers are the FIRST io_uring users
    and several hubs race the lazy ring init.  Regression for the multi-hub
    "lost completion" hang: concurrent runloom_iouring_available() first-callers
    each ran lazy_init -> multiple io_uring_setup() rings raced the shared
    ring-state pointers, so submits scribbled over each other's SQE slots and
    those ops never completed.  (Tests that pre-call iouring_available() on the
    main thread mask this -- so this snippet deliberately does NOT.)"""
    code = r'''
import sys; sys.path.insert(0, __SRCPATH__)
import os, tempfile
import runloom_c
# NB: NO runloom_c.iouring_available() here -- the fibers below are the
# first io_uring users, exercising concurrent lazy init across hubs.
N = __N__; H = __H__
paths, fds, expected, results = [], [], [], [None] * N
for i in range(N):
    content = ("ci-%04d-" % i).encode() * 32
    p = tempfile.mktemp()
    with open(p, "wb") as f:
        f.write(content)
    paths.append(p); expected.append(content)
    fds.append(os.open(p, os.O_RDONLY))

def mk(i):
    def w():
        size = len(expected[i])
        buf = bytearray(size)
        m = runloom_c.file_read(fds[i], buf, size, 0)
        results[i] = bytes(buf[:m])
    return w

runloom_c.mn_init(H)
for i in range(N):
    runloom_c.mn_go(mk(i))
runloom_c.mn_run()
runloom_c.mn_fini()
for fd in fds: os.close(fd)
for p in paths: os.unlink(p)
bad = [i for i in range(N) if results[i] != expected[i]]
print("PASS" if not bad else ("FAIL cross/missed: %r" % bad[:10]))
'''
    return (code.replace("__SRCPATH__", repr(os.path.join(REPO, "src")))
                .replace("__N__", str(n)).replace("__H__", str(hubs)))


def _mn_fileread_gc_snippet(hubs, n):
    """file_read on PIPES (forced-async: the read can't complete until a peer
    writes) under the M:N scheduler, with a concurrent thread hammering
    gc.collect() (a free-threaded stop-the-world) and a staggered feeder.
    Regression for the STW deadlock: the old hub spin-drain blocked in
    io_uring_enter while holding its tstate ATTACHED, so a GC stop-the-world
    could never complete (the feeder that would finish the read is frozen at
    the STW barrier) -- a hard hang.  The park-not-spin rework drops the tstate
    by yielding, so STW proceeds and the read completes."""
    code = r'''
import sys; sys.path.insert(0, __SRCPATH__)
import os, threading, time, gc
import runloom_c
N = __N__; H = __H__
rfds, wfds, results = [], [], [None] * N
for i in range(N):
    r, w = os.pipe(); rfds.append(r); wfds.append(w)
PAYLOAD = b"gc-stw-payload-0123456789abcdef "  # 32 bytes
stop = [False]

def mk(i):
    def w():
        buf = bytearray(len(PAYLOAD))
        m = runloom_c.file_read(rfds[i], buf, len(PAYLOAD), 0)
        results[i] = bytes(buf[:m])
    return w

def feeder():
    time.sleep(0.03)
    for w in wfds:
        try: os.write(w, PAYLOAD)
        except OSError: pass
        time.sleep(0.003)

def gcer():
    for _ in range(8):
        if stop[0]: break
        gc.collect(); time.sleep(0.01)

gt = threading.Thread(target=gcer, daemon=True); gt.start()
t = threading.Thread(target=feeder); t.start()
runloom_c.mn_init(H)
for i in range(N): runloom_c.mn_go(mk(i))
runloom_c.mn_run()
runloom_c.mn_fini()
stop[0] = True; t.join()
for fd in rfds + wfds:
    try: os.close(fd)
    except OSError: pass
bad = [i for i in range(N) if results[i] != PAYLOAD]
print("PASS" if not bad else ("FAIL missed: %d/%d %r" % (len(bad), N, bad[:8])))
'''
    return (code.replace("__SRCPATH__", repr(os.path.join(REPO, "src")))
                .replace("__N__", str(n)).replace("__H__", str(hubs)))


def test_mn_iouring_fileread_multi_hub():
    """Multi-hub file_read with CONCURRENT lazy ring init (no pre-init on the
    main thread).  Regression for the lost-completion hang: concurrent
    first-callers raced lazy_init, corrupting the shared ring so some ops never
    completed.  Fixed by serializing lazy_init under sub_lock.  Repeated in
    fresh subprocesses (mn_init + lazy init are process-global, so each run is
    a fresh concurrent-init race).  A hang shows up as a subprocess timeout."""
    for _ in range(6):
        p = _run_snippet(_mn_concurrent_init_snippet(hubs=4, n=64), timeout=30)
        assert p.returncode == 0 and "PASS" in p.stdout, (
            "rc=%d\n--- stdout ---\n%s\n--- stderr ---\n%s" % (
                p.returncode, p.stdout, p.stderr))


def test_mn_iouring_fileread_under_gc():
    """Multi-hub forced-async file_read under a concurrent GC stop-the-world.
    Regression for the deadlock where the hub spin-drain held its tstate across
    a blocking io_uring_enter, so a stop-the-world (whose unblocking needs the
    frozen feeder thread) could never complete.  Fixed by parking (yielding the
    tstate) instead of spin-draining.  A hang = subprocess timeout."""
    for _ in range(6):
        p = _run_snippet(_mn_fileread_gc_snippet(hubs=4, n=12), timeout=30)
        assert p.returncode == 0 and "PASS" in p.stdout, (
            "rc=%d\n--- stdout ---\n%s\n--- stderr ---\n%s" % (
                p.returncode, p.stdout, p.stderr))


def _mn_sockpair_recv_gc_snippet(hubs, n):
    """N pre-connected socketpairs (NO listen/accept/connect -- isolates the
    recv path) recv'd by fibers across `hubs` M:N hubs, with a FEEDER
    thread writing the peer ends STAGGERED (so each recv genuinely BLOCKS until
    data arrives) and a concurrent thread hammering gc.collect().  This is the
    socket analogue of the forced-async file_read+feeder test: it reproduces
    the multishot-recv (runloom_iouring_ms_recv, the DEFAULT TCPConn.recv) STW
    deadlock -- the hub recv spin-drained holding its tstate, so a GC stop-the-
    world whose unblocking needs the (frozen) feeder could never complete.
    Fixed by parking instead of spin-draining."""
    code = r'''
import sys; sys.path.insert(0, __SRCPATH__)
import os, socket, threading, time, gc
import runloom_c

N = __N__; H = __H__
results = [None] * N
stop = [False]
peer_fds = []   # the feeder-side raw fd per pair
recv_fds = []   # the fiber-side raw fd per pair
PAYLOAD = b"ms-recv-payload-0123456789abcdef"   # 32 bytes
for i in range(N):
    a, b = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    b.setblocking(False)
    pa, rb = a.fileno(), b.fileno()
    a.detach(); b.detach()
    peer_fds.append(pa); recv_fds.append(rb)

def mk_server(i):
    def h():
        conn = runloom_c.TCPConn(recv_fds[i])
        results[i] = conn.recv(64)
        conn.close()
    return h

def feeder():
    time.sleep(0.03)
    for fd in peer_fds:
        try: os.write(fd, PAYLOAD)
        except OSError: pass
        time.sleep(0.003)

def gcer():
    for _ in range(25):
        if stop[0]: break
        gc.collect(); time.sleep(0.005)

threading.Thread(target=gcer, daemon=True).start()
t = threading.Thread(target=feeder); t.start()
runloom_c.mn_init(H)
for i in range(N):
    runloom_c.mn_go(mk_server(i))
runloom_c.mn_run()
runloom_c.mn_fini()
stop[0] = True; t.join()
for fd in peer_fds:
    try: os.close(fd)
    except OSError: pass
bad = [i for i in range(N) if results[i] != PAYLOAD]
print("PASS" if not bad else ("FAIL missed: %d/%d %r" % (len(bad), N, bad[:8])))
'''
    return (code.replace("__SRCPATH__", repr(os.path.join(REPO, "src")))
                .replace("__N__", str(n)).replace("__H__", str(hubs)))


def test_mn_iouring_sockpair_recv_under_gc():
    """GUARD: multi-hub blocking socket recv under a concurrent GC
    stop-the-world, on pre-connected socketpairs fed by a staggered writer
    thread (isolates recv from listen/accept/connect).  Exercises the socket
    io_uring recv paths -- multishot (runloom_iouring_ms_recv) and single-shot
    per-hub-ring (runloom_iouring_ring_recv) -- which were hardened to PARK (and
    a per-op wait handshake) instead of spin-draining holding the tstate /
    inline-waking a not-yet-parked submitter, mirroring the file_read fix.

    NOTE: unlike file_read, the socket spin-drain was NOT reproducibly
    deadlock-prone on pristine here -- TCPConn recv is threshold-gated with a
    netpoll-park fallback, and the io_uring sub-paths either already park or get
    enough CQE traffic to keep tstate-holds short.  So this is a regression
    GUARD on the parked socket recv path (catches a wake/park regression =
    subprocess timeout), not a pristine-fails teeth-proof like the file_read
    tests above."""
    for _ in range(6):
        p = _run_snippet(_mn_sockpair_recv_gc_snippet(hubs=4, n=16), timeout=30)
        assert p.returncode == 0 and "PASS" in p.stdout, (
            "rc=%d\n--- stdout ---\n%s\n--- stderr ---\n%s" % (
                p.returncode, p.stdout, p.stderr))
