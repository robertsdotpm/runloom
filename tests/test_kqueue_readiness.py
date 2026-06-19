"""kqueue PUMP readiness-semantics branch coverage (macOS/BSD).

Drives the kqueue drain loop in src/runloom_c/netpoll_pump.c.inc:131-211 and the
dispatch in netpoll_pump_helpers.c.inc through BEHAVIOUR on real socketpairs +
runloom_c.wait_fd (the proven test_netpoll_conformance convention), single-thread
scheduler (_drive).  Multiple instances per branch.

Branch map:
  * ready-before-park: an already-ready fd returns immediately (EV_ADD level
    recheck at register, netpoll_register.c.inc:107-110 feeding the pump).
  * park-then-ready: peer makes the fd ready AFTER the fiber parks -- the pump's
    blocking kevent() collects the event (netpoll_pump.c.inc:182-204).  READ+WRITE.
  * timeout/deadline: a never-ready fd wakes only on its deadline -> 0
    (netpoll_pump.c.inc:150-153 timespec + drain_expired).
  * R|W subset: only the ready direction is returned.
  * no spurious wake on the unrequested direction.
  * drain-until-empty: many fds ready in ONE pump -> the for-loop over the kevent
    batch + the n<256 short-batch break (netpoll_pump.c.inc:190-209) drain them all.

NOTE (kqueue quirk): a plain readable+writable fd does NOT return mask 3 -- the
READ and WRITE one-shot knotes are two separate kevents and the first dispatch
unlinks the parker, so wait_fd returns whichever direction was delivered first.
Mask 3 only arises via the EV_EOF/EV_ERROR fold (covered in test_kqueue_eof_error).
So the both-directions assertions here check a non-empty SUBSET of the request.
"""
import socket
import sys

import pytest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith(("darwin", "freebsd", "openbsd", "netbsd")),
    reason="kqueue backend only")

sys.path.insert(0, "src")

import runloom_c  # noqa: E402

READ = 1
WRITE = 2


def _drive(*fibers):
    box = []

    def wrap(fn):
        def runner():
            try:
                fn()
            except BaseException as e:  # noqa: BLE001
                box.append(e)
        return runner

    for g in fibers:
        runloom_c.fiber(wrap(g))
    runloom_c.run()
    if box:
        raise box[0]


def _pair():
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)
    return a, b


def _fill_send_buffer(s):
    """Make s non-writable: shrink its send buffer and stuff it until EAGAIN."""
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
    except OSError:
        pass
    total = 0
    try:
        while total < (16 << 20):
            total += s.send(b"\0" * 65536)
    except (BlockingIOError, OSError):
        pass
    return total


def _reset_registration():
    for fd in range(3, 1024):
        try:
            runloom_c.netpoll_unregister(fd)
        except Exception:           # noqa: BLE001
            pass


@pytest.fixture(autouse=True)
def _reg_reset():
    _reset_registration()
    yield
    _reset_registration()


def test_backend_is_kqueue():
    assert runloom_c.netpoll_backend() == "kqueue"


# -- ready BEFORE park -------------------------------------------------------
@pytest.mark.parametrize("payload", [b"x", b"abcd", b"Z" * 2000],
                         ids=["1B", "4B", "2000B"])
def test_ready_before_park_read(payload):
    a, b = _pair()
    b.send(payload)                      # readable before the fiber parks
    out = []
    _drive(lambda: out.append(runloom_c.wait_fd(a.fileno(), READ, 1000)))
    assert out == [READ]
    a.close(); b.close()


@pytest.mark.parametrize("events", [WRITE, READ | WRITE], ids=["w", "rw"])
def test_ready_before_park_write(events):
    a, b = _pair()                       # born writable
    out = []
    _drive(lambda: out.append(runloom_c.wait_fd(a.fileno(), events, 1000)))
    assert out[0] & WRITE                # WRITE ready; (rw subset allowed)
    a.close(); b.close()


# -- park THEN ready (pump's blocking kevent collects the event) -------------
@pytest.mark.parametrize("n", [1, 4, 16], ids=["n1", "n4", "n16"])
def test_park_then_ready_read(n):
    pairs = [_pair() for _ in range(n)]
    out = []

    def reader(a):
        out.append(runloom_c.wait_fd(a.fileno(), READ, 2000))

    def waker():
        runloom_c.sched_yield()
        for _a, b in pairs:
            b.send(b"go")

    _drive(*([(lambda a=a: reader(a)) for a, _b in pairs] + [waker]))
    assert out == [READ] * n
    for a, b in pairs:
        a.close(); b.close()


def test_park_then_ready_write():
    a, b = _pair()
    _fill_send_buffer(a)                 # not writable now
    out = []

    def writer():
        out.append(runloom_c.wait_fd(a.fileno(), WRITE, 2000))

    def drainer():
        runloom_c.sched_yield()
        while True:
            try:
                if not b.recv(65536):
                    break
            except BlockingIOError:
                break                    # buffer drained -> a becomes writable

    _drive(writer, drainer)
    assert out and (out[0] & WRITE)
    a.close(); b.close()


# -- timeout / deadline ------------------------------------------------------
@pytest.mark.parametrize("timeout_ms", [50, 120, 250], ids=["t50", "t120", "t250"])
def test_timeout_deadline_wakes(timeout_ms):
    a, b = _pair()                       # never made readable
    out = []
    _drive(lambda: out.append(runloom_c.wait_fd(a.fileno(), READ, timeout_ms)))
    assert out == [0]                    # 0 == deadline timeout
    a.close(); b.close()


# -- R|W subset: only the ready direction returns ----------------------------
def test_rw_subset_returns_read_only():
    a, b = _pair()
    _fill_send_buffer(a)                 # WRITE not ready
    b.send(b"r")                         # READ ready
    out = []
    _drive(lambda: out.append(runloom_c.wait_fd(a.fileno(), READ | WRITE, 1500)))
    assert out and (out[0] & READ) and not (out[0] & WRITE)
    a.close(); b.close()


# -- no spurious wake on the unrequested direction ---------------------------
def test_no_spurious_wake_on_unrequested_direction():
    a, b = _pair()                       # writable, but we only ask READ
    out = []
    _drive(lambda: out.append(runloom_c.wait_fd(a.fileno(), READ, 150)))
    assert out == [0]                    # WRITE-ready must NOT wake a READ waiter
    a.close(); b.close()


# -- drain-until-empty: many fds ready in ONE pump ---------------------------
@pytest.mark.parametrize("n", [8, 64, 200], ids=["n8", "n64", "n200"])
def test_drain_many_ready_in_one_pump(n):
    pairs = [_pair() for _ in range(n)]
    woke = bytearray(n)

    def reader(i, a):
        if runloom_c.wait_fd(a.fileno(), READ, 3000) == READ:
            woke[i] = 1

    def waker():
        runloom_c.sched_yield()
        for _a, b in pairs:             # make ALL ready before the pump runs
            b.send(b"x")

    fibers = [(lambda i=i, a=a: reader(i, a)) for i, (a, _b) in enumerate(pairs)]
    _drive(*(fibers + [waker]))
    assert sum(woke) == n
    for a, b in pairs:
        a.close(); b.close()


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
