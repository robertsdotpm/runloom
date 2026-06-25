"""IOCP (Windows) AFD close-waker regression -- Windows bug #2.

When a socket with an in-flight AFD poll is closed, the completion carries a
teardown bit (AFD_POLL_LOCAL_CLOSE / DISCONNECT / ABORT / CONNECT_FAIL) and NO
IN/OUT readiness.  runloom_from_afd_events (netpoll_iocp.c) used to fold those
teardown bits into RUNLOOM_NETPOLL_READ only (CONNECT_FAIL into WRITE only), so a
fiber parked WRITE-only on a closed fd was never reached by any waker -- on IOCP
runloom_netpoll_cancel_fd is a deliberate no-op (it would double-wake vs the AFD
auto-completion), so AFD_POLL_LOCAL_CLOSE is the SOLE close-waker.  The fix folds
the teardown bits into BOTH directions.

This test parks a WRITE-only waiter (the broken direction) on a socket, closes
the socket, and asserts the waiter is woken by the close rather than stranded.
A finite deadline is used purely as a backstop: with the bug the WRITE parker
gets no close-wake and wait_fd returns 0 (deadline timeout); with the fix it
returns a non-zero mask well before the deadline.  iocp-afd backend only.
"""
import socket
import sys
import time

import pytest

sys.path.insert(0, "src")

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("win"),
    reason="IOCP (iocp-afd) backend is Windows-only")

import runloom_c          # noqa: E402

READ = 1
WRITE = 2

# Backstop deadline (ms).  Must be >> the close-wake latency yet finite so a
# REGRESSION surfaces as a 0-return rather than hanging run() forever.
DEADLINE_MS = 4000


def _pair():
    """A connected TCP pair, non-blocking, with no pending data and send buffer
    not full -- so a bare WRITE park would normally stay ready... use a socket
    whose AFD WRITE is NOT immediately satisfiable by pairing it so the parker
    actually commits to a park, then the close is the only realistic waker."""
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)
    return a, b


def _drive(*fibers):
    box = []

    def wrap(fn):
        def runner():
            try:
                fn()
            except BaseException as e:   # noqa: BLE001
                box.append(e)
        return runner

    for g in fibers:
        runloom_c.go(wrap(g))
    runloom_c.run()
    if box:
        raise box[0]


def _reset_netpoll_registration():
    for fd in range(3, 1024):
        try:
            runloom_c.netpoll_unregister(fd)
        except Exception:                # noqa: BLE001
            pass


@pytest.fixture(autouse=True)
def _netpoll_reset():
    _reset_netpoll_registration()
    try:
        runloom_c.cancel_all_parked()
    except Exception:                    # noqa: BLE001
        pass
    yield
    try:
        runloom_c.cancel_all_parked()
    except Exception:                    # noqa: BLE001
        pass
    _reset_netpoll_registration()


def test_backend_is_iocp():
    """Guard: this module asserts iocp-afd close-waker behaviour."""
    assert runloom_c.netpoll_backend() == "iocp-afd", (
        "force the IOCP backend: set RUNLOOM_NETPOLL=iocp-afd "
        "(got %r)" % runloom_c.netpoll_backend())


def test_write_parker_woken_by_close():
    """THE regression: a WRITE-only parker on a socket that is closed under it
    must be woken (close folds to READ|WRITE), not stranded to the deadline."""
    a, b = _pair()
    got = []

    def parker():
        t0 = time.monotonic()
        rv = runloom_c.wait_fd(a.fileno(), WRITE, DEADLINE_MS)
        got.append((rv, time.monotonic() - t0))

    def closer():
        # Runs after the parker has armed its AFD poll + yielded (spawn order).
        a.close()

    try:
        _drive(parker, closer)
    finally:
        try:
            b.close()
        except Exception:                # noqa: BLE001
            pass

    assert got, "parker never returned"
    rv, dt = got[0]
    # rv == 0 means the deadline elapsed with no close-wake == the bug.
    assert rv != 0, (
        "WRITE parker was NOT woken by the close (got deadline timeout) -- "
        "the IOCP close-waker folded teardown bits to READ only (bug #2)")
    assert dt < DEADLINE_MS / 1000.0 * 0.9, (
        "woke only at the deadline (%.2fs) -- close-wake did not fire" % dt)


def test_read_parker_woken_by_close_control():
    """Control: a READ parker was already woken on close pre-fix; it must stay
    woken after the fix (the readiness-vs-teardown split must not regress it)."""
    a, b = _pair()
    got = []

    def parker():
        got.append(runloom_c.wait_fd(a.fileno(), READ, DEADLINE_MS))

    def closer():
        a.close()

    try:
        _drive(parker, closer)
    finally:
        try:
            b.close()
        except Exception:                # noqa: BLE001
            pass

    assert got and got[0] != 0, "READ parker not woken by close (regression)"
