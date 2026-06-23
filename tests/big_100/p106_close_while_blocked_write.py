"""big_100 / 106 -- close while blocked in write.

Per unit: a connected socketpair with shrunk buffers.  A writer goroutine fills
its send buffer (the peer never reads) so it is genuinely blocked-in-write, then
a separate goroutine closes the writer's fd; the writer must observe the close
and exit.

Why this is shaped the way it is (all real runtime sharp edges, see FINDINGS):
  * monkey makes the cooperative send() PARK on a full buffer, so a writer that
    sendall()'d a large payload could never first signal that it is blocked.  We
    therefore fill with RAW os.write captured BEFORE monkey.patch() (the patched
    os.write is also cooperative and would park), which returns BlockingIOError
    when full -- a deterministic "now blocked-in-write" point.
  * Closing a socket does NOT reliably wake a goroutine parked in a cooperative
    send / wait_fd(WRITE) on a full buffer, and wait_fd(fd, WRITE, ceiling) does
    not even honour its timeout there -- so we cannot park-and-be-woken.  Instead
    the writer detects the close by watching fileno() go to -1 (reliable,
    bounded), which is the supported observation of a closed fd.

Stresses: cross-goroutine close of a write-blocked fd, the cooperative send /
write-arm path, fd-close observation under M:N.  Fully local (socketpair).
"""
import os
import socket

import harness
import runloom

# Capture the RAW os entry points BEFORE the harness runs monkey.patch(): the
# patched versions are cooperative (os.write parks on a full buffer instead of
# raising), which would defeat the deterministic buffer-fill below.
RAW_WRITE = os.write
RAW_SET_BLOCKING = os.set_blocking

BUFSZ = 4096
CHUNK = b"W" * 65536
POLL_CEILING = 3000             # ~3s of 1ms cooperative polls -> bounded, no hang


def writer(H, sock, ready, done):
    """Fill the send buffer (blocked-in-write), then watch for the cross-goroutine
    close (fileno -> -1).  Report through `done`: 1 = observed the close, 2 =
    timed out without seeing it."""
    try:
        fd = sock.fileno()
    except (OSError, ValueError):
        ready.send(True)
        done.send(1)
        return
    try:
        RAW_SET_BLOCKING(fd, False)
        while True:
            RAW_WRITE(fd, CHUNK)             # raw: raises BlockingIOError when full
    except BlockingIOError:
        pass                                 # buffer full -> blocked in write
    except OSError:
        ready.send(True)
        done.send(1)
        return
    ready.send(True)                         # genuinely unable to send
    for _ in range(POLL_CEILING):
        if sock.fileno() < 0:                # the closer closed our fd
            done.send(1)
            return
        runloom.sleep(0.001)                 # cooperative: frees the hub
    done.send(2)


def unit(H, wid, rng, closed, woken):
    a, b = socket.socketpair()
    for s in (a, b):
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, BUFSZ)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, BUFSZ)
        except OSError:
            pass
    ready = runloom.Chan(1)
    done = runloom.Chan(1)
    H.fiber(writer, H, a, ready, done)
    ready.recv()                             # writer filled the buffer; blocked
    runloom.sleep(0.002)
    closed[wid] += 1
    try:
        a.close()                            # cross-goroutine close of the write fd
    except OSError:
        pass
    try:
        b.close()
    except OSError:
        pass
    how, _ok = done.recv()
    if how == 1:
        woken[wid] += 1
    return True


def worker(H, wid, rng, state):
    closed, woken = state
    for _ in H.round_range():
        unit(H, wid, rng, closed, woken)
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.closed = [0] * H.funcs
    H.woken = [0] * H.funcs
    H.state = (H.closed, H.woken)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    tc = sum(H.closed)
    tw = sum(H.woken)
    H.check(tc > 0, "no writers were ever closed (test did no work)")
    H.check(tw == tc,
            "writers_observed_close {0} != writers_closed {1} (a blocked writer "
            "never saw the close)".format(tw, tc))
    H.log("writers_closed={0} writers_observed_close={1}".format(tc, tw))


if __name__ == "__main__":
    # Moderate concurrency on purpose (see p105 / FINDINGS): the close-vs-blocked-
    # write handoff plus raw-syscall socketpair()/close() churn does not scale.
    harness.main("p106_close_while_blocked_write", body, setup=setup, post=post,
                 default_funcs=150, max_funcs=300,
                 describe="fill a socketpair send buffer so a goroutine blocks in "
                          "write, then close the fd; the writer must observe it")
