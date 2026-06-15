"""FINDING (2026-06-15): io_uring recv deadlocks on a backpressured transfer.

Forcing every TCPConn.recv through the opt-in io_uring recv backend
(RUNLOOM_TCPCONN_IOURING=1) DEADLOCKS a standard large loopback transfer: the
receiver parks in conn.recv() partway through a 4 MiB send and is never woken
again, so the single-thread scheduler's run() never returns.

  * DEFAULT epoll backend (RUNLOOM_TCPCONN_IOURING unset): transfers fine.
  * io_uring recv forced on: server goroutine wedges in recv() (observed at
    tests/test_tcp_scenarios.py:96, test_writealot_backpressure) -> hang.

This is the libuv `test-tcp-writealot` pattern (sender parks on write-readiness
while the receiver drains, repeatedly), so it is a normal supported operation in
default mode. The hang points to a lost-wakeup / missed-completion hole in the
io_uring recv path under backpressure (large transfer -> many recvs -> a recv
completion does not wake the parked receiver). It is LATENT: io_uring recv is
opt-in per connection and the suite runs default mode, so default users are
unaffected -- but RUNLOOM_TCPCONN_IOURING is therefore NOT a transparent drop-in
today. Corroboration: forcing the mode also failed test_adv_tcpconn and 3 cases
in test_cov95_tcp_conn.

Status: NOT fixed (deliberately deferred -- opt-in code). This file is the repro,
and the reason the io_uring-recv lines in netpoll_wake_iouring.c.inc / io_uring_l_*
cannot be driven to full coverage by a clean-exit test (see COVERAGE.md).

Run manually (it is self-bounded by a watchdog; NOT part of the default suite):
  PYTHON_GIL=0 PYTHONPATH=src python3 tests/regressions/iouring_recv_backpressure_deadlock.py          # default: OK
  PYTHON_GIL=0 PYTHONPATH=src RUNLOOM_TCPCONN_IOURING=1 \
      python3 tests/regressions/iouring_recv_backpressure_deadlock.py      # io_uring: DEADLOCK (watchdog fires)
"""
import faulthandler
import os
import socket
import sys
import zlib

sys.path.insert(0, "src")
import runloom_c

SIZE = 4 * 1024 * 1024
WATCHDOG_S = 15


def _bound_port(listener):
    fd = listener.fileno()
    sk = socket.socket(fileno=socket.dup(fd))
    port = sk.getsockname()[1]
    sk.close()
    return port


def main():
    mode = "io_uring" if os.environ.get("RUNLOOM_TCPCONN_IOURING") == "1" else "default(epoll)"
    payload = (bytes(range(256)) * ((SIZE + 255) // 256))[:SIZE]
    want_crc = zlib.crc32(payload)
    port_holder = [None]
    got = [None]

    def server():
        listener = runloom_c.TCPConn.listen("127.0.0.1", 0)
        port_holder[0] = _bound_port(listener)
        conn = listener.accept()
        crc = 0
        total = 0
        while True:
            chunk = conn.recv(65536)        # <-- wedges here under io_uring recv
            if not chunk:
                break
            crc = zlib.crc32(chunk, crc)
            total += len(chunk)
        got[0] = (total, crc)
        conn.close()
        listener.close()

    def client():
        while port_holder[0] is None:
            runloom_c.sched_yield()
        c = runloom_c.TCPConn.connect("127.0.0.1", port_holder[0])
        c.send_all(payload)
        c.close()

    # Watchdog: if the transfer deadlocks, dump every thread's stack and exit
    # non-zero rather than hang forever.
    faulthandler.dump_traceback_later(WATCHDOG_S, exit=True)
    print("[repro] backend = {0}; transferring {1} bytes ...".format(mode, SIZE))
    runloom_c.go(server)
    runloom_c.go(client)
    runloom_c.run()
    faulthandler.cancel_dump_traceback_later()

    ok = got[0] == (SIZE, want_crc)
    print("[repro] backend = {0}: {1} (got={2})".format(
        mode, "OK -- transfer completed" if ok else "WRONG RESULT", got[0]))
    sys.exit(0 if ok else 2)


if __name__ == "__main__":
    main()
