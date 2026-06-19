"""Subprocess workload for the netpoll fault-injection harness.

Run STANDALONE (not collected by pytest) under ``strace -e inject=`` by
test_netpoll_faultinject.py.  It exercises the netpoll happy path -- a
fiber parks in runloom_c.wait_fd and a feeder OS thread makes the fd
readable -- so that an error injected into the underlying epoll_wait/epoll_ctl
syscall hits a real park/wake, not a no-op.

Modes (argv[1]):
  happy   -- park, get woken by a real edge, must print "WOKE <mask>" and exit 0.
  timeout -- park on a never-ready fd with a short timeout; must return 0
             (timeout) and exit 0.  Used for the EBADF persistent-error case:
             under injection the wake never comes, so the run is dominated by
             how the pump behaves on a failing epoll_wait (spin vs backoff).

Prints a single status line and exits 0 on the expected outcome, non-zero (or
crashes, which the harness also catches) otherwise.
"""
import os
import socket
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "src"))
import runloom_c

READ = 1
TIMEOUT_MS = int(os.environ.get("RUNLOOM_FAULT_TIMEOUT_MS", "2000"))


def mode_happy():
    r, w = socket.socketpair()
    r.setblocking(False)
    got = []
    err = []

    def feeder():
        # give the fiber time to park, then produce one readable edge
        import time
        time.sleep(0.05)
        try:
            w.send(b"x")
        except OSError:
            pass

    t = threading.Thread(target=feeder, daemon=True)
    t.start()

    def waiter():
        # Catch AT the park site: an injected epoll_ctl failure surfaces here
        # as OSError, and a fiber that dies with an unhandled exception
        # leaves run() returning with an empty `got` -- indistinguishable from
        # a silent hang.  Recording the errno proves the error path is clean.
        try:
            got.append(runloom_c.wait_fd(r.fileno(), READ, TIMEOUT_MS))
        except OSError as e:
            err.append(e.errno)

    runloom_c.fiber(waiter)
    runloom_c.run()
    t.join(5)
    r.close(); w.close()
    if got and (got[0] & READ):
        print("WOKE %d" % got[0])
        return 0
    if err:
        print("OSERROR errno=%d" % err[0])   # clean syscall-error propagation
        return 42
    print("FAIL got=%r" % got)
    return 1


def mode_timeout():
    r, w = socket.socketpair()
    r.setblocking(False)
    got = []

    def waiter():
        got.append(runloom_c.wait_fd(r.fileno(), READ, TIMEOUT_MS))

    runloom_c.fiber(waiter)
    runloom_c.run()
    r.close(); w.close()
    # Either a clean timeout (0) or a raised OSError handled by the caller is
    # acceptable here; what matters to the harness is HOW MUCH CPU/how many
    # syscalls the pump burned getting here, and that we did not crash.
    print("DONE got=%r backend=%s" % (got, runloom_c.netpoll_backend()))
    return 0


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "happy"
    try:
        if mode == "happy":
            return mode_happy()
        if mode == "timeout":
            return mode_timeout()
    except OSError as e:
        # A syscall error surfaced through wait_fd as OSError -- this is the
        # CLEAN handling we want for epoll_ctl ENOMEM/EINVAL: report and exit
        # with a distinct code so the harness can tell "handled" from "crashed".
        print("OSERROR errno=%s" % e.errno)
        return 42
    print("BADMODE %r" % mode)
    return 2


if __name__ == "__main__":
    sys.exit(main())
