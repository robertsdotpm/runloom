"""Subprocess workload for the runloom_tcp (TCPConn) fault-injection harness.

Run STANDALONE (not collected by pytest) under ``strace -e inject=`` by
test_tcp_faultinject.py.  Drives a real loopback TCPConn echo (connect / accept
/ recv / send) as two cooperative goroutines, so an error injected into one of
those syscalls hits the live non-blocking + netpoll-retry loop.

Modes (argv[1]):
  echo           -- client sends "ping", server echoes; must print "OK ping".
  connectrefused -- connect to a closed port; must surface OSError(ECONNREFUSED).

Prints one status line; exit 0 = OK, 42 = clean OSError (prints errno), other =
unexpected/crash.
"""
import os
import socket
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "src"))
import runloom_c


def _drive(*goroutines):
    box = []

    def wrap(fn):
        def runner():
            try:
                fn()
            except BaseException as e:   # noqa: BLE001 - reported to caller
                box.append(e)
        return runner

    for g in goroutines:
        runloom_c.go(wrap(g))
    runloom_c.run()
    return box


def mode_echo():
    port = [None]
    result = [None]

    def server():
        listener = runloom_c.TCPConn.listen("127.0.0.1", 0)
        port[0] = listener.fileno() and _port(listener)
        conn = listener.accept()
        data = conn.recv(1024)
        conn.send_all(data)
        conn.close()
        listener.close()

    def client():
        while port[0] is None:
            runloom_c.sched_yield()
        c = runloom_c.TCPConn.connect("127.0.0.1", port[0])
        c.send_all(b"ping")
        result[0] = c.recv(1024)
        c.close()

    errs = _drive(server, client)
    if errs:
        e = errs[0]
        if isinstance(e, OSError):
            print("OSERROR errno=%s" % e.errno)
            return 42
        print("FAIL exc=%r" % e)
        return 1
    if result[0] == b"ping":
        print("OK ping")
        return 0
    print("FAIL result=%r" % result[0])
    return 1


def _port(listener):
    # socket.dup (WSADuplicateSocket on Windows), NOT os.dup: os.dup is a CRT
    # fd op and corrupts a raw WinSock socket handle on Windows.
    s = socket.socket(fileno=socket.dup(listener.fileno()))
    try:
        return s.getsockname()[1]
    finally:
        s.detach()
        s.close()


def mode_connectrefused():
    # Find a closed port: bind+close to learn a free number, then connect.
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    dead_port = probe.getsockname()[1]
    probe.close()
    box = {}

    def client():
        try:
            runloom_c.TCPConn.connect("127.0.0.1", dead_port)
            box["unexpected"] = True
        except OSError as e:
            box["errno"] = e.errno

    _drive(client)
    if box.get("unexpected"):
        print("FAIL connect to dead port succeeded")
        return 1
    print("OSERROR errno=%s" % box.get("errno"))
    return 42


def mode_connectonly():
    # A PASSIVE listener (listen, never accept): the kernel completes the
    # 3-way handshake into the accept queue, so TCPConn.connect succeeds without
    # any server goroutine -- a connect failure thus surfaces directly instead
    # of stranding a server in accept().  Used for the connect-path injection.
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    lsock.bind(("127.0.0.1", 0))
    lsock.listen(16)
    target_port = lsock.getsockname()[1]
    box = {}

    def client():
        try:
            c = runloom_c.TCPConn.connect("127.0.0.1", target_port)
            box["ok"] = True
            c.close()
        except OSError as e:
            box["errno"] = e.errno

    _drive(client)
    lsock.close()
    if box.get("ok"):
        print("OK connect")
        return 0
    if "errno" in box:
        print("OSERROR errno=%s" % box["errno"])
        return 42
    print("FAIL box=%r" % box)
    return 1


def mode_recvonce():
    # One connected socket, a single recv with no peer data: used to inject a
    # non-retryable error (e.g. ECONNRESET) onto exactly the first recvfrom and
    # assert it surfaces as OSError.  A real server thread feeds nothing; the
    # injected error returns the recv immediately.
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    lsock.bind(("127.0.0.1", 0))
    lsock.listen(16)
    target_port = lsock.getsockname()[1]
    box = {}

    def client():
        try:
            c = runloom_c.TCPConn.connect("127.0.0.1", target_port)
            data = c.recv(1024)
            box["data"] = data
            c.close()
        except OSError as e:
            box["errno"] = e.errno

    _drive(client)
    lsock.close()
    if "errno" in box:
        print("OSERROR errno=%s" % box["errno"])
        return 42
    print("DATA len=%s" % (len(box.get("data") or b"")))
    return 0


def mode_sendonce():
    # One socket connected to a PASSIVE listener (never accepted): a single
    # send with an injected non-retryable error must surface as OSError.  No
    # peer goroutine parks, so an injected send failure cannot hang the run.
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    lsock.bind(("127.0.0.1", 0))
    lsock.listen(16)
    target_port = lsock.getsockname()[1]
    box = {}

    def client():
        try:
            c = runloom_c.TCPConn.connect("127.0.0.1", target_port)
            c.send_all(b"x" * 64)
            box["sent"] = True
            c.close()
        except OSError as e:
            box["errno"] = e.errno

    _drive(client)
    lsock.close()
    if "errno" in box:
        print("OSERROR errno=%s" % box["errno"])
        return 42
    print("SENT ok=%s" % box.get("sent"))
    return 0


def mode_acceptfail():
    # A real (raw) client fills the accept queue, then a single accept() with an
    # injected non-retryable error must surface as OSError -- no peer goroutine
    # parks, so the workload cannot hang.
    listener = runloom_c.TCPConn.listen("127.0.0.1", 0)
    target_port = _port(listener)
    raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    raw.connect(("127.0.0.1", target_port))
    box = {}

    def server():
        try:
            conn = listener.accept()
            box["ok"] = True
            conn.close()
        except OSError as e:
            box["errno"] = e.errno

    _drive(server)
    raw.close()
    listener.close()
    if "errno" in box:
        print("OSERROR errno=%s" % box["errno"])
        return 42
    if box.get("ok"):
        print("OK accept")
        return 0
    print("FAIL box=%r" % box)
    return 1


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "echo"
    dispatch = {
        "echo": mode_echo,
        "connectrefused": mode_connectrefused,
        "connectonly": mode_connectonly,
        "recvonce": mode_recvonce,
        "sendonce": mode_sendonce,
        "acceptfail": mode_acceptfail,
    }
    fn = dispatch.get(mode)
    if fn is None:
        print("BADMODE %r" % mode)
        return 2
    rc = fn()
    # The compiled-in (kqueue/Windows) fault harness sets FAULT_SITE so it can
    # confirm the injection actually fired; strace runs (Linux) leave it unset.
    site = os.environ.get("FAULT_SITE")
    if site:
        try:
            print("FAULTS=%d" % runloom_c._fault_count(site))
        except Exception:
            pass
    return rc


if __name__ == "__main__":
    sys.exit(main())
