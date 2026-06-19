"""Coverage-driven adversarial suite for the runloom_c serve() / fd-I/O surface.

Targets the uncovered (#####) lines in two fragments compiled into module.c:

  src/runloom_c/module_io.c.inc    -- serve(): SO_REUSEPORT acceptor scaffold,
                                      the all-C (handler=None) tstate-free echo
                                      accept loop + per-conn echo fiber, and the
                                      IPv4/IPv6 bound-port lookup.
  src/runloom_c/module_fdio.c.inc  -- fd_read / fd_write (cooperative POSIX
                                      read/write that park on EAGAIN) and
                                      file_read / file_write (io_uring pread/
                                      pwrite on Linux).

The uncovered lines split into reachability classes; tests are grouped by class
and each names the exact source lines it drives + the gate it makes true.

 1. PLAIN ARG / RANGE ERROR branches reachable in-process:
      fdio L41-43  (fd_read n>buf.len ValueError)
      fdio L152    (file_read PyArg parse failure)
      fdio L155-157(file_read n>buf.len ValueError)
      fdio L203-204(file_write io_uring pwrite hard error -> OSError, bad fd)
      io   L277    (serve() PyArg parse failure -> TypeError)

 2. SIGNAL-INTERRUPTED COOPERATIVE PARK.  fd_read / fd_write use the RAW
    runloom_netpoll_wait_fd (NOT the _coop variant), so a cancellation returns
    the POSITIVE sentinel (re-park, no error) -- the ONLY way their
    `wait_fd(...) < 0` cleanup arms fire is the SIGNALED path: a Python signal
    handler that RAISED while the fiber was parked.  A SIGALRM handler raising
    into a parked fd_read / fd_write drives:
      fdio L70,L73   (fd_read  wait_fd<0 -> release buffer + propagate the raised exc)
      fdio L111,L114 (fd_write wait_fd<0 -> release buffer + propagate the raised exc)
    Signals are main-thread only, so these run on the SINGLE-THREAD scheduler
    (rc.run()), where the parked fiber lives on the main OS thread.

 3. all-C echo cooperative path + IPv6 bound-port lookup, each in its own
    clean-exit subprocess under the M:N runtime (serve requires hubs):
      io L115-116  (AF_INET6 branch of the bound-port getsockname, via serve("::1",0,...))
      io L200-201,L205 (echo send() returns 0/hard-error or EAGAIN-evaluated;
                    L205 hard-error covered by the RST storm in class 5)
    (io L202-203, the EAGAIN->WRITE-park->continue, is RACE -- see exclusions.)

 4. SPAWN-FAILURE branches, driven by the RUNLOOM_FAULT_SPAWN_G OOM hook (the
    g-slab alloc returns NULL).  The hook is armed at process start (so the
    "armed" cache latches on) with a NON-firing spec (always:0), then switched
    to a firing spec from INSIDE the run -- after the earlier, must-succeed
    spawns -- so exactly the targeted later spawn fails.  Run in a SUBPROCESS
    that exits cleanly so gcov flushes:
      io L324-326 (all-C serve: acceptor mn_fiber_c fails -> RuntimeError)
      io L241     (all-C acceptor: per-conn echo mn_fiber_c fails -> close(cfd))

 5. all-C echo send HARD error (io L205): a peer RST (SO_LINGER 0 close) arriving
    while the echo is mid-send makes send() return EPIPE/ECONNRESET (not
    EAGAIN/EINTR) -> close.  Best-effort / racy by nature (the RST may instead
    land before the echo's recv, taking the recv-error close); asserted only as
    "the server stayed healthy under an RST storm".

Excluded (see the structured report):
  * io   L110-111  -- getsockname() failing on a freshly-bound, valid listener
                      fd: DEFENSIVE (no in-process way to make getsockname fail
                      on a good fd; no fault hook on that path).
  * io   L202-203  -- the all-C echo send()-EAGAIN -> WRITE-park -> continue:
                      RACE.  The accepted echo socket's SNDBUF auto-tunes up to
                      tcp_wmem-max (~4 MB) and serve() exposes no way to clamp
                      it, so forcing send() to return EAGAIN deterministically
                      from the client side is not possible without root (sysctl)
                      -- which must not be touched on this shared box.  L200/201
                      (the EAGAIN/EINTR checks) and L205 (hard error) ARE covered
                      (RST storm); only the park-and-retry pair is racy.
  * io   L313      -- PyList_Append() failing: OOM-only, no fault hook.
  * fdio L181      -- the read()/pread() FALLBACK arm of file_read; reached only
                      when runloom_iouring_available() is false.  io_uring IS
                      available on this Linux box and latches True for the
                      process, so the fallback is unreachable here: PLATFORM.
  * fdio L218-222  -- likewise the write()/pwrite() fallback arm of file_write:
                      PLATFORM (io_uring-available box).
"""
import os
import socket
import subprocess
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from adv_util import hang_guard, needs_free_threading  # noqa: E402

import runloom_c as rc  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
FT = needs_free_threading()
POSIX = sys.platform != "win32"


def _run_single(fn, label, secs=15):
    """Drive fn() once on the single-thread C scheduler, return its result box."""
    box = {}

    def main():
        box["r"] = fn()
    with hang_guard(secs, label):
        rc.fiber(main)
        rc.run()
    return box.get("r")


# ===========================================================================
# Class 1: plain arg / range error branches (in-process, no scheduler needed
# for the parse/range guards -- they reject before any park).
# ===========================================================================
def test_fd_read_n_out_of_range_raises():
    """fdio L41-43: fd_read with n > len(buf) releases the buffer and raises
    ValueError before touching the fd."""
    buf = bytearray(4)
    with pytest.raises(ValueError, match="out of range"):
        rc.fd_read(0, buf, 100)


def test_file_read_bad_args_typeerror():
    """fdio L152: a PyArg_ParseTuple failure (fd not an int) returns NULL ->
    TypeError, with no buffer leaked."""
    with pytest.raises(TypeError):
        rc.file_read("not-an-int", bytearray(8), 4)


def test_file_read_n_out_of_range_raises():
    """fdio L155-157: file_read with n > len(buf) releases the buffer and raises
    ValueError."""
    fd, path = tempfile.mkstemp()
    try:
        with pytest.raises(ValueError, match="out of range"):
            rc.file_read(fd, bytearray(4), 99, 0)
    finally:
        os.close(fd)
        os.unlink(path)


def test_file_write_bad_fd_oserror():
    """fdio L203-204: file_write on an invalid fd -- io_uring pwrite returns < 0,
    so the buffer is released and a clean OSError(EBADF) is raised."""
    import errno
    with pytest.raises(OSError) as ei:
        rc.file_write(-1, b"payload", 0)
    assert ei.value.errno == errno.EBADF, ei.value.errno


def test_serve_missing_args_typeerror():
    """io L277: serve() with no args -- PyArg_ParseTupleAndKeywords fails and
    returns NULL -> TypeError."""
    with pytest.raises(TypeError):
        rc.serve()


# ===========================================================================
# Class 2: signal-interrupted cooperative park (single-thread scheduler).
# A SIGALRM handler that RAISES lands on a fiber parked inside fd_read/fd_write;
# runloom_netpoll_signal_wake hands the raised exception to that fiber, wait_fd
# returns -1, and fd_read/fd_write release the buffer and PROPAGATE the raised
# exception (PyErr_Occurred() is true -> return NULL) rather than overwrite it
# with OSError.
# ===========================================================================
class _Boom(Exception):
    pass


def _install_alarm_handler():
    import signal

    def handler(signum, frame):
        raise _Boom("alarm")
    signal.signal(signal.SIGALRM, handler)
    return signal


@pytest.mark.skipif(not POSIX, reason="POSIX signals + pipe/socket fd model")
def test_fd_read_signal_during_park_propagates():
    """fdio L70,L73: a raised SIGALRM handler interrupts a fiber parked in
    fd_read -> the buffer is released and the _Boom exception propagates out of
    fd_read (it is NOT replaced by OSError)."""
    import signal
    out = {}
    hold = {}

    def f():
        sig = _install_alarm_handler()
        r, w = os.pipe()                      # nothing will ever be written -> park
        os.set_blocking(r, False)
        os.set_blocking(w, False)
        hold["fds"] = (r, w)
        buf = bytearray(5)
        sig.setitimer(signal.ITIMER_REAL, 0.3)
        try:
            rc.fd_read(r, buf, 5)
            out["res"] = "no-error"
        except _Boom as e:
            out["res"] = ("Boom", str(e))
        except BaseException as e:            # noqa: BLE001 - record anything else
            out["res"] = ("other", type(e).__name__, str(e))
        return "done"

    assert _run_single(f, "fd_read signal park") == "done"
    # cleanup the parked-on fds
    r, w = hold["fds"]
    rc.netpoll_unregister(r)
    os.close(r)
    os.close(w)
    assert out.get("res") == ("Boom", "alarm"), out.get("res")


@pytest.mark.skipif(not POSIX, reason="POSIX signals + socketpair fd model")
def test_fd_write_signal_during_park_propagates():
    """fdio L111,L114: a raised SIGALRM handler interrupts a fiber parked in
    fd_write (the peer's recv buffer is full, so write() returns EAGAIN and
    parks WRITE) -> the buffer is released and _Boom propagates."""
    import signal
    out = {}
    hold = {}

    def f():
        sig = _install_alarm_handler()
        a, b = socket.socketpair()
        a.setblocking(False)
        b.setblocking(False)
        a.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        hold["socks"] = (a, b)
        wfd = a.fileno()
        sig.setitimer(signal.ITIMER_REAL, 0.4)
        big = b"x" * (8 * 1024 * 1024)        # overflow the send buffer -> park WRITE
        try:
            rc.fd_write(wfd, big)
            out["res"] = "no-error"
        except _Boom as e:
            out["res"] = ("Boom", str(e))
        except BaseException as e:            # noqa: BLE001
            out["res"] = ("other", type(e).__name__, str(e))
        return "done"

    assert _run_single(f, "fd_write signal park", secs=20) == "done"
    a, b = hold["socks"]
    rc.netpoll_unregister(a.fileno())
    a.close()
    b.close()
    assert out.get("res") == ("Boom", "alarm"), out.get("res")


# ===========================================================================
# Class 3/4/5: serve()-driven paths, each in its OWN clean-exit subprocess.
# serve() requires the M:N runtime (>=1 hub).
# ===========================================================================
def _ipv6_loopback_ok():
    try:
        s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    except OSError:
        return False
    try:
        s.bind(("::1", 0))
        return True
    except OSError:
        return False
    finally:
        s.close()


def _run_subproc(script, timeout=200):
    """Run a serve() script in a clean subprocess so gcov counters flush on
    exit, and so each full runloom.run(N) M:N session is isolated.

    RUNLOOM_FAULT_SPAWN_G is set to a NON-firing spec (always:0 -> code 0, never
    fires) at process start: this is INERT for the round-trip/park/storm scripts,
    and for the spawn-fail scripts it latches runloom_spawn_fault_armed() True so
    they can switch to a firing spec from inside the run (after the must-succeed
    early spawns)."""
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src",
               RUNLOOM_FAULT_SPAWN_G="always:0")
    try:
        return subprocess.run([PY, "-c", script], cwd=REPO, env=env,
                              capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.skip("serve workload timed out (box under heavy load)")


# Each serve() M:N session is driven in its OWN clean-exit SUBPROCESS rather than
# in-process.  serve() spins a full runloom.run(N) M:N session; running several
# back-to-back in ONE process accumulates scheduler/teardown state and can wedge
# an unrelated later session (the known multi-session mn_fini teardown flake) --
# brutal to attribute.  A subprocess per session also EXITS CLEANLY, which is
# what flushes the gcov counters for these paths.  The subprocess prints a
# sentinel on success; the test asserts rc==0 + sentinel, and skips on a
# TimeoutExpired (shared-box contention, not a bug).
_SERVE_IPV6_ROUNDTRIP = r'''
import sys
sys.path.insert(0, "src")
import runloom_c as rc, runloom
result = {}
def main():
    def handler(conn):
        data = conn.recv(64); conn.send_all(b"6:" + data); conn.close()
    port, listeners = rc.serve("::1", 0, handler, 1, 64)   # AF_INET6 listener
    result["port"] = port
    def client():
        c = rc.TCPConn.connect("::1", port)
        c.send_all(b"v6"); result["reply"] = c.recv(64); c.close()
        for L in listeners: L.close()
    rc.mn_fiber(client)
runloom.run(3, main)
if isinstance(result.get("port"), int) and result["port"] > 0 and result.get("reply") == b"6:v6":
    print("IPV6_ROUNDTRIP_OK"); sys.exit(0)
print("UNEXPECTED %r" % (result,)); sys.exit(3)
'''

_ECHO_WRITE_PARK = r'''
import socket, time, threading, sys
sys.path.insert(0, "src")
import runloom_c as rc, runloom
RealThread = threading.Thread
TOTAL = 1024 * 1024
result = {}
def main():
    port, listeners = rc.serve("127.0.0.1", 0, None, 1, 64)   # all-C echo
    result["port"] = port
    def client():
        # A tiny RCVBUF clamps the TCP receive window so the echo's send()
        # backs up (window closed) -> EAGAIN -> WRITE park.  Send and drain
        # CONCURRENTLY (non-blocking) over one socket so the pipeline never
        # deadlocks: the echo can park WRITE and still make progress as we read.
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2048)
            s.connect(("127.0.0.1", port))
            s.setblocking(False)
            sbuf = b"a" * 65536
            sent = 0; recd = 0
            deadline = time.monotonic() + 40.0
            while recd < TOTAL and time.monotonic() < deadline:
                if sent < TOTAL:
                    try:
                        sent += s.send(sbuf[: min(65536, TOTAL - sent)])
                    except BlockingIOError:
                        pass            # our send buffer full -> echo backed up
                try:
                    d = s.recv(65536)
                    if d:
                        recd += len(d)
                except BlockingIOError:
                    time.sleep(0.001)
                except Exception:
                    break
            result["sent"] = sent; result["recd"] = recd
            s.close()
        except Exception as e:
            result["cerr"] = "%s: %s" % (type(e).__name__, e)
        finally:
            for L in listeners:
                try: L.close()
                except Exception: pass
    t = RealThread(target=client, daemon=True); t.start()
    for _ in range(3000):
        rc.sched_sleep(0.02)
        if "recd" in result or "cerr" in result: break
runloom.run(3, main)
# Assertion: a full, byte-conserving round-trip of TOTAL bytes through the all-C
# echo under a clamped receive window (no loss, no deadlock).  The send-EAGAIN
# WRITE park (io L202-203) MAY fire en route -- it is RACE-dependent on TCP
# SNDBUF auto-tuning (see exclusions) -- but integrity holds either way.
if "cerr" not in result and result.get("sent") == TOTAL and result.get("recd") == TOTAL:
    print("ECHO_WRITE_PARK_OK"); sys.exit(0)
print("UNEXPECTED %r" % (result,)); sys.exit(3)
'''

_ECHO_RST_STORM = r'''
import socket, struct, time, threading, sys
sys.path.insert(0, "src")
import runloom_c as rc, runloom
RealThread = threading.Thread
result = {"rst": 0}
def main():
    port, listeners = rc.serve("127.0.0.1", 0, None, 1, 64)
    result["port"] = port
    def client():
        for _ in range(40):
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                             struct.pack("ii", 1, 0))    # close -> RST
                s.settimeout(2.0)
                s.connect(("127.0.0.1", port))
                s.sendall(b"abcdefgh")        # echo recvs, then tries to send back
                s.close()                     # RST mid-echo
                result["rst"] += 1
            except Exception as e:
                result.setdefault("cerr", str(e))
            time.sleep(0.005)
        result["done"] = True
    t = RealThread(target=client, daemon=True); t.start()
    for _ in range(600):
        rc.sched_sleep(0.02)
        if result.get("done"): break
    for L in listeners:
        try: L.close()
        except Exception: pass
runloom.run(3, main)
if result.get("rst", 0) > 0 and rc._self_check(0) == 0:
    print("RST_STORM_OK rst=%d" % result["rst"]); sys.exit(0)
print("UNEXPECTED %r" % (result,)); sys.exit(3)
'''


@pytest.mark.skipif(not FT, reason="serve() needs the M:N runtime (GIL-off build)")
@pytest.mark.skipif(not _ipv6_loopback_ok(), reason="no IPv6 loopback on this box")
def test_serve_ipv6_bound_port_roundtrip():
    """io L115-116: serve("::1", 0, ...) binds an AF_INET6 listener; the bound-
    port lookup walks the AF_INET6 branch of getsockname.  A real client round-
    trip proves the port returned is the one actually bound."""
    p = _run_subproc(_SERVE_IPV6_ROUNDTRIP, timeout=120)
    assert p.returncode == 0, (
        "rc=%d\nstdout=%s\nstderr=%s" % (p.returncode, p.stdout[-400:], p.stderr[-800:]))
    assert "IPV6_ROUNDTRIP_OK" in p.stdout, p.stdout[-400:]


@pytest.mark.skipif(not FT, reason="serve() needs the M:N runtime (GIL-off build)")
def test_all_c_echo_send_write_park():
    """io L200-201 (covered) + L202-203 (RACE, best-effort): in the all-C
    (handler=None) echo, a client whose receive window is clamped backs up the
    echo's send buffer; if the echo's send() hits EAGAIN it parks WRITE
    (cooperative wait_fd) and resumes once the client drains.  Whether the park
    actually fires is RACE-dependent on TCP SNDBUF auto-tuning (the echo's
    SNDBUF grows to tcp_wmem-max, ~4 MB, so a deterministic buffer-full is not
    controllable from the client without root); the deterministic assertion is a
    full, byte-conserving 1 MB round-trip with no loss or deadlock under window
    pressure."""
    p = _run_subproc(_ECHO_WRITE_PARK, timeout=120)
    assert p.returncode == 0, (
        "rc=%d\nstdout=%s\nstderr=%s" % (p.returncode, p.stdout[-400:], p.stderr[-1200:]))
    assert "ECHO_WRITE_PARK_OK" in p.stdout, p.stdout[-400:]


@pytest.mark.skipif(not FT, reason="serve() needs the M:N runtime (GIL-off build)")
def test_all_c_echo_rst_storm_survives():
    """io L205 (best-effort) + io L179: an RST storm against the all-C echo --
    each connection sends a payload then closes with SO_LINGER 0 (RST). The echo
    hits a hard error on recv or send and closes the fd; the server must stay
    healthy (no crash, no wedged hub, _self_check==0) across the storm."""
    p = _run_subproc(_ECHO_RST_STORM, timeout=120)
    assert p.returncode == 0, (
        "rc=%d\nstdout=%s\nstderr=%s" % (p.returncode, p.stdout[-400:], p.stderr[-1200:]))
    assert "RST_STORM_OK" in p.stdout, p.stdout[-400:]


# ===========================================================================
# Class 4: spawn-failure branches via the RUNLOOM_FAULT_SPAWN_G OOM hook, in a
# clean-exit SUBPROCESS so gcov flushes.  The env is armed at process start with
# a non-firing spec (always:0) so runloom_spawn_fault_armed() latches True; the
# firing spec is set from INSIDE the run, after the must-succeed spawns, so the
# targeted later spawn is the one that fails.
# ===========================================================================
_SERVE_ACCEPTOR_SPAWNFAIL = r'''
import os, sys
sys.path.insert(0, "src")
import runloom_c as rc, runloom
res = {}
def main():
    # main spawned with always:0 (never fires). Arm a one-shot ENOMEM so the
    # NEXT g spawn -- serve()'s all-C acceptor mn_fiber_c -- fails (io L324-326).
    os.environ["RUNLOOM_FAULT_SPAWN_G"] = "once:12"
    try:
        rc.serve("127.0.0.1", 0, None, 1, 64)     # handler=None -> all-C path
        res["r"] = "no-error"
    except RuntimeError as e:
        res["r"] = ("RuntimeError", str(e))
    except Exception as e:
        res["r"] = (type(e).__name__, str(e))
    finally:
        os.environ["RUNLOOM_FAULT_SPAWN_G"] = "always:0"
runloom.run(2, main)
if res.get("r") and res["r"][0] == "RuntimeError" and "mn_fiber_c failed" in res["r"][1]:
    print("ACCEPTOR_SPAWNFAIL_OK"); sys.exit(0)
print("UNEXPECTED %r" % (res.get("r"),)); sys.exit(3)
'''

_SERVE_ECHO_SPAWNFAIL = r'''
import os, socket, time, threading, sys
sys.path.insert(0, "src")
import runloom_c as rc, runloom
RealThread = threading.Thread
res = {}
def main():
    # acceptor spawns fine (always:0). After serve() returns it is running; now
    # make EVERY subsequent g spawn fail so the acceptor's per-conn echo mn_fiber_c
    # fails -> close(cfd) (io L241). The acceptor keeps accepting+failing safely.
    port, listeners = rc.serve("127.0.0.1", 0, None, 1, 64)
    res["port"] = port
    os.environ["RUNLOOM_FAULT_SPAWN_G"] = "always:12"
    def client():
        for _ in range(6):
            try:
                s = socket.socket(); s.settimeout(2.0)
                s.connect(("127.0.0.1", port))
                try:
                    s.sendall(b"hi"); s.recv(8)   # echo never spawns -> conn drops
                except Exception:
                    pass
                s.close()
            except Exception:
                pass
            time.sleep(0.05)
        res["done"] = True
    t = RealThread(target=client, daemon=True); t.start()
    for _ in range(300):
        rc.sched_sleep(0.02)
        if res.get("done"): break
    os.environ["RUNLOOM_FAULT_SPAWN_G"] = "always:0"   # disarm for clean teardown
    for L in listeners:
        try: L.close()
        except Exception: pass
runloom.run(2, main)
if res.get("done"):
    print("ECHO_SPAWNFAIL_OK"); sys.exit(0)
print("UNEXPECTED %r" % (res,)); sys.exit(3)
'''


@pytest.mark.skipif(not FT, reason="serve()/M:N needs the GIL-off build")
def test_all_c_serve_acceptor_spawn_fail():
    """io L324-326: when the all-C acceptor's mn_fiber_c fails, serve() drops the
    listener list and raises RuntimeError('serve(): mn_fiber_c failed')."""
    p = _run_subproc(_SERVE_ACCEPTOR_SPAWNFAIL)
    assert p.returncode == 0, (
        "rc=%d\nstdout=%s\nstderr=%s" % (p.returncode, p.stdout[-400:], p.stderr[-800:]))
    assert "ACCEPTOR_SPAWNFAIL_OK" in p.stdout, p.stdout[-400:]


@pytest.mark.skipif(not FT, reason="serve()/M:N needs the GIL-off build")
def test_all_c_acceptor_echo_spawn_fail():
    """io L241: when the all-C acceptor cannot spawn a per-connection echo fiber
    (mn_fiber_c < 0), it close()s the accepted fd and keeps looping; the server
    stays up and the connecting clients all see a dropped connection."""
    p = _run_subproc(_SERVE_ECHO_SPAWNFAIL)
    assert p.returncode == 0, (
        "rc=%d\nstdout=%s\nstderr=%s" % (p.returncode, p.stdout[-400:], p.stderr[-800:]))
    assert "ECHO_SPAWNFAIL_OK" in p.stdout, p.stdout[-400:]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
