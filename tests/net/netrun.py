"""Tiny M:N runner for the remote-internet suite (NOT the big_100 harness).

big_100/harness.py is a SCALE harness (tens of thousands of goroutines, memory
guard, sharded counters).  This suite is POLITE and small -- a few dozen probe
goroutines total against real public servers -- so it reuses only the two cheap,
load-bearing pieces: a REAL-OS-thread watchdog (a lost wake on a live remote
socket must still fail the process, not hang forever) and cooperative,
timeout-bounded socket I/O built on runloom_c.wait_fd + REAL (pre-monkey) socket
ops -- the exact pattern netutil.udp_recvfrom_timeout uses to dodge the
patched-recv spurious-wake wedge.

A program supplies TASKS = [(category, af, proto, probe_fn), ...] and calls
main().  probe_fn(io, server, timeout_ms) runs one round-trip and returns:
    ("pass",    detail)   -- an identity-matched, well-formed response
    ("env",     reason)   -- refused/timeout/all-down/not-our-transaction (SKIP)
    ("finding", detail)   -- OUR transaction came back corrupted -> real bug
or raises (any exception) -> CRASH.  The runner tries servers score-first and
STOPS a task on its first "pass" (polite).  Aggregate exit:
    PASS(0) if >=1 task passed and none produced a finding/crash;
    FINDING(1)/CRASH(2)/HANG(3) if any did;
    SKIP(77) if every task was all-ENV (the ordinary network-down night).
"""
import errno
import os
import socket
import struct
import sys
import time

# Capture REAL entry points BEFORE monkey.patch() (the watchdog needs a real
# clock + real thread; the I/O path needs the real, un-patched socket class so a
# spurious readiness signal can't wedge us in the patched recv loop).
REAL_MONO = time.monotonic
REAL_SLEEP = time.sleep
import _thread as _real_thread
_RealSocket = socket.socket

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # tests/net on path
import netlist  # noqa: E402  (verdict codes + finding emitter)

WAIT_READ = 1
WAIT_WRITE = 2


class EnvSkip(Exception):
    """This server is ENV (refused/timeout/EOF/not-our-transaction) -> try next."""


class Finding(Exception):
    """OUR transaction returned corrupted -> a real transport bug."""


# ------------------------------------------------------------- cooperative I/O
class IO(object):
    """Timeout-bounded socket helpers, cooperative under runloom.run via wait_fd.

    Uses REAL non-blocking sockets + runloom_c.wait_fd so each op parks the
    goroutine with a hard timeout and never loops on a spurious wake."""

    def __init__(self):
        import runloom_c
        self._rc = runloom_c

    def _resolve(self, host, port, family, socktype):
        try:
            ai = socket.getaddrinfo(host, port, family, socktype)
        except OSError as e:
            raise EnvSkip("getaddrinfo %s" % e)
        if not ai:
            raise EnvSkip("no addrinfo")
        return ai[0]

    def tcp_connect(self, host, port, family, timeout_ms):
        af, st, proto, _, sa = self._resolve(host, port, family, socket.SOCK_STREAM)
        s = _RealSocket(af, socket.SOCK_STREAM, proto)
        s.setblocking(False)
        try:
            err = s.connect_ex(sa)
            if err not in (0, errno.EINPROGRESS, errno.EWOULDBLOCK):
                raise EnvSkip("connect errno %d" % err)
            if err != 0:
                if not (self._rc.wait_fd(s.fileno(), WAIT_WRITE, timeout_ms) & WAIT_WRITE):
                    raise EnvSkip("connect timeout")
                so = s.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                if so != 0:
                    raise EnvSkip("connect SO_ERROR %d" % so)
            return s
        except BaseException:
            try:
                s.close()
            except OSError:
                pass
            raise

    def send_all(self, s, data, timeout_ms):
        view = memoryview(data)
        while view:
            try:
                n = s.send(view)
                view = view[n:]
            except (BlockingIOError, InterruptedError):
                if not (self._rc.wait_fd(s.fileno(), WAIT_WRITE, timeout_ms) & WAIT_WRITE):
                    raise EnvSkip("send timeout")
            except OSError as e:
                raise EnvSkip("send %s" % e)

    def recv_n(self, s, n, timeout_ms):
        """Read EXACTLY n bytes or raise EnvSkip (timeout / EOF / reset)."""
        buf = bytearray()
        while len(buf) < n:
            if not (self._rc.wait_fd(s.fileno(), WAIT_READ, timeout_ms) & WAIT_READ):
                raise EnvSkip("recv timeout")
            try:
                chunk = s.recv(n - len(buf))
            except (BlockingIOError, InterruptedError):
                continue
            except OSError as e:
                raise EnvSkip("recv %s" % e)
            if not chunk:
                raise EnvSkip("eof after %d/%d bytes" % (len(buf), n))
            buf += chunk
        return bytes(buf)

    def udp_roundtrip(self, host, port, family, request, maxlen, timeout_ms):
        af, st, proto, _, sa = self._resolve(host, port, family, socket.SOCK_DGRAM)
        s = _RealSocket(af, socket.SOCK_DGRAM, proto)
        s.setblocking(False)
        try:
            try:
                s.sendto(request, sa)
            except OSError as e:
                raise EnvSkip("sendto %s" % e)
            if not (self._rc.wait_fd(s.fileno(), WAIT_READ, timeout_ms) & WAIT_READ):
                raise EnvSkip("recv timeout")
            try:
                data, _addr = s.recvfrom(maxlen)
            except (BlockingIOError, InterruptedError):
                raise EnvSkip("spurious wake")
            except OSError as e:
                raise EnvSkip("recvfrom %s" % e)
            return data
        finally:
            try:
                s.close()
            except OSError:
                pass


# --------------------------------------------------------------- args + watchdog
def _parse_argv(argv):
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--hubs", type=int, default=8)
    ap.add_argument("--top", type=int, default=32)
    ap.add_argument("--timeout", type=float, default=3.0, help="per-probe seconds")
    ap.add_argument("--report-dir", default=None)
    ap.add_argument("--hang-timeout", type=float, default=None)
    return ap.parse_args(argv)


class _State(object):
    def __init__(self):
        self.completed = 0            # probes that returned (any verdict)
        self.outstanding = 0
        self.lock = _real_thread.allocate_lock()
        self.finished = False
        self.results = []             # (task_label, verdict, detail)


def _watchdog(st, hang_timeout, name):
    import faulthandler
    faulthandler.enable()
    last = -1
    last_change = REAL_MONO()
    while not st.finished:
        REAL_SLEEP(1.0)
        if st.finished:
            return
        with st.lock:
            done = st.completed
            out = st.outstanding
        if done != last:
            last = done
            last_change = REAL_MONO()
        # Only a HANG if work is outstanding yet nothing has completed for the
        # window -- a lost wake on a live remote socket (the class this suite
        # exists to catch), not a slow-but-progressing sweep.
        if out > 0 and (REAL_MONO() - last_change) > hang_timeout:
            sys.stderr.write("\n[%s] WATCHDOG HANG: %d probe(s) outstanding, no "
                             "completion for %.0fs -- lost wake on a remote "
                             "socket\n" % (name, out, hang_timeout))
            sys.stderr.flush()
            try:
                faulthandler.dump_traceback(all_threads=True)
            except Exception:
                pass
            os._exit(netlist.HANG)


def main(name, tasks, argv=None):
    """Entry point for an n0* program.  Returns an exit code (call sys.exit)."""
    if not netlist.enabled():
        sys.stderr.write("[%s] SKIP: RUNLOOM_NET_TESTS!=1 (opt-in gate)\n" % name)
        return netlist.SKIP
    args = _parse_argv(sys.argv[1:] if argv is None else argv)
    timeout_ms = int(args.timeout * 1000)
    hang_timeout = args.hang_timeout or max(30.0, args.timeout * 6 + 10)
    report_dir = args.report_dir or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "_findings", name)

    import runloom
    import runloom.monkey

    st = _State()
    data = netlist.load(timeout=args.timeout,
                        log=lambda m: sys.stderr.write("[%s] %s\n" % (name, m)))

    def log(m):
        sys.stderr.write("[%s] %s\n" % (name, m))
        sys.stderr.flush()

    def run_task(label, category, af, proto, probe_fn):
        servers = netlist.servers_for(data, category, af, proto, top=args.top, log=log)
        if not servers:
            with st.lock:
                st.results.append((label, "env", "no servers"))
            return
        io = IO()
        env_reasons = 0
        for srv in servers:
            with st.lock:
                st.outstanding += 1
            verdict, detail = "env", "?"
            try:
                verdict, detail = probe_fn(io, srv, timeout_ms)
            except Finding as f:
                verdict, detail = "finding", str(f)
            except EnvSkip as e:
                verdict, detail = "env", str(e)
            except Exception as e:               # noqa: BLE001 -> CRASH
                verdict, detail = "crash", "%s: %s" % (type(e).__name__, e)
            finally:
                with st.lock:
                    st.outstanding -= 1
                    st.completed += 1
            if verdict == "pass":
                with st.lock:
                    st.results.append((label, "pass", "%s via %s" % (detail, srv["ip"])))
                return                            # polite: stop on first success
            if verdict in ("finding", "crash"):
                with st.lock:
                    st.results.append((label, verdict, "%s (server %s:%s)"
                                       % (detail, srv["ip"], srv["port"])))
                return
            env_reasons += 1
        with st.lock:
            st.results.append((label, "env", "all %d servers ENV (last: %s)"
                               % (env_reasons, detail)))

    def root():
        wg = runloom.WaitGroup()
        wg.add(len(tasks))

        def one(t):
            try:
                run_task(*t)
            finally:
                wg.done()

        for t in tasks:
            runloom.fiber(one, t)
        wg.wait()

    _real_thread.start_new_thread(_watchdog, (st, hang_timeout, name))
    runloom.monkey.patch()
    try:
        runloom.run(args.hubs, root)
    except SystemExit:
        raise
    except BaseException as exc:                  # noqa: BLE001 -> CRASH
        st.finished = True
        log("run() raised: %s: %s" % (type(exc).__name__, exc))
        netlist.write_finding(report_dir, "net-crash", "%s|run" % name,
                              "run() raised %s: %s" % (type(exc).__name__, exc))
        return netlist.CRASH
    finally:
        st.finished = True

    # ---- aggregate ----
    passed = [r for r in st.results if r[1] == "pass"]
    findings = [r for r in st.results if r[1] == "finding"]
    crashes = [r for r in st.results if r[1] == "crash"]
    for label, verdict, detail in st.results:
        tag = {"pass": "PASS", "env": "SKIP", "finding": "FINDING",
               "crash": "CRASH"}[verdict]
        log("%-7s %-14s %s" % (tag, label, detail))

    if crashes:
        for label, _v, detail in crashes:
            netlist.write_finding(report_dir, "net-crash", "%s|%s" % (name, label), detail)
        return netlist.CRASH
    if findings:
        for label, _v, detail in findings:
            netlist.write_finding(report_dir, "net-protocol", "%s|%s" % (name, label), detail)
        return netlist.FINDING
    if passed:
        return netlist.PASS
    return netlist.SKIP
