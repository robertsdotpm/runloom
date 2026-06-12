"""PoC: on-the-fly compile a goroutine handler to native code (no manual port).

Question this answers:
  (1) Can an *existing* Python handler be compiled to native at runtime, with no
      hand-port, and still cooperate with the M:N goroutine scheduler?
  (2) What is the throughput delta vs the interpreted handler?

Mechanism: `cython.compile(fn)` -- takes the live function object, pulls its
source, transpiles to C, builds a .so, and returns a native callable. We then
spawn THAT as the per-connection goroutine. The cooperative socket I/O
(TCPConn.recv_into / send_all) is unchanged C either way; only the handler's
own loop/compute logic moves from the interpreter into compiled code.

One mode per process (the M:N runtime is set up once per run):

  io           pure-Python echo handler (I/O bound -- the bench_serve handler)
  io-c         same handler, cython.compile'd
  cpu          echo handler + per-message compute (untyped Python)
  cpu-c        same, cython.compile'd  (the "no annotations at all" number)
  cpu-typed-c  same, cython.compile'd, loop vars annotated (~2-line port)

Usage:  poc_compile_handler.py <mode> <N> <hubs> <dur>
"""
import os, sys, time, random
sys.path.insert(0, "src")
os.environ.setdefault("RUNLOOM_SYSMON_QUIET", "1")
import cython
import cycompile
import runloom
import runloom_c

TCPConn = runloom_c.TCPConn
REAL_MONO = time.monotonic

mode = sys.argv[1] if len(sys.argv) > 1 else "io"
N    = int(sys.argv[2]) if len(sys.argv) > 2 else 512
HUBS = int(sys.argv[3]) if len(sys.argv) > 3 else 8
DUR  = float(sys.argv[4]) if len(sys.argv) > 4 else 3.0
WARM = 1.0

# Per-message compute weight for the cpu* modes (inner iterations).  Big enough
# that the handler's own work is a real fraction of the round trip, so the
# compile delta is visible above socket noise.
SPIN = 1500

NSH = 1 << 16
MASK = NSH - 1


# ----------------------------- handlers --------------------------------------
# All take (conn, stop) -- no closures/globals, so cython.compile (inline mode,
# which does not capture an outer scope) can compile them verbatim.

def echo_io(conn, stop):
    buf = bytearray(64)
    try:
        while not stop[0]:
            n = conn.recv_into(buf, 8)
            if not n:
                break
            conn.send_all(memoryview(buf)[:n])
    except OSError:
        pass
    finally:
        try:
            conn.close()
        except OSError:
            pass


def echo_cpu(conn, stop):
    buf = bytearray(64)
    try:
        while not stop[0]:
            n = conn.recv_into(buf, 8)
            if not n:
                break
            acc = 0
            for k in range(SPIN):
                for j in range(n):
                    acc = (acc + buf[j] * k) & 0xffffffff
            buf[0] = acc & 0xff
            conn.send_all(memoryview(buf)[:n])
    except OSError:
        pass
    finally:
        try:
            conn.close()
        except OSError:
            pass


def echo_cpu_typed(conn, stop):
    buf = bytearray(64)
    acc: cython.long
    k: cython.int
    j: cython.int
    n: cython.int
    try:
        while not stop[0]:
            n = conn.recv_into(buf, 8)
            if not n:
                break
            acc = 0
            for k in range(SPIN):
                for j in range(n):
                    acc = (acc + buf[j] * k) & 0xffffffff
            buf[0] = acc & 0xff
            conn.send_all(memoryview(buf)[:n])
    except OSError:
        pass
    finally:
        try:
            conn.close()
        except OSError:
            pass


def echo_cpu_native(conn, stop):
    # Fully native compute: typed scalars AND a typed unsigned-char view over
    # the buffer, so the inner loop has ZERO PyObjects -- `cbuf[j]` is a raw
    # byte load, the arithmetic is machine ops. The only PyObjects left are the
    # recv_into/send_all method calls (the scheduler boundary), which is
    # unavoidable from Python source and is not the bottleneck.
    buf = bytearray(64)
    cbuf: cython.uchar[:]
    acc: cython.ulong
    mask: cython.ulong = 0xffffffff       # typed C constant -- NOT a Python int
    k: cython.int
    j: cython.int
    n: cython.int
    try:
        while not stop[0]:
            n = conn.recv_into(buf, 8)
            if not n:
                break
            cbuf = buf                      # acquire a typed view over the bytes
            acc = 0
            for k in range(SPIN):
                for j in range(n):
                    acc = (acc + cbuf[j] * k) & mask
            buf[0] = acc & 0xff
            conn.send_all(memoryview(buf)[:n])
    except OSError:
        pass
    finally:
        try:
            conn.close()
        except OSError:
            pass


_SPIN_PRE = "SPIN = %d" % SPIN


def resolve_handler(mode):
    # Native modes are built (cythonized + compiled + imported) right here, on
    # the main thread, before the M:N runtime starts.
    if mode == "io":
        return echo_io
    if mode == "io-c":
        return cycompile.compile_funcs([echo_io])[0]
    if mode == "cpu":
        return echo_cpu
    if mode == "cpu-c":
        return cycompile.compile_funcs([echo_cpu], preamble=_SPIN_PRE)[0]
    if mode == "cpu-typed-c":
        return cycompile.compile_funcs([echo_cpu_typed], preamble=_SPIN_PRE)[0]
    if mode == "cpu-native-c":
        return cycompile.compile_funcs([echo_cpu_native], preamble=_SPIN_PRE)[0]
    raise SystemExit("bad mode %r" % mode)


# ----------------------------- bench scaffold --------------------------------
stop = [False]
rts = [0] * NSH
conn_flags = bytearray(N)
win = [0]


def open_listeners(host, n, backlog):
    """n SO_REUSEPORT listeners on one explicit port (no getsockname on
    TCPConn, so we pick the port and retry on collision)."""
    rp = 1 if n > 1 else 0
    for _ in range(80):
        port = random.randint(20000, 60000)
        try:
            first = TCPConn.listen(host, port, backlog, rp)
        except OSError:
            continue
        lst = [first]
        ok = True
        for _ in range(n - 1):
            try:
                lst.append(TCPConn.listen(host, port, backlog, 1))
            except OSError:
                ok = False
                break
        if ok:
            return port, lst
        for ln in lst:
            try:
                ln.close()
            except OSError:
                pass
    raise SystemExit("could not bind listeners")


def acceptor(ln, handler):
    while not stop[0]:
        try:
            conn = ln.accept()
        except OSError:
            break
        runloom.go(handler, conn, stop, stack_size=512 * 1024)


def client(idx, port):
    try:
        c = TCPConn.connect("127.0.0.1", port)
    except OSError:
        return
    conn_flags[idx] = 1
    buf = bytearray(64)
    slot = idx & MASK
    try:
        while not stop[0]:
            c.send_all(b"hellopyg")
            n = c.recv_into(buf, 8)
            if not n:
                break
            rts[slot] = rts[slot] + 1
    except OSError:
        pass
    finally:
        try:
            c.close()
        except OSError:
            pass


def root(handler):
    port, listeners = open_listeners("127.0.0.1", HUBS, min(N, 65535))
    for ln in listeners:
        runloom.go(acceptor, ln, handler, stack_size=256 * 1024)
    for i in range(N):
        runloom.go(client, i, port)

    def controller():
        t0 = REAL_MONO()
        while sum(conn_flags) < N:
            runloom.sleep(0.01)
            if REAL_MONO() - t0 > 60:
                break
        est = sum(conn_flags)
        runloom.sleep(WARM)
        start = sum(rts)
        m0 = REAL_MONO()
        runloom.sleep(DUR)
        w = REAL_MONO() - m0
        win[0] = sum(rts) - start
        stop[0] = True
        for ln in listeners:
            try:
                ln.close()
            except OSError:
                pass
        print("RESULT mode=%-12s N=%-6d hubs=%d est=%d/%d  %.1fK req/s"
              % (mode, N, HUBS, est, N, win[0] / w / 1000.0))

    runloom.go(controller)
    while not stop[0]:
        runloom.sleep(0.05)


def main():
    t0 = REAL_MONO()
    handler = resolve_handler(mode)
    compile_s = REAL_MONO() - t0
    if "-c" in mode:
        print("COMPILE mode=%-12s built/imported in %.2fs gil=%s"
              % (mode, compile_s,
                 sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else "?"))
    runloom.run(HUBS, lambda: root(handler))


if __name__ == "__main__":
    main()
