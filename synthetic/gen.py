# -*- coding: utf-8 -*-
"""Generator for the 1000 synthetic runloom toy programs (fresh M:N corpus).

A "project type" is  test_type x category x primitive x format  =
  2 x 10 x 10 x 5  =  1000 combinations.

Every generated program:
  * runs on runloom's M:N scheduler via the ONE public entry point
    `runloom.run(NHUB, root)` with NHUB > 1 -- free-threaded 3.13t, GIL off,
    real multi-core parallelism;
  * exercises ONE cooperative primitive (the mechanism) themed by ONE
    category (naming + payload), carrying a payload in ONE format;
  * is "success" (drives the primitive to a correct result) or "failure"
    (drives it to a specific, reliable, offline error and asserts it);
  * spawns workers with `runloom.fiber(...)` from the root goroutine and verifies
    AFTER `run()` returns (goroutine exceptions are swallowed by the scheduler,
    so the main thread must do the asserting);
  * prints exactly "PASS" and exits 0 when runloom is healthy, else
    prints "FAIL: ..." and exits 1.

A real runloom bug therefore shows up as: a hang (harness timeout), a
crash (negative/abnormal exit), or a "FAIL"/non-PASS outcome.

Paths are resolved relative to each program's own location at runtime, so the
corpus is position-independent.  Run this file with any interpreter (it only
emits text + a self-signed cert): `python synthetic/gen.py`.
"""
import os
import json
import subprocess

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # pygo-synth
SRC = os.path.join(ROOT, "src")
SYN = os.path.join(ROOT, "synthetic")
ASSETS = os.path.join(SYN, "_assets")

NHUB = 8

CATEGORIES = [
    ("web frameworks and APIs", "web-frameworks-and-apis"),
    ("HTTP clients and networking", "http-clients-and-networking"),
    ("databases and ORMs", "databases-and-orms"),
    ("message queues and streaming", "message-queues-and-streaming"),
    ("real-time communication and websockets", "real-time-communication-and-websockets"),
    ("task scheduling and background jobs", "task-scheduling-and-background-jobs"),
    ("microservices and RPC", "microservices-and-rpc"),
    ("automation and scraping", "automation-and-scraping"),
    ("developer tooling and infrastructure", "developer-tooling-and-infrastructure"),
    ("concurrency runtimes and synchronization", "concurrency-runtimes-and-synchronization"),
]

PRIMS = ["sockets", "ssl", "disk_files", "processes", "threads",
         "multiprocessing", "select_selectors", "queues", "signals", "dns"]

FORMATS = ["json", "compressed", "unicode", "bytes", "freeform"]

TTS = ["success", "failure"]

# ----------------------------------------------------------------------
# Format codecs.  Each defines mk_payload()/encode(obj)->bytes/decode(buf)
# such that decode(encode(mk_payload())) == mk_payload().
# ----------------------------------------------------------------------
FMT_JSON = '''
def mk_payload():
    return {"category": THEME, "kind": "json",
            "items": [THEME + "#" + str(i) for i in range(8)],
            "count": 8, "ok": True, "ratio": 0.5}

def encode(obj):
    return json.dumps(obj, ensure_ascii=False, sort_keys=True).encode("utf-8")

def decode(buf):
    return json.loads(bytes(buf).decode("utf-8"))
'''

FMT_COMPRESSED = '''
def mk_payload():
    unit = (THEME + " | stream record | ").encode("utf-8")
    return unit * 8000               # ~300 KB pre-compression (exercises heavy offload)

def encode(obj):
    return zlib.compress(obj, 6)     # transports small; decompress reconstructs

def decode(buf):
    return zlib.decompress(bytes(buf))
'''

FMT_UNICODE = '''
def mk_payload():
    return ("「" + THEME + "」 数据 données データ Ωμέγα \U0001f40d\U0001f9f5 ") * 3

def encode(obj):
    return obj.encode("utf-8")

def decode(buf):
    return bytes(buf).decode("utf-8")
'''

FMT_BYTES = '''
def mk_payload():
    head = (THEME + ":bytes:").encode("utf-8")
    return head + bytes((i * 7 + 11) % 256 for i in range(96))

def encode(obj):
    return bytes(obj)

def decode(buf):
    return bytes(buf)
'''

FMT_FREEFORM_PICKLE = '''
def mk_payload():
    return {"theme": THEME, "vals": [1, 2, 3, 5, 8], "nested": {"slug": CATSLUG, "flag": True}}

def encode(obj):
    return pickle.dumps(obj, protocol=4)

def decode(buf):
    return pickle.loads(bytes(buf))
'''

FMT_FREEFORM_BASE64 = '''
def mk_payload():
    return (THEME + "::b64::").encode("utf-8") + bytes((i * 3) % 256 for i in range(80))

def encode(obj):
    return base64.b64encode(bytes(obj))

def decode(buf):
    return base64.b64decode(bytes(buf))
'''

FMT_FREEFORM_STRUCT = '''
def mk_payload():
    digest = hashlib.sha256(THEME.encode("utf-8")).digest()
    return tuple(digest[i] for i in range(8))

def encode(obj):
    return struct.pack(">8B", *obj)

def decode(buf):
    return tuple(struct.unpack(">8B", bytes(buf)))
'''

FMT_FREEFORM_CSV = '''
def mk_payload():
    return [[THEME, str(i), str(i * i)] for i in range(6)]

def encode(obj):
    out = io.StringIO()
    writer = csv.writer(out)
    for row in obj:
        writer.writerow(row)
    return out.getvalue().encode("utf-8")

def decode(buf):
    rows = list(csv.reader(io.StringIO(bytes(buf).decode("utf-8"))))
    return [r for r in rows if r]
'''

FREEFORM_VARIANTS = [FMT_FREEFORM_PICKLE, FMT_FREEFORM_BASE64,
                     FMT_FREEFORM_STRUCT, FMT_FREEFORM_CSV]
FREEFORM_NAMES = ["pickle", "base64", "struct", "csv"]


def fmt_block(fmt, idx):
    if fmt == "json":
        return FMT_JSON, "json"
    if fmt == "compressed":
        return FMT_COMPRESSED, "zlib"
    if fmt == "unicode":
        return FMT_UNICODE, "utf-8"
    if fmt == "bytes":
        return FMT_BYTES, "raw"
    v = idx % 4
    return FREEFORM_VARIANTS[v], FREEFORM_NAMES[v]


# ----------------------------------------------------------------------
# Primitive bodies.  Written with the raw mn_init / <spawns> / mn_run /
# mn_fini envelope; to_run_envelope() rewrites that into the public
# `runloom.run(NHUB, __root)` form before emission, and GO is rebound to
# the public `runloom.fiber`.  Each body references module constants (THEME,
# NW, NHUB, CERT, KEY, PY) and preamble helpers (finish, make_coordinator,
# recvexact).
# ----------------------------------------------------------------------
BODY = {}

BODY[("sockets", "success")] = '''
def main():
    runloom.monkey.patch()
    GO = runloom_c.mn_fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    port = listener.getsockname()[1]
    results = runloom.Chan(NW)
    state = {"good": 0}

    def handle(conn):
        try:
            hdr = recvexact(conn, 4)
            length = struct.unpack(">I", hdr)[0]
            body = recvexact(conn, length)
            conn.sendall(struct.pack(">I", len(body)) + body)
        except Exception:
            pass
        finally:
            conn.close()

    def accept_loop():
        for _ in range(NW):
            conn, _ = listener.accept()
            GO(lambda c=conn: handle(c))

    def client():
        ok = False
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("127.0.0.1", port))
            sock.sendall(struct.pack(">I", len(enc)) + enc)
            hdr = recvexact(sock, 4)
            length = struct.unpack(">I", hdr)[0]
            got = recvexact(sock, length)
            ok = decode(got) == payload
            sock.close()
        except Exception:
            ok = False
        results.send(ok)

    runloom.mn_init(NHUB)
    GO(make_coordinator(results, NW, state))
    GO(accept_loop)
    for _ in range(NW):
        GO(client)
    runloom.mn_run()
    runloom.mn_fini()
    listener.close()
    finish(state["good"] == NW, state)
'''

BODY[("sockets", "failure")] = '''
def main():
    runloom.monkey.patch()
    GO = runloom_c.mn_fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()                       # free port, nothing listening
    state = {}

    def client():
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("127.0.0.1", port))
            sock.sendall(struct.pack(">I", len(enc)) + enc)
            sock.close()
            state["err"] = None
        except OSError as exc:
            state["err"] = type(exc).__name__

    runloom.mn_init(NHUB)
    GO(client)
    runloom.mn_run()
    runloom.mn_fini()
    finish(bool(state.get("err")), state)
'''

BODY[("ssl", "success")] = '''
def main():
    runloom.monkey.patch()
    GO = runloom_c.mn_fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.load_cert_chain(CERT, KEY)
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    port = listener.getsockname()[1]
    results = runloom.Chan(NW)
    state = {"good": 0}

    def handle(raw):
        try:
            conn = server_ctx.wrap_socket(raw, server_side=True,
                                          do_handshake_on_connect=False)
            conn.do_handshake()
            hdr = recvexact(conn, 4)
            length = struct.unpack(">I", hdr)[0]
            body = recvexact(conn, length)
            conn.sendall(struct.pack(">I", len(body)) + body)
            conn.close()
        except Exception:
            try:
                raw.close()
            except OSError:
                pass

    def accept_loop():
        for _ in range(NW):
            raw, _ = listener.accept()
            GO(lambda r=raw: handle(r))

    def client():
        ok = False
        raw = None
        try:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            raw = socket.create_connection(("127.0.0.1", port))
            conn = ctx.wrap_socket(raw, server_hostname="localhost",
                                   do_handshake_on_connect=False)
            conn.do_handshake()
            conn.sendall(struct.pack(">I", len(enc)) + enc)
            hdr = recvexact(conn, 4)
            length = struct.unpack(">I", hdr)[0]
            got = recvexact(conn, length)
            ok = decode(got) == payload
            conn.close()
        except Exception:
            ok = False
            if raw is not None:
                try:
                    raw.close()
                except OSError:
                    pass
        results.send(ok)

    runloom.mn_init(NHUB)
    GO(make_coordinator(results, NW, state))
    GO(accept_loop)
    for _ in range(NW):
        GO(client)
    runloom.mn_run()
    runloom.mn_fini()
    listener.close()
    finish(state["good"] == NW, state)
'''

BODY[("ssl", "failure")] = '''
def main():
    runloom.monkey.patch()
    GO = runloom_c.mn_fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(8)
    port = listener.getsockname()[1]
    state = {}

    def server():
        try:
            conn, _ = listener.accept()
            conn.recv(64)               # plain read -- never speaks TLS
            conn.close()
        except OSError:
            pass
        finally:
            listener.close()

    def client():
        raw = None
        try:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            raw = socket.create_connection(("127.0.0.1", port))
            conn = ctx.wrap_socket(raw, server_hostname="localhost",
                                   do_handshake_on_connect=False)
            conn.do_handshake()
            state["err"] = None
        except OSError as exc:          # ssl.SSLError is an OSError subclass
            state["err"] = type(exc).__name__
        finally:
            if raw is not None:
                try:
                    raw.close()
                except OSError:
                    pass

    runloom.mn_init(NHUB)
    GO(server)
    GO(client)
    runloom.mn_run()
    runloom.mn_fini()
    finish(bool(state.get("err")), state)
'''

BODY[("disk_files", "success")] = '''
def main():
    runloom.monkey.patch()
    GO = runloom_c.mn_fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    workdir = tempfile.mkdtemp(prefix="rlsyn_")
    results = runloom.Chan(NW)
    state = {"good": 0}

    def worker(idx):
        ok = False
        try:
            path = os.path.join(workdir, "chunk{0}.bin".format(idx))
            with open(path, "wb") as handle:
                handle.write(enc)
            with open(path, "rb") as handle:
                got = handle.read()
            ok = decode(got) == payload
        except Exception:
            ok = False
        results.send(ok)

    runloom.mn_init(NHUB)
    GO(make_coordinator(results, NW, state))
    for i in range(NW):
        GO(lambda i=i: worker(i))
    runloom.mn_run()
    runloom.mn_fini()
    shutil.rmtree(workdir, ignore_errors=True)
    finish(state["good"] == NW, state)
'''

BODY[("disk_files", "failure")] = '''
def main():
    runloom.monkey.patch()
    GO = runloom_c.mn_fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    workdir = tempfile.mkdtemp(prefix="rlsyn_")
    state = {}

    def worker():
        try:
            with open(os.path.join(workdir, "missing", "x.bin"), "rb") as handle:
                handle.read()
            state["err"] = None
        except FileNotFoundError as exc:
            state["err"] = type(exc).__name__

    runloom.mn_init(NHUB)
    GO(worker)
    runloom.mn_run()
    runloom.mn_fini()
    shutil.rmtree(workdir, ignore_errors=True)
    finish(state.get("err") == "FileNotFoundError", state)
'''

BODY[("processes", "success")] = '''
CHILD_ECHO = "import sys; sys.stdout.buffer.write(sys.stdin.buffer.read())"

def main():
    runloom.monkey.patch()
    GO = runloom_c.mn_fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    results = runloom.Chan(NW)
    state = {"good": 0}

    def worker():
        ok = False
        try:
            done = subprocess.run([PY, "-c", CHILD_ECHO], input=enc,
                                  capture_output=True)
            ok = (done.returncode == 0) and (decode(done.stdout) == payload)
        except Exception:
            ok = False
        results.send(ok)

    runloom.mn_init(NHUB)
    GO(make_coordinator(results, NW, state))
    for _ in range(NW):
        GO(worker)
    runloom.mn_run()
    runloom.mn_fini()
    finish(state["good"] == NW, state)
'''

BODY[("processes", "failure")] = '''
def main():
    runloom.monkey.patch()
    GO = runloom_c.mn_fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    state = {}

    def worker():
        try:
            subprocess.run([PY, "-c", "import sys; sys.exit(3)"], check=True,
                           capture_output=True)
            state["err"] = None
        except subprocess.CalledProcessError as exc:
            state["err"] = exc.returncode

    runloom.mn_init(NHUB)
    GO(worker)
    runloom.mn_run()
    runloom.mn_fini()
    finish(state.get("err") == 3, state)
'''

BODY[("threads", "success")] = '''
def main():
    runloom.monkey.patch()
    GO = runloom_c.mn_fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    # Real OS threads coordinated via the monkey-patched threading.Lock,
    # joined cooperatively from a goroutine.  Correctness rests on the
    # atomicity of list.append under free-threading (not on the lock
    # serialising), so this stays valid under M:N parallelism -- the
    # lock is exercised under contention but never gates the result.
    lock = threading.Lock()
    collected = []
    state = {}

    def thread_body(idx):
        ok = decode(enc) == payload
        with lock:
            collected.append(ok)

    def driver():
        workers = [threading.Thread(target=thread_body, args=(i,))
                   for i in range(NW)]
        for t in workers:
            t.start()
        for t in workers:
            t.join()                 # cooperative join parks the goroutine
        state["count"] = sum(1 for ok in collected if ok)
        state["total"] = len(collected)

    runloom.mn_init(NHUB)
    GO(driver)
    runloom.mn_run()
    runloom.mn_fini()
    finish(state.get("count") == NW and state.get("total") == NW, state)
'''

BODY[("threads", "failure")] = '''
def main():
    runloom.monkey.patch()
    GO = runloom_c.mn_fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    state = {}

    def worker():
        try:
            threading.Lock().release()      # release an unlocked lock
            state["err"] = None
        except RuntimeError as exc:
            state["err"] = type(exc).__name__

    runloom.mn_init(NHUB)
    GO(worker)
    runloom.mn_run()
    runloom.mn_fini()
    finish(state.get("err") == "RuntimeError", state)
'''

BODY[("multiprocessing", "success")] = '''
def mp_echo(in_q, out_q):
    out_q.put(in_q.get())

def main():
    runloom.monkey.patch()
    GO = runloom_c.mn_fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    ctx = mp.get_context("spawn")
    in_q = ctx.Queue()
    out_q = ctx.Queue()
    proc = ctx.Process(target=mp_echo, args=(in_q, out_q))
    state = {}

    def driver():
        proc.start()
        in_q.put(enc)
        got = out_q.get()
        proc.join()
        state["ok"] = decode(got) == payload
        state["exit"] = proc.exitcode

    runloom.mn_init(NHUB)
    GO(driver)
    runloom.mn_run()
    runloom.mn_fini()
    in_q.close()
    out_q.close()
    finish(state.get("ok") is True and state.get("exit") == 0, state)
'''

BODY[("multiprocessing", "failure")] = '''
def mp_fail(in_q):
    in_q.get()
    raise RuntimeError("intended child failure for synthetic test")

def main():
    runloom.monkey.patch()
    GO = runloom_c.mn_fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    ctx = mp.get_context("spawn")
    in_q = ctx.Queue()
    proc = ctx.Process(target=mp_fail, args=(in_q,))
    state = {}

    def driver():
        proc.start()
        in_q.put(enc)
        proc.join()
        state["exit"] = proc.exitcode

    runloom.mn_init(NHUB)
    GO(driver)
    runloom.mn_run()
    runloom.mn_fini()
    in_q.close()
    finish(state.get("exit") not in (0, None), state)
'''

BODY[("select_selectors", "success")] = '''
def main():
    runloom.monkey.patch()
    GO = runloom_c.mn_fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    state = {}

    def writer():
        runloom.sleep(0.01)
        right.sendall(struct.pack(">I", len(enc)) + enc)
        right.close()

    def reader():
        sel = selectors.DefaultSelector()
        sel.register(left, selectors.EVENT_READ)
        buf = bytearray()
        length = None
        try:
            while True:
                events = sel.select(timeout=3.0)
                if not events:
                    break
                chunk = left.recv(65536)
                if not chunk:
                    break
                buf += chunk
                if length is None and len(buf) >= 4:
                    length = struct.unpack(">I", bytes(buf[:4]))[0]
                if length is not None and len(buf) >= 4 + length:
                    break
        finally:
            sel.close()
        if length is not None and len(buf) >= 4 + length:
            state["ok"] = decode(bytes(buf[4:4 + length])) == payload
        else:
            state["ok"] = False
        left.close()

    runloom.mn_init(NHUB)
    GO(reader)
    GO(writer)
    runloom.mn_run()
    runloom.mn_fini()
    finish(state.get("ok") is True, state)
'''

BODY[("select_selectors", "failure")] = '''
def main():
    runloom.monkey.patch()
    GO = runloom_c.mn_fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    state = {}

    def worker():
        try:
            sock = socket.socket()
            sock.close()
            sel = selectors.DefaultSelector()
            sel.register(sock, selectors.EVENT_READ)    # closed fd
            sel.close()
            state["err"] = None
        except (ValueError, OSError) as exc:
            state["err"] = type(exc).__name__

    runloom.mn_init(NHUB)
    GO(worker)
    runloom.mn_run()
    runloom.mn_fini()
    finish(bool(state.get("err")), state)
'''

BODY[("queues", "success")] = '''
def main():
    runloom.monkey.patch()
    GO = runloom_c.mn_fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    per = 4
    bus = queue.Queue()
    state = {}

    def producer():
        for _ in range(per):
            bus.put(enc)

    def consumer():
        good = 0
        for _ in range(NW * per):
            item = bus.get()
            if decode(item) == payload:
                good += 1
            bus.task_done()
        state["good"] = good

    runloom.mn_init(NHUB)
    GO(consumer)
    for _ in range(NW):
        GO(producer)
    runloom.mn_run()
    runloom.mn_fini()
    finish(state.get("good") == NW * per, state)
'''

BODY[("queues", "failure_empty")] = '''
def main():
    runloom.monkey.patch()
    GO = runloom_c.mn_fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    state = {}

    def worker():
        try:
            queue.Queue().get_nowait()      # empty
            state["err"] = None
        except queue.Empty:
            state["err"] = "Empty"

    runloom.mn_init(NHUB)
    GO(worker)
    runloom.mn_run()
    runloom.mn_fini()
    finish(state.get("err") == "Empty", state)
'''

BODY[("queues", "failure_full")] = '''
def main():
    runloom.monkey.patch()
    GO = runloom_c.mn_fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    state = {}

    def worker():
        try:
            bounded = queue.Queue(maxsize=1)
            bounded.put_nowait(enc)
            bounded.put_nowait(enc)         # full
            state["err"] = None
        except queue.Full:
            state["err"] = "Full"

    runloom.mn_init(NHUB)
    GO(worker)
    runloom.mn_run()
    runloom.mn_fini()
    finish(state.get("err") == "Full", state)
'''

BODY[("signals", "success")] = '''
def main():
    runloom.monkey.patch()
    GO = runloom_c.mn_fiber
    payload = mk_payload()
    enc = encode(payload)
    fmt_ok = decode(enc) == payload
    sig = signal.SIGUSR1
    state = {"hit": False}

    def handler(signum, frame):
        state["hit"] = True

    previous = signal.signal(sig, handler)      # install on main thread

    def waiter():
        for _ in range(400):
            if state["hit"]:
                return
            runloom.sleep(0.005)

    def raiser():
        runloom.sleep(0.02)
        os.kill(os.getpid(), sig)

    runloom.mn_init(NHUB)
    GO(waiter)
    GO(raiser)
    runloom.mn_run()
    runloom.mn_fini()
    signal.signal(sig, previous)
    finish(fmt_ok and state["hit"], state)
'''

BODY[("signals", "failure")] = '''
def main():
    runloom.monkey.patch()
    GO = runloom_c.mn_fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    state = {}

    def worker():
        try:
            signal.signal(signal.SIGKILL, signal.SIG_IGN)   # uncatchable
            state["err"] = None
        except (OSError, RuntimeError, ValueError) as exc:
            state["err"] = type(exc).__name__

    runloom.mn_init(NHUB)
    GO(worker)
    runloom.mn_run()
    runloom.mn_fini()
    finish(bool(state.get("err")), state)
'''

BODY[("dns", "success")] = '''
def main():
    runloom.monkey.patch()
    GO = runloom_c.mn_fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    results = runloom.Chan(NW)
    state = {"good": 0}

    def worker():
        ok = False
        try:
            infos = socket.getaddrinfo("localhost", 80,
                                       proto=socket.IPPROTO_TCP)
            ok = len(infos) >= 1
        except Exception:
            ok = False
        results.send(ok)

    runloom.mn_init(NHUB)
    GO(make_coordinator(results, NW, state))
    for _ in range(NW):
        GO(worker)
    runloom.mn_run()
    runloom.mn_fini()
    finish(state["good"] == NW, state)
'''

BODY[("dns", "failure")] = '''
def main():
    runloom.monkey.patch()
    GO = runloom_c.mn_fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    state = {}

    def worker():
        try:
            # Numeric host (no name lookup) + an unknown service name -> the
            # service resolves against /etc/services locally and fails fast
            # with gaierror, no network required.
            socket.getaddrinfo("127.0.0.1", "no-such-svc-xyz",
                               proto=socket.IPPROTO_TCP)
            state["err"] = None
        except socket.gaierror:
            state["err"] = "gaierror"

    runloom.mn_init(NHUB)
    GO(worker)
    runloom.mn_run()
    runloom.mn_fini()
    finish(state.get("err") == "gaierror", state)
'''


def body_for(prim, tt, idx):
    if prim == "queues" and tt == "failure":
        return BODY[("queues", "failure_empty")] if idx % 2 == 0 \
            else BODY[("queues", "failure_full")]
    return BODY[(prim, tt)]


def to_run_envelope(body):
    """Rewrite the raw  mn_init(NHUB) / <spawns> / mn_run() / mn_fini()
    envelope into the public  def __root(): <spawns>; runloom.run(NHUB, __root)
    form, and rebind GO to the public runloom.fiber.  The spawns become the root
    goroutine; run() drives the M:N scheduler to completion."""
    body = body.replace("    GO = runloom_c.mn_fiber", "    GO = runloom.fiber")
    lines = body.split("\n")
    out = []
    i, n = 0, len(lines)
    while i < n:
        line = lines[i]
        if line.strip() == "runloom.mn_init(NHUB)":
            indent = line[:len(line) - len(line.lstrip())]
            out.append(indent + "def __root():")
            i += 1
            while i < n and lines[i].strip() != "runloom.mn_run()":
                spawn = lines[i]
                out.append("" if spawn.strip() == "" else "    " + spawn)
                i += 1
            if i < n and lines[i].strip() == "runloom.mn_run()":
                i += 1
            if i < n and lines[i].strip() == "runloom.mn_fini()":
                i += 1
            out.append(indent + "runloom.run(NHUB, __root)")
        else:
            out.append(line)
            i += 1
    return "\n".join(out)


PREAMBLE = '''# -*- coding: utf-8 -*-
"""@@DOC@@

Synthetic runloom toy program (auto-generated).
  test type : @@TT@@
  category  : @@CATTITLE@@
  primitive : @@PRIM@@
  format    : @@FMT@@ (@@FMTNAME@@)
  scheduler : M:N via runloom.run(@@NHUB@@, root), free-threaded 3.13t, GIL off

Exercises runloom's main API -- the root goroutine spawns workers with
runloom.fiber(...) onto @@NHUB@@ hub threads, using the monkey-patched cooperative
@@PRIM@@ primitive to carry a @@FMT@@ payload.  Prints PASS and exits 0 when
healthy; FAIL / hang / crash signals a bug.
"""
import sys
import os
import io
import time
import json
import zlib
import base64
import struct
import csv
import pickle
import hashlib
import tempfile
import shutil
import socket
import ssl
import selectors
import signal
import subprocess
import threading
import queue
import multiprocessing as mp

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "src"))
import runloom
import runloom_c

THEME = "@@CATTITLE@@"
CATSLUG = "@@CATSLUG@@"
PRIM = "@@PRIM@@"
FMT = "@@FMT@@"
TESTTYPE = "@@TT@@"
NW = @@NW@@
NHUB = @@NHUB@@
CERT = os.path.join(HERE, "..", "_assets", "cert.pem")
KEY = os.path.join(HERE, "..", "_assets", "key.pem")
PY = sys.executable


def finish(ok, info=""):
    if ok:
        print("PASS")
    else:
        print("FAIL:", info)
        sys.exit(1)


def make_coordinator(results, n, state):
    """Goroutine that fans in n boolean results over a channel."""
    def coordinator():
        good = 0
        for _ in range(n):
            value, _ = results.recv()
            if value:
                good += 1
        state["good"] = good
    return coordinator


def recvexact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("short read")
        buf += chunk
    return bytes(buf)

# ---- format codec ----
@@FMTBLOCK@@

# ---- body ----
@@BODY@@

if __name__ == "__main__":
    main()
'''


def render(tt, cattitle, catslug, prim, fmt, idx):
    fblock, fname = fmt_block(fmt, idx)
    nw = 3 + (idx % 5)
    doc = ("{0} -- a {1} toy using the {2} primitive with a {3} payload, "
           "expecting {4}.").format(cattitle.capitalize(), cattitle, prim,
                                     fmt, tt)
    out = PREAMBLE
    out = out.replace("@@DOC@@", doc)
    out = out.replace("@@CATTITLE@@", cattitle)
    out = out.replace("@@CATSLUG@@", catslug)
    out = out.replace("@@PRIM@@", prim)
    out = out.replace("@@FMT@@", fmt)
    out = out.replace("@@FMTNAME@@", fname)
    out = out.replace("@@TT@@", tt)
    out = out.replace("@@NW@@", str(nw))
    out = out.replace("@@NHUB@@", str(NHUB))
    # FMTBLOCK / BODY last so their literal text isn't touched by replaces
    out = out.replace("@@FMTBLOCK@@", fblock.strip("\n"))
    out = out.replace("@@BODY@@", to_run_envelope(body_for(prim, tt, idx)).strip("\n"))
    return out, nw, fname


def ensure_assets():
    os.makedirs(ASSETS, exist_ok=True)
    cert = os.path.join(ASSETS, "cert.pem")
    key = os.path.join(ASSETS, "key.pem")
    if os.path.exists(cert) and os.path.exists(key):
        return
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-keyout", key,
         "-out", cert, "-days", "3650", "-nodes", "-subj", "/CN=localhost"],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main():
    ensure_assets()
    manifest = []
    idx = 0
    count = 0
    for tt in TTS:
        for cattitle, catslug in CATEGORIES:
            for prim in PRIMS:
                for fmt in FORMATS:
                    source, nw, fname = render(tt, cattitle, catslug,
                                               prim, fmt, idx)
                    dirname = "{0:04d}__{1}__{2}__{3}__{4}".format(
                        idx + 1, tt, catslug, prim, fmt)
                    progdir = os.path.join(SYN, dirname)
                    os.makedirs(progdir, exist_ok=True)
                    path = os.path.join(progdir, "main.py")
                    with open(path, "w", encoding="utf-8") as handle:
                        handle.write(source)
                    manifest.append({
                        "id": idx + 1,
                        "dir": dirname,
                        "test_type": tt,
                        "category": cattitle,
                        "category_slug": catslug,
                        "primitive": prim,
                        "format": fmt,
                        "format_variant": fname,
                        "nworkers": nw,
                        "expect_pass": True,
                    })
                    idx += 1
                    count += 1
    with open(os.path.join(SYN, "manifest.json"), "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=1)
    print("generated", count, "programs into", SYN)


if __name__ == "__main__":
    main()
