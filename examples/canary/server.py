"""The runloom canary service (docs/dev/RELIABILITY_PROGRAM.md R6).

A small but REAL service on runloom, meant to run continuously for weeks.  It
exercises the whole stack at once -- TCP accept/echo, channels + select + timers
(the chat room), and blocking-pool offload -- and serves its own
`runloom.stats()` so its health is observable from outside.  It is also an R1
soak subject: a sampler thread writes the same CSV the soak harness reads, so
the slope oracle can pass/fail a canary run exactly like any other soak.

Endpoints (all on 127.0.0.1 by default):
  --echo-port   TCP echo (the throughput/park-wake workhorse)
  --chat-port   TCP chat room: every line is broadcast to all joined clients
                via a per-client channel + a select fan-out (channels, select,
                timers, TCP together)
  --status-port line-based status: send "stats\\n" -> one JSON line of
                {uptime_s, stats: {...}}; "ping\\n" -> "pong\\n"

The claim this service exists to earn: "a runloom server ran continuously for
N days with flat gauges."  That is the only reliability claim users trust, and
the sampler CSV + oracle make it measurable rather than a vibe.
"""
import argparse
import json
import os
import socket
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import time as _time
_raw_sleep = _time.sleep

import runloom
import runloom.monkey
runloom.monkey.patch()
import runloom_c

# Arm the field crash + self-hang telemetry (R5) so a canary wedge produces an
# artifact instead of a silent stall.
runloom_c.install_crash_handler(
    "goroutines,backtrace",
    os.environ.get("CANARY_CRASH_FILE", "canary_crash.txt"))

_START = _time.monotonic()
_STOP = [False]


def _listener(port, backlog=128):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", port))
    srv.listen(backlog)
    return srv


# --------------------------------------------------------------------------
# echo
# --------------------------------------------------------------------------
def _echo_conn(conn):
    try:
        while not _STOP[0]:
            data = conn.recv(4096)
            if not data:
                break
            conn.sendall(data)
    except OSError:
        pass
    finally:
        conn.close()


def _echo_server(srv):
    while not _STOP[0]:
        try:
            conn, _ = srv.accept()
        except OSError:
            break
        runloom.fiber(lambda c=conn: _echo_conn(c))


# --------------------------------------------------------------------------
# chat room: channels + select + timers + TCP
# --------------------------------------------------------------------------
_chat_members = {}          # id -> outbound Chan
_chat_lock = threading.Lock()
_chat_next = [0]


def _chat_broadcast(msg):
    with _chat_lock:
        chans = list(_chat_members.values())
    for ch in chans:
        try:
            ch.send(msg)           # buffered; never blocks the broadcaster
        except Exception:
            pass


def _chat_conn(conn):
    with _chat_lock:
        cid = _chat_next[0]
        _chat_next[0] += 1
        out = runloom_c.Chan(64)
        _chat_members[cid] = out
    _chat_broadcast(b"+ member %d joined\n" % cid)

    # writer fiber: drain this client's outbound channel to its socket, with a
    # periodic keepalive tick (timers) so an idle member still cycles.  Use a
    # STOPPABLE Ticker and Stop() it on exit -- a bare Tick() would leak its
    # ticker fiber for the process lifetime, exactly the long-uptime leak the
    # canary exists to NOT have.
    def writer():
        ticker = runloom.time.NewTicker(5.0)
        try:
            while not _STOP[0]:
                idx, (val, ok) = runloom_c.select([
                    ("recv", out), ("recv", ticker.c)])
                if idx == 0:
                    if not ok:
                        break
                    conn.sendall(val)
                else:
                    conn.sendall(b": tick\n")
        except OSError:
            pass
        finally:
            ticker.Stop()
    runloom.fiber(writer)

    # reader: each inbound line is broadcast to all members.
    try:
        buf = b""
        while not _STOP[0]:
            data = conn.recv(4096)
            if not data:
                break
            buf += data
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                _chat_broadcast(b"[%d] %s\n" % (cid, line))
    except OSError:
        pass
    finally:
        with _chat_lock:
            _chat_members.pop(cid, None)
        try:
            out.close()
        except Exception:
            pass
        conn.close()
        _chat_broadcast(b"- member %d left\n" % cid)


def _chat_server(srv):
    while not _STOP[0]:
        try:
            conn, _ = srv.accept()
        except OSError:
            break
        runloom.fiber(lambda c=conn: _chat_conn(c))


# --------------------------------------------------------------------------
# status endpoint: serves runloom.stats() + uptime
# --------------------------------------------------------------------------
def _status_conn(conn):
    try:
        buf = b""
        while not _STOP[0]:
            data = conn.recv(1024)
            if not data:
                break
            buf += data
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                cmd = line.strip().lower()
                if cmd == b"ping":
                    conn.sendall(b"pong\n")
                elif cmd == b"stats":
                    payload = {
                        "uptime_s": round(_time.monotonic() - _START, 1),
                        "stats": {k: v for k, v in runloom.stats().items()
                                  if isinstance(v, (int, float))},
                    }
                    conn.sendall((json.dumps(payload) + "\n").encode())
                else:
                    conn.sendall(b"commands: ping | stats\n")
    except OSError:
        pass
    finally:
        conn.close()


def _status_server(srv):
    while not _STOP[0]:
        try:
            conn, _ = srv.accept()
        except OSError:
            break
        runloom.fiber(lambda c=conn: _status_conn(c))


# --------------------------------------------------------------------------
# soak sampler: write the same CSV the R1 oracle reads
# --------------------------------------------------------------------------
def _proc_metrics():
    m = {"rss_kb": 0, "vsz_kb": 0, "threads": 0, "vmas": 0, "fds": 0}
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    m["rss_kb"] = int(line.split()[1])
                elif line.startswith("VmSize:"):
                    m["vsz_kb"] = int(line.split()[1])
                elif line.startswith("Threads:"):
                    m["threads"] = int(line.split()[1])
    except OSError:
        pass
    try:
        with open("/proc/self/maps") as f:
            m["vmas"] = sum(1 for _ in f)
    except OSError:
        pass
    try:
        m["fds"] = len(os.listdir("/proc/self/fd"))
    except OSError:
        pass
    return m


def _sampler(csv_path, interval):
    import gc
    hdr = None
    with open(csv_path, "w", buffering=1) as csv:
        while not _STOP[0]:
            gc.collect()
            stats = {k: v for k, v in runloom.stats().items()
                     if isinstance(v, int)}
            row = {"t": round(_time.monotonic() - _START, 1)}
            row.update(_proc_metrics())
            row.update(stats)
            if hdr is None:
                hdr = list(row.keys())
                csv.write(",".join(hdr) + "\n")
            csv.write(",".join(str(row.get(k, "")) for k in hdr) + "\n")
            slept = 0.0
            while slept < interval and not _STOP[0]:
                _raw_sleep(min(1.0, interval - slept))
                slept += 1.0


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--echo-port", type=int, default=8801)
    ap.add_argument("--chat-port", type=int, default=8802)
    ap.add_argument("--status-port", type=int, default=8803)
    ap.add_argument("--csv", default=None, help="soak sample CSV (default: none)")
    ap.add_argument("--interval", type=float, default=30.0)
    ap.add_argument("--seconds", type=float, default=0,
                    help="stop after N seconds (0 = run until SIGTERM)")
    args = ap.parse_args(argv)

    if args.csv:
        threading.Thread(target=_sampler, args=(args.csv, args.interval),
                         daemon=True).start()

    import signal
    def _term(_s, _f):
        _STOP[0] = True
    signal.signal(signal.SIGTERM, _term)
    signal.signal(signal.SIGINT, _term)

    echo = _listener(args.echo_port)
    chat = _listener(args.chat_port)
    status = _listener(args.status_port)
    print("[canary] echo=%d chat=%d status=%d" %
          (args.echo_port, args.chat_port, args.status_port), flush=True)

    def root():
        runloom.fiber(lambda: _echo_server(echo))
        runloom.fiber(lambda: _chat_server(chat))
        runloom.fiber(lambda: _status_server(status))
        # deadline / shutdown watcher
        def watch():
            while not _STOP[0]:
                if args.seconds and (_time.monotonic() - _START) >= args.seconds:
                    _STOP[0] = True
                runloom_c.sched_sleep(0.5)
            for s in (echo, chat, status):
                try:
                    s.close()
                except OSError:
                    pass
        runloom.fiber(watch)
    runloom.fiber(root)
    runloom_c.run()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
