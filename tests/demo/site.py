"""A small website on top of mnweb, exercising the M:N sync runtime.

What it does (all on the M:N scheduler, GIL off):

  * GET  /          HTML landing page: host IP, hostname, visitor count,
                    requests served, uptime, hub count.  Counts as a visit.
  * GET  /ip        the host's primary IP address (plain text).
  * GET  /count     increments + returns the visitor counter (JSON).
  * POST /visit     records a named visit (body = visitor name).
  * GET  /health    cheap liveness probe -- no DB, no counter (for the watchdog).
  * GET  /stats     JSON: scheduler stats + per-hub state + app counters.
  * GET  /slow      cooperative sleep, then ok (exercises the timer path).

Every request is logged to an access log file AND written to a local
sqlite database by a single dedicated "db writer" goroutine fed over a
channel -- the Go-idiomatic "one owner per resource" pattern.  The writes
are genuinely blocking (they park the owning hub thread on disk I/O).

The visitor / request counters are shared mutable state touched from
every hub thread, so with the GIL off they MUST be guarded -- a cooperative
runloom.sync.Lock does that here.

Crash + hang diagnostics are armed at startup:
  * runloom_c.install_crash_handler  -- fatal-signal reporter -> crash file + core
  * runloom_c.install_traceback_signal -- `kill -QUIT` dumps all goroutines
  * faulthandler                      -- Python-level backstop
  * a heartbeat goroutine             -- writes run/health.json every 2s so an
                                         external watchdog can spot a wedge.
"""
import faulthandler
import json
import os
import socket
import sqlite3
import sys
import time

import runloom_c
import runloom.sync as sync

import mnweb

RUNDIR = os.environ.get("SITE_RUNDIR", os.path.join(os.path.dirname(__file__), "run"))
DB_PATH = os.environ.get("SITE_DB", os.path.join(RUNDIR, "site.db"))
ACCESS_LOG = os.path.join(RUNDIR, "site-access.log")
HEALTH_JSON = os.path.join(RUNDIR, "health.json")
CRASH_REPORT = os.path.join(RUNDIR, "crash_report.txt")
FAULT_LOG = os.path.join(RUNDIR, "faulthandler.log")

START_TIME = time.time()
HOSTNAME = socket.gethostname()


def primary_ip():
    """Best-effort primary outbound IP (no packets actually sent)."""
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect(("8.8.8.8", 80))
        return probe.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        probe.close()


HOST_IP = primary_ip()


class Counters:
    """Shared counters, lock-guarded for GIL-off correctness."""
    def __init__(self, visitors=0):
        self.lock = sync.Lock()
        self.visitors = visitors
        self.requests = 0

    def add_request(self):
        with self.lock:
            self.requests += 1
            return self.requests

    def add_visit(self):
        with self.lock:
            self.visitors += 1
            return self.visitors

    def snapshot(self):
        with self.lock:
            return self.visitors, self.requests


# A write job is (kind, params).  kind in {"request", "visit", "flush"}.
db_chan = runloom_c.Chan(4096)
counters = Counters()
access_fp = None


def init_db_and_counters():
    """Set up the schema and recover the visitor count before serving."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS requests("
        "id INTEGER PRIMARY KEY, ts REAL, method TEXT, path TEXT, "
        "addr TEXT, status INTEGER, elapsed_ms REAL)")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS visits("
        "id INTEGER PRIMARY KEY, ts REAL, name TEXT, addr TEXT)")
    conn.commit()
    visits = conn.execute("SELECT COUNT(*) FROM visits").fetchone()[0]
    conn.close()
    return visits


def db_writer():
    """Single owner of the sqlite connection.  Blocking writes, batched
    commits.  Runs forever as one goroutine; the channel serialises all
    access so no cross-hub sqlite sharing ever happens."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    pending = 0
    while True:
        item, ok = db_chan.recv()
        if not ok:
            break
        kind, params = item
        if kind == "request":
            conn.execute(
                "INSERT INTO requests(ts, method, path, addr, status, elapsed_ms) "
                "VALUES (?, ?, ?, ?, ?, ?)", params)
            pending += 1
        elif kind == "visit":
            conn.execute(
                "INSERT INTO visits(ts, name, addr) VALUES (?, ?, ?)", params)
            pending += 1
        elif kind == "flush":
            pass
        if pending and (kind == "flush" or pending >= 64):
            conn.commit()            # blocking disk write
            pending = 0
    conn.commit()
    conn.close()


def db_flusher():
    """Wake the writer every 250ms so pending rows commit even when idle."""
    while True:
        runloom_c.sched_sleep(0.25)
        try:
            db_chan.try_send(("flush", None))
        except Exception:
            pass


def record_request(req, resp, elapsed_ms):
    """after_request hook: access log line + a (blocking) DB write."""
    addr = req.addr[0] if req.addr else "?"
    n = counters.add_request()
    line = "{:.3f} {} {} {} {} {} {:.2f}ms #{}\n".format(
        time.time(), addr, req.method, req.path, req.version, resp.status,
        elapsed_ms, n)
    if access_fp is not None:
        access_fp.write(line)
        access_fp.flush()
    # Hand the row to the db writer.  try_send avoids parking the request
    # goroutine if the writer is momentarily behind; drop-on-full keeps the
    # site responsive under a burst (the count of drops is observable via
    # the channel length in /stats).
    db_chan.try_send(("request", (time.time(), req.method, req.path, addr,
                                  resp.status, elapsed_ms)))


def probe_example():
    """Fetch example.com over the M:N sync egress path.  We don't use the
    result -- it just exercises DNS + cooperative connect + recv."""
    status, body = mnweb.fetch("example.com", "/", port=80)
    print("[site] example.com probe -> status={} bytes={}".format(
        status, len(body)), flush=True)


def heartbeat():
    """Write run/health.json every 2s.  A stale file == a wedged scheduler."""
    while True:
        visitors, requests = counters.snapshot()
        try:
            stats = runloom_c.stats()
        except Exception as exc:
            stats = {"error": repr(exc)}
        try:
            hubs = runloom_c.mn_hub_states()
        except Exception as exc:
            hubs = [{"error": repr(exc)}]
        payload = {
            "ts": time.time(),
            "pid": os.getpid(),
            "uptime_s": time.time() - START_TIME,
            "visitors": visitors,
            "requests_served": requests,
            "db_queue": len(db_chan),
            "stats": stats,
            "hubs": hubs,
        }
        tmp = HEALTH_JSON + ".tmp"
        with open(tmp, "w") as fp:
            json.dump(payload, fp)
        os.replace(tmp, HEALTH_JSON)
        runloom_c.sched_sleep(2.0)


# --------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------
app = mnweb.App()
app.after_request(record_request)


PAGE = """<!doctype html>
<html><head><title>mnweb demo</title>
<style>body{{font-family:system-ui,sans-serif;max-width:42rem;margin:3rem auto;line-height:1.6}}
code{{background:#f0f0f0;padding:.1rem .3rem;border-radius:.2rem}}</style></head>
<body>
<h1>mnweb &mdash; runloom M:N sync demo</h1>
<p>Served by <code>{host}</code> at <code>{ip}</code> across <code>{hubs}</code> hub threads
(GIL off, backend <code>{backend}</code>/<code>{netpoll}</code>).</p>
<ul>
  <li>Visitors: <strong>{visitors}</strong></li>
  <li>Requests served: <strong>{requests}</strong></li>
  <li>Uptime: <strong>{uptime:.0f}s</strong></li>
</ul>
<p>Endpoints: <code>/ip</code> <code>/count</code> <code>/stats</code> <code>/slow</code> <code>/health</code>
&middot; <code>POST /visit</code></p>
</body></html>"""


@app.route("/")
def index(req):
    n = counters.add_visit()
    db_chan.try_send(("visit", (time.time(), "anon",
                                req.addr[0] if req.addr else "?")))
    html = PAGE.format(host=HOSTNAME, ip=HOST_IP, hubs=runloom_c.mn_hub_count(),
                       backend=runloom_c.backend(), netpoll=runloom_c.netpoll_backend(),
                       visitors=n, requests=counters.snapshot()[1],
                       uptime=time.time() - START_TIME)
    return mnweb.Response(html, content_type="text/html; charset=utf-8")


@app.route("/ip")
def ip(req):
    return HOST_IP + "\n"


@app.route("/count")
def count(req):
    n = counters.add_visit()
    db_chan.try_send(("visit", (time.time(), "counter",
                                req.addr[0] if req.addr else "?")))
    return mnweb.Response(json.dumps({"visitors": n}) + "\n",
                          content_type="application/json")


@app.route("/visit", methods=("POST",))
def visit(req):
    name = req.body.decode("utf-8", "replace").strip()[:120] or "anon"
    n = counters.add_visit()
    db_chan.try_send(("visit", (time.time(), name,
                                req.addr[0] if req.addr else "?")))
    return mnweb.Response(json.dumps({"visitors": n, "name": name}) + "\n",
                          content_type="application/json")


@app.route("/health")
def health(req):
    # Deliberately cheap: no DB, no counter increment, no allocation games.
    return "ok\n"


@app.route("/slow")
def slow(req):
    runloom_c.sched_sleep(0.5)
    return "slept 0.5s\n"


@app.route("/stats")
def stats(req):
    visitors, requests = counters.snapshot()
    body = {
        "host": HOSTNAME, "ip": HOST_IP,
        "visitors": visitors, "requests_served": requests,
        "uptime_s": time.time() - START_TIME,
        "db_queue": len(db_chan),
        "scheduler": runloom_c.stats(),
        "hubs": runloom_c.mn_hub_states(),
    }
    return mnweb.Response(json.dumps(body, default=str) + "\n",
                          content_type="application/json")


def register_debug_routes():
    """Fault-injection routes, registered only when DEMO_ALLOW_CRASH=1.
    Used to validate the crash/hang detection pipeline on demand; never
    enabled in a normal run."""
    @app.route("/debug/segv")
    def debug_segv(req):
        # A wild-pointer read -> genuine SIGSEGV the crash handler cannot
        # recover (no guard-page classification): process dies + cores.
        # Exercises the supervisor's CRASH path.
        import ctypes
        ctypes.string_at(1)
        return "unreachable\n"

    @app.route("/debug/crash")
    def debug_crash(req):
        # A goroutine stack overflow -> guard-page SIGSEGV the handler
        # classifies as handoff-recoverable: the process survives with a
        # stranded hub (service dead, heartbeat alive).  Exercises the
        # supervisor's HANG/wedge path.
        runloom_c._crash_selftest_overflow()
        return "unreachable\n"

    @app.route("/debug/wedge")
    def debug_wedge(req):
        # Non-cooperative blocking that pins a hub thread.  Enough concurrent
        # hits (>= hub count) starve the scheduler -> looks like a hang.
        secs = float(req.query.split("=")[-1]) if "=" in req.query else 120.0
        time.sleep(secs)        # real OS sleep: does NOT yield the hub
        return "woke\n"


def arm_diagnostics():
    # NOTE: deliberately do NOT call faulthandler.enable() here.  runloom's
    # crash handler chains out by restoring the PREVIOUS signal disposition
    # and re-executing the faulting instruction; if faulthandler owns that
    # disposition, under the multithreaded M:N runtime the chain-out does not
    # reliably terminate -- faulting threads park in the handler's re-entrancy
    # guard forever (a wedge, no core).  Leaving SIG_DFL as the chain target
    # makes a fault dump the goroutine registry + native backtrace and then
    # core + die cleanly, which is what we want for autonomous restarts.  The
    # Python stack is still recoverable from the core via `py-bt` in gdb.
    runloom_c.set_introspect_timestamps(True)
    level = os.environ.get("RUNLOOM_CRASH", "goroutine,backtrace")
    runloom_c.install_crash_handler(level, CRASH_REPORT)
    runloom_c.install_traceback_signal()        # kill -QUIT -> goroutine dump
    print("[site] diagnostics armed (crash handler={}, level={}, traceback signal=SIGQUIT)".format(
        runloom_c.crash_handler_installed(), level), flush=True)


def main():
    global access_fp
    os.makedirs(RUNDIR, exist_ok=True)
    access_fp = open(ACCESS_LOG, "a")
    counters.visitors = init_db_and_counters()
    arm_diagnostics()
    if os.environ.get("DEMO_ALLOW_CRASH"):
        register_debug_routes()
        print("[site] debug fault-injection routes ENABLED (/debug/segv, /debug/crash, /debug/wedge)", flush=True)

    host = os.environ.get("SITE_HOST", "127.0.0.1")
    port = int(os.environ.get("SITE_PORT", "8080"))
    hubs = int(os.environ.get("SITE_HUBS", "4"))
    print("[site] host={} ip={} visitors(restored)={}".format(
        HOSTNAME, HOST_IP, counters.visitors), flush=True)
    # probe_example runs once at startup then every 10 minutes.
    app.run(host, port, hubs=hubs,
            background_goroutines=(db_writer, db_flusher, heartbeat,
                                   probe_example,
                                   mnweb.every(600, probe_example)))


if __name__ == "__main__":
    main()
