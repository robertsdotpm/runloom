"""serve(): a Python handler that RAISES before conn.close().

Audit gap (docs/dev/API_COVERAGE_GAPS.md #1, "serve() handler raises"): when a
Python handler passed to runloom_c.serve() raises an exception *before* it calls
conn.close(), m_serve_acceptor / runloom_g_entry catch the escaping exception and
report it via the unraisable hook ("Exception ignored ..."), and the acceptor
keeps serving other connections.  The audit flags what happens to the OFFENDING
connection: the acceptor already dropped its own conn reference
(module_io.c.inc L62), so the accepted TCPConn is kept alive only by the handler
fiber's `partial(handler, conn)` (g->callable) plus the captured traceback
(runloom_sched_core.c.inc L485-489: `g->error = value` with PyException_SetTraceback
-> the handler frame -> the `conn` local).  serve() never calls conn.close() on
the error path, so the fd is closed only when the accepted TCPConn is finally
released -- and that happens only when the handler goroutine struct is reaped
(Py_XDECREF(g->callable)+Py_XDECREF(g->error) at L952-954).

VERIFIED BEHAVIOUR (this test): a completed detached handler goroutine is NOT
reaped at its own completion -- it is reaped as a side effect of the hub later
running the NEXT goroutine on that runq (mn_sched_hub_main.c.inc L1303-1306), or
at session teardown.  So the LAST offending connection in a quiet burst has
nothing to trigger its reap: its conn is never released, its fd is never closed,
and its peer is STRANDED on a live connection for the whole lifetime of the
server -- while the acceptor keeps serving every other connection normally.  A
forced gc.collect() does NOT free it (confirmed: not GC-frame-pinning; it is the
un-reaped goroutine holding g->callable/g->error, hence conn).

We assert BOTH halves of correct behaviour:

  (a) subsequent connections still round-trip  -- the acceptor keeps serving;
  (b) EVERY offending client's socket sees EOF/close within a bounded time --
      the runtime does not strand the peer on a connection nobody will ever
      answer or close.

To measure (b) HONESTLY we take the offending client's bounded EOF-wait entirely
WHILE THE SERVER IS STILL ALIVE and otherwise quiet: the main fiber never tears
down the session during the measurement, so any EOF the peer sees must come from
serve() closing the conn on handler error -- not from session teardown
accidentally reaping the goroutine.  Under those conditions the tail offending
peer is deterministically stranded (its socket.settimeout expires with no EOF),
so (b) fails: that failure is the audited serve()-handler-raises bug.  Do not
weaken the assertion to make it green.

Bounded time is enforced three ways so a real strand surfaces as a failed
assertion, never a wedged process: each client socket carries settimeout(), the
serve() M:N session runs under a wall-clock hang_guard, and the whole thing runs
in a subprocess with an outer timeout.  serve() spins a full runloom.run(N) M:N
session (it needs >=2 hubs), so -- as in test_cov95_module_io.py -- each session
is driven in its own clean-exit subprocess: isolates scheduler/teardown state and
dodges the known multi-session mn_fini teardown flake.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from adv_util import needs_free_threading  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
FT = needs_free_threading()


def _run_subproc(script, timeout=120):
    """Run a serve() script in a clean subprocess (isolated M:N session, clean
    exit).  Skip -- not fail -- on a wall-clock timeout: that means the whole
    box is wedged (shared-box contention), not that this specific gap misbehaved
    (the in-script socket timeouts already turn a stranded-peer hang into a
    structured False result long before this outer timeout could fire)."""
    import subprocess
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    try:
        return subprocess.run([PY, "-c", script], cwd=REPO, env=env,
                              capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as e:
        pytest.skip("serve() workload timed out at the process level "
                    "(shared box under load): %s" % (e,))


# The handler recv()s one request, then -- for the poison request (first byte
# 'P') -- RAISES before ever calling conn.close(); anything else is echoed back
# and closed normally.  Making the raise depend on the REQUEST (not on accept
# order) keeps the offending vs good classification deterministic regardless of
# how the acceptor interleaves the concurrent connects.
#
# Two phases, to measure (b) honestly without a teardown-delivered EOF artifact:
#
#   Phase 1 (strand check):  NBAD offending clients connect + poison + wait for
#     EOF with settimeout(SOCK_TIMEOUT).  The main fiber keeps the server ALIVE
#     and otherwise QUIET (only sched_sleep, no listener close, no other conns)
#     until all NBAD have recorded a result.  A completed handler goroutine is
#     reaped only when the hub next runs another goroutine on that runq, so the
#     tail of a quiet burst has nothing to trigger its reap -> its conn is never
#     released -> its peer never gets EOF -> the socket times out.  Correct
#     behaviour is EVERY offending peer seeing EOF (stranded == 0).
#
#   Phase 2 (still-serving check):  only after phase 1 has fully recorded do the
#     NGOOD well-behaved clients run, proving the acceptor keeps serving.
#
# All socket I/O runs on real OS threads (genuine peers external to the M:N
# runtime); the main fiber only sched_sleeps and coordinates via Events, so it
# never blocks a hub.  Each socket carries settimeout() so a stranded peer
# returns in bounded time instead of hanging forever.
_SERVE_HANDLER_RAISES = r'''
import socket, sys, threading, time
sys.path.insert(0, "src")
sys.path.insert(0, "tests")
import runloom_c as rc, runloom
from adv_util import hang_guard

SOCK_TIMEOUT = 4.0          # per-recv ceiling: a stranded peer times out here
NBAD = 3                    # offending (handler-raises) connections, phase 1
NGOOD = 3                   # well-behaved (echo) connections, phase 2
lock = threading.Lock()
result = {"stranded": [], "secs": [], "good": [], "good_ok": None}

def main():
    def handler(conn):
        # A real Python handler.  Reads the request, and for the poison request
        # RAISES before ever calling conn.close() -- exactly the audited case.
        req = conn.recv(64)
        if req[:1] == b"P":
            raise RuntimeError("serve handler boom -- raised before conn.close()")
        conn.send_all(b"echo:" + req)
        conn.close()

    port, listeners = rc.serve("127.0.0.1", 0, handler, 1, 128)
    result["port"] = port
    start_good = threading.Event()

    def offending_client():
        def run():
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(SOCK_TIMEOUT)
                s.connect(("127.0.0.1", port))
                s.sendall(b"POISON")        # -> handler recv()s this, then raises
                t0 = time.monotonic()
                # Correct: serve() releases the conn -> TCPConn dealloc closes the
                # fd -> EOF (b"").  Broken: the fd is never closed -> recv blocks
                # to the socket timeout -> peer stranded on a live connection.
                data = s.recv(64)
                with lock:
                    result["stranded"].append(data != b"")   # non-EOF -> stranded
                    result["secs"].append(round(time.monotonic() - t0, 3))
                s.close()
            except socket.timeout:
                with lock:
                    result["stranded"].append(True)
                    result["secs"].append(round(SOCK_TIMEOUT, 3))
            except Exception as e:
                with lock:
                    result["stranded"].append("%s: %s" % (type(e).__name__, e))
        return run

    def good_client(i):
        def run():
            start_good.wait(30)             # phase 2: only after the strand check
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(SOCK_TIMEOUT)
                s.connect(("127.0.0.1", port))
                msg = ("hello-%d" % i).encode()
                s.sendall(msg)
                data = s.recv(64)
                with lock:
                    result["good"].append(data == b"echo:" + msg)
                s.close()
            except Exception as e:
                with lock:
                    result["good"].append("%s: %s" % (type(e).__name__, e))
        return run

    threads = [threading.Thread(target=offending_client(), daemon=True)
               for _ in range(NBAD)]
    threads += [threading.Thread(target=good_client(i), daemon=True)
                for i in range(NGOOD)]
    for t in threads:
        t.start()

    # Phase 1: keep the server alive and quiet until every offending client has
    # finished its bounded EOF-wait.  Main fiber only sched_sleeps (never blocks
    # a hub, never tears down the session mid-measurement).
    while True:
        with lock:
            done = len(result["stranded"]) >= NBAD
        if done:
            break
        rc.sched_sleep(0.02)

    # Phase 2: now prove the acceptor is still serving other connections.
    start_good.set()
    deadline = time.monotonic() + 20.0
    while time.monotonic() < deadline:
        with lock:
            done = len(result["good"]) >= NGOOD
        if done:
            break
        rc.sched_sleep(0.02)
    with lock:
        result["good_ok"] = (len(result["good"]) == NGOOD
                             and all(x is True for x in result["good"]))

    for t in threads:
        t.join(timeout=2.0)
    for L in listeners:
        try:
            L.close()
        except Exception:
            pass

with hang_guard(90, "serve_handler_raises", capture=True):
    runloom.run(3, main)

stranded = [x for x in result["stranded"] if x is not False]  # True or an errstr
n_stranded = len(stranded)
good_ok = bool(result["good_ok"])
print("RESULT good=%r good_ok=%r stranded=%d/%d strand_flags=%r secs=%r"
      % (result["good"], good_ok, n_stranded, NBAD,
         result["stranded"], result["secs"]))
# Exit codes let the parent distinguish the three outcomes precisely:
#   0  both halves correct (server keeps serving AND every offending peer got EOF)
#   7  server keeps serving but >=1 offending peer is stranded  -> the audited bug
#   3  anything else unexpected (e.g. the server stopped serving other conns)
if good_ok and n_stranded == 0 and len(result["stranded"]) == NBAD:
    print("SERVE_HANDLER_ERROR_OK"); sys.exit(0)
if good_ok and n_stranded >= 1:
    print("BUG_OFFENDING_PEER_STRANDED"); sys.exit(7)
print("UNEXPECTED"); sys.exit(3)
'''


@pytest.mark.skipif(not FT, reason="serve() needs the M:N runtime (GIL-off build)")
def test_serve_handler_raise_keeps_serving_and_closes_offending_peer():
    """serve() Python handlers that raise before conn.close() on the offending
    connections.  Correct behaviour, both asserted:

      (a) the acceptor keeps serving -- the well-behaved connections still
          round-trip (good_ok=True);
      (b) EVERY offending client's socket sees EOF within a bounded time -- serve()
          must release the conn on the handler-error path so TCPConn dealloc
          closes the fd; no offending peer is stranded on a live connection nobody
          will ever answer or close (stranded == 0).

    (a) holds in every run.  (b) is the audited bug: serve() never closes the
    conn on handler error and relies on the handler goroutine being reaped to
    release it (g->callable/g->error -> conn), but a completed detached goroutine
    is reaped only when the hub next runs another goroutine on its runq -- so the
    tail offending connection in a quiet burst is never reaped, its fd is never
    closed, and its peer is stranded for the server's lifetime.  The subprocess
    then exits 7 and the assertion below fails: that failure IS the repro; do not
    weaken it.
    """
    p = _run_subproc(_SERVE_HANDLER_RAISES, timeout=150)
    ctx = "rc=%d\nstdout=%s\nstderr=%s" % (
        p.returncode, p.stdout[-1800:], p.stderr[-800:])

    # (a) The server kept serving the well-behaved connections.  This must hold
    # regardless of the offending-peer outcome.
    assert "good_ok=True" in p.stdout, "server stopped serving other conns\n" + ctx

    # (b) Every offending client saw EOF/close within the bounded socket timeout.
    # A stranded peer prints stranded=N/NBAD (N>=1) -> exit 7.  This is the bug.
    assert p.returncode != 7, (
        "BUG (serve-handler-error): a serve() handler raised before conn.close() "
        "and the offending connection was NEVER closed -- its peer got no EOF "
        "within the socket timeout while the server was still alive and serving "
        "other conns (stranded forever on a live fd). serve() takes no conn.close() "
        "on the handler-error path; the conn stays pinned by the un-reaped handler "
        "goroutine (g->callable/g->error -> conn), and a completed detached "
        "goroutine is only reaped when the hub next runs another goroutine on its "
        "runq -- so the tail of a quiet burst is stranded for the server's "
        "lifetime.\n" + ctx)
    assert p.returncode == 0 and "SERVE_HANDLER_ERROR_OK" in p.stdout, ctx
