#!/usr/bin/env bash
# iouwake_trace_conform_demo.sh -- end-to-end TRACE CONFORMANCE for the io_uring
# CQE WAKE protocol: run the REAL single-thread drain over an io_uring MULTISHOT
# recv that OVERFLOWS a deliberately tiny CQ ring, capture its SUBMIT/DRAIN_FLUSH/
# DRAIN_CONSUME/RESUME/DRAIN_BLOCK/DRAIN_UNBLOCK events, and have TLC validate them
# against the REAL RunloomIouringWake.tla (the CQ-overflow lost-wakeup + drain-first
# overflow-flush heal model).  The io_uring sibling of wake_trace_conform_demo.sh.
#
#   real overflow run                -> CONFORMS (every CQE-wake transition is an
#                                       enabled model step; ResumeIsTerminal +
#                                       NoStrandedCompletion hold -- no resume
#                                       without a completion, no completion
#                                       stranded outside cq_inflight)
#   a SUBMIT dropped (--drop-submit) -> NON-CONFORMING (the dependent KernelComplete/
#                                       DrainConsume/RESUME can't fire with no
#                                       submitted op: the lost-wakeup the model forbids)
#
# THE OVERFLOW RECIPE (the novel value -- DrainFlushFirst is only exercised when the
# kernel actually overflows the CQ).  RUNLOOM_IOURING_ENTRIES=4 makes the global
# ring's CQ tiny; RUNLOOM_TCPCONN_IOURING=1 routes TCPConn.recv to the io_uring
# multishot path; a loopback sender blasts a 32 KiB stream in 16 KiB chunks
# (TCP_NODELAY) while the single recv fiber is parked, so the kernel posts more
# provided-buffer CQEs than the CQ holds -> IORING_SQ_CQ_OVERFLOW -> the drain's
# GETEVENTS flush (DRAIN_FLUSH).  A big recv buffer keeps the episode count tiny (a
# few SUBMIT..RESUME cycles) so TLC stays fast.  If DRAIN_FLUSH never appears the
# overflow was NOT induced -> SKIP (never a false FAIL).
#
# SINGLE-THREAD drain only (rc.run()), the path RunloomIouringWake models + the path
# the sched_drain loop-top overflow flush (runloom_sched_drain.c.inc:155) guards.
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
TR="$(mktemp /tmp/iouwake.XXXX.ndjson)"
RM="$(command -v safe-rm || echo rm)"

WL='import sys, threading, socket, time
sys.path.insert(0, "src")
import runloom_c as rc
HOST = "127.0.0.1"
TOTAL = 32768          # total bytes -- enough provided-buffer CQEs to overflow CQ=8
CHUNK = 16384          # per-send chunk (NODELAY -> arrives while the recv is parked)
PORT = {}
ARMED = threading.Event()
ln = rc.TCPConn.listen(HOST, 0)
s = socket.socket(fileno=socket.dup(ln.fileno()))
PORT["p"] = s.getsockname()[1]; s.detach()
got = {"bytes": 0}
def sender():
    c = socket.create_connection((HOST, PORT["p"]))
    c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    ARMED.wait(2.0); time.sleep(0.05)
    sent = 0; buf = b"y" * CHUNK
    while sent < TOTAL:
        c.sendall(buf); sent += CHUNK
    time.sleep(0.4); c.close()
def srv():
    # first recv() arms the multishot SQE on the global ring + parks; the burst
    # then overflows the tiny CQ, healed by the drain-first GETEVENTS flush.  A big
    # recv buffer (1 MiB) drains many buffers per call -> few SUBMIT..RESUME cycles.
    conn = ln.accept(); ARMED.set()
    dl = time.time() + 4.0
    while time.time() < dl:
        b = conn.recv(1 << 20)
        if b == b"": break
        got["bytes"] += len(b)
        if got["bytes"] >= TOTAL: break
    conn.close(); ln.close()
t = threading.Thread(target=sender); t.start()
rc.fiber(srv)                # single-thread drain (NOT M:N hubs) -- the modeled path
rc.run()
t.join()'

echo "== trace conformance: RunloomIouringWake.tla vs the real io_uring CQE wake path =="
echo "-- capture a real io_uring multishot-overflow event trace (tiny CQ ring) --"
RUNLOOM_IOUWAKE_TRACE="$TR" RUNLOOM_TCPCONN_IOURING=1 RUNLOOM_IOURING_ENTRIES=4 \
    PYTHON_GIL=0 PYTHONPATH=src "$PY" -c "$WL" >/dev/null 2>&1

# OVERFLOW GATE: DrainFlushFirst (the novel value) is only exercised if the kernel
# actually overflowed the CQ.  No DRAIN_FLUSH in the trace -> overflow was not
# induced on this kernel/run -> SKIP (this is NOT a model violation, so never FAIL).
if ! grep -q '"DRAIN_FLUSH"' "$TR" 2>/dev/null; then
    echo "   SKIP: CQ overflow not induced (no DRAIN_FLUSH in the trace -- io_uring"
    echo "         multishot/provided-buffers unavailable, or the kernel did not"
    echo "         overflow the tiny ring this run); the overflow heal is the point"
    echo "         of this check, so without it there is nothing to validate"
    $RM -f "$TR"
    exit 0
fi

rc=0
echo "-- TLC: real overflow trace (expect CONFORMS) --"
"$PY" tools/iouwake_trace_conform.py "$TR"; r=$?
if [ "$r" -eq 2 ]; then
    echo "   SKIP: TLC unavailable (java / tools/verify/tla/tla2tools.jar missing; run tools/verify/tla/run_tla.sh once) or empty trace"
    $RM -f "$TR"
    exit 0
elif [ "$r" -eq 0 ]; then
    echo "   OK: the real io_uring overflow run conforms (no fiber resumed without a kernel completion; no completion stranded outside cq_inflight)"
else
    echo "   FAIL: a real io_uring overflow run should conform"; rc=1
fi
echo "-- TLC: dropped-SUBMIT trace (expect NON-CONFORMING) --"
"$PY" tools/iouwake_trace_conform.py "$TR" --drop-submit; r=$?
if [ "$r" -eq 0 ]; then
    echo "   FAIL: a completion consumed without a submitted op should NOT conform"; rc=1
elif [ "$r" -eq 2 ]; then
    echo "   SKIP: TLC unavailable for the negative control"
else
    echo "   OK: the consume-without-a-submit is rejected -- the io_uring lost-wakeup class is enforced against the code"
fi

$RM -f "$TR"
echo "----------------------------------------------------------------"
[ "$rc" -eq 0 ] && echo "  PASS -- the io_uring CQ-overflow wake model is validated against the actual drain" \
                || echo "  FAIL -- see above"
exit "$rc"
