#!/usr/bin/env bash
# stw_trace_conform_demo.sh -- end-to-end TRACE CONFORMANCE for the STW model (M2):
# run the REAL extension under an instrumented --with-pydebug CPython, capture the
# actual stop_the_world handshake, and have TLC validate it against the REAL
# RunloomCPythonSTW.tla actions.  This is the "holy-shit" bridge -- conforming
# runloom's interaction against the HOST's own internal STW protocol.
#
#   real run                 -> CONFORMS (STWExclusive holds at every stopped state)
#   an in-window GCPark/Self  -> NON-CONFORMING (a hub left un-suspended while the
#     Suspend dropped             world is stopped -- the gc-churn UAF class)
#
# Requires the instrumented pydebug interp (apply verify/cpython_patches/
# pystate_stw_trace.patch + rebuild) AND a runloom_c built against its ABI.  SKIPS
# CLEANLY (exit 0) when that is not set up -- e.g. a normal stock-ABI build -- so
# it is safe to call from the gate; it only runs where the pydebug oracle lives.
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.13.13t/bin/python3}"
PYD="${RUNLOOM_PYDEBUG_PYTHON:-/home/x/projects/cpython-pydebug/python}"
RM="$(command -v safe-rm || echo rm)"

skip() { echo "== STW (M2) trace conformance =="; echo "  SKIP: $1"; exit 0; }

command -v java >/dev/null 2>&1 || skip "java not found (TLC needs it)"
[ -x "$PYD" ] || skip "no pydebug interp at $PYD (set RUNLOOM_PYDEBUG_PYTHON)"
[ -f "$ROOT/verify/tla/tla2tools.jar" ] || skip "tla2tools.jar not present (the verify phase fetches it; or run verify/tla/run_tla.sh once)"

TR="$(mktemp /tmp/stwconf.XXXX.ndjson)"
WL='import gc, sys; sys.path.insert(0,"src")
try:
    import runloom_c
except Exception:
    sys.exit(7)                       # ext not built against this (pydebug) ABI
NW=8; NC=2
done = runloom_c.Chan(NW+NC); stop=[False]
def worker():
    for _ in range(40):                   # long-lived: spans many STW cycles
        a={};b={};a["b"]=b;b["a"]=a;a["s"]=a; del a,b
        runloom_c.sched_yield_classic()
    done.send(1)
def collector():
    while not stop[0]:
        gc.collect(); runloom_c.sched_yield_classic()
    done.send(1)
def stopper():
    for _ in range(NW): done.recv()
    stop[0]=True
    for _ in range(NC): done.recv()
runloom_c.mn_init(3)
for _ in range(NC): runloom_c.mn_go(collector)
for _ in range(NW): runloom_c.mn_go(worker)
runloom_c.mn_go(stopper); runloom_c.mn_run(); runloom_c.mn_fini()'

echo "== STW (M2) trace conformance: RunloomCPythonSTW.tla vs the real handshake =="
echo "-- capture the real stop_the_world trace (instrumented pydebug interp) --"
RUNLOOM_STW_TRACE="$TR" PYTHON_GIL=0 "$PYD" -c "$WL" >/dev/null 2>&1
rc=$?
if [ "$rc" = 7 ] || ! grep -q '"a":"GCStopComplete"' "$TR" 2>/dev/null; then
    $RM -f "$TR"
    skip "no STW trace captured -- the pydebug interp isn't instrumented OR runloom_c isn't built against its ABI (apply pystate_stw_trace.patch + rebuild; build the ext with RUNLOOM_PYDEBUG_PYTHON)"
fi

rc=0
echo "-- TLC: real trace (expect CONFORMS) --"
out="$("$PY" tools/stw_trace_conform.py "$TR" 2>&1)"
echo "$out" | sed 's/^/   /'
if echo "$out" | grep -q CONFORMS; then
    echo "   OK: the real stop-the-world handshake conforms to RunloomCPythonSTW"
else
    echo "   FAIL: a real run should conform"; rc=1
fi

echo "-- TLC: an in-window suspend dropped (expect NON-CONFORMING) --"
BUG="$(mktemp /tmp/stwconf_bug.XXXX.ndjson)"
"$PY" - "$TR" "$BUG" <<'PY'
import sys, json
from collections import Counter
sys.path.insert(0, "tools"); import stw_trace_conform as S
raw = S.load_events(sys.argv[1]); raw.sort(key=lambda e: e.get("s", 0))
ev = S.drop_fast_cycles(raw)
# Drop a GCPark/SelfSuspend whose hub REAPPEARS later (so the tool can't infer it
# departed and Destroy it -- it is provably still present at its stop).  That hub
# then stays un-suspended at the stop, so the model cannot complete that
# GCStopComplete -> non-conforming.  Proves the conform really enforces "every
# present hub suspended before the world stops".
lastidx = {}
for i, e in enumerate(ev):
    lastidx[e["t"]] = i
susp = [i for i, e in enumerate(ev)
        if e["a"] in ("GCPark", "SelfSuspend") and lastidx[e["t"]] > i]
victim = susp[len(susp) // 2]
with open(sys.argv[2], "w") as f:
    for k, e in enumerate(ev):
        if k != victim:
            f.write(json.dumps(e) + "\n")
PY
if "$PY" tools/stw_trace_conform.py "$BUG" | sed 's/^/   /' | grep -q NON-CONFORMING; then
    echo "   OK: a hub left un-suspended at stop is rejected -- STWExclusive enforced against the code"
else
    echo "   FAIL: the corrupted (un-suspended-hub) trace should NOT conform"; rc=1
fi

$RM -f "$TR" "$BUG"
echo "----------------------------------------------------------------"
[ "$rc" -eq 0 ] && echo "  PASS -- the STW model is validated against the actual host handshake" \
                || echo "  FAIL -- see above"
exit "$rc"
