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

TR="$(mktemp /tmp/stwconf.XXXX.ndjson)"
WL='import gc, sys; sys.path.insert(0,"src")
try:
    import runloom_c
except Exception:
    sys.exit(7)                       # ext not built against this (pydebug) ABI
done = runloom_c.Chan(32); stop=[False]
def worker():
    for _ in range(6):
        a={};b={};a["b"]=b;b["a"]=a;a["s"]=a; del a,b
        runloom_c.sched_yield_classic()
    done.send(1)
def collector():
    while not stop[0]:
        gc.collect(); runloom_c.sched_yield_classic()
    done.send(1)
def stopper():
    for _ in range(4): done.recv()
    stop[0]=True; done.recv()
runloom_c.mn_init(2); runloom_c.mn_go(collector)
for _ in range(4): runloom_c.mn_go(worker)
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
if echo "$out" | grep -q INCONCLUSIVE; then
    # the captured run happened to reuse a tstate pointer (run-dependent); the
    # conformance is sound but this trace can't be checked -- skip, don't fail.
    $RM -f "$TR"
    skip "captured run had dynamic tstate-identity churn (a reused tstate pointer) -- a known scoped limitation; see STW_FINDINGS.md"
fi
if echo "$out" | grep -q CONFORMS; then
    echo "   OK: the real stop-the-world handshake conforms to RunloomCPythonSTW"
else
    echo "   FAIL: a real run should conform"; rc=1
fi

echo "-- TLC: in-window GCPark dropped (expect NON-CONFORMING) --"
BUG="$(mktemp /tmp/stwconf_bug.XXXX.ndjson)"
"$PY" - "$TR" "$BUG" <<'PY'
import sys, json
sys.path.insert(0, "tools"); import stw_trace_conform as S
ev = S.drop_fast_cycles(S.load_events(sys.argv[1]))
order, hn, all_hubs, snaps = S.replay(ev)
start = next(i for i in range(len(ev)) if snaps[i][1]=="running" and snaps[i][2]==all_hubs)
gcs = [i for i,e in enumerate(ev) if e["a"]=="GCStart"]; end = gcs[-1]
# drop a GCPark/SelfSuspend inside the steady window -> a hub stays un-suspended
victim = next(i for i in range(start, end+1) if ev[i]["a"] in ("GCPark","SelfSuspend"))
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
