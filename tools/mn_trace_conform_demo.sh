#!/usr/bin/env bash
# mn_trace_conform_demo.sh -- end-to-end TRACE CONFORMANCE for the controlled
# baton: run the REAL scheduler, capture its Arrive/Rendezvous/Grant/Release
# events, and have TLC validate them against the REAL RunloomMNControl.tla.
# Connects runloom's core scheduler model to the binary.
#
#   real controlled-scheduler run   -> CONFORMS (MutualExclusion + BatonConsistent
#                                       + DeterministicGrant hold)
#   a Release dropped from the trace -> NON-CONFORMING (the next Grant can't fire
#                                       with the baton still held -- the double-hold
#                                       MutualExclusion forbids)
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
TR="$(mktemp /tmp/baton.XXXX.ndjson)"
TR_BUG="$(mktemp /tmp/baton_bug.XXXX.ndjson)"

WL='import sys; sys.path.insert(0,"src"); import runloom_c
runloom_c.mn_init(3)
ch = runloom_c.Chan()
def recv():
    while True:
        v, ok = ch.recv()
        if not ok: break
for _ in range(4): runloom_c.mn_fiber(recv)
def prod():
    for v in range(8): ch.send(v)
    ch.close()
runloom_c.mn_fiber(prod)
runloom_c.mn_run(); runloom_c.mn_fini()'

echo "== trace conformance: RunloomMNControl.tla vs the real controlled baton =="
echo "-- capture a real baton event trace (seeded controlled scheduler) --"
RUNLOOM_MN_SEED="${RUNLOOM_MN_SEED:-7}" RUNLOOM_MN_EVENTS="$TR" \
    PYTHON_GIL=0 PYTHONPATH=src "$PY" -c "$WL" >/dev/null 2>&1
# negative control: drop the first Release -> the holder keeps the baton
awk 'BEGIN{d=0} /"a":"Release"/ && d==0 {d=1; next} {print}' "$TR" > "$TR_BUG"

rc=0
echo "-- TLC: real trace (expect CONFORMS) --"
"$PY" tools/mn_trace_conform.py "$TR"; r=$?
if [ "$r" -eq 2 ]; then
    # rc=2 from the helper == java / tools/verify/tla/tla2tools.jar unavailable.
    # That is a missing tool, NOT a model violation -> SKIP, don't false-FAIL.
    echo "   SKIP: TLC unavailable (java / tools/verify/tla/tla2tools.jar missing; run tools/verify/tla/run_tla.sh once)"
    "$(command -v safe-rm || echo rm)" -f "$TR" "$TR_BUG"
    exit 0
elif [ "$r" -eq 0 ]; then
    echo "   OK: the real baton run conforms (at most one hub holds the baton)"
else
    echo "   FAIL: a real controlled-scheduler run should conform"; rc=1
fi
echo "-- TLC: dropped-Release trace (expect NON-CONFORMING) --"
"$PY" tools/mn_trace_conform.py "$TR_BUG"; r=$?
if [ "$r" -eq 0 ]; then
    echo "   FAIL: a double-hold should NOT conform"; rc=1
elif [ "$r" -eq 2 ]; then
    echo "   SKIP: TLC unavailable for the negative control"
else
    echo "   OK: the double-hold is rejected -- MutualExclusion enforced against the code"
fi

"$(command -v safe-rm || echo rm)" -f "$TR" "$TR_BUG"
echo "----------------------------------------------------------------"
[ "$rc" -eq 0 ] && echo "  PASS -- the baton model is validated against the actual scheduler" \
                || echo "  FAIL -- see above"
exit "$rc"
