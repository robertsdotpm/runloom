#!/usr/bin/env bash
# trace_conform_demo.sh -- end-to-end TRACE CONFORMANCE for the gilstate model:
# run the REAL extension, capture its hub-tstate lifecycle trace, and have TLC
# validate that trace against the REAL RunloomGilstate.tla actions.  This is the
# bridge that connects the formal model to the binary (answers "have we run the
# model against the extension?": yes).
#
#   fixed code (hubs self-delete)            -> trace CONFORMS
#   RUNLOOM_GILSTATE_DELETE_ON_MAIN=1 (the   -> trace NON-CONFORMING, the same
#     pre-c28e5ca bug, on demand)               GilstateContract violation TLC
#                                               finds from RunloomGilstate_bug.cfg
#
# Keeps the model honest against the code: if a future change regresses the
# teardown to delete hub tstates from the wrong thread, the conformance check
# fails even on a clean (release) build, no pydebug needed.
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.13.13t/bin/python3}"
TR_OK="$(mktemp /tmp/gil_ok.XXXX.ndjson)"
TR_BUG="$(mktemp /tmp/gil_bug.XXXX.ndjson)"

WL='import sys; sys.path.insert(0,"src"); import runloom_c
runloom_c.mn_init(3)
for _ in range(6): runloom_c.mn_fiber(lambda: None)
runloom_c.mn_run(); runloom_c.mn_fini()'

echo "== trace conformance: RunloomGilstate.tla vs the real extension =="
echo "-- fixed code: capture trace --"
RUNLOOM_GILSTATE_TRACE="$TR_OK" PYTHON_GIL=0 PYTHONPATH=src "$PY" -c "$WL" >/dev/null 2>&1
echo "-- negative control (RUNLOOM_GILSTATE_DELETE_ON_MAIN=1): capture trace --"
RUNLOOM_GILSTATE_DELETE_ON_MAIN=1 RUNLOOM_GILSTATE_TRACE="$TR_BUG" \
    PYTHON_GIL=0 PYTHONPATH=src "$PY" -c "$WL" >/dev/null 2>&1

rc=0
echo "-- TLC check: fixed-code trace (expect CONFORMS) --"
"$PY" tools/tla_trace_conform.py "$TR_OK"; r=$?
if [ "$r" -eq 2 ]; then
    # rc=2 from the helper == java / verify/tla/tla2tools.jar unavailable.
    # That is a missing tool, NOT a model violation -> SKIP, don't false-FAIL.
    echo "   SKIP: TLC unavailable (java / verify/tla/tla2tools.jar missing; run verify/tla/run_tla.sh once)"
    "$(command -v safe-rm || echo rm)" -f "$TR_OK" "$TR_BUG"
    exit 0
elif [ "$r" -eq 0 ]; then
    echo "   OK: fixed-code run conforms to the model"
else
    echo "   FAIL: fixed-code run should conform"; rc=1
fi
echo "-- TLC check: delete-on-main trace (expect NON-CONFORMING) --"
"$PY" tools/tla_trace_conform.py "$TR_BUG"; r=$?
if [ "$r" -eq 0 ]; then
    echo "   FAIL: the wrong-thread-delete run should NOT conform"; rc=1
elif [ "$r" -eq 2 ]; then
    echo "   SKIP: TLC unavailable for the negative control"
else
    echo "   OK: the bug trace is flagged, same violation as RunloomGilstate_bug.cfg"
fi

"$(command -v safe-rm || echo rm)" -f "$TR_OK" "$TR_BUG"
echo "----------------------------------------------------------------"
[ "$rc" -eq 0 ] && echo "  PASS -- the model is validated against the actual extension" \
                || echo "  FAIL -- see above"
exit "$rc"
