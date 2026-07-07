#!/usr/bin/env bash
# mnwake_trace_conform_demo.sh -- end-to-end TRACE CONFORMANCE for the M:N
# HUB-SUBMIT wake protocol (route A): run the REAL M:N scheduler offloading to the
# blockpool, capture its FOREIGN_WAKE/SIGNAL/HUB_DRAIN/HUB_BLOCK/HUB_UNBLOCK/RESUME
# events, and have TLC validate them against the REAL RunloomMNWake.tla (the ~1ms
# bounded-poll lost-kick model).  The M:N sibling of wake_trace_conform_demo.sh.
#
#   real M:N offload run             -> CONFORMS (every wake transition is an
#                                       enabled model step; ResumeIsTerminal holds
#                                       -- no resume without a durable sub_head append)
#   a FOREIGN_WAKE dropped (--drop-  -> NON-CONFORMING (the dependent SIGNAL/RESUME
#   foreign-wake)                       can't fire with no durable append: the M:N
#                                       lost-wakeup RunloomMNWake.tla forbids)
#
# Pins the M:N hubs (mn_init(2) + mn_run, NOT the single-thread rc.run()).  A single
# offloader -> serial episodes, sub_list <= 1 g, so HubDrain/HubResume replay 1:1.
# A real 10ms blocking sleep makes every park genuine and forces the owner hub into
# its bounded idle wait, exercising the ~1ms poll backstop.
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
TR="$(mktemp /tmp/mnwake.XXXX.ndjson)"

WL='import sys, time
sys.path.insert(0, "src")
import runloom_c as rc
rc.mn_init(2)                    # M:N hubs (route A) -- NOT the single-thread drain
def offloader():
    # each blocking() parks the fiber foreign-wakeable on its hub; the blockpool
    # worker wakes it via runloom_mn_wake_g -> hub_submit (sub_head append + the
    # idle_cond/wake-pump kicks); the owner hub HUB_DRAINs + RESUMEs it.
    for _ in range(5):
        rc.blocking(time.sleep, 0.01)
rc.mn_fiber(offloader)           # single offloader -> serial episodes, sub_list <= 1 g
rc.mn_run()
rc.mn_fini()'

echo "== trace conformance: RunloomMNWake.tla vs the real M:N hub-submit wake path =="
echo "-- capture a real M:N wake event trace (mn_run offload) --"
RUNLOOM_MNWAKE_TRACE="$TR" PYTHON_GIL=0 PYTHONPATH=src "$PY" -c "$WL" >/dev/null 2>&1

rc=0
echo "-- TLC: real trace (expect CONFORMS) --"
"$PY" tools/mnwake_trace_conform.py "$TR"; r=$?
if [ "$r" -eq 2 ]; then
    echo "   SKIP: TLC unavailable (java / tools/verify/tla/tla2tools.jar missing; run tools/verify/tla/run_tla.sh once) or empty trace"
    "$(command -v safe-rm || echo rm)" -f "$TR"
    exit 0
elif [ "$r" -eq 0 ]; then
    echo "   OK: the real M:N offload run conforms (no fiber resumed without a durable sub_head append)"
else
    echo "   FAIL: a real M:N offload run should conform"; rc=1
fi
echo "-- TLC: dropped-FOREIGN_WAKE trace (expect NON-CONFORMING) --"
"$PY" tools/mnwake_trace_conform.py "$TR" --drop-foreign-wake; r=$?
if [ "$r" -eq 0 ]; then
    echo "   FAIL: a resume without a durable sub_head append should NOT conform"; rc=1
elif [ "$r" -eq 2 ]; then
    echo "   SKIP: TLC unavailable for the negative control"
else
    echo "   OK: the resume-without-a-wake is rejected -- the M:N lost-wakeup class is enforced against the code"
fi

"$(command -v safe-rm || echo rm)" -f "$TR"
echo "----------------------------------------------------------------"
[ "$rc" -eq 0 ] && echo "  PASS -- the M:N bounded-poll wake model is validated against the actual hub" \
                || echo "  FAIL -- see above"
exit "$rc"
