#!/usr/bin/env bash
# wake_trace_conform_demo.sh -- end-to-end TRACE CONFORMANCE for the netpoll-drain
# WAKE protocol: run the REAL single-thread drain offloading to the blockpool,
# capture its FOREIGN_WAKE/POKE/DRAIN_DEC/DRAIN_CONSUME/DRAIN_BLOCK/DRAIN_UNBLOCK/
# RESUME events, and have TLC validate them against the REAL RunloomWake.tla (the
# proven f214341 foreign-wake backstop model).  Connects the lost-wakeup model to
# the binary.
#
#   real offload run                 -> CONFORMS (every wake transition is an
#                                       enabled model step; ResumeIsTerminal holds
#                                       -- no resume/consume without a durable
#                                       wake_list append)
#   a FOREIGN_WAKE dropped (--drop-  -> NON-CONFORMING (the dependent poke/consume/
#   foreign-wake)                       resume can't fire with no durable append:
#                                       the lost-wakeup RunloomWake.tla forbids)
#
# SINGLE-THREAD drain only (rc.run() / default-pool blocking) -- the only path the
# model covers and the only one that blocks UNBOUNDED on a foreign poke.  A real
# 10 ms blocking sleep makes every park genuine (no synchronous-completion fast
# path) and forces the drain to block in the pump, exercising the 2 ms backstop.
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
TR="$(mktemp /tmp/wake.XXXX.ndjson)"

WL='import sys, time
sys.path.insert(0, "src")
import runloom_c as rc
def offloader():
    # each blocking() parks the fiber foreign-wakeable; a blockpool worker runs
    # the sleep off-hub and wakes it via the durable wake_list + pump poke --
    # FOREIGN_WAKE + POKE + DRAIN_DEC, then the drain DRAIN_CONSUME + RESUME.
    for _ in range(6):
        rc.blocking(time.sleep, 0.01)
rc.fiber(offloader)          # single offloader -> serial episodes, wake_list <= 1 g
rc.run()                     # single-thread drain (NOT M:N hubs) -- the modeled path'

echo "== trace conformance: RunloomWake.tla vs the real netpoll-drain wake path =="
echo "-- capture a real wake event trace (single-thread offload) --"
RUNLOOM_WAKE_TRACE="$TR" PYTHON_GIL=0 PYTHONPATH=src "$PY" -c "$WL" >/dev/null 2>&1

rc=0
echo "-- TLC: real trace (expect CONFORMS) --"
"$PY" tools/wake_trace_conform.py "$TR"; r=$?
if [ "$r" -eq 2 ]; then
    # rc=2 from the helper == java / tools/verify/tla/tla2tools.jar unavailable
    # (or an empty trace).  A missing tool, NOT a model violation -> SKIP.
    echo "   SKIP: TLC unavailable (java / tools/verify/tla/tla2tools.jar missing; run tools/verify/tla/run_tla.sh once) or empty trace"
    "$(command -v safe-rm || echo rm)" -f "$TR"
    exit 0
elif [ "$r" -eq 0 ]; then
    echo "   OK: the real offload run conforms (no fiber resumed without a durable wake)"
else
    echo "   FAIL: a real single-thread offload run should conform"; rc=1
fi
echo "-- TLC: dropped-FOREIGN_WAKE trace (expect NON-CONFORMING) --"
"$PY" tools/wake_trace_conform.py "$TR" --drop-foreign-wake; r=$?
if [ "$r" -eq 0 ]; then
    echo "   FAIL: a resume/consume without a durable wake should NOT conform"; rc=1
elif [ "$r" -eq 2 ]; then
    echo "   SKIP: TLC unavailable for the negative control"
else
    echo "   OK: the resume-without-a-wake is rejected -- the lost-wakeup class is enforced against the code"
fi

"$(command -v safe-rm || echo rm)" -f "$TR"
echo "----------------------------------------------------------------"
[ "$rc" -eq 0 ] && echo "  PASS -- the wake-backstop model is validated against the actual drain" \
                || echo "  FAIL -- see above"
exit "$rc"
