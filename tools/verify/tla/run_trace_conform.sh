#!/usr/bin/env bash
# run_trace_conform.sh -- gate the TRACE-CONFORMANCE checks in check_all.
#
# Trace conformance replays the REAL extension's transition trace through a
# model's OWN actions under TLC, so it checks the actual binary against the
# actual spec (not the model against itself).  Today this lives only in two
# manual demos; this wraps them as a verify-phase "engine" so the model<->binary
# link is CI-ENFORCED -- a C change that regresses gilstate teardown or the baton
# protocol now turns check_all red instead of passing green.
#
# Conformed models (each demo runs the positive=CONFORMS check AND a negative
# control that MUST be flagged NON-CONFORMING -- so a passing demo proves teeth):
#   - RunloomGilstate (M4 / contract C6)   via RUNLOOM_GILSTATE_TRACE
#   - RunloomMNControl (controlled baton)  via RUNLOOM_MN_EVENTS
#   - RunloomWake (foreign-wake backstop)  via RUNLOOM_WAKE_TRACE
#   - RunloomMNWake (M:N hub-submit wake)  via RUNLOOM_MNWAKE_TRACE
#   - RunloomIouringWake (io_uring CQE wake / CQ-overflow heal) via RUNLOOM_IOUWAKE_TRACE
#
# SKIPS CLEANLY (prints a skip line, "0 passed, 0 failed", exits 0 -> contributes
# nothing) when a prerequisite is absent: java, the TLA jar, a free-threaded
# 3.13t python, or a built runloom_c -- exactly like the other verify engines
# skip an absent tool.  Prints "N passed, M failed" for run_verify.sh's
# eng_finish() to fold into the suite tally.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"        # tools/verify/tla
ROOT="$(cd "$HERE/../../.." && pwd)"         # tools/verify/tla -> repo root is THREE up
JAR="${TLA_JAR:-$HERE/tla2tools.jar}"
URL="https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.13.13t/bin/python3}"
RM="$(command -v safe-rm || echo rm)"

skip() { echo "-- trace conformance (model vs the REAL extension) --"; \
         echo "  SKIP: $1"; echo "  0 passed, 0 failed"; exit 0; }

command -v java >/dev/null 2>&1 || skip "java not found (TLC needs it)"
{ [ -x "$PY" ] || command -v "$PY" >/dev/null 2>&1; } \
    || skip "free-threaded 3.13t python not found ($PY) -- set RUNLOOM_PYTHON"
ls "$ROOT"/src/runloom_c*.so >/dev/null 2>&1 \
    || skip "runloom_c not built (python setup.py build_ext --inplace)"
# Ensure the TLA jar (same source as run_tla.sh).  Download to a unique temp then
# atomic rename, so a concurrent run_tla.sh fetch can't corrupt it.
if [ ! -f "$JAR" ]; then
    tmp="$(mktemp "$HERE/.jar.XXXXXX")"
    if curl -fsSL -o "$tmp" "$URL" 2>/dev/null && [ -s "$tmp" ]; then
        mv -f "$tmp" "$JAR"
    else
        $RM -f "$tmp"
    fi
fi
[ -f "$JAR" ] || skip "tla2tools.jar absent and could not fetch (offline?)"

echo "-- trace conformance (model vs the REAL extension run) --"
pass=0; fail=0
run_demo() {  # human-label  demo-script-relpath
    local label="$1" script="$2" out
    out="$(mktemp /tmp/traceconf.XXXX)"
    printf '  [conform] %-26s ' "$label"
    if RUNLOOM_PYTHON="$PY" bash "$ROOT/$script" >"$out" 2>&1; then
        echo "PASS"; pass=$((pass + 1))
    else
        echo "FAIL"; fail=$((fail + 1)); tail -10 "$out" | sed 's/^/        /'
    fi
    $RM -f "$out"
}

run_demo "gilstate (M4/C6)"  tools/trace_conform_demo.sh
run_demo "MNControl baton"   tools/mn_trace_conform_demo.sh
run_demo "Wake backstop"     tools/wake_trace_conform_demo.sh
run_demo "MN hub-submit wake" tools/mnwake_trace_conform_demo.sh
run_demo "io_uring CQE wake" tools/iouwake_trace_conform_demo.sh

echo "  $pass passed, $fail failed"
[ "$fail" -eq 0 ]
