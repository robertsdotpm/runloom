#!/usr/bin/env bash
# racerd.sh -- compositional STATIC race + memory-safety analysis with Infer.
#
# runloom already hunts races dynamically (ThreadSanitizer on the whole ext, real
# threads) and proves the lock-free algorithms in tools/verify/.  Infer adds a third
# angle that needs neither a running binary nor the racy interleaving to occur:
#
#   RacerD -- compositional static race detection.  Reasons per-procedure about
#             which memory is accessed under which locks and flags unprotected
#             concurrent access, INTERPROCEDURALLY and without executing.  It
#             finds races on paths a dynamic run never schedules.
#               Blackshear, Gorogiannis, O'Hearn, Sergey, "RacerD: Compositional
#               Static Race Detection", OOPSLA 2018.
#   Pulse  -- Infer's memory-safety analysis (use-after-free, null-deref, leaks)
#             over the same capture -- complements ASan/UBSan statically.
#
# Complementary to TSan: TSan is sound-ish but only sees executed interleavings;
# RacerD is interprocedural and path-insensitive, so each catches what the other
# misses.  Advisory (lock-discipline heuristics produce some false positives on
# hand-rolled C atomics) -- read the report, don't gate on it.
#
# Infer is a prebuilt binary (not in apt); this skips cleanly when absent.
# Install: https://github.com/facebook/infer/releases  (untar, add bin/ to PATH)
# Run:     tools/racerd.sh
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY="${PYTHON:-$HOME/.pyenv/versions/3.13.13t/bin/python3}"
command -v "$PY" >/dev/null 2>&1 || PY=python3

if ! command -v infer >/dev/null 2>&1; then
    echo "[infer] not installed -- skipping (https://github.com/facebook/infer/releases)"
    exit 0
fi

OUT="${INFER_OUT:-$ROOT/infer-out}"
echo "[infer] capturing the runloom_c ext build and running RacerD + Pulse"
# A clean rebuild so Infer's compiler wrapper captures every translation unit.
"$(command -v safe-rm || echo rm)" -rf build "$OUT" 2>/dev/null
if ! infer run --racerd --pulse --results-dir "$OUT" -- \
        "$PY" setup.py build_ext --inplace --force >/dev/null 2>&1; then
    echo "[infer] capture/analyze failed -- see $OUT (advisory phase, not gating)"
    exit 0
fi

echo "[infer] report ($OUT/report.txt):"
if [ -s "$OUT/report.txt" ]; then
    grep -E 'src/runloom_c/' "$OUT/report.txt" | head -60 || head -60 "$OUT/report.txt"
    n="$(grep -c . "$OUT/report.txt" 2>/dev/null || echo 0)"
    echo "[infer] $n report line(s).  Advisory only -- triage src/runloom_c/* findings;"
    echo "        hand-rolled C11 atomics will draw lock-discipline false positives."
else
    echo "[infer] no issues reported."
fi
exit 0
