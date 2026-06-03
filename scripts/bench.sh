#!/usr/bin/env sh
# bench.sh -- run the pygo benchmark suite in the cleanest env this box allows,
# write JSON + a dated report, and gate each suite against its committed
# baseline. This is the LOCAL perf gate (we have no hosted CI -- see CLAUDE.md),
# the perf-side analogue of scripts/check_all.sh.
#
# Free-threaded 3.13t with the GIL forced off; ASLR off (setarch -R, inherited
# across the harness's gil=0 re-exec) for layout-stable numbers; the harness
# pins to one NUMA node itself.
#
# Usage:
#   scripts/bench.sh                 # micro + mn, gate vs committed baseline
#   PYGO_BENCH_NOGATE=1 scripts/bench.sh    # run + report, don't fail on regress
#   PYTHON=~/.pyenv/versions/3.13.13t/bin/python3 scripts/bench.sh
set -eu
REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"
PY="${PYTHON:-python3}"
RESULTS="bench/results"
REPORT_DIR="$RESULTS/reports"
mkdir -p "$REPORT_DIR"
STAMP="$(date -u +%Y%m%d-%H%M%S)"
REPORT="$REPORT_DIR/bench-$STAMP.md"
# 15% default: even with >25ms samples the shared-VM noise floor is ~6-8% on
# the fastest micros, so a tighter gate false-positives. Use interleaved A/B
# (like the F6a TLS A/B) for smaller, real deltas.
TOL="${PYGO_BENCH_TOL:-0.15}"

RUN="env PYTHONPATH=src PYTHON_GIL=0"
SETARCH=""
command -v setarch >/dev/null 2>&1 && SETARCH="setarch -R"

printf '# pygo bench run %s\n\n' "$STAMP" | tee "$REPORT"
rc=0
for suite in micro mn; do
    printf '>> bench.%s\n' "$suite"
    # snapshot the committed baseline BEFORE the harness overwrites it in-tree
    base="/tmp/pygo_base_$suite.json"
    have_base=0
    git show "HEAD:$RESULTS/$suite.json" > "$base" 2>/dev/null && have_base=1
    $SETARCH $RUN "$PY" -m "bench.$suite" | tee -a "$REPORT"
    if [ "$have_base" = 1 ]; then
        printf '\n## %s regression gate (min_s, tol %s)\n' "$suite" "$TOL" | tee -a "$REPORT"
        if $RUN "$PY" -m bench.regress "$base" "$RESULTS/$suite.json" \
               --metric min_s --tol "$TOL" | tee -a "$REPORT"; then :; else rc=1; fi
    fi
    printf '\n' | tee -a "$REPORT"
done

printf 'report: %s\n' "$REPORT"
if [ "$rc" = 1 ] && [ "${PYGO_BENCH_NOGATE:-0}" != 1 ]; then
    printf 'PERF GATE: regression detected (set PYGO_BENCH_NOGATE=1 to ignore)\n'
    exit 1
fi
printf 'PERF GATE: ok\n'
