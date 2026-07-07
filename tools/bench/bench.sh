#!/usr/bin/env bash
# bench.sh -- quick rigorous microbench sweep (informational).
#
# Runs every workload through rigor.py and prints a 95% CI for each.  This is
# the `bench` phase of scripts/check_all.sh; it is opt-in and NOT part of the
# gating `all` set, because absolute throughput is machine-dependent.  It
# exits non-zero only if a workload actually crashes -- never on a number.
#
# Env: PYTHON, BENCH_RUNS (8), BENCH_INNER (3), BENCH_WARMUP (1),
#      BENCH_SCALE (per-workload default if unset).
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"

PY="${PYTHON:-}"
if [ -z "$PY" ]; then
    for c in "$HOME/.pyenv/versions/3.14.4t/bin/python3" python3.13t python3; do
        command -v "$c" >/dev/null 2>&1 && { PY="$c"; break; }
    done
fi
if [ -z "$PY" ]; then
    echo "  [bench] SKIP -- no python interpreter found"; exit 0
fi

RUNS="${BENCH_RUNS:-8}"; INNER="${BENCH_INNER:-3}"; WARMUP="${BENCH_WARMUP:-1}"
SCALE_ARG=""; [ -n "${BENCH_SCALE:-}" ] && SCALE_ARG="--scale ${BENCH_SCALE}"

rc=0
for w in spawn chan_pingpong chan_buffered yield_storm; do
    # shellcheck disable=SC2086
    "$PY" "$HERE/rigor.py" run "$w" --runs "$RUNS" --inner "$INNER" \
          --warmup "$WARMUP" $SCALE_ARG || rc=1
done
exit "$rc"
