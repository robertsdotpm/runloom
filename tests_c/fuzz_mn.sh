#!/usr/bin/env bash
# fuzz_mn.sh -- seeded fuzz driver for tests_c/bench_mn.
#
# Sweeps seeds 1..MAX looking for a failure.  On failure, re-runs with
# PYGO_DEBUG_DIAG=all so the lifecycle event ring is dumped to stderr
# alongside self_check output.  The reproducing seed is printed last so
# subsequent debugging runs can be deterministic.
#
# Usage:
#   tests_c/fuzz_mn.sh [N] [H] [M] [MAX_SEEDS]
# Defaults: N=512 H=4 M=20 MAX_SEEDS=200
set -u

N=${1:-512}
H=${2:-4}
M=${3:-20}
MAX=${4:-200}
BENCH=$(dirname "$0")/bench_mn

if [ ! -x "$BENCH" ]; then
    echo "missing $BENCH -- run 'make -C tests_c bench_mn' first" >&2
    exit 2
fi

echo "[fuzz] N=$N H=$H M=$M  scanning seeds 1..$MAX"
for s in $(seq 1 "$MAX"); do
    if ! timeout 15 "$BENCH" "$N" "$H" "$M" "$s" > /tmp/fuzz_mn.$$.out 2>&1; then
        rc=$?
        echo "[fuzz] FAIL at seed=$s rc=$rc"
        cat /tmp/fuzz_mn.$$.out
        rm -f /tmp/fuzz_mn.$$.out
        echo
        echo "[fuzz] reproducing with PYGO_DEBUG_DIAG=all ..."
        PYGO_DEBUG_DIAG=all timeout 20 "$BENCH" "$N" "$H" "$M" "$s"
        echo "[fuzz] reproducer:  $BENCH $N $H $M $s"
        exit 1
    fi
done
rm -f /tmp/fuzz_mn.$$.out
echo "[fuzz] clean across $MAX seeds"
