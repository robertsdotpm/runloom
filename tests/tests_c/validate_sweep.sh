#!/usr/bin/env bash
# Validation sweep: run bench_mn at H=4/8/16/32, many seeds, under -P
# oversubscription with a per-run timeout.  rc=124 => hang (the residual
# lost-wake).  Any other nonzero => crash/fail.  Target: 0 hangs.
set -u
BIN=$(pwd)/tests_c/bench_mn
PER_H=${1:-3000}; P=${2:-12}; WD=${3:-15}
total_hang=0; total_fail=0
for H in 4 8 16 32; do
    echo "=== H=$H : $PER_H runs, -P$P, ${WD}s watchdog ==="
    res=$(seq 1 "$PER_H" | xargs -P"$P" -I{} bash -c \
        'timeout '"$WD"' '"$BIN"' 1024 '"$H"' 5 {} >/dev/null 2>&1; rc=$?; if [ $rc -ne 0 ]; then echo "$rc {}"; fi')
    hangs=$(echo -n "$res" | grep -c '^124 ' || true)
    fails=$(echo -n "$res" | grep -vc '^124 ' || true)
    [ -z "$res" ] && fails=0
    echo "  hangs(rc=124)=$hangs  other-fails=$fails"
    [ -n "$res" ] && echo "$res" | head -20
    total_hang=$((total_hang+hangs))
    [ -n "$res" ] && total_fail=$((total_fail + $(echo "$res" | grep -vc '^124 ')))
done
echo "==== TOTAL hangs=$total_hang ===="
