#!/usr/bin/env bash
# coz_profile.sh -- causal (virtual-speedup) profiling of runloom with Coz.
#
# A normal profiler tells you where wall-time accumulates; in a concurrent
# runtime that is almost never where the bottleneck is.  Coz answers the
# question that actually matters -- "which code, if I made it faster, would
# make the WHOLE PROGRAM faster?" -- by running controlled virtual-speedup
# experiments and measuring the effect on a progress point.
#   Curtsinger & Berger, "Coz: Finding Code that Counts with Causal
#   Profiling", SOSP 2015.
#
# This is uniquely well suited to a scheduler: the park/wake, run-queue and
# channel paths interleave, so accumulated self-time is misleading.
# target.py marks each unit of work as a Coz progress point.
#
# For line-level virtual speedups inside runloom_c the .so should carry debug
# info (build with CFLAGS="-g -gdwarf-4"); Coz still works without it at
# coarser granularity.
#
# Install: https://github.com/plasma-umass/coz  (the `coz` binary) and
#          `pip install coz` (the Python progress-point shim).
# Run:     tools/bench/profile/coz_profile.sh
# View:    open coz-profile in https://plasma-umass.org/coz/
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
PY="${PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
command -v "$PY" >/dev/null 2>&1 || PY=python3

if ! command -v coz >/dev/null 2>&1; then
    echo "[coz] not installed -- skipping (see https://github.com/plasma-umass/coz)"
    exit 0
fi

OUT="${COZ_OUTPUT:-coz-profile}"
echo "[coz] causal profiling target.py -> $OUT (view at https://plasma-umass.org/coz/)"
PYTHON_GIL=0 coz run --output "$OUT" --- "$PY" "$HERE/target.py"
rc=$?
if [ $rc -eq 0 ]; then
    echo "[coz] done -- load '$OUT' in the Coz viewer for virtual-speedup curves"
fi
exit $rc
