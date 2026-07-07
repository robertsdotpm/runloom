#!/bin/sh
# sweep.sh LABEL [extra args to scaling.py]
# Runs the spawn-scaling harness at issuers = 1, 4, 8 (hubs=8, pinned to cores
# 16-23, GIL off), appending one JSON line per point to results/LABEL.jsonl.
# Toggle the experiment via env BEFORE calling (RUNLOOM_STACK_ARENA=1, an
# LD_PRELOAD shim, ...).  Pass --n / --stack-size as extra args.
#
#   RUNLOOM_STACK_ARENA=1 experiments/spawn_perf/sweep.sh arena
#   tools/keep_resident/runloom-keep-resident env RUNLOOM_STACK_ARENA=1 \
#       experiments/spawn_perf/sweep.sh arena_keepres
set -e
here=$(cd "$(dirname "$0")" && pwd)
label="$1"; shift || true
[ -n "$label" ] || { echo "usage: sweep.sh LABEL [extra args]" >&2; exit 2; }
py=$HOME/.pyenv/versions/3.14.4t/bin/python3
out="$here/results/$label.jsonl"
mkdir -p "$here/results"
: > "$out"
for iss in 1 4 8; do
    taskset -c 16-23 env PYTHON_GIL=0 RUNLOOM_SYSMON=0 RUNLOOM_PREEMPT=0 RUNLOOM_HANDOFF=0 \
        "$py" "$here/scaling.py" --hubs 8 --issuers "$iss" --label "$label" "$@" \
        >> "$out"
done
echo "wrote $out"
