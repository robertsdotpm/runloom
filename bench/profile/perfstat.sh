#!/usr/bin/env sh
# perf stat for a runloom workload, SOFTWARE events only.
#
# This box is a VM with no hardware PMU (campaign finding F5): cycles /
# instructions / cache-misses read <not supported>. The software events
# below DO work and are what we use for page-fault accounting (F4),
# context-switch and migration counts, and wall/CPU time.
#
# Usage:  bench/profile/perfstat.sh <workload> [run_workload opts]
#   e.g.  bench/profile/perfstat.sh spawn --n 10000 --reps 3
#         EVENTS=task-clock,page-faults bench/profile/perfstat.sh yield --n 100
#
# PYTHON_GIL=0 is set so no transitively-imported C ext can re-enable the GIL.
set -eu
WORKLOAD="${1:-spawn}"
[ "$#" -gt 0 ] && shift || true
PY="${PYTHON:-python3}"
EVENTS="${EVENTS:-task-clock,context-switches,cpu-migrations,page-faults,minor-faults,major-faults}"
exec perf stat -e "$EVENTS" -- \
  env PYTHONPATH=src PYTHON_GIL=0 "$PY" -m bench.profile.run_workload "$WORKLOAD" "$@" --quiet
