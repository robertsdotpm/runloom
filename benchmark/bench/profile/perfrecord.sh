#!/usr/bin/env sh
# On-CPU flame graph of a runloom workload via perf timer-sampling.
#
# This box has no hardware PMU (finding F5), so we sample `task-clock`
# (software timer) instead of `cycles`; flame-graph widths are on-CPU
# wall-time share, which is exactly what we want. We build the ext with
# -O2 -g, so `--call-graph dwarf` resolves runloom_c frames despite -O2
# omitting frame pointers.
#
# Usage:  bench/profile/perfrecord.sh <workload> [run_workload opts]
#   e.g.  bench/profile/perfrecord.sh pingpong --n 100000 --reps 6
#         bench/profile/perfrecord.sh mn --n 128 --iter 2000 --hubs 8 --reps 3
#
# Writes  bench/results/profiles/<workload>_flame.{folded,svg}  (gitignored;
# the .folded also loads into speedscope.app or flamegraph.pl) and prints the
# flat top self-time table.  Needs perf_event_paranoid <= 1 (see scripts).
set -eu
WORKLOAD="${1:-pingpong}"
[ "$#" -gt 0 ] && shift || true
PY="${PYTHON:-python3}"
OUT="${OUT:-bench/results/profiles/${WORKLOAD}_flame}"
FREQ="${FREQ:-1999}"
DATA="$(mktemp /tmp/runloom_perf.XXXXXX).data"
mkdir -p "$(dirname "$OUT")"

perf record -o "$DATA" -e task-clock -F "$FREQ" --call-graph dwarf -- \
  env PYTHONPATH=src PYTHON_GIL=0 "$PY" -m bench.profile.run_workload \
  "$WORKLOAD" "$@" --quiet

echo "--- flat top self-time (top 20) ---"
perf report -i "$DATA" --stdio --no-children -g none 2>/dev/null \
  | grep -vE '^#|^$' | head -20

PYTHONPATH=src "$PY" -m bench.profile.flamegraph "$DATA" --out "$OUT"
rm -f "$DATA"
