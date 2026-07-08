#!/usr/bin/env bash
# forever.sh -- run one soak workload in back-to-back iterations, forever
# (docs/dev/RELIABILITY_PROGRAM.md R1/R4).  Each iteration is a bounded
# soak.py run that lands a PASS/FAIL slope-oracle REPORT.md; one summary line
# per iteration is appended to docs/dev/soak/forever_<workload>_SUMMARY.txt,
# so "how has it been doing?" is one file read, whenever you next look.
#
# Bounded iterations (not one endless run) because the slope oracle only
# writes its verdict at the END of a run -- a truly endless run would never
# produce one.  Each iteration reuses the same --stamp, so the run directory
# is overwritten each time; the SUMMARY file is the permanent ledger.
#
# Usage:
#   tools/soak/forever.sh <workload> [iter_hours] [workers] [extra soak.py args...]
#   tools/soak/forever.sh cserve_echo 6 2
#
# Detach it so it survives the shell/session (survives logout, not reboot):
#   setsid nice -n 8 tools/soak/forever.sh cserve_echo 6 2 >/dev/null 2>&1 &
#
# Stop:  pkill -f 'soak/forever.sh'; pkill -f 'soak.py --workload <workload>'
set +e
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
WL="${1:?usage: forever.sh <workload> [iter_hours] [workers] [extra args...]}"
HOURS="${2:-6}"
WORKERS="${3:-2}"
shift; [ $# -gt 0 ] && shift; [ $# -gt 0 ] && shift
SUM="$ROOT/docs/dev/soak/forever_${WL}_SUMMARY.txt"
OUTDIR="$ROOT/docs/dev/soak/soak_${WL}_forever"
# Permanent per-iteration ledger of retain-forever pool END values.  The per-run
# slope oracle forgives a settled pool (it must), which leaves one single-window
# blind spot: a slow constant leak whose 6h movement stays under the metric floor.
# Across iterations that separates cleanly -- a pool plateaus (fixed HWM), a leak
# climbs without bound -- so cross_iter_ratchet.py records each iteration's END
# here and raises CROSS-ITER-LEAK when a pool climbs with no asymptote.
LEDGER="$ROOT/docs/dev/soak/forever_${WL}_ratchet_ledger.csv"
cd "$ROOT"

echo "== forever soak started: $WL, ${HOURS}h iterations, $WORKERS workers, $(date '+%F %T') ==" >> "$SUM"
i=0
while true; do
  i=$((i+1)); start="$(date '+%F %T')"
  nice -n 8 env PYTHON_GIL=0 PYTHONPATH=src "$PY" tools/soak/soak.py \
      --workload "$WL" --hours "$HOURS" --workers "$WORKERS" \
      --interval 30 --warmup-frac 0.1 --stamp forever "$@" >/dev/null 2>&1
  v="$(grep -m1 -i verdict "$OUTDIR/REPORT.md" 2>/dev/null \
       || echo 'Verdict: (no report -- crashed/killed)')"
  # Record this iteration's pool END values + judge the cross-iteration trend.
  ci="$(nice -n 8 "$PY" tools/soak/cross_iter_ratchet.py "$OUTDIR" "$LEDGER" "$i" 2>&1)"
  [ $? -ne 0 ] && v="$v  ||  CROSS-ITER-LEAK: $(echo "$ci" | tr '\n' ' ')"
  echo "iter $i  $start -> $(date '+%F %T')  |  $v" >> "$SUM"
  # On a FAIL/HANG the run dir holds the evidence (CSVs + hang triage); keep it
  # by renaming before the next iteration would overwrite it.
  case "$v" in
    *PASS*) : ;;
    *) [ -d "$OUTDIR" ] && cp -r "$OUTDIR" "${OUTDIR}_fail_iter${i}" 2>/dev/null ;;
  esac
  sleep 15
done
