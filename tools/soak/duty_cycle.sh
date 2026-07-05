#!/usr/bin/env bash
# duty_cycle.sh -- one nightly reliability rotation (docs/dev/RELIABILITY_PROGRAM.md
# R4).  Reliability only stays true if something accrues fuzz-hours + stress-hours
# when nobody is at the keyboard.  This runs one rotation of the existing hunters,
# load-gated + niced so it never fights foreground work, and files every finding
# into the triage inbox (tools/soak/inbox.py).
#
# Stages (each stage's tool already exists; this only sequences + inboxes them):
#   1. hang_hunter   -- randomized realistic M:N workloads; auto-triages hangs/crashes
#   2. lifefuzz      -- generative always-terminating life-cycle programs (a HANG is
#                       a real lost wake; a nonzero exit is a bug)
#   3. (weekly)      -- one soak-matrix preset (asan-24h / tsan-24h / normal-72h),
#                       rotated by day-of-week; the machine-day ledger accrues
#
# Durations default to the nightly plan; --smoke shrinks them to seconds to verify
# the plumbing.  Load-gated: skips a stage while 1-min load exceeds LOAD_FRAC*cores.
#
# Usage:
#   tools/soak/duty_cycle.sh                 # nightly durations (hours)
#   tools/soak/duty_cycle.sh --smoke         # seconds, for a plumbing check
#   tools/soak/duty_cycle.sh --matrix asan-24h   # force a specific weekly slot
#
# NOT self-installing.  To run nightly, enable the systemd --user timer (see
# tools/soak/systemd/README.md) -- but get the box owner's OK first (it consumes
# real CPU for hours).
set -u

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.13.13t/bin/python3}"
LOAD_FRAC="${LOAD_FRAC:-0.7}"
NCPU="$(nproc 2>/dev/null || echo 4)"
DATE="$(date +%F)"
INBOX_ARTIFACTS="$ROOT/docs/dev/soak/inbox_artifacts/$DATE"
mkdir -p "$INBOX_ARTIFACTS"

SMOKE=0
FORCE_MATRIX=""
while [ $# -gt 0 ]; do
  case "$1" in
    --smoke) SMOKE=1 ;;
    --matrix) FORCE_MATRIX="$2"; shift ;;
    *) echo "unknown arg: $1"; exit 2 ;;
  esac
  shift
done

if [ "$SMOKE" = "1" ]; then
  HH_DUR=20; LF_DUR=15; DO_MATRIX_SMOKE=1
else
  HH_DUR=14400; LF_DUR=7200; DO_MATRIX_SMOKE=0   # 4h hang_hunter, 2h lifefuzz
fi

load_ok() {
  local l1; l1="$(cut -d' ' -f1 /proc/loadavg 2>/dev/null || echo 0)"
  awk -v l="$l1" -v c="$NCPU" -v f="$LOAD_FRAC" 'BEGIN{exit !(l < c*f)}'
}

inbox() {  # kind title artifact
  "$PY" tools/soak/inbox.py --add --kind "$1" --title "$2" --artifact "$3" --date "$DATE"
}

echo "== duty-cycle rotation $DATE (smoke=$SMOKE, load-gate ${LOAD_FRAC}x${NCPU}) =="

# --- stage 1: hang_hunter (self-load-gated + self-triaging) ---
if load_ok; then
  echo "-- hang_hunter ${HH_DUR}s --"
  HH_OUT="$INBOX_ARTIFACTS/hang_hunter"
  mkdir -p "$HH_OUT"
  nice -n 10 "$PY" -m tools.hang_hunter.daemon --duration "$HH_DUR" \
      --load-frac "$LOAD_FRAC" --report-dir "$HH_OUT" >"$HH_OUT/run.log" 2>&1
  # A real finding has a "KIND:" line (HANG/CRASH); status.txt and other
  # summaries do NOT -- skip those so the inbox only gets actual bugs.
  for rep in "$HH_OUT"/*.txt; do
    [ -e "$rep" ] || continue
    kind="$(grep -m1 -oE 'KIND: [A-Z]+' "$rep" | awk '{print $2}')"
    [ -n "$kind" ] || continue
    sig="$(grep -m1 -oE 'KEY: [0-9a-f]+' "$rep" | awk '{print $2}')"
    inbox "$kind" "hang_hunter $(basename "$rep")" "$rep"
  done
else
  echo "-- hang_hunter SKIPPED (load too high) --"
fi

# --- stage 2: lifefuzz ---
if load_ok && [ -f tools/lifefuzz/lifefuzz.py ]; then
  echo "-- lifefuzz ${LF_DUR}s --"
  LF_OUT="$INBOX_ARTIFACTS/lifefuzz"; mkdir -p "$LF_OUT"
  end=$(( $(date +%s) + LF_DUR )); n=0; fails=0
  while [ "$(date +%s)" -lt "$end" ]; do
    load_ok || { sleep 5; continue; }
    seed=$(( (n * 2654435761) % 2000000000 ))
    # lifefuzz uses subcommands: `run <seed>` executes one generative program
    # (always-terminating, so a HANG is a real lost wake; nonzero exit = a bug).
    if ! nice -n 10 env PYTHON_GIL=0 PYTHONPATH="$ROOT/src" \
           "$PY" tools/lifefuzz/lifefuzz.py run "$seed" --timeout 20 \
           >"$LF_OUT/seed_${seed}.log" 2>&1; then
      fails=$((fails+1))
      inbox "lifefuzz-fail" "lifefuzz seed=$seed exited nonzero" "$LF_OUT/seed_${seed}.log"
    else
      rm -f "$LF_OUT/seed_${seed}.log"   # keep only failures
    fi
    n=$((n+1))
    [ "$SMOKE" = "1" ] && [ "$n" -ge 3 ] && break
  done
  echo "   lifefuzz: $n runs, $fails failures"
else
  echo "-- lifefuzz SKIPPED --"
fi

# --- stage 3 (weekly / smoke): one soak-matrix preset ---
MATRIX_PRESET=""
if [ -n "$FORCE_MATRIX" ]; then
  MATRIX_PRESET="$FORCE_MATRIX"
elif [ "$DO_MATRIX_SMOKE" = "1" ]; then
  MATRIX_PRESET="smoke"
else
  # rotate by day-of-week: Sat asan-24h, Sun tsan-24h, else none
  case "$(date +%u)" in
    6) MATRIX_PRESET="asan-24h" ;;
    7) MATRIX_PRESET="tsan-24h" ;;
  esac
fi
if [ -n "$MATRIX_PRESET" ] && load_ok; then
  echo "-- matrix $MATRIX_PRESET --"
  if ! bash tools/soak/matrix.sh "$MATRIX_PRESET" >"$INBOX_ARTIFACTS/matrix_${MATRIX_PRESET}.log" 2>&1; then
    inbox "matrix-fail" "matrix $MATRIX_PRESET FAILED" "$INBOX_ARTIFACTS/matrix_${MATRIX_PRESET}.log"
  fi
fi

OPEN="$("$PY" tools/soak/inbox.py --count)"
echo "== rotation done -- $OPEN open inbox item(s) =="
