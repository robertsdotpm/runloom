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
  HH_DUR=20; LF_DUR=15; RR_DUR=20; DO_MATRIX_SMOKE=1
  FUZZ_DUR=20; PCT_SEEDS=8; PCT_SWEEP_SEEDS=2; WAKE_SKEW=4; WAKE_REPS=1; MUT_MAX=3; FS_LIMIT="--limit 4"
else
  # 4h hang_hunter, 2h lifefuzz, 2h rr-chaos (rr-chaos SKIPs in seconds while
  # the host vPMU can't record -- see tools/soak/rr_chaos.sh)
  HH_DUR=14400; LF_DUR=7200; RR_DUR=7200; DO_MATRIX_SMOKE=0
  FUZZ_DUR=1800; PCT_SEEDS=200; PCT_SWEEP_SEEDS=10; WAKE_SKEW=8; WAKE_REPS=3; MUT_MAX=40; FS_LIMIT=""
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

# --- stage 3: rr-chaos lost-wake hunt (self-gating: SKIPs while the host vPMU
# can't record; auto-activates the day `rr record /bin/true` works) ---
if load_ok; then
  echo "-- rr-chaos ${RR_DUR}s --"
  RR_OUT="$INBOX_ARTIFACTS/rr_chaos"; mkdir -p "$RR_OUT"
  nice -n 10 bash tools/soak/rr_chaos.sh "$RR_DUR" "$RR_OUT" \
      > "$RR_OUT/run.log" 2>&1
  grep -E "^rr-chaos" "$RR_OUT/run.log" | sed 's/^/   /'
  # every FINDING line carries a replayable trace -> inbox it
  grep -E "^FINDING " "$RR_OUT/run.log" | while read -r _ kind rest; do
    inbox "$kind" "rr-chaos $rest" "$RR_OUT/run.log"
  done
else
  echo "-- rr-chaos SKIPPED (load too high) --"
fi

# --- stage 4: counted-exhaustive fault sweep (SQLite-style anomaly testing:
# fail the Nth reach of every runloom fault site until exhausted; fast --
# minutes -- so it runs nightly, not weekly) ---
if load_ok; then
  echo "-- counted fault sweep --"
  FS_OUT="$INBOX_ARTIFACTS/fault_sweep_counted.log"
  FS_SITES=""
  [ "$SMOKE" = "1" ] && FS_SITES="FD_READ FD_WRITE"   # plumbing check: 2 fast sites
  # shellcheck disable=SC2086
  if ! nice -n 10 env PYTHON_GIL=0 "$PY" tools/fault_sweep_counted.py $FS_SITES \
       > "$FS_OUT" 2>&1; then
    inbox "fault-sweep" "counted-exhaustive sweep found CRASH/HANG" "$FS_OUT"
  fi
  grep -E "^== done" "$FS_OUT" | sed 's/^/   /'
else
  echo "-- counted fault sweep SKIPPED (load too high) --"
fi

# --- stage 6: atheris coverage-guided C-API fuzz (nightly).  run.sh self-SKIPs
# (exit 0) when atheris isn't importable and self-bounds by its seconds arg; a
# nonzero rc is a real process crash = the finding. ---
if load_ok; then
  echo "-- atheris fuzz ${FUZZ_DUR}s --"
  FZ_OUT="$INBOX_ARTIFACTS/atheris"; mkdir -p "$FZ_OUT"
  if ! nice -n 10 env RUNLOOM_PYTHON="$PY" bash tools/fuzz/atheris/run.sh "$FUZZ_DUR" \
       > "$FZ_OUT/run.log" 2>&1; then
    inbox "atheris-crash" "atheris C-API fuzz CRASH" "$FZ_OUT/run.log"
  fi
  grep -E "^atheris:" "$FZ_OUT/run.log" | sed 's/^/   /'
else
  echo "-- atheris fuzz SKIPPED (load too high) --"
fi

# --- stage 7: PCT probabilistic-schedule exploration (nightly).  demo permutes
# single-hub parked-wake order and checks conservation (exit 1 = a lost value);
# a bounded sweep replays an order-sensitive test under PCT_SWEEP_SEEDS seeds
# (exit 1 = an order-dependent failure, repro line in the log). ---
if load_ok; then
  echo "-- pct demo (${PCT_SEEDS} seeds) + sweep (${PCT_SWEEP_SEEDS} seeds) --"
  PCT_OUT="$INBOX_ARTIFACTS/pct"; mkdir -p "$PCT_OUT"
  if ! nice -n 10 env PYTHON_GIL=0 "$PY" tools/pct/pct_explore.py demo \
       --seeds "$PCT_SEEDS" > "$PCT_OUT/demo.log" 2>&1; then
    inbox "pct-conservation" "PCT demo conservation violation" "$PCT_OUT/demo.log"
  fi
  tail -3 "$PCT_OUT/demo.log" | sed 's/^/   /'
  if [ -f tests/test_sched_fairness.py ]; then
    if ! nice -n 10 env PYTHON_GIL=0 "$PY" tools/pct/pct_explore.py sweep \
         tests/test_sched_fairness.py --seeds "$PCT_SWEEP_SEEDS" --depth 3 \
         > "$PCT_OUT/sweep.log" 2>&1; then
      inbox "pct-order-bug" "PCT sweep order-dependent FAILURE" "$PCT_OUT/sweep.log"
    fi
  fi
else
  echo "-- pct SKIPPED (load too high) --"
fi

# --- stage 8: Layer-3 wake-skew test policy (nightly).  Rebuilds the ext with
# -DRUNLOOM_WAKE_SKEW to widen the park/wake race window so a lost-wake shows as
# a HANG (nonzero rc).  GOTCHA: wake_skew_test.sh does NOT restore the normal
# ext, so rebuild it here or every later stage + foreground build inherits the
# skew instrumentation. ---
if load_ok; then
  echo "-- wake-skew (skew=$WAKE_SKEW x$WAKE_REPS) --"
  WK_OUT="$INBOX_ARTIFACTS/wake_skew"; mkdir -p "$WK_OUT"
  if ! nice -n 10 env PYTHON="$PY" bash tools/wake_skew_test.sh "$WAKE_SKEW" "$WAKE_REPS" \
       > "$WK_OUT/run.log" 2>&1; then
    inbox "wake-skew" "wake-skew test hung/failed under skew=$WAKE_SKEW" "$WK_OUT/run.log"
  fi
  grep -E "^WAKE-SKEW" "$WK_OUT/run.log" | sed 's/^/   /'
  env -u RUNLOOM_EXTRA_CFLAGS PYTHON_GIL=0 "$PY" setup.py build_ext --inplace --force \
      >/tmp/duty_wake_skew_restore.log 2>&1 \
      && echo "   normal ext restored" || echo "   WARN: normal rebuild failed (/tmp/duty_wake_skew_restore.log)"
else
  echo "-- wake-skew SKIPPED (load too high) --"
fi
# --- end nightly extra stages (before the weekly matrix) ---

# --- stage 9 (weekly, Tue): mutation testing -- does the suite have TEETH?
# Heavy (rebuilds the ext per mutant), so weekly + a DETERMINISTIC rotating
# 2-file subset chosen by ISO week number: no state to track, every file covered
# on a fixed cadence.  mutate.py restores the .c and rebuilds a clean .so in a
# finally, so it self-heals the tree.  A SURVIVING mutant is the finding. ---
MUT_DOW=2
if [ "$SMOKE" = "1" ] || [ "$(date +%u)" = "$MUT_DOW" ]; then
  if load_ok; then
    MUT_FILES=(src/runloom_c/chan.c src/runloom_c/mn_sched.c src/runloom_c/netpoll.c \
               src/runloom_c/coro.c src/runloom_c/io_uring.c src/runloom_c/runloom_tcp.c \
               src/runloom_c/runloom_sched.c src/runloom_c/cldeque.c \
               src/runloom_c/runloom_blockpool.c src/runloom_c/rl_handle.c)
    NF=${#MUT_FILES[@]}
    WK=$(( 10#$(date +%V) ))          # ISO week, base-10 (leading-zero safe)
    for k in 0 1; do                  # rotate a 2-file subset per week
      idx=$(( (WK + k) % NF )); tgt="${MUT_FILES[$idx]}"
      [ -f "$tgt" ] || continue
      load_ok || break
      base="$(basename "$tgt" .c)"
      echo "-- mutate $tgt (week $WK, max $MUT_MAX) --"
      MU_OUT="$INBOX_ARTIFACTS/mutate_${base}"; mkdir -p "$MU_OUT"
      nice -n 10 env PYTHON="$PY" "$PY" tools/mutate/mutate.py "$tgt" \
          --max "$MUT_MAX" --seed "$WK" --json "$MU_OUT/result.json" \
          > "$MU_OUT/run.log" 2>&1
      if grep -q '"survived": [1-9]' "$MU_OUT/result.json" 2>/dev/null; then
        inbox "mutate-survivor" "mutate $base: surviving mutant(s) = test gap" "$MU_OUT/run.log"
      fi
      grep -E "mutation score" "$MU_OUT/run.log" | sed 's/^/   /'
    done
  else
    echo "-- mutate SKIPPED (load too high) --"
  fi
fi

# --- stage 10 (weekly, Wed): exhaustive libclang fault-site sweep across TUs.
# DISTINCT from stage 4 (the fast compiled-in counted sweep): this instruments
# EVERY fallible call site via libclang in the mutant worktree and is HOURS, so
# weekly + clean-SKIP when clang-18 is absent.  An UNCHECKED error path (a forced
# failure no test noticed) is the finding. ---
FS_DOW=3
if command -v clang-18 >/dev/null 2>&1 && { [ "$SMOKE" = "1" ] || [ "$(date +%u)" = "$FS_DOW" ]; }; then
  if load_ok; then
    XFS_TUS=""; [ "$SMOKE" = "1" ] && XFS_TUS="netpoll"   # smoke: one TU
    echo "-- exhaustive fault sweep (sweep_all${FS_LIMIT:+ $FS_LIMIT}${XFS_TUS:+ $XFS_TUS}) --"
    XFS_OUT="$INBOX_ARTIFACTS/fault_sweep_libclang"; mkdir -p "$XFS_OUT"
    # shellcheck disable=SC2086
    nice -n 15 bash tools/mutate/faultsites/sweep_all.sh $FS_LIMIT $XFS_TUS \
        > "$XFS_OUT/run.log" 2>&1
    grep -E "survivors report:" "$XFS_OUT/run.log" | while read -r _ _ rep; do
      [ -f "$rep" ] || continue
      if grep -qvE '^#|^$' "$rep"; then     # a non-comment line = a real unchecked path
        inbox "fault-unchecked" "libclang sweep: unchecked error path(s) in $(basename "$rep")" "$rep"
      fi
    done
    grep -E "^== sweep_all done" "$XFS_OUT/run.log" | sed 's/^/   /'
  else
    echo "-- exhaustive fault sweep SKIPPED (load too high) --"
  fi
fi

# --- stage 5 (weekly / smoke): one soak-matrix preset ---
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
