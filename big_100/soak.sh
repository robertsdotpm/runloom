#!/usr/bin/env bash
# Autonomous serial soak: run all 100 projects one at a time, loop forever.
# Focus on REAL signals -- SEGV/ABRT (crash) and true 0-progress hangs --
# while tolerating slow teardown at high concurrency (not a bug).  Serial so it
# never overwhelms a loaded box.  Uses each project's tuned default funcs unless
# SOAK_FUNCS is set.
set -u
cd "$(dirname "$0")"
PY=~/.pyenv/versions/3.13.13t/bin/python3
LOG=/tmp/soak/soak.log
mkdir -p /tmp/soak
export RUNLOOM_SYSMON_QUIET=1 PYTHON_GIL=0

DUR=${SOAK_DUR:-8}
HUBS=${SOAK_HUBS:-8}
HT=${SOAK_HT:-90}
WALL=${SOAK_WALL:-280}          # generous: slow teardown at scale is not a fail

pass=1
while true; do
  echo "===== SOAK PASS $pass  $(date '+%H:%M:%S')  load=$(cut -d' ' -f1 /proc/loadavg) =====" >>"$LOG"
  for p in $(ls p[0-9]*.py | sort); do
    name=${p%.py}
    out=/tmp/soak/${name}.log
    args=(--duration "$DUR" --hubs "$HUBS" --hang-timeout "$HT")
    [ -n "${SOAK_FUNCS:-}" ] && args+=(--funcs "$SOAK_FUNCS")
    "$PY" "$p" "${args[@]}" >"$out" 2>&1 &
    PID=$!
    waited=0
    while kill -0 $PID 2>/dev/null && [ $waited -lt $WALL ]; do sleep 3; waited=$((waited+3)); done
    if kill -0 $PID 2>/dev/null; then kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null; e=137; else wait $PID; e=$?; fi
    progressed=$(grep -c "t=" "$out" 2>/dev/null)
    if grep -q "PASS (exit 0)" "$out" 2>/dev/null; then
      echo "  $name PASS" >>"$LOG"
    elif [ "$e" = "139" ] || [ "$e" = "134" ] || [ "$e" = "136" ]; then
      cp "$out" /tmp/soak/CRASH_${name}_p${pass}.log
      echo "  $name **CRASH** (exit $e) <<< INVESTIGATE" >>"$LOG"
    elif [ "$e" = "137" ] && [ "${progressed:-0}" -eq 0 ]; then
      cp "$out" /tmp/soak/HANG_${name}_p${pass}.log
      echo "  $name **TRUE-HANG** (0 progress) <<< INVESTIGATE" >>"$LOG"
    elif [ "$e" = "137" ]; then
      echo "  $name slow-teardown (wallkill after progress; not a bug)" >>"$LOG"
    else
      cp "$out" /tmp/soak/FAIL_${name}_p${pass}.log
      echo "  $name fail (exit $e): $(tail -1 "$out" | cut -c1-60)" >>"$LOG"
    fi
  done
  echo "===== END PASS $pass  crashes=$(grep -c CRASH "$LOG")  true-hangs=$(grep -c TRUE-HANG "$LOG") =====" >>"$LOG"
  pass=$((pass+1))
done
