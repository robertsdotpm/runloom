#!/usr/bin/env bash
# Parallel hunt: oversubscribe the CPU with P concurrent bench_mn runs
# (the interleave the residual needs), no per-run timeout so a hang
# stays ALIVE.  A watchdog scans for any bench_mn older than WD seconds,
# captures its full gdb signature, then stops the sweep.
#
# Usage: hunt_par.sh [N] [H] [M] [TOTAL] [P] [WD_S]
set -u
N=${1:-1024}; H=${2:-8}; M=${3:-5}
TOTAL=${4:-4000}; P=${5:-12}; WD=${6:-15}
BIN=$(pwd)/tests_c/bench_mn
OUT=/tmp/hunt_capture.txt
: > "$OUT"
echo "par-hunt: N=$N H=$H M=$M total=$TOTAL P=$P watchdog=${WD}s"

# Launch the sweep in the background.  PYGO_DEBUG_DIAG so a captured
# hang has a populated event ring.
export PYGO_DEBUG_DIAG=ring,gstate
( seq 1 "$TOTAL" | xargs -P"$P" -I{} "$BIN" "$N" "$H" "$M" {} >/dev/null 2>&1 ) &
SWEEP=$!
echo "sweep pid=$SWEEP"

capture() {
    local pid=$1
    echo "=== HANG pid=$pid (elapsed>${WD}s) ===" | tee -a "$OUT"
    echo "--- cmdline ---" | tee -a "$OUT"
    tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | tee -a "$OUT"; echo | tee -a "$OUT"
    echo "--- sockets Recv-Q>0 owned by pid ---" | tee -a "$OUT"
    ss -tnp 2>/dev/null | grep "pid=$pid," | awk 'NR==1||$2>0' | tee -a "$OUT"
    echo "--- ss state counts ---" | tee -a "$OUT"
    ss -tn 2>/dev/null | awk 'NR>1{c[$1]++} END{for(k in c) print k,c[k]}' | tee -a "$OUT"
    echo "--- gdb ---" | tee -a "$OUT"
    timeout 50 sudo -n gdb -p "$pid" -batch \
        -ex 'set pagination off' \
        -ex 'thread apply all bt' \
        -ex 'call (void)pygo_diag_dump(2)' \
        -ex 'call (int)pygo_self_check(1)' 2>&1 | tee -a "$OUT"
    echo "PID=$pid" > /tmp/hunt_pid.txt
    echo "=== pid=$pid left ALIVE for manual gdb ===" | tee -a "$OUT"
}

while kill -0 "$SWEEP" 2>/dev/null; do
    # Find any bench_mn whose elapsed seconds exceed WD.
    while read -r pid etimes _; do
        [ -z "$pid" ] && continue
        if [ "$etimes" -ge "$WD" ] 2>/dev/null; then
            capture "$pid"
            # tear down the sweep and every sibling EXCEPT the captured pid
            kill "$SWEEP" 2>/dev/null
            for q in $(pgrep -x bench_mn); do
                [ "$q" = "$pid" ] || kill -9 "$q" 2>/dev/null
            done
            exit 42
        fi
    done < <(pgrep -a bench_mn | awk '{print $1}' | xargs -r -I{} sh -c 'echo {} $(ps -o etimes= -p {} 2>/dev/null)')
    sleep 0.5
done
wait "$SWEEP"
echo "sweep finished, no hang in $TOTAL runs"
exit 0
