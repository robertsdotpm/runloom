#!/usr/bin/env bash
# Hunt the ~0.1% netpoll lost-wake in bench_mn and, on the first hang,
# capture the full gdb signature before killing the process.
#
# Usage: hunt_hang.sh [N] [H] [M] [START_SEED] [N_SEEDS] [WATCHDOG_S]
set -u
N=${1:-1024}; H=${2:-8}; M=${3:-5}
START=${4:-1}; COUNT=${5:-400}; WD=${6:-12}
BIN=tests_c/bench_mn
OUT=/tmp/hunt_capture.txt
: > "$OUT"

echo "hunt: N=$N H=$H M=$M seeds=[$START..$((START+COUNT-1))] watchdog=${WD}s"
for ((s=START; s<START+COUNT; s++)); do
    RUNLOOM_DEBUG_DIAG=ring,gstate "$BIN" "$N" "$H" "$M" "$s" >/tmp/hunt_run.txt 2>&1 &
    pid=$!
    # Poll for completion.
    done=0
    for ((t=0; t<WD*10; t++)); do
        if ! kill -0 "$pid" 2>/dev/null; then done=1; break; fi
        sleep 0.1
    done
    if [ "$done" -eq 1 ]; then
        wait "$pid"; rc=$?
        if [ "$rc" -ne 0 ]; then
            echo "seed=$s rc=$rc (non-hang failure)"; cat /tmp/hunt_run.txt
        fi
        continue
    fi

    # ---- HANG CAPTURED ----
    echo "=== HANG seed=$s pid=$pid ===" | tee -a "$OUT"
    echo "--- partial stdout ---" | tee -a "$OUT"
    cat /tmp/hunt_run.txt | tee -a "$OUT"
    echo "--- ss sockets with Recv-Q>0 for pid ---" | tee -a "$OUT"
    ss -tnp 2>/dev/null | grep "pid=$pid," | awk '$2>0 {print}' | tee -a "$OUT"
    echo "--- ss summary (state counts) ---" | tee -a "$OUT"
    ss -tn 2>/dev/null | awk 'NR>1{c[$1]++} END{for(k in c) print k, c[k]}' | tee -a "$OUT"
    echo "--- gdb: diag_dump + self_check + threads ---" | tee -a "$OUT"
    timeout 40 sudo -n gdb -p "$pid" -batch \
        -ex 'set pagination off' \
        -ex 'thread apply all bt' \
        -ex 'call (void)runloom_diag_dump(2)' \
        -ex 'call (int)runloom_self_check(1)' 2>&1 | tee -a "$OUT"
    echo "=== leaving pid=$pid ALIVE for manual gdb (kill it when done) ===" | tee -a "$OUT"
    echo "PID=$pid" > /tmp/hunt_pid.txt
    exit 42
done
echo "no hang in $COUNT seeds"
exit 0
