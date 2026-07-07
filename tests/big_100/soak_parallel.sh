#!/usr/bin/env bash
# Parallel soak: run all 100 projects with auto-computed concurrency.
#
# Sizing logic (can be overridden by env vars):
#   HUBS    M:N hubs per program  (default: 4, giving each job 4 hub threads)
#   JOBS    programs in parallel  (default: (cores - RESERVE_CORES) / 4)
#   FUNCS   goroutines per prog   (default: 100000)
#   DUR     seconds per run       (default: 30)
#   HT      hang-timeout          (default: 90)
#   WALL    per-program wall cap  (default: DUR+120)
#   LOOP    0=single pass, 1=loop forever (default: 1)
#
# IP isolation:
#   Each concurrent job gets a unique /24 subnet from the 127/8 loopback block
#   so their 28k-port ephemeral pools don't collide.  The base slot counter
#   advances monotonically across passes (stored in /tmp/soak_ip_slot_ctr) to
#   avoid TIME_WAIT collisions between passes.
#
# Examples:
#   ./soak_parallel.sh                              # defaults
#   FUNCS=10000 DUR=10 ./soak_parallel.sh           # quick smoke
#   FUNCS=100000 DUR=60 JOBS=4 ./soak_parallel.sh   # conservative 100k run
#   LOOP=0 ./soak_parallel.sh                       # single pass then exit
#
# Resource math for FUNCS=100000:
#   Virtual stack: 100k x ~32KB = ~3.2GB virt per program (lazy: RSS << virt)
#   FD ceiling:    network programs open ~2 FDs per goroutine; ulimit raised below
#   CPU:           HUBS threads per program x JOBS parallel = target-cores

set -uo pipefail
cd "$(dirname "$0")"

PY=${PY:-/home/x/.pyenv/versions/3.14.4t/bin/python3}
LOGDIR=/tmp/soak_parallel
SOAK_LOG="$LOGDIR/soak.log"
mkdir -p "$LOGDIR"

# ---- resource detection ---------------------------------------------------
CORES=$(nproc 2>/dev/null || echo 4)
RAM_GB=$(awk '/MemAvailable/{printf "%d", $2/1024/1024}' /proc/meminfo 2>/dev/null || echo 8)

# ---- tunables with env-var overrides --------------------------------------
FUNCS=${FUNCS:-1000000}
# RESERVE_CORES: leave this many cores idle (for VS Code / system).
RESERVE_CORES=${RESERVE_CORES:-10}
_USABLE=$(( CORES - RESERVE_CORES < 4 ? 4 : CORES - RESERVE_CORES ))

# Fixed 4 hubs per job; formula: JOBS = (usable_cores) / 4
HUBS=${HUBS:-4}
DUR=${DUR:-30}
HT=${HT:-90}
DRAIN=${DRAIN:-120}
LOOP=${LOOP:-1}

# Jobs = usable_cores / HUBS, floored at 1, capped at 32.
# Each job gets 4 hubs, so JOBS * 4 ~= usable cores.
_raw_jobs=$(( _USABLE / HUBS ))
JOBS=${JOBS:-$_raw_jobs}
[ "$JOBS" -lt 1  ] && JOBS=1
[ "$JOBS" -gt 32 ] && JOBS=32

WALL=${WALL:-$(( DUR + 120 ))}

# ---- monotonic IP slot counter (across passes, survives restart) -----------
# Each pass uses JOBS consecutive slots starting at IP_SLOT_BASE.
# Slots are stored in /tmp/soak_ip_slot_ctr so passes don't reuse addresses
# until the 127/8 space wraps (~254 * 254 = 64516 subnets available).
IP_SLOT_CTR=/tmp/soak_ip_slot_ctr

# ---- raise FD limit -------------------------------------------------------
# Most programs now use max_concurrent to cap live goroutines regardless of
# --funcs, so per-program FD usage is bounded by max_concurrent * ~5, not by
# FUNCS.  Network programs without explicit caps still churn sockets fast (open
# then close), so peak open sockets << H.funcs.  Request 1M FDs as a generous
# ceiling (the kernel cap on most systems); JOBS doesn't matter much here since
# each job's FD namespace is shared with the parent shell.
_needed=1048576
_cur=$(ulimit -n)
if [ "$_cur" -lt "$_needed" ] && [ "$_cur" != "unlimited" ]; then
    ulimit -n "$_needed" 2>/dev/null || true
fi

export RUNLOOM_SYSMON_QUIET=1 PYTHON_GIL=0 HUBS JOBS FUNCS DUR

echo "=====================================================================" | tee -a "$SOAK_LOG"
echo "soak_parallel: cores=$CORES  reserved=$RESERVE_CORES  usable=$_USABLE  available_RAM=${RAM_GB}GB" | tee -a "$SOAK_LOG"
echo "  FUNCS=$FUNCS  HUBS=$HUBS  JOBS=$JOBS  DUR=${DUR}s  LOOP=$LOOP" | tee -a "$SOAK_LOG"
echo "  fd_limit=$(ulimit -n)  wall_cap=${WALL}s" | tee -a "$SOAK_LOG"
echo "=====================================================================" | tee -a "$SOAK_LOG"

pass=1

run_pass() {
    local t0=$SECONDS

    # Advance the monotonic slot counter atomically.  Each pass consumes JOBS
    # slots; the counter file holds the NEXT free slot.
    local slot_base
    if [ -f "$IP_SLOT_CTR" ]; then
        slot_base=$(cat "$IP_SLOT_CTR" 2>/dev/null || echo 0)
    else
        slot_base=0
    fi
    # Advance by JOBS; wrap at 32767 (well under 127/8's 64k subnets)
    local next_slot=$(( (slot_base + JOBS) % 32767 ))
    echo "$next_slot" > "$IP_SLOT_CTR"

    echo "" | tee -a "$SOAK_LOG"
    echo "===== PASS $pass  $(date '+%H:%M:%S')  load=$(cut -d' ' -f1 /proc/loadavg)  ip-slot-base=$slot_base =====" | tee -a "$SOAK_LOG"

    "$PY" run_all.py \
        --jobs "$JOBS" \
        --hubs "$HUBS" \
        --duration "$DUR" \
        --funcs "$FUNCS" \
        --hang-timeout "$HT" \
        --drain-timeout "$DRAIN" \
        --ip-slot-base "$slot_base" \
        2>&1
    local rc=$?

    local elapsed=$(( SECONDS - t0 ))
    local crashes=$(grep -c "CRASH" "$SOAK_LOG" 2>/dev/null || echo 0)
    local hangs=$(grep  -c "HANG"  "$SOAK_LOG" 2>/dev/null || echo 0)
    echo "===== END PASS $pass  rc=$rc  wall=${elapsed}s  crashes_total=$crashes  hangs_total=$hangs =====" | tee -a "$SOAK_LOG"
    return $rc
}

if [ "$LOOP" = "1" ]; then
    while true; do
        run_pass || true
        pass=$(( pass + 1 ))
    done
else
    run_pass
fi
