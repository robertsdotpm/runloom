#!/usr/bin/env bash
# Parallel soak: run all 100 projects with auto-computed concurrency.
#
# Sizing logic (can be overridden by env vars):
#   HUBS    M:N hubs per program  (default: min(8, cores/4))
#   JOBS    programs in parallel  (default: floor(cores/HUBS), capped at 8 for
#                                  large-funcs runs to avoid memory/FD pressure)
#   FUNCS   goroutines per prog   (default: 100000)
#   DUR     seconds per run       (default: 30)
#   HT      hang-timeout          (default: 90)
#   WALL    per-program wall cap  (default: DUR+120)
#   LOOP    0=single pass, 1=loop forever (default: 1)
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

PY=${PY:-/home/x/.pyenv/versions/3.13.13t/bin/python3}
LOGDIR=/tmp/soak_parallel
SOAK_LOG="$LOGDIR/soak.log"
mkdir -p "$LOGDIR"

# ---- resource detection ---------------------------------------------------
CORES=$(nproc 2>/dev/null || echo 4)
RAM_GB=$(awk '/MemAvailable/{printf "%d", $2/1024/1024}' /proc/meminfo 2>/dev/null || echo 8)

# ---- tunables with env-var overrides --------------------------------------
FUNCS=${FUNCS:-100000}
# RESERVE_CORES: leave this many cores idle (for VS Code / system).
RESERVE_CORES=${RESERVE_CORES:-10}
_USABLE=$(( CORES - RESERVE_CORES < 4 ? 4 : CORES - RESERVE_CORES ))
HUBS=${HUBS:-$(( _USABLE > 32 ? 8 : (_USABLE > 16 ? 6 : 4) ))}
DUR=${DUR:-30}
HT=${HT:-90}
DRAIN=${DRAIN:-120}
LOOP=${LOOP:-1}

# Jobs: cores/hubs, but cap lower for large funcs to avoid OOM.
# 100k funcs ~ 200-500MB RSS per program (lazy stacks).  With 70GB available,
# we can run ~8-16 concurrently before RAM becomes the binding constraint.
# Network programs also need FDs: 100k goroutines x 2 FDs = 200k FDs per prog.
# At JOBS=4 that's 800k FDs; at JOBS=8 it's 1.6M.  We raise the per-process
# limit below; system-wide fs.file-max is usually >>1M.
_raw_jobs=$(( _USABLE / HUBS ))
_mem_cap=$(( RAM_GB / 2 ))   # ~500MB/prog headroom
_fd_cap=8                    # above 8 progs, FD pressure becomes real at 100k
if   [ "$FUNCS" -ge 100000 ]; then JOBS=${JOBS:-$(( _raw_jobs < _fd_cap  ? _raw_jobs : _fd_cap  ))}
elif [ "$FUNCS" -ge  50000 ]; then JOBS=${JOBS:-$(( _raw_jobs < 12       ? _raw_jobs : 12       ))}
else                                JOBS=${JOBS:-$_raw_jobs}
fi
# Floor at 1, cap at 16
[ "$JOBS" -lt 1  ] && JOBS=1
[ "$JOBS" -gt 16 ] && JOBS=16

WALL=${WALL:-$(( DUR + 120 ))}

# ---- raise FD limit -------------------------------------------------------
# Each program with 100k goroutines can open up to ~200k FDs (network).
# JOBS programs in parallel => JOBS * 200k.  Raise per-process nofile.
_needed=$(( JOBS * FUNCS * 2 + 4096 ))
_cur=$(ulimit -n)
if [ "$_cur" -lt "$_needed" ] && [ "$_cur" != "unlimited" ]; then
    ulimit -n "$_needed" 2>/dev/null || \
    ulimit -n 1048576   2>/dev/null || true
fi

export RUNLOOM_SYSMON_QUIET=1 PYTHON_GIL=0

echo "=====================================================================" | tee -a "$SOAK_LOG"
echo "soak_parallel: cores=$CORES  reserved=$RESERVE_CORES  usable=$_USABLE  available_RAM=${RAM_GB}GB" | tee -a "$SOAK_LOG"
echo "  FUNCS=$FUNCS  HUBS=$HUBS  JOBS=$JOBS  DUR=${DUR}s  LOOP=$LOOP" | tee -a "$SOAK_LOG"
echo "  fd_limit=$(ulimit -n)  wall_cap=${WALL}s" | tee -a "$SOAK_LOG"
echo "=====================================================================" | tee -a "$SOAK_LOG"

pass=1

run_pass() {
    local t0=$SECONDS
    echo "" | tee -a "$SOAK_LOG"
    echo "===== PASS $pass  $(date '+%H:%M:%S')  load=$(cut -d' ' -f1 /proc/loadavg) =====" | tee -a "$SOAK_LOG"

    "$PY" run_all.py \
        --jobs "$JOBS" \
        --hubs "$HUBS" \
        --duration "$DUR" \
        --funcs "$FUNCS" \
        --hang-timeout "$HT" \
        --drain-timeout "$DRAIN" \
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
