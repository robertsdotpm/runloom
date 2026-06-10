#!/bin/bash
# ramp_campaign.sh -- drive every big_100 program toward 1,000,000 goroutines.
#
# Each run is wrapped in a systemd scope (MemoryMax=24G, MemorySwapMax=0,
# TasksMax=20000) + timeout, so no run can OOM/fork-bomb/hang the box. Tries 1M
# first; on failure brackets DOWN (100K, 10K, 1024) to find the max clean level
# and capture the failure signature. Resumable: skips programs already in
# logs/million_done.txt. Default backend only (no RUNLOOM_IOURING_LOOP).
set +e
cd /home/x/projects/pygo-big100/big_100 || exit 2
sudo -n prlimit --pid $$ --nofile=8388608:8388608 2>/dev/null   # bg shell reverts to 4096
PYBIN="$HOME/.pyenv/versions/3.13.13t/bin/python3"
RES=logs/million_results.tsv
DONE=logs/million_done.txt
mkdir -p logs
[ -f "$RES" ] || printf "program\tlevel\tverdict\texit\trss_mb\telapsed\tprogress\terror\n" > "$RES"
touch "$DONE"
SCOPE=(systemd-run --user --scope -q -p MemoryMax=24G -p MemorySwapMax=0 -p TasksMax=20000 --)

run_one() {  # prog level timeout -> appends a TSV row, echoes "verdict exit"
    local prog=$1 lvl=$2 to=$3 dur=$(( $3 - 25 ))
    [ $dur -lt 10 ] && dur=10
    local t0 t1 ec rss verdict err prog progress log
    log="logs/runs/${prog%.py}_${lvl}.log"
    mkdir -p logs/runs
    t0=$(date +%s)
    "${SCOPE[@]}" timeout "$to" env PYTHON_GIL=0 "$PYBIN" "$prog" \
          --funcs "$lvl" --hubs 8 --rounds 1 --duration "$dur" > "$log" 2>&1
    ec=$?
    t1=$(date +%s)
    rss=$(grep -oE 'mem_rss_mb *: *[0-9]+' "$log" | grep -oE '[0-9]+$' | tail -1)
    verdict=$(grep -oE 'VERDICT *: *[A-Z]+' "$log" | grep -oE '[A-Z]+$' | tail -1)
    # how far did it get (last "exited=N/M")
    progress=$(grep -oE 'exited=[0-9]+/[0-9]+' "$log" | tail -1)
    if [ -z "$verdict" ]; then
        if [ "$ec" -eq 124 ]; then verdict=TIMEOUT
        elif [ "$ec" -eq 137 ]; then verdict=OOMKILL
        elif [ "$ec" -eq 139 ]; then verdict=SEGV
        elif [ "$ec" -eq 134 ]; then verdict=ABORT
        else verdict=NOVERDICT; fi
    fi
    err=$(grep -iE 'Traceback|Error|FATAL|Segmentation|Aborted|core dumped|watchdog|HANG|invariant|Killed|ENOMEM|EMFILE|Too many|Cannot allocate|RecursionError' "$log" | grep -ivE 'fail=0|failures *: *0' | head -1 | tr '\t' ' ' | sed 's/  */ /g' | cut -c1-130)
    printf "%s\t%d\t%s\t%d\t%s\t%ds\t%s\t%s\n" "$prog" "$lvl" "$verdict" "$ec" "${rss:-?}" "$((t1-t0))" "${progress:-?}" "$err" >> "$RES"
    printf "%s %d" "$verdict" "$ec"
}

for prog in $(ls p[0-9]*.py | sort -V); do
    grep -qxF "$prog" "$DONE" && continue
    echo ">>> $prog  $(date '+%Y-%m-%d %H:%M:%S')"
    vr=$(run_one "$prog" 1000000 150)
    if [ "$vr" = "PASS 0" ]; then
        echo "$prog 1M_PASS" >> "$DONE"; continue
    fi
    # bracket down to find the max clean concurrency
    best=none
    for lvl in 100000 10000 1024; do
        vr=$(run_one "$prog" "$lvl" 100)
        if [ "$vr" = "PASS 0" ]; then best=$lvl; break; fi
    done
    echo "$prog max_clean=$best" >> "$DONE"
done
echo "CAMPAIGN COMPLETE $(date '+%Y-%m-%d %H:%M:%S')" >> "$RES"
echo "DONE_ALL" >> "$DONE"
