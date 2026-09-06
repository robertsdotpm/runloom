#!/usr/bin/env bash
# hunt_tlc.sh -- reproduce a rare TLC check failure on demand, and keep
# everything needed to explain it.
#
# WHY THIS EXISTS.  RunloomMNControl (Preempt=FALSE) failed once inside a full
# check_all_fast run and could not be reproduced afterwards: 12+ consecutive
# passes, including 5 under deliberate 64-way CPU saturation.  A failure that
# rare is only ever caught by luck, and when the gate caught it the evidence
# was a single word ("FAIL") because the caller pipes TLC into `grep -q`.  This
# script is the deliberate version of that luck.
#
# WHAT IT DOES DIFFERENTLY FROM JUST LOOPING.  A bare `until ! run; do done`
# loop reproduces nothing useful, because these failures are suspected to be
# RESOURCE-driven -- TLC OOM-killed or starved under the parallel verify pool
# (VERIFY_JOBS=nproc, each TLC wanting 1g and 4 workers).  So this can run K
# copies CONCURRENTLY, which is the condition the gate actually creates and a
# serial loop never will.  And on the first failure it freezes the scene:
# TLC's full output, its metadir, a heap dump if the JVM OOMed, a thread dump
# if it hung, and the machine's memory/load at that moment.
#
# USAGE
#   tools/verify/tla/hunt_tlc.sh                       # the known-flaky check
#   tools/verify/tla/hunt_tlc.sh -n 200 -j 16          # 200 rounds, 16 at once
#   tools/verify/tla/hunt_tlc.sh -k mnok               # a different check
#   tools/verify/tla/hunt_tlc.sh -n 50 -j 8 -x 256m    # squeeze the heap
#
#   -n ITERS   rounds to run          (default 100)
#   -j JOBS    concurrent TLC copies per round (default 1; try nproc/4)
#   -k CHECK   which check            (default mnnp; see CHECKS below)
#   -x XMX     -Xmx per JVM           (default 1g -- lower it to force the
#              OOM path and confirm the classifier end-to-end)
#   -t SECS    per-run timeout        (default 300; a hang is a finding too)
#   -o DIR     artifact dir           (default /tmp/runloom_tlc_hunt.<stamp>)
#
# Exit: 0 = never failed in ITERS rounds.  1 = reproduced (artifacts written).
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
JAR="${TLA_JAR:-$HERE/tla2tools.jar}"

# name | config | spec | the string the check greps for
# Keep in step with run_tla.sh.  Only the negative controls are interesting
# here (they fail by NOT finding a violation, which is the ambiguous case).
CHECKS="
mnnp|RunloomMNControl_nopreempt.cfg|RunloomMNControl.tla|Temporal properties were violated
mnok|RunloomMNControl.cfg|RunloomMNControl.tla|No error has been found
bug|RunloomSched_bug.cfg|RunloomSched.tla|Temporal properties were violated
ok|RunloomSched.cfg|RunloomSched.tla|No error has been found
"

ITERS=100; JOBS=1; CHECK=mnnp; XMX=1g; TMO=300
OUT="/tmp/runloom_tlc_hunt.$(date -u +%Y%m%dT%H%M%SZ)"
while getopts "n:j:k:x:t:o:h" o; do
  case "$o" in
    n) ITERS=$OPTARG ;; j) JOBS=$OPTARG ;; k) CHECK=$OPTARG ;;
    x) XMX=$OPTARG ;; t) TMO=$OPTARG ;; o) OUT=$OPTARG ;;
    h) sed -n '1,40p' "$0"; exit 0 ;;
    *) exit 2 ;;
  esac
done

row="$(printf '%s\n' "$CHECKS" | grep "^${CHECK}|" || true)"
[ -z "$row" ] && { echo "unknown check '$CHECK'; known:"; printf '%s\n' "$CHECKS" | grep '|' | cut -d'|' -f1 | sed 's/^/  /'; exit 2; }
CFG="$(echo "$row" | cut -d'|' -f2)"
SPEC="$(echo "$row" | cut -d'|' -f3)"
NEEDLE="$(echo "$row" | cut -d'|' -f4)"

command -v java >/dev/null 2>&1 || { echo "hunt_tlc: java not found -- nothing to hunt"; exit 2; }
[ -f "$JAR" ] || { echo "hunt_tlc: $JAR missing"; exit 2; }

mkdir -p "$OUT"
echo "hunt_tlc: check=$CHECK  iters=$ITERS  jobs=$JOBS  -Xmx$XMX  timeout=${TMO}s"
echo "          expecting TLC output to contain: \"$NEEDLE\""
echo "          artifacts on failure -> $OUT"
echo

# One TLC run. Writes its log to $1; returns 0 if the needle appeared.
one() {
    local log="$1" meta="$2"
    # cd like run_tlc does -- the .cfg/.tla are resolved relative to $HERE,
    # and running from anywhere else makes TLC throw "unexpected exception"
    # which looks exactly like a reproduction. (It did, first try.)
    # -s QUIT: SIGQUIT makes a JVM print a FULL THREAD DUMP to stdout and keep
    # running, so on a hang the dump lands in the log by itself -- no race to
    # attach jstack before the process dies. -k 5 then hard-kills it 5s later.
    # (An earlier version tried jstack after the fact and always missed: the
    # JVM was already gone.)
    # -Djava.io.tmpdir: see run_tla.sh's run_tlc -- concurrent TLC JVMs sharing
    # /tmp race on the standard modules SANY extracts from the jar under fixed
    # names. This harness's whole point is -j concurrency, so without a private
    # tmpdir it manufactures the very "flake" it is hunting.
    local jtmp="${log%.log}.tmp"; mkdir -p "$jtmp"
    ( cd "$HERE" && timeout -s QUIT -k 5 "$TMO" java "-Xmx$XMX" \
        "-Djava.io.tmpdir=$jtmp" \
        -XX:+HeapDumpOnOutOfMemoryError "-XX:HeapDumpPath=${log%.log}.hprof" \
        -cp "$JAR" tlc2.TLC -workers "${TLC_WORKERS:-4}" \
        -metadir "$meta" -config "$CFG" "$SPEC" ) > "$log" 2>&1
    local rc=$?
    # A timeout is itself a finding: record it so the triage below can say so
    # rather than reporting a needle-miss with no explanation.
    { [ $rc -eq 124 ] || [ $rc -eq 137 ]; } && \
        echo "__HUNT_TIMEOUT__ after ${TMO}s (thread dump above, via SIGQUIT)" >> "$log"
    grep -q "$NEEDLE" "$log"
}

# Freeze the scene. Called once, on the first failure.
capture() {
    local log="$1" meta="$2" round="$3" slot="$4"
    {
        echo "=== hunt_tlc reproduction ==="
        date -u; echo
        echo "check      : $CHECK ($CFG / $SPEC)"
        echo "expected   : $NEEDLE"
        echo "round      : $round   concurrent slot: $slot of $JOBS"
        echo "-Xmx       : $XMX     workers: ${TLC_WORKERS:-4}   timeout: ${TMO}s"
        echo
        echo "--- verdict ---"
        if grep -q "__HUNT_TIMEOUT__" "$log"; then
            echo "  TIMED OUT -- TLC never reached a verdict."
            echo "  A JVM thread dump (SIGQUIT) is in tlc.log; grep for \"Full thread dump\"."
        elif grep -qiE "OutOfMemoryError|Java heap space|GC overhead" "$log"; then
            echo "  OOM -- infrastructure. A heap dump is beside this file if the JVM wrote one."
        elif grep -qiE "TLC threw an unexpected exception|Parsing or semantic analysis failed" "$log"; then
            echo "  TLC ERRORED before model checking -- usually a bad spec/config path,"
            echo "  not a reproduction. First lines:"
            grep -iE "^Error|Exception|cannot|not found" "$log" | head -3 | sed "s/^/    /"
        elif grep -q "No error has been found" "$log"; then
            echo "  COMPLETED WITH NO VIOLATION."
            echo "  For a negative control this is a REAL REGRESSION, not flakiness:"
            echo "  the injected bug is no longer detected. Do not retry past it."
        else
            echo "  Needle absent and no recognised cause -- see the log."
        fi
        echo
        echo "--- machine at failure ---"
        echo "loadavg : $(cat /proc/loadavg 2>/dev/null)"
        free -m 2>/dev/null | head -2 | sed 's/^/        /'
        echo "java    : $(java -version 2>&1 | head -1)"
        echo
        echo "--- reproduce this one run ---"
        # Keep -Djava.io.tmpdir in the PRINTED command too: a copy-pasteable
        # repro that omits it is exactly how the #688 race gets reintroduced by
        # hand, and it would then look like a fresh model failure.
        echo "  mkdir -p /tmp/mnnp_repro.tmp && cd $HERE && java -Xmx$XMX \\"
        echo "      -Djava.io.tmpdir=/tmp/mnnp_repro.tmp -cp $JAR tlc2.TLC -workers ${TLC_WORKERS:-4} \\"
        echo "      -metadir /tmp/mnnp_repro -config $CFG $SPEC"
        echo
        echo "--- attach a debugger to a live run ---"
        echo "  TLC_JDWP=1 tools/verify/tla/run_tla.sh     # then connect to :5005"
        echo
        echo "--- TLC output (tail) ---"
        tail -60 "$log"
    } > "$OUT/REPORT.txt" 2>&1

    cp "$log" "$OUT/tlc.log" 2>/dev/null
    [ -f "${log%.log}.hprof" ] && cp "${log%.log}.hprof" "$OUT/" 2>/dev/null
    [ -d "$meta" ] && cp -r "$meta" "$OUT/metadir" 2>/dev/null
}

fail=0
for round in $(seq 1 "$ITERS"); do
    pids=(); logs=(); metas=()
    for slot in $(seq 1 "$JOBS"); do
        lg="$OUT/.run_${round}_${slot}.log"; mt="$OUT/.meta_${round}_${slot}"
        mkdir -p "$mt"
        logs+=("$lg"); metas+=("$mt")
        one "$lg" "$mt" & pids+=($!)
    done
    for i in "${!pids[@]}"; do
        if ! wait "${pids[$i]}"; then
            echo
            echo "!! REPRODUCED on round $round (slot $((i+1))/$JOBS)"
            capture "${logs[$i]}" "${metas[$i]}" "$round" "$((i+1))"
            fail=1
        fi
    done
    [ "$fail" = 1 ] && break
    # Only keep artifacts from the failing round.
    rm -rf ${logs[@]+"${logs[@]}"} ${metas[@]+"${metas[@]}"} 2>/dev/null
    printf '.'
    [ $((round % 50)) -eq 0 ] && printf ' %d\n' "$round"
done
echo

if [ "$fail" = 1 ]; then
    echo "artifacts: $OUT"
    echo "  REPORT.txt  -- verdict, machine state, reproduce + debugger commands"
    echo "  tlc.log     -- full TLC output"
    ls "$OUT"/*.hprof >/dev/null 2>&1 && echo "  *.hprof     -- JVM heap dump (OOM path)"
    [ -d "$OUT/metadir" ] && echo "  metadir/    -- TLC state graph + checkpoints"
    echo
    sed -n '/--- verdict ---/,/^$/p' "$OUT/REPORT.txt"
    exit 1
fi

echo "no failure in $ITERS round(s) of $JOBS -- $((ITERS * JOBS)) TLC runs."
echo "If you are hunting the load-only flake, raise -j (the gate runs"
echo "VERIFY_JOBS=nproc concurrently) or squeeze the heap with -x."
rm -rf "$OUT"
exit 0
