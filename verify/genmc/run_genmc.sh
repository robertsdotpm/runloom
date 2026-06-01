#!/usr/bin/env bash
# run_genmc.sh -- verify the netpoll commit-claim protocol with GenMC, on the
# REAL C (pthreads + C11 atomics) under the RC11 weak memory model.  Where Spin
# (netpoll_deadline.pml) proves no bad SC interleaving, GenMC explores every
# RC11 execution of the actual atomics + the pool->lock mutex, so it also
# catches a misplaced fence / a missing lock (a data race).
#
# Needs the `genmc` executable.  Set GENMC=/path/to/genmc, or it searches PATH
# and a couple of common build/install locations.  Build: see
# https://github.com/MPI-SWS/genmc (needs LLVM + g++>=14 / clang).
#
# Checks:
#   netpoll_claim.c              -> No errors (no data race, value-correct,
#                                   exactly-once across all RC11 executions).
#   netpoll_claim.c -DBUG_NO_LOCK-> Non-atomic race: the aborting g reads
#                                   ready_out WITHOUT the pool->lock round-trip,
#                                   racing the claimer's publish.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"

G="${GENMC:-}"
if [ -z "$G" ]; then
    for cand in "$(command -v genmc 2>/dev/null)" \
                /usr/local/bin/genmc/genmc /usr/local/bin/genmc \
                /tmp/genmc/build2/bin/genmc; do
        if [ -n "$cand" ] && [ -x "$cand" ] && [ ! -d "$cand" ]; then G="$cand"; break; fi
    done
fi

pass=0; fail=0
green() { printf '\033[32m%s\033[0m' "$1"; }
red()   { printf '\033[31m%s\033[0m' "$1"; }

if [ -z "$G" ] || [ ! -x "$G" ]; then
    echo "  (genmc not found -- skipping;  set GENMC=/path/to/genmc)"
    exit 0
fi

echo "-- GenMC (RC11, real C: pthreads + C11 atomics) --"

printf '  [genmc] %-30s ' "netpoll_claim.c"
if "$G" -- "$HERE/netpoll_claim.c" >"$HERE/.genmc.pos.log" 2>&1 \
        && grep -q "No errors were detected" "$HERE/.genmc.pos.log"; then
    n="$(sed -n 's/.*complete executions explored: \([0-9]*\).*/\1/p' "$HERE/.genmc.pos.log" | tail -1)"
    green "PASS"; echo " -- no data race / value-correct / exactly-once (${n:-?} RC11 execs)"; pass=$((pass+1))
else
    red "FAIL"; echo " -- see $HERE/.genmc.pos.log"; fail=$((fail+1))
fi

printf '  [genmc] %-30s ' "netpoll_claim.c(-DBUG_NO_LOCK)"
if "$G" -- -DBUG_NO_LOCK "$HERE/netpoll_claim.c" >"$HERE/.genmc.neg.log" 2>&1; then
    red "FAIL"; echo " (expected a race) -- see $HERE/.genmc.neg.log"; fail=$((fail+1))
else
    if grep -qiE "race|error" "$HERE/.genmc.neg.log"; then
        green "PASS"; echo " -- correctly DETECTS the ready_out data race (no lock round-trip)"; pass=$((pass+1))
    else
        red "FAIL"; echo " (errored but no race reported) -- see $HERE/.genmc.neg.log"; fail=$((fail+1))
    fi
fi

rm -f "$HERE/.genmc.pos.log" "$HERE/.genmc.neg.log" 2>/dev/null
echo "  $pass passed, $fail failed"

# Chase-Lev work-stealing deque oracle -- its own harness (single-element
# take/steal race, 2-element SC-fence necessity, resize, + the REAL
# src/pygo_core/cldeque.c driven verbatim, each with a negative control).
# Wired in here so the standard sweep exercises it; its exit status folds
# into ours, so run_verify.sh fails if either tier regresses.
cl_rc=0
if [ -x "$HERE/run_chase_lev.sh" ]; then
    echo ""
    "$HERE/run_chase_lev.sh" || cl_rc=1
fi
[ "$fail" -eq 0 ] && [ "$cl_rc" -eq 0 ]
