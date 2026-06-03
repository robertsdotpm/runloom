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

# --- park_safe/wake_safe cross-thread handshake (runloom_sched.c) --------------
# Drift-guard: the harness's correct default assumes the source has the two
# StoreLoad seq_cst fences (added after GenMC found a lost wakeup in the
# fence-free version).  If the source loses a fence, the harness no longer
# mirrors it -- fail loudly so it gets re-synced rather than silently passing.
SCHED_C="$HERE/../../src/runloom_c/runloom_sched.c"
printf '  [genmc] %-30s ' "park/wake SC-fence drift-guard"
if [ -f "$SCHED_C" ] && [ "$(grep -c '__atomic_thread_fence(__ATOMIC_SEQ_CST)' "$SCHED_C")" -ge 2 ]; then
    green "PASS"; echo " -- runloom_sched.c park/wake retains its StoreLoad fences"; pass=$((pass+1))
else
    red "FAIL"; echo " -- runloom_sched.c lost a seq_cst fence; re-sync sched_parkwake.c"; fail=$((fail+1))
fi

printf '  [genmc] %-30s ' "sched_parkwake.c"
if "$G" -- "$HERE/sched_parkwake.c" >"$HERE/.genmc.pos.log" 2>&1 \
        && grep -q "No errors were detected" "$HERE/.genmc.pos.log"; then
    green "PASS"; echo " -- no lost wake / enqueued-at-most-once under RC11"; pass=$((pass+1))
else
    red "FAIL"; echo " -- see $HERE/.genmc.pos.log"; fail=$((fail+1))
fi
for ctl in BUG_NO_SC_FENCE BUG_NO_RECHECK BUG_NO_BUMP; do
    printf '  [genmc] %-30s ' "sched_parkwake.c(-D$ctl)"
    if "$G" -- "-D$ctl" "$HERE/sched_parkwake.c" >"$HERE/.genmc.neg.log" 2>&1; then
        red "FAIL"; echo " (expected a lost wake) -- see $HERE/.genmc.neg.log"; fail=$((fail+1))
    else
        if grep -qiE "violation|error" "$HERE/.genmc.neg.log"; then
            green "PASS"; echo " -- correctly DETECTS the lost wakeup"; pass=$((pass+1))
        else
            red "FAIL"; echo " (errored but no violation) -- see $HERE/.genmc.neg.log"; fail=$((fail+1))
        fi
    fi
done

# --- io_uring SINGLE-op park/wake commit handshake (io_uring.c op->wait) -----
# Drift-guard: the harness mirrors the RELEASE result-store + ACQ_REL exchange /
# CAS on op->wait.  If io_uring.c drops the handshake or weakens those orders,
# fail loudly so iouring_waitcommit.c gets re-synced rather than silently
# passing against a stale model.
IOU_C="$HERE/../../src/runloom_c/io_uring.c"
printf '  [genmc] %-30s ' "iouring wait-commit drift-guard"
if [ -f "$IOU_C" ] \
   && grep -q "RUNLOOM_IOURING_WAIT_PARKED" "$IOU_C" \
   && grep -q "RUNLOOM_IOURING_WAIT_DONE" "$IOU_C" \
   && [ "$(grep -c '__atomic_exchange_n(&op->wait' "$IOU_C")" -ge 2 ]; then
    green "PASS"; echo " -- io_uring.c retains the op->wait commit handshake (drain + ring)"; pass=$((pass+1))
else
    red "FAIL"; echo " -- io_uring.c changed the op->wait handshake; re-sync iouring_waitcommit.c"; fail=$((fail+1))
fi

printf '  [genmc] %-30s ' "iouring_waitcommit.c"
if "$G" -- "$HERE/iouring_waitcommit.c" >"$HERE/.genmc.pos.log" 2>&1 \
        && grep -q "No errors were detected" "$HERE/.genmc.pos.log"; then
    n="$(sed -n 's/.*complete executions explored: \([0-9]*\).*/\1/p' "$HERE/.genmc.pos.log" | tail -1)"
    green "PASS"; echo " -- no lost wake / no wake-without-park / result-visible (${n:-?} RC11 execs)"; pass=$((pass+1))
else
    red "FAIL"; echo " -- see $HERE/.genmc.pos.log"; fail=$((fail+1))
fi
for ctl in BUG_PARK_PLAIN_STORE BUG_EXCHANGE_RELAXED BUG_WOKE_RELAXED BUG_LOAD_RELAXED; do
    printf '  [genmc] %-30s ' "iouring_waitcommit.c(-D$ctl)"
    if "$G" -- "-D$ctl" "$HERE/iouring_waitcommit.c" >"$HERE/.genmc.neg.log" 2>&1; then
        red "FAIL"; echo " (expected a violation) -- see $HERE/.genmc.neg.log"; fail=$((fail+1))
    else
        if grep -qiE "violation|error" "$HERE/.genmc.neg.log"; then
            green "PASS"; echo " -- correctly DETECTS the lost wake / stale result"; pass=$((pass+1))
        else
            red "FAIL"; echo " (errored but no violation) -- see $HERE/.genmc.neg.log"; fail=$((fail+1))
        fi
    fi
done

rm -f "$HERE/.genmc.pos.log" "$HERE/.genmc.neg.log" 2>/dev/null
echo "  $pass passed, $fail failed"

# Chase-Lev work-stealing deque oracle -- its own harness (single-element
# take/steal race, 2-element SC-fence necessity, resize, + the REAL
# src/runloom_c/cldeque.c driven verbatim, each with a negative control).
# Wired in here so the standard sweep exercises it; its exit status folds
# into ours, so run_verify.sh fails if either tier regresses.
cl_rc=0
if [ -x "$HERE/run_chase_lev.sh" ]; then
    echo ""
    "$HERE/run_chase_lev.sh" || cl_rc=1
fi
[ "$fail" -eq 0 ] && [ "$cl_rc" -eq 0 ]
