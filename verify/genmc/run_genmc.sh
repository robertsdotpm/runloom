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

# --- park_safe/wake_safe cross-thread handshake (runloom_sched*) ---------------
# Drift-guard: the harness's correct default assumes the source has the two
# StoreLoad seq_cst fences (added after GenMC found a lost wakeup in the
# fence-free version).  If the source loses a fence, the harness no longer
# mirrors it -- fail loudly so it gets re-synced rather than silently passing.
# The C-layout refactor split runloom_sched.c into runloom_sched_*.c.inc
# modules (the park/wake fences now live in runloom_sched_parkwake.c.inc), so
# count across the whole module set rather than one filename.
SRCDIR="$HERE/../../src/runloom_c"
printf '  [genmc] %-30s ' "park/wake SC-fence drift-guard"
nfence=$(grep -h '__atomic_thread_fence(__ATOMIC_SEQ_CST)' \
            "$SRCDIR"/runloom_sched.c "$SRCDIR"/runloom_sched_*.c.inc 2>/dev/null | wc -l)
if [ "$nfence" -ge 2 ]; then
    green "PASS"; echo " -- runloom_sched* park/wake retains its StoreLoad fences"; pass=$((pass+1))
else
    red "FAIL"; echo " -- runloom_sched* lost a seq_cst fence; re-sync sched_parkwake.c"; fail=$((fail+1))
fi

# --- netpoll data-fd LEVEL-arming drift-guard --------------------------------
# Two correctness arguments depend on the epoll data-fd staying LEVEL-triggered
# (EPOLLIN|EPOLLRDHUP, NO EPOLLET): (1) runloom_fd_pending_wake_consume's poll()
# re-check (a stale stash is discarded because LEVEL re-fires a genuine edge);
# (2) runloom_netpoll_unpark_many / cancel_g NOT disarming the fd after a direct
# wake (a later level re-fire is harmlessly stashed + poll-rechecked).  Switching
# to EPOLLET would silently break both -- so fail loudly if the arm changes.
printf '  [genmc] %-30s ' "netpoll LEVEL-arming drift-guard"
REG="$SRCDIR/netpoll_register.c.inc"
if grep -q 'ev\.events |= EPOLLIN | EPOLLRDHUP' "$REG" \
   && ! grep -q 'ev\.events.*EPOLLET' "$REG"; then
    green "PASS"; echo " -- netpoll data-fd arms LEVEL (no EPOLLET); poll re-check + unpark no-disarm stay sound"; pass=$((pass+1))
else
    red "FAIL"; echo " -- netpoll_register arming changed; re-audit pending_wake poll() + unpark_many no-disarm"; fail=$((fail+1))
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

# --- SEAM: the two park/wake protocols composed on one migratable fiber -------
# The Dekker handshake (sched_parkwake.c) and the wake_state machine
# (iouring/global-runq) are each proven in isolation; this checks their
# COMPOSITION -- a park that commits via Dekker then the wake_state CAS, racing
# wake_g -- holds no-lost-wake + enqueued-at-most-once under RC11.  Gate this
# BEFORE promoting RUNLOOM_STEAL_WOKEN / RUNLOOM_PER_G_TSTATE toward default.
printf '  [genmc] %-30s ' "sched_parkwake_seam.c"
if "$G" -- "$HERE/sched_parkwake_seam.c" >"$HERE/.genmc.pos.log" 2>&1 \
        && grep -q "No errors were detected" "$HERE/.genmc.pos.log"; then
    green "PASS"; echo " -- Dekker+wake_state seam: no lost / at-most-once under RC11"; pass=$((pass+1))
else
    red "FAIL"; echo " -- see $HERE/.genmc.pos.log"; fail=$((fail+1))
fi
printf '  [genmc] %-30s ' "sched_parkwake_seam.c(-DBUG_NO_RUNNING_WOKEN_REQUEUE)"
if "$G" -- "-DBUG_NO_RUNNING_WOKEN_REQUEUE" "$HERE/sched_parkwake_seam.c" >"$HERE/.genmc.neg.log" 2>&1; then
    red "FAIL"; echo " (expected a lost wake) -- see $HERE/.genmc.neg.log"; fail=$((fail+1))
else
    if grep -qiE "violation|error" "$HERE/.genmc.neg.log"; then
        green "PASS"; echo " -- correctly DETECTS the lost wakeup (blind PARKED store)"; pass=$((pass+1))
    else
        red "FAIL"; echo " (errored but no violation) -- see $HERE/.genmc.neg.log"; fail=$((fail+1))
    fi
fi
# Informational probe (not a gate): -DSEAM_MIX_DEKKER asks whether waking ONE
# migratable fiber via BOTH routes (wake_safe + wake_g) can double-enqueue.  If
# it reports a violation, the constraint "migratable fibers are woken via wake_g
# ONLY" is load-bearing -- keep it enforced in the wake routing.
if [ "${RUNLOOM_GENMC_SEAM_MIX:-}" = "1" ]; then
    printf '  [genmc] %-30s ' "sched_parkwake_seam.c(-DSEAM_MIX_DEKKER,info)"
    if "$G" -- "-DSEAM_MIX_DEKKER" "$HERE/sched_parkwake_seam.c" >"$HERE/.genmc.mix.log" 2>&1 \
            && grep -q "No errors were detected" "$HERE/.genmc.mix.log"; then
        echo "mixing both wake routes is safe"
    else
        echo "mixing both wake routes DOUBLE-ENQUEUES -> keep wake_g-only invariant (see .genmc.mix.log)"
    fi
fi

# --- io_uring SINGLE-op park/wake commit handshake (io_uring.c op->wait) -----
# Drift-guard: the harness mirrors the RELEASE result-store + ACQ_REL exchange /
# CAS on op->wait.  If io_uring.c drops the handshake or weakens those orders,
# fail loudly so iouring_waitcommit.c gets re-synced rather than silently
# passing against a stale model.
# The refactor split io_uring.c into io_uring*.c.inc modules (the op->wait
# markers + exchanges now live in io_uring_l_{ring,buf,...}.c.inc), so check
# the whole io_uring module set rather than one filename.
IOU_SRCS=( "$HERE"/../../src/runloom_c/io_uring.c "$HERE"/../../src/runloom_c/io_uring*.c.inc )
printf '  [genmc] %-30s ' "iouring wait-commit drift-guard"
if grep -qh "RUNLOOM_IOURING_WAIT_PARKED" "${IOU_SRCS[@]}" 2>/dev/null \
   && grep -qh "RUNLOOM_IOURING_WAIT_DONE" "${IOU_SRCS[@]}" 2>/dev/null \
   && [ "$(grep -h '__atomic_exchange_n(&op->wait' "${IOU_SRCS[@]}" 2>/dev/null | wc -l)" -ge 2 ]; then
    green "PASS"; echo " -- io_uring* retains the op->wait commit handshake (drain + ring)"; pass=$((pass+1))
else
    red "FAIL"; echo " -- io_uring* changed the op->wait handshake; re-sync iouring_waitcommit.c"; fail=$((fail+1))
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
