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

# --- blocking-offload job lifetime (runloom_blockpool.c) ----------------------
# The offload job lives on the PARKED FIBER's coroutine stack (freed when the
# fiber returns), yet a worker OS thread touches it -- and a spurious wake
# (task.cancel -> G.wake) can resume + free it while the worker still runs.  The
# `done` release/acquire handshake (worker snapshots its wake target BEFORE done
# and touches nothing after; fiber waits for done before freeing) closes the UAF.
printf '  [genmc] %-30s ' "blockpool_job.c"
if "$G" -- "$HERE/blockpool_job.c" >"$HERE/.genmc.pos.log" 2>&1 \
        && grep -q "No errors were detected" "$HERE/.genmc.pos.log"; then
    n="$(sed -n 's/.*complete executions explored: \([0-9]*\).*/\1/p' "$HERE/.genmc.pos.log" | tail -1)"
    green "PASS"; echo " -- no use-after-free of the stack job / result-seen (${n:-?} RC11 execs)"; pass=$((pass+1))
else
    red "FAIL"; echo " -- see $HERE/.genmc.pos.log"; fail=$((fail+1))
fi

for bug in BUG_FIBER_NO_DONE_WAIT BUG_WORKER_LATE_READ; do
    printf '  [genmc] %-30s ' "blockpool_job.c(-D$bug)"
    if "$G" -- "-D$bug" "$HERE/blockpool_job.c" >"$HERE/.genmc.neg.log" 2>&1; then
        red "FAIL"; echo " (expected a UAF) -- see $HERE/.genmc.neg.log"; fail=$((fail+1))
    elif grep -qiE "violation|error|assert" "$HERE/.genmc.neg.log"; then
        green "PASS"; echo " -- correctly DETECTS the stack-job use-after-free"; pass=$((pass+1))
    else
        red "FAIL"; echo " (errored but no UAF reported) -- see $HERE/.genmc.neg.log"; fail=$((fail+1))
    fi
done

# --- channel refcount free protocol (chan_waiters.c.inc) ----------------------
# Two hubs hold a ref to a shared channel.  incref is RELAXED (held across); decref
# is ACQ_REL so the decrement-to-0 orders the runloom_mutex_destroy + PyMem_Free
# after every holder's last use -- no use-after-free, freed once.
printf '  [genmc] %-30s ' "chan_refcount.c"
if "$G" -- "$HERE/chan_refcount.c" >"$HERE/.genmc.pos.log" 2>&1 \
        && grep -q "No errors were detected" "$HERE/.genmc.pos.log"; then
    n="$(sed -n 's/.*complete executions explored: \([0-9]*\).*/\1/p' "$HERE/.genmc.pos.log" | tail -1)"
    green "PASS"; echo " -- channel freed once, no use-after-free under RC11 (${n:-?} execs)"; pass=$((pass+1))
else
    red "FAIL"; echo " -- see $HERE/.genmc.pos.log"; fail=$((fail+1))
fi

printf '  [genmc] %-30s ' "chan_refcount.c(-DBUG_DECREF_RELAXED)"
if "$G" -- -DBUG_DECREF_RELAXED "$HERE/chan_refcount.c" >"$HERE/.genmc.neg.log" 2>&1; then
    red "FAIL"; echo " (expected a UAF race) -- see $HERE/.genmc.neg.log"; fail=$((fail+1))
elif grep -qiE "race|error|violation" "$HERE/.genmc.neg.log"; then
    green "PASS"; echo " -- correctly DETECTS the free vs field-access data race (relaxed decref)"; pass=$((pass+1))
else
    red "FAIL"; echo " (errored but no race reported) -- see $HERE/.genmc.neg.log"; fail=$((fail+1))
fi

# --- park_safe/wake_safe cross-thread handshake (runloom_sched*) ---------------
# Drift-guard: the harness's correct default assumes the source has the two
# StoreLoad seq_cst fences (added after GenMC found a lost wakeup in the
# fence-free version).  If the source loses a fence, the harness no longer
# mirrors it -- fail loudly so it gets re-synced rather than silently passing.
# The C-layout refactor split runloom_sched.c into runloom_sched_*.c.inc
# modules (the park/wake fences now live in runloom_sched_parkwake.c.inc), so
# count across the whole module set rather than one filename.
SRCDIR="$HERE/../../../src/runloom_c"
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
# Drift-guard: sched_parkwake_seam.c is a FAITHFUL SLICE (not byte-shared), so
# its wake_state enum must track runloom_sched.h exactly -- names AND encodings.
# This model already drifted once (a 4-state copy of the 6-state kernel: the
# SWEEPING/SWEEPING_WOKEN sweeper as a third claimer went unmodeled, so the gate
# was silently green against a stale FSM).  Assert the enum matches so a new
# state / changed encoding fails LOUDLY and forces a re-sync of the model + the
# sweeper thread, rather than proving properties of a copy.
printf '  [genmc] %-30s ' "wake_state FSM drift-guard"
if python3 - "$SRCDIR/runloom_sched.h" "$HERE/sched_parkwake_seam.c" <<'PYEOF'
import re, sys
hdr = open(sys.argv[1]).read(); model = open(sys.argv[2]).read()
src = {m.group(1): int(m.group(2))
       for m in re.finditer(r'#define\s+RUNLOOM_WS_(\w+)\s+(\d+)', hdr)}
mod = {m.group(1): int(m.group(2))
       for m in re.finditer(r'\bWS_(\w+)\s*=\s*(\d+)', model)}
sys.exit(0 if src and src == mod else 1)
PYEOF
then
    green "PASS"; echo " -- model wake_state enum matches runloom_sched.h (6 states)"; pass=$((pass+1))
else
    red "FAIL"; echo " -- wake_state FSM in runloom_sched.h diverged from the model; re-sync sched_parkwake_seam.c (enum + the sweeper claimer)"; fail=$((fail+1))
fi

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
IOU_SRCS=( "$HERE"/../../../src/runloom_c/io_uring.c "$HERE"/../../../src/runloom_c/io_uring*.c.inc )
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

# --- LIFECYCLE migration-drain trio: the per-g-tstate hub->hub MIGRATION drain ---
# The weak-memory fidelity layer the RunloomTstateMigration.tla PLACEMENT proof
# defers to (LIFECYCLE_INVARIANTS.md "deep CPython surfaces").  Three orthogonal
# drains a correct mimalloc-heap migration must run, each proven as the data race
# the drain prevents under RC11:
#   mimalloc_page_free.c -- WHO may touch a page (per-page xthread_id abandon/adopt)
#   qsbr_drain.c         -- WHEN a deferred free may run (QSBR grace period)
#   brc_merge.c          -- WHO may merge a refcount (biased-refcount owner drain)
# Each is gated off in the shipping runtime (RUNLOOM_ALLOW_UNSAFE_MIGRATION); these
# are the SPEC a candidate abandon/adopt handshake must satisfy before it is trusted.
genmc_model() {                 # name  correct-grep  "BUG1 BUG2 ..."  blurb
    local f="$1" posgrep="$2" bugs="$3" blurb="$4"
    printf '  [genmc] %-30s ' "$f"
    if "$G" -- "$HERE/$f" >"$HERE/.genmc.pos.log" 2>&1 \
            && grep -q "$posgrep" "$HERE/.genmc.pos.log"; then
        n="$(sed -n 's/.*complete executions explored: \([0-9]*\).*/\1/p' "$HERE/.genmc.pos.log" | tail -1)"
        green "PASS"; echo " -- $blurb (${n:-?} RC11 execs)"; pass=$((pass+1))
    else
        red "FAIL"; echo " -- see $HERE/.genmc.pos.log"; fail=$((fail+1))
    fi
    for bug in $bugs; do
        printf '  [genmc] %-30s ' "$f(-D$bug)"
        if "$G" -- "-D$bug" "$HERE/$f" >"$HERE/.genmc.neg.log" 2>&1; then
            red "FAIL"; echo " (expected a race/violation) -- see $HERE/.genmc.neg.log"; fail=$((fail+1))
        elif grep -qiE "race|error|violation|assert" "$HERE/.genmc.neg.log"; then
            green "PASS"; echo " -- correctly DETECTS the undrained-migration hazard"; pass=$((pass+1))
        else
            red "FAIL"; echo " (errored but no race/violation) -- see $HERE/.genmc.neg.log"; fail=$((fail+1))
        fi
    done
}
genmc_model mimalloc_page_free.c "No errors were detected" \
    "BUG_LOCAL_ON_STALE BUG_ADOPT_RELAXED" \
    "per-page xthread_id abandon/adopt orders the owner-only heap-queue path -- no race"
genmc_model qsbr_drain.c "No errors were detected" \
    "BUG_NO_GRACE BUG_POLL_RELAXED" \
    "QSBR grace poll orders the deferred free after every reader's quiescent state"
genmc_model brc_merge.c "No errors were detected" \
    "BUG_MERGE_AFTER_MIGRATE" \
    "biased-refcount merge drained on the owner thread -- no cross-thread ob_ref_local race"

rm -f "$HERE/.genmc.pos.log" "$HERE/.genmc.neg.log" "$HERE/.genmc.mix.log" 2>/dev/null
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
