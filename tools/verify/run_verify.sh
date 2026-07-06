#!/usr/bin/env bash
# run_verify.sh -- run every runloom formal-verification check and report.
#
# Two engines:
#   SPIN  -- exhaustive interleaving model checker (Promela models of the
#            lock-free algorithms; sequentially-consistent memory model).
#   CBMC  -- bounded model checker run on the UNMODIFIED C source of the
#            Chase-Lev deque (real index arithmetic + __atomic_* orderings).
#
# A model PASSES when the checker reports zero errors / VERIFICATION
# SUCCESSFUL.  One NEGATIVE control (wake_state with BUGGY_DROP_WAKE) must
# FAIL -- proving the no-lost-wake property actually has teeth.
#
# PARALLELISM: every Spin model and every CBMC group is an INDEPENDENT job
# (its own $WORK subdir + log), so they run through a bounded worker pool
# rather than strictly serially -- the CBMC checks are the wall-clock floor and
# the box has many cores.  Concurrency = VERIFY_JOBS (default: nproc).  Set
# VERIFY_JOBS=1 for the old strictly-serial behaviour (handy when bisecting a
# single model).  CBMC teeth stay serial WITHIN each group, so at most one cbmc
# per group runs at a time -> concurrent cbmc <= number of cbmc groups (~7),
# which keeps peak memory bounded.
#
# Usage: verify/run_verify.sh [-q]      (-q = less chatter)
#        VERIFY_JOBS=1 verify/run_verify.sh        # strictly serial
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"   # verify lives under tools/, so repo root is two up
SPIN_DIR="$HERE/spin"
CBMC_DIR="$HERE/cbmc"
WORK="$(mktemp -d /tmp/runloom_verify.XXXXXX)"
QUIET=0; [ "${1:-}" = "-q" ] && QUIET=1

pass=0; fail=0; skipped=0; FAILED=""
green() { printf '\033[32m%s\033[0m' "$1"; }
red()   { printf '\033[31m%s\033[0m' "$1"; }

note() { [ "$QUIET" = 0 ] && echo "    $*"; }

have() { command -v "$1" >/dev/null 2>&1; }

# ---------------- model<->source drift lints (cheap, no engine) ---------
# Guard against the two drift classes the audit found: stale doc->source
# citations (cite_drift) and hand-transcribed models drifting from the source
# they mirror (model_source_drift, the model-logic analogue).  Both run in <1s,
# need no build, and fail the verify gate on drift so a transcription can't
# silently diverge from src/runloom_c.  (See docs/dev/frontier/MODEL_SOURCE_AUDIT.md.)
if have python3; then
    for lint in cite_drift model_source_drift; do
        [ -f "$HERE/$lint.py" ] || continue
        printf '  [lint] %-28s ' "$lint"
        if python3 "$HERE/$lint.py" >"/tmp/runloom_$lint.log" 2>&1; then
            echo "OK"; pass=$((pass + 1))
        else
            echo "DRIFT (see /tmp/runloom_$lint.log)"; fail=$((fail + 1)); FAILED="$FAILED $lint"
        fi
    done
    # feature_gate_lint: catch a UAPI feature-macro gate that silently compiles
    # to the #else stub because its TU forgot the defining header (a shipped
    # hang once).  Needs a C preprocessor + UAPI headers; exit 2 => headers
    # absent => SKIP (not fail) so the gate stays runnable off-Linux.
    if [ -f "$HERE/feature_gate_lint.py" ] && have cc; then
        printf '  [lint] %-28s ' "feature_gate"
        python3 "$HERE/feature_gate_lint.py" >"/tmp/runloom_feature_gate.log" 2>&1
        rc=$?
        if [ "$rc" = 0 ]; then
            echo "OK"; pass=$((pass + 1))
        elif [ "$rc" = 2 ]; then
            echo "SKIP (no UAPI headers)"; skipped=$((skipped + 1))
        else
            echo "STUB-TRAP (see /tmp/runloom_feature_gate.log)"
            fail=$((fail + 1)); FAILED="$FAILED feature_gate"
        fi
    fi
    # semantics conformance: EXECUTE the audited kernel-contract probes (epoll
    # ET/LEVEL/EXCLUSIVE/ONESHOT) against THIS box's kernel, so a misread that a
    # hand model would bless fails here instead.  SKIPs cleanly off-Linux.
    if [ -f "$HERE/semantics/conformance.py" ] && have cc; then
        printf '  [lint] %-28s ' "semantics_conformance"
        if python3 "$HERE/semantics/conformance.py" >"/tmp/runloom_semconform.log" 2>&1; then
            echo "OK"; pass=$((pass + 1))
        else
            echo "KERNEL-DIVERGES (see /tmp/runloom_semconform.log)"
            fail=$((fail + 1)); FAILED="$FAILED semantics_conformance"
        fi
    fi
    # fd chokepoint: epoll_ctl (kernel registration) must stay single-writer --
    # the ratchet that keeps the stale-cache-vs-kernel class from spreading again.
    if [ -f "$HERE/fd_chokepoint_lint.py" ]; then
        printf '  [lint] %-28s ' "fd_chokepoint"
        if python3 "$HERE/fd_chokepoint_lint.py" >"/tmp/runloom_fd_chokepoint.log" 2>&1; then
            echo "OK"; pass=$((pass + 1))
        else
            echo "SURFACE-SPREAD (see /tmp/runloom_fd_chokepoint.log)"
            fail=$((fail + 1)); FAILED="$FAILED fd_chokepoint"
        fi
    fi
    # tstate manifest: every PyThreadState field must have a decided disposition
    # so a new CPython field can't slip in unclassified (item 15).  SKIPs if
    # libclang is absent (returns 0 with a SKIP line).
    if [ -f "$HERE/tstate_manifest_lint.py" ]; then
        printf '  [lint] %-28s ' "tstate_manifest"
        if python3 "$HERE/tstate_manifest_lint.py" >"/tmp/runloom_tstate_manifest.log" 2>&1; then
            grep -q "SKIP" "/tmp/runloom_tstate_manifest.log" && echo "SKIP (no libclang)" \
                && skipped=$((skipped + 1)) || { echo "OK"; pass=$((pass + 1)); }
        else
            echo "UNCLASSIFIED-FIELD (see /tmp/runloom_tstate_manifest.log)"
            fail=$((fail + 1)); FAILED="$FAILED tstate_manifest"
        fi
    fi
    # demonic-oracle CBMC harnesses (arm/re-arm window + two-ledger refinement):
    # cheap (tiny models, seconds), kernel-independent, and each checks its own
    # negative controls behave as audited (default + teeth).  SKIP if no cbmc.
    if have cbmc; then
        for dh in run_demonic run_refinement; do
            [ -f "$HERE/cbmc/$dh.sh" ] || continue
            printf '  [cbmc] %-28s ' "$dh"
            if bash "$HERE/cbmc/$dh.sh" >"/tmp/runloom_$dh.log" 2>&1; then
                echo "OK"; pass=$((pass + 1))
            else
                echo "DRIFTED (see /tmp/runloom_$dh.log)"
                fail=$((fail + 1)); FAILED="$FAILED $dh"
            fi
        done
    fi
fi

# ---------------- parallel job engine ----------------------------------
# Each check runs as a pooled background job that prints its own one-line
# result and RETURNS 0 (pass) / non-0 (fail).  collect() folds those verdicts
# into pass/fail/FAILED *in the main shell* -- a subshell's counter increment
# would be lost -- and replays each job's captured output in submission order,
# so the report is byte-identical to the serial run regardless of finish order.
NPROC="$(command -v nproc >/dev/null 2>&1 && nproc || echo 4)"
JOBS="${VERIFY_JOBS:-$NPROC}"
case "$JOBS" in ''|*[!0-9]*) JOBS=1 ;; esac
[ "$JOBS" -ge 1 ] || JOBS=1
NJOB=0
JDIR="$WORK/.jobs"

# VERIFY_FAST=1 -- the "fast verify" lane: run ALL Spin models + every cheap CBMC
# proof, but SKIP the 3 genuinely slow CBMC proofs (measured floor, not guesses):
#   cldeque concurrent proof ~148s, INV_race disjoint monitor ~5-10min, and the
#   timer min-heap ~76s.  Keeps ~all formal coverage as a sub-minute smoke gate;
#   the extensive lane (VERIFY_FAST unset) runs these too.  The deque still has
#   Spin (cldeque + live_deque) + the C stress test (check_all ctest) coverage.
FAST="${VERIFY_FAST:-0}"
case "$FAST" in 1|yes|true|on) FAST=1 ;; *) FAST=0 ;; esac

# new_phase <tag> -- fresh job dir + counter, so two collect() calls never
# re-read each other's results (no cleanup / rm needed; the dirs live in $WORK).
# POOL_PIDS is reset so collect() waits only for THIS phase's jobs -- not for the
# background external engines (TLA+/Alloy/...), which are drained separately.
POOL_PIDS=()
new_phase() { NJOB=0; JDIR="$WORK/.jobs.$1"; mkdir -p "$JDIR"; POOL_PIDS=(); }

# block until fewer than $JOBS pooled jobs are still running
sem() {
    while [ "$(jobs -rp | wc -l)" -ge "$JOBS" ]; do wait -n 2>/dev/null || break; done
}

# launch <label> <fn> [args...]  -- run fn(args...) as a pooled background job.
# <label> is the token printed in the "failed:" list if the job fails.
launch() {
    local label="$1"; shift
    NJOB=$((NJOB + 1))
    local id; id="$(printf '%05d' "$NJOB")"
    printf '%s' "$label" > "$JDIR/$id.label"
    sem
    { local rc=0; "$@" || rc=$?; printf '%s' "$rc" > "$JDIR/$id.rc"; } \
        > "$JDIR/$id.out" 2>&1 &
    POOL_PIDS+=($!)
}

# wait for THIS phase's pool jobs (not the bg engines), then replay output in
# order + tally verdicts.
collect() {
    [ ${#POOL_PIDS[@]} -gt 0 ] && wait "${POOL_PIDS[@]}" 2>/dev/null
    local f id rc label
    for f in $(ls "$JDIR"/*.out 2>/dev/null | sort); do
        id="$(basename "$f" .out)"
        rc="$(cat "$JDIR/$id.rc" 2>/dev/null || echo 1)"
        label="$(cat "$JDIR/$id.label" 2>/dev/null)"
        cat "$f"
        if [ "$rc" = 0 ]; then pass=$((pass + 1))
        else fail=$((fail + 1)); FAILED="$FAILED $label"; fi
    done
}

# ---- Spin: a model passes iff pan reports "errors: 0" -----------------
run_spin() {
    local name="$1" extra="${2:-}"
    # UNIQUE dir per invocation: a model checked more than once (positive +
    # variant/negative controls -- e.g. select_close + its 4 -DBUG_* runs) would
    # otherwise share $WORK/$name and clobber each other's pan.c/pan/run.log when
    # the checks run concurrently.  (run_spin_live already does this.)
    local d; d="$(mktemp -d "$WORK/${name}.XXXXXX")"; cp "$SPIN_DIR/$name.pml" "$d/"
    ( cd "$d" || exit 2
      spin $extra -a "$name.pml" >gen.log 2>&1 || { echo "SPINGEN_FAIL"; exit 0; }
      cc -O2 -o pan pan.c >cc.log 2>&1 || { echo "CC_FAIL"; exit 0; }
      ./pan -m500000 >run.log 2>&1
      grep -q "errors: 0" run.log && echo OK || echo BAD )
}

# Each check_* below PRINTS its result line and RETURNS 0 (pass) / 1 (fail);
# `launch` captures both.  (They used to mutate pass/fail directly; collect()
# now owns the tally so the counts survive the worker subshells.)
check_spin() {  # name, human-description
    local name="$1" desc="$2"
    printf '  [spin] %-34s ' "$name"
    local r; r="$(run_spin "$name")"
    if [ "$r" = OK ]; then green "PASS"; echo " -- $desc"; return 0
    else red "FAIL"; echo " ($r) -- $desc"; return 1; fi
}

# a SECOND positive config of the same model, selected by a -D define (MUST pass)
check_spin_variant() {  # name, define, human-description
    local name="$1" def="$2" desc="$3"
    printf '  [spin] %-34s ' "$name(-D$def)"
    local r; r="$(run_spin "$name" "-D$def")"
    if [ "$r" = OK ]; then green "PASS"; echo " -- $desc"; return 0
    else red "FAIL"; echo " ($r) -- $desc"; return 1; fi
}

# negative control: this model MUST fail (proves the check catches the bug)
check_spin_must_fail() {  # name, define, human-description
    local name="$1" def="$2" desc="$3"
    printf '  [spin] %-34s ' "$name(-D$def)"
    local r; r="$(run_spin "$name" "-D$def")"
    if [ "$r" = BAD ]; then green "PASS"; echo " -- correctly DETECTS: $desc"; return 0
    else red "FAIL"; echo " (expected the injected bug to be caught) -- $desc"; return 1; fi
}

# ---- Spin LIVENESS: acceptance-cycle detection (pan -a), optionally under
#      weak fairness (pan -a -f).  A liveness model passes iff pan finds no
#      acceptance cycle ("errors: 0"); a negative control passes iff it DOES
#      find one.  NOREDUCE keeps partial-order reduction out of the
#      fairness/acceptance search (sound, and the models are tiny). ----------
run_spin_live() {  # name, panflags (e.g. "-a -f"), extra-spin-define
    local name="$1" panflags="$2" extra="${3:-}"
    local d; d="$(mktemp -d "$WORK/${name}.XXXX")"; cp "$SPIN_DIR/$name.pml" "$d/"
    ( cd "$d" || exit 2
      spin $extra -a "$name.pml" >gen.log 2>&1 || { echo "SPINGEN_FAIL"; exit 0; }
      cc -O2 -DNOREDUCE -o pan pan.c >cc.log 2>&1 || { echo "CC_FAIL"; exit 0; }
      ./pan $panflags -m500000 >run.log 2>&1
      grep -q "errors: 0" run.log && echo OK || echo BAD )
}

check_spin_live() {  # name, panflags, human-description
    local name="$1" panflags="$2" desc="$3"
    printf '  [spin] %-26s ' "$name [$panflags]"
    local r; r="$(run_spin_live "$name" "$panflags")"
    if [ "$r" = OK ]; then green "PASS"; echo " -- $desc"; return 0
    else red "FAIL"; echo " ($r) -- $desc"; return 1; fi
}

# negative liveness control: pan MUST find an acceptance cycle here.
check_spin_live_must_fail() {  # name, panflags, define-or-empty, human-description
    local name="$1" panflags="$2" def="$3" desc="$4"
    local extra=""; [ -n "$def" ] && extra="-D$def"
    printf '  [spin] %-26s ' "$name [$panflags${def:+ -D$def}]"
    local r; r="$(run_spin_live "$name" "$panflags" "$extra")"
    if [ "$r" = BAD ]; then green "PASS"; echo " -- $desc"; return 0
    else red "FAIL"; echo " (expected a liveness violation) -- $desc"; return 1; fi
}

# ---- external verification engines (herd7/GenMC/Dartagnan/TLA+/Alloy/Coq/Iris/
#      iRC11) -- independent sub-scripts that each print "N passed, 0 failed".
#      Launch them in the BACKGROUND right now so they overlap the Spin+CBMC pool
#      (TLA+ alone is ~30-40s; serially they were the fast-lane tail).  Drained +
#      folded into the tally at the very end, in declaration order. ------------
ENGDIR="$WORK/.engines"; mkdir -p "$ENGDIR"
ENG_ORDER=""
eng_bg() {  # label  script-relpath
    [ -x "$HERE/$2" ] || return 0
    ENG_ORDER="$ENG_ORDER $1"
    ( "$HERE/$2" > "$ENGDIR/$1.out" 2>&1; printf '%s' "$?" > "$ENGDIR/$1.rc" ) &
}
eng_finish() {  # fold every launched engine's result in, in declaration order
    local label out rc n
    for label in $ENG_ORDER; do
        [ -f "$ENGDIR/$label.out" ] || continue
        out="$(cat "$ENGDIR/$label.out")"; rc="$(cat "$ENGDIR/$label.rc" 2>/dev/null || echo 1)"
        echo "$out"
        if [ "$rc" = 0 ]; then
            if echo "$out" | grep -q "passed, 0 failed"; then
                n="$(echo "$out" | sed -n 's/.* \([0-9]*\) passed, 0 failed/\1/p' | tail -1)"
                [ -n "$n" ] && pass=$((pass+n))
            else
                skipped=$((skipped+1))   # rc=0 but no pass-count -> engine skipped (tool absent)
            fi
        else
            fail=$((fail+1)); FAILED="$FAILED $label"
        fi
    done
}
eng_bg litmus    litmus/run_litmus.sh
eng_bg genmc     genmc/run_genmc.sh
eng_bg dartagnan dartagnan/run_dartagnan.sh
eng_bg tla       tla/run_tla.sh
eng_bg traceconf tla/run_trace_conform.sh
eng_bg alloy     alloy/run_alloy.sh
eng_bg coq       coq/run_coq.sh
eng_bg iris      iris/run_iris.sh
eng_bg rc11      iris/rc11/run_rc11.sh

echo "================ runloom formal verification ================"
if [ "$FAST" = 1 ]; then
  echo "  (worker pool: VERIFY_JOBS=$JOBS;  VERIFY_FAST=1 -- skipping 3 slow CBMC proofs)"
else
  echo "  (worker pool: VERIFY_JOBS=$JOBS)"
fi
if have spin && have cc; then
    echo "-- SPIN (exhaustive interleaving, SC memory model) --"
    new_phase spin
    launch cldeque        check_spin cldeque      "Chase-Lev deque: no lost / duplicated work-item"
    launch wake_state     check_spin wake_state   "per-g wake_state machine: no lost wake / no double-resume / no dup runq entry"
    launch parked_safe    check_spin parked_safe  "park_safe/wake_safe handshake: no lost wake, balanced"
    launch select_claim   check_spin select_claim "select fired_case CAS: fires at most one case, exactly-once wake"
    launch select_close   check_spin select_close "select Phase-2 vs send/close: no lost/NULL/spurious-sentinel result, conservation"
    launch hub_submit     check_spin hub_submit   "default M:N wake (hub_submit in_sub_queue dedup + done-check): no resume-after-done, runs once"
    launch tstate_attach_detach check_spin tstate_attach_detach "per-g tstate resume slice: attach/detach balanced -- every loop top holds the hub tstate (depth 1); resume runs only with the g tstate attached"
    launch stack_depot    check_spin stack_depot  "cross-hub stack-memory magazine: PARTITION (every mapping in exactly one of live/TLS/depot/munmap'd) + SIZE-MATCH on handout + DEPOT-BOUND (VMA cap)"
    launch pbuf_bid       check_spin pbuf_bid     "io_uring provided-buffer-ring bid ownership: PARTITION (ring+inflight==1 per bid) + NO-DUP (no double-return) + NO-LOSS (all returned at close)"
    launch blockpool      check_spin blockpool    "blocking-offload wake order: re-queue before dec inflight -> no lost wake"
    launch netpoll_commit check_spin netpoll_commit "netpoll park/wake commit (Go netpollblockcommit): no lost wake, resumed at most once"
    launch netpoll_rearm  check_spin netpoll_rearm  "netpoll register-once LEVEL arm (shipped scheme): LEVEL re-reports a still-ready fd so a late-linking parker is never edge-dropped -> no lost wake"
    launch netpoll_multipool check_spin netpoll_multipool "netpoll multi-pool dispatch: pool->sub lock hierarchy is deadlock-free, parker claimed once"
    launch netpoll_pump_kick check_spin netpoll_pump_kick "cross-hub pump-wake DEDUP (RUNLOOM_WAKE_DEDUP): coalesced kick never loses a wake (Dekker clear-then-recheck)"
    launch hub_fanout     check_spin hub_fanout   "hub_submit 3-way waker-route fanout COMPOSITION: whichever wait mode (running/idle/ring/pump) the target hub is in, some route reaches it -- no lost wake"
    launch iouring_msclose check_spin iouring_msclose "io_uring multishot handle lifetime: no use-after-free under single-owner recv/close"
    launch iouring_msclose-cc check_spin_variant iouring_msclose BUG_CONCURRENT_CLOSE "handle refcount makes a CONCURRENT close-vs-parked-recv (shared conn) memory-safe -- no UAF"
    launch netpoll_iouring_loop check_spin netpoll_iouring_loop "io_uring-as-loop backend wake/re-arm: Dekker ring_waiting handshake (no lost cross-hub wake) + multishot re-arm + at-most-once op resume"
    launch netpoll_deadline check_spin netpoll_deadline "netpoll fd-dispatch vs timeout-drain vs cancel: g resumed once with the winning claimer's value"
    launch netpoll_forceunlink check_spin netpoll_forceunlink "netpoll force_unlink vs pump: parker released exactly once, no use-after-free"
    launch cross_thread_wake check_spin cross_thread_wake "Phase C per-thread sched: wake_safe routes a woken g to its owner sched -> no lost wake"
    launch park_generic_timed check_spin park_generic_timed "fd-free timed park: timer-drain + wake_safe both CAS parked_safe -> g enqueued exactly once, no lost wake"
    launch netpoll_kqueue check_spin netpoll_kqueue "netpoll kqueue arm (BSD/macOS): EV_ADD|EV_ONESHOT re-add per park closes the not-yet-linked window -> no lost wake"
    launch netpoll_afd    check_spin netpoll_afd    "netpoll IOCP+AFD (Windows) poll-ctx lifetime: completion frees exactly once, no use-after-free, shared-IOCP wake never freed as a ctx"
    launch netpoll_parker_link check_spin netpoll_parker_link "netpoll parker link/unlink surgery at a REUSED stack address: global list + per-fd buckets stay ACYCLIC (pump walk terminates), slot-pointer trick valid, pool->total balanced"
    launch chan_buffer    check_spin chan_buffer    "buffered-channel ring + waiter FIFO: conservation (no value lost/dup), strict FIFO wake, buffer bounds [0,cap], no park-despite-ready"
    launch chan_buffer-unbuf check_spin_variant chan_buffer UNBUF "cap-0 rendezvous channel: same conservation/FIFO/bounds over the unbuffered handoff + multi-parked-sender path"
    launch foreign_thread_fallback check_spin foreign_thread_fallback "foreign-OS-thread cooperative primitive: a non-goroutine caller real-OS-blocks (never parks a NULL g / allocs a sched); mutual exclusion holds"
    launch sched_drain    check_spin sched_drain    "single-thread drain census + deadlock detector: no premature exit while runnable/wakeable work remains, deadlock declared only at genuine quiescence"
    launch wake_state-neg check_spin_must_fail wake_state  BUGGY_DROP_WAKE   "a wake dropped during RUNNING (classic lost-wakeup)"
    launch park_generic_timed-neg check_spin_must_fail park_generic_timed BUG_TIMER_NO_CAS "timer enqueues without the parked_safe CAS -> double-resume when a real wake also fires"
    launch hub_submit-neg check_spin_must_fail hub_submit  BUG_NO_DEDUP      "no in_sub_queue dedup/done-check -> resume of a freed (done) coro"
    launch tstate_attach_detach-neg check_spin_must_fail tstate_attach_detach BUG_EARLY_CONTINUE_AFTER_ATTACH "an early continue after attaching the g tstate -> the g tstate is left attached at the next loop top (the BUG-#2 cross-hub-tstate class)"
    launch stack_depot-neg check_spin_must_fail stack_depot BUG_NO_SIZE_GUARD "a pool pop ignores the size header -> a size-mismatched mapping is handed to a coro (the stack-alias/UAF surface)"
    launch stack_depot-neg2 check_spin_must_fail stack_depot BUG_NO_DEPOT_CAP  "flush skips the depot cap check -> the depot grows unbounded (the vm.max_map_count VMA-exhaustion surface)"
    launch pbuf_bid-neg check_spin_must_fail pbuf_bid BUG_DOUBLE_RETURN "pbuf_return doesn't check inflight -> a buffer is placed in the kernel ring twice (handed out twice)"
    launch pbuf_bid-neg2 check_spin_must_fail pbuf_bid BUG_LOSE_ON_CLOSE "handle close drops inflight buffers without returning them -> bids leak out of the ring"
    launch blockpool-neg check_spin_must_fail blockpool   BUG_DEC_BEFORE_REQUEUE "dec inflight before re-queue -> drain exits, goroutine stranded"
    launch netpoll_commit-neg check_spin_must_fail netpoll_commit BUG_NO_COMMIT     "no park-commit CAS -> pump's parked-check races the park -> lost wake"
    launch netpoll_rearm-neg check_spin_must_fail netpoll_rearm  BUG_EDGE_TRIGGERED "old EPOLLET register-once (no LEVEL re-report) -> a pre-link edge is dropped + never refires -> lost wake"
    launch netpoll_multipool-neg check_spin_must_fail netpoll_multipool BUG_LOCK_ORDER  "sub_lock-before-pool_lock (reverse hierarchy) -> ABBA deadlock with dispatch"
    launch netpoll_multipool-neg2 check_spin_must_fail netpoll_multipool BUG_MASK_AFTER_ARM "dispatch bitmask bit set AFTER the backend arm -> a pump skips the parker's pool -> lost wake"
    launch netpoll_pump_kick-neg check_spin_must_fail netpoll_pump_kick BUG_NO_RECHECK "pump parks WITHOUT re-checking sub_head after clearing wake_pending -> a coalesced kick is lost (hub blocks forever)"
    launch hub_fanout-neg-pump check_spin_must_fail hub_fanout BUG_NO_PUMP "drop the unconditional pump kick -> a PUMP-mode hub is stranded (the backstop route is load-bearing)"
    launch hub_fanout-neg-idle check_spin_must_fail hub_fanout BUG_NO_IDLE_SIG "never signal idle_cond -> an IDLE-mode hub is stranded (the pump kick can't reach a condvar wait)"
    launch iouring_msclose-neg check_spin_must_fail iouring_msclose BUG_NO_REFCOUNT "drop the handle refcount (old code) -> the closing CQE frees while a recv is parked -> the woken recv re-locks freed memory (use-after-free)"
    launch netpoll_iouring_loop-neg check_spin_must_fail netpoll_iouring_loop BUG_NO_FENCE      "drop the SEQ_CST Dekker fences -> StoreLoad reorder loses the cross-hub kick"
    launch netpoll_iouring_loop-neg2 check_spin_must_fail netpoll_iouring_loop BUG_NO_RECHECK    "drop the sub_head re-check -> announce/submit race loses the wake even with the fence"
    launch netpoll_iouring_loop-neg3 check_spin_must_fail netpoll_iouring_loop BUG_NO_REARM      "drop the terminal-CQE multishot re-arm -> a parked infra consumer is never woken"
    launch netpoll_iouring_loop-neg4 check_spin_must_fail netpoll_iouring_loop BUG_DOUBLE_RESUME "op drainer wakes unconditionally (not gated on prev==PARKED) -> double-resume of a fiber that never parked"
    launch netpoll_deadline-neg check_spin_must_fail netpoll_deadline BUG_SWEEP_NO_COMMIT "naive timeout sweep (no commit claim) -> spurious timeout clobbers a delivered mask / double resume"
    launch netpoll_deadline-neg2 check_spin_must_fail netpoll_deadline BUG_CANCEL_NO_COMMIT "cancel wakes without the commit claim -> clobbers a delivered value / double resume"
    launch netpoll_forceunlink-neg check_spin_must_fail netpoll_forceunlink BUG_NO_RECHECK "force_unlink trusts the stale pre-lock token -> double-free of a parker the resumed g already released"
    launch cross_thread_wake-neg check_spin_must_fail cross_thread_wake BUG_ROUTE_TO_WAKER "wake routed to the waker thread's list (pre-Phase-C) -> owner never drains -> lost wake"
    launch netpoll_kqueue-neg check_spin_must_fail netpoll_kqueue BUG_EV_CLEAR "old kqueue scheme (register-once + EV_CLEAR, edge-triggered, skip re-park kevent) -> dropped edge never refires -> lost wake"
    launch netpoll_kqueue-neg2 check_spin_must_fail netpoll_kqueue BUG_REENABLE_NOT_READD "re-arm via EV_ENABLE not EV_ADD -> ENOENT on the ONESHOT-auto-deleted knote -> nothing armed -> lost wake"
    launch netpoll_afd-neg check_spin_must_fail netpoll_afd BUG_FREE_ON_PENDING "submit frees the ctx on STATUS_PENDING while a completion is still queued -> wait consumes a freed ctx -> use-after-free"
    launch netpoll_afd-neg2 check_spin_must_fail netpoll_afd BUG_WAKE_AS_COMPLETION "wait omits the ov==NULL check -> the NULL-overlapped pump-wake is CONTAINING_RECORD'd + freed as a ctx -> wild free"
    launch netpoll_parker_link-neg check_spin_must_fail netpoll_parker_link BUG_NO_STALE_CLEAR "drop the stale self-ref clear in link -> a reused-address parker forms a self-cycle (nxt[p]==p) -> the pump's bounded list walk wedges"
    launch netpoll_parker_link-neg2 check_spin_must_fail netpoll_parker_link BUG_DOUBLE_DEC "unlink decrements pool->total unconditionally (not gated on 'touched') -> an idempotent no-op unlink underflows total while a real parker remains -> idle path mis-sleeps"
    launch chan_buffer-neg check_spin_must_fail chan_buffer BUG_LIFO_WAITERS "wake parked waiters LIFO instead of FIFO -> a later waiter is served before an earlier one (FIFO violation)"
    launch chan_buffer-neg2 check_spin_must_fail chan_buffer BUG_DROP_ON_CLOSE "close drops a buffered value instead of letting receivers drain it -> value loss (conservation violation)"
    launch foreign_thread_fallback-neg check_spin_must_fail foreign_thread_fallback BUG_FOREIGN_PARKS "route the foreign (non-goroutine) caller down the cooperative-park branch -> parks a NULL g / allocs a sched on a foreign thread (the SIGSEGV/UAF class)"
    launch sched_drain-neg check_spin_must_fail sched_drain BUG_EXIT_WITH_WORK "drain exits on empty ready ring without the wakeable-work census -> a pending foreign wake's g is stranded + deadlock declared on live work"
    launch select_close-neg check_spin_must_fail select_close BUG_CLOSE_NULL   "close-wake delivers NULL instead of closed (the SIGSEGV)"
    launch select_close-neg2 check_spin_must_fail select_close BUG_ABORT_NOCASE "abort returns the no-case sentinel for a blocking select"
    launch select_close-neg3 check_spin_must_fail select_close BUG_ABORT_DROP   "abort evicts + drops an already-delivered value"
    launch select_close-neg4 check_spin_must_fail select_close BUG_SPURIOUS     "spurious wake errors out instead of retrying"

    echo "-- SPIN liveness (acceptance-cycle detection; -f = weak fairness) --"
    # Non-starvation of the wake path REQUIRES fairness: holds under -f,
    # and (the teeth) FAILS without it -- a busy peer can starve the woken g.
    launch live_wake check_spin_live          live_wake  "-a -f"  "wake non-starvation: a woken g is eventually resumed (under weak fairness)"
    launch live_wake-neg check_spin_live_must_fail live_wake "-a"  "" "without fairness a busy peer starves the woken g forever -> fairness is load-bearing"
    # Lock-free progress of the steal path needs NO fairness: holds under -a,
    # and the blocking-design control (a preemptible lock holder) FAILS it.
    launch live_deque check_spin_live          live_deque "-a"     "deque lock-free progress: every item consumed under ANY scheduling (no fairness assumed)"
    launch live_deque-neg check_spin_live_must_fail live_deque "-a" BUG_BLOCKING "a blocking steal whose lock holder is preempted stalls all waiters -> not lock-free"
    collect
else
    echo "  (spin / cc not found -- skipping Spin models;  sudo apt-get install spin)"
    skipped=$((skipped + 1))
fi

# ---- CBMC: real cldeque.c under concurrent pthreads -------------------
# Each group is one job; its teeth run serially WITHIN the job so at most one
# cbmc per group is live -> concurrent cbmc <= number of groups (memory-safe).
cbmc_cldeque() {
    printf '  [cbmc] %-34s ' "cldeque.c"
    # Verify the real cldeque.c at a small capacity (-DRUNLOOM_CLDEQUE_CAP=4):
    # the algorithm is identical, but a 4-slot buffer keeps the SAT
    # encoding tractable.  Production default stays 4096; the stress test
    # in tests/tests_c/test_cldeque.c exercises the full-size deque.
    if cbmc "$CBMC_DIR/cldeque_cbmc.c" "$ROOT/src/runloom_c/cldeque.c" \
            -I "$CBMC_DIR/stubs" -I "$ROOT/src/runloom_c" \
            -DRUNLOOM_CLDEQUE_CAP=4 \
            >"$WORK/cbmc.log" 2>&1; then
        green "PASS"; echo " -- no loss / no duplication / no phantom under all interleavings"; return 0
    else
        red "FAIL"; echo " -- see $WORK/cbmc.log"; return 1
    fi
}

cbmc_disjoint() {
    # INV_race disjointness monitor on the same real cldeque.c (compiled with the
    # zero-cost RUNLOOM_CLDEQUE_VERIFY ghost hooks): segment-disjointness at pop's
    # fenced top-read + TAKEN-once.  Its own harness also runs the -DBUG_SELFTEST
    # negative control (teeth).  Slower (--unwind 8); fold its result in.
    printf '  [cbmc] %-34s ' "cldeque.c INV_race monitor"
    if [ -x "$CBMC_DIR/run_cldeque_disjoint.sh" ] \
            && "$CBMC_DIR/run_cldeque_disjoint.sh" >"$WORK/cbmc_disjoint.log" 2>&1; then
        green "PASS"; echo " -- INV_race: disjointness + TAKEN-once (+ teeth) on real cldeque.c"; return 0
    else
        red "FAIL"; echo " -- see $WORK/cbmc_disjoint.log"; return 1
    fi
}

cbmc_sched() {
    # runloom_sched.c single-threaded data structures: the ready FIFO ring (FIFO /
    # no-loss / no-dup across wraparound + grow) and the per-g tstate save/restore
    # (completeness + cross-g isolation), each with a negative control (teeth).
    printf '  [cbmc] %-34s ' "runloom_sched.c ready-ring + tstate"
    if [ -x "$CBMC_DIR/run_sched_cbmc.sh" ] \
            && "$CBMC_DIR/run_sched_cbmc.sh" >"$WORK/cbmc_sched.log" 2>&1; then
        green "PASS"; echo " -- ready-ring FIFO/grow + tstate save/restore (+ teeth)"; return 0
    else
        red "FAIL"; echo " -- see $WORK/cbmc_sched.log"; return 1
    fi
}

cbmc_wakestate() {
    # per-g wake_state FSM (RUNLOOM_PER_G_TSTATE global runq): totality (every
    # ENABLED event has a defined transition) + no-lost-wake (a remembered wake
    # is always enqueued, never returns to PARKED unenqueued).  Teeth: the
    # -DBUG_LOSE_WAKE config drops a remembered wake at release and MUST fail.
    printf '  [cbmc] %-34s ' "wake_state FSM (totality+no-lost-wake)"
    local ws_ok=1
    cbmc "$CBMC_DIR/wake_state_fsm_cbmc.c" --unwind 19 --unwinding-assertions \
        >"$WORK/cbmc_wakestate.log" 2>&1 || ws_ok=0
    # teeth: this MUST report VERIFICATION FAILED (cbmc exits non-zero); if it
    # passes, the harness lost its teeth -> our check fails.
    cbmc "$CBMC_DIR/wake_state_fsm_cbmc.c" --unwind 19 --unwinding-assertions \
        -DBUG_LOSE_WAKE >"$WORK/cbmc_wakestate_teeth.log" 2>&1 && ws_ok=0
    cbmc "$CBMC_DIR/wake_state_fsm_cbmc.c" --unwind 19 --unwinding-assertions \
        -DBUG_TIMER_CLAIM_DROPS >"$WORK/cbmc_wakestate_teeth2.log" 2>&1 && ws_ok=0
    if [ "$ws_ok" = 1 ]; then
        green "PASS"; echo " -- 6-state CAS FSM proven (+ teeth: BUG_LOSE_WAKE, BUG_TIMER_CLAIM_DROPS fail)"; return 0
    else
        red "FAIL"; echo " -- see $WORK/cbmc_wakestate*.log"; return 1
    fi
}

cbmc_ioclassify() {
    # I/O-return classifier FSM (runloom_io_fsm.h): totality (every (call,rc,errno)
    # yields an in-range event, never the violation abort) + mask-soundness (the
    # event is always one the call kind may emit -- the link to a consumer switch).
    # rc and errno are fully symbolic.  Teeth: BUG_SEND_EOF (a SEND yields EOF,
    # outside its mask) and BUG_OOR (out-of-range event) MUST each fail.
    printf '  [cbmc] %-34s ' "io_classify FSM (totality+mask-sound)"
    local io_ok=1
    cbmc "$CBMC_DIR/io_classify_cbmc.c" -I "$ROOT/src/runloom_c" \
        >"$WORK/cbmc_ioclassify.log" 2>&1 || io_ok=0
    cbmc "$CBMC_DIR/io_classify_cbmc.c" -I "$ROOT/src/runloom_c" \
        -DBUG_SEND_EOF >"$WORK/cbmc_ioclassify_teeth1.log" 2>&1 && io_ok=0
    cbmc "$CBMC_DIR/io_classify_cbmc.c" -I "$ROOT/src/runloom_c" \
        -DBUG_OOR >"$WORK/cbmc_ioclassify_teeth2.log" 2>&1 && io_ok=0
    if [ "$io_ok" = 1 ]; then
        green "PASS"; echo " -- classifier proven total+sound (+ teeth: BUG_SEND_EOF, BUG_OOR fail)"; return 0
    else
        red "FAIL"; echo " -- see $WORK/cbmc_ioclassify*.log"; return 1
    fi
}

cbmc_timerheap() {
    # deadline/sleep MIN-HEAP mechanics (sift + arbitrary-remove-by-heap_index):
    # heap property + peek-min + index-consistency + bounds after every op.
    printf '  [cbmc] %-34s ' "timer min-heap (sift+index+bounds)"
    local th_ok=1
    cbmc "$CBMC_DIR/timer_heap_cbmc.c" --unwind 8 --unwinding-assertions \
        >"$WORK/cbmc_timerheap.log" 2>&1 || th_ok=0
    cbmc "$CBMC_DIR/timer_heap_cbmc.c" --unwind 8 --unwinding-assertions \
        -DBUG_NO_INDEX_UPDATE >"$WORK/cbmc_timerheap_teeth.log" 2>&1 && th_ok=0
    if [ "$th_ok" = 1 ]; then
        green "PASS"; echo " -- heap-property/peek/index/bounds proven (+ teeth: BUG_NO_INDEX_UPDATE fails)"; return 0
    else
        red "FAIL"; echo " -- see $WORK/cbmc_timerheap*.log"; return 1
    fi
}

cbmc_preempt() {
    # preemption defer-during-destruction gate (the p69b weakref-UAF guard):
    # SAFETY (never yield mid object-destruction) + NO_LOST_PREEMPT (a deferred
    # preempt is taken at the first safe frame, never dropped), over arbitrary
    # (trigger, in_destruction) frame sequences.  Teeth: BUG_YIELD_IN_DEST (yield
    # anyway -> the p69b UAF) and BUG_DROP_ON_DEFER (lose the preempt) MUST fail.
    printf '  [cbmc] %-34s ' "preempt defer gate (safety+no-lost)"
    local pd_ok=1
    cbmc "$CBMC_DIR/preempt_defer_cbmc.c" \
        >"$WORK/cbmc_preempt.log" 2>&1 || pd_ok=0
    cbmc "$CBMC_DIR/preempt_defer_cbmc.c" \
        -DBUG_YIELD_IN_DEST >"$WORK/cbmc_preempt_teeth1.log" 2>&1 && pd_ok=0
    cbmc "$CBMC_DIR/preempt_defer_cbmc.c" \
        -DBUG_DROP_ON_DEFER >"$WORK/cbmc_preempt_teeth2.log" 2>&1 && pd_ok=0
    if [ "$pd_ok" = 1 ]; then
        green "PASS"; echo " -- gate proven safe+no-lost (+ teeth: BUG_YIELD_IN_DEST, BUG_DROP_ON_DEFER fail)"; return 0
    else
        red "FAIL"; echo " -- see $WORK/cbmc_preempt*.log"; return 1
    fi
}

echo "-- CBMC (bounded, on the UNMODIFIED cldeque.c source) --"
if have cbmc; then
    new_phase cbmc
    # The 3 slow proofs (cldeque ~148s, disjoint ~5-10min, timer-heap ~76s) run
    # only in the extensive lane; everything below is <=2s and always runs.
    if [ "$FAST" != 1 ]; then
        launch cbmc-cldeque    cbmc_cldeque
        launch cbmc-disjoint   cbmc_disjoint
        launch cbmc-timerheap  cbmc_timerheap
    else
        echo "  [cbmc] (fast lane: skipping cldeque + disjoint + timer-heap proofs)"
    fi
    launch cbmc-sched      cbmc_sched
    launch cbmc-wakestate  cbmc_wakestate
    launch cbmc-ioclassify cbmc_ioclassify
    launch cbmc-preempt    cbmc_preempt
    collect
else
    echo "  (cbmc not found -- skipping;  sudo apt-get install cbmc)"
    skipped=$((skipped + 1))
fi

# ---- external engines: drain the background runs (launched up top) + fold in
# Pool jobs (Spin/CBMC) were already awaited via collect()/POOL_PIDS; the only
# remaining background jobs are the external engines (herd7/GenMC/Dartagnan/
# TLA+/Alloy/Coq/Iris/iRC11), which ran CONCURRENTLY with the pool.  Wait for
# them, then fold each into the tally in declaration order.
wait
eng_finish

echo "----------------------------------------------------------"
echo "  $pass passed, $fail failed, $skipped skipped"
[ -n "$FAILED" ] && echo "  failed:$FAILED"
[ "$skipped" -gt 0 ] && echo "  ($skipped engine(s)/phase(s) skipped -- tool absent; see lines above)"
[ "$QUIET" = 0 ] && echo "  logs under: $WORK"
echo "=========================================================="
# NON-VACUOUS verdict: a real failure fails the phase, AND an all-skipped run
# (no spin/cbmc/herd7/... installed -> nothing actually verified) must NOT
# masquerade as a green.  Only a run with >=1 real PASS and 0 failures succeeds.
if [ "$fail" -ne 0 ]; then
    exit 1
elif [ "$pass" -eq 0 ]; then
    echo "  $(red "NO FORMAL CHECKS RAN") -- every engine was skipped (no verification tools found)."
    echo "  install at least one:  sudo apt-get install cbmc spin   (herd7/genmc/tlc optional)"
    exit 1
fi
exit 0
