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
# Usage: verify/run_verify.sh [-q]      (-q = less chatter)
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
SPIN_DIR="$HERE/spin"
CBMC_DIR="$HERE/cbmc"
WORK="$(mktemp -d /tmp/runloom_verify.XXXXXX)"
QUIET=0; [ "${1:-}" = "-q" ] && QUIET=1

pass=0; fail=0; FAILED=""
green() { printf '\033[32m%s\033[0m' "$1"; }
red()   { printf '\033[31m%s\033[0m' "$1"; }

note() { [ "$QUIET" = 0 ] && echo "    $*"; }

have() { command -v "$1" >/dev/null 2>&1; }

# ---- Spin: a model passes iff pan reports "errors: 0" -----------------
run_spin() {
    local name="$1" extra="${2:-}"
    local d="$WORK/$name"; mkdir -p "$d"; cp "$SPIN_DIR/$name.pml" "$d/"
    ( cd "$d" || exit 2
      spin $extra -a "$name.pml" >gen.log 2>&1 || { echo "SPINGEN_FAIL"; exit 0; }
      cc -O2 -o pan pan.c >cc.log 2>&1 || { echo "CC_FAIL"; exit 0; }
      ./pan -m500000 >run.log 2>&1
      grep -q "errors: 0" run.log && echo OK || echo BAD )
}

check_spin() {  # name, human-description
    local name="$1" desc="$2"
    printf '  [spin] %-34s ' "$name"
    local r; r="$(run_spin "$name")"
    if [ "$r" = OK ]; then green "PASS"; echo " -- $desc"; pass=$((pass+1))
    else red "FAIL"; echo " ($r) -- $desc"; fail=$((fail+1)); FAILED="$FAILED $name"; fi
}

# negative control: this model MUST fail (proves the check catches the bug)
check_spin_must_fail() {  # name, define, human-description
    local name="$1" def="$2" desc="$3"
    printf '  [spin] %-34s ' "$name(-D$def)"
    local r; r="$(run_spin "$name" "-D$def")"
    if [ "$r" = BAD ]; then green "PASS"; echo " -- correctly DETECTS: $desc"; pass=$((pass+1))
    else red "FAIL"; echo " (expected the injected bug to be caught) -- $desc"; fail=$((fail+1)); FAILED="$FAILED $name-neg"; fi
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
    if [ "$r" = OK ]; then green "PASS"; echo " -- $desc"; pass=$((pass+1))
    else red "FAIL"; echo " ($r) -- $desc"; fail=$((fail+1)); FAILED="$FAILED $name-live"; fi
}

# negative liveness control: pan MUST find an acceptance cycle here.
check_spin_live_must_fail() {  # name, panflags, define-or-empty, human-description
    local name="$1" panflags="$2" def="$3" desc="$4"
    local extra=""; [ -n "$def" ] && extra="-D$def"
    printf '  [spin] %-26s ' "$name [$panflags${def:+ -D$def}]"
    local r; r="$(run_spin_live "$name" "$panflags" "$extra")"
    if [ "$r" = BAD ]; then green "PASS"; echo " -- correctly DETECTS: $desc"; pass=$((pass+1))
    else red "FAIL"; echo " (expected a liveness violation) -- $desc"; fail=$((fail+1)); FAILED="$FAILED $name-live-neg"; fi
}

echo "================ runloom formal verification ================"
if have spin && have cc; then
    echo "-- SPIN (exhaustive interleaving, SC memory model) --"
    check_spin cldeque      "Chase-Lev deque: no lost / duplicated work-item"
    check_spin wake_state   "per-g wake_state machine: no lost wake / no double-resume / no dup runq entry"
    check_spin parked_safe  "park_safe/wake_safe handshake: no lost wake, balanced"
    check_spin select_claim "select fired_case CAS: fires at most one case, exactly-once wake"
    check_spin select_close "select Phase-2 vs send/close: no lost/NULL/spurious-sentinel result, conservation"
    check_spin hub_submit   "default M:N wake (hub_submit in_sub_queue dedup + done-check): no resume-after-done, runs once"
    check_spin tstate_attach_detach "per-g tstate resume slice: attach/detach balanced -- every loop top holds the hub tstate (depth 1); resume runs only with the g tstate attached"
    check_spin stack_depot  "cross-hub stack-memory magazine: PARTITION (every mapping in exactly one of live/TLS/depot/munmap'd) + SIZE-MATCH on handout + DEPOT-BOUND (VMA cap)"
    check_spin pbuf_bid     "io_uring provided-buffer-ring bid ownership: PARTITION (ring+inflight==1 per bid) + NO-DUP (no double-return) + NO-LOSS (all returned at close)"
    check_spin blockpool    "blocking-offload wake order: re-queue before dec inflight -> no lost wake"
    check_spin netpoll_commit "netpoll park/wake commit (Go netpollblockcommit): no lost wake, resumed at most once"
    check_spin netpoll_rearm  "netpoll LT+ONESHOT re-arm closes the not-yet-linked window: no lost wake"
    check_spin netpoll_multipool "netpoll multi-pool dispatch: pool->sub lock hierarchy is deadlock-free, parker claimed once"
    check_spin iouring_msclose "io_uring multishot handle lifetime: no use-after-free under single-owner recv/close"
    check_spin netpoll_iouring_loop "io_uring-as-loop backend wake/re-arm: Dekker ring_waiting handshake (no lost cross-hub wake) + multishot re-arm + at-most-once op resume"
    check_spin netpoll_deadline "netpoll fd-dispatch vs timeout-drain vs cancel: g resumed once with the winning claimer's value"
    check_spin netpoll_forceunlink "netpoll force_unlink vs pump: parker released exactly once, no use-after-free"
    check_spin cross_thread_wake "Phase C per-thread sched: wake_safe routes a woken g to its owner sched -> no lost wake"
    check_spin park_generic_timed "fd-free timed park: timer-drain + wake_safe both CAS parked_safe -> g enqueued exactly once, no lost wake"
    check_spin netpoll_kqueue "netpoll kqueue arm (BSD/macOS): EV_ADD|EV_ONESHOT re-add per park closes the not-yet-linked window -> no lost wake"
    check_spin netpoll_afd    "netpoll IOCP+AFD (Windows) poll-ctx lifetime: completion frees exactly once, no use-after-free, shared-IOCP wake never freed as a ctx"
    check_spin_must_fail wake_state  BUGGY_DROP_WAKE   "a wake dropped during RUNNING (classic lost-wakeup)"
    check_spin_must_fail park_generic_timed BUG_TIMER_NO_CAS "timer enqueues without the parked_safe CAS -> double-resume when a real wake also fires"
    check_spin_must_fail hub_submit  BUG_NO_DEDUP      "no in_sub_queue dedup/done-check -> resume of a freed (done) coro"
    check_spin_must_fail tstate_attach_detach BUG_EARLY_CONTINUE_AFTER_ATTACH "an early continue after attaching the g tstate -> the g tstate is left attached at the next loop top (the BUG-#2 cross-hub-tstate class)"
    check_spin_must_fail stack_depot BUG_NO_SIZE_GUARD "a pool pop ignores the size header -> a size-mismatched mapping is handed to a coro (the stack-alias/UAF surface)"
    check_spin_must_fail stack_depot BUG_NO_DEPOT_CAP  "flush skips the depot cap check -> the depot grows unbounded (the vm.max_map_count VMA-exhaustion surface)"
    check_spin_must_fail pbuf_bid BUG_DOUBLE_RETURN "pbuf_return doesn't check inflight -> a buffer is placed in the kernel ring twice (handed out twice)"
    check_spin_must_fail pbuf_bid BUG_LOSE_ON_CLOSE "handle close drops inflight buffers without returning them -> bids leak out of the ring"
    check_spin_must_fail blockpool   BUG_DEC_BEFORE_REQUEUE "dec inflight before re-queue -> drain exits, goroutine stranded"
    check_spin_must_fail netpoll_commit BUG_NO_COMMIT     "no park-commit CAS -> pump's parked-check races the park -> lost wake"
    check_spin_must_fail netpoll_rearm  BUG_EDGE_TRIGGERED "old EPOLLET register-once (no LT re-arm) -> dropped edge never refires -> lost wake"
    check_spin_must_fail netpoll_multipool BUG_LOCK_ORDER  "sub_lock-before-pool_lock (reverse hierarchy) -> ABBA deadlock with dispatch"
    check_spin_must_fail iouring_msclose BUG_CONCURRENT_CLOSE "concurrent recv+close on a shared multishot handle -> use-after-free"
    check_spin_must_fail netpoll_iouring_loop BUG_NO_FENCE      "drop the SEQ_CST Dekker fences -> StoreLoad reorder loses the cross-hub kick"
    check_spin_must_fail netpoll_iouring_loop BUG_NO_RECHECK    "drop the sub_head re-check -> announce/submit race loses the wake even with the fence"
    check_spin_must_fail netpoll_iouring_loop BUG_NO_REARM      "drop the terminal-CQE multishot re-arm -> a parked infra consumer is never woken"
    check_spin_must_fail netpoll_iouring_loop BUG_DOUBLE_RESUME "op drainer wakes unconditionally (not gated on prev==PARKED) -> double-resume of a fiber that never parked"
    check_spin_must_fail netpoll_deadline BUG_SWEEP_NO_COMMIT "naive timeout sweep (no commit claim) -> spurious timeout clobbers a delivered mask / double resume"
    check_spin_must_fail netpoll_deadline BUG_CANCEL_NO_COMMIT "cancel wakes without the commit claim -> clobbers a delivered value / double resume"
    check_spin_must_fail netpoll_forceunlink BUG_NO_RECHECK "force_unlink trusts the stale pre-lock token -> double-free of a parker the resumed g already released"
    check_spin_must_fail cross_thread_wake BUG_ROUTE_TO_WAKER "wake routed to the waker thread's list (pre-Phase-C) -> owner never drains -> lost wake"
    check_spin_must_fail netpoll_kqueue BUG_EV_CLEAR "old kqueue scheme (register-once + EV_CLEAR, edge-triggered, skip re-park kevent) -> dropped edge never refires -> lost wake"
    check_spin_must_fail netpoll_kqueue BUG_REENABLE_NOT_READD "re-arm via EV_ENABLE not EV_ADD -> ENOENT on the ONESHOT-auto-deleted knote -> nothing armed -> lost wake"
    check_spin_must_fail netpoll_afd BUG_FREE_ON_PENDING "submit frees the ctx on STATUS_PENDING while a completion is still queued -> wait consumes a freed ctx -> use-after-free"
    check_spin_must_fail netpoll_afd BUG_WAKE_AS_COMPLETION "wait omits the ov==NULL check -> the NULL-overlapped pump-wake is CONTAINING_RECORD'd + freed as a ctx -> wild free"
    check_spin_must_fail select_close BUG_CLOSE_NULL   "close-wake delivers NULL instead of closed (the SIGSEGV)"
    check_spin_must_fail select_close BUG_ABORT_NOCASE "abort returns the no-case sentinel for a blocking select"
    check_spin_must_fail select_close BUG_ABORT_DROP   "abort evicts + drops an already-delivered value"
    check_spin_must_fail select_close BUG_SPURIOUS     "spurious wake errors out instead of retrying"

    echo "-- SPIN liveness (acceptance-cycle detection; -f = weak fairness) --"
    # Non-starvation of the wake path REQUIRES fairness: holds under -f,
    # and (the teeth) FAILS without it -- a busy peer can starve the woken g.
    check_spin_live          live_wake  "-a -f"  "wake non-starvation: a woken g is eventually resumed (under weak fairness)"
    check_spin_live_must_fail live_wake "-a"  "" "without fairness a busy peer starves the woken g forever -> fairness is load-bearing"
    # Lock-free progress of the steal path needs NO fairness: holds under -a,
    # and the blocking-design control (a preemptible lock holder) FAILS it.
    check_spin_live          live_deque "-a"     "deque lock-free progress: every item consumed under ANY scheduling (no fairness assumed)"
    check_spin_live_must_fail live_deque "-a" BUG_BLOCKING "a blocking steal whose lock holder is preempted stalls all waiters -> not lock-free"
else
    echo "  (spin / cc not found -- skipping Spin models;  sudo apt-get install spin)"
fi

# ---- CBMC: real cldeque.c under concurrent pthreads -------------------
echo "-- CBMC (bounded, on the UNMODIFIED cldeque.c source) --"
if have cbmc; then
    printf '  [cbmc] %-34s ' "cldeque.c"
    # Verify the real cldeque.c at a small capacity (-DRUNLOOM_CLDEQUE_CAP=4):
    # the algorithm is identical, but a 4-slot buffer keeps the SAT
    # encoding tractable.  Production default stays 4096; the stress test
    # in tests_c/test_cldeque.c exercises the full-size deque.
    if cbmc "$CBMC_DIR/cldeque_cbmc.c" "$ROOT/src/runloom_c/cldeque.c" \
            -I "$CBMC_DIR/stubs" -I "$ROOT/src/runloom_c" \
            -DRUNLOOM_CLDEQUE_CAP=4 \
            >"$WORK/cbmc.log" 2>&1; then
        green "PASS"; echo " -- no loss / no duplication / no phantom under all interleavings"
        pass=$((pass+1))
    else
        red "FAIL"; echo " -- see $WORK/cbmc.log"; fail=$((fail+1)); FAILED="$FAILED cbmc-cldeque"
    fi
    # INV_race disjointness monitor on the same real cldeque.c (compiled with the
    # zero-cost RUNLOOM_CLDEQUE_VERIFY ghost hooks): segment-disjointness at pop's
    # fenced top-read + TAKEN-once.  Its own harness also runs the -DBUG_SELFTEST
    # negative control (teeth).  Slower (--unwind 8); fold its result in.
    printf '  [cbmc] %-34s ' "cldeque.c INV_race monitor"
    if [ -x "$CBMC_DIR/run_cldeque_disjoint.sh" ] \
            && "$CBMC_DIR/run_cldeque_disjoint.sh" >"$WORK/cbmc_disjoint.log" 2>&1; then
        green "PASS"; echo " -- INV_race: disjointness + TAKEN-once (+ teeth) on real cldeque.c"
        pass=$((pass+1))
    else
        red "FAIL"; echo " -- see $WORK/cbmc_disjoint.log"; fail=$((fail+1)); FAILED="$FAILED cbmc-disjoint"
    fi
    # runloom_sched.c single-threaded data structures: the ready FIFO ring (FIFO /
    # no-loss / no-dup across wraparound + grow) and the per-g tstate save/restore
    # (completeness + cross-g isolation), each with a negative control (teeth).
    printf '  [cbmc] %-34s ' "runloom_sched.c ready-ring + tstate"
    if [ -x "$CBMC_DIR/run_sched_cbmc.sh" ] \
            && "$CBMC_DIR/run_sched_cbmc.sh" >"$WORK/cbmc_sched.log" 2>&1; then
        green "PASS"; echo " -- ready-ring FIFO/grow + tstate save/restore (+ teeth)"
        pass=$((pass+1))
    else
        red "FAIL"; echo " -- see $WORK/cbmc_sched.log"; fail=$((fail+1)); FAILED="$FAILED cbmc-sched"
    fi
    # per-g wake_state FSM (RUNLOOM_PER_G_TSTATE global runq): totality (every
    # ENABLED event has a defined transition) + no-lost-wake (a remembered wake
    # is always enqueued, never returns to PARKED unenqueued).  Teeth: the
    # -DBUG_LOSE_WAKE config drops a remembered wake at release and MUST fail.
    printf '  [cbmc] %-34s ' "wake_state FSM (totality+no-lost-wake)"
    ws_ok=1
    cbmc "$CBMC_DIR/wake_state_fsm_cbmc.c" --unwind 19 --unwinding-assertions \
        >"$WORK/cbmc_wakestate.log" 2>&1 || ws_ok=0
    # teeth: this MUST report VERIFICATION FAILED (cbmc exits non-zero); if it
    # passes, the harness lost its teeth -> our check fails.
    cbmc "$CBMC_DIR/wake_state_fsm_cbmc.c" --unwind 19 --unwinding-assertions \
        -DBUG_LOSE_WAKE >"$WORK/cbmc_wakestate_teeth.log" 2>&1 && ws_ok=0
    cbmc "$CBMC_DIR/wake_state_fsm_cbmc.c" --unwind 19 --unwinding-assertions \
        -DBUG_TIMER_CLAIM_DROPS >"$WORK/cbmc_wakestate_teeth2.log" 2>&1 && ws_ok=0
    if [ "$ws_ok" = 1 ]; then
        green "PASS"; echo " -- 6-state CAS FSM proven (+ teeth: BUG_LOSE_WAKE, BUG_TIMER_CLAIM_DROPS fail)"
        pass=$((pass+1))
    else
        red "FAIL"; echo " -- see $WORK/cbmc_wakestate*.log"; fail=$((fail+1)); FAILED="$FAILED cbmc-wakestate"
    fi

    # I/O-return classifier FSM (runloom_io_fsm.h): totality (every (call,rc,errno)
    # yields an in-range event, never the violation abort) + mask-soundness (the
    # event is always one the call kind may emit -- the link to a consumer switch).
    # rc and errno are fully symbolic.  Teeth: BUG_SEND_EOF (a SEND yields EOF,
    # outside its mask) and BUG_OOR (out-of-range event) MUST each fail.
    printf '  [cbmc] %-34s ' "io_classify FSM (totality+mask-sound)"
    io_ok=1
    cbmc "$CBMC_DIR/io_classify_cbmc.c" -I "$ROOT/src/runloom_c" \
        >"$WORK/cbmc_ioclassify.log" 2>&1 || io_ok=0
    cbmc "$CBMC_DIR/io_classify_cbmc.c" -I "$ROOT/src/runloom_c" \
        -DBUG_SEND_EOF >"$WORK/cbmc_ioclassify_teeth1.log" 2>&1 && io_ok=0
    cbmc "$CBMC_DIR/io_classify_cbmc.c" -I "$ROOT/src/runloom_c" \
        -DBUG_OOR >"$WORK/cbmc_ioclassify_teeth2.log" 2>&1 && io_ok=0
    if [ "$io_ok" = 1 ]; then
        green "PASS"; echo " -- classifier proven total+sound (+ teeth: BUG_SEND_EOF, BUG_OOR fail)"
        pass=$((pass+1))
    else
        red "FAIL"; echo " -- see $WORK/cbmc_ioclassify*.log"; fail=$((fail+1)); FAILED="$FAILED cbmc-ioclassify"
    fi

    # preemption defer-during-destruction gate (the p69b weakref-UAF guard):
    # SAFETY (never yield mid object-destruction) + NO_LOST_PREEMPT (a deferred
    # preempt is taken at the first safe frame, never dropped), over arbitrary
    # (trigger, in_destruction) frame sequences.  Teeth: BUG_YIELD_IN_DEST (yield
    # anyway -> the p69b UAF) and BUG_DROP_ON_DEFER (lose the preempt) MUST fail.
    printf '  [cbmc] %-34s ' "preempt defer gate (safety+no-lost)"
    pd_ok=1
    cbmc "$CBMC_DIR/preempt_defer_cbmc.c" \
        >"$WORK/cbmc_preempt.log" 2>&1 || pd_ok=0
    cbmc "$CBMC_DIR/preempt_defer_cbmc.c" \
        -DBUG_YIELD_IN_DEST >"$WORK/cbmc_preempt_teeth1.log" 2>&1 && pd_ok=0
    cbmc "$CBMC_DIR/preempt_defer_cbmc.c" \
        -DBUG_DROP_ON_DEFER >"$WORK/cbmc_preempt_teeth2.log" 2>&1 && pd_ok=0
    if [ "$pd_ok" = 1 ]; then
        green "PASS"; echo " -- gate proven safe+no-lost (+ teeth: BUG_YIELD_IN_DEST, BUG_DROP_ON_DEFER fail)"
        pass=$((pass+1))
    else
        red "FAIL"; echo " -- see $WORK/cbmc_preempt*.log"; fail=$((fail+1)); FAILED="$FAILED cbmc-preempt"
    fi
else
    echo "  (cbmc not found -- skipping;  sudo apt-get install cbmc)"
fi

# ---- herd7: C11/RC11 weak-memory litmus (fence placement) -------------
# Spin is sequentially consistent; these probe the actual memory_order on the
# netpoll commit / wake-list paths.  run_litmus.sh skips cleanly if herd7 is
# absent, and prints its own pass/fail line; fold its result into the total.
if [ -x "$HERE/litmus/run_litmus.sh" ]; then
    if lit_out="$("$HERE/litmus/run_litmus.sh" 2>&1)"; then
        echo "$lit_out"
        # count the litmus passes into the suite total when herd7 ran
        if echo "$lit_out" | grep -q "passed, 0 failed"; then
            lp="$(echo "$lit_out" | sed -n 's/.* \([0-9]*\) passed, 0 failed/\1/p' | tail -1)"
            [ -n "$lp" ] && pass=$((pass+lp))
        fi
    else
        echo "$lit_out"
        fail=$((fail+1)); FAILED="$FAILED litmus"
    fi
fi

# ---- GenMC: real C (pthreads + atomics) under RC11 --------------------
# Explores every RC11 execution of the actual netpoll claim protocol, so it
# catches a data race / misplaced fence, not just a bad interleaving.  Skips
# cleanly if genmc is absent.
if [ -x "$HERE/genmc/run_genmc.sh" ]; then
    if gmc_out="$("$HERE/genmc/run_genmc.sh" 2>&1)"; then
        echo "$gmc_out"
        if echo "$gmc_out" | grep -q "passed, 0 failed"; then
            gp="$(echo "$gmc_out" | sed -n 's/.* \([0-9]*\) passed, 0 failed/\1/p' | tail -1)"
            [ -n "$gp" ] && pass=$((pass+gp))
        fi
    else
        echo "$gmc_out"
        fail=$((fail+1)); FAILED="$FAILED genmc"
    fi
fi

# ---- Dartagnan: SMT bounded encoding of the SAME litmus tests under a .cat --
# A third, independent engine on the fence-placement questions: encodes bounded
# executions + the RC11 memory model as one SMT formula (CAV'19).  Reuses the
# herd7 litmus corpus; agreement across herd7 + GenMC + Dartagnan is strong.
# Skips cleanly if Dartagnan/cat absent.
if [ -x "$HERE/dartagnan/run_dartagnan.sh" ]; then
    if dat_out="$("$HERE/dartagnan/run_dartagnan.sh" 2>&1)"; then
        echo "$dat_out"
        if echo "$dat_out" | grep -q "passed, 0 failed"; then
            dp="$(echo "$dat_out" | sed -n 's/.* \([0-9]*\) passed, 0 failed/\1/p' | tail -1)"
            [ -n "$dp" ] && pass=$((pass+dp))
        fi
    else
        echo "$dat_out"
        fail=$((fail+1)); FAILED="$FAILED dartagnan"
    fi
fi

# ---- TLC: composed-scheduler TLA+ spec (emergent end-to-end properties) ----
# Spin verifies each primitive separately; this checks their COMPOSITION
# (multi-hub dispatch + wake/park one-shot race) for no-lost-goroutine.
# Skips cleanly if java/jar absent; prints its own pass/fail line.
if [ -x "$HERE/tla/run_tla.sh" ]; then
    if tla_out="$("$HERE/tla/run_tla.sh" 2>&1)"; then
        echo "$tla_out"
        if echo "$tla_out" | grep -q "passed, 0 failed"; then
            tp="$(echo "$tla_out" | sed -n 's/.* \([0-9]*\) passed, 0 failed/\1/p' | tail -1)"
            [ -n "$tp" ] && pass=$((pass+tp))
        fi
    else
        echo "$tla_out"; fail=$((fail+1)); FAILED="$FAILED tla"
    fi
fi

# ---- Alloy: structural invariant of the netpoll parker graph -------------
# Formalizes what runloom_self_check walks at runtime (no list cycle, every
# bucket entry on the global list).  Skips cleanly if java/jar absent.
if [ -x "$HERE/alloy/run_alloy.sh" ]; then
    if al_out="$("$HERE/alloy/run_alloy.sh" 2>&1)"; then
        echo "$al_out"
        if echo "$al_out" | grep -q "passed, 0 failed"; then
            ap="$(echo "$al_out" | sed -n 's/.* \([0-9]*\) passed, 0 failed/\1/p' | tail -1)"
            [ -n "$ap" ] && pass=$((pass+ap))
        fi
    else
        echo "$al_out"; fail=$((fail+1)); FAILED="$FAILED alloy"
    fi
fi

# ---- Coq: machine-checked, UNBOUNDED protocol invariants -----------------
# Spin/CBMC are bounded; this proves the wake_state safety invariants over
# every reachable state (any number of transitions).  Skips if coqc absent.
if [ -x "$HERE/coq/run_coq.sh" ]; then
    if cq_out="$("$HERE/coq/run_coq.sh" 2>&1)"; then
        echo "$cq_out"
        if echo "$cq_out" | grep -q "passed, 0 failed"; then
            cp2="$(echo "$cq_out" | sed -n 's/.* \([0-9]*\) passed, 0 failed/\1/p' | tail -1)"
            [ -n "$cp2" ] && pass=$((pass+cp2))
        fi
    else
        echo "$cq_out"; fail=$((fail+1)); FAILED="$FAILED coq"
    fi
fi

# ---- Iris: concurrent separation logic on running HeapLang programs -------
# The deepest tier: proves a real concurrent program (CmpXchg races, parallel
# composition), thread-modular.  Skips cleanly if coqc/Iris absent.
if [ -x "$HERE/iris/run_iris.sh" ]; then
    if ir_out="$("$HERE/iris/run_iris.sh" 2>&1)"; then
        echo "$ir_out"
        if echo "$ir_out" | grep -q "passed, 0 failed"; then
            ip="$(echo "$ir_out" | sed -n 's/.* \([0-9]*\) passed, 0 failed/\1/p' | tail -1)"
            [ -n "$ip" ] && pass=$((pass+ip))
        fi
    else
        echo "$ir_out"; fail=$((fail+1)); FAILED="$FAILED iris"
    fi
fi

# ---- iRC11 / RC11 weak-memory separation logic (gpfsl) -------------------
# The genuine weak-memory tier: a running concurrent program proved under RC11.
# Needs the gpfsl opam switch; skips cleanly otherwise (see WEAK_MEMORY.md).
if [ -x "$HERE/iris/rc11/run_rc11.sh" ]; then
    if rc_out="$("$HERE/iris/rc11/run_rc11.sh" 2>&1)"; then
        echo "$rc_out"
        if echo "$rc_out" | grep -q "passed, 0 failed"; then
            rp="$(echo "$rc_out" | sed -n 's/.* \([0-9]*\) passed, 0 failed/\1/p' | tail -1)"
            [ -n "$rp" ] && pass=$((pass+rp))
        fi
    else
        echo "$rc_out"; fail=$((fail+1)); FAILED="$FAILED rc11"
    fi
fi

echo "----------------------------------------------------------"
echo "  $pass passed, $fail failed"
[ -n "$FAILED" ] && echo "  failed:$FAILED"
[ "$QUIET" = 0 ] && echo "  logs under: $WORK"
echo "=========================================================="
[ "$fail" -eq 0 ]
