#!/usr/bin/env bash
# run_tla.sh -- TLC model-check the composed-scheduler TLA+ spec.
#
# Checks the correct protocol (all invariants + the AllComplete liveness
# property hold) and the negative control (Buggy=TRUE drops the pending-wake
# check -> AllComplete MUST be violated by a lost-wake lasso).  Prints a
# "N passed, M failed" line so run_verify.sh can fold it into the suite total.
#
# Needs java; fetches tla2tools.jar on first run (cached next to this script).
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
JAR="${TLA_JAR:-$HERE/tla2tools.jar}"
URL="https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar"

echo "-- TLA+ (TLC: composed M:N scheduler, wake/park race) --"
if ! command -v java >/dev/null 2>&1; then
    echo "  (java not found -- skipping TLA+;  apt-get install default-jre)"
    exit 0
fi
if [ ! -f "$JAR" ]; then
    curl -fsSL -o "$JAR" "$URL" 2>/dev/null || {
        echo "  (could not fetch tla2tools.jar -- skipping TLA+)"; exit 0; }
fi

pass=0; fail=0
META="$(mktemp -d /tmp/runloom_tlc.XXXX)"
run_tlc() { ( cd "$HERE" && java -cp "$JAR" tlc2.TLC -metadir "$META/$1" "${@:2}" 2>&1 ); }

printf '  [tlc] %-28s ' "RunloomSched (correct)"
if run_tlc ok -config RunloomSched.cfg RunloomSched.tla | grep -q "No error has been found"; then
    echo "PASS -- TypeOK/NoDoubleRun/DoneIsTerminal + AllComplete (liveness)"; pass=$((pass+1))
else
    echo "FAIL -- correct spec should hold"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomSched (Buggy=TRUE)"
if run_tlc bug -deadlock -config RunloomSched_bug.cfg RunloomSched.tla | grep -q "Temporal properties were violated"; then
    echo "PASS -- correctly DETECTS lost wakeup -> AllComplete violated"; pass=$((pass+1))
else
    echo "FAIL -- the injected lost-wake bug should violate AllComplete"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomHandoff (rescue)"
if run_tlc hook -config RunloomHandoff.cfg RunloomHandoff.tla | grep -q "No error has been found"; then
    echo "PASS -- TypeOK/NoConcurrentDrain + AllDrained (stall-recovery liveness)"; pass=$((pass+1))
else
    echo "FAIL -- correct handoff spec should hold"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomHandoff (no rescue)"
if run_tlc hobug -deadlock -config RunloomHandoff_bug.cfg RunloomHandoff.tla | grep -q "Temporal properties were violated"; then
    echo "PASS -- correctly DETECTS stranded work without the rescue M -> AllDrained violated"; pass=$((pass+1))
else
    echo "FAIL -- removing the rescue should strand a wedged hub's work"; fail=$((fail+1))
fi

# ---- Controlled M:N scheduler (RUNLOOM_MN_SEED experiment): the baton +
# rendezvous protocol.  Correct = mutual-exclusion + deadlock-free +
# deterministic grant.  Two negative controls model the two real obstacles:
# no preemption -> a CPU-bound hub starves all (the deadlock fixed by keeping
# preemption on); no barrier -> a grant over a partial requester set (the
# residual nondeterminism the rendezvous removes).
printf '  [tlc] %-28s ' "RunloomMNControl (correct)"
if run_tlc mnok -config RunloomMNControl.cfg RunloomMNControl.tla | grep -q "No error has been found"; then
    echo "PASS -- MutualExclusion/BatonConsistent/DeterministicGrant + AllRun (no deadlock)"; pass=$((pass+1))
else
    echo "FAIL -- controlled-baton+rendezvous spec should hold"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomMNControl (Preempt=FALSE)"
if run_tlc mnnp -config RunloomMNControl_nopreempt.cfg RunloomMNControl.tla | grep -q "Temporal properties were violated"; then
    echo "PASS -- correctly DETECTS the baton deadlock (CPU-bound hub starves all) without preemption"; pass=$((pass+1))
else
    echo "FAIL -- no preemption should violate AllRun (liveness)"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomMNControl (Barrier=FALSE)"
if run_tlc mnnb -config RunloomMNControl_nobarrier.cfg RunloomMNControl.tla | grep -q "is violated"; then
    echo "PASS -- correctly DETECTS a grant over a partial requester set (nondeterminism) without the rendezvous"; pass=$((pass+1))
else
    echo "FAIL -- no barrier should violate DeterministicGrant"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomMNControl (timers+clock)"
if run_tlc mntm -config RunloomMNControl_timer.cfg RunloomMNControl.tla | grep -q "No error has been found"; then
    echo "PASS -- logical clock: DeterministicTick + MutualExclusion + AllRun hold with timer waits"; pass=$((pass+1))
else
    echo "FAIL -- timers + logical clock spec should hold"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomMNControl (LogicalClock=F)"
if run_tlc mnlc -config RunloomMNControl_nologicalclock.cfg RunloomMNControl.tla | grep -q "is violated"; then
    echo "PASS -- correctly DETECTS a later timer firing before an earlier deadline (nondeterminism) without the logical clock"; pass=$((pass+1))
else
    echo "FAIL -- no logical clock should violate DeterministicTick"; fail=$((fail+1))
fi

# ---- CPython STW boundary (RunloomCPythonSTW): the contract between runloom's
# hubs and free-threaded CPython's stop-the-world machinery (M1 attach/detach +
# M2 stop_the_world, read from Python/pystate.c -- see docs/dev/cpython_boundary.md).
# Correct = STWExclusive (no non-requester hub is ATTACHED while the world is
# stopped).  Negative control models bug 2 / contract C3: re-attaching a SUSPENDED
# tstate without the wait_attach gate -> a hub attached during a stopped world.
printf '  [tlc] %-28s ' "RunloomCPythonSTW (correct)"
if run_tlc stwok -config RunloomCPythonSTW.cfg RunloomCPythonSTW.tla | grep -q "No error has been found"; then
    echo "PASS -- STWExclusive + RequesterAttached hold (STW reclaims with all others suspended)"; pass=$((pass+1))
else
    echo "FAIL -- correct STW-boundary spec should hold"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomCPythonSTW (Bypass=T)"
if run_tlc stwbug -deadlock -config RunloomCPythonSTW_bug.cfg RunloomCPythonSTW.tla | grep -q "is violated"; then
    echo "PASS -- correctly DETECTS the handoff re-attach (a hub ATTACHED while the world is stopped) -> STWExclusive violated"; pass=$((pass+1))
else
    echo "FAIL -- re-attaching a suspended tstate mid-STW should violate STWExclusive"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomCPythonSTW (liveness)"
if run_tlc stwlive -config RunloomCPythonSTW_live.cfg RunloomCPythonSTW.tla | grep -q "No error has been found"; then
    echo "PASS -- STWCompletes: every requested stop-the-world eventually completes"; pass=$((pass+1))
else
    echo "FAIL -- a correctly-detaching system should always complete STW"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomCPythonSTW (BlockAttach)"
if run_tlc stwlb -deadlock -config RunloomCPythonSTW_livebug.cfg RunloomCPythonSTW.tla | grep -q "Temporal properties were violated"; then
    echo "PASS -- correctly DETECTS the STW-monopoly hang (a hub blocks while attached) -> STWCompletes violated"; pass=$((pass+1))
else
    echo "FAIL -- a hub blocked-while-attached should wedge stop-the-world"; fail=$((fail+1))
fi

# ---- M4: the GILState-TSS binding (RunloomGilstate): the teardown contract C6,
# the bug the --with-pydebug oracle found (pystate.c:345).  Correct = each hub
# deletes its own tstate on its own thread.  Negative control deletes hub tstates
# from the main thread -> the assert fires + the binding is corrupted.
printf '  [tlc] %-28s ' "RunloomGilstate (correct)"
if run_tlc gilok -config RunloomGilstate.cfg RunloomGilstate.tla | grep -q "No error has been found"; then
    echo "PASS -- GilstateContract + GilBindingConsistent hold (hub deletes its own tstate on its own thread)"; pass=$((pass+1))
else
    echo "FAIL -- correct gilstate teardown should hold"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomGilstate (wrong thread)"
if run_tlc gilbug -deadlock -config RunloomGilstate_bug.cfg RunloomGilstate.tla | grep -q "is violated"; then
    echo "PASS -- correctly DETECTS the pystate.c:345 abort (deleting a hub tstate from the main thread) -> GilstateContract violated"; pass=$((pass+1))
else
    echo "FAIL -- deleting a gilstate-bound tstate from the wrong thread should violate the contract"; fail=$((fail+1))
fi

# ---- Tier-1 #2: the per-g tstate / mimalloc-heap MIGRATION hazard
# (RunloomTstateMigration): models mimalloc's per-PAGE owner thread and proves
# the abandon-on-detach + adopt-on-attach handshake is NECESSARY to migrate a
# tstate hub->hub without a cross-thread page op (the SEGV gated off in 70e6ddb).
# Correct = the handshake keeps owner == operating thread; the negative control
# drops it -> a page allocated on hub A is operated on hub B.
printf '  [tlc] %-28s ' "RunloomTstateMigration (handshake)"
if run_tlc mig -config RunloomTstateMigration.cfg RunloomTstateMigration.tla | grep -q "No error has been found"; then
    echo "PASS -- NoCrossThreadPageOp + NoForeignOwnerWhileAttached hold (abandon/adopt keeps page owner == operating hub)"; pass=$((pass+1))
else
    echo "FAIL -- the abandon/adopt handshake spec should hold"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomTstateMigration (no h/shake)"
if run_tlc migbug -deadlock -config RunloomTstateMigration_bug.cfg RunloomTstateMigration.tla | grep -q "is violated"; then
    echo "PASS -- correctly DETECTS the mimalloc heap migrating hub->hub (a page owned by hub A operated on hub B) -> NoForeignOwnerWhileAttached violated"; pass=$((pass+1))
else
    echo "FAIL -- migrating a tstate without the abandon/adopt handshake should violate page ownership"; fail=$((fail+1))
fi

# ---- Tier-2 #6: the runloom_g_t REFCOUNT LEDGER composed with the wake_state
# machine (RunloomGRefcount).  wake_state.pml proves the entry/owner discipline;
# this proves the integer refcount stays consistent with it (rc == scheduler ref +
# the 0/1 global-runq queue ref), so a g is freed exactly once and never while a
# queue entry could still resume it.  Negative control drops the QUEUED->RUNNING
# decref (a consumed queue ref leaked).
printf '  [tlc] %-28s ' "RunloomGRefcount (correct)"
if run_tlc grcok -config RunloomGRefcount.cfg RunloomGRefcount.tla | grep -q "No error has been found"; then
    echo "PASS -- Ledger + RcNonNeg + FreedConsistent hold (refcount tracks the wake_state)"; pass=$((pass+1))
else
    echo "FAIL -- the refcount-ledger spec should hold"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomGRefcount (lost decref)"
if run_tlc grcbug -deadlock -config RunloomGRefcount_bug.cfg RunloomGRefcount.tla | grep -q "is violated"; then
    echo "PASS -- correctly DETECTS a consumed global-runq entry that forgets runloom_g_decref -> the queue ref leaks (Ledger violated, g never freed)"; pass=$((pass+1))
else
    echo "FAIL -- a lost queue-ref decref should violate the refcount ledger"; fail=$((fail+1))
fi

# ---- WHOLE-PROGRAM LIVENESS (RunloomComposite): the scheduler + every wake
# source (channels / netpoll / timers / foreign) composed, checked for NoHang
# (every goroutine eventually completes).  Real hangs live in the seams between
# subsystems; the two negative controls are the two ways the shared route-to-home
# + don't-idle-past-a-wake machinery breaks.
printf '  [tlc] %-28s ' "RunloomComposite (correct)"
if run_tlc compok -config RunloomComposite.cfg RunloomComposite.tla | grep -q "No error has been found"; then
    echo "PASS -- NoHang holds: every g completes across the channel + external (fd/timer/foreign) seams"; pass=$((pass+1))
else
    echo "FAIL -- the composed scheduler should be hang-free"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomComposite (Quiesce)"
if run_tlc compq -deadlock -config RunloomComposite_quiesce.cfg RunloomComposite.tla | grep -q "Temporal properties were violated"; then
    echo "PASS -- correctly DETECTS the census-idle wake-guard hang (a hub idles past a wake) -> NoHang violated"; pass=$((pass+1))
else
    echo "FAIL -- removing the wake-guard should hang"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomComposite (Route)"
if run_tlc compr -deadlock -config RunloomComposite_route.cfg RunloomComposite.tla | grep -q "Temporal properties were violated"; then
    echo "PASS -- correctly DETECTS the wake-misrouting hang (external wake to the wrong hub) -> NoHang violated"; pass=$((pass+1))
else
    echo "FAIL -- misrouting an external wake should hang"; fail=$((fail+1))
fi

"$(command -v safe-rm || echo rm)" -rf "$META"
echo "  $pass passed, $fail failed"
[ "$fail" -eq 0 ]
