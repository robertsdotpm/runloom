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

"$(command -v safe-rm || echo rm)" -rf "$META"
echo "  $pass passed, $fail failed"
[ "$fail" -eq 0 ]
