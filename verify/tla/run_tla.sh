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
META="$(mktemp -d /tmp/pygo_tlc.XXXX)"
run_tlc() { ( cd "$HERE" && java -cp "$JAR" tlc2.TLC -metadir "$META/$1" "${@:2}" 2>&1 ); }

printf '  [tlc] %-28s ' "PygoSched (correct)"
if run_tlc ok -config PygoSched.cfg PygoSched.tla | grep -q "No error has been found"; then
    echo "PASS -- TypeOK/NoDoubleRun/DoneIsTerminal + AllComplete (liveness)"; pass=$((pass+1))
else
    echo "FAIL -- correct spec should hold"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "PygoSched (Buggy=TRUE)"
if run_tlc bug -deadlock -config PygoSched_bug.cfg PygoSched.tla | grep -q "Temporal properties were violated"; then
    echo "PASS -- correctly DETECTS lost wakeup -> AllComplete violated"; pass=$((pass+1))
else
    echo "FAIL -- the injected lost-wake bug should violate AllComplete"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "PygoHandoff (rescue)"
if run_tlc hook -config PygoHandoff.cfg PygoHandoff.tla | grep -q "No error has been found"; then
    echo "PASS -- TypeOK/NoConcurrentDrain + AllDrained (stall-recovery liveness)"; pass=$((pass+1))
else
    echo "FAIL -- correct handoff spec should hold"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "PygoHandoff (no rescue)"
if run_tlc hobug -deadlock -config PygoHandoff_bug.cfg PygoHandoff.tla | grep -q "Temporal properties were violated"; then
    echo "PASS -- correctly DETECTS stranded work without the rescue M -> AllDrained violated"; pass=$((pass+1))
else
    echo "FAIL -- removing the rescue should strand a wedged hub's work"; fail=$((fail+1))
fi

# ---- Controlled M:N scheduler (PYGO_MN_SEED experiment): the baton +
# rendezvous protocol.  Correct = mutual-exclusion + deadlock-free +
# deterministic grant.  Two negative controls model the two real obstacles:
# no preemption -> a CPU-bound hub starves all (the deadlock fixed by keeping
# preemption on); no barrier -> a grant over a partial requester set (the
# residual nondeterminism the rendezvous removes).
printf '  [tlc] %-28s ' "PygoMNControl (correct)"
if run_tlc mnok -config PygoMNControl.cfg PygoMNControl.tla | grep -q "No error has been found"; then
    echo "PASS -- MutualExclusion/BatonConsistent/DeterministicGrant + AllRun (no deadlock)"; pass=$((pass+1))
else
    echo "FAIL -- controlled-baton+rendezvous spec should hold"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "PygoMNControl (Preempt=FALSE)"
if run_tlc mnnp -config PygoMNControl_nopreempt.cfg PygoMNControl.tla | grep -q "Temporal properties were violated"; then
    echo "PASS -- correctly DETECTS the baton deadlock (CPU-bound hub starves all) without preemption"; pass=$((pass+1))
else
    echo "FAIL -- no preemption should violate AllRun (liveness)"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "PygoMNControl (Barrier=FALSE)"
if run_tlc mnnb -config PygoMNControl_nobarrier.cfg PygoMNControl.tla | grep -q "is violated"; then
    echo "PASS -- correctly DETECTS a grant over a partial requester set (nondeterminism) without the rendezvous"; pass=$((pass+1))
else
    echo "FAIL -- no barrier should violate DeterministicGrant"; fail=$((fail+1))
fi

"$(command -v safe-rm || echo rm)" -rf "$META"
echo "  $pass passed, $fail failed"
[ "$fail" -eq 0 ]
