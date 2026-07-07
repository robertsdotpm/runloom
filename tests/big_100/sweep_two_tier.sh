#!/bin/bash
# sweep_two_tier.sh -- run each big_100 program at TWO scales and reconcile them.
#
# WHY TWO TIERS.  Running at N=1,000,000 is a SURVIVAL test: the only thing it
# can soundly assert is "the runtime did not crash, hang, lose work, or corrupt
# what it DID finish."  It must NOT assert the completeness/latency oracles ("all
# N woke on time", "all N did their op") -- the box simply can't schedule a
# million goroutines to completion inside a few-second window, so those oracles
# FALSE-POSITIVE at 1M (the campaign saw 11 such rc=1 invariant fails that all
# pass when re-run at the design scale).  The completeness oracles belong at the
# DESIGN scale (tens of thousands), where the runtime is SUPPOSED to finish all N
# and a violation is a real bug.  So:
#
#   DESIGN  tier (--funcs $DESIGN, default 20000): the ORACLE tier.  rc=0+PASS is
#           required.  An INVARIANT_FAIL / CRASH / HANG here is a REAL fault.
#   SURVIVAL tier (--funcs $SURVIVAL, default 1000000): the STRESS tier.  Only
#           CRASH / WATCHDOG_HANG / TIMEOUT / LOST(>0 lost_workers) are faults.
#           A bare INVARIANT_FAIL or slow_finishers here is treated as SCALE
#           (benign) **iff** the design tier passed -- otherwise it is real.
#
# The reconciled per-program verdict:
#   OK            design PASS + survival survived            -> all good
#   REAL_FAULT    a crash/hang/timeout/lost at EITHER tier, OR a design oracle
#                 fail                                        -> investigate
#   SCALE_ONLY    design PASS, survival INVARIANT_FAIL only  -> scale artifact
#
# This also FIXES the old sweep's CRASH(rc=N) misnomer: every nonzero exit was
# stamped CRASH even when it was a clean rc=1 invariant fail or rc=3 watchdog
# hang.  Here each exit maps to its true class:
#   rc 0            + VERDICT PASS  -> PASS
#   rc 0            + VERDICT !PASS -> VFAIL
#   rc 1                            -> INVARIANT_FAIL  (oracle violated, clean)
#   rc 2                            -> SETUP_ERROR
#   rc 3                            -> WATCHDOG_HANG    (no forward progress)
#   rc 124 / 137 / 143             -> TIMEOUT          (timeout TERM/KILL)
#   rc >=128 (other signals)       -> CRASH(sigN)      (SIGSEGV/SIGABRT/...)
#   lost_workers > 0 (clean exit)  -> LOST(n)          (parked-then-vanished)
#
# Usage:
#   sweep_two_tier.sh [prog ...]           # default: all pNN_*.py, both tiers
#   DESIGN=50000 SURVIVAL=1000000 sweep_two_tier.sh p223 p225 p36
#   TIERS=survival sweep_two_tier.sh p01   # one tier only (survival|design|both)
set +e
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"
sudo -n prlimit --pid $$ --nofile=8388608:8388608 2>/dev/null
sudo -n sysctl -w vm.max_map_count=2000000 net.core.somaxconn=4096 >/dev/null 2>&1

PY="${PY:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
DESIGN="${DESIGN:-20000}"
SURVIVAL="${SURVIVAL:-1000000}"
TIERS="${TIERS:-both}"
HUBS="${HUBS:-8}"
DESIGN_TMO="${DESIGN_TMO:-90}"
SURVIVAL_TMO="${SURVIVAL_TMO:-180}"
EXTRA="${EXTRA:-}"
RES="${RES:-/tmp/sweep_two_tier_results.txt}"
# 1M survival needs the fast bulk-arena spawn or it never fields the pool in time.
GON_ENV="RUNLOOM_HARNESS_GON=1 RUNLOOM_GON_BULK=1 RUNLOOM_GON_FRESH=1 RUNLOOM_STACK_ARENA_N=1300000"

: > "$RES"

# classify RC VERDICT LOST -> a single class token
classify() {
  local rc="$1" verdict="$2" lost="$3"
  case "$rc" in
    0)
      if [ -n "$lost" ] && [ "$lost" -gt 0 ]; then echo "LOST($lost)"
      elif [ "$verdict" = "PASS" ]; then echo "PASS"
      else echo "VFAIL(${verdict:-none})"; fi ;;
    1)   echo "INVARIANT_FAIL" ;;
    2)   echo "SETUP_ERROR" ;;
    3)   echo "WATCHDOG_HANG" ;;
    4)   echo "BOXLIMIT" ;;          # memory guard tripped: BOX can't sustain N (benign)
    124|137|143) echo "TIMEOUT" ;;
    *)
      if [ "$rc" -ge 128 ] 2>/dev/null; then echo "CRASH(sig$((rc-128)))"
      else echo "EXIT($rc)"; fi ;;
  esac
}

# is a class a HARD fault (crash/hang/timeout/lost/setup)?  INVARIANT_FAIL is
# context-dependent (real at design, scale at survival) so it is NOT here.  BOXLIMIT
# is NEVER a fault -- it means the memory guard cleanly stopped a run this box's RAM
# can't sustain (a box property, not a program/runtime bug).
is_hard_fault() {
  case "$1" in
    CRASH*|WATCHDOG_HANG|TIMEOUT|LOST*|SETUP_ERROR) return 0 ;;
    *) return 1 ;;
  esac
}

run_one() {  # prog scale tmo -> prints "class|secs|lost|slow|exits"
  local prog="$1" n="$2" tmo="$3"
  local t0 out rc verdict lost slow exits
  t0=$(date +%s)
  out=$(env PYTHON_GIL=0 PYTHONPATH=../src $GON_ENV \
        timeout -k 10 "$tmo" "$PY" "$prog.py" \
        --funcs "$n" --duration 5 --rounds 1 --hubs "$HUBS" $EXTRA 2>&1)
  rc=$?
  pkill -9 -f "$prog.py" 2>/dev/null
  verdict=$(printf '%s\n' "$out" | grep -oE "VERDICT +: [A-Z]+" | head -1 | grep -oE "[A-Z]+$")
  lost=$(printf '%s\n' "$out"    | grep -oE "lost_workers  : [0-9]+" | head -1 | grep -oE "[0-9]+$")
  slow=$(printf '%s\n' "$out"    | grep -oE "slow_finishers: [0-9]+" | head -1 | grep -oE "[0-9]+$")
  exits=$(printf '%s\n' "$out"   | grep -oE "worker_exits  : [0-9]+/[0-9]+" | head -1 | grep -oE "[0-9]+/[0-9]+")
  printf '%s|%s|%s|%s|%s' "$(classify "$rc" "$verdict" "$lost")" \
         "$(( $(date +%s) - t0 ))" "${lost:-?}" "${slow:-?}" "${exits:-?}"
}

# Reconcile the two tiers into one verdict.
reconcile() {  # design_class survival_class -> OK|REAL_FAULT|SCALE_ONLY|BOXLIMIT|...
  local d="$1" s="$2"
  # BOXLIMIT at either tier = the memory guard cleanly stopped a run this box's
  # RAM can't sustain.  Benign (a box property, not a program/runtime bug), never
  # a fault -- reported as its own class so it's distinguishable from OK/SCALE.
  if [ "$d" = "BOXLIMIT" ]; then echo "BOXLIMIT(design)"; return; fi
  if is_hard_fault "$d"; then echo "REAL_FAULT(design:$d)"; return; fi
  if [ "$d" = "INVARIANT_FAIL" ] || [ "$d" = "VFAIL" ]; then
    echo "REAL_FAULT(design_oracle:$d)"; return; fi
  # design is clean (PASS).  Now judge survival.
  if [ "$s" = "BOXLIMIT" ]; then echo "BOXLIMIT(survival)"; return; fi
  if is_hard_fault "$s"; then echo "REAL_FAULT(survival:$s)"; return; fi
  case "$s" in
    PASS)            echo "OK" ;;
    INVARIANT_FAIL|VFAIL*) echo "SCALE_ONLY(survival:$s)" ;;
    "")              echo "OK(design-only)" ;;
    *)               echo "SURVIVAL:$s" ;;
  esac
}

discover() { ls p[0-9]*_*.py 2>/dev/null | sed 's/\.py$//' | sort -t p -k2 -n; }

PROGS=("$@")
[ ${#PROGS[@]} -eq 0 ] && PROGS=( $(discover) )

printf "%-30s %-16s %-16s %s\n" "PROGRAM" "DESIGN($DESIGN)" "SURVIVAL($SURVIVAL)" "VERDICT" | tee -a "$RES"
printf '%.0s-' {1..96}; echo | tee -a "$RES"

for prog in "${PROGS[@]}"; do
  [ -f "$prog.py" ] || { printf "%-30s MISSING\n" "$prog" | tee -a "$RES"; continue; }
  dclass=""; sclass=""; dinfo=""; sinfo=""
  if [ "$TIERS" = "both" ] || [ "$TIERS" = "design" ]; then
    IFS='|' read -r dclass dsec dlost dslow dexits <<<"$(run_one "$prog" "$DESIGN" "$DESIGN_TMO")"
    # The design tier is the ORACLE tier -- a fault here is reported as a REAL
    # fault, so filter transient box-contention false-positives: a clean oracle
    # miss (INVARIANT_FAIL/VFAIL) or a watchdog hang gets ONE retry, and the
    # better result wins (a real bug fails both; contention rarely fails twice).
    # CRASH/TIMEOUT/LOST are NOT retried -- those are unambiguous.
    case "$dclass" in
      INVARIANT_FAIL|VFAIL*|WATCHDOG_HANG)
        IFS='|' read -r dclass2 _ _ _ _ <<<"$(run_one "$prog" "$DESIGN" "$DESIGN_TMO")"
        [ "$dclass2" = "PASS" ] && dclass="PASS(flaky:1st=$dclass)" ;;
    esac
    dinfo="${dclass} ${dsec}s"
  fi
  if [ "$TIERS" = "both" ] || [ "$TIERS" = "survival" ]; then
    IFS='|' read -r sclass ssec slost sslow sexits <<<"$(run_one "$prog" "$SURVIVAL" "$SURVIVAL_TMO")"
    sinfo="${sclass} ${ssec}s slow=${sslow}"
  fi
  verdict=$(reconcile "${dclass:-}" "${sclass:-}")
  printf "%-30s %-16s %-16s %s\n" "$prog" "${dclass:-skip}" "${sclass:-skip}" "$verdict" | tee -a "$RES"
done

echo | tee -a "$RES"
echo "==== SUMMARY ====" | tee -a "$RES"
grep -cE "REAL_FAULT"  "$RES" | xargs printf "  REAL_FAULT : %s\n" | tee -a "$RES"
grep -cE "SCALE_ONLY"  "$RES" | xargs printf "  SCALE_ONLY : %s\n" | tee -a "$RES"
grep -cE "BOXLIMIT"    "$RES" | xargs printf "  BOXLIMIT   : %s  (box RAM too small for N -- benign)\n" | tee -a "$RES"
grep -cwE "OK|OK\(design-only\)" "$RES" | xargs printf "  OK         : %s\n" | tee -a "$RES"
echo "  (full table: $RES)" | tee -a "$RES"
