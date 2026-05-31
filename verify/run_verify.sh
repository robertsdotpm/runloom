#!/usr/bin/env bash
# run_verify.sh -- run every pygo formal-verification check and report.
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
WORK="$(mktemp -d /tmp/pygo_verify.XXXXXX)"
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

echo "================ pygo formal verification ================"
if have spin && have cc; then
    echo "-- SPIN (exhaustive interleaving, SC memory model) --"
    check_spin cldeque      "Chase-Lev deque: no lost / duplicated work-item"
    check_spin wake_state   "per-g wake_state machine: no lost wake / no double-resume / no dup runq entry"
    check_spin parked_safe  "park_safe/wake_safe handshake: no lost wake, balanced"
    check_spin select_claim "select fired_case CAS: fires at most one case, exactly-once wake"
    check_spin select_close "select Phase-2 vs send/close: no lost/NULL/spurious-sentinel result, conservation"
    check_spin_must_fail wake_state  BUGGY_DROP_WAKE   "a wake dropped during RUNNING (classic lost-wakeup)"
    check_spin_must_fail select_close BUG_CLOSE_NULL   "close-wake delivers NULL instead of closed (the SIGSEGV)"
    check_spin_must_fail select_close BUG_ABORT_NOCASE "abort returns the no-case sentinel for a blocking select"
    check_spin_must_fail select_close BUG_ABORT_DROP   "abort evicts + drops an already-delivered value"
    check_spin_must_fail select_close BUG_SPURIOUS     "spurious wake errors out instead of retrying"
else
    echo "  (spin / cc not found -- skipping Spin models;  sudo apt-get install spin)"
fi

# ---- CBMC: real cldeque.c under concurrent pthreads -------------------
echo "-- CBMC (bounded, on the UNMODIFIED cldeque.c source) --"
if have cbmc; then
    printf '  [cbmc] %-34s ' "cldeque.c"
    # Verify the real cldeque.c at a small capacity (-DPYGO_CLDEQUE_CAP=4):
    # the algorithm is identical, but a 4-slot buffer keeps the SAT
    # encoding tractable.  Production default stays 4096; the stress test
    # in tests_c/test_cldeque.c exercises the full-size deque.
    if cbmc "$CBMC_DIR/cldeque_cbmc.c" "$ROOT/src/pygo_core/cldeque.c" \
            -I "$CBMC_DIR/stubs" -I "$ROOT/src/pygo_core" \
            -DPYGO_CLDEQUE_CAP=4 \
            >"$WORK/cbmc.log" 2>&1; then
        green "PASS"; echo " -- no loss / no duplication / no phantom under all interleavings"
        pass=$((pass+1))
    else
        red "FAIL"; echo " -- see $WORK/cbmc.log"; fail=$((fail+1)); FAILED="$FAILED cbmc-cldeque"
    fi
else
    echo "  (cbmc not found -- skipping;  sudo apt-get install cbmc)"
fi

echo "----------------------------------------------------------"
echo "  $pass passed, $fail failed"
[ -n "$FAILED" ] && echo "  failed:$FAILED"
[ "$QUIET" = 0 ] && echo "  logs under: $WORK"
echo "=========================================================="
[ "$fail" -eq 0 ]
