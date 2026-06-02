#!/usr/bin/env bash
# run_dartagnan.sh -- a THIRD, independent engine on the exact fence-placement
# questions herd7 and GenMC already check, using a different proof technology.
#
# Dartagnan (DAT3M) is a bounded model checker that takes a .cat memory model
# plus a program and encodes the bounded executions AND the memory model into a
# single SMT formula -- the "circuit encoding" of weak-memory verification --
# then asks an SMT solver whether the target condition is reachable.
#   Gavrilenko, Ponce de Leon, Furbach, Heljanko, Meyer, "BMC for Weak Memory
#   Models: Relation Analysis for Compact SMT Encodings", CAV 2019.
#
# This reuses the SAME verify/litmus/*.litmus tests herd7 runs (Dartagnan reads
# the herd C-litmus format natively) under an RC11 .cat model.  Three engines
# now answer the same questions by independent means:
#     herd7    -- axiomatic enumeration of executions          (verify/litmus)
#     GenMC    -- stateless model checking, real C, RC11        (verify/genmc)
#     Dartagnan-- SMT bounded encoding, .cat-parametric         (here)
# Agreement across all three is far stronger evidence than any one alone, and
# Dartagnan's cat-parametric encoding lets us swap memory models (rc11/imm/sc)
# without touching the tests.
#
# Dartagnan builds with Maven/JDK17 or ships as a Docker image; not in apt.
# Install: https://github.com/hernanponcedeleon/Dat3M
#   either  build the jar and `export DAT3M_HOME=/path/to/Dat3M`
#   or      `docker pull dat3m/dat3m`
# Provide an RC11 cat model via CAT=... (Dat3M ships cat/rc11.cat).
# Run: verify/dartagnan/run_dartagnan.sh
#
# NOTE: the one thing to confirm against your Dartagnan build is the verdict
# tokens in classify() below -- different releases print PASS/FAIL vs
# "can be violated"/"holds".  The harness logs raw output to $WORK for exactly
# this.  Everything else (corpus, cat, expected table, wiring) is done.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
LIT="$HERE/../litmus"
WORK="$(mktemp -d /tmp/pygo_dartagnan.XXXXXX)"
BOUND="${DARTAGNAN_BOUND:-2}"

green() { printf '\033[32m%s\033[0m' "$1"; }
red()   { printf '\033[31m%s\033[0m' "$1"; }

# ---- locate a Dartagnan runner -------------------------------------------
DART=""
if command -v dartagnan >/dev/null 2>&1; then
    DART="dartagnan"
elif [ -n "${DAT3M_HOME:-}" ] && ls "$DAT3M_HOME"/dartagnan/target/dartagnan-*.jar >/dev/null 2>&1; then
    DART="java -jar $(ls "$DAT3M_HOME"/dartagnan/target/dartagnan-*.jar | head -1)"
elif command -v docker >/dev/null 2>&1 && docker image inspect dat3m/dat3m >/dev/null 2>&1; then
    DART="docker run --rm -v $HERE/..:/verify dat3m/dat3m dartagnan"
    LIT="/verify/litmus"
fi
if [ -z "$DART" ]; then
    echo "  (Dartagnan not found -- skipping; see verify/dartagnan/README.md)"
    exit 0
fi

# ---- locate an RC11 cat model --------------------------------------------
CAT="${CAT:-${DAT3M_HOME:-}/cat/rc11.cat}"
if [ ! -f "$CAT" ] && [ "${DART#docker}" = "$DART" ]; then
    echo "  (Dartagnan present but no RC11 .cat -- set CAT=/path/to/rc11.cat; skipping)"
    exit 0
fi
[ "${DART#docker}" != "$DART" ] && CAT="${CAT:-/Dat3M/cat/rc11.cat}"

# reachable=yes for an expected-Sometimes test, =no for an expected-Never test.
# Adjust these token sets to your Dartagnan version if a test reports UNKNOWN.
classify() {  # raw-output -> echoes yes|no|unknown
    if printf '%s' "$1" | grep -qiE 'result[: ]+fail|can be violated|is reachable|violation|witness found|unsafe'; then
        echo yes
    elif printf '%s' "$1" | grep -qiE 'result[: ]+pass|holds|unreachable|no violation|is safe|property.*satisf'; then
        echo no
    else
        echo unknown
    fi
}

pass=0; fail=0
check() {  # name, want(Sometimes|Never), description
    local name="$1" want="$2" desc="$3"
    local want_reach; [ "$want" = "Sometimes" ] && want_reach=yes || want_reach=no
    printf '  [dartagnan] %-26s ' "$name"
    local out; out="$($DART "$CAT" "$LIT/$name.litmus" --bound="$BOUND" 2>&1)"
    echo "$out" >"$WORK/$name.log"
    local got; got="$(classify "$out")"
    if [ "$got" = "$want_reach" ]; then
        green "PASS"; echo " (reachable=$got) -- $desc"; pass=$((pass+1))
    elif [ "$got" = unknown ]; then
        red "FAIL"; echo " (UNKNOWN verdict -- check $WORK/$name.log / classify tokens)"; fail=$((fail+1))
    else
        red "FAIL"; echo " (reachable=$got, want=$want_reach) -- $desc"; fail=$((fail+1))
    fi
}

echo "-- Dartagnan (SMT bounded encoding under $(basename "$CAT"), bound=$BOUND) --"
check commit_cas_then_publish Sometimes "commit-CAS acquire alone allows the stale ready_out read"
check commit_lock_publish     Never     "pool->lock round-trip forbids the stale read"
check wakelist_mpsc           Never     "cross-thread wake_list handoff carries g state"
check parkwake_no_fence       Sometimes "release/acquire alone allows the park/wake SB lost wakeup"
check parkwake_sc_fence       Never     "seq_cst StoreLoad fences forbid the lost wakeup"

echo "  $pass passed, $fail failed"
[ "$fail" -eq 0 ]
