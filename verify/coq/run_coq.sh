#!/usr/bin/env bash
# run_coq.sh -- machine-check the Coq proofs of pygo protocol invariants.
#
# WakeState.v proves the wake_state machine's safety invariants over EVERY
# reachable state (unbounded), complementing the bounded Spin model.  A passing
# coqc IS the proof check -- Coq accepts the development only if every Qed holds.
# Prints "N passed, M failed" so run_verify.sh can fold it into the total.
#
# Needs coqc.  Install (no sudo): opam install -y coq  (then `eval $(opam env)`),
# or with sudo: apt-get install coq.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"

echo "-- Coq (machine-checked, UNBOUNDED protocol invariants) --"
# make a coqc visible: prefer PATH, else the opam switch that has it
if ! command -v coqc >/dev/null 2>&1; then
    for sw in coq herd default; do
        if [ -x "$HOME/.opam/$sw/bin/coqc" ]; then
            eval "$(opam env --switch="$sw" 2>/dev/null)"; break
        fi
    done
fi
if ! command -v coqc >/dev/null 2>&1; then
    echo "  (coqc not found -- skipping;  opam install -y coq)"; exit 0
fi

pass=0; fail=0
cd "$HERE"   # keep .vo/.glob/.lia.cache artifacts local (gitignored here)
for v in "$HERE"/*.v; do
    name="$(basename "$v" .v)"
    printf '  [coq] %-22s ' "$name"
    if coqc -q "$name.v" >/tmp/coqc.$$.log 2>&1; then
        echo "PASS -- all theorems machine-checked"; pass=$((pass+1))
    else
        echo "FAIL -- see below"; sed 's/^/      /' /tmp/coqc.$$.log | tail -15; fail=$((fail+1))
    fi
done
"$(command -v safe-rm || echo rm)" -f /tmp/coqc.$$.log "$HERE"/*.vo "$HERE"/*.vok "$HERE"/*.vos "$HERE"/*.glob 2>/dev/null
"$(command -v safe-rm || echo rm)" -rf "$HERE"/.*.aux 2>/dev/null
echo "  $pass passed, $fail failed"
[ "$fail" -eq 0 ]
