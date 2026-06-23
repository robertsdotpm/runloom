#!/usr/bin/env bash
# run_iris.sh -- machine-check the Iris (HeapLang) separation-logic proofs.
#
# These prove RUNNING concurrent HeapLang programs (CmpXchg races, parallel
# composition) against Iris's concurrent separation logic -- a thread-modular
# program proof, beyond the finite-state Spin checks and the transition-system
# Coq proofs. A passing coqc IS the proof check.
# Prints "N passed, M failed" so run_verify.sh can fold it into the total.
#
# Needs coqc + Iris.  Install (no sudo):
#   opam repo add coq-released https://coq.inria.fr/opam/released
#   opam install -y coq-iris-heap-lang
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"

echo "-- Iris (HeapLang concurrent separation logic) --"
if ! command -v coqc >/dev/null 2>&1; then
    for sw in coq herd default; do
        [ -x "$HOME/.opam/$sw/bin/coqc" ] && { eval "$(opam env --switch="$sw" 2>/dev/null)"; break; }
    done
fi
if ! command -v coqc >/dev/null 2>&1; then
    echo "  (coqc not found -- skipping;  opam install -y coq-iris-heap-lang)"; exit 0
fi

pass=0; fail=0
cd "$HERE"
for v in "$HERE"/*.v; do
    [ -e "$v" ] || continue
    name="$(basename "$v" .v)"
    printf '  [iris] %-22s ' "$name"
    if coqc -q "$name.v" >/tmp/iris.$$.log 2>&1; then
        echo "PASS -- all separation-logic specs machine-checked"; pass=$((pass+1))
    else
        if grep -qiE 'Cannot find|Unable to locate|library iris' /tmp/iris.$$.log; then
            echo "SKIP -- Iris not installed (opam install -y coq-iris-heap-lang)"
        else
            echo "FAIL -- see below"; sed 's/^/      /' /tmp/iris.$$.log | tail -18; fail=$((fail+1))
        fi
    fi
done
"$(command -v safe-rm || echo rm)" -f /tmp/iris.$$.log "$HERE"/*.vo "$HERE"/*.vok "$HERE"/*.vos "$HERE"/*.glob 2>/dev/null
echo "  $pass passed, $fail failed"
[ "$fail" -eq 0 ]
