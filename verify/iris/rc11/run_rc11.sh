#!/usr/bin/env bash
# run_rc11.sh -- machine-check the iRC11 / RC11 weak-memory separation-logic
# proofs (gpfsl).  These prove a running concurrent program under the RELAXED
# (RC11) memory model -- the genuine weak-memory tier above the SC Iris proofs.
# A passing compile IS the proof check.  Prints "N passed, M failed".
#
# Needs gpfsl, which pins iris-dev and so lives in its own opam switch.  Install:
#   opam switch create pygo-weakmem --packages=ocaml-system
#   opam repo add coq-released https://coq.inria.fr/opam/released
#   opam repo add iris-dev git+https://gitlab.mpi-sws.org/iris/opam.git
#   opam install -y coq-gpfsl
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
SW="${PYGO_WEAKMEM_SWITCH:-pygo-weakmem}"

echo "-- iRC11 / RC11 weak memory (gpfsl) --"
if [ ! -d "$HOME/.opam/$SW/lib/coq/user-contrib/gpfsl" ]; then
    echo "  (gpfsl not installed in switch '$SW' -- skipping; see WEAK_MEMORY.md)"; exit 0
fi
eval "$(opam env --switch="$SW" --set-switch 2>/dev/null)"
export PATH="$HOME/.opam/$SW/bin:$PATH"

# Rocq 9.2+ uses `rocq compile`; older toolchains use `coqc`.
if command -v rocq >/dev/null 2>&1; then COMPILE() { rocq compile -q "$1"; }
elif command -v coqc >/dev/null 2>&1; then COMPILE() { coqc -q "$1"; }
else echo "  (no rocq/coqc in switch '$SW' -- skipping)"; exit 0; fi

pass=0; fail=0
cd "$HERE"
for v in "$HERE"/*.v; do
    [ -e "$v" ] || continue
    name="$(basename "$v" .v)"
    printf '  [rc11] %-22s ' "$name"
    if COMPILE "$name.v" >/tmp/rc11.$$.log 2>&1; then
        echo "PASS -- RC11 separation-logic proof machine-checked"; pass=$((pass+1))
    else
        echo "FAIL -- see below"; grep -iE 'error|tactic failure' /tmp/rc11.$$.log | head -8; fail=$((fail+1))
    fi
done
"$(command -v safe-rm || echo rm)" -f /tmp/rc11.$$.log "$HERE"/*.vo "$HERE"/*.vok "$HERE"/*.vos "$HERE"/*.glob 2>/dev/null
echo "  $pass passed, $fail failed"
[ "$fail" -eq 0 ]
