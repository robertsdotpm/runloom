#!/usr/bin/env bash
# new_model.sh <name> <verified-fn> [source.c] -- scaffold a CBMC verification
# harness so adding a formal model is ~an hour, not a heroic afternoon (item 14).
#
# Emits a demonic-oracle CBMC skeleton wired to the project's conventions:
#   * a SOURCE-ANCHOR header naming the function it mirrors (kept honest by
#     model_source_drift.py, which hashes the anchored source);
#   * a default config that must VERIFY and a -DBUG_DEMO negative control that
#     must FAIL (teeth by construction -- run_verify pairs them);
#   * the exact run_verify.sh registration lines and the COVERAGE.md row to add.
#
# It does NOT edit run_verify.sh / COVERAGE.md for you (those are reviewed by
# hand) -- it prints the snippets so wiring is copy-paste, not archaeology.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
NAME="${1:?usage: new_model.sh <name> <verified-fn> [source.c]}"
FN="${2:?need the function/property name this model verifies}"
SRC="${3:-src/runloom_c/<TU>.c}"
OUT="$HERE/cbmc/${NAME}_cbmc.c"

if [ -e "$OUT" ]; then echo "refusing to overwrite existing $OUT"; exit 1; fi

# Write the template with a QUOTED heredoc (no shell expansion inside the C),
# then substitute the @PLACEHOLDER@s -- so nothing in the C body is ever run.
cat > "$OUT" <<'EOF'
/* @NAME@_cbmc.c -- DEMONIC-ORACLE verification of @FN@.
 *
 * SOURCE-ANCHOR: mirrors @FN@ in @SRC@.  Keep the modelled logic faithful to
 * that function; model_source_drift.py hashes the anchor so a source change not
 * reflected here fails the drift lint (register it in model_source_anchors.json).
 *
 * THE PROPERTY (state it precisely, in terms an assert can check):
 *   <one-line safety/liveness invariant @FN@ must preserve for EVERY sequence of
 *    the environment's demonic returns>
 *
 * Configs (each a separate CBMC run; run_verify pairs pass+neg):
 *   (default)     faithful model of @FN@                    -> must VERIFY.
 *   -DBUG_DEMO    one hand-injected break of the invariant  -> must FAIL (teeth).
 *
 * AVOID VACUITY: the negative control must reach the SAME assertion the default
 * does and make it FALSE -- if -DBUG_DEMO passes, the assert is dead (this is the
 * exact trap the netpoll demonic harness fell into once).  Model the environment
 * (syscalls, peers, the waker) as returning ANY value in its documented set
 * (tools/verify/kernel_contract/syscall_returns.json), never assume success.
 */

int nondet_int(void);
_Bool nondet_bool(void);

/* environment: model whatever @FN@ calls as demonic (any legal return). */
static int demonic_return(void)
{
    return nondet_bool() ? 0 : -1;   /* TODO: the real call's return set */
}

int main(void)
{
    int st = nondet_int();           /* TODO: arbitrary COHERENT start state */
    int rc = demonic_return();
#ifdef BUG_DEMO
    st = st + 1;                     /* TODO: the bug that makes the property false */
#else
    (void)rc;                        /* TODO: the real @FN@ transition */
#endif
    __CPROVER_assert(1 /* TODO: the real invariant over st */,
                     "@FN@: <the property>");
    (void)st;
    return 0;
}
EOF

sed -i "s|@NAME@|${NAME}|g; s|@FN@|${FN}|g; s|@SRC@|${SRC}|g" "$OUT"

echo "scaffolded $OUT"
echo
echo "=== add to tools/verify/run_verify.sh (near the other CBMC launches): ==="
printf '    launch %s      check_cbmc %s            "" \\\n' "$NAME" "$NAME"
printf '        "%s: <the property, one line>"\n' "$FN"
printf '    launch %s-neg  check_cbmc_must_fail %s  BUG_DEMO \\\n' "$NAME" "$NAME"
printf '        "negative control: injected break of %s is caught"\n' "$FN"
echo
echo "=== add to tools/verify/COVERAGE.md: ==="
echo "| ${NAME} | CBMC | ${FN} (${SRC}) | demonic-oracle | pass + BUG_DEMO neg |"
echo
echo "Next: fill the TODOs (start state, transition, bug, invariant), register the"
echo "source anchor in model_source_anchors.json, then confirm the default"
echo "VERIFIES and BUG_DEMO FAILS before wiring into run_verify.sh."
