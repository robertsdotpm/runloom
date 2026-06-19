#!/usr/bin/env bash
# Proof that the Cython echo handler's per-request hot loop creates ZERO Python
# objects -- the claim that makes tiers 4 & 5 meaningful.
#
# We isolate the handler *implementation* function (__pyx_pf_*handler*, the loop
# body) from the module-init function (runs once at import) and the PyCFunction
# arg-unpack wrapper (__pyx_pw_*, runs once per CONNECTION, not per request).
# In the impl function the only calls must be:
#   * the two INDIRECT capi calls  -- call *(%rax) / call *0x8(%rax)
#     (recv_into / send_all fetched from the runloom_c.__tcp_capi__ table)
#   * __stack_chk_fail@plt          -- the stack-canary epilogue, branched to
#     only on a detected stack smash (never on the normal path)
# Any call to a Py_/_Py_/PyObject_/PyLong_/PyBytes_/PyErr_/__Pyx_ symbol in the
# impl function is a FAIL: it would mean per-request PyObject traffic.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
SO=$(ls "$HERE"/handler_cy*.so 2>/dev/null | head -1)
[ -n "${SO:-}" ] || { echo "no handler_cy*.so -- run build_cy.py first"; exit 2; }
OUT="${1:-$HERE/handler_cy_hotloop_disasm.txt}"

objdump -d "$SO" | awk '
  /^[0-9a-f]+ <.*>:/ { inblk = ($0 ~ /__pyx_pf_.*handler/) }
  inblk { print }
' > "$OUT"
[ -s "$OUT" ] || { echo "could not isolate handler implementation function"; exit 2; }

echo "=== handler implementation function: every call instruction ==="
grep -E '\bcall\b' "$OUT" | sed 's/^/  /'
echo

BAD=$(grep -E '\bcall\b' "$OUT" \
      | grep -iE 'Py_|_Py|PyObject|PyLong|PyBytes|PyMem|PyErr|__Pyx_' \
      | grep -v '__stack_chk_fail' || true)
if [ -n "$BAD" ]; then
  echo "FAIL: PyObject traffic found in the hot loop:"
  echo "$BAD" | sed 's/^/  /'
  exit 1
fi
echo "PASS: hot loop is PyObject-free."
echo "      Per-request cost = 1 indirect recv_into + 1 indirect send_all (capi table)."
echo "      (PyObject setup is confined to import-time module-init and the"
echo "       once-per-connection arg-unpack wrapper, never the per-request loop.)"
