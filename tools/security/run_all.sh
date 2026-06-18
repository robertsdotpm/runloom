#!/usr/bin/env sh
# Run the runloom security-verification checks (see FINDINGS.md). Free-threaded
# 3.13t, GIL forced off. Exits non-zero if any check fails.
#
# For the race checks (S2/S3) under ThreadSanitizer, build the whole ext with
# tools/run_sanitizers_ext.sh and run test_signal_storm.py / test_refcount_race.py
# under it (TSan catches a race even on a run that doesn't happen to corrupt).
set -eu
cd "$(dirname "$0")/../.."
PY="${PYTHON:-python3}"
export PYTHON_GIL=0
export PYTHONPATH=src
H=tools/security/stack_scrub_helper
echo "== S1 recycled-stack hygiene =="
if command -v cc >/dev/null 2>&1; then
    cc -O0 -shared -fPIC -o "$H.so" "$H.c"
    "$PY" tools/security/test_stack_scrub.py
else
    echo "  SKIP: cc not found (cannot build stack_scrub_helper)"
fi
echo "== S2 signal storm ==";           "$PY" tools/security/test_signal_storm.py
echo "== S3 cross-hub refcount race =="; "$PY" tools/security/test_refcount_race.py
echo "== S4 valgrind memcheck =="
if command -v valgrind >/dev/null 2>&1; then
    REALPY="$("$PY" -c 'import sys; print(sys.executable)')"
    valgrind --tool=memcheck --trace-children=yes --leak-check=no \
        --errors-for-leak-kinds=none --error-exitcode=99 \
        --suppressions=tools/security/runloom.supp \
        "$REALPY" tools/security/vg_smoke.py >/tmp/runloom_vg.out 2>&1 \
        && grep -E 'ERROR SUMMARY' /tmp/runloom_vg.out | tail -1 \
        || { echo "  valgrind found errors:"; grep -E 'ERROR SUMMARY|Invalid|uninitialised' /tmp/runloom_vg.out | tail; exit 1; }
else
    echo "  SKIP: valgrind not installed"
fi
echo "== S6 bridge network fuzz =="; "$PY" tools/security/fuzz_bridge.py --iters 600
echo "== S7 TLS bridge fuzz =="
"$PY" -c 'import cryptography' 2>/dev/null \
    && "$PY" tools/security/fuzz_tls_bridge.py --iters 300 \
    || echo "  SKIP: cryptography not installed (pip install cryptography)"
echo "== S8 C-API adversarial fuzz =="
# run as a SUBPROCESS so a segfault/abort in argument handling is caught as a
# non-zero/signal exit (the last CAPI_TRY on stderr is the culprit).
"$PY" tools/security/fuzz_capi.py --iters 3000 \
    || { echo "  FAIL: fuzz_capi crashed (last attempt above is the culprit)"; exit 1; }
echo "== S9 build hardening =="; sh tools/security/check_hardening.sh
echo "== all security checks passed =="
