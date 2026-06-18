#!/usr/bin/env bash
# check_all.sh -- run every layer of runloom's correctness stack.
#
# Layers, fastest first:
#   static      security SAST on the C core (parallel across cores):     ~15s
#               gates  = gcc -fanalyzer+taint, seclint (banned unbounded fns)
#               advise = clang analyzer/ArrayBound, clang-tidy cert-*, cppcheck
#   tests       Python test suite (pytest tests/)               ~seconds
#   mn          M:N scheduler fuzzer (tools/mn_stress.py)        ~seconds-min
#   replay      controlled-M:N deterministic replay probes       ~seconds-min
#   lincheck    linearizability (Porcupine + stateful select)   ~seconds
#   dst         deterministic simulation seed sweep             ~seconds
#   ctest       C deque concurrency stress (tests_c/test_cldeque) ~seconds
#   sanitizers  C deque harness under ASan/TSan/UBSan            ~seconds-min
#   exttsan     WHOLE ext under ThreadSanitizer (real runtime)  ~30s-min
#   verify      formal proofs: Spin models + CBMC on real C      ~3-4 min
#   bench       rigorous microbench sweep (informational)        ~1-3 min
#   combo       pairwise config-matrix interaction sweep          ~1-2 min
#
# Usage:
#   scripts/check_all.sh                 # tests + mn + lincheck + dst + ctest
#   scripts/check_all.sh all             # everything incl. sanitizers + verify
#   scripts/check_all.sh verify          # just the formal proofs (parallel)
#   scripts/check_all.sh verify-fast     # proofs minus the 3 slow CBMC monsters
#   scripts/check_all.sh tests ctest     # pick phases
#   scripts/check_all.sh bench           # perf only (NOT in `all` -- machine-dependent)
#   scripts/check_all.sh combo           # config-matrix sweep (candidate for `all`)
#
# Two convenience wrappers wrap the common tiers (see scripts/check_all_fast,
# scripts/check_all_extensive):
#   check_all_fast       = tests mn replay lincheck dst ctest verify-fast
#                          (the routine PRE-MERGE gate -- full Spin + cheap CBMC,
#                           skips the 3 slow proofs; ~minutes)
#   check_all_extensive  = all  (every proof + sanitizers; run before a risky
#                          merge / periodically; the verify phase is now parallel)
#
# The verify / verify-fast phases run their checks through a parallel worker pool
# (VERIFY_JOBS, default nproc).  See verify/run_verify.sh.
#
# Env:
#   PYTHON=...   interpreter for the Python suite + fuzzer
#                (default: a free-threaded 3.13t if found, else python3)
#   VERIFY_JOBS=N  formal-verification worker pool size (default: nproc; 1=serial)
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Skip pytest's third-party plugin autoload for every phase that shells into
# pytest.  ~20 unrelated plugins are installed in this env and pytest imports
# all of them per process (~4s/file of pure overhead), and one pulls _brotli
# which re-enables the GIL -- wrong for the free-threaded target.  The suite
# uses none of them.  Opt back in with RUNLOOM_TEST_PYTEST_PLUGINS=1.
if [ "${RUNLOOM_TEST_PYTEST_PLUGINS:-}" != "1" ]; then
    export PYTEST_DISABLE_PLUGIN_AUTOLOAD=1
fi

# Prefer a free-threaded build -- that's where the M:N scheduler is real.
if [ -z "${PYTHON:-}" ]; then
    for cand in "$HOME/.pyenv/versions/3.13.13t/bin/python3" python3.13t python3; do
        if command -v "$cand" >/dev/null 2>&1; then PYTHON="$cand"; break; fi
    done
fi

phases=("$@")
[ ${#phases[@]} -eq 0 ] && phases=(tests mn replay lincheck dst ctest)
if [ "${phases[0]}" = all ]; then
  phases=(tests mn replay lincheck dst ctest static sanitizers exttsan verify)
fi

rc=0
hr() { printf '\n========== %s ==========\n' "$1"; }

for ph in "${phases[@]}"; do
  case "$ph" in
    tests)
      hr "Python test suite (per-file subprocesses)"
      # Use run_isolated.py (one file per subprocess), NOT in-process
      # `pytest tests/`: the latter accumulates cross-file state leaks -- a
      # prior file can leave an M:N runtime / threads wedged on a lock -- and
      # deadlocks the whole run (observed: an 11-hour hang on this phase).
      # run_isolated starts each file clean, so a real hang is one file's.
      PYTHON_GIL=0 PYTHONPATH=src "$PYTHON" tests/run_isolated.py || rc=1
      ;;
    mn)
      hr "M:N scheduler fuzzer (stable gate)"
      # --stable: known-good patterns, so this is a clean regression gate.
      # For full fuzzing (which reproduces the contended-select crash,
      # finding A in tools/README.md) run: tools/mn_stress.py --iters N
      "$PYTHON" tools/mn_stress.py --iters "${MN_ITERS:-150}" --stable || rc=1
      ;;
    replay)
      hr "Controlled M:N deterministic replay (RUNLOOM_MN_BARRIER)"
      # Same seed must reproduce one signature across reps; each probe exits
      # non-zero if any seed varies.  Guards the five replay levers
      # (tools/mn_controlled/README.md) against silent regression.
      "$PYTHON" tools/mn_controlled/repro_probe.py "${REPLAY_SEEDS:-8}" "${REPLAY_REPS:-6}" || rc=1
      "$PYTHON" tools/mn_controlled/repro_select.py "${REPLAY_SEEDS:-8}" "${REPLAY_REPS:-6}" || rc=1
      "$PYTHON" tools/mn_controlled/repro_timer.py "${REPLAY_SEEDS:-8}" "${REPLAY_REPS:-6}" || rc=1
      ;;
    static)
      hr "Static + security analysis (gcc -fanalyzer+taint & seclint gates; clang/cert/cppcheck advisory)"
      PYTHON="$PYTHON" bash tools/static_analysis.sh || rc=1
      hr "Wake-protocol lint (every wake_state transition is NOTE-witnessed)"
      bash scripts/check_wake_protocol.sh || rc=1
      ;;
    lincheck)
      hr "Linearizability (Porcupine + stateful select model)"
      PYTHON="$PYTHON" bash tools/lincheck/check_lin.sh || rc=1
      ;;
    dst)
      hr "Deterministic simulation (seed sweep)"
      PYTHON_GIL=0 PYTHONPATH=src "$PYTHON" tools/dst/dst.py sweep "${DST_SEEDS:-200}" || rc=1
      ;;
    exttsan)
      hr "Whole-ext ThreadSanitizer (real runtime under TSan)"
      PYTHON="$PYTHON" tools/run_sanitizers_ext.sh "${MN_ITERS:-150}" || rc=1
      ;;
    ctest)
      hr "C deque concurrency stress"
      make -C tests_c test_cldeque >/dev/null && \
        tests_c/test_cldeque "${CLDEQUE_PUSHES:-100000}" 4 4 || rc=1
      ;;
    sanitizers)
      hr "C sanitizer harnesses (ASan/TSan/UBSan)"
      tools/run_sanitizers.sh || rc=1
      ;;
    verify)
      hr "Formal verification (Spin + CBMC, parallel)"
      verify/run_verify.sh || rc=1
      ;;
    verify-fast)
      hr "Formal verification -- fast lane (all Spin + cheap CBMC; skips 3 slow proofs)"
      VERIFY_FAST=1 verify/run_verify.sh || rc=1
      ;;
    bench)
      hr "Rigorous microbench sweep (informational -- bootstrap CIs)"
      PYTHON="$PYTHON" bash tools/bench/bench.sh || rc=1
      ;;
    combo)
      hr "Combinatorial config-matrix sweep (pairwise interactions)"
      PYTHON_GIL=0 "$PYTHON" tools/combinatorial/covering.py --iters "${COMBO_ITERS:-40}" || rc=1
      ;;
    *)
      echo "unknown phase: $ph (want: tests mn replay lincheck dst ctest static sanitizers exttsan verify verify-fast bench combo all)"; rc=2 ;;
  esac
done

hr "summary"
if [ "$rc" -eq 0 ]; then echo "ALL GREEN"; else echo "FAILURES (rc=$rc)"; fi
exit "$rc"
