#!/usr/bin/env bash
# check_all.sh -- run every layer of pygo's correctness stack.
#
# Layers, fastest first:
#   static      gcc -fanalyzer + cppcheck on the C core           ~1-2 min
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
#   scripts/check_all.sh verify          # just the formal proofs
#   scripts/check_all.sh tests ctest     # pick phases
#   scripts/check_all.sh bench           # perf only (NOT in `all` -- machine-dependent)
#   scripts/check_all.sh combo           # config-matrix sweep (candidate for `all`)
#
# Env:
#   PYTHON=...   interpreter for the Python suite + fuzzer
#                (default: a free-threaded 3.13t if found, else python3)
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Skip pytest's third-party plugin autoload for every phase that shells into
# pytest.  ~20 unrelated plugins are installed in this env and pytest imports
# all of them per process (~4s/file of pure overhead), and one pulls _brotli
# which re-enables the GIL -- wrong for the free-threaded target.  The suite
# uses none of them.  Opt back in with PYGO_TEST_PYTEST_PLUGINS=1.
if [ "${PYGO_TEST_PYTEST_PLUGINS:-}" != "1" ]; then
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
      hr "Python test suite"
      "$PYTHON" -m pytest tests/ -q -p no:cacheprovider || rc=1
      ;;
    mn)
      hr "M:N scheduler fuzzer (stable gate)"
      # --stable: known-good patterns, so this is a clean regression gate.
      # For full fuzzing (which reproduces the contended-select crash,
      # finding A in tools/README.md) run: tools/mn_stress.py --iters N
      "$PYTHON" tools/mn_stress.py --iters "${MN_ITERS:-150}" --stable || rc=1
      ;;
    replay)
      hr "Controlled M:N deterministic replay (PYGO_MN_BARRIER)"
      # Same seed must reproduce one signature across reps; each probe exits
      # non-zero if any seed varies.  Guards the five replay levers
      # (tools/mn_controlled/README.md) against silent regression.
      "$PYTHON" tools/mn_controlled/repro_probe.py "${REPLAY_SEEDS:-8}" "${REPLAY_REPS:-6}" || rc=1
      "$PYTHON" tools/mn_controlled/repro_select.py "${REPLAY_SEEDS:-8}" "${REPLAY_REPS:-6}" || rc=1
      "$PYTHON" tools/mn_controlled/repro_timer.py "${REPLAY_SEEDS:-8}" "${REPLAY_REPS:-6}" || rc=1
      ;;
    static)
      hr "Static analysis (gcc -fanalyzer gate + cppcheck advisory)"
      PYTHON="$PYTHON" bash tools/static_analysis.sh || rc=1
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
      hr "Formal verification (Spin + CBMC)"
      verify/run_verify.sh || rc=1
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
      echo "unknown phase: $ph (want: tests mn lincheck dst ctest static sanitizers exttsan verify bench combo all)"; rc=2 ;;
  esac
done

hr "summary"
if [ "$rc" -eq 0 ]; then echo "ALL GREEN"; else echo "FAILURES (rc=$rc)"; fi
exit "$rc"
