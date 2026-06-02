# pygo — project guidance

## No hosted CI
- **Do NOT add GitHub Actions or any hosted CI.** Never create
  `.github/workflows/*.yml` (the existing `.github/workflows-disabled/` is
  deliberately disabled — leave it disabled). GitHub-hosted CI is **not free**
  (especially macOS minutes), and we don't want it. This has been asked for
  repeatedly.
- Our "CI" is **local**: `scripts/check_all.sh`. Phases: `tests mn lincheck dst
  ctest sanitizers exttsan verify` (run `scripts/check_all.sh all` for
  everything, or name phases). Run that before proposing a merge.

## Build & test
- Target is **free-threaded CPython 3.13t** — the M:N scheduler is only real
  with the GIL off. Use `~/.pyenv/versions/3.13.13t/bin/python3` with
  `PYTHON_GIL=0`.
- Build: `python setup.py build_ext --inplace`; run with `PYTHONPATH=src`.
- Run the suite via `tests/run_isolated.py` (one file per subprocess); the
  in-process `pytest tests/` flakes under cross-file state leaks.

## Concurrency tooling (tools/)
- `run_sanitizers.sh` (deque ASan/TSan/UBSan), `run_sanitizers_ext.sh` (whole
  ext under TSan via preloaded libtsan + `setarch -R`), `lincheck/` (Porcupine
  + stateful select model), `dst/` (deterministic simulation), `mutate/`
  (mutation testing), `coverage.sh` (gcov), plus `../verify/` (Spin/CBMC/
  GenMC/herd7). See `tools/README.md`.
