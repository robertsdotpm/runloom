# tests_stdlib — CPython stdlib suite under runloom's M:N scheduler

A bug-hunting harness that runs the CPython 3.13t standard-library test corpus
inside runloom goroutines on the M:N scheduler, to shake out scheduler /
stackful-coroutine bugs against real, diverse Python workloads. The corpus
itself is **not committed** (it's ~80 MB of verbatim upstream `Lib/test`) — run
the `rsync` under "Vendor the corpus" once before the first sweep.

## Layout

| path | what |
|------|------|
| `test/` | local copy of CPython 3.13t `Lib/test` (the corpus). **Not committed** — gitignored, ~80 MB of verbatim upstream source; populate once with the `rsync` below. Shadows the installed `test` package when `tests_stdlib/` is first on `sys.path`. |
| `c_stack_sizes.py` / `stdlib_c_stack_sizes.json` | catalogs each stdlib C function's `.eh_frame` prologue frame size — the fat single frames (e.g. `select_select_impl` ~49 KB) that can overflow a small goroutine stack. Regenerate with `python tests_stdlib/c_stack_sizes.py`. |
| `run_one_mn.py` | child: runs ONE module's unittest suite inside a goroutine (`mn_init`/`mn_go`/`mn_run`), one per subprocess. |
| `sweep_mn.py` | driver: discovers all modules, runs each child with a timeout in a parallel pool, classifies PASS/FAIL/LOADERR/CRASH/HANG/ERROR, saves logs. |
| `triage.py` | clusters `results/` into bug buckets and regenerates the auto-section of `BUGS.md`. |
| `BUGS.md` | the running bug triage. |
| `results/` | per-run artifacts (gitignored): `results.csv` + `<STATUS>/<module>.log`. |

## What "ported to run in a goroutine with N:M on" means here

Tests are **not** monkey-patched (threading/asyncio/socket stay native). Each
module's `unittest` suite is *loaded and run inside a goroutine* under the
work-stealing M:N scheduler with the GIL off (`PYTHON_GIL=0`), so the
stackful-coroutine engine and scheduler are exercised by real Python. The test
files themselves are kept verbatim; the harness does the porting.

## Run it

```bash
# full sweep (≈690 modules), parallel:
PYTHON_GIL=0 python tests_stdlib/sweep_mn.py --jobs 10 --hubs 4 --timeout 180

# one module, verbose (last stderr line before a crash names the dying test):
PYTHON_GIL=0 PYTHONPATH=tests_stdlib:src python tests_stdlib/run_one_mn.py test.test_heapq 4

# cluster the results into BUGS.md:
python tests_stdlib/triage.py
```

## Vendor the corpus

The `test/` corpus is gitignored, so populate it once (and to refresh it after a
CPython upgrade) by copying your interpreter's own `Lib/test`:

```bash
rsync -a --exclude='__pycache__' \
  "$(python -c 'import test,os;print(os.path.dirname(test.__file__))')/" \
  tests_stdlib/test/
```
