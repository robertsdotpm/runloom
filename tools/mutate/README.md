# mutate -- mutation testing for the C core

Coverage says which lines *run*; mutation says whether a test would *notice* if
that line were wrong. `mutate.py` introduces one small, compilable fault at a
time (flip a comparison, swap `&&`/`||`, negate an `if`), rebuilds the extension,
and runs a fast slice of the suite:

- mutant **KILLED** (a test failed/hung) -> the suite has teeth at that line;
- mutant **SURVIVED** (everything still passed) -> a real test gap at that exact
  line -- the interesting output.

Surviving mutants are the product: each names a line whose behaviour no test
constrains. Stillborn (won't-compile) mutants are skipped, not scored.

```sh
python tools/mutate/mutate.py            # sweep mutants, report survivors
```

Complements `tools/coverage.sh` (which lines run) and `tools/fault_sweep.py`
(error-path robustness).
