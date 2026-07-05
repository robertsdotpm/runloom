# Schemata mutation testing (the practical C-core mutation sweep)

`tools/mutate/mutate.py` proved the idea but rebuilds the extension per mutant
(~2 min each) — ~180 h for the core, so it was never swept. This is the
schemata answer: **rewrite the source once so every mutation point is a
runtime-selectable branch, compile once, then each mutant is an env var**. No
compiler in the loop after the first build. (Exactly the "patch it at run time
instead of recompiling" idea.)

## Why it needed work here

- **The tool**: [dredd](https://github.com/mc-imperial/dredd) (Google/Imperial,
  Clang-based) does the schemata rewrite. `setup_dredd.sh` fetches the release +
  its LLVM-17 runtime libs; callers feed it clang-18's builtin-header dir.
- **The .inc problem**: dredd only mutates the *primary* `.c` it's given, never
  `#include`d fragments — but runloom's core logic lives in `*.c.inc` fragments.
  `flatten.py` inlines a TU's fragments into one physical `.c` first (tracking
  provenance so a mutant maps back to the real `.inc:line`), so dredd reaches
  all of it. (No `#line` directives — they'd send dredd's presumed location back
  into the `.inc` and it would skip the code again.)
- **The dlopen TLS problem**: dredd's prelude uses `thread_local`, which a
  `dlopen`'d `.so` can't allocate in the static-TLS surge block. The mutant set
  is process-global (one env var), so `build_target.sh` drops the qualifier — a
  plain global is equivalent for a sweep.

## Use

    tools/mutate/schemata/setup_dredd.sh            # once (fetches dredd)
    tools/mutate/schemata/build_target.sh netpoll   # flatten+mutate+build ONE TU
    tools/mutate/schemata/sweep.py netpoll --sample 500   # fast first signal
    tools/mutate/schemata/sweep.py netpoll                # full TU sweep

Everything runs in an **isolated git worktree** (`RUNLOOM_MUT_WORKTREE`, default
`~/projects/pygo-mutants`) at live HEAD — the live tree's `.so` is never touched,
so the soaks keep running.

## Reading a result

A mutant is **KILLED** if any test fails or **hangs** (the timeout — lost-wake
class), **SURVIVED** if the subset stays green. `<TU>.survivors.txt` lists the
survivors by real `.inc:line`: **each is a line whose behaviour no test in the
subset constrains** — the true untested-logic list, sharper than coverage
(coverage says a line *ran*; a survivor says a bug there would go *unnoticed*).
The sweep is resumable (`<TU>.sweep.jsonl` checkpoint).

Scale: netpoll alone = **18,890 mutants** (dredd does all relational + expression
replacements, ~8/op-site). The subset in `sweep.py`'s AFFINITY map is the cheap
first cut; a *confirmed* survivor must survive the whole suite (or a
coverage-sliced superset) — the natural weekly-rotation escalation.
