# Dartagnan — SMT bounded encoding (the third weak-memory engine)

pygo already checks its fence placement two ways: **herd7** (axiomatic
enumeration of executions, `verify/litmus/`) and **GenMC** (stateless model
checking of the real C under RC11, `verify/genmc/`). Dartagnan adds a third,
*technologically independent* way: it encodes the bounded executions **and**
the memory model into a single SMT formula and asks a solver. This is the
"circuit encoding" school of concurrency verification.

> Gavrilenko, Ponce de León, Furbach, Heljanko, Meyer. *BMC for Weak Memory
> Models: Relation Analysis for Compact SMT Encodings.* CAV 2019.

## Why a third engine

| engine | technique | sees |
|--------|-----------|------|
| herd7 | axiomatic, enumerate all executions | small litmus, any `.cat` |
| GenMC | stateless model checking, real C | real protocol, RC11 |
| **Dartagnan** | **SMT encoding of executions × `.cat`** | **litmus or C, any `.cat`, bounded** |

The three fail in different ways for different reasons, so agreement is much
stronger evidence than any one alone. Dartagnan's distinguishing power: the
memory model is an *input* (`.cat`), so the same tests can be re-checked under
`rc11`, `imm`, `sc`, … by swapping one file — useful when reasoning about
whether a fence that's necessary under RC11 is still necessary under a weaker
or stronger model.

## What it checks

The **same** `verify/litmus/*.litmus` tests herd7 runs (Dartagnan reads the
herd C-litmus format natively), under an RC11 `.cat`, with the identical
expected outcomes:

| test | expected | meaning |
|------|----------|---------|
| `commit_cas_then_publish` | reachable | commit-CAS acquire *alone* allows a stale `ready_out` read |
| `commit_lock_publish` | unreachable | the `pool->lock` round-trip forbids it |
| `wakelist_mpsc` | unreachable | cross-thread `wake_list` handoff carries g state |
| `parkwake_no_fence` | reachable | release/acquire alone allows the park/wake SB lost wakeup |
| `parkwake_sc_fence` | unreachable | `seq_cst` StoreLoad fences forbid it |

## Run it

```sh
# build the jar (Maven + JDK17) and point at it:
export DAT3M_HOME=/path/to/Dat3M
export CAT=$DAT3M_HOME/cat/rc11.cat
verify/dartagnan/run_dartagnan.sh

# or via the master runner (skips cleanly if Dartagnan is absent):
verify/run_verify.sh
```

Install: <https://github.com/hernanponcedeleon/Dat3M> (jar via Maven, or
`docker pull dat3m/dat3m`).

## Status

**Scaffold.** The corpus reuse, `.cat`/runner detection, expected-outcome
table, and `run_verify.sh` wiring are done and the script skips cleanly when
Dartagnan is absent. The one thing to confirm against your Dartagnan build is
the verdict-token mapping in `classify()` (releases differ: `PASS`/`FAIL` vs
"can be violated"/"holds"); raw output is logged under the temp `$WORK` dir for
exactly that. Extending to the real-C harnesses in `verify/genmc/*.c` (via
Dartagnan's LLVM/SMACK frontend) is the natural follow-up.
