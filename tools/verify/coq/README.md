# Coq — machine-checked, unbounded protocol invariants

Spin and CBMC are *bounded* (a fixed number of actors / a SAT unwinding); the
verify/ README is explicit that "they are bounds". This directory holds the
**unbounded, machine-checked** counterpart: Coq proofs that hold over *every*
reachable state of a protocol's transition system, for any number of
transitions, via an inductive invariant.

## `WakeState.v`
The unbounded analog of `verify/spin/wake_state.pml`. Models the per-g
`wake_state` machine (the 6-state PARKED/QUEUED/RUNNING/RUNNING_WOKEN/
SWEEPING/SWEEPING_WOKEN protocol from `runloom_sched.h`) as a transition relation
and proves the two safety invariants the pml's `check_inv()` asserts:

- **INV1** `qentries = (state = QUEUED ? 1 : 0)` — no duplicated / orphaned
  run-queue entry;
- **INV2** `owners = (state ∈ {RUNNING,RUNNING_WOKEN} ? 1 : 0)` — at most one
  hub owns the g, i.e. no double resume.

`step_preserves` shows every transition (waker / hub pull+release / sweeper
claim+release) preserves the invariant; `inv_reachable` lifts it to all
reachable states by induction — no bound. `invariant_has_teeth` shows a buggy
hub-pull that fails to decrement `qentries` lands in a state the invariant
rejects (the exact INV1 violation the Spin assert guards), so the proof is not
vacuous.

**Scope:** this proves the protocol's *transition system* (like the Spin model
but unbounded); it does not model the C11 weak-memory orderings. A full
Iris/FSL++ weak-memory separation-logic proof of the lock-free C is the deeper,
multi-week item on the roadmap (`verify/extra/README.md`).

## Run
```sh
verify/coq/run_coq.sh        # coqc every *.v; a passing coqc IS the proof check
```
Needs `coqc` (Rocq/Coq). No sudo: `opam install -y coq rocq-stdlib`. The runner
finds `coqc` on PATH or in the opam switch and folds its result into
`verify/run_verify.sh`.
