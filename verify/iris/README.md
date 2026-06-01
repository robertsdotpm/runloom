# Iris — concurrent separation logic on running HeapLang programs

The deepest tier of the roadmap. Where Spin/TLA+/Coq reason about an *abstract*
state machine and CBMC/herd7/GenMC reason about a *bounded* schedule of the C,
Iris proves a **running concurrent program** in HeapLang against a higher-order
concurrent separation logic — thread-modular, unbounded in threads, with real
`CmpXchg` and parallel composition.

This is built in **stages** (each merged on its own); see `VALIDATION.md`.

## `OneShotWake.v` (Stage 1 — plain/SC Iris)
A CAS-based one-shot wake (`mk_wake` / `consume = CmpXchg w true false`) with an
exclusive ghost token. Proves, against two threads racing in real parallel
(`consume #w ||| consume #w`), that the wake is **claimed at most once** —
`consume_at_most_once`: the two callers cannot both win, because winning yields
an exclusive `claimed γ` token and two such tokens are contradictory
(`claimed_exclusive`). This is the "no double resume" / "claimed exactly once"
property of `wake_state.pml` and `netpoll_commit.pml`, lifted from a finite-state
check to a thread-modular program proof.

## Scope and the weak-memory ceiling
`OneShotWake.v` is **sequentially-consistent** Iris (HeapLang has an SC memory
model). The honest ceiling of this arc:

- **Stage 2** (planned): the full `wake_state` 6-state protocol as a HeapLang
  module with ghost state, proving exactly-once/no-double-resume under
  concurrency (not just the one-shot fragment).
- **Stage 3** (weak memory): re-establish the publish/claim property under
  **RC11** using **iRC11 / gpfsl** (the relaxed-memory separation logic built
  on Iris). gpfsl lives in the iris-dev opam repo, not `coq-released`; if it
  does not install against this Iris/Rocq, the herd7 litmus tests
  (`verify/litmus/`) and GenMC (`verify/genmc/`) remain the weak-memory
  evidence for the same fences, and this stays documented as open.

A complete iRC11 proof of the full Chase-Lev deque or the entire netpoll claim
protocol is a research-paper-scale effort; this directory advances the frontier
as far as is soundly checkable here and marks the boundary explicitly rather
than leaving `Admitted` holes claimed as proofs.

## Run
```sh
verify/iris/run_iris.sh      # coqc every *.v; folded into verify/run_verify.sh
```
Install (no sudo):
```sh
opam repo add coq-released https://coq.inria.fr/opam/released
opam install -y coq-iris-heap-lang
```
