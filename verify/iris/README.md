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

## `WakeQueue.v` (Stage 2 — plain/SC Iris)
The full `wake_state` lifecycle as a 3-state cell PARKED(0)→QUEUED(1)→
RUNNING(2) with two CAS transitions, proving as a running concurrent program
the two invariants `wake_state.pml` checks:
- `wake_at_most_once` (INV1): among racing wakers, at most one enqueues the g
  (no duplicate / orphan run-queue entry);
- `pull_at_most_once` (INV2): among racing hubs, at most one runs it
  (no double resume).
Two exclusive ghost tokens (enq/run) flow to the unique CAS winner of each
transition; two winners would hold two copies of an exclusive token.

## Scope and the weak-memory ceiling
Stages 1–2 are **sequentially-consistent** Iris (HeapLang has an SC memory
model). **Stage 3** — re-establishing pygo's release/acquire fences under
**RC11** with **iRC11 / gpfsl** — is the genuine research-scale ceiling, and is
documented in `WEAK_MEMORY.md` rather than left as `Admitted` stubs. The key
honesty point: the weak-memory *fence correctness* this would target (the
netpoll commit publish needs the `pool->lock` round-trip, not the CAS acquire)
is **already machine-checked** under RC11 by the herd7 litmus tests
(`verify/litmus/`) and GenMC (`verify/genmc/`), both green in the suite. iRC11
would add the unbounded, compositional separation-logic spec on top; the opam
install is feasible (clean resolution; see `WEAK_MEMORY.md`) but the proof
itself is a research artifact. This directory advances the frontier as far as
is soundly checkable here and marks the boundary explicitly.

## Run
```sh
verify/iris/run_iris.sh      # coqc every *.v; folded into verify/run_verify.sh
```
Install (no sudo):
```sh
opam repo add coq-released https://coq.inria.fr/opam/released
opam install -y coq-iris-heap-lang
```
