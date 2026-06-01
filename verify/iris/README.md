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

## `rc11/CommitPublish.v` (Stage 3 — weak memory, DONE)
Stages 1–2 are **sequentially-consistent** Iris (HeapLang has an SC memory
model). **Stage 3** is the genuine weak-memory tier: `rc11/CommitPublish.v`
proves pygo's commit-publish release/acquire pattern correct under **RC11** in
**iRC11 / gpfsl** — a running concurrent program whose parker reads the
published readiness (42), never the stale 0, under the relaxed-memory model.
It is machine-checked (`rc11/run_rc11.sh` → PASS), not an `Admitted` stub. See
`WEAK_MEMORY.md` for the property, the proof, and the build. The same fence is
independently corroborated by the herd7 litmus tests (`verify/litmus/`) and
GenMC (`verify/genmc/`) — three independent weak-memory witnesses. (gpfsl pins
iris-dev, so it lives in its own opam switch and never disturbs the
released-Iris build of Stages 1–2.) A full iRC11 proof of the *entire* deque /
claim protocol remains research-scale; the load-bearing commit-publish core is
done.

## Run
```sh
verify/iris/run_iris.sh      # coqc every *.v; folded into verify/run_verify.sh
```
Install (no sudo):
```sh
opam repo add coq-released https://coq.inria.fr/opam/released
opam install -y coq-iris-heap-lang
```
