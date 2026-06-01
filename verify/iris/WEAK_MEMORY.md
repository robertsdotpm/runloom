# Stage 3 — weak memory (iRC11 / gpfsl): the honest ceiling

Stages 1–2 (`OneShotWake.v`, `WakeQueue.v`) are **sequentially-consistent**
Iris: HeapLang has an SC operational semantics. The genuine weak-memory tier —
proving pygo's release/acquire fences correct under **RC11** in a separation
logic — requires **iRC11** (the relaxed-memory logic built on Iris, shipped as
the `gpfsl` development). This file records exactly what that proof would
establish, what is *already* machine-checked without it, and why this is the
boundary.

## The target property: the netpoll commit publish

`pygo_pump_*` (the claimer) publishes the readiness and then releases
`pool->lock`; the aborting parker re-acquires `pool->lock` before reading
`ready_mask`:

```
claimer:   *ready_out = mask;            // plain store
           unlock(pool->lock);           // RELEASE
parker:    lock(pool->lock);             // ACQUIRE
           r = *ready_mask;              // must observe `mask`, never stale
```

This is the **message-passing (MP)** shape: data written before a release-store
is visible to a thread that reads the released location with an acquire. The
property is that the parker **never reads a stale `ready_out`** — and, crucially
(verify/litmus §15), that this holds **because of the `pool->lock` round-trip**,
not the commit-CAS acquire alone.

## What is ALREADY machine-checked (no gpfsl needed)

The *fence placement* for exactly this property is verified under RC11 today,
and both run green in `verify/run_verify.sh`:

- **herd7 litmus** (`verify/litmus/`): `commit_cas_then_publish` shows the
  CAS-acquire alone admits a stale read (**Sometimes**); `commit_lock_publish`
  shows the `pool->lock` round-trip forbids it (**Never**). The C11/RC11
  axiomatic model, exhaustive over the litmus shape.
- **GenMC** (`verify/genmc/netpoll_claim.c`): the *whole* claim protocol as
  real C (pthreads + C11 atomics) under RC11, exploring every execution —
  no data race on `ready_out`, value-correct, exactly-once; the `-DBUG_NO_LOCK`
  control reintroduces the race and is caught.

So the weak-memory **correctness of the fences** is established. What iRC11
would add is an **unbounded, compositional separation-logic spec**: a lock that
*carries a resource* across its release/acquire, giving a thread-modular MP
specification that composes with the SC proofs of Stages 1–2 — the analog of
going from herd7/GenMC (the deque fences) to a full Iris proof.

## The iRC11 development that remains (research-scale)

With gpfsl one would, in iRC11's view-based logic:

1. give the lock an iRC11 *release/acquire* spec where `release`/`acquire`
   transfer an invariant-guarded resource (the published `ready_out` with its
   "claimed" token from Stage 1/2);
2. model `ready_out` as an iRC11 **atomic points-to** (`at↦`) with the
   appropriate write/read views;
3. prove the MP theorem: after the parker's acquire-lock, it owns the resource
   the claimer released, hence reads the published value — *no stale read*,
   for any number of claimers, under RC11.

This is a substantial, specialized development (iRC11's atomic-points-to and
view reasoning), on the order of a research artifact — the genuine multi-week
ceiling. It is **not** scaffolded here as an `Admitted` proof, because an
unchecked proof claimed as proven would be dishonest; the boundary is marked
explicitly instead.

## Feasible install path (if/when this is taken up)

The opam resolution is clean (it pins Iris to a dev version, so it should go in
a **separate switch** to avoid disturbing the SC Stages 1–2, which build
against released Iris 4.4):

```sh
opam switch create pygo-weakmem --packages=ocaml-system
opam repo add coq-released https://coq.inria.fr/opam/released
opam repo add iris-dev git+https://gitlab.mpi-sws.org/iris/opam.git
opam install -y coq-gpfsl          # pulls matching iris-dev + stdpp dev
```
gpfsl ships its own MP / message-passing examples (`gpfsl-examples`) that are
the right template to adapt for the commit-publish spec above.
