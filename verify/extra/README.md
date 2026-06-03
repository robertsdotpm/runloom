# Additional verification engines — runnable harnesses + roadmap

The core suite (`verify/run_verify.sh`) now runs, in one pass:

| engine | what it checks | status |
|--------|----------------|--------|
| **Spin** (safety) | every interleaving of each lock-free primitive (SC) | integrated |
| **Spin** (liveness) | non-starvation under weak fairness; lock-free progress | **added** (`spin/live_*.pml`) |
| **CBMC** | the real `cldeque.c` with its `__atomic_*` orders | integrated |
| **herd7** | C11/RC11 fence placement on the netpoll/wake paths | integrated |
| **GenMC** | the real claim protocol as C under RC11 | integrated |
| **TLC (TLA+)** | the **composed** M:N scheduler, emergent no-lost-goroutine | **added** (`tla/`) |
| **Alloy** | the netpoll parker-graph structural invariant (`self_check`) | **added** (`alloy/`) |

This directory holds harnesses for engines that need a tool not present in
this environment (no passwordless sudo / not in apt), plus the roadmap for the
heavy machine-checked proofs. Each script **skips cleanly** when its tool is
absent, and runs the real check when installed.

## Runnable once the tool is installed

- **`rr_chaos.sh`** — Mozilla **rr** `record --chaos` over a runloom workload, with
  perfect deterministic replay + reverse execution. The fastest way to capture
  *and* root-cause the residual cross-file leaked-parker flake on the real M:N
  path. `sudo apt-get install rr` (+ `kernel.perf_event_paranoid<=1`, ptrace;
  usually needs a real host, not a container).

- **`nidhugg.sh`** — **Nidhugg**, a second stateless model checker (LLVM,
  source-/optimal-DPOR) on the *same* `genmc/netpoll_claim.c`. An
  algorithmically-distinct confirmation of GenMC, and adds TSO/PSO. Build from
  source against LLVM/clang (https://github.com/nidhugg/nidhugg).

## Roadmap — heavier, higher-assurance engines

Ordered by leverage. Each lists the runloom target, what it would prove beyond
the current suite, and the install.

1. **Ivy — parameterized / unbounded.** Re-express the wake/steal protocol so
   correctness is proven for an *arbitrary* number of thieves/wakers/hubs, not
   the current bounded `2`. Directly answers the README's "but they are bounds"
   caveat via an inductive invariant (EPR-decidable). `pip install ms-ivy`
   (z3-backed). Target: `wake_state` + `hub_submit` as a parameterized system;
   prove no-lost-wake / exactly-once for all N.

2. **CDSChecker** — C11 stateless model checker. Third independent weak-memory
   angle on `cldeque.c` and the netpoll claim, complementary to GenMC/Nidhugg.
   Build from source (http://plrg.eecs.uci.edu/software_page/42-2/).

3. **KLEE — symbolic execution of the straight-line C the README leaves
   unmodelled.** Systematically covers the deadline min-heap (sift-up/down,
   arbitrary-remove via `heap_index`) and the per-fd list surgery — pure
   `pool->lock`-serialised code with no concurrency, so symbolic path coverage
   (not interleaving) is the right tool. Targets: `runloom_netpoll` heap ops in
   `netpoll.c`. Needs LLVM bitcode + KLEE (https://klee.github.io).

4. **PRISM / Storm — quantitative.** Model the scheduler as a CTMC/MDP and
   verify *probabilistic* bounds: expected wake latency, P(livelock) < ε under
   randomized scheduling. Pairs with the connection-dynamics RTT/jitter model.
   `apt-get install prism` or build Storm.

5. **Iris / FSL++ (Coq) — machine-checked, unbounded.** The ceiling: a fully
   general, machine-checked proof (no bounds) of the two load-bearing
   algorithms — the Chase-Lev deque and the netpoll commit protocol — in a
   weak-memory separation logic (FSL++/GPS handle release/acquire + fences).
   Both have precedent in the literature, so it is following, not inventing.
   `opam install coq coq-iris`. This is the multi-week item; everything above
   is days.

## Why these are not in `run_verify.sh`

`run_verify.sh` is meant to run green on a stock checkout with `spin`, `cbmc`,
and (optionally) `herd7`/`genmc`/`java`. The engines here either need a tool
that is not apt-installable without a dev toolchain (Nidhugg, CDSChecker, KLEE,
Iris), need host privileges (rr), or are a research-scale effort (Iris). They
are kept runnable-on-demand so adding the tool is the only step.
