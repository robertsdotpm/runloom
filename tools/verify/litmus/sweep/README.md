# Fence-order sweep — litmus synthesis for the park/wake fence

`verify/litmus/` ships two hand-written points of the park/wake store-buffering
(SB / Dekker) shape: `parkwake_no_fence` (Sometimes — the bug) and
`parkwake_sc_fence` (Never — the fix). This directory fills in the **whole
lattice between them** and lets herd7 find the weakest sufficient fence
automatically.

`gen_sweep.py` synthesises 12 variants — store ∈ {relaxed, release} × load ∈
{relaxed, acquire} × fence ∈ {none, release, seq_cst}, symmetric on both
threads. `run_sweep.sh` runs each through herd7 (RC11) and reports the result.

```sh
verify/litmus/sweep/run_sweep.sh        # skips cleanly if herd7 absent
```

Validated result (herd7 / RC11):

```
   store     load      fence      observation
   ...
   relaxed   relaxed   scfence    Never      (forbids the lost wakeup)
   relaxed   acquire   nofence    Sometimes  (lost wakeup REACHABLE)
   relaxed   acquire   relfence   Sometimes  (lost wakeup REACHABLE)
   ...
  CONCLUSION: the seq_cst StoreLoad fence is NECESSARY and SUFFICIENT --
  it is the only fence that forbids the lost wakeup, and it does so even
  with relaxed store/load (release/acquire and release fences do not).
```

So the `seq_cst` fence in `runloom_sched_parkwake.c.inc` (`runloom_sched_wake_safe`) carries *all* the ordering: the
lost wakeup is forbidden across the whole lattice iff that fence is present.
This generalizes the two endpoints into a necessity+sufficiency proof.

## Memalloy (the heavyweight alternative)

This generator *enumerates a known template*. Memalloy (Wickerson et al,
POPL'17) instead *searches* with Alloy/SMT for litmus tests that distinguish
two memory models — useful for discovering corner cases you didn't think to
template (e.g. "find an execution where RC11 and IMM disagree on this fence").
Wiring it in needs Alloy + the memalloy framework
(<https://github.com/johnwickerson/memalloy>); point it at the runloom `.cat` /
the RC11 model and harvest distinguishing tests into `generated/`. Deferred
until Alloy is in the toolchain; the enumerative sweep covers the park/wake
question completely in the meantime.
