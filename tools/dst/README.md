# dst -- Deterministic Simulation Testing

`dst.py` drives REAL runloom channels / `select` on the single-thread cooperative
scheduler (`runloom_c.go` + `run`), which is deterministic: for a fixed set of
goroutines making fixed yield decisions the run-queue order is fixed, so the whole
execution is reproducible. A **seeded decision oracle** chooses *where* each
goroutine yields (`runloom_c.sched_yield`) -- a different seed explores a different
interleaving, the SAME seed reproduces an execution exactly. So a failing run
reduces to a single integer seed (unlike a raw flake).

Two pluggable scheduling strategies:
- **`UniformYield(p)`** -- yield at each decision point with probability `p`
  (classic randomized interleaving; many preemptions).
- **`PCTBounded(d)`** -- PCT-style: pick `d-1` preemption step indices up front
  from the calibrated horizon and yield ONLY there (depth-`d` bug guarantee).

```sh
python tools/dst/dst.py                 # sweep seeds
python tools/dst/dst.py --seed 12345    # reproduce one execution exactly
```

Part of `scripts/check_all.sh` (the `dst` phase). See also `tools/pct/`
(PCT for the single hub) and `tools/mn_controlled/` (the M:N analogue with a
baton-gated replay).
