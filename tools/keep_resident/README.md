# keep_resident — the +2× spawn shim (keep mimalloc memory resident)

A tiny `LD_PRELOAD` shim that no-ops `madvise(MADV_DONTNEED/MADV_FREE)`, so
freed heap pages stay **resident** instead of being returned to the OS — Go's
keep-resident strategy.

## Why

On a spawn-heavy free-threaded (3.13t) runloom workload, CPython's mimalloc QSBR
collector purges a heap segment **~once per fiber completion**
(`madvise(MADV_DONTNEED)`). Each purge broadcasts a **TLB-shootdown IPI to every
hub CPU** (`smp_call_function_many → flush_tlb_mm_range`). That dominates spawn
CPU (~32%) and caps parallel scaling. It is **not** a runloom-poolable allocation
(five pooling candidates were measured no-ops — see `docs/dev/spawn_cost.md`); it
is intrinsic to CPython's per-fiber object lifecycle under free-threading. The
only runtime lever is to stop the purge.

## Measured effect (8 hubs, cores 16–23, per-size arena on)

| | 8-issuer spawn/s |
|---|---|
| arena only | ~245k |
| arena + keep_resident | **~500k (~2×)** |

Scaling also tightens (105 → 337 → 515k @ 1/4/8 issuers) because the cross-hub
IPI storm is what capped it.

## Tradeoff

Same as Go's: you hold memory instead of giving it back. **RSS is higher** — the
heap does not shrink back to the kernel until the process exits. Use **only** for
spawn-churn-heavy, RSS-tolerant workloads; do **not** use under long-lived,
memory-constrained servers.

## Use

```sh
tools/keep_resident/runloom-keep-resident env PYTHON_GIL=0 PYTHONPATH=src python3 app.py
```

The wrapper builds `keep_resident.so` on first use (the `.so` is gitignored). Or
build + preload manually:

```sh
cc -O2 -shared -fPIC -o keep_resident.so keep_resident.c -ldl
LD_PRELOAD=$PWD/keep_resident.so PYTHON_GIL=0 python3 app.py
```

## Clean alternative (no LD_PRELOAD)

Rebuild CPython with mimalloc `mi_option_purge_delay = -1`. The
`MIMALLOC_PURGE_DELAY` env is **ignored** by CPython's vendored mimalloc and
`mi_option_set` is not an exported symbol, so this shim is the only **runtime**
way to get the same effect.
