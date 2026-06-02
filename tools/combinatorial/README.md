# tools/combinatorial — config-matrix interaction testing

pygo's runtime knobs multiply: netpoll backend × P-handoff × preemption ×
sysmon × … . Bugs hide in the *interactions*, not in any single setting, but
the full cartesian product is wasteful to test and one-factor-at-a-time misses
interactions entirely.

`covering.py` builds a **t-way covering array** — a small set of configurations
in which every combination of `t` knob-values appears at least once — and runs
the M:N scheduler fuzzer (`mn_stress`) under each. Empirically most interaction
faults are triggered by ≤2–3 factors, so pairwise/3-way catches them cheaply.

> Kuhn, Wallace, Gallo, *Software Fault Interactions and Implications for
> Software Testing*, IEEE TSE 2004. Cohen et al, AETG (greedy construction).

## Run it

```sh
PY=~/.pyenv/versions/3.13.13t/bin/python3
$PY tools/combinatorial/covering.py --list            # array + coverage stats
$PY tools/combinatorial/covering.py --iters 40        # run each config
$PY tools/combinatorial/covering.py --t 3             # 3-way (stronger, more rows)
$PY tools/combinatorial/covering.py --include-experimental   # + unstable knobs
```

Also `scripts/check_all.sh combo`.

## Supported vs experimental factors

The default (gating) matrix covers only knobs that are *meant* to work, so it
stays a clean regression gate: `PYGO_NETPOLL` (epoll/select/io_uring),
`PYGO_HANDOFF`, `PYGO_PREEMPT`, `PYGO_SYSMON` — 24 cartesian configs reduced to
**7** pairwise, all CLEAN.

`--include-experimental` adds `PYGO_STEAL_WOKEN` and `PYGO_PER_G_TSTATE` (the
known-dead "Fix B" cross-hub-migration path). This is also the tool's own first
success story: the very first pairwise run over the supported set *plus*
`STEAL_WOKEN` immediately isolated a single-factor SIGSEGV — every failing
config had `STEAL_WOKEN=1`, every passing one had `=0`, pointing straight at the
documented live-frame-migration crash. That's interaction testing earning its
keep on day one.

## Candidate for `all`

`combo` is currently opt-in (like `bench`). Because the supported matrix is a
real correctness gate that passes cleanly, it's a good candidate to fold into
`check_all.sh all` once it's been run a few times on the target hardware (it
assumes the Linux netpoll backends are available).
