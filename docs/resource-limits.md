# Resource limits & kernel tuning

runloom is built to run **hundreds of thousands to millions** of fibers and
connections in one process. At that scale you will hit **OS limits** long before
you hit a runloom limit. None of these are runloom bugs — they are kernel/ulimit
ceilings that any high-concurrency runtime (Go, nginx, an event loop) must raise
too. This page lists every limit that matters, why, and exactly how to raise it.

> **TL;DR for large-N:** raise `vm.max_map_count` and `RLIMIT_NOFILE` first —
> those two bite before anything else.

---

## The four limits that matter

### 1. `vm.max_map_count` — the one that bites first

Each fiber's stack is mapped as a guard page (`PROT_NONE`) plus a usable region —
**~2 VMAs (memory-map entries) per fiber**. The kernel caps the number of
mappings a process may hold at `vm.max_map_count` (**default 65530**), so you run
out of *mappings* at roughly **~32,000 live fibers** on a stock kernel — far
before you run out of RAM. Exceeding it surfaces as `mmap` failing with `ENOMEM`
(not `EMFILE`), often looking like a hang or a stall around ~32k/500k fibers.

```sh
# allow ~2 VMAs per fiber, plus headroom; this is for ~1M fibers
sudo sysctl -w vm.max_map_count=4000000
# persist:
echo 'vm.max_map_count=4000000' | sudo tee /etc/sysctl.d/99-runloom.conf
```

Rule of thumb: **`vm.max_map_count >= 2 × peak_live_fibers + slack`**.

This is also the limit that **`prewarm()` and a raised `RUNLOOM_STACK_DEPOT_CAP`
push against** — pooled/prewarmed stacks each hold their VMAs even when idle (see
[Tuning knobs](#runloom-tuning-knobs)). So if you prewarm 200k stacks, budget
`max_map_count` for them too.

### 2. `RLIMIT_NOFILE` — open file descriptors

Every socket, pipe, and file is an fd. A server with N live connections needs
**at least N fds** (plus a fixed ~100–150 floor for the scheduler / netpoll /
blocking-offload pool). The default soft limit (often **1024**) caps you almost
immediately; exceeding it is `EMFILE` ("Too many open files"), which looks like
connections silently failing or a super-linear slowdown.

```sh
# raise THIS process's soft+hard limit (per shell/login):
ulimit -n 1048576
# or for a specific pid (root):
sudo prlimit --pid <PID> --nofile=1048576:1048576
```

For a service, set it in the unit file / launcher:
```ini
# systemd unit
[Service]
LimitNOFILE=1048576
```

> **Dev/CI gotcha:** some IDE/agent shells force a **hard** `RLIMIT_NOFILE` of
> 4096 on every child shell *after* OS policy applies, so `ulimit -n` can't raise
> past it. Raise the shell's ceiling first with
> `sudo prlimit --pid $$ --nofile=1048576:1048576` in the *same* command block.

### 3. `fs.nr_open` — the ceiling on `RLIMIT_NOFILE`

`RLIMIT_NOFILE` can never exceed the kernel ceiling `fs.nr_open` (**default
~1,048,576**). To set a per-process nofile above ~1M, raise this first:

```sh
sudo sysctl -w fs.nr_open=16000000
echo 'fs.nr_open=16000000' | sudo tee -a /etc/sysctl.d/99-runloom.conf
```

### 4. `net.core.somaxconn` — listen backlog (accept storms)

A burst of simultaneous `connect()`s queues in the listen socket's accept
backlog. `TCPConn.listen(host, port, backlog=128)` defaults to 128, and the
effective backlog is clamped to `net.core.somaxconn` (**default 4096** on modern
kernels, 128 on older ones). Under an accept storm, an undersized backlog drops
SYNs (clients see connection resets / timeouts).

```sh
sudo sysctl -w net.core.somaxconn=65535
```
…and pass a matching `backlog=` to `listen()`.

---

## Memory sizing

VMAs and fds run out before RAM, but size memory too:

- **Virtual address space per fiber:** the default stack reservation is **512 KiB**
  (`RUNLOOM_DEFAULT_STACK_SIZE`), but it is *virtual* — only the pages a fiber
  actually touches become resident (it grows down from the top). A trivial fiber
  costs a few KB of RSS, not 512 KB.
- **RSS per fiber (C handler):** ~2–7 KB of touched stack + scheduler bookkeeping.
  The README's measured matrix: **2,000,000 C-handler connections ≈ 14.3 GB,
  ~4 M VMAs.**
- **RSS per fiber (Python handler):** add ~26 KB/fiber for the Python frame /
  datastack — so a Python-per-connection design is RAM-bound well before a C one.
- With the default **`MADV_FREE`** stack reclaim, freed/pooled stack pages stay
  *counted* in RSS until the kernel reclaims them under pressure — so RSS can look
  higher than the live set. Set `RUNLOOM_STACK_MADV=dontneed` for eager reclaim if
  your RSS metrics matter more than spawn/complete CPU.

---

## Runloom tuning knobs

These environment variables interact with the limits above:

| Env var | Default | Effect on limits |
|---|---|---|
| `RUNLOOM_DEFAULT_STACK_SIZE` | `524288` (512 KiB) | bigger stacks → more virtual space + RSS per fiber |
| `RUNLOOM_STACK_DEPOT_CAP` | `1024` | retained pooled stacks → **VMAs held when idle**; raise it (near your peak) only alongside `vm.max_map_count` |
| `RUNLOOM_STACK_MADV` | `free` | `free` = lazy RSS (cheaper CPU); `dontneed` = eager RSS reclaim; `off` = keep resident |
| `prewarm(n, stack_size, background)` | — | pre-maps `n` stacks → consumes `~2n` VMAs up front; needs `vm.max_map_count` + `RUNLOOM_STACK_DEPOT_CAP` budgeted for `n` |

---

## Quick recipe

For a server targeting **1,000,000 fibers / connections**:

```sh
# kernel ceilings (persist in /etc/sysctl.d/99-runloom.conf)
sudo sysctl -w fs.nr_open=16000000
sudo sysctl -w vm.max_map_count=4000000      # ~2 VMAs/fiber + slack
sudo sysctl -w net.core.somaxconn=65535

# per-process fd limit (or LimitNOFILE=1048576 in the systemd unit)
ulimit -n 1048576

# then run; if you prewarm, budget the depot cap + max_map_count for it
RUNLOOM_STACK_DEPOT_CAP=200000 python your_server.py
```

If something stalls or `ENOMEM`s around a few hundred thousand fibers, it is
almost always `vm.max_map_count`. If connections fail with `EMFILE`, it is
`RLIMIT_NOFILE`. Raise those two first.
