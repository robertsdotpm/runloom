# hang-hunter

An autonomous, always-on **stress + fuzz daemon** for the runloom M:N scheduler
that **auto-triages and deduplicates** any hang or crash it finds.

## Why

Our targeted tools each check a specific slice:

- `verify/` — formal models (Spin / CBMC / GenMC / herd7) of small concurrency
  primitives (park/wake, ready-ring, the chan/select state machine);
- `tools/lincheck/` — linearizability of the channel/select operations;
- `tools/dst/` — deterministic simulation;
- `tools/run_sanitizers*.sh` — TSan/ASan over the extension.

None of them caught — and structurally none *could* catch — the stop-the-world
**monopoly deadlock** (`test_gc_stw_under_goroutine_churn`): it was an emergent
*scheduling-fairness* failure that only appears when realistic goroutine churn
drives near-continuous stop-the-world. The hang-hunter exists to find exactly
that class: keep a pool of randomized, realistic workloads running, and the
instant one wedges or crashes, capture a root-cause-ready report.

## What it does

- Runs randomized runloom workloads in parallel, **load-gated** (pauses launching
  when 1-minute load exceeds `--load-frac × cores`, default 0.7) so it never
  fights the CI runner or foreground work; children are `nice`d.
- **HANG** (a job still alive past its timeout): attaches gdb to the *live*
  process and captures every thread's backtrace + the interpreter
  stop-the-world state (`requested` / `world_stopped` / `countdown` / `requester`)
  + each tstate's attach state (DETACHED/ATTACHED/SUSPENDED) + each hub's queue
  snapshot (deque depth, ready-ring depth, pending). That trio fingerprints the
  scheduler failure mode at a glance.
- **CRASH** (nonzero exit / signal): captures the core's all-thread backtrace.
- **Dedup**: every finding is keyed by a backtrace signature, so thousands of
  repeats of one bug collapse to a single report-with-count (`status.txt`), and
  distinct bugs stand out. The first occurrence of each signature gets a full
  report file with a one-line **repro** command.

## Engines

Shipped (run on this box today):

- **stress** — randomized real workloads: `gc_churn` (the stop-the-world churn
  that exposed the monopoly deadlock — kept as a permanent regression hunter) and
  `chan_storm` (parallel producer/consumer over buffered channels). Random hub
  counts / sizes and random scheduler env knobs (sysmon / preempt / handoff on
  and off, world-yield ns — never 0, which would disable a fix).
- **hypo** — Hypothesis-generated *always-terminating* programs (random op
  sequences across random hub counts); any hang is therefore a real bug, and
  Hypothesis shrinks assertion failures to a minimal repro.
- **lifefuzz** — one generative *life-cycle* program from `tools/lifefuzz` per job
  (varied-stack goroutines, channel ref churn, nested spawn/migration, timed parks,
  select+close, undrained buffers). Always-terminating, so a hang is a real lost
  wakeup; a nonzero exit (crash / life-cycle-oracle violation) is a bug. Each job
  pins a `RUNLOOM_MN_SEED` so the daemon's repro replays the exact execution, and
  the worker's internal watchdog is set high so a true wedge reaches the daemon's
  gdb-on-live-process triage.

Auto-selected when the ext is **TSan-built**:

- **lifefuzz-tsan** — the same generative programs under the gold-standard TSan ext
  (`setarch -R` + `LD_PRELOAD=libtsan` + the runloom suppressions); a non-suppressed
  data race exits 86 → CRASH triage. This is the engine that found the deadlock-
  census race cluster (`tools/README.md` Finding D). Because a TSan-linked ext can
  only load with `libtsan` preloaded, this engine **replaces** the normal set when
  the built ext links libtsan — build it via `tools/run_sanitizers_ext.sh`, then run
  the daemon (it auto-detects).

Hooks for engines that need installs not present here:

- **atheris** — coverage-guided fuzzing of the Python API → C (pip; may need a
  build against free-threaded 3.13t).
- **afl / libFuzzer** — a C harness over the deque / chan / select primitives
  (needs clang).

Add one by writing a `*_job(rng, py) -> Job` function in `workloads.py` and
registering it in `ENGINES`.

## Usage

```sh
# from the repo root
python tools/hang_hunter/daemon.py --once 200            # ~200 jobs then exit
python tools/hang_hunter/daemon.py --duration 3600       # hunt for an hour
python tools/hang_hunter/daemon.py --daemon              # until SIGINT/SIGTERM

# flags
#   --engines stress,hypo   which engines to draw jobs from
#   --jobs N                parallel jobs (0 = auto from cores × load-frac)
#   --load-frac 0.7         pause launching above this fraction of cores
#   --report-dir DIR        where reports + status.txt land
#   --python /path          interpreter (default: free-threaded 3.13t)
```

Watch `‹report-dir›/status.txt` for the live tally and distinct findings.

## Requirements

- Live-attach triage needs ptrace: `/proc/sys/kernel/yama/ptrace_scope = 0`
  (or run as root). Without it, hangs are still detected and killed but the
  backtrace will say gdb could not attach.
- Crash backtraces need a writable `core_pattern`; the daemon tries to point it
  at `‹report-dir›/cores/core.%p` via `sudo -n` and prints a hint if it can't.

As-experimental: this is a hunting tool, not part of the build or the suite.
