# faultinj -- syscall / allocation fault injection

A small `LD_PRELOAD` shim (`faultinj.c` + `Makefile`) that fails a chosen
allocation or syscall on the Nth call, plus a compact runloom `workload.py` that
exercises the paths whose cleanup branches the coverage report flagged as
untested: channel send/recv, the single-thread scheduler, the M:N scheduler (hub
threads -> `eventfd`/`epoll`), and a timed park (`timerfd` / deadline heap).

`workload.py` prints `WORKLOAD_OK` on success; any crash or hang under an injected
failure is a cleanup-path bug.

```sh
make -C tools/faultinj                                  # build the shim
# drive it from the sweep harness:
python tools/fault_sweep.py                             # fail each Nth op in turn
```

`tools/fault_sweep.py` is the orchestrator that uses this shim to fail each
allocation/syscall in turn and classify the outcome (OK / GRACEFUL / CRASH /
HANG). The Python-level analogue (fail each runloom-internal alloc site) lives in
`RUNLOOM_FAULT_*` env knobs exercised by `tests/`.
