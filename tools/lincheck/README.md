# lincheck -- linearizability checking for runloom channels

A real concurrent send/recv/close history from an M:N run, checked against the
sequential FIFO-channel spec with [Porcupine](porcupine) -- proving the channel
API linearizes (every concurrent execution is equivalent to *some* legal
sequential one).

`check_lin.sh` runs the whole pipeline:
1. **record** a concurrent history from a real M:N run -- twice: once with plain
   `recv` consumers, once with `select()` consumers (`record_history.py`);
2. **check** both against the FIFO-channel spec with Porcupine (expect
   `LINEARIZABLE`). The `select` run proves select-recv linearizes identically to
   `recv` while driving `chan.c`'s multi-waiter Phase-2 path;
3. **teeth**: corrupt the history (phantom delivery) and re-check (expect NOT
   LINEARIZABLE) -- so the checker is known to be able to fail;
4. the stateful Hypothesis model of the channel API (`stateful_chan.py`:
   send/recv/close + a genuine two-channel `select` rule).

```sh
tools/lincheck/check_lin.sh
# Env: PYTHON=<interp> (default: free-threaded 3.13t if present)
```

Part of `scripts/check_all.sh` (the `lincheck` phase). The deque/select/park-wake
*algorithms* are additionally machine-proven in `../../verify/`; lincheck checks
the assembled channel as a black box on the real binary.
