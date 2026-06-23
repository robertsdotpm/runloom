#!/usr/bin/env bash
# Run the Python-goroutine scale bench with the fd ceiling raised.
#
# Every fresh shell on this box reverts RLIMIT_NOFILE to a hard cap of 4096
# (the launcher resets it; it can't be raised from inside without privilege),
# which strangles any run past ~2000 connections with EMFILE. This wrapper
# raises its OWN shell's hard+soft nofile via `sudo prlimit --pid $$` and then
# execs the bench, which inherits the high limit. No root for the bench itself.
#
# Usage: tests_c/scale_bench.sh N [HUBS] [M] [IDLE_S]
set +e
ROOT="/home/x/projects/pygo-big100"
PY="/home/x/.pyenv/versions/3.13.13t/bin/python3"

# Raise this shell's fd ceiling (idempotent; needs passwordless sudo).
sudo -n prlimit --pid $$ --nofile=8388608:8388608 2>/dev/null

exec env RUNLOOM_SYSMON_QUIET=1 PYTHON_GIL=0 PYTHONPATH="$ROOT/src" \
    "$PY" "$ROOT/tests_c/bench_server_py.py" "$@"
