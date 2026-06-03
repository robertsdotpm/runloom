#!/usr/bin/env sh
# perf c2c -- cache-line contention / false-sharing detector for the M:N
# scheduler's lock-free structures (ready ring, wake_list head, the
# per-hub/per-sched fields that several hub threads hammer).
#
# !! CANNOT RUN ON THIS VM !! perf c2c needs PEBS memory-access events
# (mem-loads / mem-stores), which require a real hardware PMU. This box is
# virtualized with no PMU (campaign finding F5), so this script is shipped
# for a bare-metal run, not executed here. On metal it will show, per
# cache line, the HITM (hit-modified) remote-access count that is the
# signature of false sharing -- exactly what you want when chasing why M:N
# throughput plateaus (F2) or why a hot atomic costs more than its op.
#
# Usage (on bare metal, as root or perf_event_paranoid<=1):
#   bench/profile/perfc2c.sh mn --n 256 --iter 2000 --hubs 16 --reps 4
#
# Then read: the "Shared Data Cache Line Table" -- lines with high
# %HITM and contended offsets from different pygo_sched fields are the
# false-sharing suspects; pad/align those to a cache line.
set -eu
WORKLOAD="${1:-mn}"
[ "$#" -gt 0 ] && shift || true
PY="${PYTHON:-python3}"
DATA="$(mktemp /tmp/pygo_c2c.XXXXXX).data"

if ! perf mem record -e list 2>/dev/null | grep -q .; then
    cat >&2 <<'MSG'
perf c2c/mem events unavailable -- this needs a hardware PMU with PEBS
(bare metal). On the campaign VM these read <not supported> (finding F5).
Run this script on a bare-metal x86 box instead.
MSG
fi

perf c2c record -o "$DATA" -- \
    env PYTHONPATH=src PYTHON_GIL=0 "$PY" -m bench.profile.run_workload \
    "$WORKLOAD" "$@" --quiet
perf c2c report -i "$DATA" --stdio
echo "(raw: $DATA)"
