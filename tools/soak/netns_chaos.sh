#!/usr/bin/env bash
# netns_chaos.sh -- run a command inside a private network namespace whose
# loopback carries tc-netem impairment (docs/dev/RELIABILITY_PROGRAM.md R3
# network chaos).  Ages the runtime's retransmit / partial-read / timeout /
# reorder paths that a clean loopback never exercises.
#
# No global root state: `unshare --net --map-root-user` gives a FRESH netns in
# which the caller is root, so `tc`/`ip` on THIS namespace's `lo` need no sudo
# and cannot touch the host network (cribbed from tests/big_100/harness.py's
# --netns).  The impairment vanishes with the namespace when the command exits.
#
# Usage:
#   tools/soak/netns_chaos.sh [loss%] [delay_ms] [jitter_ms] -- <cmd...>
#   # defaults: 1% loss, 50 ms delay, 10 ms jitter, reorder
#
# Example -- a lossy-loopback tcp_churn soak:
#   tools/soak/netns_chaos.sh 1 50 10 -- \
#     python3 tools/soak/soak.py --workload tcp_churn --minutes 30 --workers 2
#
# Requires: unshare (util-linux), ip + tc (iproute2), a kernel with netem
# (sch_netem).  Degrades with a clear message if unavailable.
set -u

LOSS="${1:-1}"; DELAY="${2:-50}"; JITTER="${3:-10}"
shift || true; shift || true; shift || true
[ "${1:-}" = "--" ] && shift
[ $# -ge 1 ] || { echo "usage: netns_chaos.sh [loss%] [delay_ms] [jitter_ms] -- <cmd...>"; exit 2; }

command -v unshare >/dev/null 2>&1 || { echo "netns_chaos: unshare missing -- SKIP (running cmd on plain loopback)"; exec "$@"; }
command -v tc >/dev/null 2>&1 || { echo "netns_chaos: tc missing -- SKIP"; exec "$@"; }

# Inside the new netns: bring up lo, attach netem to it, then exec the command.
# --map-root-user makes us root in the ns so tc/ip on lo need no privilege.
exec unshare --net --map-root-user -- bash -c '
  set -e
  LOSS="$1"; DELAY="$2"; JITTER="$3"; shift 3
  ip link set lo up
  # netem on loopback: loss, delay+jitter, and 25% reorder (packets that beat
  # their delayed predecessors) -- the full "unreliable path" shape.
  if tc qdisc add dev lo root netem \
        loss "${LOSS}%" delay "${DELAY}ms" "${JITTER}ms" 25% 2>/dev/null; then
    echo "netns_chaos: lo netem loss=${LOSS}% delay=${DELAY}ms±${JITTER}ms reorder=25%"
  else
    echo "netns_chaos: netem attach failed (no sch_netem?) -- clean loopback"
  fi
  exec "$@"
' bash "$LOSS" "$DELAY" "$JITTER" "$@"
