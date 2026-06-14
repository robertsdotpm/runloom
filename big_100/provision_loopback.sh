#!/bin/bash
# provision_loopback.sh -- provision a big block of 127/8 loopback aliases on
# macOS so big_100 socket sweeps can give each program a FRESH IP window
# (sweep_mac.sh uses --ip-start-offset 8k..8k+7 per program).  Fresh dst IPs
# mean a closed connection's 4-tuple never collides with the next program's via
# TIME_WAIT -- the root cause of false HANGs in back-to-back socket-storm runs.
#
# macOS does NOT auto-route 127.0.0.0/8 (unlike Linux): every non-127.0.0.1
# loopback address needs an explicit `ifconfig lo0 alias`.  This aliases the
# offsets the harness' _ip_for_offset() maps to (offset o -> 127.((o+1)>>16 &
# 255).((o+1)>>8 & 255).((o+1) & 255)), for o in [0, COUNT).
#
# Also shortens net.inet.tcp.msl so TIME_WAIT drains in ~2s instead of ~30s.
# Both are runtime-only (lost on reboot) -- re-run after a reboot.
#
# Usage: provision_loopback.sh [count]   (default 800 -> covers 99 progs * 8)
set +e
COUNT="${1:-800}"
echo "provisioning $COUNT lo0 aliases (127.0.0.1 .. offset $COUNT)..."
sudo -n sh -c "for o in \$(seq 0 $((COUNT-1))); do v=\$((o+1)); ifconfig lo0 alias 127.\$(((v>>16)&255)).\$(((v>>8)&255)).\$((v&255)) 2>/dev/null; done"
echo "lo0 127.x alias count: $(ifconfig lo0 | grep -c 'inet 127')"
sudo -n sysctl -w kern.ipc.somaxconn=200000 net.inet.tcp.msl=1000 2>&1 | tr '\n' ' '; echo
echo "done.  (re-run after reboot)"
