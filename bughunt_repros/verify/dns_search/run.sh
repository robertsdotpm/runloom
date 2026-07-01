#!/bin/sh
# Runs INSIDE `unshare -r -n -m`: private netns + mountns, fake root.
set -e
D=/tmp/claude-1000/-home-x-projects-nat-simulator/d7b7a911-918e-435e-af6a-ee2aacf6c59d/scratchpad/repros/verify/dns_search
PY=/tmp/claude-1000/-home-x-projects-nat-simulator/d7b7a911-918e-435e-af6a-ee2aacf6c59d/scratchpad/pygo/.venv/bin/python

ip link set lo up
mount --bind "$D/fake_resolv.conf" /etc/resolv.conf

: > "$D/queries.log"
"$PY" "$D/dnsserver.py" "$D/queries.log" &
SRV=$!
sleep 0.5

echo "--- resolv.conf in namespace ---"
cat /etc/resolv.conf
echo "--- stock python (glibc getaddrinfo) ---"
timeout 15 "$PY" "$D/client_stock.py" || true
echo "--- runloom monkey.patch()ed getaddrinfo ---"
timeout 15 "$PY" "$D/client_runloom.py" || true
echo "--- queries the fake nameserver received ---"
cat "$D/queries.log"
kill $SRV 2>/dev/null || true
