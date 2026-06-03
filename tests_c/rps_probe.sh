#!/usr/bin/env bash
# Steady-state requests/sec: many round-trips per connection (M>>1) so the
# 2M-connection setup cost is amortized and the K/s figure reflects echo
# request throughput, not connection establishment. (The N=2M scale run uses
# M=1, so its rate is connection-bound — different metric.)
set -u
cd /home/x/projects/runloom
exec > tests_c/rps.log 2>&1
# args to bench_server_runloom: N H M   (M = round-trips per connection)
for cfg in "20000 8 200" "20000 16 200" "20000 32 200" "100000 16 100"; do
    echo "### bench_server_runloom $cfg   (N H M; requests = N*M)"
    timeout 300 sudo -n env RUNLOOM_PER_G_TSTATE=0 tests_c/bench_server_runloom $cfg 2>&1
    echo "---- rc=$? ----"
done
echo "=== rps_probe done $(date -Is) ==="
