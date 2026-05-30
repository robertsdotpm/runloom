#!/usr/bin/env bash
# Driver: build bench_server_pygo, sanity-run, then push N to 2,000,000.
# Logs everything (incl. peak RSS via /usr/bin/time -v) to tests_c/n2m.log
# so results survive a stalled interactive result channel.
set -u
cd /home/x/projects/pygo
# Single-instance guard: a second launch no-ops instead of racing the first.
exec 9>/tmp/n2m.lock
flock -n 9 || { echo "another run_n2m holds the lock; exiting" >&2; exit 0; }
LOG=tests_c/n2m2.log
exec > "$LOG" 2>&1
echo "=== run_n2m start $(date -Is) ==="

echo "--- host enablers (idempotent) ---"
# vm.max_map_count is THE binding limit: pygo mmaps ~2 VMAs/goroutine (stack +
# guard page), so ~2M live gs at N=1M need >4M maps; the 1.05M default reboot
# value made mn_go_c fail with ENOMEM at ~500K-1M gs (RSS only ~4GB, 69GB free).
sudo -n sysctl -w vm.max_map_count=16777216
sudo -n sysctl -w fs.nr_open=8388608 net.core.somaxconn=1048576 net.ipv4.tcp_max_syn_backlog=1048576
# client-side ephemeral-port relief: widen range + reuse TIME_WAIT for outgoing.
# (single src-IP -> single listener port means sport is the scarce resource at N>64K)
sudo -n sysctl -w "net.ipv4.ip_local_port_range=1024 65535" net.ipv4.tcp_tw_reuse=1 net.ipv4.tcp_fin_timeout=5
echo "nr_open=$(cat /proc/sys/fs/nr_open) somaxconn=$(cat /proc/sys/net/core/somaxconn) syn_backlog=$(cat /proc/sys/net/ipv4/tcp_max_syn_backlog) max_map_count=$(cat /proc/sys/vm/max_map_count)"
echo "port_range=$(cat /proc/sys/net/ipv4/ip_local_port_range) tw_reuse=$(cat /proc/sys/net/ipv4/tcp_tw_reuse) fin_timeout=$(cat /proc/sys/net/ipv4/tcp_fin_timeout)"

echo "--- flush libvirt loopback nft noise if present (best-effort) ---"
sudo -n nft flush ruleset 2>/dev/null && echo "nft flushed" || echo "nft flush skipped"

echo "--- build bench_server_pygo (3.13t .so) ---"
make -C tests_c bench_server_pygo 2>&1
BR=$?
echo "make rc=$BR"
ls -la tests_c/bench_server_pygo 2>&1
if [ $BR -ne 0 ] || [ ! -x tests_c/bench_server_pygo ]; then
    echo "BUILD FAILED — aborting"; echo "=== run_n2m end $(date -Is) ==="; exit 1
fi

run_n () {
    local NN=$1 HH=$2 RR=$3 TMO=${4:-600}
    echo ""
    echo "################ RUN N=$NN H=$HH RAMP=$RR (timeout ${TMO}s) @ $(date -Is) ################"
    timeout "$TMO" /usr/bin/time -v sudo -n env PYGO_PER_G_TSTATE=0 tests_c/bench_server_pygo "$NN" "$HH" "$RR" 2>&1
    echo "---- exit rc=$? (124=timeout) ----"
    # cool-down so TIME_WAIT/ports recycle before the next, heavier run
    sleep 8
    echo "post-run TIME_WAIT≈$(ss -tan state time-wait 2>/dev/null | wc -l) estab≈$(ss -tan state established 2>/dev/null | wc -l)"
}

echo "--- milestone reproduce N=1048576 (now with max_map_count raised) ---"
run_n 1048576 8 1 600

echo "--- TARGET N=2000000 RAMP=1 (milestone methodology) ---"
run_n 2000000 8 1 600

echo "=== run_n2m end $(date -Is) ==="
