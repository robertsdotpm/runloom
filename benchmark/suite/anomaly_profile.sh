#!/usr/bin/env bash
# Profile the open anomaly: the Cython handler on epoll is server-bound ~2x lower
# than the Python handler on epoll, despite being zero-PyObject + zero-alloc.
#
# Runs each server on loopback under steady loadgen load, pinned to the server
# cores, and captures (a) perf stat HW counters (IPC, cache + branch misses,
# context-switches) and (b) perf record top functions on the server cores. The
# DIFF between the two profiles is the answer. Loopback (not the veth netns) so
# it can't collide with a running netns bench; the firewall tax is constant
# across both servers so the comparison holds.
#
# Needs sudo (perf + sysctl). Output: results/anomaly_*.txt
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
PY=/home/x/.pyenv/versions/3.13.13t/bin/python3
SRC=/home/x/projects/pygo-bench/src
LOADGEN="$HERE/clients/loadgen"
RES=/home/x/projects/pygo-bench/benchmark/results
SRVCPU=16-59
CLICPU=0-15
CONNS=1024
WINDOW=8

sudo -n sysctl -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0 >/dev/null 2>&1

run_one() {
  local tier="$1" port="$2"; shift 2
  local srv_args=("$@")
  echo "===== tier=$tier port=$port ====="
  # launch server on loopback, pinned to server cores
  PYTHONPATH=$SRC PYTHON_GIL=0 RUNLOOM_DEBUG= taskset -c $SRVCPU \
      $PY "${srv_args[@]}" --host 127.0.0.1 --port "$port" --hubs 44 --token "ANOM_$tier" \
      > "$RES/anomaly_srv_$tier.out" 2>&1 &
  local srvwrap=$!
  # wait for LISTENING
  for _ in $(seq 1 40); do grep -q LISTENING "$RES/anomaly_srv_$tier.out" && break; sleep 0.25; done
  if ! grep -q LISTENING "$RES/anomaly_srv_$tier.out"; then
    echo "  server failed:"; tail -3 "$RES/anomaly_srv_$tier.out"; kill $srvwrap 2>/dev/null; return 1
  fi
  # drive load
  taskset -c $CLICPU "$LOADGEN" -addr "127.0.0.1:$port" -conns $CONNS -payload 1024 \
      -ramp 2 -measure 40 -gomaxprocs 16 > "$RES/anomaly_load_$tier.out" 2>&1 &
  local loadpid=$!
  sleep 4   # past ramp, into steady state
  # HW counters on the server cores
  sudo -n perf stat -C $SRVCPU -e task-clock,instructions,cycles,cache-misses,cache-references,branch-misses,context-switches \
      -- sleep $WINDOW 2> "$RES/anomaly_stat_$tier.txt"
  # top functions on the server cores
  sudo -n perf record -C $SRVCPU -F 499 -g -o "$RES/anomaly_$tier.data" -- sleep $WINDOW >/dev/null 2>&1
  sudo -n perf report -i "$RES/anomaly_$tier.data" --stdio --percent-limit 1 2>/dev/null \
      | grep -E "^\s+[0-9]+\." | head -30 > "$RES/anomaly_top_$tier.txt"
  # teardown
  kill $loadpid 2>/dev/null
  sudo -n pkill -9 -f "ANOM_$tier" 2>/dev/null
  kill $srvwrap 2>/dev/null
  sleep 1
  echo "  $(grep rps "$RES/anomaly_load_$tier.out" 2>/dev/null | tail -1 | head -c 120)"
}

run_one py     19111 "$HERE/servers/runloom_epoll_py_tcpcon.py"
run_one cython 19112 "$HERE/servers/runloom_iouring_cython_tcpcon.py" "--optimize" "none"

echo; echo "===== perf stat diff (py vs cython, server cores, ${WINDOW}s) ====="
paste <(grep -E "instructions|cache-misses|cache-references|branch-misses|context-switches|insn per" "$RES/anomaly_stat_py.txt") \
      <(grep -E "instructions|cache-misses|cache-references|branch-misses|context-switches|insn per" "$RES/anomaly_stat_cython.txt") 2>/dev/null
echo; echo "top funcs: $RES/anomaly_top_{py,cython}.txt"
sudo -n sysctl -w kernel.perf_event_paranoid=2 kernel.kptr_restrict=1 >/dev/null 2>&1
