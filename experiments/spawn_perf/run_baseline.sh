#!/bin/sh
set -e
here=$(cd "$(dirname "$0")" && pwd)
N=${N:-300000}
# 1) no arena (the original wall)
env RUNLOOM_STACK_ARENA= "$here/sweep.sh" baseline_noarena --n "$N"
# 2) arena (current committed best)
env RUNLOOM_STACK_ARENA=1 "$here/sweep.sh" arena --n "$N"
# 3) arena + keep_resident shim
RL_KR="$here/../../tools/keep_resident/runloom-keep-resident"
"$RL_KR" env RUNLOOM_STACK_ARENA=1 "$here/sweep.sh" arena_keepres --n "$N"
echo "ALL BASELINES DONE"
