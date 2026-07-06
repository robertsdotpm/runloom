#!/usr/bin/env bash
# run_capped.sh <program.py> [prog args...]  -- run ONE big_100 program in a
# sandbox that CANNOT take down the VM, no matter how it misbehaves.  Built
# because a tiny-file storm once filled the box's disk: a program that gets
# KILLED (timeout/OOM/crash) never runs its shutil.rmtree cleanup, so its temp
# files leak and accumulate until /tmp (bytes OR inodes) is exhausted -> VM dead.
#
# Every conceivable runaway resource is capped by a DIFFERENT mechanism:
#   - disk / inodes (the tmp-spam killer): a private size+inode-capped TMPFS is
#     $TMPDIR + cwd, so make_tmpdir()/tempfile land there; a storm hits ENOSPC on
#     the tmpfs, the real disk is never touched.  A trap UNMOUNTS it on exit --
#     nuking every file instantly EVEN IF the program was SIGKILL'd (its own
#     cleanup can't run then; the launcher's can).
#   - memory: a systemd --user cgroup scope with MemoryMax + MemorySwapMax=0 ->
#     the kernel OOM-kills the PROGRAM, not the VM.
#   - fork/thread bombs: TasksMax on the same scope.
#   - wall clock: RuntimeMaxSec (+ a `timeout` belt).
#   - giant single file (incl. writes OUTSIDE the tmpfs): RLIMIT_FSIZE.
#   - core-dump disk spam: RLIMIT_CORE=0.  cpu runaway: RLIMIT_CPU.  fds: a high
#     but FINITE RLIMIT_NOFILE (big_100 needs many; a runaway can't get infinite).
#
# Defaults leave the VM real headroom (a legit 1M-conn run still fits); override
# via env: CAP_MEM, CAP_TASKS, CAP_TMPFS, CAP_INODES, CAP_FSIZE, CAP_TIMEOUT,
# CAP_NOFILE, CAP_CPU.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.13.13t/bin/python3}"
PROG="${1:?usage: run_capped.sh <program.py> [args...]}"; shift || true
[ -f "$PROG" ] || PROG="$HERE/$PROG"
[ -f "$PROG" ] || { echo "no such program: $PROG"; exit 2; }

# --- VM-safe defaults (leave headroom; a runaway can be big but not TOTAL) ----
TOTAL_MB="$(awk '/MemTotal/{print int($2/1024)}' /proc/meminfo)"
CAP_MEM="${CAP_MEM:-$(( TOTAL_MB * 70 / 100 ))M}"   # 70% RAM -> 30% stays for VM
CAP_TASKS="${CAP_TASKS:-4096}"                       # hubs+offload+subprocs, not fibers
CAP_TMPFS="${CAP_TMPFS:-4G}"                          # tmp storm caps here, not on disk
CAP_INODES="${CAP_INODES:-2000000}"                  # tiny-file storm caps on inodes too
CAP_FSIZE="${CAP_FSIZE:-2147483648}"                 # 2 GiB max single file (bytes)
CAP_TIMEOUT="${CAP_TIMEOUT:-300}"                    # seconds
CAP_NOFILE="${CAP_NOFILE:-2000000}"                  # high but finite
CAP_CPU="${CAP_CPU:-1200}"                            # cpu-seconds

STAMP="$$_$(date +%s 2>/dev/null || echo x)"
SANDBOX="/tmp/big100_cap_$STAMP"
mkdir -p "$SANDBOX" || { echo "cannot make sandbox dir"; exit 2; }

# --- private size+inode-capped tmpfs (the structural tmp-spam cap) -----------
MOUNTED=0
if sudo -n mount -t tmpfs -o "size=$CAP_TMPFS,nr_inodes=$CAP_INODES,mode=0777" \
        tmpfs "$SANDBOX" 2>/dev/null; then
    MOUNTED=1
else
    echo "WARN: could not mount capped tmpfs (need passwd-less sudo) -- falling"
    echo "      back to a plain dir + RLIMIT_FSIZE; tmp-COUNT spam is NOT capped."
fi

cleanup() {
    # ALWAYS runs (EXIT trap), even after the child was SIGKILL'd -> its leaked
    # temp files vanish with the unmount.  Lazy umount as a last resort.
    if [ "$MOUNTED" = "1" ]; then
        sudo -n umount "$SANDBOX" 2>/dev/null || sudo -n umount -l "$SANDBOX" 2>/dev/null
    fi
    rmdir "$SANDBOX" 2>/dev/null || rm -rf "$SANDBOX" 2>/dev/null
}
trap cleanup EXIT INT TERM

echo "== run_capped: $(basename "$PROG") =="
echo "   mem<=$CAP_MEM tasks<=$CAP_TASKS tmpfs=$CAP_TMPFS(inodes=$CAP_INODES) fsize<=$((CAP_FSIZE/1024/1024))M timeout=${CAP_TIMEOUT}s"

# fd cap: RAISING the hard NOFILE needs privilege (unprivileged prlimit can only
# lower it), and big_100 legitimately needs tens of thousands of fds -- so raise
# it best-effort via sudo on the launcher (children inherit); it is finite either
# way, which is all the safety cap requires.  fsize/core/cpu below are LOWERINGS,
# allowed unprivileged, so they go in the per-run prlimit.
sudo -n prlimit --pid $$ --nofile="$CAP_NOFILE:$CAP_NOFILE" 2>/dev/null || true
INNER="prlimit --fsize=$CAP_FSIZE --core=0 --cpu=$CAP_CPU -- \
  env TMPDIR=$SANDBOX TMP=$SANDBOX TEMP=$SANDBOX HOME=$SANDBOX PYTHON_GIL=0 PYTHONPATH=$ROOT/src \
  $PY $PROG $*"

rc=0
if command -v systemd-run >/dev/null 2>&1 && \
   systemd-run --user --scope --quiet -p Description=big100cap /bin/true >/dev/null 2>&1; then
    # kernel-enforced memory/task/time cgroup + our prlimits + timeout belt
    timeout -k 10 "$((CAP_TIMEOUT + 30))" \
      systemd-run --user --scope --quiet \
        -p "MemoryMax=$CAP_MEM" -p "MemorySwapMax=0" -p "TasksMax=$CAP_TASKS" \
        -p "RuntimeMaxSec=$CAP_TIMEOUT" \
        -- bash -c "cd '$SANDBOX'; exec $INNER"
    rc=$?
else
    echo "WARN: systemd-run --user unavailable -- memory/task caps OFF (prlimit+tmpfs only)"
    timeout -k 10 "$CAP_TIMEOUT" bash -c "cd '$SANDBOX'; exec $INNER"
    rc=$?
fi

case $rc in
  0)   echo "== OK (rc=0) ==" ;;
  124|137) echo "== CAPPED/KILLED (rc=$rc: timeout or OOM/Tasks kill -- VM protected) ==" ;;
  *)   echo "== exited rc=$rc ==" ;;
esac
exit $rc
