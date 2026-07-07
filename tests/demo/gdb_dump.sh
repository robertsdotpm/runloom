#!/usr/bin/env bash
# gdb_dump.sh -- extract C + Python backtraces from a runloom process.
#
#   gdb_dump.sh core <corefile>   analyse a core dump
#   gdb_dump.sh pid  <pid>        attach to a live (e.g. wedged) process
#
# Used by supervisor.sh for autonomous diagnosis, and by hand to attach a
# debugger to catch errors interactively (see also attach.sh).
set -u
PYBIN="${PYBIN:-$HOME/.pyenv/versions/3.14.4t/bin/python3.13t}"
PYGDB="${PYGDB:-$HOME/.pyenv/versions/3.14.4t/bin/python3.13-gdb.py}"

mode="${1:-}"
target="${2:-}"

common=(-batch -nx
        -ex "set pagination off"
        -ex "set debuginfod enabled off"
        -ex "source $PYGDB")

case "$mode" in
  core)
    gdb "${common[@]}" \
        -ex "thread apply all bt 24" \
        -ex "echo \n===== PYTHON STACKS =====\n" \
        -ex "thread apply all py-bt" \
        "$PYBIN" "$target" 2>&1
    ;;
  pid)
    gdb "${common[@]}" \
        -ex "attach $target" \
        -ex "thread apply all bt 24" \
        -ex "echo \n===== PYTHON STACKS =====\n" \
        -ex "thread apply all py-bt" \
        -ex "detach" \
        2>&1
    ;;
  *)
    echo "usage: $0 {core <file>|pid <pid>}" >&2
    exit 2
    ;;
esac
