#!/usr/bin/env bash
# attach.sh -- attach an interactive gdb to the running server (or client),
# with the CPython gdb helpers loaded so `py-bt`, `py-list`, `py-up` work.
#
#   ./attach.sh            attach to the server
#   ./attach.sh client     attach to the burst client
#   ./attach.sh <pid>      attach to an explicit pid
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYGDB="${PYGDB:-$HOME/.pyenv/versions/3.13.13t/bin/python3.13-gdb.py}"

arg="${1:-server}"
case "$arg" in
  server) pid=$(cat "$HERE/run/server.pid" 2>/dev/null) ;;
  client) pid=$(cat "$HERE/run/client.pid" 2>/dev/null) ;;
  *)      pid="$arg" ;;
esac
[ -n "${pid:-}" ] || { echo "no pid for '$arg'"; exit 1; }
echo "attaching gdb to pid $pid  (try: thread apply all py-bt | bt | info threads)"
exec gdb -q \
    -ex "set pagination off" \
    -ex "set debuginfod enabled off" \
    -ex "source $PYGDB" \
    -ex "attach $pid"
