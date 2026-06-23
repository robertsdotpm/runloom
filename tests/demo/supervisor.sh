#!/usr/bin/env bash
# supervisor.sh -- run the mnweb server + burst client forever, detect
# crashes and hangs, gather core dumps + gdb backtraces into incident
# reports, and restart whatever died.
#
# Detection:
#   crash  -- a child exits with a fatal signal (SEGV/ABRT/BUS/ILL/FPE) or
#             any non-intentional non-zero code.  The in-process runloom
#             crash handler has already written a report + a core; we attach
#             gdb to the core for C + Python backtraces.
#   hang   -- server: /health stops answering AND run/health.json goes stale
#             (the heartbeat goroutine stopped) for HANG_STRIKES polls.
#             client: its log stops growing for CLIENT_STALL_S.
#             We `kill -QUIT` (raw goroutine dump to the log), snapshot live
#             gdb backtraces, then bounce the process.
#
# Each incident is a markdown file in run/incidents/ and the flag file
# run/NEW_INCIDENT is touched so a watcher (Claude) can notice and dive in.
set -u

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="$(cd "$HERE/../src" && pwd)"
RUN="$HERE/run"
INC="$RUN/incidents"
CORES="$RUN/cores"

PY="${PY:-$HOME/.pyenv/versions/3.13.13t/bin/python3.13t}"
export PYTHON_GIL=0
export PYTHONPATH="$SRC"
# goroutine dump + native backtrace, then chain to SIG_DFL -> core + die.
# (Including 'py'/'wait'/'gdb' makes a fault wedge instead of coring under M:N.)
export RUNLOOM_CRASH="${RUNLOOM_CRASH:-goroutine,backtrace}"

SERVER_PORT="${SERVER_PORT:-8080}"
SERVER_HUBS="${SERVER_HUBS:-4}"
CLIENT_HUBS="${CLIENT_HUBS:-4}"
CLIENT_BURST="${CLIENT_BURST:-100}"
CLIENT_INTERVAL="${CLIENT_INTERVAL:-60}"

HEALTH_INTERVAL="${HEALTH_INTERVAL:-3}"     # poll cadence (s)
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-4}"       # per-probe curl timeout (s)
HANG_STRIKES="${HANG_STRIKES:-3}"           # consecutive bad polls -> hang
HEARTBEAT_STALE="${HEARTBEAT_STALE:-12}"    # health.json older than this == wedged
CLIENT_STALL_S=$(( CLIENT_INTERVAL * 2 + 45 ))
KEEP_CORES="${KEEP_CORES:-3}"

SRV_LOG="$RUN/server.log";  SRV_PID="$RUN/server.pid";  SRV_EXIT="$RUN/server.exit"
CLI_LOG="$RUN/client.log";  CLI_PID="$RUN/client.pid";  CLI_EXIT="$RUN/client.exit"
SRV_INTENT="$RUN/server.intentional"
CLI_INTENT="$RUN/client.intentional"

mkdir -p "$RUN" "$INC" "$CORES"

log() { printf '%s  %s\n' "$(date -Is)" "$*" | tee -a "$RUN/supervisor.log"; }

setup_system() {
    ulimit -c unlimited
    sudo sysctl -w "kernel.core_pattern=$CORES/core.%e.%p.%t" >/dev/null 2>&1 \
        && log "core_pattern -> $CORES/core.%e.%p.%t" || log "WARN: could not set core_pattern"
    sudo sysctl -w kernel.yama.ptrace_scope=0 >/dev/null 2>&1 \
        && log "ptrace_scope -> 0 (gdb attach enabled)" || log "WARN: could not set ptrace_scope"
}

rotate_cores() {
    local extra
    extra=$(ls -t "$CORES"/core.* 2>/dev/null | tail -n +$((KEEP_CORES + 1)))
    [ -n "$extra" ] && { echo "$extra" | xargs -r safe-rm -f 2>/dev/null || echo "$extra" | xargs -r rm -f; }
}

# spawn <pidfile> <exitfile> <logfile> -- then the command.
# Records the child's real pid (pidfile) and, on death, its exit code
# (exitfile).  Zombie-proof liveness check = "exitfile does not exist".
spawn() {
    local pidf="$1" exitf="$2" logf="$3"; shift 3
    rm -f "$pidf" "$exitf"
    (
        "$@" >>"$logf" 2>&1 &
        local cpid=$!
        echo "$cpid" >"$pidf"
        wait "$cpid"
        echo "$?" >"$exitf"
    ) &
    # Wait briefly for the pid file.
    local i
    for i in 1 2 3 4 5 6 7 8 9 10; do
        [ -s "$pidf" ] && break
        sleep 0.1
    done
}

start_server() {
    rm -f "$SRV_INTENT"
    log "starting server on :$SERVER_PORT ($SERVER_HUBS hubs)"
    SITE_PORT="$SERVER_PORT" SITE_HUBS="$SERVER_HUBS" SITE_RUNDIR="$RUN" \
        spawn "$SRV_PID" "$SRV_EXIT" "$SRV_LOG" "$PY" "$HERE/site.py"
    SERVER_PID="$(cat "$SRV_PID" 2>/dev/null || echo 0)"
    log "server pid=$SERVER_PID"
}

start_client() {
    rm -f "$CLI_INTENT"
    log "starting client -> :$SERVER_PORT (burst=$CLIENT_BURST interval=${CLIENT_INTERVAL}s)"
    CLIENT_PORT="$SERVER_PORT" CLIENT_HUBS="$CLIENT_HUBS" CLIENT_BURST="$CLIENT_BURST" \
        CLIENT_INTERVAL="$CLIENT_INTERVAL" CLIENT_RUNDIR="$RUN" \
        spawn "$CLI_PID" "$CLI_EXIT" "$CLI_LOG" "$PY" "$HERE/burst_client.py"
    CLIENT_PID="$(cat "$CLI_PID" 2>/dev/null || echo 0)"
    log "client pid=$CLIENT_PID"
}

health_ok() {
    local out
    out=$(curl -s --max-time "$HEALTH_TIMEOUT" "http://127.0.0.1:$SERVER_PORT/health" 2>/dev/null)
    [ "$out" = "ok" ]
}

heartbeat_stale() {
    local ts now
    [ -f "$RUN/health.json" ] || return 0
    ts=$("$PY" -c "import json,sys;print(json.load(open('$RUN/health.json'))['ts'])" 2>/dev/null) || return 0
    now=$(date +%s)
    awk "BEGIN{exit !(($now - $ts) > $HEARTBEAT_STALE)}"
}

signame() { kill -l "$1" 2>/dev/null || echo "?"; }

incident_crash() {
    local name="$1" code="$2" logf="$3" crashf="$4"
    local sig=$((code - 128))
    local core stamp f
    stamp=$(date +%Y%m%d-%H%M%S)
    f="$INC/INCIDENT-$stamp-$name-crash.md"
    core=$(ls -t "$CORES"/core.* 2>/dev/null | head -1)
    {
        echo "# CRASH: $name exited code=$code (signal $sig $(signame $sig))"
        echo; echo "_$(date -Is)_  pid was ${5:-?}"; echo
        echo "## last 40 log lines ($logf)"; echo '```'; tail -40 "$logf" 2>/dev/null; echo '```'
        echo "## runloom crash report ($crashf)"; echo '```'; tail -80 "$crashf" 2>/dev/null; echo '```'
        if [ -n "$core" ]; then
            echo "## core: $core ($(du -h "$core" 2>/dev/null | cut -f1))"
            local full="${f%.md}.gdb.txt"
            bash "$HERE/gdb_dump.sh" core "$core" > "$full" 2>&1
            echo "## gdb backtrace -- full in $(basename "$full")"
            echo '```'; cat "$full"; echo '```'
        else
            echo "## core: (none found)"
        fi
    } > "$f"
    log "CRASH incident -> $f"
    touch "$RUN/NEW_INCIDENT"
}

incident_hang() {
    local name="$1" pid="$2" logf="$3"
    local stamp f
    stamp=$(date +%Y%m%d-%H%M%S)
    f="$INC/INCIDENT-$stamp-$name-hang.md"
    log "HANG detected ($name pid=$pid) -- kill -QUIT for goroutine dump, then gdb snapshot"
    kill -QUIT "$pid" 2>/dev/null
    sleep 1
    {
        echo "# HANG: $name pid=$pid stopped making progress"
        echo; echo "_$(date -Is)_"; echo
        echo "## last 40 log lines ($logf) -- includes the SIGQUIT goroutine dump"
        echo '```'; tail -40 "$logf" 2>/dev/null; echo '```'
        if [ -f "$RUN/health.json" ]; then
            echo "## last heartbeat (run/health.json)"; echo '```'; cat "$RUN/health.json" 2>/dev/null; echo '```'
        fi
        # Snapshot the WEDGED process to a core BEFORE we touch it, so the full
        # state (every goroutine frame, the holder of any contended lock, the
        # CPython critical-section stacks) is recoverable offline with py-bt.
        local core="$CORES/hang.$pid.$(date +%s)"
        if gcore -o "$core" "$pid" >/dev/null 2>&1; then
            echo "## gcore: ${core}.${pid}"
        fi
        # Full, UNTRUNCATED live backtrace (all threads, C + py-bt) -> sidecar.
        local full="${f%.md}.gdb.txt"
        bash "$HERE/gdb_dump.sh" pid "$pid" > "$full" 2>&1
        echo "## live gdb backtrace (all threads) -- full in $(basename "$full")"
        echo '```'; cat "$full"; echo '```'
    } > "$f"
    log "HANG incident -> $f"
    touch "$RUN/NEW_INCIDENT"
}

kill_proc() {  # pid intentional-flag exitfile
    local pid="$1" intent="$2" exitf="$3" i
    touch "$intent"
    kill -TERM "$pid" 2>/dev/null
    for i in $(seq 1 20); do [ -f "$exitf" ] && return; sleep 0.2; done
    kill -KILL "$pid" 2>/dev/null
    for i in $(seq 1 10); do [ -f "$exitf" ] && return; sleep 0.2; done
}

write_status() {
    cat > "$RUN/status.txt" <<EOF
updated:   $(date -Is)
server:    pid=$SERVER_PID port=$SERVER_PORT strikes=$server_strikes health=$1
client:    pid=$CLIENT_PID stall=${client_stall}s
incidents: $(ls "$INC"/INCIDENT-* 2>/dev/null | wc -l)
EOF
}

shutdown() {
    log "supervisor shutting down -- stopping children"
    [ -n "${SERVER_PID:-}" ] && { touch "$SRV_INTENT"; kill -TERM "$SERVER_PID" 2>/dev/null; }
    [ -n "${CLIENT_PID:-}" ] && { touch "$CLI_INTENT"; kill -TERM "$CLIENT_PID" 2>/dev/null; }
    sleep 1
    [ -n "${SERVER_PID:-}" ] && kill -KILL "$SERVER_PID" 2>/dev/null
    [ -n "${CLIENT_PID:-}" ] && kill -KILL "$CLIENT_PID" 2>/dev/null
    rm -f "$RUN/supervisor.pid"
    exit 0
}
trap shutdown TERM INT

# -------------------------------------------------------------------
echo "$$" > "$RUN/supervisor.pid"
setup_system
log "supervisor up (pid $$, server :$SERVER_PORT, client burst=$CLIENT_BURST/${CLIENT_INTERVAL}s)"
start_server
sleep 2
start_client

server_strikes=0
client_stall=0
last_client_size=0

while true; do
    sleep "$HEALTH_INTERVAL"
    rotate_cores

    # ---- SERVER ----
    if [ -f "$SRV_EXIT" ]; then
        code=$(cat "$SRV_EXIT" 2>/dev/null || echo 0)
        if [ -f "$SRV_INTENT" ]; then
            log "server exited (intentional, code=$code)"
        else
            log "server DIED code=$code"
            incident_crash server "$code" "$SRV_LOG" "$RUN/crash_report.txt" "$SERVER_PID"
        fi
        start_server
        server_strikes=0
    else
        if health_ok; then
            server_strikes=0
            server_health="ok"
        else
            server_strikes=$((server_strikes + 1))
            server_health="bad($server_strikes)"
            log "server health probe failed ($server_strikes/$HANG_STRIKES)"
            # Sustained /health failure IS the wedge signal -- the process may
            # be alive (a stranded hub, a recoverable-but-stuck goroutine) with
            # its heartbeat still ticking, so do NOT gate on heartbeat staleness.
            if [ "$server_strikes" -ge "$HANG_STRIKES" ]; then
                if heartbeat_stale; then
                    log "  heartbeat ALSO stale -> full scheduler wedge"
                else
                    log "  heartbeat still fresh -> service dead but process alive (stranded hub)"
                fi
                incident_hang server "$SERVER_PID" "$SRV_LOG"
                kill_proc "$SERVER_PID" "$SRV_INTENT" "$SRV_EXIT"
                start_server
                server_strikes=0
            fi
        fi
    fi
    : "${server_health:=unknown}"

    # ---- CLIENT ----
    if [ -f "$CLI_EXIT" ]; then
        code=$(cat "$CLI_EXIT" 2>/dev/null || echo 0)
        if [ -f "$CLI_INTENT" ]; then
            log "client exited (intentional, code=$code)"
        else
            log "client DIED code=$code"
            incident_crash client "$code" "$CLI_LOG" "$RUN/client_crash_report.txt" "$CLIENT_PID"
        fi
        start_client
        client_stall=0
        last_client_size=0
    else
        size=$(stat -c %s "$CLI_LOG" 2>/dev/null || echo 0)
        if [ "$size" -gt "$last_client_size" ]; then
            last_client_size=$size
            client_stall=0
        else
            client_stall=$((client_stall + HEALTH_INTERVAL))
            if [ "$client_stall" -ge "$CLIENT_STALL_S" ]; then
                incident_hang client "$CLIENT_PID" "$CLI_LOG"
                kill_proc "$CLIENT_PID" "$CLI_INTENT" "$CLI_EXIT"
                start_client
                client_stall=0
                last_client_size=0
            fi
        fi
    fi

    write_status "$server_health"
done
