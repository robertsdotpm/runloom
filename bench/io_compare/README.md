# I/O concurrency comparison — runloom vs gevent / uvloop / asyncio

TCP echo, keepalive wire protocol (10 B req / 1029 B resp), driven by the
common Go loadgen so every runtime competes head-to-head on the identical
workload. `io_ms` simulates backend/DB I/O (the server parks the task).

## Runtimes
- `server_asyncio.py` — stdlib asyncio (single core).
- `server_uvloop.py`  — asyncio + libuv (single core, the fast-asyncio bar).
- `server_gevent.py`  — greenlet + libev, blocking-style (single core) —
  runloom's **direct competitor**.
- `server_runloom.py`    — runloom stackful goroutines; `H` hubs (multi-core on 3.13t).

## Ecosystem note
gevent and uvloop **cannot run on free-threaded 3.13t** (gevent's cffi dep
won't build; uvloop's wheel is GIL-only here for gevent's case). So the
four-way single-core comparison runs on **GIL'd 3.13**; runloom's multi-core
scaling is shown separately on 3.13t (where uvloop/asyncio also run but stay
single-core).

## Run
```sh
go build -o loadgen loadgen.go
# single-core, GIL'd 3.13 (all four comparable):
GP=~/.pyenv/versions/3.13.13/bin/python3
$GP server_gevent.py 127.0.0.1 9001 1 &      # io=1ms
./loadgen -addr 127.0.0.1:9001 -n 1000 -ramp 2 -warmup 2 -measure 6
# runloom: RUNLOOM_SRC=<repo>/src PYTHONPATH=$RUNLOOM_SRC $GP server_runloom.py host port io H
```
Results: see ../results/io_compare.md.
