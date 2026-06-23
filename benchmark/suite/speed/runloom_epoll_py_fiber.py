"""The [runloom] column of the speed benchmark. All metrics run under the REAL
M:N scheduler runloom.run(hubs) -- never run(1), which is the different M:1
cooperative scheduler (decision #5).

  --metric spawn      : spawn N no-op fibers, drain  -> seconds (orchestrator
                        subtracts an n=0 startup baseline)
  --metric ctxswitch  : 2-fiber unbuffered-Chan ping-pong, N round-trips -> seconds
  --metric rtt        : 1 fiber, N sequential round-trips to a Go echo server
  --metric http       : H fibers, keepalive HTTP/1.1 GET vs a Go httpd, windowed req/s
"""
import argparse
import json
import os
import socket
import time

import runloom
import runloom_c
import runloom.sync as rs

HUBS_DEFAULT = int((os.cpu_count() or 1) * 0.7)


def noop():
    pass


def m_spawn(n, hubs, stack_size=0):
    # Naked single-spawn rate.  Default (stack_size=0) uses runloom.fiber_fast --
    # a thin Python spawn with no per-spawn work, the apples-to-apples vs Go's
    # `go f()`.  The DEFAULT runloom.fiber adds the grow-down auto-sizer (small
    # right-sized stacks, an RSS feature Go lacks); its learned size now spawns
    # down the DEFERRED stack-alloc path, so the default is ~1.7M/s warm
    # (small-stacks AND fast) -- not the old ~7x-slower eager-alloc number.
    # optimize("throughput")/("memory") swaps runloom.fiber between fiber_fast and
    # grow-down.  stack_size>0 pins each fiber's C stack (decomposition variant).
    if stack_size > 0:
        def root():
            for _ in range(n):
                runloom.fiber(noop, stack_size=stack_size)
    else:
        def root():
            f = runloom.fiber_fast
            for _ in range(n):
                f(noop)
    t0 = time.perf_counter()
    runloom.run(hubs, root)
    return {"seconds": time.perf_counter() - t0, "n": n, "cores": hubs,
            "stack_size": stack_size}


def _make_distinct_worker(K, yobj):
    # The real contention is SHARED CLOSURE CELLS, not the code object (proven in
    # SCHEDULER_SCALING_FINDINGS.md "CORRECTION" + suite/speed/hot_diag.py: one
    # SHARED code object scales fine; one SHARED closure's cells do not).  This
    # worker reads `sy`/`K` as GLOBALS in its own dict, so it has NO shared cells
    # -- which is why it scales.  (The `shared` mode uses a nested closure, so all
    # fibers share its cells == the wall.)  User-facing fix: @runloom.hot.
    g = {"sy": yobj, "K": K, "__builtins__": __builtins__}
    exec(compile("def w():\n for _ in range(K):\n  sy()", "<w>", "exec"), g)
    return g["w"]


def m_ctxswitch(n, hubs, distinct=False):
    # "Context switch under load": G concurrent fibers each yield K times, so the
    # hubs stay full of ready work and switches are same-hub re-dispatch -- the
    # realistic cost a loaded server pays, NOT a 2-fiber ping-pong (which forces
    # a cross-hub wake of a freshly-parked idle hub every op: ~30us, pathological
    # and unrepresentative). G*K == n total switches.
    #
    # The yield object is runloom_c.sched_yield, an IMMORTAL process-lifetime
    # singleton (module_init.c.inc), so the per-yield refcount-contention layer
    # is already gone for both modes.  --distinct ALSO de-shares the code object
    # (above), so the only residual cost is the per-yield Python frame itself,
    # which is per-hub-parallel and therefore scales.  shared (default) == the
    # naive "one handler fn for every fiber" server; distinct == the fixed path.
    G = max(2, hubs * 16)
    K = max(1, n // G)
    sched_yield = runloom_c.sched_yield

    if distinct:
        workers = [_make_distinct_worker(K, sched_yield) for _ in range(G)]

        def root():
            for w in workers:
                runloom.fiber(w)
    else:
        def worker():
            for _ in range(K):
                sched_yield()

        def root():
            for _ in range(G):
                runloom.fiber(worker)
    t0 = time.perf_counter()
    runloom.run(hubs, root)
    return {"seconds": time.perf_counter() - t0, "n": n, "cores": hubs,
            "switches": G * K, "fibers": G, "yields_each": K,
            "mode": "distinct" if distinct else "shared"}


def _recvn(sock, n):
    got = 0
    while got < n:
        b = sock.recv(n - got)
        if not b:
            return False
        got += len(b)
    return True


def m_rtt(host, port, n, payload):
    out = {}

    def root():
        s = rs.tcp_connect(host, port)
        try:
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except Exception:
            pass
        msg = b"\xab" * payload
        for _ in range(1000):           # warmup
            s.sendall(msg)
            _recvn(s, payload)
        t0 = time.perf_counter()
        for _ in range(n):
            s.sendall(msg)
            _recvn(s, payload)
        out["seconds"] = time.perf_counter() - t0
        s.close()
    runloom.run(2, root)
    return {"ns_per_rtt": out["seconds"] * 1e9 / n, "n": n, "payload": payload,
            "cores": 1}


HTTP_REQ = b"GET / HTTP/1.1\r\nHost: b\r\nConnection: keep-alive\r\n\r\n"


def _http_once(sock):
    sock.sendall(HTTP_REQ)
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(4096)
        if not chunk:
            return False
        data += chunk
    header, _, rest = data.partition(b"\r\n\r\n")
    cl = 0
    for line in header.split(b"\r\n"):
        if line[:15].lower() == b"content-length:":
            cl = int(line.split(b":", 1)[1])
    body = rest
    while len(body) < cl:
        chunk = sock.recv(4096)
        if not chunk:
            return False
        body += chunk
    return True


def m_http(host, port, hubs, conns, ramp, measure):
    counters = bytearray(8 * conns)  # sharded counters (race-free, 1 writer each)
    import struct
    state = {"measuring": False, "stop": False, "live": 0}

    def worker(idx):
        try:
            s = rs.tcp_connect(host, port)
        except Exception:
            return
        try:
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except Exception:
            pass
        c = 0
        while not state["stop"]:
            if not _http_once(s):
                break
            if state["measuring"]:
                c += 1
        struct.pack_into("<q", counters, idx * 8, c)
        try:
            s.close()
        except Exception:
            pass

    def timer():
        runloom.sleep(ramp)
        state["measuring"] = True
        t0 = time.perf_counter()
        runloom.sleep(measure)
        state["measuring"] = False
        state["stop"] = True
        state["elapsed"] = time.perf_counter() - t0

    def root():
        for i in range(conns):
            runloom.fiber(worker, i)
        runloom.fiber(timer)

    runloom.run(hubs, root)
    total = sum(struct.unpack_from("<q", counters, i * 8)[0] for i in range(conns))
    elapsed = state.get("elapsed", measure)
    return {"rps": total / elapsed, "reqs": total, "measure_s": elapsed,
            "conns": conns, "cores": hubs}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--metric", required=True,
                    choices=["spawn", "ctxswitch", "rtt", "http"])
    ap.add_argument("--n", type=int, default=1_000_000)
    ap.add_argument("--hubs", type=int, default=HUBS_DEFAULT)
    ap.add_argument("--host", default="10.99.0.1")
    ap.add_argument("--port", type=int, default=9100)
    ap.add_argument("--payload", type=int, default=64)
    ap.add_argument("--conns", type=int, default=64)
    ap.add_argument("--ramp", type=float, default=1.0)
    ap.add_argument("--measure", type=float, default=3.0)
    ap.add_argument("--distinct", action="store_true",
                    help="ctxswitch: give each fiber its own code object "
                         "(de-shares co_code_adaptive; the fixed Python path)")
    ap.add_argument("--stack-size", type=int, default=0,
                    help="spawn: pin each fiber's C stack size in bytes (0 = default)")
    args = ap.parse_args()

    if args.metric == "spawn":
        res = m_spawn(args.n, args.hubs, stack_size=args.stack_size)
    elif args.metric == "ctxswitch":
        res = m_ctxswitch(args.n, args.hubs, distinct=args.distinct)
    elif args.metric == "rtt":
        res = m_rtt(args.host, args.port, args.n, args.payload)
    else:
        res = m_http(args.host, args.port, args.hubs, args.conns, args.ramp, args.measure)
    res.update({"runtime": "runloom", "metric": args.metric})
    print(json.dumps(res))


if __name__ == "__main__":
    main()
