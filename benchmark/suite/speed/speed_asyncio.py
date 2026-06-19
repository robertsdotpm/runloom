"""The [asyncio] and [uvloop] columns of the speed benchmark (single-threaded,
GIL build -- decision #4). --loop selects the event loop policy.

  spawn      : gather N no-op coroutines  -> seconds (orchestrator baselines n=0)
  ctxswitch  : G tasks each `await sleep(0)` K times (loaded-yield) -> seconds
  rtt        : 1 stream, N sequential round-trips to a Go echo server
  http       : H tasks, keepalive HTTP/1.1 GET vs a Go httpd, windowed req/s
"""
import argparse
import asyncio
import json
import socket
import time

HTTP_REQ = b"GET / HTTP/1.1\r\nHost: b\r\nConnection: keep-alive\r\n\r\n"


async def m_spawn(n):
    async def noop():
        return
    t0 = time.perf_counter()
    if n:
        await asyncio.gather(*[noop() for _ in range(n)])
    return {"seconds": time.perf_counter() - t0, "n": n, "cores": 1}


async def m_ctxswitch(n):
    G = 64
    K = max(1, n // G)

    async def worker():
        for _ in range(K):
            await asyncio.sleep(0)
    t0 = time.perf_counter()
    await asyncio.gather(*[worker() for _ in range(G)])
    return {"seconds": time.perf_counter() - t0, "switches": G * K,
            "fibers": G, "yields_each": K, "cores": 1}


async def m_rtt(host, port, n, payload):
    reader, writer = await asyncio.open_connection(host, port)
    sock = writer.get_extra_info("socket")
    if sock:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    msg = b"\xab" * payload
    for _ in range(1000):
        writer.write(msg)
        await writer.drain()
        await reader.readexactly(payload)
    t0 = time.perf_counter()
    for _ in range(n):
        writer.write(msg)
        await writer.drain()
        await reader.readexactly(payload)
    dt = time.perf_counter() - t0
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass
    return {"ns_per_rtt": dt * 1e9 / n, "n": n, "payload": payload, "cores": 1}


def _content_length(header):
    for line in header.split(b"\r\n"):
        if line[:15].lower() == b"content-length:":
            return int(line.split(b":", 1)[1])
    return 0


async def m_http(host, port, conns, ramp, measure):
    state = {"measuring": False, "stop": False}
    counters = [0] * conns

    async def worker(idx):
        try:
            reader, writer = await asyncio.open_connection(host, port)
        except Exception:
            return
        sock = writer.get_extra_info("socket")
        if sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            while not state["stop"]:
                writer.write(HTTP_REQ)
                await writer.drain()
                header = await reader.readuntil(b"\r\n\r\n")
                cl = _content_length(header)
                if cl:
                    await reader.readexactly(cl)
                if state["measuring"]:
                    counters[idx] += 1
        except Exception:
            pass
        finally:
            writer.close()

    tasks = [asyncio.create_task(worker(i)) for i in range(conns)]
    await asyncio.sleep(ramp)
    state["measuring"] = True
    t0 = time.perf_counter()
    await asyncio.sleep(measure)
    state["measuring"] = False
    state["stop"] = True
    elapsed = time.perf_counter() - t0
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    total = sum(counters)
    return {"rps": total / elapsed, "reqs": total, "measure_s": elapsed,
            "conns": conns, "cores": 1}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--metric", required=True,
                    choices=["spawn", "ctxswitch", "rtt", "http"])
    ap.add_argument("--loop", default="asyncio", choices=["asyncio", "uvloop"])
    ap.add_argument("--n", type=int, default=1_000_000)
    ap.add_argument("--host", default="10.99.0.1")
    ap.add_argument("--port", type=int, default=9100)
    ap.add_argument("--payload", type=int, default=64)
    ap.add_argument("--conns", type=int, default=64)
    ap.add_argument("--ramp", type=float, default=1.0)
    ap.add_argument("--measure", type=float, default=3.0)
    args = ap.parse_args()
    if args.loop == "uvloop":
        import uvloop
        uvloop.install()
    rt = args.loop

    if args.metric == "spawn":
        res = asyncio.run(m_spawn(args.n))
    elif args.metric == "ctxswitch":
        res = asyncio.run(m_ctxswitch(args.n))
    elif args.metric == "rtt":
        res = asyncio.run(m_rtt(args.host, args.port, args.n, args.payload))
    else:
        res = asyncio.run(m_http(args.host, args.port, args.conns, args.ramp, args.measure))
    res.update({"runtime": rt, "metric": args.metric})
    print(json.dumps(res))


if __name__ == "__main__":
    main()
