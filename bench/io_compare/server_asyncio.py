#!/usr/bin/env python3
"""asyncio baseline -- single-core event loop, the standard Python answer.

Per-connection coroutine: read fixed REQ, await asyncio.sleep(io_delay)
(simulated backend/DB I/O), write fixed RESP, loop.  asyncio is inherently
single-threaded (one core), which is exactly the limitation pygo's
free-threaded multi-core model is meant to lift -- so this is the honest
"what you'd use today" comparison point.

Usage: server_asyncio.py [host] [port] [io_ms]
"""
import asyncio
import sys

REQ_LEN = 10
RESP = b"200 " + b"x" * 1024 + b"\n"   # 1029 bytes


async def handle(reader, writer, io_s):
    try:
        while True:
            data = await reader.readexactly(REQ_LEN)
            if not data:
                break
            if io_s > 0:
                await asyncio.sleep(io_s)
            writer.write(RESP)
            await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionError, OSError):
        pass
    finally:
        try:
            writer.close()
        except OSError:
            pass


async def main(host, port, io_s):
    server = await asyncio.start_server(
        lambda r, w: handle(r, w, io_s), host, port,
        limit=1 << 20, backlog=65535)
    sock = server.sockets[0]
    # TCP_NODELAY on accepted conns: asyncio sets it by default for
    # start_server streams since 3.6, so latency is clean.
    print("asyncio-server listening on %s io=%sms" % (sock.getsockname(), io_s * 1000),
          flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9000
    io_s = (float(sys.argv[3]) if len(sys.argv) > 3 else 0.0) / 1000.0
    try:
        asyncio.run(main(host, port, io_s))
    except KeyboardInterrupt:
        pass
