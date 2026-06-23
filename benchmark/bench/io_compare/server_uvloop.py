#!/usr/bin/env python3
"""uvloop baseline -- asyncio + libuv, the fast-asyncio bar. Same handler as
server_asyncio.py, only the loop policy differs. Single-core.
Usage: server_uvloop.py [host] [port] [io_ms]"""
import asyncio
import sys
import uvloop

REQ_LEN = 10
RESP = b"200 " + b"x" * 1024 + b"\n"


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
    print("uvloop-server listening on %s io=%sms" % (sock.getsockname(), io_s * 1000),
          flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9000
    io_s = (float(sys.argv[3]) if len(sys.argv) > 3 else 0.0) / 1000.0
    uvloop.install()
    try:
        asyncio.run(main(host, port, io_s))
    except KeyboardInterrupt:
        pass
