"""asyncio.StreamReader supports `async for line in reader`. The bridge's
StreamReader (returned by aio.open_connection / aio.start_server) claims
API-compat; does async-iteration work?"""
import sys, asyncio
import runloom.aio as aio

async def main():
    async def handler(reader, writer):
        writer.write(b"a\nb\nc\n")
        await writer.drain()
        writer.close()
    server = await aio.start_server(handler, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    reader, writer = await aio.open_connection(host, port)
    lines = []
    async for line in reader:
        lines.append(line)
    writer.close()
    server.close()
    return lines

try:
    lines = aio.run(main())
    print("lines:", lines)
    if lines != [b"a\n", b"b\n", b"c\n"]:
        print("BUG: wrong lines")
        sys.exit(1)
    print("OK")
except TypeError as e:
    print("BUG: async-for over bridge StreamReader raises TypeError: %s" % e)
    sys.exit(1)
