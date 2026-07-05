import asyncio
async def main():
    async def handler(r, w):
        w.write(b'a\nb\n'); await w.drain(); w.close()
    server = await asyncio.start_server(handler, '127.0.0.1', 0)
    host, port = server.sockets[0].getsockname()[:2]
    reader, writer = await asyncio.open_connection(host, port)
    async for line in reader:
        print(line)
    print("aiter present:", hasattr(asyncio.StreamReader, "__aiter__"))
asyncio.run(main())
