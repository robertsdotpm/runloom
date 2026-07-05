import asyncio, socket
async def main():
    loop=asyncio.get_event_loop()
    server=await loop.create_server(asyncio.Protocol,'127.0.0.1',0)
    port=server.sockets[0].getsockname()[1]
    t=loop.create_task(server.serve_forever())
    await asyncio.sleep(0.1); t.cancel()
    try: await t
    except asyncio.CancelledError: pass
    await asyncio.sleep(0.2)
    s=socket.socket(); s.settimeout(2)
    try: s.connect(('127.0.0.1',port)); ok=True; s.close()
    except OSError: ok=False
    server.close(); return ok
print('connect after cancel succeeded:', asyncio.run(main()))
