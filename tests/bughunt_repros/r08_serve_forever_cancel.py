"""asyncio.Server.serve_forever(): cancelling it must CLOSE the server
(stock asyncio: except CancelledError -> self.close(); await wait_closed()).
Check loop.create_server's _ProtocolServer."""
import sys, asyncio, socket
import runloom.aio as aio

async def main():
    loop = asyncio.get_event_loop()
    server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    t = loop.create_task(server.serve_forever())
    await asyncio.sleep(0.1)
    t.cancel()
    try:
        await t
    except asyncio.CancelledError:
        pass
    await asyncio.sleep(0.2)
    # stock asyncio: server is now closed; a connect gets refused.
    print("is_serving after cancel:", server.is_serving())
    s = socket.socket()
    s.settimeout(2)
    try:
        s.connect(("127.0.0.1", port))
        connected = True
        s.close()
    except OSError:
        connected = False
    server.close()
    return server.is_serving(), connected

serving, connected = aio.run(main())
if connected or serving:
    print("BUG: cancelling serve_forever() left the server accepting "
          "(stock asyncio closes it): serving=%s connect_succeeded=%s"
          % (serving, connected))
    sys.exit(1)
print("OK")
