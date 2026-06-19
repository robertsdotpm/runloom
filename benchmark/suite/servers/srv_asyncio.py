"""Baseline server: canonical asyncio Protocol echo (and uvloop via --loop).

Single-threaded (decision #4: run on the GIL build, its best case). uvloop uses
the identical protocol, only the event-loop policy changes.
"""
import argparse
import asyncio
import socket


class EchoProtocol(asyncio.Protocol):
    def connection_made(self, transport):
        self.transport = transport
        sock = transport.get_extra_info("socket")
        if sock is not None:
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass

    def data_received(self, data):
        self.transport.write(data)   # echo


async def amain(host, port):
    loop = asyncio.get_running_loop()
    server = await loop.create_server(EchoProtocol, host, port, backlog=4096,
                                      reuse_address=True)
    print("LISTENING %d" % server.sockets[0].getsockname()[1], flush=True)
    async with server:
        await server.serve_forever()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="10.99.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--loop", default="asyncio", choices=["asyncio", "uvloop"])
    ap.add_argument("--token", default="")
    args = ap.parse_args()
    if args.loop == "uvloop":
        import uvloop
        uvloop.install()
    asyncio.run(amain(args.host, args.port))


if __name__ == "__main__":
    main()
