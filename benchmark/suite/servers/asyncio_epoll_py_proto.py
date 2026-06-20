"""Std name: asyncio_epoll_py_proto  (this file ALSO backs uvloop_libuv_py_proto,
launched with --loop uvloop).

Baseline server: canonical asyncio Protocol echo (and uvloop via --loop).

Single-threaded (decision #4: run on the GIL build, its best case). uvloop uses
the identical protocol, only the event-loop policy changes.

--work N applies the SAME FNV-1a byte hash as the runloom work curve (py_fnv,
identical constants) N times over each chunk before echoing, folded into byte 0
so it can't be elided. work=0 is the plain echo. This is the interpreted-Python
reference for the cross-runtime handler work curve (1 core, like the echo bench).
"""
import argparse
import asyncio
import socket

FNV_OFF = 2166136261        # 0x811c9dc5
FNV_PRIME = 16777619        # 0x01000193


def py_fnv(buf, n, passes):
    """Identical to srv_runloom_work.py:py_fnv -- pure inline arithmetic."""
    h = FNV_OFF
    for _ in range(passes):
        for i in range(n):
            h = ((h ^ buf[i]) * FNV_PRIME) & 0xffffffff
    return h


class EchoProtocol(asyncio.Protocol):
    def __init__(self, work):
        self.work = work

    def connection_made(self, transport):
        self.transport = transport
        sock = transport.get_extra_info("socket")
        if sock is not None:
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass

    def data_received(self, data):
        if self.work:
            b = bytearray(data)
            h = py_fnv(b, len(b), self.work)
            b[0] = (b[0] ^ (h & 0xff)) & 0xff   # fold in -> no elision
            self.transport.write(b)
        else:
            self.transport.write(data)   # echo (work=0)


async def amain(host, port, work):
    loop = asyncio.get_running_loop()
    server = await loop.create_server(lambda: EchoProtocol(work), host, port,
                                      backlog=4096, reuse_address=True)
    print("LISTENING %d" % server.sockets[0].getsockname()[1], flush=True)
    async with server:
        await server.serve_forever()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="10.99.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--loop", default="asyncio", choices=["asyncio", "uvloop"])
    ap.add_argument("--work", type=int, default=0, help="FNV passes per chunk (0 = echo)")
    ap.add_argument("--token", default="")
    args = ap.parse_args()
    if args.loop == "uvloop":
        import uvloop
        uvloop.install()
    asyncio.run(amain(args.host, args.port, args.work))


if __name__ == "__main__":
    main()
