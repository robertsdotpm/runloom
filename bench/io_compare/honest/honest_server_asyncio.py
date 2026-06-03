#!/usr/bin/env python3
"""HONEST_BENCH asyncio server -- tiered workload incl. the 100ms pathological
CPU tier that blocks the single event loop. Usage: host port [io_unused]"""
import asyncio, os, random, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import workload as w
REQ_LEN = 10; RESP = b"200 " + b"x" * 1024 + b"\n"


async def handle(reader, writer):
    rng = random.Random()
    try:
        while True:
            await reader.readexactly(REQ_LEN)
            kind, dur = w.tier(rng.random())
            if kind == "io":
                await asyncio.sleep(dur)
            else:
                w.burn_cpu(100)               # blocks the loop (pathological)
            writer.write(RESP); await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionError, OSError):
        pass
    finally:
        try: writer.close()
        except OSError: pass


async def main(host, port):
    srv = await asyncio.start_server(lambda r, wr: handle(r, wr), host, port,
                                     limit=1 << 20, backlog=65535)
    print("honest-asyncio listening", srv.sockets[0].getsockname(), flush=True)
    async with srv:
        await srv.serve_forever()

if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9000
    try: asyncio.run(main(host, port))
    except KeyboardInterrupt: pass
