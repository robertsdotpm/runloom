"""Datagram transport surface: isinstance(asyncio.DatagramTransport),
abort(), reuse_address ValueError (stock raises since 3.8 - CVE-ish)."""
import sys, asyncio
import runloom.aio as aio

problems = []

async def main():
    loop = asyncio.get_event_loop()
    class P(asyncio.DatagramProtocol):
        pass
    tr, proto = await loop.create_datagram_endpoint(
        P, local_addr=("127.0.0.1", 0))
    if not isinstance(tr, asyncio.DatagramTransport):
        problems.append("transport is NOT an asyncio.DatagramTransport "
                        "(isinstance checks in libraries fail)")
    try:
        tr.abort()
    except AttributeError:
        problems.append("DatagramTransport.abort() missing -> AttributeError")
        tr.close()
    # reuse_address: stock asyncio raises ValueError (UDP hijack protection)
    try:
        tr2, _ = await loop.create_datagram_endpoint(
            P, local_addr=("127.0.0.1", 0), reuse_address=True)
        problems.append("reuse_address=True silently accepted and SO_REUSEADDR "
                        "set (stock asyncio raises ValueError; UDP port-hijack "
                        "protection bypassed)")
        tr2.close()
    except ValueError:
        pass

aio.run(main())
for p in problems:
    print("BUG:", p)
if problems:
    sys.exit(1)
print("OK")
