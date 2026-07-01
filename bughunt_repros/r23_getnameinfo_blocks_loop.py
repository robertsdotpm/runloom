"""loop.getnameinfo() runs the blocking C getnameinfo INLINE on the loop
thread (loop_net.py:357), unlike getaddrinfo which is offloaded. A slow
reverse-DNS lookup freezes EVERY task on the loop. Simulate slowness."""
import sys, time, socket, asyncio
import runloom.aio as aio

orig = socket.getnameinfo
def slow_getnameinfo(sockaddr, flags):
    time.sleep(2.0)          # simulates an unresponsive resolver
    return ("host", "port")
socket.getnameinfo = slow_getnameinfo

async def main():
    loop = asyncio.get_event_loop()
    ticks = []
    async def ticker():
        while True:
            ticks.append(loop.time())
            await asyncio.sleep(0.05)
    t = loop.create_task(ticker())
    await asyncio.sleep(0.2)
    t0 = loop.time()
    await loop.getnameinfo(("127.0.0.1", 80))
    dt = loop.time() - t0
    await asyncio.sleep(0.2)
    t.cancel()
    # largest gap between ticker heartbeats during the lookup:
    gaps = [b - a for a, b in zip(ticks, ticks[1:])]
    return dt, max(gaps)

dt, maxgap = aio.run(main())
socket.getnameinfo = orig
print("getnameinfo took %.2fs; max ticker stall %.2fs" % (dt, maxgap))
if maxgap > 1.0:
    print("BUG: loop.getnameinfo blocks the entire event loop "
          "(stock asyncio offloads it to the executor)")
    sys.exit(1)
print("OK")
