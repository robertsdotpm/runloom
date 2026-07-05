"""Historical bug: sequential run_in_executor-heavy runs hanging."""
import sys, asyncio, time
import runloom.aio as aio

def blocking(n):
    time.sleep(0.001)
    return n * 2

async def main():
    loop = asyncio.get_event_loop()
    futs = [loop.run_in_executor(None, blocking, i) for i in range(50)]
    r = await asyncio.gather(*futs)
    return sum(r)

for i in range(15):
    r = aio.run(main())
    assert r == sum(k * 2 for k in range(50)), r
    print("run %d ok" % i, flush=True)
print("OK")
