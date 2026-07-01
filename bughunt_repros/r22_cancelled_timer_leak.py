"""Cancelled call_later timers: the timer fiber keeps sleeping until the
ORIGINAL deadline even after handle.cancel() (asyncio removes >=50% cancelled
timers from the heap). With aiohttp-style 1h timeouts per request this
accumulates a live fiber (+stack) per request. Measure RSS + do they go away?"""
import sys, os, gc, asyncio
import runloom.aio as aio

def rss():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS"):
                return int(line.split()[1])

async def main():
    loop = asyncio.get_event_loop()
    gc.collect()
    base = rss()
    handles = []
    for i in range(20000):
        h = loop.call_later(3600, lambda: None)
        h.cancel()
        handles.append(h)
    del handles
    gc.collect()
    await asyncio.sleep(0.5)
    gc.collect()
    grow = rss() - base
    print("RSS growth after 20k cancelled 1h timers: %d KiB (%.1f KiB/timer)"
          % (grow, grow / 20000))
    return grow

grow = aio.run(main())
if grow > 100 * 1024:   # >100 MiB for 20k cancelled timers
    print("BUG: cancelled timers retain a live sleeping fiber until the "
          "original deadline -> unbounded memory under timeout-heavy load")
    sys.exit(1)
print("acceptable")
