import gc, asyncio
import runloom.aio as aio
def rss():
    for line in open('/proc/self/status'):
        if line.startswith('VmRSS'): return int(line.split()[1])
async def main():
    loop=asyncio.get_event_loop(); gc.collect(); base=rss()
    for i in range(20000):
        loop.call_later(3600, lambda: None).cancel()
    gc.collect(); await asyncio.sleep(0.5); gc.collect()
    print('RSS growth: %d KiB' % (rss()-base))
aio.run(main())
