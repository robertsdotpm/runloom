import asyncio
import runloom.aio as aio
async def main():
    loop=asyncio.get_event_loop(); order=[]
    when=loop.time()+0.05
    for i in range(20): loop.call_at(when, order.append, i)
    await asyncio.sleep(0.3); return order
res = aio.run(main())
print("runloom:", res)
print("FIFO?", res == list(range(20)))
