import asyncio

async def main():
    loop = asyncio.get_event_loop()
    order = []
    when = loop.time() + 0.05
    for i in range(20):
        loop.call_at(when, order.append, i)
    await asyncio.sleep(0.3)
    return order

# force stock selector loop
res = asyncio.run(main())
print("stock asyncio:", res)
print("FIFO?", res == list(range(20)))
