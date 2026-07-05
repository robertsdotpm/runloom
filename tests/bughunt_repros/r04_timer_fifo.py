"""Timer semantics: equal-deadline call_at callbacks must fire in FIFO
(insertion) order, like asyncio's heap (counter tiebreak). Also call_soon FIFO."""
import sys, asyncio
import runloom.aio as aio

async def main():
    loop = asyncio.get_event_loop()
    order = []
    when = loop.time() + 0.05
    n = 20
    for i in range(n):
        loop.call_at(when, order.append, i)
    await asyncio.sleep(0.3)
    return order

async def main2():
    loop = asyncio.get_event_loop()
    order = []
    for i in range(50):
        loop.call_soon(order.append, i)
    await asyncio.sleep(0.1)
    return order

order = aio.run(main())
print("call_at equal deadline order:", order)
ok = order == sorted(order) and len(order) == 20
order2 = aio.run(main2())
print("call_soon order ok:", order2 == list(range(50)), len(order2))
if not ok:
    print("BUG: equal-deadline call_at not FIFO")
    sys.exit(1)
if order2 != list(range(50)):
    print("BUG: call_soon not FIFO")
    sys.exit(1)
print("OK")
